"""
Typed asynchronous client for the existing Hospital Directory API.

Responsibilities:
- Construct correctly formatted upstream requests.
- Reuse HTTP connections.
- Validate successful responses.
- Classify known rejections versus uncertain write outcomes.
- Emit structured operational logs.

This module does not:
- Commit database transactions.
- Retry hospital creation automatically.
- Decide whether a batch should be activated.
"""

import logging
from datetime import datetime
from time import perf_counter
from uuid import UUID

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class HospitalRecord(BaseModel):
    """Hospital object returned by the external directory."""

    id: int
    name: str = Field(min_length=1)
    address: str = Field(min_length=1)
    phone: str | None = None
    creation_batch_id: UUID | None = None
    active: bool | None = None
    created_at: datetime | None = None


class UpstreamRejected(Exception):
    """
    The upstream service explicitly rejected a create request.

    We currently treat HTTP 400 and 422 as definitive input
    rejections, subject to the upstream API contract.
    """

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"Upstream rejected the request with HTTP {status_code}")


class UpstreamOutcomeUnknown(Exception):
    """
    An external write may have succeeded, but is unconfirmed.

    Callers must reconcile before deciding whether to retry.
    """


class UpstreamReadFailure(Exception):
    """The external directory could not be read reliably."""


def build_http_client() -> httpx.AsyncClient:
    """
    Create a configured, reusable asynchronous HTTP client.

    The caller owns the client's lifecycle and must close it
    when the processing service shuts down.
    """
    settings = get_settings()

    return httpx.AsyncClient(
        base_url=settings.upstream_base_url.rstrip("/"),
        timeout=httpx.Timeout(
            connect=10.0,
            read=settings.upstream_timeout_seconds,
            write=10.0,
            pool=5.0,
        ),
        limits=httpx.Limits(
            max_connections=settings.upstream_max_connections,
            max_keepalive_connections=settings.upstream_max_connections,
        ),
        follow_redirects=False,
    )


class HospitalDirectoryClient:
    """Typed adapter around the upstream Hospital Directory API."""

    def __init__(self, http_client: httpx.AsyncClient) -> None:
        """
        Accept an externally managed HTTP client.

        Injecting the client makes testing and connection sharing
        straightforward without creating global network resources.
        """
        self._http = http_client

    async def create_hospital(
        self,
        *,
        batch_id: UUID,
        name: str,
        address: str,
        phone: str | None,
    ) -> HospitalRecord:
        """
        Create one inactive hospital associated with a batch.

        A successful response is validated before being returned.

        Raises:
            UpstreamRejected:
                Explicit HTTP 400 or 422 rejection.

            UpstreamOutcomeUnknown:
                Network failure, unexpected HTTP status,
                malformed response, or inconsistent response.

        No automatic retries are performed.
        """
        payload = {
            "name": name,
            "address": address,
            "phone": phone,
            "creation_batch_id": str(batch_id),
        }

        start = perf_counter()

        try:
            response = await self._http.post(
                "/hospitals/",
                json=payload,
            )
        except httpx.RequestError as exc:
            logger.warning(
                "upstream.create.outcome_unknown error_type=%s",
                type(exc).__name__,
                extra={"batch_id": str(batch_id)},
            )
            raise UpstreamOutcomeUnknown(
                "Hospital creation outcome is uncertain."
            ) from exc

        duration_ms = round(
            (perf_counter() - start) * 1000,
            2,
        )

        logger.info(
            "upstream.create.response",
            extra={
                "batch_id": str(batch_id),
                "upstream_status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )

        if response.status_code in {400, 422}:
            raise UpstreamRejected(response.status_code)

        if response.status_code != 200:
            raise UpstreamOutcomeUnknown(
                f"Unexpected creation status: {response.status_code}"
            )

        try:
            hospital = HospitalRecord.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise UpstreamOutcomeUnknown(
                "Creation returned an invalid hospital response."
            ) from exc

        # A successful response must not contradict the batch
        # identity or the inactive-on-create contract.
        if (
            hospital.creation_batch_id is not None
            and hospital.creation_batch_id != batch_id
        ):
            raise UpstreamOutcomeUnknown("Created hospital has an unexpected batch ID.")

        if hospital.active is True:
            raise UpstreamOutcomeUnknown(
                "Hospital unexpectedly active before activation."
            )

        return hospital

    async def get_batch(
        self,
        batch_id: UUID,
    ) -> list[HospitalRecord]:
        """
        Retrieve hospitals belonging to one upstream batch.

        Used for normal verification and future reconciliation.

        A failed GET cannot establish that the batch is empty.
        """
        try:
            response = await self._http.get(f"/hospitals/batch/{batch_id}")
        except httpx.RequestError as exc:
            logger.warning(
                "upstream.batch.read_failed error_type=%s",
                type(exc).__name__,
                extra={"batch_id": str(batch_id)},
            )
            raise UpstreamReadFailure("Could not retrieve the upstream batch.") from exc

        logger.info(
            "upstream.batch.read_response",
            extra={
                "batch_id": str(batch_id),
                "upstream_status_code": response.status_code,
            },
        )

        if response.status_code != 200:
            raise UpstreamReadFailure(
                f"Unexpected batch read status: {response.status_code}"
            )

        try:
            payload = response.json()

            if not isinstance(payload, list):
                raise TypeError("Expected a list of hospital records.")

            return [HospitalRecord.model_validate(item) for item in payload]

        except (ValueError, ValidationError, TypeError) as exc:
            raise UpstreamReadFailure("Upstream batch returned invalid data.") from exc

    async def activate_batch(self, batch_id: UUID) -> None:
        """
        Request activation of an upstream hospital batch.

        HTTP 200 confirms an acknowledged activation request.

        The orchestration layer must subsequently verify the
        expected hospitals are active before marking the local
        batch as completed.
        """
        try:
            response = await self._http.patch(f"/hospitals/batch/{batch_id}/activate")
        except httpx.RequestError as exc:
            logger.warning(
                "upstream.activation.outcome_unknown error_type=%s",
                type(exc).__name__,
                extra={"batch_id": str(batch_id)},
            )
            raise UpstreamOutcomeUnknown(
                "Batch activation outcome is uncertain."
            ) from exc

        logger.info(
            "upstream.activation.response",
            extra={
                "batch_id": str(batch_id),
                "upstream_status_code": response.status_code,
            },
        )

        if response.status_code != 200:
            raise UpstreamOutcomeUnknown(
                f"Unexpected activation status: {response.status_code}"
            )
