"""
Safe activation of a previously created hospital batch.

Responsibilities:
- Claim exclusive activation ownership.
- Verify upstream hospitals before activation.
- Record activation intent before the external PATCH.
- Verify upstream state after activation.
- Atomically mark local hospitals and the batch completed.

No blind activation retries are performed.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select

from app.core.config import get_settings
from app.integrations.hospital_directory import (
    HospitalDirectoryClient,
    HospitalRecord,
    UpstreamOutcomeUnknown,
    UpstreamReadFailure,
)
from app.persistence.database import SessionFactory
from app.persistence.models import BatchHospital, BatchJob

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExpectedHospital:
    """Expected upstream identity and data for one created row."""

    id: int
    name: str
    address: str
    phone: str | None


def hospitals_match(
    batch_id: UUID,
    expected: list[ExpectedHospital],
    actual: list[HospitalRecord],
) -> bool:
    """
    Compare exact hospital identities and business fields.

    This check protects against missing, extra, duplicated, or
    mismatched records. Batch identity must also be confirmed.
    """
    if len(expected) != len(actual):
        return False

    actual_by_id = {hospital.id: hospital for hospital in actual}

    if len(actual_by_id) != len(actual):
        return False

    for item in expected:
        hospital = actual_by_id.get(item.id)

        if hospital is None:
            return False

        if hospital.creation_batch_id != batch_id:
            return False

        if (
            hospital.name != item.name
            or hospital.address != item.address
            or hospital.phone != item.phone
        ):
            return False

    return True


class BatchActivationService:
    """Coordinate upstream activation and durable completion."""

    async def _claim(
        self,
        batch_id: UUID,
        worker_id: str,
    ) -> bool:
        """
        Acquire exclusive activation ownership in PostgreSQL.

        A batch with a previously recorded activation request
        cannot be claimed automatically again. Such a batch
        requires reconciliation instead of repeating PATCH.
        """
        settings = get_settings()

        async with SessionFactory.begin() as session:
            batch = await session.scalar(
                select(BatchJob)
                .where(
                    BatchJob.id == batch_id,
                    BatchJob.status == "activating",
                    BatchJob.lease_owner.is_(None),
                    BatchJob.activation_requested_at.is_(None),
                )
                .with_for_update(skip_locked=True)
            )

            if batch is None:
                return False

            batch.lease_owner = worker_id
            batch.lease_expires_at = datetime.now(UTC) + timedelta(
                seconds=settings.worker_lease_seconds
            )

        logger.info(
            "batch.activation.claimed",
            extra={
                "batch_id": str(batch_id),
                "worker_id": worker_id,
            },
        )

        return True

    async def _load_expected(
        self,
        batch_id: UUID,
    ) -> tuple[int, list[ExpectedHospital]]:
        """Load the durable list of expected upstream hospitals."""
        async with SessionFactory() as session:
            batch = await session.get(BatchJob, batch_id)

            if batch is None:
                raise RuntimeError("Batch not found.")

            rows = (
                await session.scalars(
                    select(BatchHospital)
                    .where(BatchHospital.batch_id == batch_id)
                    .order_by(BatchHospital.row_number)
                )
            ).all()

            if len(rows) != batch.total_hospitals:
                return batch.total_hospitals, []

            if any(
                row.status != "created" or row.upstream_hospital_id is None
                for row in rows
            ):
                return batch.total_hospitals, []

            expected = [
                ExpectedHospital(
                    id=row.upstream_hospital_id,
                    name=row.name,
                    address=row.address,
                    phone=row.phone,
                )
                for row in rows
            ]

            return batch.total_hospitals, expected

    async def _record_activation_intent(
        self,
        batch_id: UUID,
        worker_id: str,
    ) -> None:
        """
        Persist activation intent BEFORE sending PATCH.

        If the process crashes after this commit, recovery
        must verify upstream activation rather than blindly
        issuing the PATCH again.
        """
        async with SessionFactory.begin() as session:
            batch = await session.scalar(
                select(BatchJob).where(BatchJob.id == batch_id).with_for_update()
            )

            if (
                batch is None
                or batch.status != "activating"
                or batch.lease_owner != worker_id
                or batch.activation_requested_at is not None
            ):
                raise RuntimeError("Activation ownership is invalid.")

            if batch.lease_expires_at is None or batch.lease_expires_at <= datetime.now(
                UTC
            ):
                raise RuntimeError("Activation lease has expired.")

            batch.activation_requested_at = datetime.now(UTC)

        logger.info(
            "batch.activation.intent_committed",
            extra={
                "batch_id": str(batch_id),
                "worker_id": worker_id,
            },
        )

    async def _finish(
        self,
        batch_id: UUID,
        worker_id: str,
        *,
        completed: bool,
        error_code: str | None = None,
    ) -> str:
        """
        Atomically persist the activation result.

        All row updates and the batch-level state change occur
        within one PostgreSQL transaction.
        """
        now = datetime.now(UTC)

        async with SessionFactory.begin() as session:
            batch = await session.scalar(
                select(BatchJob).where(BatchJob.id == batch_id).with_for_update()
            )

            if (
                batch is None
                or batch.status != "activating"
                or batch.lease_owner != worker_id
            ):
                raise RuntimeError("Activation ownership changed unexpectedly.")

            if completed:
                rows = (
                    await session.scalars(
                        select(BatchHospital)
                        .where(BatchHospital.batch_id == batch_id)
                        .with_for_update()
                    )
                ).all()

                if len(rows) != batch.total_hospitals or any(
                    row.status != "created" for row in rows
                ):
                    raise RuntimeError("Cannot complete an inconsistent batch.")

                for row in rows:
                    row.status = "activated"

                batch.status = "completed"
                batch.completed_at = now
                batch.activation_confirmed_at = now
                batch.last_error = None

            else:
                batch.status = "needs_reconciliation"
                batch.last_error = error_code

            batch.lease_owner = None
            batch.lease_expires_at = None

            final_status = batch.status

        logger.info(
            "batch.activation.finished",
            extra={
                "batch_id": str(batch_id),
                "batch_status": final_status,
                "worker_id": worker_id,
            },
        )

        return final_status

    async def activate_batch(
        self,
        batch_id: UUID,
        client: HospitalDirectoryClient,
    ) -> str:
        """
        Activate one ready batch and verify the final outcome.

        Returns:
            completed:
                Every expected hospital is confirmed active.
            needs_reconciliation:
                An external result was missing or uncertain.
            skipped:
                This invocation did not acquire activation ownership.

        No automatic PATCH retry is performed.
        """
        worker_id = uuid4().hex

        if not await self._claim(batch_id, worker_id):
            return "skipped"

        total, expected = await self._load_expected(batch_id)

        if len(expected) != total:
            return await self._finish(
                batch_id,
                worker_id,
                completed=False,
                error_code="incomplete_local_hospital_records",
            )

        # Read before writing: ensure that the external batch
        # contains exactly the hospitals we expect.
        try:
            before = await client.get_batch(batch_id)
        except UpstreamReadFailure:
            return await self._finish(
                batch_id,
                worker_id,
                completed=False,
                error_code="preactivation_read_failed",
            )

        if not hospitals_match(batch_id, expected, before):
            return await self._finish(
                batch_id,
                worker_id,
                completed=False,
                error_code="upstream_batch_mismatch",
            )

        # Recovery-friendly case: all hospitals are already active.
        if all(hospital.active is True for hospital in before):
            return await self._finish(
                batch_id,
                worker_id,
                completed=True,
            )

        # Mixed or missing active values require investigation.
        if not all(hospital.active is False for hospital in before):
            return await self._finish(
                batch_id,
                worker_id,
                completed=False,
                error_code="unexpected_preactivation_state",
            )

        # This commit must happen before the PATCH request.
        await self._record_activation_intent(batch_id, worker_id)

        try:
            await client.activate_batch(batch_id)
        except UpstreamOutcomeUnknown:
            logger.warning(
                "batch.activation.outcome_unknown",
                extra={"batch_id": str(batch_id)},
            )

        # Even when PATCH returned HTTP 200, verify the result.
        # Even when PATCH timed out, a successful GET may prove
        # that activation actually completed.
        try:
            after = await client.get_batch(batch_id)
        except UpstreamReadFailure:
            return await self._finish(
                batch_id,
                worker_id,
                completed=False,
                error_code="postactivation_read_failed",
            )

        if hospitals_match(batch_id, expected, after) and all(
            hospital.active is True for hospital in after
        ):
            return await self._finish(
                batch_id,
                worker_id,
                completed=True,
            )

        return await self._finish(
            batch_id,
            worker_id,
            completed=False,
            error_code="activation_not_confirmed",
        )
