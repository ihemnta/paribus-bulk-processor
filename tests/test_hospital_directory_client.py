"""Focused tests for external hospital creation outcomes."""

from uuid import uuid4

import httpx
import pytest

from app.integrations.hospital_directory import (
    HospitalDirectoryClient,
    UpstreamOutcomeUnknown,
    UpstreamRejected,
)


@pytest.mark.asyncio
async def test_successful_hospital_creation():
    """A successful POST returns a validated hospital ID."""
    batch_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/hospitals/"

        return httpx.Response(
            200,
            json={
                "id": 101,
                "name": "General Hospital",
                "address": "Patna",
                "phone": None,
                "creation_batch_id": str(batch_id),
                "active": False,
            },
        )

    async with httpx.AsyncClient(
        base_url="https://mock.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = HospitalDirectoryClient(http_client)

        result = await client.create_hospital(
            batch_id=batch_id,
            name="General Hospital",
            address="Patna",
            phone=None,
        )

    assert result.id == 101
    assert result.active is False
    assert result.creation_batch_id == batch_id


@pytest.mark.asyncio
async def test_validation_rejection():
    """HTTP 422 is classified as an explicit rejection."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            json={"detail": "Invalid hospital"},
        )

    async with httpx.AsyncClient(
        base_url="https://mock.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = HospitalDirectoryClient(http_client)

        with pytest.raises(UpstreamRejected):
            await client.create_hospital(
                batch_id=uuid4(),
                name="Hospital",
                address="Patna",
                phone=None,
            )


@pytest.mark.asyncio
async def test_read_timeout_is_uncertain():
    """A timeout must not trigger an automatic second POST."""

    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1

        raise httpx.ReadTimeout(
            "Simulated timeout after request submission.",
            request=request,
        )

    async with httpx.AsyncClient(
        base_url="https://mock.example",
        transport=httpx.MockTransport(handler),
    ) as http_client:
        client = HospitalDirectoryClient(http_client)

        with pytest.raises(UpstreamOutcomeUnknown):
            await client.create_hospital(
                batch_id=uuid4(),
                name="Hospital",
                address="Patna",
                phone=None,
            )

    assert request_count == 1
