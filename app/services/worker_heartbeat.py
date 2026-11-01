"""
Asynchronous heartbeat for an owned background-worker batch.

The heartbeat periodically renews an existing PostgreSQL lease.
It does not claim batches or execute upstream HTTP operations.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.services.job_claim_service import renew_worker_lease

logger = logging.getLogger(__name__)


@asynccontextmanager
async def maintain_worker_lease(
    batch_id: UUID,
    worker_id: str,
    *,
    interval_seconds: float | None = None,
) -> AsyncIterator[asyncio.Event]:
    """
    Renew an owned worker lease until the surrounding work ends.

    Yields an Event that becomes set if renewal fails or the
    worker loses ownership.

    This does not cancel in-flight external HTTP requests.
    The processor's own ownership checks must continue to
    prevent new writes after lease expiry.
    """
    lease_seconds = get_settings().worker_lease_seconds

    interval = interval_seconds if interval_seconds is not None else lease_seconds / 3

    if not 0 < interval < lease_seconds:
        raise ValueError("Heartbeat interval must be shorter than the lease.")

    stop_event = asyncio.Event()
    lease_lost = asyncio.Event()

    async def heartbeat() -> None:
        """Renew periodically until stopped or ownership is lost."""
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=interval,
                )
                return
            except TimeoutError:
                pass

            try:
                renewed = await renew_worker_lease(
                    batch_id,
                    worker_id,
                )
            except SQLAlchemyError as exc:
                logger.error(
                    "worker.heartbeat.failed error_type=%s",
                    type(exc).__name__,
                    extra={
                        "batch_id": str(batch_id),
                        "worker_id": worker_id,
                    },
                )
                lease_lost.set()
                return

            if not renewed:
                logger.warning(
                    "worker.heartbeat.ownership_lost",
                    extra={
                        "batch_id": str(batch_id),
                        "worker_id": worker_id,
                    },
                )
                lease_lost.set()
                return

    task = asyncio.create_task(
        heartbeat(),
        name=f"heartbeat-{batch_id}",
    )

    try:
        yield lease_lost
    finally:
        stop_event.set()
        await task
