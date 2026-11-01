"""
Orchestrate creation and activation for PostgreSQL-backed batches.

The API does not execute this workflow directly.
A separate worker process calls these functions.

Responsibilities:
- Claim queued jobs.
- Maintain leases while creating hospitals.
- Process pending rows using bounded concurrency.
- Activate batches after successful creation.
- Discover batches awaiting activation.
"""

import asyncio
import logging
from uuid import UUID, uuid4

from sqlalchemy import select

from app.core.config import get_settings
from app.integrations.hospital_directory import HospitalDirectoryClient
from app.persistence.database import SessionFactory
from app.persistence.models import BatchJob
from app.services.batch_activation import BatchActivationService
from app.services.batch_processor import process_claimed_batch
from app.services.job_claim_service import (
    claim_next_queued_batch,
    claim_queued_batch,
)
from app.services.worker_heartbeat import maintain_worker_lease

logger = logging.getLogger(__name__)


async def _run_claimed_batch(
    batch_id: UUID,
    worker_id: str,
    client: HospitalDirectoryClient,
) -> str:
    """
    Process one already claimed batch.

    The heartbeat runs concurrently with row processing.
    A lease-loss signal prevents newly scheduled POST requests.

    An unexpected crash leaves durable state for recovery.
    """
    logger.info(
        "worker.processing.started",
        extra={
            "batch_id": str(batch_id),
            "worker_id": worker_id,
        },
    )

    async with maintain_worker_lease(
        batch_id,
        worker_id,
    ) as lease_lost:
        status = await process_claimed_batch(
            batch_id=batch_id,
            worker_id=worker_id,
            client=client,
            max_concurrency=4,
            lease_lost=lease_lost,
        )

    # Don't start another external operation if our heartbeat
    # reported that we lost ownership during processing.
    if lease_lost.is_set():
        logger.warning(
            "worker.processing.lease_lost",
            extra={
                "batch_id": str(batch_id),
                "worker_id": worker_id,
            },
        )
        return "lease_lost"

    if status != "activating":
        return status

    # The creation processor has committed its final state
    # and released the creation lease.
    #
    # Activation obtains a separate ownership lease.
    #
    # Until an activation heartbeat is integrated, bound
    # the activation operation to less than the lease time.
    timeout_seconds = get_settings().worker_lease_seconds / 2

    try:
        async with asyncio.timeout(timeout_seconds):
            return await BatchActivationService().activate_batch(
                batch_id,
                client,
            )

    except TimeoutError:
        # A PATCH may have occurred before cancellation.
        # Do not retry it here. Lease-expiry recovery and
        # read-only activation reconciliation handle this.
        logger.warning(
            "worker.activation.timeout",
            extra={"batch_id": str(batch_id)},
        )
        return "activation_pending_recovery"


async def process_queued_batch_by_id(
    batch_id: UUID,
    client: HospitalDirectoryClient,
) -> str:
    """
    Claim and process an explicitly selected queued batch.

    Intended for controlled smoke tests and manual execution.
    """
    worker_id = uuid4().hex

    claimed = await claim_queued_batch(
        batch_id,
        worker_id,
    )

    if claimed is None:
        return "skipped"

    return await _run_claimed_batch(
        batch_id,
        worker_id,
        client,
    )


async def process_next_queued_batch(
    client: HospitalDirectoryClient,
) -> tuple[UUID, str] | None:
    """
    Find, atomically claim, and process the next queued batch.

    Returns None if the queue currently has no queued jobs.
    """
    worker_id = uuid4().hex

    batch_id = await claim_next_queued_batch(worker_id)

    if batch_id is None:
        return None

    result = await _run_claimed_batch(
        batch_id,
        worker_id,
        client,
    )

    return batch_id, result


async def find_ready_activation_batch() -> UUID | None:
    """
    Find an unowned batch ready to enter activation.

    Activation itself performs the exclusive claim.
    Batches with a prior PATCH intent are excluded because
    they require reconciliation rather than a repeated PATCH.
    """
    async with SessionFactory() as session:
        statement = (
            select(BatchJob.id)
            .where(
                BatchJob.status == "activating",
                BatchJob.lease_owner.is_(None),
                BatchJob.activation_requested_at.is_(None),
            )
            .order_by(BatchJob.created_at, BatchJob.id)
            .limit(1)
        )

        return await session.scalar(statement)
