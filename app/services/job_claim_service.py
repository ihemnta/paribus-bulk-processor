"""
PostgreSQL-backed batch claiming for background workers.

This module implements exclusive job claiming and a dry-run release.
It does not invoke the external Hospital Directory API.

The claim and status transition happen in one transaction to
prevent competing workers from claiming the same queued batch.
"""

import logging
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select, update

from app.core.config import get_settings
from app.persistence.database import SessionFactory
from app.persistence.models import BatchJob

logger = logging.getLogger(__name__)


async def claim_queued_batch(
    batch_id: UUID,
    worker_id: str,
) -> int | None:
    """
    Atomically claim a queued batch for one worker.

    PostgreSQL's FOR UPDATE SKIP LOCKED prevents competing
    transactions from claiming the same locked batch row.

    Args:
        batch_id: Explicit UUID of the batch being claimed.
        worker_id: Unique identifier for the worker process.

    Returns:
        Number of hospitals in the claimed batch, or None if
        the batch is missing, locked, or no longer queued.

    The transaction commits before this function returns.
    """
    settings = get_settings()
    now = datetime.now(UTC)

    async with SessionFactory.begin() as session:
        statement = (
            select(BatchJob)
            .where(
                BatchJob.id == batch_id,
                BatchJob.status == "queued",
            )
            .with_for_update(skip_locked=True)
        )

        result = await session.execute(statement)
        batch = result.scalar_one_or_none()

        if batch is None:
            return None

        # This status change occurs while holding the row lock.
        batch.status = "processing"
        batch.lease_owner = worker_id
        batch.lease_expires_at = now + timedelta(seconds=settings.worker_lease_seconds)

        hospital_count = batch.total_hospitals

        await session.flush()

    # The transaction has now committed successfully.
    logger.info(
        "worker.batch.claimed",
        extra={
            "batch_id": str(batch_id),
            "worker_id": worker_id,
            "total_hospitals": hospital_count,
        },
    )

    return hospital_count


async def release_dry_run_claim(
    batch_id: UUID,
    worker_id: str,
) -> bool:
    """
    Release a batch claimed solely for dry-run verification.

    Only the worker holding the matching lease can release it.

    This function must NEVER be used as a general-purpose
    recovery mechanism for batches that performed external
    hospital-creation requests.

    Returns:
        True when the matching claim was released.
        False when the ownership or processing state differs.
    """
    async with SessionFactory.begin() as session:
        statement = (
            update(BatchJob)
            .where(
                BatchJob.id == batch_id,
                BatchJob.status == "processing",
                BatchJob.lease_owner == worker_id,
            )
            .values(
                status="queued",
                lease_owner=None,
                lease_expires_at=None,
            )
            .returning(BatchJob.id)
        )

        result = await session.execute(statement)
        released_id = result.scalar_one_or_none()

    released = released_id is not None

    if released:
        logger.info(
            "worker.dry_run.released",
            extra={
                "batch_id": str(batch_id),
                "worker_id": worker_id,
            },
        )

    return released


async def renew_worker_lease(
    batch_id: UUID,
    worker_id: str,
) -> bool:
    """
    Extend the lease only when the same worker still owns it.

    An expired lease must not be revived automatically because
    another recovery process may already be investigating it.

    Returns:
        True if the lease was renewed and committed.
        False if the worker no longer owns a valid lease.
    """
    now = datetime.now(UTC)
    new_expiry = now + timedelta(seconds=get_settings().worker_lease_seconds)

    async with SessionFactory.begin() as session:
        statement = (
            update(BatchJob)
            .where(
                BatchJob.id == batch_id,
                BatchJob.status.in_(("processing", "activating")),
                BatchJob.lease_owner == worker_id,
                BatchJob.lease_expires_at > now,
            )
            .values(lease_expires_at=new_expiry)
            .returning(BatchJob.id)
        )

        result = await session.execute(statement)
        renewed = result.scalar_one_or_none() is not None

    if renewed:
        logger.debug(
            "worker.lease.renewed",
            extra={
                "batch_id": str(batch_id),
                "worker_id": worker_id,
            },
        )

    return renewed


async def claim_next_queued_batch(worker_id: str) -> UUID | None:
    """
    Atomically claim the oldest available queued batch.

    Returns the claimed batch UUID, or None when no eligible
    batch is available.

    Selection, lease assignment and status transition occur
    in one PostgreSQL transaction.
    """
    if not worker_id:
        raise ValueError("Worker ID cannot be empty.")

    now = datetime.now(UTC)

    expires_at = now + timedelta(seconds=get_settings().worker_lease_seconds)

    async with SessionFactory.begin() as session:
        statement = (
            select(BatchJob)
            .where(BatchJob.status == "queued")
            .order_by(BatchJob.created_at, BatchJob.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )

        batch = await session.scalar(statement)

        if batch is None:
            return None

        batch.status = "processing"
        batch.lease_owner = worker_id
        batch.lease_expires_at = expires_at

        batch_id = batch.id

    logger.info(
        "worker.batch.claimed",
        extra={
            "batch_id": str(batch_id),
            "worker_id": worker_id,
        },
    )

    return batch_id
