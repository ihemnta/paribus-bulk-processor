"""
Durable, bounded-concurrency hospital creation processor.

The processor operates only on a previously claimed batch.
It does not claim jobs, retry unknown external writes,
or activate a batch.

Every row transition is committed independently.
No database transaction remains open during HTTP requests.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import select

from app.integrations.hospital_directory import (
    HospitalDirectoryClient,
    UpstreamOutcomeUnknown,
    UpstreamRejected,
)
from app.persistence.database import SessionFactory
from app.persistence.models import BatchHospital, BatchJob

logger = logging.getLogger(__name__)

RowResult = Literal["created", "failed", "unknown"]


class WorkerOwnershipError(RuntimeError):
    """The worker no longer owns the expected batch."""


@dataclass(frozen=True, slots=True)
class HospitalWorkItem:
    """Immutable snapshot of a hospital row prepared for HTTP work."""

    id: int
    row_number: int
    name: str
    address: str
    phone: str | None
    attempt: int


async def _check_owner(
    session,
    batch_id: UUID,
    worker_id: str,
    *,
    require_valid_lease: bool = False,
) -> BatchJob:
    """
    Lock and validate the batch's worker ownership.

    The calling transaction holds the batch lock until commit.
    An expired lease prevents starting additional external writes.
    """
    batch = await session.scalar(
        select(BatchJob).where(BatchJob.id == batch_id).with_for_update()
    )

    if batch is None or batch.status != "processing" or batch.lease_owner != worker_id:
        raise WorkerOwnershipError("Batch is not owned by this worker.")

    if require_valid_lease and (
        batch.lease_expires_at is None or batch.lease_expires_at <= datetime.now(UTC)
    ):
        raise WorkerOwnershipError("Worker lease has expired.")

    return batch


async def _load_pending_ids(
    batch_id: UUID,
    worker_id: str,
) -> list[int]:
    """Load the IDs of rows that have never been attempted."""
    async with SessionFactory() as session:
        batch = await session.get(BatchJob, batch_id)

        if (
            batch is None
            or batch.status != "processing"
            or batch.lease_owner != worker_id
        ):
            raise WorkerOwnershipError("Cannot load rows without batch ownership.")

        result = await session.scalars(
            select(BatchHospital.id)
            .where(
                BatchHospital.batch_id == batch_id,
                BatchHospital.status == "pending",
            )
            .order_by(BatchHospital.row_number)
        )

        return list(result.all())


async def _mark_creating(
    batch_id: UUID,
    row_id: int,
    worker_id: str,
) -> HospitalWorkItem:
    """
    Commit the intent to send a hospital creation request.

    This commit MUST finish before the external POST begins.
    A crash after this transition leaves the row in 'creating',
    requiring reconciliation rather than an automatic retry.
    """
    async with SessionFactory.begin() as session:
        await _check_owner(
            session,
            batch_id,
            worker_id,
            require_valid_lease=True,
        )

        row = await session.scalar(
            select(BatchHospital)
            .where(
                BatchHospital.id == row_id,
                BatchHospital.batch_id == batch_id,
            )
            .with_for_update()
        )

        if row is None or row.status != "pending":
            raise RuntimeError("Hospital row is not pending.")

        row.status = "creating"
        row.attempt_count += 1
        row.last_attempted_at = datetime.now(UTC)
        row.last_error = None

        item = HospitalWorkItem(
            id=row.id,
            row_number=row.row_number,
            name=row.name,
            address=row.address,
            phone=row.phone,
            attempt=row.attempt_count,
        )

    logger.info(
        "hospital.create.started",
        extra={
            "batch_id": str(batch_id),
            "hospital_row_id": row_id,
            "attempt": item.attempt,
        },
    )

    return item


async def _save_result(
    batch_id: UUID,
    worker_id: str,
    row_id: int,
    status: RowResult,
    *,
    upstream_id: int | None = None,
    error_code: str | None = None,
) -> None:
    """
    Persist one confirmed or uncertain hospital creation outcome.

    Only the owning worker may update a row currently in
    'creating' state. Error codes must be sanitized and must
    not contain raw upstream payloads or hospital information.
    """
    async with SessionFactory.begin() as session:
        await _check_owner(session, batch_id, worker_id)

        row = await session.scalar(
            select(BatchHospital)
            .where(
                BatchHospital.id == row_id,
                BatchHospital.batch_id == batch_id,
            )
            .with_for_update()
        )

        if row is None or row.status != "creating":
            raise RuntimeError("Unexpected hospital row transition.")

        row.status = status
        row.upstream_hospital_id = upstream_id
        row.last_error = error_code

    logger.info(
        "hospital.create.finished",
        extra={
            "batch_id": str(batch_id),
            "hospital_row_id": row_id,
            "row_status": status,
        },
    )


async def _process_one(
    batch_id: UUID,
    worker_id: str,
    row_id: int,
    client: HospitalDirectoryClient,
    semaphore: asyncio.Semaphore,
    lease_lost: asyncio.Event | None,
) -> None:
    """
    Process one hospital with bounded concurrency.

    Expected upstream errors are persisted as row outcomes.
    Unexpected application or database failures propagate,
    leaving unresolved work available for future reconciliation.
    """
    async with semaphore:
        if lease_lost is not None and lease_lost.is_set():
            raise WorkerOwnershipError("Worker lease was lost before row processing.")

        item = await _mark_creating(
            batch_id,
            row_id,
            worker_id,
        )

        if lease_lost is not None and lease_lost.is_set():
            raise WorkerOwnershipError(
                "Worker lease was lost before the external POST."
            )

        try:
            hospital = await client.create_hospital(
                batch_id=batch_id,
                name=item.name,
                address=item.address,
                phone=item.phone,
            )

        except UpstreamRejected as exc:
            await _save_result(
                batch_id,
                worker_id,
                item.id,
                "failed",
                error_code=f"upstream_http_{exc.status_code}",
            )

        except UpstreamOutcomeUnknown:
            await _save_result(
                batch_id,
                worker_id,
                item.id,
                "unknown",
                error_code="upstream_outcome_unknown",
            )

        else:
            await _save_result(
                batch_id,
                worker_id,
                item.id,
                "created",
                upstream_id=hospital.id,
            )


async def _finish_creation_phase(
    batch_id: UUID,
    worker_id: str,
) -> str:
    """
    Aggregate row outcomes and commit the next batch state.

    'activating' means creation is complete and the batch
    is ready to enter activation processing. It does not
    mean the upstream activation PATCH has been sent.
    """
    async with SessionFactory.begin() as session:
        batch = await _check_owner(
            session,
            batch_id,
            worker_id,
        )

        result = await session.scalars(
            select(BatchHospital.status).where(BatchHospital.batch_id == batch_id)
        )
        statuses = list(result.all())

        if len(statuses) != batch.total_hospitals or any(
            status in {"unknown", "creating", "pending"} for status in statuses
        ):
            next_status = "needs_reconciliation"
        elif "failed" in statuses:
            next_status = "failed"
        elif all(status == "created" for status in statuses):
            next_status = "activating"
        else:
            next_status = "needs_reconciliation"

        batch.status = next_status
        batch.lease_owner = None
        batch.lease_expires_at = None

        if next_status == "failed":
            batch.last_error = "hospital_creation_failed"
        elif next_status == "needs_reconciliation":
            batch.last_error = "hospital_outcome_requires_review"
        else:
            batch.last_error = None

    logger.info(
        "batch.creation_phase.finished",
        extra={
            "batch_id": str(batch_id),
            "batch_status": next_status,
        },
    )

    return next_status


async def process_claimed_batch(
    batch_id: UUID,
    worker_id: str,
    client: HospitalDirectoryClient,
    *,
    max_concurrency: int = 4,
    lease_lost: asyncio.Event | None = None,
) -> str:
    """
    Process every pending row of one already claimed batch.

    A shared semaphore limits concurrent HTTP operations.
    Each row task opens its own short-lived database sessions.

    Args:
        batch_id: UUID of the claimed batch.
        worker_id: Worker that currently owns the batch.
        client: Injected Hospital Directory adapter.
        max_concurrency: Maximum simultaneous row operations.

    Returns:
        Final creation-phase batch status.

    Note:
        This stage deliberately does not call activate_batch().
    """
    if not 1 <= max_concurrency <= 4:
        raise ValueError("Concurrency must be between 1 and 4.")

    row_ids = await _load_pending_ids(
        batch_id,
        worker_id,
    )
    semaphore = asyncio.Semaphore(max_concurrency)

    # TaskGroup propagates unexpected failures and cancels
    # remaining tasks. In-flight external outcomes may then
    # require later reconciliation.
    async with asyncio.TaskGroup() as tasks:
        for row_id in row_ids:
            tasks.create_task(
                _process_one(
                    batch_id,
                    worker_id,
                    row_id,
                    client,
                    semaphore,
                    lease_lost,
                )
            )

    return await _finish_creation_phase(
        batch_id,
        worker_id,
    )
