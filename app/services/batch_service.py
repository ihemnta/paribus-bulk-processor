"""
Business operations for durable hospital batch processing.

This service owns the transaction boundary for batch creation.
It converts ORM records into public response schemas and does
not directly perform external Hospital Directory API requests.
"""

import logging
from collections.abc import Sequence
from uuid import UUID

from app.persistence.database import SessionFactory
from app.persistence.repositories.batch_repository import BatchRepository
from app.schemas.batch import BatchProgressResponse, HospitalProgress
from app.services.csv_validator import HospitalCSVRow

logger = logging.getLogger(__name__)


class BatchService:
    """Coordinate batch persistence and retrieval."""

    async def create_queued_batch(
        self,
        hospitals: Sequence[HospitalCSVRow],
    ) -> UUID:
        """
        Atomically create one batch and all its hospital records.

        On success, the batch is durable and eligible for later
        background processing.

        If any database operation fails, the transaction is rolled
        back and no partially initialized batch is committed.
        """
        if not 1 <= len(hospitals) <= 20:
            raise ValueError("A batch must contain between 1 and 20 hospitals.")

        # The context manager creates a session and a transaction.
        # Normal exit commits; an exception causes rollback.
        async with SessionFactory.begin() as session:
            repository = BatchRepository(session)
            batch = await repository.create_queued(hospitals)
            batch_id = batch.id

        # Log success only AFTER the transaction commits.
        logger.info(
            "batch.queued",
            extra={
                "batch_id": str(batch_id),
                "total_hospitals": len(hospitals),
            },
        )

        return batch_id

    async def get_batch_progress(
        self,
        batch_id: UUID,
    ) -> BatchProgressResponse | None:
        """
        Retrieve the persisted processing state of one batch.

        Row statuses are aggregated into progress counters.
        Missing batches return None; the API maps that to HTTP 404.
        """
        async with SessionFactory() as session:
            repository = BatchRepository(session)
            batch = await repository.get_by_id(batch_id)

            if batch is None:
                return None

            rows = sorted(
                batch.hospitals,
                key=lambda hospital: hospital.row_number,
            )

            processed = sum(row.status in {"created", "activated"} for row in rows)
            failed = sum(row.status == "failed" for row in rows)
            unknown = sum(row.status == "unknown" for row in rows)
            pending = sum(row.status == "pending" for row in rows)
            processing = sum(row.status == "creating" for row in rows)

            return BatchProgressResponse(
                batch_id=batch.id,
                status=batch.status,
                total_hospitals=batch.total_hospitals,
                processed_hospitals=processed,
                failed_hospitals=failed,
                unknown_hospitals=unknown,
                pending_hospitals=pending,
                processing_hospitals=processing,
                batch_activated=(batch.activation_confirmed_at is not None),
                created_at=batch.created_at,
                completed_at=batch.completed_at,
                hospitals=[
                    HospitalProgress(
                        row=row.row_number,
                        name=row.name,
                        hospital_id=row.upstream_hospital_id,
                        status=row.status,
                    )
                    for row in rows
                ],
            )
