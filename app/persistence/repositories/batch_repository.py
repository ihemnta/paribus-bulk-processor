"""
Database operations for hospital batch jobs.

The repository encapsulates SQLAlchemy persistence details.
It does not decide transaction boundaries, perform CSV parsing,
or call the external Hospital Directory API.
"""

from collections.abc import Sequence
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.persistence.models import BatchHospital, BatchJob
from app.services.csv_validator import HospitalCSVRow


class BatchRepository:
    """Provide database operations for batches and their hospital rows."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository with a caller-owned session."""
        self.session = session

    async def create_queued(
        self,
        hospitals: Sequence[HospitalCSVRow],
    ) -> BatchJob:
        """
        Stage one queued batch and all hospital rows for insertion.

        The caller must open and control the transaction.

        A flush forces PostgreSQL to execute the INSERT statements
        and check database constraints, but does not commit them.

        Args:
            hospitals: Previously validated and normalized CSV rows.

        Returns:
            Newly inserted ORM batch object.

        Raises:
            SQLAlchemyError: If database insertion fails.
        """
        batch = BatchJob(
            id=uuid4(),
            status="queued",
            total_hospitals=len(hospitals),
        )

        batch.hospitals = [
            BatchHospital(
                row_number=hospital.row,
                name=hospital.name,
                address=hospital.address,
                phone=hospital.phone,
                status="pending",
                attempt_count=0,
            )
            for hospital in hospitals
        ]

        self.session.add(batch)

        # Execute pending INSERT statements without committing.
        await self.session.flush()

        return batch

    async def get_by_id(self, batch_id: UUID) -> BatchJob | None:
        """
        Fetch one batch and its hospital rows.

        selectinload explicitly fetches related rows while the
        asynchronous session is active, avoiding implicit lazy
        database IO during response serialization.
        """
        statement = (
            select(BatchJob)
            .options(selectinload(BatchJob.hospitals))
            .where(BatchJob.id == batch_id)
        )

        result = await self.session.execute(statement)

        return result.scalar_one_or_none()
