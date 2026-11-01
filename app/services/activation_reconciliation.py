"""
Read-only recovery for an interrupted batch activation.

No upstream POST or PATCH is performed.

A batch is marked completed only when every expected
hospital is confirmed active in the upstream directory.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from app.integrations.hospital_directory import (
    HospitalDirectoryClient,
    UpstreamReadFailure,
)
from app.persistence.database import SessionFactory
from app.persistence.models import BatchHospital, BatchJob
from app.services.batch_activation import (
    ExpectedHospital,
    hospitals_match,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ActivationRecoveryResult:
    """Outcome of read-only activation reconciliation."""

    batch_id: UUID
    outcome: str
    reason: str | None = None


def _eligible(
    batch: BatchJob,
    rows: list[BatchHospital],
) -> bool:
    """Check whether an interrupted activation may be verified."""
    return (
        batch.status == "needs_reconciliation"
        and batch.lease_owner is None
        and batch.activation_requested_at is not None
        and len(rows) == batch.total_hospitals
        and all(
            row.status == "created" and row.upstream_hospital_id is not None
            for row in rows
        )
    )


def _expected(
    rows: list[BatchHospital],
) -> list[ExpectedHospital]:
    """Build expected hospital identities from local records."""
    return [
        ExpectedHospital(
            id=row.upstream_hospital_id,
            name=row.name,
            address=row.address,
            phone=row.phone,
        )
        for row in rows
    ]


class ActivationReconciliationService:
    """Confirm prior activation without repeating the PATCH."""

    async def reconcile(
        self,
        batch_id: UUID,
        client: HospitalDirectoryClient,
    ) -> ActivationRecoveryResult:
        """
        Recover an activation whose outcome was not persisted.

        Returns:
            completed: All expected hospitals are confirmed active.
            unresolved: The upstream result is not confirmed.
            skipped: The batch is not eligible for this recovery.

        Database state is rechecked under a lock before
        committing the completion transition.
        """
        async with SessionFactory() as session:
            batch = await session.get(BatchJob, batch_id)

            if batch is None:
                raise ValueError("Batch not found.")

            rows = list(
                (
                    await session.scalars(
                        select(BatchHospital)
                        .where(BatchHospital.batch_id == batch_id)
                        .order_by(BatchHospital.row_number)
                    )
                ).all()
            )

            if not _eligible(batch, rows):
                return ActivationRecoveryResult(
                    batch_id,
                    "skipped",
                    "batch_not_eligible",
                )

            expected = _expected(rows)

        # External read happens outside any write transaction.
        try:
            remote = await client.get_batch(batch_id)
        except UpstreamReadFailure:
            return ActivationRecoveryResult(
                batch_id,
                "unresolved",
                "upstream_read_failed",
            )

        if not hospitals_match(batch_id, expected, remote):
            return ActivationRecoveryResult(
                batch_id,
                "unresolved",
                "upstream_batch_mismatch",
            )

        # Inactive, partially active, or unspecified states
        # do not establish that activation completed.
        if not all(hospital.active is True for hospital in remote):
            return ActivationRecoveryResult(
                batch_id,
                "unresolved",
                "activation_not_confirmed",
            )

        # Lock and revalidate the local state before modifying it.
        async with SessionFactory.begin() as session:
            batch = await session.scalar(
                select(BatchJob).where(BatchJob.id == batch_id).with_for_update()
            )

            if batch is None:
                raise ValueError("Batch not found.")

            rows = list(
                (
                    await session.scalars(
                        select(BatchHospital)
                        .where(BatchHospital.batch_id == batch_id)
                        .order_by(BatchHospital.row_number)
                        .with_for_update()
                    )
                ).all()
            )

            if not _eligible(batch, rows) or not hospitals_match(
                batch_id,
                _expected(rows),
                remote,
            ):
                return ActivationRecoveryResult(
                    batch_id,
                    "skipped",
                    "local_state_changed",
                )

            now = datetime.now(UTC)

            for row in rows:
                row.status = "activated"

            batch.status = "completed"
            batch.activation_confirmed_at = now
            batch.completed_at = now
            batch.last_error = None
            batch.lease_owner = None
            batch.lease_expires_at = None

        logger.info(
            "batch.activation.reconciled",
            extra={
                "batch_id": str(batch_id),
                "batch_status": "completed",
            },
        )

        return ActivationRecoveryResult(
            batch_id,
            "completed",
        )
