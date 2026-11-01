"""
Recover PostgreSQL batch jobs whose worker leases expired.

Recovery is conservative:
- Never send external POST or PATCH requests.
- Preserve all confirmed hospital IDs.
- Convert abandoned 'creating' rows to 'unknown'.
- Requeue only work that has not begun external creation.
- Require reconciliation when an external outcome is uncertain.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from app.persistence.database import SessionFactory
from app.persistence.models import BatchHospital, BatchJob

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    """Durable state transition for one expired worker lease."""

    batch_id: UUID
    previous_status: str
    new_status: str
    uncertain_rows: int


async def recover_expired_batches(
    *,
    limit: int = 20,
) -> list[RecoveryDecision]:
    """
    Recover at most 'limit' abandoned batch jobs.

    Uses PostgreSQL row locks to coordinate multiple recovery
    processes. A healthy worker that has renewed its lease
    will not be selected.

    No external HTTP operations are performed.
    """
    if not 1 <= limit <= 100:
        raise ValueError("Recovery limit must be 1 to 100.")

    now = datetime.now(UTC)
    decisions: list[RecoveryDecision] = []

    async with SessionFactory.begin() as session:
        result = await session.scalars(
            select(BatchJob)
            .where(
                BatchJob.status.in_(("processing", "activating")),
                BatchJob.lease_owner.is_not(None),
                BatchJob.lease_expires_at.is_not(None),
                BatchJob.lease_expires_at <= now,
            )
            .order_by(BatchJob.lease_expires_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        batches = list(result.all())

        for batch in batches:
            previous_status = batch.status

            rows = list(
                (
                    await session.scalars(
                        select(BatchHospital)
                        .where(BatchHospital.batch_id == batch.id)
                        .order_by(BatchHospital.row_number)
                        .with_for_update()
                    )
                ).all()
            )

            valid_count = len(rows) == batch.total_hospitals
            uncertain_count = 0

            if previous_status == "processing":
                # 'creating' means a POST may have been sent.
                # Even when the worker crashed before the POST,
                # it is safer to require reconciliation.
                for row in rows:
                    if row.status == "creating":
                        row.status = "unknown"
                        row.last_error = "worker_interrupted"

                uncertain_count = sum(row.status == "unknown" for row in rows)

                known_states = all(
                    row.status in {"pending", "created", "failed"} for row in rows
                )

                if (
                    not valid_count
                    or uncertain_count > 0
                    or not known_states
                    or batch.activation_requested_at is not None
                ):
                    batch.status = "needs_reconciliation"
                    batch.last_error = "abandoned_creation"

                elif any(row.status == "pending" for row in rows):
                    # Pending rows have no committed external
                    # creation intent. The worker can safely
                    # pick up those rows on the next run.
                    batch.status = "queued"
                    batch.last_error = None

                elif any(row.status == "failed" for row in rows):
                    batch.status = "failed"
                    batch.last_error = "hospital_creation_failed"

                elif all(row.status == "created" for row in rows):
                    batch.status = "activating"
                    batch.last_error = None

                else:
                    batch.status = "needs_reconciliation"
                    batch.last_error = "inconsistent_creation_state"

            else:
                # The activation phase might already have sent
                # PATCH. Never repeat it solely because a lease
                # expired.
                ready_to_activate = valid_count and all(
                    row.status == "created" for row in rows
                )

                if batch.activation_requested_at is None and ready_to_activate:
                    # There is no committed PATCH intent.
                    # Allow the activation service to claim it.
                    batch.status = "activating"
                    batch.last_error = None

                else:
                    batch.status = "needs_reconciliation"
                    batch.last_error = "abandoned_activation"

            batch.lease_owner = None
            batch.lease_expires_at = None

            decisions.append(
                RecoveryDecision(
                    batch_id=batch.id,
                    previous_status=previous_status,
                    new_status=batch.status,
                    uncertain_rows=uncertain_count,
                )
            )

    # This logging occurs after the transaction commits.
    for decision in decisions:
        logger.info(
            "worker.batch.recovered from=%s to=%s uncertain=%s",
            decision.previous_status,
            decision.new_status,
            decision.uncertain_rows,
            extra={
                "batch_id": str(decision.batch_id),
                "batch_status": decision.new_status,
            },
        )

    return decisions
