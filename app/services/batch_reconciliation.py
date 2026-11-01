"""
Read-only reconciliation of uncertain hospital creation outcomes.

This service never creates or activates upstream hospitals.

It compares durable local records with an upstream batch snapshot.
Only uniquely matched records are recovered. Missing, duplicated,
or inconsistent records remain unresolved for investigation.
"""

import logging
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select

from app.integrations.hospital_directory import (
    HospitalDirectoryClient,
    HospitalRecord,
    UpstreamReadFailure,
)
from app.persistence.database import SessionFactory
from app.persistence.models import BatchHospital, BatchJob

logger = logging.getLogger(__name__)

UNCERTAIN_STATES = {"unknown", "creating"}


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Summary of a safe, read-only reconciliation operation."""

    batch_id: UUID
    outcome: str
    batch_status: str
    recovered_hospitals: int
    reason: str | None = None


def _same_hospital(
    local: BatchHospital,
    remote: HospitalRecord,
) -> bool:
    """Compare normalized hospital business fields exactly."""
    return (
        local.name == remote.name
        and local.address == remote.address
        and local.phone == remote.phone
    )


def _build_recovery_plan(
    batch_id: UUID,
    rows: list[BatchHospital],
    remote_hospitals: list[HospitalRecord],
) -> tuple[dict[int, int], str | None]:
    """
    Match uncertain rows to uniquely identifiable upstream records.

    Returns:
        Mapping from local hospital-row ID to upstream hospital ID,
        or a reason explaining why recovery cannot be confirmed.

    No database changes are made by this function.
    """
    remote_by_id = {hospital.id: hospital for hospital in remote_hospitals}

    if len(remote_by_id) != len(remote_hospitals):
        return {}, "duplicate_upstream_ids"

    # All upstream records must belong to this batch and remain
    # inactive. Activation is a separate orchestration phase.
    for remote in remote_hospitals:
        if remote.creation_batch_id != batch_id or remote.active is not False:
            return {}, "invalid_upstream_batch_state"

    available = dict(remote_by_id)

    # First verify every creation already confirmed locally.
    # These records must never be assigned to uncertain rows.
    for row in rows:
        if row.status != "created":
            continue

        if row.upstream_hospital_id is None:
            return {}, "confirmed_row_missing_upstream_id"

        remote = available.pop(row.upstream_hospital_id, None)

        if remote is None or not _same_hospital(row, remote):
            return {}, "confirmed_hospital_mismatch"

    plan: dict[int, int] = {}

    # Resolve only unknown/creating rows. A missing match does
    # NOT authorize another POST because the remote write could
    # still be in flight or not yet visible to the GET request.
    for row in rows:
        if row.status not in UNCERTAIN_STATES:
            continue

        candidates = [
            remote for remote in available.values() if _same_hospital(row, remote)
        ]

        if len(candidates) != 1:
            return {}, "missing_or_ambiguous_hospital"

        matched = candidates[0]

        plan[row.id] = matched.id
        del available[matched.id]

    # An unexplained extra remote record may represent a duplicate
    # or an out-of-band write. Do not silently ignore it.
    if available:
        return {}, "unexpected_upstream_hospitals"

    return plan, None


class BatchReconciliationService:
    """Reconcile uncertain creation results without new upstream writes."""

    async def reconcile_creation(
        self,
        batch_id: UUID,
        client: HospitalDirectoryClient,
    ) -> ReconciliationResult:
        """
        Reconcile one batch in needs_reconciliation status.

        The upstream GET happens outside a database transaction.
        Local state is rechecked under a row lock before updates.

        This function never retries POST or PATCH operations.
        """
        async with SessionFactory() as session:
            batch = await session.get(BatchJob, batch_id)

            if batch is None:
                raise ValueError("Batch not found.")

            if batch.status != "needs_reconciliation" or batch.lease_owner is not None:
                return ReconciliationResult(
                    batch_id=batch_id,
                    outcome="skipped",
                    batch_status=batch.status,
                    recovered_hospitals=0,
                    reason="batch_not_eligible",
                )

            result = await session.scalars(
                select(BatchHospital.status).where(BatchHospital.batch_id == batch_id)
            )

            if not any(status in UNCERTAIN_STATES for status in result.all()):
                return ReconciliationResult(
                    batch_id=batch_id,
                    outcome="skipped",
                    batch_status=batch.status,
                    recovered_hospitals=0,
                    reason="no_uncertain_creation_rows",
                )

        try:
            remote_hospitals = await client.get_batch(batch_id)
        except UpstreamReadFailure:
            logger.warning(
                "batch.reconciliation.read_failed",
                extra={"batch_id": str(batch_id)},
            )

            return ReconciliationResult(
                batch_id=batch_id,
                outcome="unresolved",
                batch_status="needs_reconciliation",
                recovered_hospitals=0,
                reason="upstream_read_failed",
            )

        # Begin the transaction only after the network read.
        async with SessionFactory.begin() as session:
            batch = await session.scalar(
                select(BatchJob).where(BatchJob.id == batch_id).with_for_update()
            )

            if (
                batch is None
                or batch.status != "needs_reconciliation"
                or batch.lease_owner is not None
            ):
                return ReconciliationResult(
                    batch_id=batch_id,
                    outcome="skipped",
                    batch_status=(batch.status if batch else "not_found"),
                    recovered_hospitals=0,
                    reason="batch_state_changed",
                )

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

            uncertain_rows = [row for row in rows if row.status in UNCERTAIN_STATES]

            if not uncertain_rows:
                return ReconciliationResult(
                    batch_id=batch_id,
                    outcome="skipped",
                    batch_status=batch.status,
                    recovered_hospitals=0,
                    reason="no_uncertain_creation_rows",
                )

            plan, error = _build_recovery_plan(
                batch_id,
                rows,
                remote_hospitals,
            )

            if error is not None:
                batch.last_error = error

                outcome = ReconciliationResult(
                    batch_id=batch_id,
                    outcome="unresolved",
                    batch_status=batch.status,
                    recovered_hospitals=0,
                    reason=error,
                )

            else:
                for row in uncertain_rows:
                    row.upstream_hospital_id = plan[row.id]
                    row.status = "created"
                    row.last_error = None

                if any(row.status == "failed" for row in rows):
                    batch.status = "failed"
                    batch.last_error = "known_creation_failure"

                elif all(row.status == "created" for row in rows):
                    batch.status = "activating"
                    batch.last_error = None

                else:
                    batch.status = "needs_reconciliation"
                    batch.last_error = "pending_rows_require_recovery"

                outcome = ReconciliationResult(
                    batch_id=batch_id,
                    outcome="reconciled",
                    batch_status=batch.status,
                    recovered_hospitals=len(plan),
                )

        # Log only after the database transaction finishes.
        logger.info(
            "batch.reconciliation.finished outcome=%s recovered=%s",
            outcome.outcome,
            outcome.recovered_hospitals,
            extra={
                "batch_id": str(batch_id),
                "batch_status": outcome.batch_status,
            },
        )

        return outcome
