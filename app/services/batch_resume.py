import logging
from uuid import UUID

from sqlalchemy import select

from app.integrations.hospital_directory import (
    HospitalDirectoryClient,
    HospitalRecord,
)
from app.persistence.database import SessionFactory
from app.persistence.models import BatchHospital, BatchJob
from app.schemas.batch import ResumeBatchRequest

logger = logging.getLogger(__name__)

RETRYABLE_REJECTIONS = {
    "upstream_http_400",
    "upstream_http_422",
}


class BatchNotFoundError(Exception):
    """The requested batch does not exist."""


class BatchNotResumableError(Exception):
    """The batch cannot safely be returned to queued state."""


def _validate_local_state(
    batch: BatchJob,
    rows: list[BatchHospital],
    request: ResumeBatchRequest,
) -> list[BatchHospital]:
    """Validate exact corrections and safe retry eligibility."""
    if (
        batch.status != "failed"
        or batch.lease_owner is not None
        or batch.activation_requested_at is not None
    ):
        raise BatchNotResumableError("Batch is not an unowned, failed creation batch.")

    if len(rows) != batch.total_hospitals:
        raise BatchNotResumableError("Stored hospital count is inconsistent.")

    if any(row.status not in {"created", "failed"} for row in rows):
        raise BatchNotResumableError("Batch contains unresolved hospital operations.")

    failed = [row for row in rows if row.status == "failed"]

    corrections = {item.row: item for item in request.corrections}

    if not failed or {row.row_number for row in failed} != set(corrections):
        raise BatchNotResumableError(
            "Provide exactly one correction for every failed row."
        )

    for row in failed:
        if (
            row.last_error not in RETRYABLE_REJECTIONS
            or row.upstream_hospital_id is not None
        ):
            raise BatchNotResumableError(
                "A failed row has no safe, confirmed rejection."
            )

        corrected = corrections[row.row_number]

        if (
            row.name,
            row.address,
            row.phone,
        ) == (
            corrected.name,
            corrected.address,
            corrected.phone,
        ):
            raise BatchNotResumableError(
                "Failed row values must be corrected before retry."
            )

    # Avoid introducing duplicate business identities through
    # the explicit correction path.
    identities = [
        (
            corrections[row.row_number].name,
            corrections[row.row_number].address,
            corrections[row.row_number].phone,
        )
        if row.status == "failed"
        else (row.name, row.address, row.phone)
        for row in rows
    ]

    if len(set(identities)) != len(identities):
        raise BatchNotResumableError(
            "Corrected batch contains duplicate hospital identities."
        )

    return failed


def _verify_upstream(
    batch_id: UUID,
    rows: list[BatchHospital],
    remote_hospitals: list[HospitalRecord],
) -> None:
    """
    Require the upstream batch to contain precisely the locally
    confirmed hospitals, with no unexplained additional records.

    This is a conservative preflight check, not a distributed
    transaction or an absolute exactly-once guarantee.
    """
    created = [row for row in rows if row.status == "created"]

    if len(remote_hospitals) != len(created):
        raise BatchNotResumableError(
            "Upstream hospital count differs from confirmed rows."
        )

    remote_by_id = {hospital.id: hospital for hospital in remote_hospitals}

    if len(remote_by_id) != len(remote_hospitals):
        raise BatchNotResumableError("Duplicate upstream hospital IDs were detected.")

    for row in created:
        if row.upstream_hospital_id is None:
            raise BatchNotResumableError("A created row is missing its upstream ID.")

        remote = remote_by_id.get(row.upstream_hospital_id)

        if (
            remote is None
            or remote.creation_batch_id != batch_id
            or remote.active is not False
            or remote.name != row.name
            or remote.address != row.address
            or remote.phone != row.phone
        ):
            raise BatchNotResumableError(
                "An existing hospital failed upstream verification."
            )


class BatchResumeService:
    """Requeue explicit corrections without replaying successful work."""

    async def resume_failed_batch(
        self,
        batch_id: UUID,
        request: ResumeBatchRequest,
        client: HospitalDirectoryClient,
    ) -> int:
        """
        Atomically requeue corrected, definitively failed rows.

        Returns:
            Number of rows requeued.

        Raises:
            BatchNotFoundError:
                The batch does not exist.
            BatchNotResumableError:
                State or upstream verification prevents safe resume.
            UpstreamReadFailure:
                The external batch could not be verified.
        """
        # Fail fast before making the external GET request.
        async with SessionFactory() as session:
            batch = await session.get(BatchJob, batch_id)

            if batch is None:
                raise BatchNotFoundError("Batch not found.")

            rows = list(
                (
                    await session.scalars(
                        select(BatchHospital)
                        .where(BatchHospital.batch_id == batch_id)
                        .order_by(BatchHospital.row_number)
                    )
                ).all()
            )

            _validate_local_state(batch, rows, request)

        # Read-only external request, outside the transaction.
        remote_hospitals = await client.get_batch(batch_id)

        corrections = {item.row: item for item in request.corrections}

        async with SessionFactory.begin() as session:
            batch = await session.scalar(
                select(BatchJob).where(BatchJob.id == batch_id).with_for_update()
            )

            if batch is None:
                raise BatchNotFoundError("Batch not found.")

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

            # Recheck after the network call to catch changes.
            failed_rows = _validate_local_state(
                batch,
                rows,
                request,
            )
            _verify_upstream(
                batch_id,
                rows,
                remote_hospitals,
            )

            for row in failed_rows:
                correction = corrections[row.row_number]

                row.name = correction.name
                row.address = correction.address
                row.phone = correction.phone
                row.status = "pending"
                row.last_error = None

                # Preserve attempt_count and last_attempted_at.
                # Those fields are historical operational evidence.

            batch.status = "queued"
            batch.last_error = None
            batch.lease_owner = None
            batch.lease_expires_at = None

            requeued_count = len(failed_rows)

        # Log after successful commit, without raw hospital data.
        logger.info(
            "batch.resume.queued requeued_hospitals=%s",
            requeued_count,
            extra={"batch_id": str(batch_id)},
        )

        return requeued_count
