"""
HTTP API for hospital bulk-processing batches.

This milestone persists validated CSV rows as a queued job.
Background execution and upstream activation are implemented
in later milestones.
"""

import logging
from collections.abc import AsyncIterator
from secrets import compare_digest
from typing import Annotated
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    File,
    Header,
    HTTPException,
    Response,
    UploadFile,
)
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.api.upload_utils import read_hospital_csv_upload
from app.core.config import get_settings
from app.integrations.hospital_directory import (
    HospitalDirectoryClient,
    UpstreamReadFailure,
    build_http_client,
)
from app.persistence.database import SessionFactory
from app.persistence.models import BatchHospital, BatchJob
from app.schemas.batch import (
    BatchAcceptedResponse,
    BatchProgressResponse,
    ReconcileBatchResponse,
    ResumeBatchRequest,
    ResumeBatchResponse,
)
from app.services.activation_reconciliation import (
    ActivationReconciliationService,
)
from app.services.batch_reconciliation import (
    BatchReconciliationService,
)
from app.services.batch_resume import (
    BatchNotFoundError,
    BatchNotResumableError,
    BatchResumeService,
)
from app.services.batch_service import BatchService
from app.services.csv_validator import validate_hospital_csv

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/hospitals",
    tags=["Hospital Bulk Processing"],
)

batch_service = BatchService()


@router.post(
    "/bulk",
    status_code=202,
    response_model=BatchAcceptedResponse,
    summary="Accept and queue a hospital bulk upload",
)
async def create_bulk_batch(
    file: Annotated[
        UploadFile,
        File(description="CSV containing 1 to 20 hospitals."),
    ],
    response: Response,
) -> BatchAcceptedResponse:
    """
    Validate and atomically persist an uploaded hospital batch.

    Processing is not performed inline. A later worker will
    consume durable queued batches from PostgreSQL.

    Returns:
        HTTP 202 with a batch identifier and status URL.

    Raises:
        HTTPException 422: CSV fails business validation.
        HTTPException 503: Database persistence fails.
    """
    data = await read_hospital_csv_upload(file)
    result = validate_hospital_csv(data)

    if not result.report.valid:
        logger.warning(
            "batch.upload.validation_failed",
            extra={
                "total_hospitals": result.report.total_hospitals,
                "error_count": len(result.report.errors),
            },
        )

        raise HTTPException(
            status_code=422,
            detail=result.report.model_dump(),
        )

    try:
        batch_id = await batch_service.create_queued_batch(result.hospitals)
    except SQLAlchemyError as exc:
        # Never expose SQL statement parameters or potentially
        # sensitive hospital values through database error logs.
        logger.error(
            "batch.persistence.failed error_type=%s",
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=503,
            detail="Unable to save the batch. Please retry later.",
        ) from None

    status_url = f"/hospitals/bulk/{batch_id}"

    response.headers["Location"] = status_url

    return BatchAcceptedResponse(
        batch_id=batch_id,
        status="queued",
        total_hospitals=result.report.total_hospitals,
        processed_hospitals=0,
        failed_hospitals=0,
        batch_activated=False,
        status_url=status_url,
    )


@router.get(
    "/bulk/{batch_id}",
    response_model=BatchProgressResponse,
    summary="Get hospital batch processing status",
)
async def get_bulk_batch(
    batch_id: UUID,
) -> BatchProgressResponse:
    """
    Retrieve the saved batch and its individual row statuses.

    Returns:
        HTTP 200 with the durable batch state.

    Raises:
        HTTPException 404: Batch does not exist.
        HTTPException 503: Database query fails.
    """
    try:
        progress = await batch_service.get_batch_progress(batch_id)
    except SQLAlchemyError as exc:
        logger.error(
            "batch.read.failed error_type=%s",
            type(exc).__name__,
            extra={"batch_id": str(batch_id)},
        )
        raise HTTPException(
            status_code=503,
            detail="Unable to retrieve batch status.",
        ) from None

    if progress is None:
        raise HTTPException(
            status_code=404,
            detail="Batch not found.",
        )

    return progress


@router.post(
    "/bulk/{batch_id}/resume",
    status_code=202,
    response_model=ResumeBatchResponse,
    summary="Correct and resume a definitively failed batch",
)
async def resume_bulk_batch(
    batch_id: UUID,
    request: ResumeBatchRequest,
    x_admin_key: Annotated[str | None, Header()] = None,
) -> ResumeBatchResponse:
    """
    Validate admin access, verify upstream records, and requeue
    corrected failed rows.

    This endpoint does not perform hospital creation itself.
    It only makes eligible rows available to the background worker.

    Returns:
        HTTP 202 when the corrected batch is durably queued.

    Raises:
        HTTP 403 for invalid admin credentials.
        HTTP 404 when the batch is missing.
        HTTP 409 when the batch cannot safely be resumed.
        HTTP 503 when dependencies are unavailable.
    """
    expected_key = get_settings().admin_api_key

    if not expected_key:
        raise HTTPException(
            status_code=503,
            detail="Administrative resume is not configured.",
        )

    if x_admin_key is None or not compare_digest(x_admin_key, expected_key):
        raise HTTPException(
            status_code=403,
            detail="Invalid administrative credentials.",
        )

    try:
        async with build_http_client() as http_client:
            client = HospitalDirectoryClient(http_client)

            requeued = await BatchResumeService().resume_failed_batch(
                batch_id,
                request,
                client,
            )

    except BatchNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="Batch not found.",
        ) from exc

    except BatchNotResumableError as exc:
        logger.warning(
            "batch.resume.rejected reason=%s",
            str(exc),
            extra={"batch_id": str(batch_id)},
        )

        raise HTTPException(
            status_code=409,
            detail=str(exc),
        ) from exc

    except UpstreamReadFailure as exc:
        raise HTTPException(
            status_code=503,
            detail="Upstream batch verification is unavailable.",
        ) from exc

    except SQLAlchemyError:
        logger.error(
            "batch.resume.database_failed",
            extra={"batch_id": str(batch_id)},
        )

        raise HTTPException(
            status_code=503,
            detail="Database operation failed.",
        ) from None

    return ResumeBatchResponse(
        batch_id=batch_id,
        status="queued",
        requeued_hospitals=requeued,
        status_url=f"/hospitals/bulk/{batch_id}",
    )


def require_reconciliation_admin(
    x_admin_key: Annotated[str | None, Header()] = None,
) -> None:
    """
    Require the configured administrative API key.

    The same secret is already used by the manual resume
    endpoint. Never include its value in logs or responses.
    """
    expected_key = get_settings().admin_api_key

    if not expected_key:
        raise HTTPException(
            status_code=503,
            detail="Administrative access is not configured.",
        )

    if x_admin_key is None or not compare_digest(x_admin_key, expected_key):
        raise HTTPException(
            status_code=403,
            detail="Invalid administrative credentials.",
        )


async def get_reconciliation_client() -> AsyncIterator[HospitalDirectoryClient]:
    """
    Provide an upstream client for one reconciliation request.

    FastAPI manages cleanup after the request. Tests can
    override this dependency with an HTTPX MockTransport.
    """
    async with build_http_client() as http_client:
        yield HospitalDirectoryClient(http_client)


@router.post(
    "/bulk/{batch_id}/reconcile",
    response_model=ReconcileBatchResponse,
    summary="Safely reconcile an uncertain hospital batch",
)
async def reconcile_bulk_batch(
    batch_id: UUID,
    _admin: Annotated[None, Depends(require_reconciliation_admin)],
    client: Annotated[
        HospitalDirectoryClient,
        Depends(get_reconciliation_client),
    ],
) -> ReconcileBatchResponse:
    """
    Perform an authenticated, read-only upstream reconciliation.

    Creation reconciliation resolves unknown hospital writes.
    Activation reconciliation verifies a previous PATCH result.

    This endpoint never sends an upstream POST or PATCH.
    """
    try:
        async with SessionFactory() as session:
            batch = await session.get(BatchJob, batch_id)

            if batch is None:
                raise HTTPException(
                    status_code=404,
                    detail="Batch not found.",
                )

            if batch.status != "needs_reconciliation" or batch.lease_owner is not None:
                raise HTTPException(
                    status_code=409,
                    detail="Batch is not eligible for reconciliation.",
                )

            statuses = list(
                (
                    await session.scalars(
                        select(BatchHospital.status).where(
                            BatchHospital.batch_id == batch_id
                        )
                    )
                ).all()
            )

            activation_requested = batch.activation_requested_at is not None

        uncertain_creation = any(
            status in {"unknown", "creating"} for status in statuses
        )

        if activation_requested:
            if uncertain_creation or not all(
                status == "created" for status in statuses
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Inconsistent activation recovery state.",
                )

            result = await ActivationReconciliationService().reconcile(
                batch_id,
                client,
            )

            return ReconcileBatchResponse(
                batch_id=batch_id,
                phase="activation",
                outcome=result.outcome,
                batch_status=(
                    "completed"
                    if result.outcome == "completed"
                    else "needs_reconciliation"
                ),
                reason=result.reason,
            )

        if uncertain_creation:
            result = await BatchReconciliationService().reconcile_creation(
                batch_id,
                client,
            )

            return ReconcileBatchResponse(
                batch_id=batch_id,
                phase="creation",
                outcome=result.outcome,
                batch_status=result.batch_status,
                recovered_hospitals=result.recovered_hospitals,
                reason=result.reason,
            )

        raise HTTPException(
            status_code=409,
            detail="Batch requires manual review.",
        )

    except SQLAlchemyError:
        logger.exception(
            "batch.reconciliation.database_error",
            extra={"batch_id": str(batch_id)},
        )

        raise HTTPException(
            status_code=503,
            detail="Database is temporarily unavailable.",
        ) from None
