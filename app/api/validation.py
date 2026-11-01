"""
HTTP endpoint for validating hospital CSV uploads.

Validation does not create hospital records or database batches.
"""

import logging
from typing import Annotated

from fastapi import APIRouter, File, UploadFile

from app.api.upload_utils import read_hospital_csv_upload
from app.schemas.csv_validation import CSVValidationReport
from app.services.csv_validator import validate_hospital_csv

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/hospitals",
    tags=["Hospital CSV Validation"],
)


@router.post(
    "/validate",
    response_model=CSVValidationReport,
    summary="Validate hospital CSV before bulk processing",
)
async def validate_csv_upload(
    file: Annotated[
        UploadFile,
        File(description="CSV containing hospital records."),
    ],
) -> CSVValidationReport:
    """
    Validate an uploaded CSV without creating any records.

    Returns HTTP 200 with valid=False when the CSV structure
    is readable but violates one or more business rules.
    """
    data = await read_hospital_csv_upload(file)
    result = validate_hospital_csv(data)

    logger.info(
        "csv.validation.completed",
        extra={
            "total_hospitals": result.report.total_hospitals,
            "valid_hospitals": result.report.valid_hospitals,
            "error_count": len(result.report.errors),
            "file_size_bytes": len(data),
        },
    )

    return result.report
