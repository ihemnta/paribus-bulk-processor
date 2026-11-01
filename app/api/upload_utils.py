"""
Shared HTTP upload utilities for hospital CSV endpoints.

This module handles transport-level concerns only:
filename validation, bounded file reads and resource cleanup.

CSV business validation belongs to the csv_validator service.
"""

from fastapi import HTTPException, UploadFile

from app.core.config import get_settings


async def read_hospital_csv_upload(file: UploadFile) -> bytes:
    """
    Read a CSV upload without exceeding the configured byte limit.

    Args:
        file: Uploaded multipart file from FastAPI.

    Returns:
        Raw CSV bytes suitable for the CSV validation service.

    Raises:
        HTTPException: 400 for unsupported filename extensions.
        HTTPException: 413 for files exceeding the size limit.

    The upload resource is closed even if validation fails.
    """
    settings = get_settings()

    try:
        filename = file.filename or ""

        if not filename.lower().endswith(".csv"):
            raise HTTPException(
                status_code=400,
                detail="Please upload a .csv file.",
            )

        data = await file.read(settings.max_csv_bytes + 1)

        if len(data) > settings.max_csv_bytes:
            raise HTTPException(
                status_code=413,
                detail="CSV file exceeds the configured size limit.",
            )

        return data

    finally:
        await file.close()
