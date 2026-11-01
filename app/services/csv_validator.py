"""
Pure CSV parsing and validation for hospital uploads.

This module performs no HTTP requests or database operations.
The returned normalized hospital rows will be reused by the
bulk-processing workflow in later milestones.
"""

import csv
import io
import re
from dataclasses import dataclass

from app.schemas.csv_validation import (
    CSVValidationReport,
    ValidationIssue,
)

MAX_HOSPITALS_PER_BATCH = 20

REQUIRED_COLUMNS = {"name", "address"}
OPTIONAL_COLUMNS = {"phone"}
ALLOWED_COLUMNS = REQUIRED_COLUMNS | OPTIONAL_COLUMNS

# Application-level policy: optional phone numbers must contain
# exactly ten ASCII digits when supplied.
PHONE_PATTERN = re.compile(r"[0-9]{10}")


@dataclass(frozen=True, slots=True)
class HospitalCSVRow:
    """Normalized hospital data from one CSV record."""

    row: int
    name: str
    address: str
    phone: str | None


@dataclass(frozen=True, slots=True)
class CSVParseResult:
    """Internal result containing the report and normalized rows."""

    report: CSVValidationReport
    hospitals: list[HospitalCSVRow]


def _file_error(message: str) -> CSVParseResult:
    """Return a file-level validation failure."""

    return CSVParseResult(
        report=CSVValidationReport(
            valid=False,
            total_hospitals=0,
            valid_hospitals=0,
            errors=[
                ValidationIssue(
                    row=None,
                    field="file",
                    message=message,
                )
            ],
        ),
        hospitals=[],
    )


def validate_hospital_csv(data: bytes) -> CSVParseResult:
    """
    Parse and validate a hospital CSV file.

    Rules:
        - File must be UTF-8 encoded (UTF-8 BOM is supported).
        - Header must include name and address.
        - Phone is optional.
        - Unknown and duplicate columns are rejected.
        - Hospital name and address must not be blank.
        - Between 1 and 20 hospital records are permitted.
        - Completely blank records are ignored.

    Args:
        data: Raw CSV file bytes.

    Returns:
        CSVParseResult containing:
            - Public validation report.
            - Normalized hospital rows for later processing.

    Notes:
        Row numbers are 1-based data-record indexes.
        The CSV header is not counted as a hospital record.
        No hospital is created by this function.
    """
    if not data:
        return _file_error("CSV file is empty.")

    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return _file_error("CSV must be UTF-8 encoded.")

    if not text.strip():
        return _file_error("CSV file is empty.")

    if "\x00" in text:
        return _file_error("CSV contains an invalid NUL character.")

    reader = csv.reader(
        io.StringIO(text, newline=""),
        strict=True,
    )

    try:
        raw_header = next(reader, None)
    except csv.Error:
        return _file_error("CSV header is malformed.")

    if not raw_header:
        return _file_error("CSV header is missing.")

    # Normalize surrounding whitespace, not column-name casing.
    header = [column.strip() for column in raw_header]

    if len(header) != len(set(header)):
        return _file_error("CSV contains duplicate column names.")

    missing = REQUIRED_COLUMNS - set(header)
    unexpected = set(header) - ALLOWED_COLUMNS

    if missing:
        return _file_error("Missing required columns: " + ", ".join(sorted(missing)))

    if unexpected:
        return _file_error("Unexpected columns: " + ", ".join(sorted(unexpected)))

    hospitals: list[HospitalCSVRow] = []
    errors: list[ValidationIssue] = []
    total_hospitals = 0

    try:
        for raw_row in reader:
            # Ignore entirely blank records, including empty lines.
            if not raw_row or all(not value.strip() for value in raw_row):
                continue

            total_hospitals += 1
            row_number = total_hospitals

            if len(raw_row) != len(header):
                errors.append(
                    ValidationIssue(
                        row=row_number,
                        field="row",
                        message=(
                            f"Expected {len(header)} columns, received {len(raw_row)}."
                        ),
                    )
                )
                continue

            values = {column: value.strip() for column, value in zip(header, raw_row)}

            name = values["name"]
            address = values["address"]
            phone = values.get("phone") or None

            has_error = False

            if not name:
                errors.append(
                    ValidationIssue(
                        row=row_number,
                        field="name",
                        message="Hospital name is required.",
                    )
                )
                has_error = True

            if not address:
                errors.append(
                    ValidationIssue(
                        row=row_number,
                        field="address",
                        message="Hospital address is required.",
                    )
                )
                has_error = True

            # Phone is optional, but must have exactly 10 digits when present.
            # Use fullmatch to reject prefixes, spaces, letters and separators.
            if phone is not None and PHONE_PATTERN.fullmatch(phone) is None:
                errors.append(
                    ValidationIssue(
                        row=row_number,
                        field="phone",
                        message="Phone number must contain exactly 10 digits.",
                    )
                )
                has_error = True

            if has_error:
                continue

            hospitals.append(
                HospitalCSVRow(
                    row=row_number,
                    name=name,
                    address=address,
                    phone=phone,
                )
            )

    except csv.Error:
        errors.append(
            ValidationIssue(
                row=total_hospitals + 1,
                field="file",
                message="CSV contains malformed quoting or syntax.",
            )
        )

    if total_hospitals == 0:
        errors.append(
            ValidationIssue(
                row=None,
                field="file",
                message="CSV must contain at least one hospital.",
            )
        )

    if total_hospitals > MAX_HOSPITALS_PER_BATCH:
        errors.append(
            ValidationIssue(
                row=None,
                field="file",
                message=(
                    "A batch cannot contain more than "
                    f"{MAX_HOSPITALS_PER_BATCH} hospitals."
                ),
            )
        )

    report = CSVValidationReport(
        valid=not errors,
        total_hospitals=total_hospitals,
        valid_hospitals=len(hospitals),
        errors=errors,
    )

    return CSVParseResult(
        report=report,
        hospitals=hospitals,
    )
