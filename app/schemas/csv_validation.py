"""
Public API schemas for hospital CSV validation.

These models describe the validation response returned to clients.
They contain no database or external HTTP integration logic.
"""

from pydantic import BaseModel, Field


class ValidationIssue(BaseModel):
    """Describe an individual CSV validation error."""

    row: int | None = Field(
        default=None,
        description="1-based data-row number; excludes the header.",
    )
    field: str | None = Field(
        default=None,
        description="Column or file component causing the error.",
    )
    message: str


class CSVValidationReport(BaseModel):
    """Summarize CSV validation without modifying hospital records."""

    valid: bool
    total_hospitals: int = Field(ge=0)
    valid_hospitals: int = Field(ge=0)
    errors: list[ValidationIssue] = Field(default_factory=list)
