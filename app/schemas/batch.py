"""
Public response schemas for hospital bulk-processing operations.

These models intentionally omit hospital addresses and phone numbers
from status responses. The API returns operational metadata only.
"""

import re
from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

BatchState = Literal[
    "queued",
    "processing",
    "activating",
    "completed",
    "failed",
    "needs_reconciliation",
]

HospitalState = Literal[
    "pending",
    "creating",
    "created",
    "failed",
    "unknown",
    "activated",
]


class BatchAcceptedResponse(BaseModel):
    """Returned after a validated batch commits successfully."""

    batch_id: UUID
    status: Literal["queued"] = "queued"
    total_hospitals: int = Field(ge=1, le=20)
    processed_hospitals: int = 0
    failed_hospitals: int = 0
    batch_activated: bool = False
    status_url: str


class HospitalProgress(BaseModel):
    """Public processing state of a single hospital CSV row."""

    row: int
    name: str
    hospital_id: int | None = None
    status: HospitalState


class BatchProgressResponse(BaseModel):
    """Current durable processing status of an existing batch."""

    batch_id: UUID
    status: BatchState
    total_hospitals: int

    processed_hospitals: int
    failed_hospitals: int
    unknown_hospitals: int
    pending_hospitals: int
    processing_hospitals: int

    batch_activated: bool
    created_at: datetime
    completed_at: datetime | None

    hospitals: list[HospitalProgress] = Field(default_factory=list)


class HospitalCorrection(BaseModel):
    """Corrected values for one definitively rejected CSV row."""

    row: int = Field(ge=1, le=20)
    name: str
    address: str
    phone: str | None = None

    @field_validator("name", "address")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        """Normalize and reject empty required fields."""
        normalized = value.strip()
        if not normalized:
            raise ValueError("This field cannot be empty.")
        return normalized

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, value: str | None) -> str | None:
        """Match the existing optional ten-digit phone policy."""
        if value is None:
            return None

        normalized = value.strip()
        if not normalized:
            return None

        if re.fullmatch(r"[0-9]{10}", normalized) is None:
            raise ValueError("Phone number must contain exactly 10 digits.")

        return normalized


class ResumeBatchRequest(BaseModel):
    """Explicit corrections required before requeuing failed rows."""

    corrections: list[HospitalCorrection] = Field(
        min_length=1,
        max_length=20,
    )

    @model_validator(mode="after")
    def validate_unique_rows(self) -> Self:
        """Prevent multiple corrections targeting the same CSV row."""
        numbers = [item.row for item in self.corrections]

        if len(numbers) != len(set(numbers)):
            raise ValueError("Each hospital row can be corrected only once.")

        return self


class ResumeBatchResponse(BaseModel):
    """Response after a corrected batch is durably requeued."""

    batch_id: UUID
    status: Literal["queued"] = "queued"
    requeued_hospitals: int = Field(ge=1, le=20)
    status_url: str


class ReconcileBatchResponse(BaseModel):
    """Result of a manually requested batch reconciliation."""

    batch_id: UUID
    phase: Literal["creation", "activation"]
    outcome: Literal[
        "reconciled",
        "completed",
        "unresolved",
        "skipped",
    ]
    batch_status: str
    recovered_hospitals: int = 0
    reason: str | None = None
