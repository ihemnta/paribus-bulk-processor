"""
SQLAlchemy models for durable hospital bulk processing.

The database stores workflow state and imported CSV rows.
It does not replace the upstream Hospital Directory API.

All state transitions must be controlled by the application layer.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Shared declarative base for all persistent models."""


class BatchJob(Base):
    """
    Represent one accepted hospital bulk-processing operation.

    A job is created only after its entire CSV has passed validation.
    Individual hospital records are stored in BatchHospital.
    """

    __tablename__ = "batch_jobs"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )

    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default="queued",
    )

    total_hospitals: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
    )

    # Reserved for future worker claiming and crash recovery.
    lease_owner: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )

    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    activation_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    activation_confirmed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    hospitals: Mapped[list[BatchHospital]] = relationship(
        back_populates="batch",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        CheckConstraint(
            "total_hospitals BETWEEN 1 AND 20",
            name="ck_batch_jobs_hospital_count",
        ),
        CheckConstraint(
            "status IN ("
            "'queued', 'processing', 'activating', "
            "'completed', 'failed', 'needs_reconciliation'"
            ")",
            name="ck_batch_jobs_status",
        ),
        Index(
            "ix_batch_jobs_status_lease",
            "status",
            "lease_expires_at",
        ),
    )


class BatchHospital(Base):
    """
    Represent one validated hospital row within a batch.

    An upstream hospital ID is assigned only after creation has
    been confirmed or safely reconciled.
    """

    __tablename__ = "batch_hospitals"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(),
        primary_key=True,
    )

    batch_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey("batch_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )

    row_number: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
    )

    name: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    address: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    phone: Mapped[str | None] = mapped_column(
        String(10),
        nullable=True,
    )

    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default="pending",
    )

    upstream_hospital_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )

    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="0",
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    last_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    batch: Mapped[BatchJob] = relationship(
        back_populates="hospitals",
    )

    __table_args__ = (
        UniqueConstraint(
            "batch_id",
            "row_number",
            name="uq_batch_hospital_row",
        ),
        UniqueConstraint(
            "upstream_hospital_id",
            name="uq_batch_hospital_upstream_id",
        ),
        CheckConstraint(
            "row_number BETWEEN 1 AND 20",
            name="ck_batch_hospital_row_number",
        ),
        CheckConstraint(
            "attempt_count >= 0",
            name="ck_batch_hospital_attempts",
        ),
        CheckConstraint(
            "btrim(name) <> '' AND btrim(address) <> ''",
            name="ck_batch_hospital_required_fields",
        ),
        CheckConstraint(
            "phone IS NULL OR phone ~ '^[0-9]{10}$'",
            name="ck_batch_hospital_phone",
        ),
        CheckConstraint(
            "status IN ("
            "'pending', 'creating', 'created', "
            "'failed', 'unknown', 'activated'"
            ")",
            name="ck_batch_hospital_status",
        ),
        Index(
            "ix_batch_hospitals_batch_status",
            "batch_id",
            "status",
        ),
    )
