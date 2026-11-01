"""
Structured application logging.

Every application log is emitted as JSON for easier filtering in
Docker, cloud log aggregators, and production monitoring systems.

Sensitive hospital data must not be logged.
"""

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime

# Each incoming request gets its own correlation identifier.
request_id_context: ContextVar[str | None] = ContextVar(
    "request_id",
    default=None,
)


class JsonFormatter(logging.Formatter):
    """Format Python log records as structured JSON."""

    def format(self, record: logging.LogRecord) -> str:
        """
        Convert a log record into a JSON object.

        Include request correlation and selected operational metadata,
        while avoiding raw request bodies and personal data.
        """
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": request_id_context.get(),
        }

        # Include operational fields when supplied by the caller.
        for field in (
            "method",
            "path",
            "status_code",
            "duration_ms",
            "batch_id",
            "hospital_row_id",
            "attempt",
            "total_hospitals",
            "valid_hospitals",
            "error_count",
            "file_size_bytes",
            "upstream_status_code",
            "worker_id",
            "row_status",
            "batch_status",
        ):
            if hasattr(record, field):
                payload[field] = getattr(record, field)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    """
    Configure the root logger to emit JSON logs to stdout.

    Stdout is preferred for containerized applications because the
    hosting platform manages log collection and retention.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=[handler],
        force=True,
    )
