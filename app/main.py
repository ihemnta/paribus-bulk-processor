"""
FastAPI application entry point.

Provides request correlation, structured request logs, and a basic
liveness endpoint. Business routes will be registered separately.
"""

import logging
import re
import time
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request

from app.api.batches import router as batches_router
from app.api.validation import router as validation_router
from app.core.config import get_settings
from app.core.logging import (
    configure_logging,
    request_id_context,
)
from app.persistence.database import engine

settings = get_settings()
configure_logging(settings.log_level)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    """
    Manage resources for the FastAPI application lifespan.

    Database connections are released during graceful shutdown.
    The database engine does not establish a connection merely
    because it has been constructed.
    """
    logger.info("application.starting")

    try:
        yield
    finally:
        await engine.dispose()
        logger.info("application.shutdown.completed")


app = FastAPI(
    title=settings.app_name,
    description="Hospital CSV bulk processing API",
    version="0.1.0",
    lifespan=lifespan,
)

# Register hospital CSV pre-validation endpoints.
app.include_router(validation_router)
# Register hospital batch processing
app.include_router(batches_router)


@app.middleware("http")
async def request_logging_middleware(
    request: Request,
    call_next,
):
    """
    Assign a correlation ID and log request completion.

    A client-supplied request ID is accepted only if it uses a safe,
    bounded character format. Otherwise, generate a new UUID.
    """
    incoming_id = request.headers.get("X-Request-ID", "")

    if re.fullmatch(r"[A-Za-z0-9._-]{1,64}", incoming_id):
        request_id = incoming_id
    else:
        request_id = str(uuid4())

    token = request_id_context.set(request_id)
    start_time = time.perf_counter()
    status_code = 500

    try:
        response = await call_next(request)

        status_code = response.status_code
        response.headers["X-Request-ID"] = request_id

        return response

    except Exception:
        logger.exception("http.request.unhandled_exception")
        raise

    finally:
        duration_ms = round(
            (time.perf_counter() - start_time) * 1000,
            2,
        )

        logger.info(
            "http.request.completed",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": duration_ms,
            },
        )

        request_id_context.reset(token)


@app.get("/health", tags=["Health"])
async def health_check() -> dict[str, str]:
    """
    Return application liveness status.

    This endpoint confirms the HTTP process is running.
    Database and upstream readiness checks will be added separately.
    """
    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.app_env,
    }
