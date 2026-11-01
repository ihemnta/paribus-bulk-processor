"""
Dedicated PostgreSQL-backed hospital batch worker.

Usage examples:

    # Process one specific batch:
    python -m scripts.run_worker \
        --execute --batch-id <UUID>

    # Process one available queued or activating batch:
    python -m scripts.run_worker --execute --once

    # Continuously poll the database:
    python -m scripts.run_worker --execute

Safety:
    Live writes require BOTH --execute and
    ENABLE_REAL_UPSTREAM_WRITES=true.

    The sandbox database is explicitly disallowed.

    This process must run separately from FastAPI.
"""

import argparse
import asyncio
import logging
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy.engine import make_url

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.integrations.hospital_directory import (
    HospitalDirectoryClient,
    build_http_client,
)
from app.persistence.database import SessionFactory, engine
from app.persistence.models import BatchJob
from app.services.batch_activation import BatchActivationService
from app.services.worker_recovery import recover_expired_batches
from app.services.worker_runner import (
    find_ready_activation_batch,
    process_next_queued_batch,
    process_queued_batch_by_id,
)

logger = logging.getLogger(__name__)


async def activate_ready_batch(
    batch_id: UUID,
    client: HospitalDirectoryClient,
) -> str:
    """
    Attempt activation under a bounded execution deadline.

    If activation times out, lease-expiry recovery and read-only
    reconciliation must investigate it before any further PATCH.
    """
    timeout_seconds = get_settings().worker_lease_seconds / 2

    try:
        async with asyncio.timeout(timeout_seconds):
            return await BatchActivationService().activate_batch(
                batch_id,
                client,
            )

    except TimeoutError:
        logger.warning(
            "worker.activation.timeout",
            extra={"batch_id": str(batch_id)},
        )
        return "activation_pending_recovery"


async def execute_one_cycle(
    client: HospitalDirectoryClient,
) -> bool:
    """
    Recover expired leases and execute one available work item.

    Returns True if a queued or activating job was found.
    Recovery itself performs no external HTTP writes.
    """
    await recover_expired_batches(limit=20)

    # Complete creation-finished batches before claiming
    # new creation work.
    ready_id = await find_ready_activation_batch()

    if ready_id is not None:
        result = await activate_ready_batch(
            ready_id,
            client,
        )

        logger.info(
            "worker.activation.result status=%s",
            result,
            extra={"batch_id": str(ready_id)},
        )
        return True

    claimed = await process_next_queued_batch(client)

    if claimed is None:
        return False

    batch_id, result = claimed

    logger.info(
        "worker.processing.result status=%s",
        result,
        extra={"batch_id": str(batch_id)},
    )
    return True


async def execute_selected_batch(
    batch_id: UUID,
    client: HospitalDirectoryClient,
) -> None:
    """Execute one explicitly selected batch, if eligible."""
    async with SessionFactory() as session:
        batch = await session.get(BatchJob, batch_id)

        if batch is None:
            raise ValueError("Batch not found.")

        status = batch.status
        prior_activation = batch.activation_requested_at is not None

    if status == "queued":
        result = await process_queued_batch_by_id(
            batch_id,
            client,
        )

    elif status == "activating" and not prior_activation:
        result = await activate_ready_batch(
            batch_id,
            client,
        )

    else:
        logger.warning(
            "worker.batch.not_eligible status=%s",
            status,
            extra={"batch_id": str(batch_id)},
        )
        return

    logger.info(
        "worker.selected_batch.finished status=%s",
        result,
        extra={"batch_id": str(batch_id)},
    )


async def run_worker(
    *,
    batch_id: UUID | None,
    once: bool,
    poll_seconds: float,
) -> None:
    """
    Run one selected batch, one available job, or a polling loop.

    The HTTPX client is shared for connection pooling.
    Each database operation uses its own AsyncSession.
    """
    try:
        async with build_http_client() as http_client:
            client = HospitalDirectoryClient(http_client)

            if batch_id is not None:
                await execute_selected_batch(
                    batch_id,
                    client,
                )
                return

            while True:
                worked = await execute_one_cycle(client)

                if once:
                    if not worked:
                        logger.info("worker.queue.empty")
                    return

                if not worked:
                    await asyncio.sleep(poll_seconds)

    finally:
        await engine.dispose()


def validate_live_configuration() -> None:
    """
    Refuse accidental writes to a sandbox or unexpected host.

    This is a development safety guard, not a complete
    authorization or network-security boundary.
    """
    settings = get_settings()

    if not settings.enable_real_upstream_writes:
        raise RuntimeError(
            "Live writes disabled. Set ENABLE_REAL_UPSTREAM_WRITES=true explicitly."
        )

    database_name = make_url(settings.database_url).database

    if database_name == "paribus_bulk_sandbox":
        raise RuntimeError("Refusing real upstream writes from the sandbox database.")

    upstream_host = urlsplit(str(settings.upstream_base_url))

    if (
        upstream_host.scheme != "https"
        or upstream_host.hostname != "hospital-directory.onrender.com"
    ):
        raise RuntimeError(
            "Unexpected upstream API host. Live worker execution is blocked."
        )


def main() -> None:
    """Parse worker arguments and start the async event loop."""
    parser = argparse.ArgumentParser(
        description="Run the Paribus hospital batch worker."
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Explicitly authorize live upstream processing.",
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one database polling cycle, then exit.",
    )

    parser.add_argument(
        "--batch-id",
        type=UUID,
        help="Process one explicitly selected batch UUID.",
    )

    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=get_settings().worker_poll_seconds,
    )

    args = parser.parse_args()

    if not args.execute:
        parser.error("Live execution requires the --execute flag.")

    if not 0.25 <= args.poll_seconds <= 60:
        parser.error("--poll-seconds must be between 0.25 and 60.")

    configure_logging(get_settings().log_level)
    validate_live_configuration()

    logger.info("worker.starting")

    try:
        asyncio.run(
            run_worker(
                batch_id=args.batch_id,
                once=args.once,
                poll_seconds=args.poll_seconds,
            )
        )
    except KeyboardInterrupt:
        logger.info("worker.stopped_by_user")
    except Exception:
        # The top-level worker boundary is deliberately
        # fail-fast. An unexpected error must not cause
        # blind replay of uncertain external requests.
        logger.exception("worker.fatal_error")
        raise


if __name__ == "__main__":
    main()
