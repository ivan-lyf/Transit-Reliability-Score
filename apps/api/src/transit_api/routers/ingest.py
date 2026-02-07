"""GTFS-RT ingest control and status endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter
from pydantic import BaseModel

from transit_api.config import get_settings
from transit_api.database import get_session_context
from transit_api.logging import get_logger
from transit_api.services.gtfs_rt.worker import get_worker

logger = get_logger(__name__)

router = APIRouter(tags=["ingest"])


# --- Response schemas ---


class FeedIngestStatus(BaseModel):
    """Status of a single RT feed type."""

    feed_type: str
    status: str
    last_success_at: str = ""
    last_attempt_at: str = ""
    error_message: str = ""
    entity_count: int = 0
    feed_hash: str = ""
    is_fresh: bool = True


class LastIngestResponse(BaseModel):
    """Response for /meta/last-ingest."""

    feeds: List[FeedIngestStatus]
    stale_threshold_sec: int


class WorkerStatusResponse(BaseModel):
    """Response for worker status."""

    running: bool
    poll_count: int
    last_poll_at: str = ""
    poll_interval_sec: int


class RunOnceResponse(BaseModel):
    """Response for run-once endpoint."""

    poll_id: str
    poll_count: int
    started_at: str
    ended_at: str = ""
    feeds: Dict[str, Any]


# --- Meta endpoints ---


@router.get(
    "/meta/last-ingest",
    response_model=LastIngestResponse,
    summary="Get last ingest status per feed",
)
async def get_last_ingest() -> dict[str, Any]:
    """Return ingest status for each GTFS-RT feed type with freshness flag."""
    settings = get_settings()
    stale_threshold = settings.stale_feed_threshold_sec
    now = datetime.now(timezone.utc)

    feeds: list[dict[str, Any]] = []

    try:
        async with get_session_context() as session:
            from sqlalchemy import text

            result = await session.execute(
                text("SELECT feed_type, last_success_at, last_attempt_at, status, "
                     "error_message, entity_count, feed_hash FROM rt_ingest_meta")
            )
            rows = result.fetchall()

            for row in rows:
                last_success = row[1]
                is_fresh = True
                if last_success:
                    age_sec = (now - last_success).total_seconds()
                    is_fresh = age_sec <= stale_threshold

                feeds.append({
                    "feed_type": row[0],
                    "status": row[3],
                    "last_success_at": row[1].isoformat() if row[1] else "",
                    "last_attempt_at": row[2].isoformat() if row[2] else "",
                    "error_message": row[4] or "",
                    "entity_count": row[5] or 0,
                    "feed_hash": row[6] or "",
                    "is_fresh": is_fresh,
                })
    except Exception as exc:
        logger.warning("Could not read ingest meta (table may not exist)", error=str(exc))

    return {
        "feeds": feeds,
        "stale_threshold_sec": stale_threshold,
    }


# --- Admin endpoints ---


@router.post(
    "/admin/ingest/gtfs-rt/run-once",
    response_model=RunOnceResponse,
    summary="Trigger a single GTFS-RT poll cycle",
)
async def run_once() -> dict[str, Any]:
    """Execute one poll cycle immediately (all feeds)."""
    worker = get_worker()
    report = await worker.run_once()
    return report


@router.post(
    "/admin/ingest/gtfs-rt/start",
    response_model=WorkerStatusResponse,
    summary="Start the GTFS-RT polling worker",
)
async def start_worker() -> dict[str, Any]:
    """Start the background polling worker."""
    worker = get_worker()
    await worker.start()
    return await worker.get_status()


@router.post(
    "/admin/ingest/gtfs-rt/stop",
    response_model=WorkerStatusResponse,
    summary="Stop the GTFS-RT polling worker",
)
async def stop_worker() -> dict[str, Any]:
    """Stop the background polling worker."""
    worker = get_worker()
    await worker.stop()
    return await worker.get_status()


@router.get(
    "/admin/ingest/gtfs-rt/status",
    response_model=WorkerStatusResponse,
    summary="Get GTFS-RT worker status",
)
async def worker_status() -> dict[str, Any]:
    """Get current worker status."""
    worker = get_worker()
    return await worker.get_status()
