"""GTFS-RT polling worker with configurable intervals."""

from __future__ import annotations

import asyncio
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any

from transit_api.config import get_settings
from transit_api.database import get_session_context
from transit_api.logging import get_logger
from transit_api.services.gtfs_rt.decoder import DecodeError_, GtfsRtDecoder
from transit_api.services.gtfs_rt.fetcher import FeedFetchError, GtfsRtFetcher
from transit_api.services.gtfs_rt.normalizer import GtfsRtNormalizer
from transit_api.services.gtfs_rt.writer import GtfsRtWriter

logger = get_logger(__name__)

# Feed type constants
FEED_TRIP_UPDATES = "trip_updates"
FEED_VEHICLE_POSITIONS = "vehicle_positions"
FEED_SERVICE_ALERTS = "service_alerts"


class GtfsRtWorker:
    """Polls GTFS-RT feeds on a schedule and persists normalized data.

    Usage:
        worker = GtfsRtWorker()
        await worker.start()   # launches background task
        await worker.stop()    # cancels background task

        # Or run a single poll cycle:
        report = await worker.run_once()
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._poll_interval = settings.gtfs_rt_poll_interval_sec
        self._stale_threshold = settings.stale_feed_threshold_sec
        self._fetcher = GtfsRtFetcher(
            timeout_sec=settings.gtfs_rt_fetch_timeout_sec,
            max_retries=settings.gtfs_rt_max_retries,
            backoff_base=settings.gtfs_rt_backoff_base,
        )
        self._decoder = GtfsRtDecoder()
        self._normalizer = GtfsRtNormalizer()
        self._writer = GtfsRtWriter(batch_size=settings.gtfs_rt_batch_size)

        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._poll_count = 0
        self._last_poll_at: datetime | None = None
        self._feed_urls = {
            FEED_TRIP_UPDATES: settings.gtfs_trip_updates_full_url,
            FEED_VEHICLE_POSITIONS: settings.gtfs_vehicle_positions_full_url,
            FEED_SERVICE_ALERTS: settings.gtfs_service_alerts_full_url,
        }

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def poll_count(self) -> int:
        return self._poll_count

    @property
    def last_poll_at(self) -> datetime | None:
        return self._last_poll_at

    async def start(self) -> None:
        """Start the background polling loop."""
        if self._running:
            logger.warning("Worker already running, ignoring start request")
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(
            "GTFS-RT worker started",
            poll_interval_sec=self._poll_interval,
        )

    async def stop(self) -> None:
        """Stop the background polling loop."""
        if not self._running:
            return

        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        logger.info("GTFS-RT worker stopped")

    async def run_once(self) -> dict[str, Any]:
        """Execute a single poll cycle across all feeds.

        Returns:
            Report dict with per-feed results.
        """
        poll_id = str(uuid.uuid4())[:8]
        self._poll_count += 1
        self._last_poll_at = datetime.now(timezone.utc)

        logger.info(
            "Starting poll cycle",
            poll_id=poll_id,
            poll_count=self._poll_count,
        )

        report: dict[str, Any] = {
            "poll_id": poll_id,
            "poll_count": self._poll_count,
            "started_at": self._last_poll_at.isoformat(),
            "feeds": {},
        }

        for feed_type, url in self._feed_urls.items():
            feed_report = await self._ingest_feed(feed_type, url, poll_id)
            report["feeds"][feed_type] = feed_report

        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("Poll cycle complete", poll_id=poll_id, report=report)
        return report

    async def get_status(self) -> dict[str, Any]:
        """Get current worker status for health/meta endpoints."""
        return {
            "running": self._running,
            "poll_count": self._poll_count,
            "last_poll_at": self._last_poll_at.isoformat() if self._last_poll_at else None,
            "poll_interval_sec": self._poll_interval,
            "stale_threshold_sec": self._stale_threshold,
        }

    async def _poll_loop(self) -> None:
        """Main polling loop that runs until stopped."""
        while self._running:
            try:
                await self.run_once()
            except Exception as exc:
                logger.error("Poll cycle failed unexpectedly", exc_info=exc)

            try:
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break

    async def _ingest_feed(self, feed_type: str, url: str, poll_id: str) -> dict[str, Any]:
        """Ingest a single GTFS-RT feed: fetch, decode, normalize, write.

        Isolated per feed - failure in one doesn't affect others.
        """
        result: dict[str, Any] = {
            "status": "error",
            "entity_count": 0,
            "rows_written": 0,
            "stale": False,
            "error": None,
        }

        try:
            # Fetch
            data, feed_hash = await self._fetcher.fetch(url, feed_type, poll_id)

            # Decode
            feed = self._decoder.decode(data, feed_type, poll_id)

            # Check staleness
            feed_ts = self._decoder.get_feed_timestamp(feed)
            entity_count = self._decoder.get_entity_count(feed)
            result["entity_count"] = entity_count

            if feed_ts:
                age_sec = (
                    datetime.now(timezone.utc) - datetime.fromtimestamp(feed_ts, tz=timezone.utc)
                ).total_seconds()
                if age_sec > self._stale_threshold:
                    logger.warning(
                        "Stale GTFS-RT feed detected",
                        feed_type=feed_type,
                        poll_id=poll_id,
                        feed_age_sec=int(age_sec),
                        threshold_sec=self._stale_threshold,
                    )
                    result["stale"] = True

            # Normalize
            rows = self._normalize_feed(feed_type, feed)

            # Write
            async with get_session_context() as session:
                rows_written = await self._write_feed(session, feed_type, rows, poll_id)
                result["rows_written"] = rows_written

                # Update ingest meta
                await self._writer.update_ingest_meta(
                    session,
                    feed_type=feed_type,
                    status="ok",
                    entity_count=entity_count,
                    feed_hash=feed_hash,
                )

            result["status"] = "ok"

        except (FeedFetchError, DecodeError_) as exc:
            result["error"] = str(exc)
            logger.error(
                "Feed ingest failed",
                feed_type=feed_type,
                poll_id=poll_id,
                error=str(exc),
            )
            # Update meta with error
            try:
                async with get_session_context() as session:
                    await self._writer.update_ingest_meta(
                        session,
                        feed_type=feed_type,
                        status="error",
                        error_message=str(exc),
                    )
            except Exception:
                logger.error("Failed to update ingest meta on error", feed_type=feed_type)

        except Exception as exc:
            result["error"] = str(exc)
            logger.error(
                "Unexpected feed ingest error",
                feed_type=feed_type,
                poll_id=poll_id,
                exc_info=exc,
            )
            try:
                async with get_session_context() as session:
                    await self._writer.update_ingest_meta(
                        session,
                        feed_type=feed_type,
                        status="error",
                        error_message=str(exc),
                    )
            except Exception:
                logger.error("Failed to update ingest meta on error", feed_type=feed_type)

        return result

    def _normalize_feed(self, feed_type: str, feed: Any) -> list[dict[str, Any]]:
        """Route to the correct normalizer based on feed type."""
        if feed_type == FEED_TRIP_UPDATES:
            return self._normalizer.normalize_trip_updates(feed)
        elif feed_type == FEED_VEHICLE_POSITIONS:
            return self._normalizer.normalize_vehicle_positions(feed)
        elif feed_type == FEED_SERVICE_ALERTS:
            return self._normalizer.normalize_alerts(feed)
        return []

    async def _write_feed(
        self,
        session: Any,
        feed_type: str,
        rows: list[dict[str, Any]],
        poll_id: str,
    ) -> int:
        """Route to the correct writer based on feed type."""
        if feed_type == FEED_TRIP_UPDATES:
            return await self._writer.write_trip_updates(session, rows, poll_id)
        elif feed_type == FEED_VEHICLE_POSITIONS:
            return await self._writer.write_vehicle_positions(session, rows, poll_id)
        elif feed_type == FEED_SERVICE_ALERTS:
            return await self._writer.write_alerts(session, rows, poll_id)
        return 0


# Singleton instance for the app lifecycle
_worker_instance: GtfsRtWorker | None = None


def get_worker() -> GtfsRtWorker:
    """Get or create the singleton worker instance."""
    global _worker_instance
    if _worker_instance is None:
        _worker_instance = GtfsRtWorker()
    return _worker_instance


def reset_worker() -> None:
    """Reset the singleton (for testing)."""
    global _worker_instance
    _worker_instance = None
