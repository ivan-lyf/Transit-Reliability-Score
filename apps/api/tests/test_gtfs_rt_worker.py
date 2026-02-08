"""Tests for GTFS-RT polling worker."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from transit_api.services.gtfs_rt.worker import (
    FEED_SERVICE_ALERTS,
    FEED_TRIP_UPDATES,
    FEED_VEHICLE_POSITIONS,
    GtfsRtWorker,
    reset_worker,
)

from .fixtures.gtfs_rt_fixture import build_trip_update_feed


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    """Reset singleton between tests."""
    reset_worker()


def _make_worker_with_mocks() -> tuple[GtfsRtWorker, dict]:
    """Create a worker with all internal deps mocked."""
    with patch("transit_api.services.gtfs_rt.worker.get_settings") as mock_settings:
        settings = MagicMock()
        settings.gtfs_rt_poll_interval_sec = 30
        settings.stale_feed_threshold_sec = 120
        settings.gtfs_rt_fetch_timeout_sec = 10
        settings.gtfs_rt_max_retries = 1
        settings.gtfs_rt_backoff_base = 0.01
        settings.gtfs_rt_batch_size = 100
        settings.gtfs_trip_updates_full_url = "https://example.com/tu"
        settings.gtfs_vehicle_positions_full_url = "https://example.com/vp"
        settings.gtfs_service_alerts_full_url = "https://example.com/sa"
        mock_settings.return_value = settings

        worker = GtfsRtWorker()

    mocks = {
        "fetcher": worker._fetcher,
        "decoder": worker._decoder,
        "normalizer": worker._normalizer,
        "writer": worker._writer,
    }
    return worker, mocks


class TestGtfsRtWorker:
    """Unit tests for GtfsRtWorker."""

    def test_initial_state(self) -> None:
        worker, _ = _make_worker_with_mocks()
        assert not worker.is_running
        assert worker.poll_count == 0
        assert worker.last_poll_at is None

    @pytest.mark.asyncio
    async def test_start_stop(self) -> None:
        worker, _ = _make_worker_with_mocks()

        # Mock the _poll_loop to avoid actual polling
        worker._poll_loop = AsyncMock()

        await worker.start()
        assert worker.is_running

        await worker.stop()
        assert not worker.is_running

    @pytest.mark.asyncio
    async def test_start_idempotent(self) -> None:
        worker, _ = _make_worker_with_mocks()
        worker._poll_loop = AsyncMock()

        await worker.start()
        await worker.start()  # Should not error
        assert worker.is_running

        await worker.stop()

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self) -> None:
        worker, _ = _make_worker_with_mocks()
        await worker.stop()  # Should not error

    @pytest.mark.asyncio
    async def test_get_status(self) -> None:
        worker, _ = _make_worker_with_mocks()
        status = await worker.get_status()

        assert status["running"] is False
        assert status["poll_count"] == 0
        assert status["last_poll_at"] is None
        assert status["poll_interval_sec"] == 30
        assert status["stale_threshold_sec"] == 120

    @pytest.mark.asyncio
    async def test_run_once_calls_all_feeds(self) -> None:
        worker, _mocks = _make_worker_with_mocks()

        # Mock the _ingest_feed method
        worker._ingest_feed = AsyncMock(
            return_value={
                "status": "ok",
                "entity_count": 10,
                "rows_written": 10,
                "stale": False,
                "error": None,
            }
        )

        report = await worker.run_once()

        assert report["poll_count"] == 1
        assert worker.poll_count == 1
        assert worker.last_poll_at is not None

        # Should be called 3 times (one per feed)
        assert worker._ingest_feed.call_count == 3
        feed_types_called = [call.args[0] for call in worker._ingest_feed.call_args_list]
        assert FEED_TRIP_UPDATES in feed_types_called
        assert FEED_VEHICLE_POSITIONS in feed_types_called
        assert FEED_SERVICE_ALERTS in feed_types_called

    @pytest.mark.asyncio
    async def test_run_once_increments_poll_count(self) -> None:
        worker, _ = _make_worker_with_mocks()
        worker._ingest_feed = AsyncMock(
            return_value={
                "status": "ok",
                "entity_count": 0,
                "rows_written": 0,
                "stale": False,
                "error": None,
            }
        )

        await worker.run_once()
        assert worker.poll_count == 1

        await worker.run_once()
        assert worker.poll_count == 2

    @pytest.mark.asyncio
    async def test_run_once_report_structure(self) -> None:
        worker, _ = _make_worker_with_mocks()
        worker._ingest_feed = AsyncMock(
            return_value={
                "status": "ok",
                "entity_count": 5,
                "rows_written": 5,
                "stale": False,
                "error": None,
            }
        )

        report = await worker.run_once()

        assert "poll_id" in report
        assert "poll_count" in report
        assert "started_at" in report
        assert "ended_at" in report
        assert "feeds" in report
        assert FEED_TRIP_UPDATES in report["feeds"]
        assert FEED_VEHICLE_POSITIONS in report["feeds"]
        assert FEED_SERVICE_ALERTS in report["feeds"]

    @pytest.mark.asyncio
    async def test_partial_feed_failure_isolation(self) -> None:
        """One feed failing should not prevent others from being processed."""
        worker, _ = _make_worker_with_mocks()

        call_count = 0

        async def mock_ingest(feed_type, _url, _poll_id):
            nonlocal call_count
            call_count += 1
            if feed_type == FEED_TRIP_UPDATES:
                return {
                    "status": "error",
                    "entity_count": 0,
                    "rows_written": 0,
                    "stale": False,
                    "error": "fail",
                }
            return {
                "status": "ok",
                "entity_count": 5,
                "rows_written": 5,
                "stale": False,
                "error": None,
            }

        worker._ingest_feed = AsyncMock(side_effect=mock_ingest)

        report = await worker.run_once()

        # All 3 feeds should be called despite one failing
        assert call_count == 3
        assert report["feeds"][FEED_TRIP_UPDATES]["status"] == "error"
        assert report["feeds"][FEED_VEHICLE_POSITIONS]["status"] == "ok"
        assert report["feeds"][FEED_SERVICE_ALERTS]["status"] == "ok"


class TestStaleDetection:
    """Tests for stale feed detection."""

    @pytest.mark.asyncio
    async def test_stale_feed_detected(self) -> None:
        worker, _ = _make_worker_with_mocks()
        worker._stale_threshold = 120

        # Feed with old timestamp (5 min ago)
        old_ts = int(time.time()) - 300
        data = build_trip_update_feed(feed_timestamp=old_ts)

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=MagicMock(rowcount=0))
        mock_session.commit = AsyncMock()

        with (
            patch.object(worker._fetcher, "fetch", AsyncMock(return_value=(data, "abc"))),
            patch("transit_api.services.gtfs_rt.worker.get_session_context") as mock_ctx,
        ):
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await worker._ingest_feed(
                FEED_TRIP_UPDATES, "https://example.com/tu", "poll-1"
            )

        assert result["stale"] is True

    @pytest.mark.asyncio
    async def test_fresh_feed_not_stale(self) -> None:
        worker, _ = _make_worker_with_mocks()
        worker._stale_threshold = 120

        # Feed with recent timestamp
        fresh_ts = int(time.time()) - 10
        data = build_trip_update_feed(feed_timestamp=fresh_ts)

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=MagicMock(rowcount=2))
        mock_session.commit = AsyncMock()

        with (
            patch.object(worker._fetcher, "fetch", AsyncMock(return_value=(data, "abc"))),
            patch("transit_api.services.gtfs_rt.worker.get_session_context") as mock_ctx,
        ):
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await worker._ingest_feed(
                FEED_TRIP_UPDATES, "https://example.com/tu", "poll-1"
            )

        assert result["stale"] is False


class TestWorkerResilience:
    """Tests for worker resilience to errors."""

    @pytest.mark.asyncio
    async def test_worker_survives_fetch_failure(self) -> None:
        from transit_api.services.gtfs_rt.fetcher import FeedFetchError

        worker, _ = _make_worker_with_mocks()

        with (
            patch.object(
                worker._fetcher, "fetch", AsyncMock(side_effect=FeedFetchError("timeout"))
            ),
            patch("transit_api.services.gtfs_rt.worker.get_session_context") as mock_ctx,
        ):
            mock_session = AsyncMock()
            mock_session.execute = AsyncMock()
            mock_session.commit = AsyncMock()
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await worker._ingest_feed(
                FEED_TRIP_UPDATES, "https://example.com/tu", "poll-1"
            )

        assert result["status"] == "error"
        assert "timeout" in result["error"]

    @pytest.mark.asyncio
    async def test_worker_survives_decode_failure(self) -> None:
        from transit_api.services.gtfs_rt.decoder import DecodeError_

        worker, _ = _make_worker_with_mocks()

        with (
            patch.object(worker._fetcher, "fetch", AsyncMock(return_value=(b"data", "hash"))),
            patch.object(worker._decoder, "decode", side_effect=DecodeError_("bad data")),
            patch("transit_api.services.gtfs_rt.worker.get_session_context") as mock_ctx,
        ):
            mock_session = AsyncMock()
            mock_session.execute = AsyncMock()
            mock_session.commit = AsyncMock()
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await worker._ingest_feed(
                FEED_TRIP_UPDATES, "https://example.com/tu", "poll-1"
            )

        assert result["status"] == "error"
        assert "bad data" in result["error"]
