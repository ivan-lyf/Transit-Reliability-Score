"""Integration tests for GTFS-RT pipeline (mock feeds, real normalization)."""

import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from transit_api.services.gtfs_rt.decoder import GtfsRtDecoder
from transit_api.services.gtfs_rt.normalizer import GtfsRtNormalizer
from transit_api.services.gtfs_rt.worker import (
    FEED_SERVICE_ALERTS,
    FEED_TRIP_UPDATES,
    FEED_VEHICLE_POSITIONS,
    GtfsRtWorker,
    reset_worker,
)

from fixtures.gtfs_rt_fixture import (
    build_alert_feed,
    build_multi_entity_trip_update_feed,
    build_trip_update_feed,
    build_vehicle_position_feed,
)


@pytest.fixture(autouse=True)
def _reset() -> None:
    reset_worker()


class TestFullPipelineIntegration:
    """Tests that exercise fetch -> decode -> normalize -> (mock) write."""

    @pytest.mark.asyncio
    async def test_trip_update_pipeline(self) -> None:
        """Full pipeline: raw protobuf -> decoded -> normalized rows."""
        ts = int(time.time())
        data = build_trip_update_feed(
            trip_id="T100", route_id="R10",
            stop_updates=[
                {"stop_id": "SA", "stop_sequence": 1, "arrival_delay": 30, "departure_delay": 35},
                {"stop_id": "SB", "stop_sequence": 2, "arrival_delay": 90, "departure_delay": 95},
            ],
            feed_timestamp=ts,
        )

        # Decode
        feed = GtfsRtDecoder.decode(data, "trip_updates", "test-poll")
        assert GtfsRtDecoder.get_entity_count(feed) == 1
        assert GtfsRtDecoder.get_feed_timestamp(feed) == ts

        # Normalize
        rows = GtfsRtNormalizer.normalize_trip_updates(feed)
        assert len(rows) == 2
        assert rows[0]["trip_id"] == "T100"
        assert rows[0]["stop_id"] == "SA"
        assert rows[0]["arrival_delay"] == 30
        assert rows[1]["stop_id"] == "SB"
        assert rows[1]["arrival_delay"] == 90

    @pytest.mark.asyncio
    async def test_vehicle_position_pipeline(self) -> None:
        ts = int(time.time())
        data = build_vehicle_position_feed(
            vehicle_id="V99", trip_id="T50", route_id="R5",
            lat=49.25, lon=-123.10, bearing=270.0, speed=15.0,
            feed_timestamp=ts,
        )

        feed = GtfsRtDecoder.decode(data, "vehicle_positions", "test-poll")
        rows = GtfsRtNormalizer.normalize_vehicle_positions(feed)

        assert len(rows) == 1
        assert rows[0]["vehicle_id"] == "V99"
        assert rows[0]["latitude"] == pytest.approx(49.25, abs=0.01)
        assert rows[0]["speed"] == pytest.approx(15.0)

    @pytest.mark.asyncio
    async def test_alert_pipeline(self) -> None:
        ts = int(time.time())
        data = build_alert_feed(
            alert_id="ALERT99",
            cause=6, effect=1,
            header="Bus Rerouted",
            description="Route 99 detoured due to accident",
            route_id="R99",
            active_start=ts - 1800,
            active_end=ts + 3600,
            feed_timestamp=ts,
        )

        feed = GtfsRtDecoder.decode(data, "service_alerts", "test-poll")
        rows = GtfsRtNormalizer.normalize_alerts(feed)

        assert len(rows) == 1
        assert rows[0]["alert_id"] == "ALERT99"
        assert rows[0]["cause"] == "ACCIDENT"
        assert rows[0]["effect"] == "NO_SERVICE"
        assert rows[0]["header_text"] == "Bus Rerouted"

    @pytest.mark.asyncio
    async def test_multi_entity_pipeline(self) -> None:
        ts = int(time.time())
        data = build_multi_entity_trip_update_feed(count=20, feed_timestamp=ts)

        feed = GtfsRtDecoder.decode(data, "trip_updates", "test-poll")
        rows = GtfsRtNormalizer.normalize_trip_updates(feed)

        assert len(rows) == 20
        # Verify all trip IDs are unique
        trip_ids = {r["trip_id"] for r in rows}
        assert len(trip_ids) == 20


class TestDuplicateHandling:
    """Tests for duplicate payload handling."""

    @pytest.mark.asyncio
    async def test_same_feed_produces_same_rows(self) -> None:
        """Two identical feeds should produce identical normalized rows (minus recorded_at)."""
        ts = int(time.time())
        data = build_trip_update_feed(trip_id="T1", feed_timestamp=ts)

        feed1 = GtfsRtDecoder.decode(data, "trip_updates", "poll-1")
        feed2 = GtfsRtDecoder.decode(data, "trip_updates", "poll-2")

        rows1 = GtfsRtNormalizer.normalize_trip_updates(feed1)
        rows2 = GtfsRtNormalizer.normalize_trip_updates(feed2)

        # Same content (minus recorded_at which uses now())
        assert len(rows1) == len(rows2)
        for r1, r2 in zip(rows1, rows2):
            assert r1["trip_id"] == r2["trip_id"]
            assert r1["stop_id"] == r2["stop_id"]
            assert r1["feed_timestamp"] == r2["feed_timestamp"]


class TestWorkerPollCycleIntegration:
    """Integration test for a full worker poll cycle with mocked network."""

    @pytest.mark.asyncio
    async def test_worker_run_once_with_real_decode_normalize(self) -> None:
        """Worker run_once with real protobuf decode/normalize, mocked fetch/write."""
        ts = int(time.time())
        tu_data = build_trip_update_feed(feed_timestamp=ts)
        vp_data = build_vehicle_position_feed(feed_timestamp=ts)
        sa_data = build_alert_feed(feed_timestamp=ts)

        feed_data_map = {
            "trip_updates": tu_data,
            "vehicle_positions": vp_data,
            "service_alerts": sa_data,
        }

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

        async def mock_fetch(url, feed_type, poll_id):
            data = feed_data_map[feed_type]
            return data, "fakehash"

        worker._fetcher.fetch = AsyncMock(side_effect=mock_fetch)

        # Mock DB writes
        mock_session = AsyncMock()
        result_mock = MagicMock()
        result_mock.rowcount = 2
        mock_session.execute = AsyncMock(return_value=result_mock)
        mock_session.commit = AsyncMock()

        with patch("transit_api.services.gtfs_rt.worker.get_session_context") as mock_ctx:
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            report = await worker.run_once()

        assert report["poll_count"] == 1
        for feed_type in [FEED_TRIP_UPDATES, FEED_VEHICLE_POSITIONS, FEED_SERVICE_ALERTS]:
            assert report["feeds"][feed_type]["status"] == "ok"
            assert report["feeds"][feed_type]["entity_count"] > 0

    @pytest.mark.asyncio
    async def test_worker_multiple_cycles_smoke(self) -> None:
        """Run several poll cycles with fixture feeds to ensure no crash loop."""
        ts = int(time.time())
        tu_data = build_trip_update_feed(feed_timestamp=ts)
        vp_data = build_vehicle_position_feed(feed_timestamp=ts)
        sa_data = build_alert_feed(feed_timestamp=ts)

        feed_data_map = {
            "trip_updates": tu_data,
            "vehicle_positions": vp_data,
            "service_alerts": sa_data,
        }

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

        async def mock_fetch(url, feed_type, poll_id):
            data = feed_data_map[feed_type]
            return data, "fakehash"

        worker._fetcher.fetch = AsyncMock(side_effect=mock_fetch)

        mock_session = AsyncMock()
        result_mock = MagicMock()
        result_mock.rowcount = 2
        mock_session.execute = AsyncMock(return_value=result_mock)
        mock_session.commit = AsyncMock()

        with patch("transit_api.services.gtfs_rt.worker.get_session_context") as mock_ctx:
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            for cycle in range(3):
                report = await worker.run_once()
                assert report["poll_count"] == cycle + 1
