"""Tests for GTFS-RT database writer."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from transit_api.services.gtfs_rt.writer import GtfsRtWriter


def _make_session(rowcount: int = 1) -> AsyncMock:
    """Create a mock async session."""
    session = AsyncMock()
    result_mock = MagicMock()
    result_mock.rowcount = rowcount
    session.execute = AsyncMock(return_value=result_mock)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


class TestGtfsRtWriter:
    """Unit tests for GtfsRtWriter."""

    @pytest.mark.asyncio
    async def test_write_trip_updates(self) -> None:
        session = _make_session(rowcount=2)
        writer = GtfsRtWriter(batch_size=100)
        now = datetime.now(timezone.utc)

        rows = [
            {
                "trip_id": "T1", "route_id": "R1", "stop_id": "S1",
                "stop_sequence": 1, "arrival_delay": 60, "arrival_time": None,
                "departure_delay": 65, "departure_time": None,
                "schedule_relationship": "SCHEDULED",
                "feed_timestamp": now, "recorded_at": now,
            },
            {
                "trip_id": "T1", "route_id": "R1", "stop_id": "S2",
                "stop_sequence": 2, "arrival_delay": 120, "arrival_time": None,
                "departure_delay": 125, "departure_time": None,
                "schedule_relationship": "SCHEDULED",
                "feed_timestamp": now, "recorded_at": now,
            },
        ]

        inserted = await writer.write_trip_updates(session, rows, "poll-1")
        assert inserted == 2
        session.execute.assert_called_once()
        session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_vehicle_positions(self) -> None:
        session = _make_session(rowcount=1)
        writer = GtfsRtWriter(batch_size=100)
        now = datetime.now(timezone.utc)

        rows = [
            {
                "vehicle_id": "V1", "trip_id": "T1", "route_id": "R1",
                "latitude": 49.28, "longitude": -123.12,
                "bearing": 90.0, "speed": 10.0,
                "current_stop_sequence": 3, "current_status": "STOPPED_AT",
                "feed_timestamp": now, "recorded_at": now,
            },
        ]

        inserted = await writer.write_vehicle_positions(session, rows, "poll-1")
        assert inserted == 1

    @pytest.mark.asyncio
    async def test_write_alerts(self) -> None:
        session = _make_session(rowcount=1)
        writer = GtfsRtWriter(batch_size=100)
        now = datetime.now(timezone.utc)

        rows = [
            {
                "alert_id": "A1", "cause": "TECHNICAL_PROBLEM",
                "effect": "SIGNIFICANT_DELAYS",
                "header_text": "Delays", "description_text": "Details",
                "active_period_start": 1700000000, "active_period_end": 1700003600,
                "informed_route_id": "R99", "informed_stop_id": "",
                "informed_trip_id": "",
                "feed_timestamp": now, "recorded_at": now,
            },
        ]

        inserted = await writer.write_alerts(session, rows, "poll-1")
        assert inserted == 1

    @pytest.mark.asyncio
    async def test_write_empty_rows(self) -> None:
        session = _make_session()
        writer = GtfsRtWriter()

        inserted = await writer.write_trip_updates(session, [], "poll-1")
        assert inserted == 0
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_batching(self) -> None:
        session = _make_session(rowcount=2)
        writer = GtfsRtWriter(batch_size=2)
        now = datetime.now(timezone.utc)

        rows = [
            {
                "trip_id": f"T{i}", "route_id": "R1", "stop_id": f"S{i}",
                "stop_sequence": 1, "arrival_delay": 0, "arrival_time": None,
                "departure_delay": 0, "departure_time": None,
                "schedule_relationship": "SCHEDULED",
                "feed_timestamp": now, "recorded_at": now,
            }
            for i in range(5)
        ]

        inserted = await writer.write_trip_updates(session, rows, "poll-1")
        # 3 batches: [2, 2, 1] -> rowcount=2 each call
        assert session.execute.call_count == 3
        assert session.commit.call_count == 3

    @pytest.mark.asyncio
    async def test_write_rollback_on_error(self) -> None:
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=Exception("DB error"))
        session.rollback = AsyncMock()
        writer = GtfsRtWriter()
        now = datetime.now(timezone.utc)

        rows = [
            {
                "trip_id": "T1", "route_id": "R1", "stop_id": "S1",
                "stop_sequence": 1, "arrival_delay": 0, "arrival_time": None,
                "departure_delay": 0, "departure_time": None,
                "schedule_relationship": "SCHEDULED",
                "feed_timestamp": now, "recorded_at": now,
            },
        ]

        with pytest.raises(Exception, match="DB error"):
            await writer.write_trip_updates(session, rows, "poll-1")

        session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_ingest_meta_success(self) -> None:
        session = _make_session()
        writer = GtfsRtWriter()

        await writer.update_ingest_meta(
            session,
            feed_type="trip_updates",
            status="ok",
            entity_count=42,
            feed_hash="abc123",
        )

        session.execute.assert_called_once()
        session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_ingest_meta_error(self) -> None:
        session = _make_session()
        writer = GtfsRtWriter()

        await writer.update_ingest_meta(
            session,
            feed_type="trip_updates",
            status="error",
            error_message="Connection refused",
        )

        session.execute.assert_called_once()
        # Verify params include error message
        call_args = session.execute.call_args
        params = call_args[0][1]
        assert params["status"] == "error"
        assert params["error_message"] == "Connection refused"
