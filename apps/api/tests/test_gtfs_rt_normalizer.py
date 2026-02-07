"""Tests for GTFS-RT normalizer."""

import time

import pytest
from google.transit import gtfs_realtime_pb2

from transit_api.services.gtfs_rt.normalizer import GtfsRtNormalizer

from fixtures.gtfs_rt_fixture import (
    build_alert_feed,
    build_empty_feed,
    build_multi_entity_trip_update_feed,
    build_trip_update_feed,
    build_vehicle_position_feed,
)


def _decode(data: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)
    return feed


class TestNormalizeTripUpdates:
    """Unit tests for trip update normalization."""

    def test_basic_trip_update(self) -> None:
        ts = int(time.time())
        data = build_trip_update_feed(
            trip_id="T1", route_id="R1", feed_timestamp=ts,
            stop_updates=[
                {"stop_id": "S1", "stop_sequence": 1, "arrival_delay": 60, "departure_delay": 70},
            ],
        )
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_trip_updates(feed)

        assert len(rows) == 1
        row = rows[0]
        assert row["trip_id"] == "T1"
        assert row["route_id"] == "R1"
        assert row["stop_id"] == "S1"
        assert row["stop_sequence"] == 1
        assert row["arrival_delay"] == 60
        assert row["departure_delay"] == 70
        assert row["schedule_relationship"] == "SCHEDULED"
        assert row["feed_timestamp"] is not None
        assert row["recorded_at"] is not None

    def test_multiple_stop_updates(self) -> None:
        data = build_trip_update_feed(
            stop_updates=[
                {"stop_id": "A", "stop_sequence": 1, "arrival_delay": 10, "departure_delay": 15},
                {"stop_id": "B", "stop_sequence": 2, "arrival_delay": 20, "departure_delay": 25},
                {"stop_id": "C", "stop_sequence": 3, "arrival_delay": 30, "departure_delay": 35},
            ],
        )
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_trip_updates(feed)
        assert len(rows) == 3
        assert [r["stop_id"] for r in rows] == ["A", "B", "C"]

    def test_multi_entity_feed(self) -> None:
        data = build_multi_entity_trip_update_feed(count=5)
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_trip_updates(feed)
        assert len(rows) == 5

    def test_empty_feed_returns_empty(self) -> None:
        data = build_empty_feed()
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_trip_updates(feed)
        assert rows == []

    def test_no_timestamp_returns_empty(self) -> None:
        data = build_trip_update_feed(feed_timestamp=0)
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_trip_updates(feed)
        assert rows == []

    def test_skips_empty_trip_id(self) -> None:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())
        entity = feed.entity.add()
        entity.id = "tu_empty"
        tu = entity.trip_update
        tu.trip.trip_id = ""
        stu = tu.stop_time_update.add()
        stu.stop_id = "S1"
        stu.stop_sequence = 1

        rows = GtfsRtNormalizer.normalize_trip_updates(feed)
        assert rows == []

    def test_skips_empty_stop_id(self) -> None:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())
        entity = feed.entity.add()
        entity.id = "tu_no_stop"
        tu = entity.trip_update
        tu.trip.trip_id = "T1"
        stu = tu.stop_time_update.add()
        stu.stop_id = ""
        stu.stop_sequence = 1

        rows = GtfsRtNormalizer.normalize_trip_updates(feed)
        assert rows == []


class TestNormalizeVehiclePositions:
    """Unit tests for vehicle position normalization."""

    def test_basic_vehicle_position(self) -> None:
        data = build_vehicle_position_feed(
            vehicle_id="V1", trip_id="T1", route_id="R1",
            lat=49.28, lon=-123.12, bearing=180.0, speed=10.0,
        )
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_vehicle_positions(feed)

        assert len(rows) == 1
        row = rows[0]
        assert row["vehicle_id"] == "V1"
        assert row["trip_id"] == "T1"
        assert row["route_id"] == "R1"
        assert row["latitude"] == pytest.approx(49.28, abs=0.01)
        assert row["longitude"] == pytest.approx(-123.12, abs=0.01)
        assert row["bearing"] == pytest.approx(180.0)
        assert row["speed"] == pytest.approx(10.0)
        assert row["current_status"] == "STOPPED_AT"

    def test_skips_zero_lat_lon(self) -> None:
        data = build_vehicle_position_feed(lat=0.0, lon=0.0)
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_vehicle_positions(feed)
        assert rows == []

    def test_empty_feed_returns_empty(self) -> None:
        data = build_empty_feed()
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_vehicle_positions(feed)
        assert rows == []

    def test_no_timestamp_returns_empty(self) -> None:
        data = build_vehicle_position_feed(feed_timestamp=0)
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_vehicle_positions(feed)
        assert rows == []

    def test_skips_no_vehicle_id(self) -> None:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = int(time.time())
        entity = feed.entity.add()
        entity.id = "vp_empty"
        vp = entity.vehicle
        vp.position.latitude = 49.28
        vp.position.longitude = -123.12

        rows = GtfsRtNormalizer.normalize_vehicle_positions(feed)
        assert rows == []


class TestNormalizeAlerts:
    """Unit tests for alert normalization."""

    def test_basic_alert(self) -> None:
        ts = int(time.time())
        data = build_alert_feed(
            alert_id="A1", cause=3, effect=3,
            header="Test Alert", description="Details here",
            route_id="R99", active_start=ts - 3600, active_end=ts + 3600,
            feed_timestamp=ts,
        )
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_alerts(feed)

        assert len(rows) == 1
        row = rows[0]
        assert row["alert_id"] == "A1"
        assert row["cause"] == "TECHNICAL_PROBLEM"
        assert row["effect"] == "SIGNIFICANT_DELAYS"
        assert row["header_text"] == "Test Alert"
        assert row["description_text"] == "Details here"
        assert row["active_period_start"] == ts - 3600
        assert row["active_period_end"] == ts + 3600
        assert row["informed_route_id"] == "R99"

    def test_empty_feed_returns_empty(self) -> None:
        data = build_empty_feed()
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_alerts(feed)
        assert rows == []

    def test_no_timestamp_returns_empty(self) -> None:
        data = build_alert_feed(feed_timestamp=0)
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_alerts(feed)
        assert rows == []

    def test_alert_no_active_period(self) -> None:
        data = build_alert_feed(active_start=None, active_end=None)
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_alerts(feed)
        assert len(rows) == 1
        assert rows[0]["active_period_start"] is None
        assert rows[0]["active_period_end"] is None

    def test_alert_with_stop_id(self) -> None:
        data = build_alert_feed(route_id="R1", stop_id="S1")
        feed = _decode(data)
        rows = GtfsRtNormalizer.normalize_alerts(feed)
        assert len(rows) == 1
        assert rows[0]["informed_stop_id"] == "S1"
        assert rows[0]["informed_route_id"] == "R1"
