"""Tests for GTFS-RT protobuf decoder."""

import pytest

from transit_api.services.gtfs_rt.decoder import DecodeError_, GtfsRtDecoder

from .fixtures.gtfs_rt_fixture import (
    build_alert_feed,
    build_empty_feed,
    build_multi_entity_trip_update_feed,
    build_trip_update_feed,
    build_vehicle_position_feed,
)


class TestGtfsRtDecoder:
    """Unit tests for GtfsRtDecoder."""

    def test_decode_trip_update_feed(self) -> None:
        data = build_trip_update_feed(feed_timestamp=1700000000)
        feed = GtfsRtDecoder.decode(data, "trip_updates", "poll-1")
        assert feed.header.timestamp == 1700000000
        assert len(feed.entity) == 1
        assert feed.entity[0].trip_update.trip.trip_id == "trip_001"

    def test_decode_vehicle_position_feed(self) -> None:
        data = build_vehicle_position_feed(feed_timestamp=1700000000)
        feed = GtfsRtDecoder.decode(data, "vehicle_positions", "poll-1")
        assert len(feed.entity) == 1
        vp = feed.entity[0].vehicle
        assert vp.vehicle.id == "veh_001"
        assert vp.position.latitude == pytest.approx(49.2827, abs=0.001)

    def test_decode_alert_feed(self) -> None:
        data = build_alert_feed(feed_timestamp=1700000000)
        feed = GtfsRtDecoder.decode(data, "service_alerts", "poll-1")
        assert len(feed.entity) == 1
        assert feed.entity[0].alert.cause == 3

    def test_decode_empty_feed(self) -> None:
        data = build_empty_feed(feed_timestamp=1700000000)
        feed = GtfsRtDecoder.decode(data, "trip_updates", "poll-1")
        assert len(feed.entity) == 0

    def test_decode_multi_entity(self) -> None:
        data = build_multi_entity_trip_update_feed(count=10, feed_timestamp=1700000000)
        feed = GtfsRtDecoder.decode(data, "trip_updates", "poll-1")
        assert len(feed.entity) == 10

    def test_decode_invalid_protobuf_raises(self) -> None:
        with pytest.raises(DecodeError_):
            GtfsRtDecoder.decode(b"not a protobuf", "trip_updates", "poll-1")

    def test_decode_empty_bytes_succeeds(self) -> None:
        # Empty bytes is technically a valid (but empty) protobuf
        feed = GtfsRtDecoder.decode(b"", "trip_updates", "poll-1")
        assert len(feed.entity) == 0

    def test_get_feed_timestamp(self) -> None:
        data = build_trip_update_feed(feed_timestamp=1700000000)
        feed = GtfsRtDecoder.decode(data, "trip_updates", "poll-1")
        assert GtfsRtDecoder.get_feed_timestamp(feed) == 1700000000

    def test_get_feed_timestamp_unset(self) -> None:
        data = build_trip_update_feed(feed_timestamp=0)
        feed = GtfsRtDecoder.decode(data, "trip_updates", "poll-1")
        assert GtfsRtDecoder.get_feed_timestamp(feed) == 0

    def test_get_entity_count(self) -> None:
        data = build_multi_entity_trip_update_feed(count=7, feed_timestamp=1700000000)
        feed = GtfsRtDecoder.decode(data, "trip_updates", "poll-1")
        assert GtfsRtDecoder.get_entity_count(feed) == 7
