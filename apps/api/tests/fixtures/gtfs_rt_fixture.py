"""Test fixtures for GTFS-RT protobuf data."""

from __future__ import annotations

import time

from google.transit import gtfs_realtime_pb2


def build_trip_update_feed(
    trip_id: str = "trip_001",
    route_id: str = "route_R1",
    stop_updates: list[dict] | None = None,
    feed_timestamp: int | None = None,
) -> bytes:
    """Build a serialized FeedMessage with a TripUpdate entity.

    Args:
        trip_id: The trip identifier.
        route_id: The route identifier.
        stop_updates: List of dicts with keys: stop_id, stop_sequence,
            arrival_delay, departure_delay.
        feed_timestamp: Unix timestamp for the feed header.

    Returns:
        Serialized protobuf bytes.
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = feed_timestamp or int(time.time())

    entity = feed.entity.add()
    entity.id = f"tu_{trip_id}"
    tu = entity.trip_update
    tu.trip.trip_id = trip_id
    tu.trip.route_id = route_id
    tu.trip.schedule_relationship = 0  # SCHEDULED

    if stop_updates is None:
        stop_updates = [
            {"stop_id": "stop_A", "stop_sequence": 1, "arrival_delay": 60, "departure_delay": 65},
            {"stop_id": "stop_B", "stop_sequence": 2, "arrival_delay": 120, "departure_delay": 125},
        ]

    for su in stop_updates:
        stu = tu.stop_time_update.add()
        stu.stop_id = su["stop_id"]
        stu.stop_sequence = su["stop_sequence"]
        stu.arrival.delay = su.get("arrival_delay", 0)
        stu.arrival.time = su.get("arrival_time", 0)
        stu.departure.delay = su.get("departure_delay", 0)
        stu.departure.time = su.get("departure_time", 0)

    return feed.SerializeToString()


def build_vehicle_position_feed(
    vehicle_id: str = "veh_001",
    trip_id: str = "trip_001",
    route_id: str = "route_R1",
    lat: float = 49.2827,
    lon: float = -123.1207,
    bearing: float = 90.0,
    speed: float = 12.5,
    feed_timestamp: int | None = None,
) -> bytes:
    """Build a serialized FeedMessage with a VehiclePosition entity."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = feed_timestamp or int(time.time())

    entity = feed.entity.add()
    entity.id = f"vp_{vehicle_id}"
    vp = entity.vehicle
    vp.vehicle.id = vehicle_id
    vp.trip.trip_id = trip_id
    vp.trip.route_id = route_id
    vp.position.latitude = lat
    vp.position.longitude = lon
    vp.position.bearing = bearing
    vp.position.speed = speed
    vp.current_stop_sequence = 3
    vp.current_status = 1  # STOPPED_AT

    return feed.SerializeToString()


def build_alert_feed(
    alert_id: str = "alert_001",
    cause: int = 3,  # TECHNICAL_PROBLEM
    effect: int = 3,  # SIGNIFICANT_DELAYS
    header: str = "Delay on Route 99",
    description: str = "Expect 10 min delays due to mechanical issue.",
    route_id: str = "route_99",
    stop_id: str = "",
    active_start: int | None = None,
    active_end: int | None = None,
    feed_timestamp: int | None = None,
) -> bytes:
    """Build a serialized FeedMessage with an Alert entity."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = feed_timestamp or int(time.time())

    entity = feed.entity.add()
    entity.id = alert_id
    alert = entity.alert
    alert.cause = cause
    alert.effect = effect

    ts = alert.header_text.translation.add()
    ts.text = header
    ts.language = "en"

    ds = alert.description_text.translation.add()
    ds.text = description
    ds.language = "en"

    if active_start or active_end:
        period = alert.active_period.add()
        if active_start:
            period.start = active_start
        if active_end:
            period.end = active_end

    ie = alert.informed_entity.add()
    ie.route_id = route_id
    if stop_id:
        ie.stop_id = stop_id

    return feed.SerializeToString()


def build_empty_feed(feed_timestamp: int | None = None) -> bytes:
    """Build an empty FeedMessage with no entities."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = feed_timestamp or int(time.time())
    return feed.SerializeToString()


def build_multi_entity_trip_update_feed(
    count: int = 5, feed_timestamp: int | None = None
) -> bytes:
    """Build a FeedMessage with multiple TripUpdate entities."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = feed_timestamp or int(time.time())

    for i in range(count):
        entity = feed.entity.add()
        entity.id = f"tu_trip_{i:03d}"
        tu = entity.trip_update
        tu.trip.trip_id = f"trip_{i:03d}"
        tu.trip.route_id = f"route_{i:03d}"

        stu = tu.stop_time_update.add()
        stu.stop_id = f"stop_{i:03d}"
        stu.stop_sequence = 1
        stu.arrival.delay = i * 30
        stu.departure.delay = i * 30 + 5

    return feed.SerializeToString()
