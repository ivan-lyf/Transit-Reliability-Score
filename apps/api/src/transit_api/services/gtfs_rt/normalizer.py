"""GTFS-RT normalizer: protobuf entities to DB-ready dicts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from transit_api.logging import get_logger

if TYPE_CHECKING:
    from google.transit import gtfs_realtime_pb2  # type: ignore[import-untyped]

logger = get_logger(__name__)

# Enum lookup maps
SCHEDULE_RELATIONSHIP = {
    0: "SCHEDULED",
    1: "ADDED",
    2: "UNSCHEDULED",
    3: "CANCELED",
    5: "REPLACEMENT",
}

VEHICLE_STOP_STATUS = {
    0: "INCOMING_AT",
    1: "STOPPED_AT",
    2: "IN_TRANSIT_TO",
}

CAUSE_MAP = {
    1: "UNKNOWN_CAUSE",
    2: "OTHER_CAUSE",
    3: "TECHNICAL_PROBLEM",
    4: "STRIKE",
    5: "DEMONSTRATION",
    6: "ACCIDENT",
    7: "HOLIDAY",
    8: "WEATHER",
    9: "MAINTENANCE",
    10: "CONSTRUCTION",
    11: "POLICE_ACTIVITY",
    12: "MEDICAL_EMERGENCY",
}

EFFECT_MAP = {
    1: "NO_SERVICE",
    2: "REDUCED_SERVICE",
    3: "SIGNIFICANT_DELAYS",
    4: "DETOUR",
    5: "ADDITIONAL_SERVICE",
    6: "MODIFIED_SERVICE",
    7: "OTHER_EFFECT",
    8: "UNKNOWN_EFFECT",
    9: "STOP_MOVED",
    10: "NO_EFFECT",
    11: "ACCESSIBILITY_ISSUE",
}


def _ts_to_dt(unix_ts: int) -> datetime:
    """Convert unix timestamp to timezone-aware datetime."""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc)


def _get_translation(translated_string: Any) -> str:
    """Extract first translation text from a TranslatedString, or empty string."""
    if translated_string and translated_string.translation:
        return str(translated_string.translation[0].text)
    return ""


class GtfsRtNormalizer:
    """Normalizes decoded GTFS-RT entities into flat dicts for DB insertion."""

    @staticmethod
    def normalize_trip_updates(
        feed: gtfs_realtime_pb2.FeedMessage,
    ) -> list[dict[str, Any]]:
        """Normalize TripUpdate entities into per-stop-update rows.

        Each StopTimeUpdate within a TripUpdate becomes its own row.

        Returns:
            List of dicts ready for rt_trip_updates table.
        """
        feed_ts = feed.header.timestamp
        if not feed_ts:
            return []

        feed_dt = _ts_to_dt(feed_ts)
        now = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            tu = entity.trip_update
            trip_id = tu.trip.trip_id if tu.trip.trip_id else ""
            route_id = tu.trip.route_id if tu.trip.route_id else ""
            sched_rel = SCHEDULE_RELATIONSHIP.get(tu.trip.schedule_relationship, "SCHEDULED")

            if not trip_id:
                continue

            for stu in tu.stop_time_update:
                stop_id = stu.stop_id if stu.stop_id else ""
                if not stop_id:
                    continue

                row = {
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "stop_id": stop_id,
                    "stop_sequence": stu.stop_sequence if stu.stop_sequence else 0,
                    "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                    "arrival_time": stu.arrival.time
                    if stu.HasField("arrival") and stu.arrival.time
                    else None,
                    "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
                    "departure_time": stu.departure.time
                    if stu.HasField("departure") and stu.departure.time
                    else None,
                    "schedule_relationship": sched_rel,
                    "feed_timestamp": feed_dt,
                    "recorded_at": now,
                }
                rows.append(row)

        return rows

    @staticmethod
    def normalize_vehicle_positions(
        feed: gtfs_realtime_pb2.FeedMessage,
    ) -> list[dict[str, Any]]:
        """Normalize VehiclePosition entities.

        Returns:
            List of dicts ready for rt_vehicle_positions table.
        """
        feed_ts = feed.header.timestamp
        if not feed_ts:
            return []

        feed_dt = _ts_to_dt(feed_ts)
        now = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []

        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue

            vp = entity.vehicle
            vehicle_id = ""
            if vp.HasField("vehicle"):
                vehicle_id = vp.vehicle.id if vp.vehicle.id else ""

            if not vehicle_id:
                continue

            trip_id = ""
            route_id = ""
            if vp.HasField("trip"):
                trip_id = vp.trip.trip_id if vp.trip.trip_id else ""
                route_id = vp.trip.route_id if vp.trip.route_id else ""

            lat = vp.position.latitude if vp.HasField("position") else 0.0
            lon = vp.position.longitude if vp.HasField("position") else 0.0

            if lat == 0.0 and lon == 0.0:
                continue

            bearing = (
                vp.position.bearing if vp.HasField("position") and vp.position.bearing else None
            )
            speed = vp.position.speed if vp.HasField("position") and vp.position.speed else None
            stop_seq = vp.current_stop_sequence if vp.current_stop_sequence else None
            status = VEHICLE_STOP_STATUS.get(vp.current_status, "")

            rows.append(
                {
                    "vehicle_id": vehicle_id,
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "latitude": lat,
                    "longitude": lon,
                    "bearing": bearing,
                    "speed": speed,
                    "current_stop_sequence": stop_seq,
                    "current_status": status,
                    "feed_timestamp": feed_dt,
                    "recorded_at": now,
                }
            )

        return rows

    @staticmethod
    def normalize_alerts(
        feed: gtfs_realtime_pb2.FeedMessage,
    ) -> list[dict[str, Any]]:
        """Normalize Alert entities.

        One row per (alert, informed_entity) combination.

        Returns:
            List of dicts ready for rt_alerts table.
        """
        feed_ts = feed.header.timestamp
        if not feed_ts:
            return []

        feed_dt = _ts_to_dt(feed_ts)
        now = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []

        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue

            alert = entity.alert
            alert_id = entity.id if entity.id else ""

            cause = CAUSE_MAP.get(alert.cause, "UNKNOWN_CAUSE")
            effect = EFFECT_MAP.get(alert.effect, "UNKNOWN_EFFECT")
            header = _get_translation(alert.header_text)
            description = _get_translation(alert.description_text)

            # Active periods
            period_start = None
            period_end = None
            if alert.active_period:
                period_start = (
                    alert.active_period[0].start if alert.active_period[0].start else None
                )
                period_end = alert.active_period[0].end if alert.active_period[0].end else None

            # Expand per informed entity (or single row if none)
            informed_entities = list(alert.informed_entity) if alert.informed_entity else [None]

            for ie in informed_entities:
                route_id = ""
                stop_id = ""
                trip_id = ""
                if ie is not None:
                    route_id = ie.route_id if ie.route_id else ""
                    stop_id = ie.stop_id if ie.stop_id else ""
                    if ie.HasField("trip"):
                        trip_id = ie.trip.trip_id if ie.trip.trip_id else ""

                rows.append(
                    {
                        "alert_id": alert_id,
                        "cause": cause,
                        "effect": effect,
                        "header_text": header,
                        "description_text": description,
                        "active_period_start": period_start,
                        "active_period_end": period_end,
                        "informed_route_id": route_id,
                        "informed_stop_id": stop_id,
                        "informed_trip_id": trip_id,
                        "feed_timestamp": feed_dt,
                        "recorded_at": now,
                    }
                )

        return rows
