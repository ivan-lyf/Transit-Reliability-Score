"""GTFS data normalizer - cleans and converts raw CSV rows."""

from __future__ import annotations

from typing import Any

from transit_api.logging import get_logger

logger = get_logger(__name__)


class TimeParseError(Exception):
    """Raised when a GTFS time string cannot be parsed."""


class NormalizationError(Exception):
    """Raised when a row cannot be normalized."""


class GtfsNormalizer:
    """Normalizes raw GTFS CSV rows into database-ready dicts."""

    @staticmethod
    def normalize_stop(row: dict[str, Any]) -> dict[str, Any]:
        """Normalize a stops.txt row.

        Returns:
            Dict with keys: stop_id, name, lat, lon.

        Raises:
            NormalizationError: If required fields are missing/invalid.
        """
        stop_id = _clean_str(row.get("stop_id", ""))
        name = _clean_str(row.get("stop_name", ""))
        lat_str = _clean_str(row.get("stop_lat", ""))
        lon_str = _clean_str(row.get("stop_lon", ""))

        if not stop_id:
            raise NormalizationError("Missing stop_id")
        if not name:
            raise NormalizationError(f"Missing stop_name for stop_id={stop_id}")

        try:
            lat = float(lat_str)
            lon = float(lon_str)
        except (ValueError, TypeError) as exc:
            raise NormalizationError(
                f"Invalid lat/lon for stop_id={stop_id}: lat={lat_str!r}, lon={lon_str!r}"
            ) from exc

        return {"stop_id": stop_id, "name": name, "lat": lat, "lon": lon}

    @staticmethod
    def normalize_route(row: dict[str, Any]) -> dict[str, Any]:
        """Normalize a routes.txt row.

        Returns:
            Dict with keys: route_id, short_name, long_name.

        Raises:
            NormalizationError: If required fields are missing.
        """
        route_id = _clean_str(row.get("route_id", ""))
        short_name = _clean_str(row.get("route_short_name", ""))
        long_name = _clean_str(row.get("route_long_name", ""))

        if not route_id:
            raise NormalizationError("Missing route_id")
        # GTFS allows empty short_name or long_name, but at least one should be present
        if not short_name and not long_name:
            raise NormalizationError(f"Both short_name and long_name empty for route_id={route_id}")

        # Default empty names to empty string (schema requires NOT NULL)
        if not short_name:
            short_name = ""
        if not long_name:
            long_name = ""

        return {"route_id": route_id, "short_name": short_name, "long_name": long_name}

    @staticmethod
    def normalize_trip(row: dict[str, Any]) -> dict[str, Any]:
        """Normalize a trips.txt row.

        Returns:
            Dict with keys: trip_id, route_id, service_id, direction_id.

        Raises:
            NormalizationError: If required fields are missing.
        """
        trip_id = _clean_str(row.get("trip_id", ""))
        route_id = _clean_str(row.get("route_id", ""))
        service_id = _clean_str(row.get("service_id", ""))
        direction_id_str = _clean_str(row.get("direction_id", ""))

        if not trip_id:
            raise NormalizationError("Missing trip_id")
        if not route_id:
            raise NormalizationError(f"Missing route_id for trip_id={trip_id}")
        if not service_id:
            raise NormalizationError(f"Missing service_id for trip_id={trip_id}")

        # direction_id is optional in GTFS, default to 0
        direction_id = 0
        if direction_id_str:
            try:
                direction_id = int(direction_id_str)
                if direction_id not in (0, 1):
                    logger.warning(
                        "Invalid direction_id, defaulting to 0",
                        trip_id=trip_id,
                        direction_id=direction_id_str,
                    )
                    direction_id = 0
            except ValueError:
                logger.warning(
                    "Non-integer direction_id, defaulting to 0",
                    trip_id=trip_id,
                    direction_id=direction_id_str,
                )
                direction_id = 0

        return {
            "trip_id": trip_id,
            "route_id": route_id,
            "service_id": service_id,
            "direction_id": direction_id,
        }

    @staticmethod
    def normalize_stop_time(row: dict[str, Any]) -> dict[str, Any]:
        """Normalize a stop_times.txt row.

        Converts GTFS time (HH:MM:SS, may be >24:00:00) to seconds from midnight.

        Returns:
            Dict with keys: trip_id, stop_id, stop_sequence, sched_arrival_sec.

        Raises:
            NormalizationError: If required fields are missing/invalid.
        """
        trip_id = _clean_str(row.get("trip_id", ""))
        stop_id = _clean_str(row.get("stop_id", ""))
        seq_str = _clean_str(row.get("stop_sequence", ""))
        arrival_str = _clean_str(row.get("arrival_time", ""))

        if not trip_id:
            raise NormalizationError("Missing trip_id in stop_times")
        if not stop_id:
            raise NormalizationError(f"Missing stop_id in stop_times for trip_id={trip_id}")
        if not seq_str:
            raise NormalizationError(
                f"Missing stop_sequence for trip_id={trip_id}, stop_id={stop_id}"
            )

        try:
            stop_sequence = int(seq_str)
        except ValueError as exc:
            raise NormalizationError(
                f"Invalid stop_sequence={seq_str!r} for trip_id={trip_id}"
            ) from exc

        if not arrival_str:
            raise NormalizationError(
                f"Missing arrival_time for trip_id={trip_id}, stop_sequence={stop_sequence}"
            )

        sched_arrival_sec = parse_gtfs_time(arrival_str)

        return {
            "trip_id": trip_id,
            "stop_id": stop_id,
            "stop_sequence": stop_sequence,
            "sched_arrival_sec": sched_arrival_sec,
        }


def parse_gtfs_time(time_str: str) -> int:
    """Parse a GTFS time string (HH:MM:SS) to seconds from midnight.

    Supports times >= 24:00:00 for trips spanning past midnight.

    Examples:
        "08:30:00" -> 30600
        "25:01:30" -> 90090

    Raises:
        TimeParseError: If the format is invalid.
    """
    time_str = time_str.strip()
    parts = time_str.split(":")
    if len(parts) != 3:
        msg = f"Invalid GTFS time format: {time_str!r} (expected HH:MM:SS)"
        raise TimeParseError(msg)

    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
    except ValueError as exc:
        msg = f"Non-numeric components in GTFS time: {time_str!r}"
        raise TimeParseError(msg) from exc

    if minutes < 0 or minutes > 59 or seconds < 0 or seconds > 59:
        msg = f"Invalid minutes/seconds in GTFS time: {time_str!r}"
        raise TimeParseError(msg)

    if hours < 0:
        msg = f"Negative hours in GTFS time: {time_str!r}"
        raise TimeParseError(msg)

    return hours * 3600 + minutes * 60 + seconds


def _clean_str(value: Any) -> str:
    """Trim whitespace from a value, return empty string for None."""
    if value is None:
        return ""
    return str(value).strip()
