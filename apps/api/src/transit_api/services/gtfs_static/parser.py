"""GTFS CSV parser with strict column validation and streaming."""

from __future__ import annotations

import csv
from typing import TYPE_CHECKING, Any

from transit_api.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

    from transit_api.services.gtfs_static.reader import GtfsZipReader

logger = get_logger(__name__)

# Required columns per GTFS file (subset we need)
REQUIRED_COLUMNS: dict[str, set[str]] = {
    "stops.txt": {"stop_id", "stop_name", "stop_lat", "stop_lon"},
    "routes.txt": {"route_id", "route_short_name", "route_long_name"},
    "trips.txt": {"route_id", "service_id", "trip_id"},
    "stop_times.txt": {"trip_id", "arrival_time", "stop_id", "stop_sequence"},
}


class MissingColumnError(Exception):
    """Raised when a required CSV column is missing."""


class GtfsParser:
    """Parses GTFS CSV files with column validation and streaming iteration."""

    def __init__(self, reader: GtfsZipReader) -> None:
        self._reader = reader

    def parse_file(self, filename: str) -> Iterator[dict[str, Any]]:
        """Parse a GTFS CSV file, yielding one dict per row.

        Validates required columns on first read. Streams rows
        to avoid loading entire file into memory.

        Raises:
            MissingColumnError: If required columns are missing.
        """
        text_io = self._reader.open_file(filename)
        csv_reader = csv.DictReader(text_io)

        if csv_reader.fieldnames is None:
            msg = f"Empty CSV file: {filename}"
            raise MissingColumnError(msg)

        actual_columns = set(csv_reader.fieldnames)
        required = REQUIRED_COLUMNS.get(filename, set())
        missing = required - actual_columns
        if missing:
            msg = f"Missing required columns in {filename}: {sorted(missing)}"
            raise MissingColumnError(msg)

        extra_columns = actual_columns - required
        logger.info(
            "Parsing GTFS file",
            filename=filename,
            required_columns=sorted(required),
            extra_columns=sorted(extra_columns) if extra_columns else None,
        )

        yield from csv_reader

    def parse_stops(self) -> Iterator[dict[str, Any]]:
        """Parse stops.txt."""
        return self.parse_file("stops.txt")

    def parse_routes(self) -> Iterator[dict[str, Any]]:
        """Parse routes.txt."""
        return self.parse_file("routes.txt")

    def parse_trips(self) -> Iterator[dict[str, Any]]:
        """Parse trips.txt."""
        return self.parse_file("trips.txt")

    def parse_stop_times(self) -> Iterator[dict[str, Any]]:
        """Parse stop_times.txt."""
        return self.parse_file("stop_times.txt")
