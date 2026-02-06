"""Tests for GtfsParser - CSV column validation and streaming."""

from __future__ import annotations

import pytest

from transit_api.services.gtfs_static.parser import GtfsParser, MissingColumnError
from transit_api.services.gtfs_static.reader import GtfsZipReader

from .fixtures.gtfs_fixture import build_gtfs_zip


class TestGtfsParser:
    """Tests for CSV parsing and column validation."""

    def test_parse_stops_yields_rows(self) -> None:
        zip_bytes = build_gtfs_zip()
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)
            rows = list(parser.parse_stops())

        assert len(rows) == 3
        assert rows[0]["stop_id"] == "50001"
        assert rows[0]["stop_name"] == "Waterfront Station"

    def test_parse_routes_yields_rows(self) -> None:
        zip_bytes = build_gtfs_zip()
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)
            rows = list(parser.parse_routes())

        assert len(rows) == 2
        assert rows[0]["route_id"] == "001"
        assert rows[0]["route_short_name"] == "1"

    def test_parse_trips_yields_rows(self) -> None:
        zip_bytes = build_gtfs_zip()
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)
            rows = list(parser.parse_trips())

        assert len(rows) == 3
        assert rows[0]["trip_id"] == "trip-001-001"

    def test_parse_stop_times_yields_rows(self) -> None:
        zip_bytes = build_gtfs_zip()
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)
            rows = list(parser.parse_stop_times())

        assert len(rows) == 8
        assert rows[0]["trip_id"] == "trip-001-001"
        assert rows[0]["arrival_time"] == "06:30:00"

    def test_missing_required_column_raises(self) -> None:
        bad_stops = "stop_id,stop_name\n50001,Test\n"  # missing stop_lat, stop_lon
        zip_bytes = build_gtfs_zip(stops=bad_stops)
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)
            with pytest.raises(MissingColumnError, match="stop_lat"):
                list(parser.parse_stops())

    def test_empty_csv_parses_zero_rows(self) -> None:
        empty_stops = "stop_id,stop_name,stop_lat,stop_lon\n"
        zip_bytes = build_gtfs_zip(stops=empty_stops)
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)
            rows = list(parser.parse_stops())
        assert len(rows) == 0

    def test_extra_columns_accepted(self) -> None:
        zip_bytes = build_gtfs_zip()  # fixture has extra columns like stop_code
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)
            rows = list(parser.parse_stops())
        # Extra columns should be present in the dict
        assert "stop_code" in rows[0]

    def test_stop_times_missing_column_raises(self) -> None:
        bad_st = "trip_id,stop_id\ntrip-001,50001\n"  # missing arrival_time, stop_sequence
        zip_bytes = build_gtfs_zip(stop_times=bad_st)
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)
            with pytest.raises(MissingColumnError, match="arrival_time"):
                list(parser.parse_stop_times())
