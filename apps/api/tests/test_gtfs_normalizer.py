"""Tests for GtfsNormalizer and time parsing."""

from __future__ import annotations

import pytest

from transit_api.services.gtfs_static.normalizer import (
    GtfsNormalizer,
    NormalizationError,
    TimeParseError,
    parse_gtfs_time,
)


class TestParseGtfsTime:
    """Tests for GTFS time string parsing (supports >24h)."""

    def test_normal_time(self) -> None:
        assert parse_gtfs_time("08:30:00") == 30600

    def test_midnight(self) -> None:
        assert parse_gtfs_time("00:00:00") == 0

    def test_noon(self) -> None:
        assert parse_gtfs_time("12:00:00") == 43200

    def test_end_of_day(self) -> None:
        assert parse_gtfs_time("23:59:59") == 86399

    def test_past_midnight_24h(self) -> None:
        assert parse_gtfs_time("24:00:00") == 86400

    def test_past_midnight_25h(self) -> None:
        assert parse_gtfs_time("25:01:30") == 90090

    def test_past_midnight_30h(self) -> None:
        # Some agencies use up to 30+ hours
        assert parse_gtfs_time("30:00:00") == 108000

    def test_whitespace_stripped(self) -> None:
        assert parse_gtfs_time("  08:30:00  ") == 30600

    def test_invalid_format_no_colons(self) -> None:
        with pytest.raises(TimeParseError, match="Invalid GTFS time format"):
            parse_gtfs_time("083000")

    def test_invalid_format_too_few_parts(self) -> None:
        with pytest.raises(TimeParseError, match="Invalid GTFS time format"):
            parse_gtfs_time("08:30")

    def test_invalid_non_numeric(self) -> None:
        with pytest.raises(TimeParseError, match="Non-numeric"):
            parse_gtfs_time("ab:cd:ef")

    def test_invalid_minutes_over_59(self) -> None:
        with pytest.raises(TimeParseError, match="Invalid minutes"):
            parse_gtfs_time("08:60:00")

    def test_invalid_seconds_over_59(self) -> None:
        with pytest.raises(TimeParseError, match="Invalid minutes"):
            parse_gtfs_time("08:30:60")

    def test_negative_hours(self) -> None:
        with pytest.raises(TimeParseError, match="Negative hours"):
            parse_gtfs_time("-1:00:00")


class TestNormalizeStop:
    """Tests for stop normalization."""

    def test_valid_stop(self) -> None:
        row = {
            "stop_id": "50001",
            "stop_name": "Waterfront Station",
            "stop_lat": "49.285",
            "stop_lon": "-123.111",
        }
        result = GtfsNormalizer.normalize_stop(row)
        assert result["stop_id"] == "50001"
        assert result["name"] == "Waterfront Station"
        assert result["lat"] == 49.285
        assert result["lon"] == -123.111

    def test_whitespace_trimmed(self) -> None:
        row = {
            "stop_id": " 50001 ",
            "stop_name": " Waterfront ",
            "stop_lat": " 49.285 ",
            "stop_lon": " -123.111 ",
        }
        result = GtfsNormalizer.normalize_stop(row)
        assert result["stop_id"] == "50001"
        assert result["name"] == "Waterfront"

    def test_missing_stop_id_raises(self) -> None:
        row = {"stop_id": "", "stop_name": "Test", "stop_lat": "49", "stop_lon": "-123"}
        with pytest.raises(NormalizationError, match="Missing stop_id"):
            GtfsNormalizer.normalize_stop(row)

    def test_invalid_lat_raises(self) -> None:
        row = {"stop_id": "1", "stop_name": "Test", "stop_lat": "abc", "stop_lon": "-123"}
        with pytest.raises(NormalizationError, match="Invalid lat/lon"):
            GtfsNormalizer.normalize_stop(row)

    def test_missing_name_raises(self) -> None:
        row = {"stop_id": "1", "stop_name": "", "stop_lat": "49", "stop_lon": "-123"}
        with pytest.raises(NormalizationError, match="Missing stop_name"):
            GtfsNormalizer.normalize_stop(row)


class TestNormalizeRoute:
    """Tests for route normalization."""

    def test_valid_route(self) -> None:
        row = {"route_id": "001", "route_short_name": "1", "route_long_name": "Broadway"}
        result = GtfsNormalizer.normalize_route(row)
        assert result["route_id"] == "001"
        assert result["short_name"] == "1"
        assert result["long_name"] == "Broadway"

    def test_missing_route_id_raises(self) -> None:
        row = {"route_id": "", "route_short_name": "1", "route_long_name": "Broadway"}
        with pytest.raises(NormalizationError, match="Missing route_id"):
            GtfsNormalizer.normalize_route(row)

    def test_empty_short_name_allowed_if_long_name_present(self) -> None:
        row = {"route_id": "001", "route_short_name": "", "route_long_name": "Broadway"}
        result = GtfsNormalizer.normalize_route(row)
        assert result["short_name"] == ""
        assert result["long_name"] == "Broadway"

    def test_both_names_empty_raises(self) -> None:
        row = {"route_id": "001", "route_short_name": "", "route_long_name": ""}
        with pytest.raises(NormalizationError, match="Both short_name and long_name empty"):
            GtfsNormalizer.normalize_route(row)


class TestNormalizeTrip:
    """Tests for trip normalization."""

    def test_valid_trip(self) -> None:
        row = {"trip_id": "t1", "route_id": "r1", "service_id": "WD", "direction_id": "0"}
        result = GtfsNormalizer.normalize_trip(row)
        assert result["trip_id"] == "t1"
        assert result["direction_id"] == 0

    def test_direction_id_defaults_to_zero(self) -> None:
        row = {"trip_id": "t1", "route_id": "r1", "service_id": "WD", "direction_id": ""}
        result = GtfsNormalizer.normalize_trip(row)
        assert result["direction_id"] == 0

    def test_invalid_direction_id_defaults_to_zero(self) -> None:
        row = {"trip_id": "t1", "route_id": "r1", "service_id": "WD", "direction_id": "5"}
        result = GtfsNormalizer.normalize_trip(row)
        assert result["direction_id"] == 0

    def test_non_integer_direction_id_defaults_to_zero(self) -> None:
        row = {"trip_id": "t1", "route_id": "r1", "service_id": "WD", "direction_id": "abc"}
        result = GtfsNormalizer.normalize_trip(row)
        assert result["direction_id"] == 0

    def test_missing_trip_id_raises(self) -> None:
        row = {"trip_id": "", "route_id": "r1", "service_id": "WD"}
        with pytest.raises(NormalizationError, match="Missing trip_id"):
            GtfsNormalizer.normalize_trip(row)

    def test_missing_service_id_raises(self) -> None:
        row = {"trip_id": "t1", "route_id": "r1", "service_id": ""}
        with pytest.raises(NormalizationError, match="Missing service_id"):
            GtfsNormalizer.normalize_trip(row)


class TestNormalizeStopTime:
    """Tests for stop_time normalization."""

    def test_valid_stop_time(self) -> None:
        row = {
            "trip_id": "t1",
            "stop_id": "50001",
            "stop_sequence": "1",
            "arrival_time": "06:30:00",
        }
        result = GtfsNormalizer.normalize_stop_time(row)
        assert result["trip_id"] == "t1"
        assert result["stop_id"] == "50001"
        assert result["stop_sequence"] == 1
        assert result["sched_arrival_sec"] == 23400

    def test_past_midnight_time(self) -> None:
        row = {
            "trip_id": "t1",
            "stop_id": "50001",
            "stop_sequence": "1",
            "arrival_time": "25:01:30",
        }
        result = GtfsNormalizer.normalize_stop_time(row)
        assert result["sched_arrival_sec"] == 90090

    def test_missing_arrival_time_raises(self) -> None:
        row = {"trip_id": "t1", "stop_id": "50001", "stop_sequence": "1", "arrival_time": ""}
        with pytest.raises(NormalizationError, match="Missing arrival_time"):
            GtfsNormalizer.normalize_stop_time(row)

    def test_invalid_sequence_raises(self) -> None:
        row = {
            "trip_id": "t1",
            "stop_id": "50001",
            "stop_sequence": "abc",
            "arrival_time": "06:30:00",
        }
        with pytest.raises(NormalizationError, match="Invalid stop_sequence"):
            GtfsNormalizer.normalize_stop_time(row)

    def test_missing_stop_id_raises(self) -> None:
        row = {"trip_id": "t1", "stop_id": "", "stop_sequence": "1", "arrival_time": "06:30:00"}
        with pytest.raises(NormalizationError, match="Missing stop_id"):
            GtfsNormalizer.normalize_stop_time(row)
