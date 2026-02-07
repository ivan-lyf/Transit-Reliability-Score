"""Unit tests for MatchingEngine._match_single and internal methods."""

from datetime import datetime, timezone
from unittest.mock import patch

from transit_api.services.matching.engine import MatchingEngine


def _make_engine(**kwargs):  # type: ignore[no-untyped-def]
    """Create an engine with test defaults."""
    defaults = {
        "window_minutes": 90,
        "max_candidates": 5,
        "batch_size": 1000,
        "strict_mode": False,
    }
    defaults.update(kwargs)
    with patch("transit_api.services.matching.engine.get_settings") as mock_settings:
        settings = mock_settings.return_value
        settings.match_window_minutes = defaults["window_minutes"]
        settings.match_max_candidates = defaults["max_candidates"]
        settings.match_batch_size = defaults["batch_size"]
        settings.match_strict_mode = defaults["strict_mode"]
        return MatchingEngine(**defaults)


class TestMatchSingle:
    """Tests for _match_single method."""

    def test_exact_match_with_stop_sequence(self) -> None:
        """Match by trip_id + stop_id + stop_sequence."""
        engine = _make_engine()
        rt_row = {
            "id": 1,
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 3,
            "arrival_time": None,
            "arrival_delay": 60,
            "feed_timestamp": datetime(2026, 2, 6, 8, 1, 0, tzinfo=timezone.utc),
        }
        schedule_map = {
            ("T1", "S1"): [
                {"stop_sequence": 1, "sched_arrival_sec": 28800},  # 8:00
                {"stop_sequence": 3, "sched_arrival_sec": 29400},  # 8:10
                {"stop_sequence": 5, "sched_arrival_sec": 30000},  # 8:20
            ]
        }
        result = engine._match_single(rt_row, schedule_map)
        assert result is not None
        assert result["stop_sequence"] == 3
        assert result["match_status"] == "ambiguous"  # 3 candidates
        assert result["delay_sec"] == 60  # scheduled 8:10, observed 8:11

    def test_tiebreak_lowest_stop_sequence(self) -> None:
        """Without stop_sequence in RT, pick lowest from schedule."""
        engine = _make_engine()
        rt_row = {
            "id": 1,
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 0,  # unknown
            "arrival_time": None,
            "arrival_delay": 120,
            "feed_timestamp": datetime(2026, 2, 6, 8, 2, 0, tzinfo=timezone.utc),
        }
        schedule_map = {
            ("T1", "S1"): [
                {"stop_sequence": 5, "sched_arrival_sec": 30000},
                {"stop_sequence": 2, "sched_arrival_sec": 28800},
            ]
        }
        result = engine._match_single(rt_row, schedule_map)
        assert result is not None
        assert result["stop_sequence"] == 2  # lowest

    def test_unmatched_no_candidates(self) -> None:
        """No schedule candidates -> unmatched."""
        engine = _make_engine()
        rt_row = {
            "id": 1,
            "trip_id": "T_UNKNOWN",
            "stop_id": "S1",
            "stop_sequence": 1,
            "arrival_time": None,
            "arrival_delay": None,
            "feed_timestamp": datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc),
        }
        result = engine._match_single(rt_row, {})
        assert result is not None
        assert result["match_status"] == "unmatched"
        assert result["match_confidence"] == 0.0

    def test_missing_trip_id_returns_none(self) -> None:
        """Missing trip_id -> skip (return None)."""
        engine = _make_engine()
        rt_row = {
            "id": 1,
            "trip_id": "",
            "stop_id": "S1",
            "stop_sequence": 1,
            "arrival_time": None,
            "arrival_delay": None,
            "feed_timestamp": datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc),
        }
        result = engine._match_single(rt_row, {})
        assert result is None

    def test_missing_stop_id_returns_none(self) -> None:
        """Missing stop_id -> skip (return None)."""
        engine = _make_engine()
        rt_row = {
            "id": 1,
            "trip_id": "T1",
            "stop_id": "",
            "stop_sequence": 1,
            "arrival_time": None,
            "arrival_delay": None,
            "feed_timestamp": datetime(2026, 2, 6, 8, 0, 0, tzinfo=timezone.utc),
        }
        result = engine._match_single(rt_row, {})
        assert result is None

    def test_overnight_24xx_service_date(self) -> None:
        """Trip at 25:30:00: service_date rolled back one day."""
        engine = _make_engine()
        rt_row = {
            "id": 1,
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 1,
            "arrival_time": None,
            "arrival_delay": 0,
            "feed_timestamp": datetime(2026, 2, 7, 1, 30, 0, tzinfo=timezone.utc),
        }
        schedule_map = {
            ("T1", "S1"): [
                {"stop_sequence": 1, "sched_arrival_sec": 91800},  # 25:30:00
            ]
        }
        result = engine._match_single(rt_row, schedule_map)
        assert result is not None
        from datetime import date

        assert result["service_date"] == date(2026, 2, 6)
        assert result["match_status"] == "matched"
        assert result["delay_sec"] == 0

    def test_single_candidate_matched(self) -> None:
        """Single candidate -> match_status='matched', confidence=1.0."""
        engine = _make_engine()
        rt_row = {
            "id": 1,
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 1,
            "arrival_time": None,
            "arrival_delay": 30,
            "feed_timestamp": datetime(2026, 2, 6, 8, 0, 30, tzinfo=timezone.utc),
        }
        schedule_map = {
            ("T1", "S1"): [
                {"stop_sequence": 1, "sched_arrival_sec": 28800},
            ]
        }
        result = engine._match_single(rt_row, schedule_map)
        assert result is not None
        assert result["match_status"] == "matched"
        assert result["match_confidence"] == 1.0

    def test_strict_mode_rejects_ambiguous(self) -> None:
        """Strict mode: multiple candidates -> unmatched."""
        engine = _make_engine(strict_mode=True)
        rt_row = {
            "id": 1,
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 0,
            "arrival_time": None,
            "arrival_delay": 60,
            "feed_timestamp": datetime(2026, 2, 6, 8, 1, 0, tzinfo=timezone.utc),
        }
        schedule_map = {
            ("T1", "S1"): [
                {"stop_sequence": 1, "sched_arrival_sec": 28800},
                {"stop_sequence": 2, "sched_arrival_sec": 29400},
            ]
        }
        result = engine._match_single(rt_row, schedule_map)
        assert result is not None
        assert result["match_status"] == "unmatched"
        assert result["match_confidence"] == 0.0

    def test_max_candidates_limit(self) -> None:
        """Only first max_candidates are considered."""
        engine = _make_engine(max_candidates=2)
        rt_row = {
            "id": 1,
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 5,  # matches 3rd candidate, but capped
            "arrival_time": None,
            "arrival_delay": 0,
            "feed_timestamp": datetime(2026, 2, 6, 9, 0, 0, tzinfo=timezone.utc),
        }
        schedule_map = {
            ("T1", "S1"): [
                {"stop_sequence": 1, "sched_arrival_sec": 28800},
                {"stop_sequence": 3, "sched_arrival_sec": 29400},
                {"stop_sequence": 5, "sched_arrival_sec": 30000},
            ]
        }
        result = engine._match_single(rt_row, schedule_map)
        assert result is not None
        # stop_sequence 5 not in capped list, falls back to lowest
        assert result["stop_sequence"] == 1

    def test_deterministic_output(self) -> None:
        """Same input -> same output every time."""
        engine = _make_engine()
        rt_row = {
            "id": 1,
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 0,
            "arrival_time": None,
            "arrival_delay": 30,
            "feed_timestamp": datetime(2026, 2, 6, 8, 0, 30, tzinfo=timezone.utc),
        }
        schedule_map = {
            ("T1", "S1"): [
                {"stop_sequence": 2, "sched_arrival_sec": 29400},
                {"stop_sequence": 1, "sched_arrival_sec": 28800},
            ]
        }
        r1 = engine._match_single(rt_row, schedule_map)
        r2 = engine._match_single(rt_row, schedule_map)
        assert r1 == r2
