"""Integration tests for Stage 5 matching engine with mocked DB sessions."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from transit_api.services.matching.engine import MatchingEngine


class FakeResult:
    """Fake SQLAlchemy result for testing."""

    def __init__(self, rows: List[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> List[tuple]:
        return self._rows


class FakeSession:
    """Fake async session that tracks executions and commits."""

    def __init__(
        self,
        rt_rows: Optional[List[tuple]] = None,
        schedule_rows: Optional[List[tuple]] = None,
    ) -> None:
        self.rt_rows = rt_rows or []
        self.schedule_rows = schedule_rows or []
        self.executed: List[Dict[str, Any]] = []
        self.commit_count = 0

    async def execute(self, sql: Any, params: Any = None) -> FakeResult:
        sql_str = str(sql)
        self.executed.append({"sql": sql_str, "params": params})

        if "FROM rt_trip_updates" in sql_str:
            return FakeResult(self.rt_rows)
        if "FROM stop_times" in sql_str:
            return FakeResult(self.schedule_rows)
        # INSERT (matched_arrivals)
        return FakeResult([])

    async def commit(self) -> None:
        self.commit_count += 1


def _ts(hour: int, minute: int = 0, day: int = 6) -> datetime:
    return datetime(2026, 2, day, hour, minute, 0, tzinfo=timezone.utc)


@pytest.fixture
def _mock_settings() -> Any:
    with patch("transit_api.services.matching.engine.get_settings") as mock:
        settings = MagicMock()
        settings.match_window_minutes = 90
        settings.match_max_candidates = 5
        settings.match_batch_size = 1000
        settings.match_strict_mode = False
        mock.return_value = settings
        yield settings


class TestMatchingIntegration:
    """Integration tests with FakeSession."""

    @pytest.mark.asyncio
    async def test_full_matching_flow(self, _mock_settings: Any) -> None:
        """Seed schedules + RT updates, run matching, verify report."""
        # RT trip updates (id, trip_id, stop_id, stop_seq, arr_delay, arr_time, sched_rel, feed_ts, rec_at)
        rt_rows = [
            (1, "T1", "S1", 1, 60, None, "SCHEDULED", _ts(8, 1), _ts(8, 1)),
            (2, "T1", "S2", 2, 120, None, "SCHEDULED", _ts(8, 12), _ts(8, 12)),
            (3, "T2", "S1", 1, -30, None, "SCHEDULED", _ts(9, 0), _ts(9, 0)),
        ]
        # Schedule: (trip_id, stop_id, stop_seq, sched_arrival_sec)
        schedule_rows = [
            ("T1", "S1", 1, 28800),  # 8:00
            ("T1", "S2", 2, 29400),  # 8:10
            ("T2", "S1", 1, 32400),  # 9:00
        ]

        session = FakeSession(rt_rows=rt_rows, schedule_rows=schedule_rows)
        engine = MatchingEngine(window_minutes=90, batch_size=1000, strict_mode=False)
        report = await engine.run(session=session)

        assert report.scanned_count == 3
        assert report.matched_count == 3
        assert report.unmatched_count == 0
        assert report.error_count == 0
        assert report.deduped_count == 0
        assert session.commit_count >= 1

    @pytest.mark.asyncio
    async def test_unmatched_trip(self, _mock_settings: Any) -> None:
        """RT update with no matching schedule -> unmatched."""
        rt_rows = [
            (1, "T_MISSING", "S1", 1, 0, None, "SCHEDULED", _ts(8, 0), _ts(8, 0)),
        ]
        schedule_rows: List[tuple] = []  # No matching schedules

        session = FakeSession(rt_rows=rt_rows, schedule_rows=schedule_rows)
        engine = MatchingEngine(window_minutes=90)
        report = await engine.run(session=session)

        assert report.scanned_count == 1
        assert report.unmatched_count == 1
        assert report.matched_count == 0

    @pytest.mark.asyncio
    async def test_dedup_multiple_updates(self, _mock_settings: Any) -> None:
        """Multiple RT updates for same key, only latest kept."""
        rt_rows = [
            (1, "T1", "S1", 1, 30, None, "SCHEDULED", _ts(8, 0), _ts(8, 0)),
            (2, "T1", "S1", 1, 60, None, "SCHEDULED", _ts(8, 5), _ts(8, 5)),
            (3, "T1", "S1", 1, 90, None, "SCHEDULED", _ts(8, 10), _ts(8, 10)),
        ]
        schedule_rows = [("T1", "S1", 1, 28800)]

        session = FakeSession(rt_rows=rt_rows, schedule_rows=schedule_rows)
        engine = MatchingEngine(window_minutes=90)
        report = await engine.run(session=session)

        assert report.scanned_count == 3
        assert report.deduped_count == 2
        assert report.matched_count == 1

    @pytest.mark.asyncio
    async def test_ambiguous_match(self, _mock_settings: Any) -> None:
        """Multiple schedule candidates -> ambiguous match."""
        rt_rows = [
            (1, "T1", "S1", 0, 0, None, "SCHEDULED", _ts(8, 0), _ts(8, 0)),
        ]
        schedule_rows = [
            ("T1", "S1", 1, 28800),
            ("T1", "S1", 5, 30000),
        ]

        session = FakeSession(rt_rows=rt_rows, schedule_rows=schedule_rows)
        engine = MatchingEngine(window_minutes=90)
        report = await engine.run(session=session)

        assert report.scanned_count == 1
        assert report.ambiguous_count == 1

    @pytest.mark.asyncio
    async def test_empty_rt_updates(self, _mock_settings: Any) -> None:
        """No RT updates -> report is all zeros."""
        session = FakeSession(rt_rows=[], schedule_rows=[])
        engine = MatchingEngine(window_minutes=90)
        report = await engine.run(session=session)

        assert report.scanned_count == 0
        assert report.matched_count == 0

    @pytest.mark.asyncio
    async def test_overnight_24xx_times(self, _mock_settings: Any) -> None:
        """Overnight trip at 25:30:00 correctly rolls back service_date."""
        # Feed comes in at 1:30 AM Feb 7 for a trip scheduled at 25:30:00 of Feb 6
        rt_rows = [
            (
                1,
                "T_NIGHT",
                "S1",
                1,
                0,
                None,
                "SCHEDULED",
                datetime(2026, 2, 7, 1, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 2, 7, 1, 30, 0, tzinfo=timezone.utc),
            ),
        ]
        schedule_rows = [
            ("T_NIGHT", "S1", 1, 91800),  # 25:30:00
        ]

        session = FakeSession(rt_rows=rt_rows, schedule_rows=schedule_rows)
        engine = MatchingEngine(window_minutes=180)
        report = await engine.run(session=session)

        assert report.matched_count == 1

    @pytest.mark.asyncio
    async def test_missing_keys_error_count(self, _mock_settings: Any) -> None:
        """RT updates with empty trip_id increment error/unmatched count."""
        rt_rows = [
            (1, "", "S1", 1, 0, None, "SCHEDULED", _ts(8, 0), _ts(8, 0)),
            (2, "T1", "", 1, 0, None, "SCHEDULED", _ts(8, 0), _ts(8, 0)),
        ]
        schedule_rows = [("T1", "S1", 1, 28800)]

        session = FakeSession(rt_rows=rt_rows, schedule_rows=schedule_rows)
        engine = MatchingEngine(window_minutes=90)
        report = await engine.run(session=session)

        # Both rows have missing keys -> should be unmatched
        assert report.unmatched_count == 2

    @pytest.mark.asyncio
    async def test_report_has_timing(self, _mock_settings: Any) -> None:
        """Report includes start/end timestamps and duration."""
        session = FakeSession(rt_rows=[], schedule_rows=[])
        engine = MatchingEngine(window_minutes=90)
        report = await engine.run(session=session)

        assert report.started_at
        assert report.ended_at
        assert report.duration_ms >= 0
        assert report.run_id

    @pytest.mark.asyncio
    async def test_idempotent_rerun(self, _mock_settings: Any) -> None:
        """Running matching twice produces consistent counts (ON CONFLICT)."""
        rt_rows = [
            (1, "T1", "S1", 1, 60, None, "SCHEDULED", _ts(8, 1), _ts(8, 1)),
        ]
        schedule_rows = [("T1", "S1", 1, 28800)]

        # First run
        session1 = FakeSession(rt_rows=rt_rows, schedule_rows=schedule_rows)
        engine1 = MatchingEngine(window_minutes=90)
        r1 = await engine1.run(session=session1)

        # Second run (same data)
        session2 = FakeSession(rt_rows=rt_rows, schedule_rows=schedule_rows)
        engine2 = MatchingEngine(window_minutes=90)
        r2 = await engine2.run(session=session2)

        # Both runs should produce the same matched count
        assert r1.matched_count == r2.matched_count
        assert r1.scanned_count == r2.scanned_count
