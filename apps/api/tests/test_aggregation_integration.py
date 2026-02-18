"""Integration tests for Stage 6 aggregation engine.

Requires a real PostgreSQL database:
    RUN_INTEGRATION_TESTS=1
    DATABASE_URL=postgresql+asyncpg://...

All tests use a freshly created schema (drop_all → create_all) so they are
safe to run against a dedicated test / dev database.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from unittest.mock import MagicMock, patch

from transit_api.models import Base
from transit_api.services.aggregation.engine import _AGG_SQL, run_aggregation

RUN_INTEGRATION = os.getenv("RUN_INTEGRATION_TESTS") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")

if not RUN_INTEGRATION or not DATABASE_URL:
    pytest.skip(
        "Integration tests require RUN_INTEGRATION_TESTS=1 and DATABASE_URL to be set",
        allow_module_level=True,
    )

# ---------------------------------------------------------------------------
# UTC-hour constants for Pacific Time bucket coverage
#
# These UTC hours land in the corresponding local (Pacific) bucket for BOTH
# PST (UTC-8) and PDT (UTC-7), making the tests DST-safe:
#
#   UTC 15 → PST 07:00 / PDT 08:00  → bucket '6-9'
#   UTC 18 → PST 10:00 / PDT 11:00  → bucket '9-12'
#   UTC 21 → PST 13:00 / PDT 14:00  → bucket '12-15'
# ---------------------------------------------------------------------------
_UTC_HOUR_6_9 = 15    # PST 07 / PDT 08
_UTC_HOUR_9_12 = 18   # PST 10 / PDT 11
_UTC_HOUR_12_15 = 21  # PST 13 / PDT 14


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sf(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


def _sched_ts(d: date, utc_hour: int, utc_minute: int = 0) -> datetime:
    """Return a UTC datetime with the given hour/minute on date d."""
    return datetime(d.year, d.month, d.day, utc_hour, utc_minute, tzinfo=timezone.utc)


def _recent_weekday() -> date:
    """Return the most recent Mon–Fri date (≤ today)."""
    d = date.today()
    while d.weekday() >= 5:       # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d


def _recent_saturday() -> date:
    """Return the most recent Saturday (≤ today)."""
    d = date.today()
    while d.weekday() != 5:
        d -= timedelta(days=1)
    return d


def _test_settings() -> MagicMock:
    """Minimal Settings substitute for run_aggregation()."""
    s = MagicMock()
    s.agg_lookback_days = 30
    s.service_timezone = "America/Vancouver"
    s.on_time_threshold_sec = 120
    s.agg_batch_size = 1000
    s.weight_on_time_rate = 0.6
    s.weight_p95_component = 0.25
    s.weight_p50_component = 0.15
    s.p95_max_delay_sec = 900
    s.p50_max_delay_sec = 300
    return s


def _session_patcher(sf: async_sessionmaker[AsyncSession]) -> Any:
    """Return a drop-in replacement for get_session_context() that uses sf."""

    @asynccontextmanager
    async def _ctx():  # type: ignore[return]
        async with sf() as session:
            yield session

    return _ctx


async def _seed_reference_data(sf: async_sessionmaker[AsyncSession]) -> None:
    """Insert minimal stops, routes, and trips required for FK satisfaction."""
    async with sf() as session:
        await session.execute(
            text(
                "INSERT INTO stops (stop_id, name, lat, lon) VALUES "
                "('S1', 'Stop 1', 49.2800, -123.1200), "
                "('S2', 'Stop 2', 49.2900, -123.1300)"
            )
        )
        await session.execute(
            text(
                "INSERT INTO routes (route_id, short_name, long_name) VALUES "
                "('R1', '99', 'Broadway'), "
                "('R2', '25', 'Brentwood')"
            )
        )
        await session.execute(
            text(
                "INSERT INTO trips (trip_id, route_id, service_id, direction_id) VALUES "
                "('T1', 'R1', 'SVC', 0), "
                "('T2', 'R2', 'SVC', 0)"
            )
        )
        await session.commit()


async def _insert_arrivals(
    sf: async_sessionmaker[AsyncSession],
    trip_id: str,
    stop_id: str,
    service_date: date,
    scheduled_ts: datetime,
    delays: list[int],
    match_status: str = "matched",
    stop_seq_offset: int = 0,
) -> None:
    """Insert matched_arrivals rows with the given delay values."""
    rows = [
        {
            "trip_id": trip_id,
            "stop_id": stop_id,
            "stop_sequence": stop_seq_offset + i + 1,
            "service_date": service_date,
            "scheduled_ts": scheduled_ts,
            "delay_sec": d,
            "match_status": match_status,
        }
        for i, d in enumerate(delays)
    ]
    async with sf() as session:
        await session.execute(
            text("""
                INSERT INTO matched_arrivals
                    (trip_id, stop_id, stop_sequence, service_date, scheduled_ts,
                     observed_ts, delay_sec, match_status, match_confidence,
                     source_feed_ts, created_at)
                VALUES
                    (:trip_id, :stop_id, :stop_sequence, :service_date, :scheduled_ts,
                     :scheduled_ts, :delay_sec, :match_status, 1.0,
                     :scheduled_ts, NOW())
            """),
            rows,
        )
        await session.commit()


async def _count(sf: async_sessionmaker[AsyncSession], table: str) -> int:
    async with sf() as session:
        result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))  # noqa: S608
        return int(result.scalar_one())


async def _fetch_score_agg(
    sf: async_sessionmaker[AsyncSession],
    stop_id: str,
    route_id: str,
) -> list[Any]:
    async with sf() as session:
        result = await session.execute(
            text(
                "SELECT stop_id, route_id, day_type, hour_bucket, "
                "       on_time_rate::float AS on_time_rate, "
                "       p50_delay_sec, p95_delay_sec, score, sample_n "
                "FROM score_agg "
                "WHERE stop_id = :sid AND route_id = :rid "
                "ORDER BY day_type, hour_bucket"
            ),
            {"sid": stop_id, "rid": route_id},
        )
        return result.fetchall()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_PATCH_TARGET = "transit_api.services.aggregation.engine.get_session_context"


@pytest_asyncio.fixture
async def db_engine() -> AsyncEngine:  # type: ignore[misc]
    """Fresh schema for each test function."""
    engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine  # type: ignore[misc]
    await engine.dispose()


# ---------------------------------------------------------------------------
# Tests: aggregation correctness
# ---------------------------------------------------------------------------


class TestAggregationIntegration:
    """End-to-end aggregation tests against a real PostgreSQL instance."""

    @pytest.mark.asyncio
    async def test_basic_bucket_score_exact(self, db_engine: AsyncEngine) -> None:
        """10 arrivals with known delays produce the exact expected score.

        Delay distribution: [30]*5 + [180]*5 (threshold = 120 s)
          on_time_rate = 5/10 = 0.50
          p50 = (30 + 180) / 2 = 105   (PERCENTILE_CONT interpolation)
          p95 = 180
          c1 = 0.50
          c2 = 1 - 180/900 = 0.8
          c3 = 1 - 105/300 = 0.65
          raw = 0.6*0.50 + 0.25*0.8 + 0.15*0.65 = 0.5975
          score = round(59.75) = 60
        """
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        sched = _sched_ts(weekday, _UTC_HOUR_9_12)   # in '9-12' bucket (PST or PDT)
        delays = [30, 30, 30, 30, 30, 180, 180, 180, 180, 180]
        await _insert_arrivals(sf, "T1", "S1", weekday, sched, delays)

        with patch(_PATCH_TARGET, _session_patcher(sf)):
            summary = await run_aggregation(
                lookback_days=30, dry_run=False, settings=_test_settings()
            )

        assert summary["rows_scanned"] == 1
        assert summary["buckets_updated"] == 1

        rows = await _fetch_score_agg(sf, "S1", "R1")
        assert len(rows) == 1
        row = rows[0]

        assert row.day_type == "weekday"
        assert row.hour_bucket == "9-12"
        assert row.sample_n == 10
        assert abs(row.on_time_rate - 0.5) < 1e-4
        assert row.p50_delay_sec == 105
        assert row.p95_delay_sec == 180
        assert row.score == 60

    @pytest.mark.asyncio
    async def test_multiple_day_types_and_buckets(self, db_engine: AsyncEngine) -> None:
        """Weekday+9-12 and Saturday+6-9 buckets are both written to score_agg.

        All delays = 60 s:
          on_time_rate = 1.0 (60 ≤ 120)
          p50 = p95 = 60
          c2 = 1 - 60/900 ≈ 0.9333
          c3 = 1 - 60/300 = 0.8
          raw = 0.6 + 0.25*0.9333 + 0.15*0.8 ≈ 0.9533
          score = round(95.33) = 95
        """
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        saturday = _recent_saturday()

        await _insert_arrivals(
            sf, "T1", "S1", weekday, _sched_ts(weekday, _UTC_HOUR_9_12), [60] * 8
        )
        await _insert_arrivals(
            sf, "T1", "S1", saturday, _sched_ts(saturday, _UTC_HOUR_6_9), [60] * 6
        )

        with patch(_PATCH_TARGET, _session_patcher(sf)):
            summary = await run_aggregation(
                lookback_days=30, dry_run=False, settings=_test_settings()
            )

        assert summary["rows_scanned"] == 2
        assert summary["buckets_updated"] == 2

        rows = await _fetch_score_agg(sf, "S1", "R1")
        assert len(rows) == 2

        buckets = {(r.day_type, r.hour_bucket): r for r in rows}
        assert ("weekday", "9-12") in buckets
        assert ("saturday", "6-9") in buckets

        for row in rows:
            assert abs(row.on_time_rate - 1.0) < 1e-4
            assert row.p50_delay_sec == 60
            assert row.p95_delay_sec == 60
            assert row.score == 95

    @pytest.mark.asyncio
    async def test_upsert_idempotent(self, db_engine: AsyncEngine) -> None:
        """Running aggregation twice over identical data yields the same score_agg row."""
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        await _insert_arrivals(
            sf, "T1", "S1", weekday,
            _sched_ts(weekday, _UTC_HOUR_9_12),
            [30, 60, 120, 180, 240],
        )

        ctx = _session_patcher(sf)
        kwargs: dict[str, Any] = dict(lookback_days=30, dry_run=False, settings=_test_settings())

        with patch(_PATCH_TARGET, ctx):
            await run_aggregation(**kwargs)

        rows_1st = await _fetch_score_agg(sf, "S1", "R1")
        assert len(rows_1st) == 1

        with patch(_PATCH_TARGET, ctx):
            await run_aggregation(**kwargs)

        rows_2nd = await _fetch_score_agg(sf, "S1", "R1")
        assert len(rows_2nd) == 1, "second run must not create a duplicate row"
        assert rows_2nd[0].score == rows_1st[0].score
        assert rows_2nd[0].sample_n == rows_1st[0].sample_n
        assert await _count(sf, "score_agg") == 1

    @pytest.mark.asyncio
    async def test_excludes_unmatched_rows(self, db_engine: AsyncEngine) -> None:
        """Rows with match_status != 'matched' are excluded from aggregation."""
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        sched = _sched_ts(weekday, _UTC_HOUR_9_12)

        # 5 matched rows
        await _insert_arrivals(sf, "T1", "S1", weekday, sched, [30] * 5)
        # 5 unmatched rows (different stop_sequences to satisfy unique constraint)
        await _insert_arrivals(
            sf, "T1", "S1", weekday, sched, [999] * 5,
            match_status="unmatched", stop_seq_offset=5,
        )

        with patch(_PATCH_TARGET, _session_patcher(sf)):
            summary = await run_aggregation(
                lookback_days=30, dry_run=False, settings=_test_settings()
            )

        # Only the 5 matched rows contribute to the bucket
        assert summary["rows_scanned"] == 1
        rows = await _fetch_score_agg(sf, "S1", "R1")
        assert len(rows) == 1
        assert rows[0].sample_n == 5

    @pytest.mark.asyncio
    async def test_excludes_out_of_window_hours(self, db_engine: AsyncEngine) -> None:
        """Arrivals scheduled outside the five service hour windows are dropped."""
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        # UTC 10:00 = PST 02:00 / PDT 03:00  — outside all buckets
        sched_night = _sched_ts(weekday, 10, 0)
        # UTC 18:00 = PST 10:00 / PDT 11:00  — inside '9-12' bucket
        sched_morning = _sched_ts(weekday, _UTC_HOUR_9_12)

        # 5 out-of-window arrivals
        await _insert_arrivals(sf, "T1", "S1", weekday, sched_night, [30] * 5)
        # 5 in-window arrivals (different stop_sequences)
        await _insert_arrivals(
            sf, "T1", "S1", weekday, sched_morning, [30] * 5, stop_seq_offset=5
        )

        with patch(_PATCH_TARGET, _session_patcher(sf)):
            summary = await run_aggregation(
                lookback_days=30, dry_run=False, settings=_test_settings()
            )

        assert summary["rows_scanned"] == 1
        rows = await _fetch_score_agg(sf, "S1", "R1")
        assert len(rows) == 1
        assert rows[0].sample_n == 5

    @pytest.mark.asyncio
    async def test_run_log_written_on_success(self, db_engine: AsyncEngine) -> None:
        """A successful run writes a status='success' row to agg_run_log."""
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        await _insert_arrivals(
            sf, "T1", "S1", weekday, _sched_ts(weekday, _UTC_HOUR_9_12), [30] * 5
        )

        with patch(_PATCH_TARGET, _session_patcher(sf)):
            await run_aggregation(lookback_days=7, dry_run=False, settings=_test_settings())

        async with sf() as session:
            result = await session.execute(
                text(
                    "SELECT status, lookback_days, rows_scanned, buckets_updated "
                    "FROM agg_run_log ORDER BY started_at DESC LIMIT 1"
                )
            )
            log_row = result.fetchone()

        assert log_row is not None
        assert log_row.status == "success"
        assert log_row.lookback_days == 7
        assert log_row.rows_scanned == 1
        assert log_row.buckets_updated == 1

    @pytest.mark.asyncio
    async def test_dry_run_writes_nothing(self, db_engine: AsyncEngine) -> None:
        """dry_run=True reports what would change but writes nothing to the DB."""
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        await _insert_arrivals(
            sf, "T1", "S1", weekday, _sched_ts(weekday, _UTC_HOUR_9_12), [30] * 5
        )

        with patch(_PATCH_TARGET, _session_patcher(sf)):
            summary = await run_aggregation(
                lookback_days=30, dry_run=True, settings=_test_settings()
            )

        assert summary["dry_run"] is True
        assert summary["rows_scanned"] == 1
        assert summary["buckets_updated"] == 1   # reported but not committed

        # DB must remain untouched
        assert await _count(sf, "score_agg") == 0
        assert await _count(sf, "agg_run_log") == 0

    @pytest.mark.asyncio
    async def test_different_routes_in_separate_buckets(self, db_engine: AsyncEngine) -> None:
        """Two trips on different routes at the same stop create separate buckets."""
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        sched = _sched_ts(weekday, _UTC_HOUR_9_12)

        # T1 → R1 and T2 → R2 both serve stop S1
        await _insert_arrivals(sf, "T1", "S1", weekday, sched, [30] * 5)
        await _insert_arrivals(sf, "T2", "S1", weekday, sched, [60] * 5)

        with patch(_PATCH_TARGET, _session_patcher(sf)):
            summary = await run_aggregation(
                lookback_days=30, dry_run=False, settings=_test_settings()
            )

        assert summary["rows_scanned"] == 2   # one bucket per (stop, route)
        assert await _count(sf, "score_agg") == 2

        rows_r1 = await _fetch_score_agg(sf, "S1", "R1")
        rows_r2 = await _fetch_score_agg(sf, "S1", "R2")
        assert len(rows_r1) == 1
        assert len(rows_r2) == 1

        # R1 bucket: all 30 s → on_time_rate 1.0
        assert abs(rows_r1[0].on_time_rate - 1.0) < 1e-4
        # R2 bucket: all 60 s → on_time_rate 1.0 (both ≤ 120 threshold)
        assert abs(rows_r2[0].on_time_rate - 1.0) < 1e-4


# ---------------------------------------------------------------------------
# Performance sanity test
# ---------------------------------------------------------------------------


class TestAggregationQueryPerformance:
    """Verify the aggregation SQL is syntactically valid and EXPLAIN-able."""

    @pytest.mark.asyncio
    async def test_explain_returns_valid_plan(self, db_engine: AsyncEngine) -> None:
        """EXPLAIN FORMAT JSON on _AGG_SQL must return a parseable query plan.

        With empty tables the planner uses sequential scans; that is expected.
        The assertion here is purely a syntax/validity check, ensuring no
        SQL errors slip through.  On a populated database the planner will
        prefer the ix_matched_date_trip index for the service_date predicate.
        """
        sf = _make_sf(db_engine)
        explain_sql = text("EXPLAIN (FORMAT JSON, ANALYZE false) " + _AGG_SQL.text)

        async with sf() as session:
            result = await session.execute(
                explain_sql,
                {
                    "tz": "America/Vancouver",
                    "lookback_days": 30,
                    "on_time_threshold": 120,
                },
            )
            rows = result.fetchall()

        assert len(rows) == 1, "EXPLAIN should return exactly one row"

        # asyncpg may return the JSON column as a Python object or as a string
        plan_data = rows[0][0]
        if isinstance(plan_data, str):
            plan_data = json.loads(plan_data)

        assert isinstance(plan_data, list), "EXPLAIN FORMAT JSON must return a JSON array"
        assert len(plan_data) > 0
        assert "Plan" in plan_data[0], "top-level plan node must have a 'Plan' key"

    @pytest.mark.asyncio
    async def test_explain_with_data_uses_date_filter(self, db_engine: AsyncEngine) -> None:
        """With seeded data, EXPLAIN should at minimum reference the date predicate.

        This is a structural check: we cannot assert a specific index name
        because the query planner has freedom to choose based on table stats.
        We do assert the plan string contains the table name, confirming the
        right query was planned.
        """
        sf = _make_sf(db_engine)
        await _seed_reference_data(sf)

        weekday = _recent_weekday()
        await _insert_arrivals(
            sf, "T1", "S1", weekday, _sched_ts(weekday, _UTC_HOUR_9_12), [30] * 20
        )

        explain_sql = text("EXPLAIN (FORMAT JSON, ANALYZE false) " + _AGG_SQL.text)
        async with sf() as session:
            result = await session.execute(
                explain_sql,
                {
                    "tz": "America/Vancouver",
                    "lookback_days": 30,
                    "on_time_threshold": 120,
                },
            )
            plan_data = result.fetchall()[0][0]

        if isinstance(plan_data, str):
            plan_data = json.loads(plan_data)

        plan_str = json.dumps(plan_data)
        assert "matched_arrivals" in plan_str, "plan must reference the matched_arrivals table"
        assert "trips" in plan_str, "plan must reference the trips table (for route_id JOIN)"
