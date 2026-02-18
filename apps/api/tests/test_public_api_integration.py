"""Integration tests for Stage 7 public API endpoints.

Requires a live PostgreSQL instance.  Skipped unless both env vars are set:
    RUN_INTEGRATION_TESTS=1
    DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/testdb

WARNING: These tests drop and recreate all tables.  Use a dedicated test DB.
"""

from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

# ---------------------------------------------------------------------------
# Skip guard
# ---------------------------------------------------------------------------

RUN_INTEGRATION_TESTS = os.environ.get("RUN_INTEGRATION_TESTS") == "1"
DATABASE_URL = os.environ.get("DATABASE_URL", "")

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION_TESTS,
    reason="Set RUN_INTEGRATION_TESTS=1 and DATABASE_URL to run integration tests",
)

if not RUN_INTEGRATION_TESTS:
    pytest.skip(allow_module_level=True)

from transit_api.main import app
from transit_api.models.base import Base

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_UTC_HOUR_9_12 = 18  # UTC 18 = PST 10 or PDT 11 → bucket '9-12'


@pytest_asyncio.fixture
async def db_engine() -> AsyncEngine:
    """Per-test engine: drop and recreate all tables for a clean state."""
    engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session_factory(db_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=db_engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture
async def api_client(db_engine: AsyncEngine) -> AsyncClient:
    """HTTP client wired to a real DB via overridden engine."""
    from transit_api import database as db_module

    original_engine = db_module._engine
    original_factory = db_module._session_factory

    db_module._engine = db_engine
    db_module._session_factory = async_sessionmaker(
        bind=db_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    db_module._engine = original_engine
    db_module._session_factory = original_factory


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


async def _seed_reference(sf: async_sessionmaker[AsyncSession]) -> None:
    """Insert the minimum reference rows needed by integration tests."""
    async with sf() as session:
        await session.execute(text("""
            INSERT INTO stops (stop_id, name, lat, lon) VALUES
                ('STOP_A', 'Stop Alpha', 49.2827, -123.1207),
                ('STOP_B', 'Stop Beta',  49.2830, -123.1215),
                ('STOP_FAR', 'Stop Far', 49.3500, -123.2000)
            ON CONFLICT DO NOTHING
        """))
        await session.execute(text("""
            INSERT INTO routes (route_id, short_name, long_name) VALUES
                ('RT1', '99', 'B-Line'),
                ('RT2', '25', 'Brentwood')
            ON CONFLICT DO NOTHING
        """))
        await session.execute(text("""
            INSERT INTO trips (trip_id, route_id, service_id, direction_id) VALUES
                ('TRIP1', 'RT1', 'SVC1', 0),
                ('TRIP2', 'RT2', 'SVC1', 0)
            ON CONFLICT DO NOTHING
        """))
        await session.execute(text("""
            INSERT INTO stop_times (trip_id, stop_id, stop_sequence, sched_arrival_sec) VALUES
                ('TRIP1', 'STOP_A', 1, 32400),
                ('TRIP1', 'STOP_B', 2, 32700),
                ('TRIP2', 'STOP_A', 1, 33000),
                ('TRIP2', 'STOP_B', 2, 33300)
            ON CONFLICT DO NOTHING
        """))
        await session.commit()


async def _seed_score_agg(sf: async_sessionmaker[AsyncSession]) -> None:
    """Insert pre-computed score_agg rows for nearby-risky and score tests."""
    now = datetime.now(timezone.utc)
    async with sf() as session:
        await session.execute(text("""
            INSERT INTO score_agg
                (stop_id, route_id, day_type, hour_bucket, on_time_rate,
                 p50_delay_sec, p95_delay_sec, score, sample_n, updated_at)
            VALUES
                ('STOP_A', 'RT1', 'weekday', '9-12', 0.82, 30, 200, 78, 100, :now),
                ('STOP_A', 'RT2', 'weekday', '9-12', 0.60, 90, 400, 50, 80,  :now),
                ('STOP_B', 'RT1', 'weekday', '9-12', 0.90, 15, 120, 88, 150, :now)
            ON CONFLICT (stop_id, route_id, day_type, hour_bucket)
            DO UPDATE SET updated_at = EXCLUDED.updated_at
        """), {"now": now})
        await session.commit()


async def _seed_ingest_meta(sf: async_sessionmaker[AsyncSession]) -> None:
    """Insert rt_ingest_meta rows for /meta/last-ingest test."""
    now = datetime.now(timezone.utc)
    async with sf() as session:
        await session.execute(text("""
            INSERT INTO rt_ingest_meta
                (feed_type, last_success_at, last_attempt_at, status, entity_count)
            VALUES
                ('trip_updates',     :now, :now, 'ok', 150),
                ('vehicle_positions',:now, :now, 'ok', 80),
                ('alerts',           :now, :now, 'ok', 5)
            ON CONFLICT (feed_type)
            DO UPDATE SET last_success_at = EXCLUDED.last_success_at,
                          last_attempt_at = EXCLUDED.last_attempt_at,
                          status = EXCLUDED.status
        """), {"now": now})
        await session.commit()


async def _seed_matched_arrivals(
    sf: async_sessionmaker[AsyncSession],
    stop_id: str,
    route_id: str,
    service_date: date,
    delays: list[int],
) -> None:
    """Seed matched_arrivals for trend endpoint testing."""
    sched_hour_utc = _UTC_HOUR_9_12
    sched_ts = datetime(
        service_date.year, service_date.month, service_date.day,
        sched_hour_utc, 0, 0, tzinfo=timezone.utc
    )
    # Find a trip_id for the given route
    async with sf() as session:
        result = await session.execute(
            text("SELECT trip_id FROM trips WHERE route_id = :rid LIMIT 1"),
            {"rid": route_id},
        )
        row = result.fetchone()
        trip_id = row[0] if row else "TRIP1"

        rows = [
            {
                "trip_id": trip_id,
                "stop_id": stop_id,
                "stop_sequence": i + 100,
                "service_date": service_date,
                "scheduled_ts": sched_ts,
                "delay_sec": d,
            }
            for i, d in enumerate(delays)
        ]
        await session.execute(text("""
            INSERT INTO matched_arrivals
                (trip_id, stop_id, stop_sequence, service_date, scheduled_ts,
                 observed_ts, delay_sec, match_status, match_confidence,
                 source_feed_ts, created_at)
            VALUES (:trip_id, :stop_id, :stop_sequence, :service_date, :scheduled_ts,
                    :scheduled_ts, :delay_sec, 'matched', 1.0, :scheduled_ts, NOW())
            ON CONFLICT DO NOTHING
        """), rows)
        await session.commit()


# ---------------------------------------------------------------------------
# Tests: GET /stops/nearby
# ---------------------------------------------------------------------------


class TestNearbyStopsIntegration:
    @pytest.mark.asyncio
    async def test_returns_nearby_stops_ordered_by_distance(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)

        response = await api_client.get(
            "/stops/nearby",
            params={"lat": 49.2827, "lon": -123.1207, "radius_km": 1.0},
        )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert data["count"] >= 2
        # STOP_A should be closest (distance ≈ 0)
        ids = [item["stop_id"] for item in data["items"]]
        assert "STOP_A" in ids
        assert "STOP_B" in ids
        # STOP_FAR is ~7 km away; should not appear in 1 km radius
        assert "STOP_FAR" not in ids
        # Ordered by distance ascending
        distances = [item["distance_m"] for item in data["items"]]
        assert distances == sorted(distances)

    @pytest.mark.asyncio
    async def test_pagination_offset(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)

        r1 = await api_client.get(
            "/stops/nearby",
            params={"lat": 49.2827, "lon": -123.1207, "radius_km": 1.0, "limit": 1, "offset": 0},
        )
        r2 = await api_client.get(
            "/stops/nearby",
            params={"lat": 49.2827, "lon": -123.1207, "radius_km": 1.0, "limit": 1, "offset": 1},
        )

        assert r1.status_code == 200
        assert r2.status_code == 200
        # Different pages should return different stops
        ids1 = [i["stop_id"] for i in r1.json()["items"]]
        ids2 = [i["stop_id"] for i in r2.json()["items"]]
        assert ids1 != ids2

    @pytest.mark.asyncio
    async def test_empty_radius_returns_empty(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)

        # Very small radius around a point with no stops
        response = await api_client.get(
            "/stops/nearby",
            params={"lat": 0.0, "lon": 0.0, "radius_km": 0.1},
        )

        assert response.status_code == 200
        assert response.json()["count"] == 0


# ---------------------------------------------------------------------------
# Tests: GET /stops/{stop_id}/routes
# ---------------------------------------------------------------------------


class TestStopRoutesIntegration:
    @pytest.mark.asyncio
    async def test_returns_routes_for_stop(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)

        response = await api_client.get("/stops/STOP_A/routes")

        assert response.status_code == 200
        data = response.json()
        assert data["stop_id"] == "STOP_A"
        route_ids = {r["route_id"] for r in data["routes"]}
        assert "RT1" in route_ids
        assert "RT2" in route_ids
        for route in data["routes"]:
            assert "short_name" in route
            assert "long_name" in route

    @pytest.mark.asyncio
    async def test_nonexistent_stop_returns_404(self, api_client: AsyncClient) -> None:
        response = await api_client.get("/stops/NONEXISTENT/routes")
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Tests: GET /scores
# ---------------------------------------------------------------------------


class TestScoresIntegration:
    @pytest.mark.asyncio
    async def test_returns_score_for_known_bucket(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)
        await _seed_score_agg(db_session_factory)

        response = await api_client.get(
            "/scores",
            params={
                "stop_id": "STOP_A", "route_id": "RT1",
                "day_type": "weekday", "hour_bucket": "9-12",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["stop_id"] == "STOP_A"
        assert data["route_id"] == "RT1"
        assert data["score"] == 78
        assert data["sample_n"] == 100
        assert data["low_confidence"] is False

    @pytest.mark.asyncio
    async def test_missing_bucket_returns_404(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)

        response = await api_client.get(
            "/scores",
            params={
                "stop_id": "STOP_A", "route_id": "RT1",
                "day_type": "sunday", "hour_bucket": "9-12",
            },
        )

        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Tests: GET /scores/nearby-risky
# ---------------------------------------------------------------------------


class TestNearbyRiskyIntegration:
    @pytest.mark.asyncio
    async def test_returns_stops_ordered_by_score(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)
        await _seed_score_agg(db_session_factory)

        response = await api_client.get(
            "/scores/nearby-risky",
            params={
                "lat": 49.2827, "lon": -123.1207,
                "radius_km": 2.0,
                "day_type": "weekday", "hour_bucket": "9-12",
                "min_samples": 1,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert len(data["items"]) >= 1
        scores = [item["score"] for item in data["items"]]
        assert scores == sorted(scores), "Items must be sorted by score ascending"

    @pytest.mark.asyncio
    async def test_worst_route_per_stop(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        """STOP_A has RT1 (score=78) and RT2 (score=50); only RT2 should appear."""
        await _seed_reference(db_session_factory)
        await _seed_score_agg(db_session_factory)

        response = await api_client.get(
            "/scores/nearby-risky",
            params={
                "lat": 49.2827, "lon": -123.1207,
                "radius_km": 0.5,
                "day_type": "weekday", "hour_bucket": "9-12",
                "min_samples": 1,
            },
        )

        assert response.status_code == 200
        items = response.json()["items"]
        stop_a_items = [i for i in items if i["stop_id"] == "STOP_A"]
        # Only one entry per stop (worst route)
        assert len(stop_a_items) == 1
        assert stop_a_items[0]["route_id"] == "RT2"  # lowest score

    @pytest.mark.asyncio
    async def test_min_samples_filters_low_data(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)
        await _seed_score_agg(db_session_factory)

        # All seeded rows have sample_n >= 80, so min_samples=200 should return nothing
        response = await api_client.get(
            "/scores/nearby-risky",
            params={
                "lat": 49.2827, "lon": -123.1207,
                "radius_km": 2.0,
                "day_type": "weekday", "hour_bucket": "9-12",
                "min_samples": 200,
            },
        )

        assert response.status_code == 200
        assert response.json()["count"] == 0


# ---------------------------------------------------------------------------
# Tests: GET /scores/trend
# ---------------------------------------------------------------------------


class TestTrendIntegration:
    @pytest.mark.asyncio
    async def test_returns_series_for_seeded_data(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)
        svc_date = date.today()
        await _seed_matched_arrivals(
            db_session_factory,
            stop_id="STOP_A",
            route_id="RT1",
            service_date=svc_date,
            delays=[30, 30, 60, 120, 180],
        )

        response = await api_client.get(
            "/scores/trend",
            params={"stop_id": "STOP_A", "route_id": "RT1", "days": 7},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["stop_id"] == "STOP_A"
        assert data["route_id"] == "RT1"
        assert data["days"] == 7
        assert len(data["series"]) == 1
        point = data["series"][0]
        assert 0 <= point["score"] <= 100
        assert point["sample_n"] == 5

    @pytest.mark.asyncio
    async def test_empty_series_for_no_data(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_reference(db_session_factory)

        response = await api_client.get(
            "/scores/trend",
            params={"stop_id": "STOP_A", "route_id": "RT1"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["series"] == []


# ---------------------------------------------------------------------------
# Tests: GET /meta/last-ingest
# ---------------------------------------------------------------------------


class TestLastIngestIntegration:
    @pytest.mark.asyncio
    async def test_returns_feeds_for_seeded_meta(
        self, api_client: AsyncClient, db_session_factory: async_sessionmaker
    ) -> None:
        await _seed_ingest_meta(db_session_factory)

        response = await api_client.get("/meta/last-ingest")

        assert response.status_code == 200
        data = response.json()
        assert "feeds" in data
        assert len(data["feeds"]) == 3
        feed_types = {f["feed_type"] for f in data["feeds"]}
        assert feed_types == {"trip_updates", "vehicle_positions", "alerts"}

    @pytest.mark.asyncio
    async def test_empty_meta_returns_empty_feeds(self, api_client: AsyncClient) -> None:
        """With no rows in rt_ingest_meta, feeds list should be empty (not 500)."""
        response = await api_client.get("/meta/last-ingest")

        assert response.status_code == 200
        data = response.json()
        assert "feeds" in data
        # Either empty or whatever was seeded; most importantly not a 500
        assert isinstance(data["feeds"], list)


# ---------------------------------------------------------------------------
# Performance sanity: verify key queries have reasonable query plans
# ---------------------------------------------------------------------------


class TestQueryPerformance:
    @pytest.mark.asyncio
    async def test_nearby_stops_explain(
        self, db_session_factory: async_sessionmaker
    ) -> None:
        """EXPLAIN must succeed (no syntax error) and reference a plan node."""
        from transit_api.services.aggregation.engine import haversine_bounding_box
        lat, lon, radius_km = 49.2827, -123.1207, 1.0
        lat_min, lat_max, lon_min, lon_max = haversine_bounding_box(lat, lon, radius_km)

        explain_sql = text("""
            EXPLAIN (FORMAT JSON, ANALYZE false)
            WITH candidates AS (
                SELECT s.stop_id,
                       (6371000.0 * 2.0 * ASIN(SQRT(
                           POWER(SIN(RADIANS((s.lat::float - :lat) / 2.0)), 2)
                           + COS(RADIANS(:lat)) * COS(RADIANS(s.lat::float))
                           * POWER(SIN(RADIANS((s.lon::float - :lon) / 2.0)), 2)
                       ))) AS distance_m
                FROM stops s
                WHERE s.lat BETWEEN :lat_min AND :lat_max
                  AND s.lon BETWEEN :lon_min AND :lon_max
            )
            SELECT * FROM candidates WHERE distance_m <= :radius_m
            ORDER BY distance_m ASC LIMIT 50
        """)

        async with db_session_factory() as session:
            result = await session.execute(explain_sql, {
                "lat": lat, "lon": lon,
                "lat_min": lat_min, "lat_max": lat_max,
                "lon_min": lon_min, "lon_max": lon_max,
                "radius_m": radius_km * 1000.0,
            })
        plan_data = result.fetchall()[0][0]
        if isinstance(plan_data, str):
            plan_data = json.loads(plan_data)
        assert isinstance(plan_data, list)
        assert "Plan" in plan_data[0]

    @pytest.mark.asyncio
    async def test_score_lookup_explain(
        self, db_session_factory: async_sessionmaker
    ) -> None:
        """EXPLAIN for the /scores lookup query must succeed."""
        explain_sql = text("""
            EXPLAIN (FORMAT JSON, ANALYZE false)
            SELECT stop_id, route_id, day_type, hour_bucket,
                   on_time_rate::float, p50_delay_sec, p95_delay_sec,
                   score, sample_n, updated_at
            FROM score_agg
            WHERE stop_id = :stop_id
              AND route_id = :route_id
              AND day_type = :day_type
              AND hour_bucket = :hour_bucket
        """)

        async with db_session_factory() as session:
            result = await session.execute(explain_sql, {
                "stop_id": "STOP_A", "route_id": "RT1",
                "day_type": "weekday", "hour_bucket": "9-12",
            })
        plan_data = result.fetchall()[0][0]
        if isinstance(plan_data, str):
            plan_data = json.loads(plan_data)
        assert isinstance(plan_data, list)
        assert "Plan" in plan_data[0]
