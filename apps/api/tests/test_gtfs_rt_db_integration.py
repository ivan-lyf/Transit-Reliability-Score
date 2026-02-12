"""Integration tests for GTFS-RT pipeline using a real database."""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from transit_api.models import Base
from transit_api.services.gtfs_rt.fetcher import FeedFetchError
from transit_api.services.gtfs_rt.worker import (
    FEED_SERVICE_ALERTS,
    FEED_TRIP_UPDATES,
    FEED_VEHICLE_POSITIONS,
    GtfsRtWorker,
    reset_worker,
)

from .fixtures.gtfs_rt_fixture import (
    build_alert_feed,
    build_trip_update_feed,
    build_vehicle_position_feed,
)

RUN_INTEGRATION = os.getenv("RUN_INTEGRATION_TESTS") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")

if not RUN_INTEGRATION or not DATABASE_URL:
    pytest.skip(
        "Integration tests require RUN_INTEGRATION_TESTS=1 and DATABASE_URL to be set",
        allow_module_level=True,
    )


@pytest_asyncio.fixture
async def engine() -> AsyncEngine:
    """Create database engine and ensure schema is present."""
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def session(engine: AsyncEngine) -> AsyncSession:
    """Provide a clean database session for each test."""
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    async with session_factory() as session:
        await session.execute(
            text(
                "TRUNCATE rt_trip_updates, rt_vehicle_positions, rt_alerts, rt_ingest_meta "
                "RESTART IDENTITY CASCADE"
            )
        )
        await session.commit()
        yield session


def _make_worker() -> GtfsRtWorker:
    with patch("transit_api.services.gtfs_rt.worker.get_settings") as mock_settings:
        settings = MagicMock()
        settings.gtfs_rt_poll_interval_sec = 30
        settings.stale_feed_threshold_sec = 120
        settings.gtfs_rt_fetch_timeout_sec = 10
        settings.gtfs_rt_max_retries = 1
        settings.gtfs_rt_backoff_base = 0.01
        settings.gtfs_rt_batch_size = 100
        settings.gtfs_trip_updates_full_url = "https://example.com/tu"
        settings.gtfs_vehicle_positions_full_url = "https://example.com/vp"
        settings.gtfs_service_alerts_full_url = "https://example.com/sa"
        mock_settings.return_value = settings

        return GtfsRtWorker()


@asynccontextmanager
async def _session_context(session: AsyncSession):
    yield session


async def _count(session: AsyncSession, table: str) -> int:
    result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
    return int(result.scalar_one())


async def _index_names(session: AsyncSession, table: str) -> set[str]:
    result = await session.execute(
        text("SELECT indexname FROM pg_indexes WHERE tablename = :table"),
        {"table": table},
    )
    return {row[0] for row in result.fetchall()}


class TestGtfsRtDbIntegration:
    """Integration tests using real DB writes."""

    @pytest.mark.asyncio
    async def test_ingest_all_feeds_writes_rows_and_meta(self, session: AsyncSession) -> None:
        reset_worker()
        ts = int(time.time())
        tu_data = build_trip_update_feed(
            trip_id="T100",
            stop_updates=[
                {"stop_id": "S1", "stop_sequence": 1, "arrival_delay": 20, "departure_delay": 25},
                {"stop_id": "S2", "stop_sequence": 2, "arrival_delay": 40, "departure_delay": 50},
            ],
            feed_timestamp=ts,
        )
        vp_data = build_vehicle_position_feed(
            vehicle_id="V100",
            trip_id="T100",
            route_id="R10",
            lat=49.25,
            lon=-123.10,
            feed_timestamp=ts,
        )
        sa_data = build_alert_feed(
            alert_id="ALERT100",
            route_id="R10",
            stop_id="S1",
            feed_timestamp=ts,
        )

        worker = _make_worker()

        async def mock_fetch(url: str, feed_type: str, poll_id: str):
            if feed_type == FEED_TRIP_UPDATES:
                return tu_data, "hash-tu"
            if feed_type == FEED_VEHICLE_POSITIONS:
                return vp_data, "hash-vp"
            return sa_data, "hash-sa"

        worker._fetcher.fetch = AsyncMock(side_effect=mock_fetch)

        with patch(
            "transit_api.services.gtfs_rt.worker.get_session_context",
            new=lambda: _session_context(session),
        ):
            report = await worker.run_once()

        assert report["feeds"][FEED_TRIP_UPDATES]["status"] == "ok"
        assert report["feeds"][FEED_VEHICLE_POSITIONS]["status"] == "ok"
        assert report["feeds"][FEED_SERVICE_ALERTS]["status"] == "ok"

        assert await _count(session, "rt_trip_updates") == 2
        assert await _count(session, "rt_vehicle_positions") == 1
        assert await _count(session, "rt_alerts") == 1

        result = await session.execute(
            text("SELECT feed_type, status, last_success_at FROM rt_ingest_meta")
        )
        rows = result.fetchall()
        feed_types = {row[0] for row in rows}
        assert feed_types == {
            FEED_TRIP_UPDATES,
            FEED_VEHICLE_POSITIONS,
            FEED_SERVICE_ALERTS,
        }
        assert all(row[1] == "ok" for row in rows)
        assert all(row[2] is not None for row in rows)

    @pytest.mark.asyncio
    async def test_rt_indexes_present(self, session: AsyncSession) -> None:
        trip_indexes = await _index_names(session, "rt_trip_updates")
        vehicle_indexes = await _index_names(session, "rt_vehicle_positions")
        alert_indexes = await _index_names(session, "rt_alerts")
        meta_indexes = await _index_names(session, "rt_ingest_meta")

        assert "ix_rt_trip_updates_trip_id" in trip_indexes
        assert "ix_rt_trip_updates_stop_id" in trip_indexes
        assert "ix_rt_vehicle_pos_vehicle_id" in vehicle_indexes
        assert "ix_rt_vehicle_pos_trip_id" in vehicle_indexes
        assert "ix_rt_alerts_alert_id" in alert_indexes
        assert "ix_rt_alerts_route_id" in alert_indexes
        assert "ix_rt_ingest_meta_feed_type" in meta_indexes

    @pytest.mark.asyncio
    async def test_partial_failure_does_not_block_other_feeds(self, session: AsyncSession) -> None:
        reset_worker()
        ts = int(time.time())
        vp_data = build_vehicle_position_feed(
            vehicle_id="V200",
            trip_id="T200",
            route_id="R20",
            lat=49.26,
            lon=-123.11,
            feed_timestamp=ts,
        )
        sa_data = build_alert_feed(
            alert_id="ALERT200",
            route_id="R20",
            stop_id="S2",
            feed_timestamp=ts,
        )

        worker = _make_worker()

        async def mock_fetch(url: str, feed_type: str, poll_id: str):
            if feed_type == FEED_TRIP_UPDATES:
                raise FeedFetchError("fetch-failed")
            if feed_type == FEED_VEHICLE_POSITIONS:
                return vp_data, "hash-vp"
            return sa_data, "hash-sa"

        worker._fetcher.fetch = AsyncMock(side_effect=mock_fetch)

        with patch(
            "transit_api.services.gtfs_rt.worker.get_session_context",
            new=lambda: _session_context(session),
        ):
            report = await worker.run_once()

        assert report["feeds"][FEED_TRIP_UPDATES]["status"] == "error"
        assert report["feeds"][FEED_VEHICLE_POSITIONS]["status"] == "ok"
        assert report["feeds"][FEED_SERVICE_ALERTS]["status"] == "ok"

        assert await _count(session, "rt_trip_updates") == 0
        assert await _count(session, "rt_vehicle_positions") == 1
        assert await _count(session, "rt_alerts") == 1

        result = await session.execute(
            text(
                "SELECT status, error_message FROM rt_ingest_meta WHERE feed_type = :feed_type"
            ),
            {"feed_type": FEED_TRIP_UPDATES},
        )
        status, error_message = result.one()
        assert status == "error"
        assert "fetch-failed" in error_message
