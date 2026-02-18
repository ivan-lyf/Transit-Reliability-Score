"""Integration tests validating Alembic migrations on a real database."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

RUN_INTEGRATION = os.getenv("RUN_INTEGRATION_TESTS") == "1"
DATABASE_URL = os.getenv("DATABASE_URL")

if not RUN_INTEGRATION or not DATABASE_URL:
    pytest.skip(
        "Integration tests require RUN_INTEGRATION_TESTS=1 and DATABASE_URL to be set",
        allow_module_level=True,
    )

ALEMBIC_INI = Path(__file__).parent.parent / "alembic.ini"
ALEMBIC_DIR = Path(__file__).parent.parent / "alembic"

EXPECTED_TABLES = {
    "stops",
    "routes",
    "trips",
    "stop_times",
    "rt_observations",
    "score_agg",
    "users",
    "gtfs_import_log",
    "rt_trip_updates",
    "rt_vehicle_positions",
    "rt_alerts",
    "rt_ingest_meta",
    "matched_arrivals",
}

EXPECTED_INDEXES = {
    "stops": {"ix_stops_lat_lon"},
    "trips": {"ix_trips_route_id"},
    "stop_times": {
        "ix_stop_times_stop_id",
        "ix_stop_times_trip_id",
        "uq_stop_times_trip_sequence",
    },
    "rt_observations": {"ix_rt_observations_stop_observed"},
    "score_agg": {"ix_score_agg_lookup", "ix_score_agg_stop_score", "uq_score_agg_key"},
    "users": {"ix_users_auth_id"},
    "gtfs_import_log": {"ix_gtfs_import_log_imported_at"},
    "rt_trip_updates": {
        "ix_rt_trip_updates_trip_id",
        "ix_rt_trip_updates_stop_id",
        "ix_rt_trip_updates_feed_ts",
        "ix_rt_trip_updates_dedup",
    },
    "rt_vehicle_positions": {
        "ix_rt_vehicle_pos_vehicle_id",
        "ix_rt_vehicle_pos_trip_id",
        "ix_rt_vehicle_pos_route_id",
        "ix_rt_vehicle_pos_feed_ts",
        "ix_rt_vehicle_pos_dedup",
    },
    "rt_alerts": {
        "ix_rt_alerts_alert_id",
        "ix_rt_alerts_route_id",
        "ix_rt_alerts_feed_ts",
        "ix_rt_alerts_dedup",
    },
    "rt_ingest_meta": {"ix_rt_ingest_meta_feed_type"},
    "matched_arrivals": {
        "uq_matched_arrival_key",
        "ix_matched_trip_stop_date",
        "ix_matched_stop_observed",
        "ix_matched_date_trip",
    },
}

EXPECTED_CONSTRAINTS = {
    "score_agg": {
        "uq_score_agg_key",
        "ck_day_type",
        "ck_hour_bucket",
        "ck_score_range",
        "ck_on_time_rate",
        "ck_sample_n",
    },
    "stop_times": {"uq_stop_times_trip_sequence"},
}


def _alembic_config() -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


async def _reset_schema() -> None:
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            tables = ", ".join(sorted(EXPECTED_TABLES | {"alembic_version"}))
            await conn.execute(text(f"DROP TABLE IF EXISTS {tables} CASCADE"))
    finally:
        await engine.dispose()


async def _fetch_table_names() -> set[str]:
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            )
            return {row[0] for row in result.fetchall()}
    finally:
        await engine.dispose()


async def _verify_schema() -> None:
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            )
            tables = {row[0] for row in result.fetchall()}
            assert EXPECTED_TABLES.issubset(tables)

            for table, expected in EXPECTED_INDEXES.items():
                index_result = await conn.execute(
                    text(
                        "SELECT indexname FROM pg_indexes "
                        "WHERE schemaname = 'public' AND tablename = :table"
                    ),
                    {"table": table},
                )
                index_names = {row[0] for row in index_result.fetchall()}
                assert expected.issubset(index_names)

            for table, expected in EXPECTED_CONSTRAINTS.items():
                constraint_result = await conn.execute(
                    text("SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(:table)"),
                    {"table": table},
                )
                constraint_names = {row[0] for row in constraint_result.fetchall()}
                assert expected.issubset(constraint_names)
    finally:
        await engine.dispose()


async def _seed_base_data() -> None:
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE stop_times, trips, routes, stops, score_agg CASCADE"))
            await conn.execute(
                text("INSERT INTO stops (stop_id, name, lat, lon) VALUES (:id, :name, :lat, :lon)"),
                [
                    {"id": "S1", "name": "Stop 1", "lat": 49.2827, "lon": -123.1207},
                    {"id": "S2", "name": "Stop 2", "lat": 49.2830, "lon": -123.1210},
                ],
            )
            await conn.execute(
                text(
                    "INSERT INTO routes (route_id, short_name, long_name) "
                    "VALUES (:id, :short, :long)"
                ),
                {"id": "R1", "short": "1", "long": "Route 1"},
            )
            await conn.execute(
                text(
                    "INSERT INTO trips (trip_id, route_id, service_id, direction_id) "
                    "VALUES (:trip, :route, :service, :direction)"
                ),
                {"trip": "T1", "route": "R1", "service": "WKD", "direction": 0},
            )
            await conn.execute(
                text(
                    "INSERT INTO stop_times (trip_id, stop_id, stop_sequence, sched_arrival_sec) "
                    "VALUES (:trip, :stop, :seq, :arrival)"
                ),
                {"trip": "T1", "stop": "S1", "seq": 1, "arrival": 3600},
            )
            await conn.execute(
                text(
                    "INSERT INTO score_agg ("
                    "stop_id, route_id, day_type, hour_bucket, on_time_rate, "
                    "p50_delay_sec, p95_delay_sec, score, sample_n"
                    ") VALUES ("
                    ":stop, :route, :day, :hour, :rate, :p50, :p95, :score, :sample"
                    ")"
                ),
                {
                    "stop": "S1",
                    "route": "R1",
                    "day": "weekday",
                    "hour": "6-9",
                    "rate": 0.9,
                    "p50": 60,
                    "p95": 120,
                    "score": 90,
                    "sample": 10,
                },
            )
    finally:
        await engine.dispose()


async def _expect_integrity_error(sql: str, params: dict[str, object]) -> None:
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        async with engine.begin() as conn:
            with pytest.raises(IntegrityError):
                await conn.execute(text(sql), params)
    finally:
        await engine.dispose()


async def _validate_constraints() -> None:
    await _seed_base_data()

    await _expect_integrity_error(
        "INSERT INTO score_agg ("
        "stop_id, route_id, day_type, hour_bucket, on_time_rate, "
        "p50_delay_sec, p95_delay_sec, score, sample_n"
        ") VALUES ("
        ":stop, :route, :day, :hour, :rate, :p50, :p95, :score, :sample"
        ")",
        {
            "stop": "S1",
            "route": "R1",
            "day": "weekday",
            "hour": "6-9",
            "rate": 0.8,
            "p50": 30,
            "p95": 90,
            "score": 80,
            "sample": 5,
        },
    )

    await _expect_integrity_error(
        "INSERT INTO score_agg ("
        "stop_id, route_id, day_type, hour_bucket, on_time_rate, "
        "p50_delay_sec, p95_delay_sec, score, sample_n"
        ") VALUES ("
        ":stop, :route, :day, :hour, :rate, :p50, :p95, :score, :sample"
        ")",
        {
            "stop": "S1",
            "route": "R1",
            "day": "holiday",
            "hour": "6-9",
            "rate": 0.8,
            "p50": 30,
            "p95": 90,
            "score": 80,
            "sample": 5,
        },
    )

    await _expect_integrity_error(
        "INSERT INTO score_agg ("
        "stop_id, route_id, day_type, hour_bucket, on_time_rate, "
        "p50_delay_sec, p95_delay_sec, score, sample_n"
        ") VALUES ("
        ":stop, :route, :day, :hour, :rate, :p50, :p95, :score, :sample"
        ")",
        {
            "stop": "S1",
            "route": "R1",
            "day": "weekday",
            "hour": "0-3",
            "rate": 0.8,
            "p50": 30,
            "p95": 90,
            "score": 80,
            "sample": 5,
        },
    )

    await _expect_integrity_error(
        "INSERT INTO score_agg ("
        "stop_id, route_id, day_type, hour_bucket, on_time_rate, "
        "p50_delay_sec, p95_delay_sec, score, sample_n"
        ") VALUES ("
        ":stop, :route, :day, :hour, :rate, :p50, :p95, :score, :sample"
        ")",
        {
            "stop": "S1",
            "route": "R1",
            "day": "weekday",
            "hour": "6-9",
            "rate": 0.8,
            "p50": 30,
            "p95": 90,
            "score": 120,
            "sample": 5,
        },
    )

    await _expect_integrity_error(
        "INSERT INTO score_agg ("
        "stop_id, route_id, day_type, hour_bucket, on_time_rate, "
        "p50_delay_sec, p95_delay_sec, score, sample_n"
        ") VALUES ("
        ":stop, :route, :day, :hour, :rate, :p50, :p95, :score, :sample"
        ")",
        {
            "stop": "S1",
            "route": "R1",
            "day": "weekday",
            "hour": "6-9",
            "rate": 1.5,
            "p50": 30,
            "p95": 90,
            "score": 80,
            "sample": 5,
        },
    )

    await _expect_integrity_error(
        "INSERT INTO score_agg ("
        "stop_id, route_id, day_type, hour_bucket, on_time_rate, "
        "p50_delay_sec, p95_delay_sec, score, sample_n"
        ") VALUES ("
        ":stop, :route, :day, :hour, :rate, :p50, :p95, :score, :sample"
        ")",
        {
            "stop": "S1",
            "route": "R1",
            "day": "weekday",
            "hour": "6-9",
            "rate": 0.8,
            "p50": 30,
            "p95": 90,
            "score": 80,
            "sample": -1,
        },
    )

    await _expect_integrity_error(
        "INSERT INTO stop_times (trip_id, stop_id, stop_sequence, sched_arrival_sec) "
        "VALUES (:trip, :stop, :seq, :arrival)",
        {"trip": "T1", "stop": "S2", "seq": 1, "arrival": 3700},
    )

    await _expect_integrity_error(
        "INSERT INTO trips (trip_id, route_id, service_id, direction_id) "
        "VALUES (:trip, :route, :service, :direction)",
        {"trip": "T2", "route": "MISSING", "service": "WKD", "direction": 1},
    )


def test_migration_lifecycle_and_db_validation() -> None:
    config = _alembic_config()

    asyncio.run(_reset_schema())

    command.upgrade(config, "head")
    asyncio.run(_verify_schema())

    command.downgrade(config, "base")
    tables = asyncio.run(_fetch_table_names())
    assert EXPECTED_TABLES.isdisjoint(tables)

    command.upgrade(config, "head")
    asyncio.run(_verify_schema())
    asyncio.run(_validate_constraints())
