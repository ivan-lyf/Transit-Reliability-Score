"""Integration tests for GTFS static importer using a real database."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from transit_api.models import Base
from transit_api.services.gtfs_static.importer import GtfsImporter
from transit_api.services.gtfs_static.reader import MissingRequiredFileError

from .fixtures.gtfs_fixture import STOPS_TXT_MODIFIED, build_gtfs_zip

if TYPE_CHECKING:
    from pathlib import Path

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
                "TRUNCATE stop_times, trips, routes, stops, rt_observations, score_agg, users, gtfs_import_log "
                "CASCADE"
            )
        )
        await session.commit()
        yield session


async def _count(session: AsyncSession, table: str) -> int:
    result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
    return int(result.scalar_one())


class TestGtfsImporterIntegration:
    """Integration tests using real DB writes."""

    async def test_first_import_from_empty_db(self, session: AsyncSession, tmp_path: Path) -> None:
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(build_gtfs_zip())

        importer = GtfsImporter(batch_size=100)
        report = await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=False,
            session_override=session,
        )

        assert report.counts["stops"]["inserted"] == 3
        assert report.counts["routes"]["inserted"] == 2
        assert report.counts["trips"]["inserted"] == 3
        assert report.counts["stop_times"]["inserted"] == 8
        assert await _count(session, "stops") == 3
        assert await _count(session, "routes") == 2
        assert await _count(session, "trips") == 3
        assert await _count(session, "stop_times") == 8

    async def test_reimport_same_zip_idempotent(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(build_gtfs_zip())

        importer = GtfsImporter(batch_size=100)
        await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=False,
            session_override=session,
        )

        report = await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=False,
            session_override=session,
        )

        assert report.counts["stops"]["inserted"] == 0
        assert report.counts["stops"]["updated"] == 0
        assert report.counts["stops"]["skipped"] == 3
        assert await _count(session, "stops") == 3
        assert await _count(session, "stop_times") == 8

    async def test_modified_fixture_updates_and_inserts(
        self, session: AsyncSession, tmp_path: Path
    ) -> None:
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(build_gtfs_zip())

        importer = GtfsImporter(batch_size=100)
        await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=False,
            session_override=session,
        )

        zip_file_mod = tmp_path / "gtfs_mod.zip"
        zip_file_mod.write_bytes(build_gtfs_zip(stops=STOPS_TXT_MODIFIED))

        report = await importer.run(
            source_type="local",
            source=str(zip_file_mod),
            dry_run=False,
            session_override=session,
        )

        assert report.counts["stops"]["inserted"] == 1
        assert report.counts["stops"]["updated"] == 1
        assert await _count(session, "stops") == 4

        result = await session.execute(
            text("SELECT name FROM stops WHERE stop_id = :stop_id"), {"stop_id": "50001"}
        )
        assert result.scalar_one() == "Waterfront Stn (Renamed)"

    async def test_dry_run_does_not_write(self, session: AsyncSession, tmp_path: Path) -> None:
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(build_gtfs_zip())

        importer = GtfsImporter(batch_size=100)
        report = await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=True,
            session_override=session,
        )

        assert report.counts["stops"]["read"] == 3
        assert await _count(session, "stops") == 0
        assert await _count(session, "routes") == 0
        assert await _count(session, "trips") == 0
        assert await _count(session, "stop_times") == 0

    async def test_missing_required_file_fails(self, session: AsyncSession, tmp_path: Path) -> None:
        zip_file = tmp_path / "bad_gtfs.zip"
        zip_file.write_bytes(build_gtfs_zip(exclude_files={"stops.txt"}))

        importer = GtfsImporter(batch_size=100)
        with pytest.raises(MissingRequiredFileError):
            await importer.run(
                source_type="local",
                source=str(zip_file),
                dry_run=False,
                session_override=session,
            )

        assert await _count(session, "stops") == 0
