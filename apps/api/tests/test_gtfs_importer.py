"""Tests for GtfsImporter - integration tests with DB mocking and dry_run."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from transit_api.services.gtfs_static.importer import GtfsImporter, ImportReport
from transit_api.services.gtfs_static.reader import MissingRequiredFileError

from .fixtures.gtfs_fixture import (
    build_gtfs_zip,
)


class TestImportReport:
    """Tests for ImportReport data structure."""

    def test_report_init(self) -> None:
        report = ImportReport(source="test", feed_hash="abc123")
        assert report.source == "test"
        assert report.feed_hash == "abc123"
        assert report.import_id  # auto-generated
        assert report.warnings == []
        assert report.errors == []

    def test_report_init_table(self) -> None:
        report = ImportReport(source="test", feed_hash="abc")
        report.init_table("stops")
        assert report.counts["stops"] == {
            "read": 0,
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
        }

    def test_report_finish(self) -> None:
        report = ImportReport(source="test", feed_hash="abc")
        report.finish()
        assert report.ended_at is not None
        assert report.duration_ms is not None
        assert report.duration_ms >= 0

    def test_report_to_dict(self) -> None:
        report = ImportReport(source="test", feed_hash="abc", import_id="id-1")
        report.init_table("stops")
        report.finish()
        d = report.to_dict()
        assert d["import_id"] == "id-1"
        assert d["source"] == "test"
        assert d["feed_hash"] == "abc"
        assert "stops" in d["counts"]
        assert isinstance(d["started_at"], str)
        assert isinstance(d["ended_at"], str)


class TestImporterDryRun:
    """Tests for dry_run mode (no DB writes)."""

    async def test_dry_run_local_parses_without_db(self, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip()
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        importer = GtfsImporter()
        report = await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=True,
        )

        assert report.counts["stops"]["read"] == 3
        assert report.counts["routes"]["read"] == 2
        assert report.counts["trips"]["read"] == 3
        assert report.counts["stop_times"]["read"] == 8
        assert report.errors == []
        assert report.duration_ms is not None

    async def test_dry_run_no_db_session_created(self, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip()
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        with patch("transit_api.services.gtfs_static.importer.get_session_context") as mock_ctx:
            importer = GtfsImporter()
            await importer.run(
                source_type="local",
                source=str(zip_file),
                dry_run=True,
            )
            mock_ctx.assert_not_called()


class TestImporterStrictMode:
    """Tests for strict vs lenient error handling."""

    async def test_strict_mode_fails_on_bad_row(self, tmp_path: Path) -> None:
        # Add a bad stop row (missing lat/lon)
        bad_stops = (
            "stop_id,stop_name,stop_lat,stop_lon\n50001,Test,abc,def\n50002,Good,49.0,-123.0\n"
        )
        zip_bytes = build_gtfs_zip(stops=bad_stops)
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        importer = GtfsImporter(strict=True)
        report = await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=True,
        )

        # Strict mode: first error stops processing that table
        assert report.counts["stops"]["failed"] == 1
        assert len(report.errors) >= 1

    async def test_lenient_mode_collects_warnings(self, tmp_path: Path) -> None:
        bad_stops = (
            "stop_id,stop_name,stop_lat,stop_lon\n50001,Test,abc,def\n50002,Good,49.0,-123.0\n"
        )
        zip_bytes = build_gtfs_zip(stops=bad_stops)
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        importer = GtfsImporter(strict=False)
        report = await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=True,
        )

        assert report.counts["stops"]["read"] == 2
        assert report.counts["stops"]["failed"] == 1
        assert len(report.warnings) >= 1
        assert report.errors == []


class TestImporterInvalidSource:
    """Tests for invalid source handling."""

    async def test_invalid_source_type(self) -> None:
        importer = GtfsImporter()
        report = await importer.run(
            source_type="ftp",
            source="ftp://example.com/gtfs.zip",
            dry_run=True,
        )
        assert len(report.errors) >= 1
        assert "Invalid source_type" in report.errors[0]

    async def test_missing_local_file(self) -> None:
        importer = GtfsImporter()
        with pytest.raises(FileNotFoundError):
            await importer.run(
                source_type="local",
                source="/nonexistent/gtfs.zip",
                dry_run=True,
            )

    async def test_missing_required_file_in_zip(self, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip(exclude_files={"stops.txt"})
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        importer = GtfsImporter()
        with pytest.raises(MissingRequiredFileError, match=r"stops\.txt"):
            await importer.run(
                source_type="local",
                source=str(zip_file),
                dry_run=True,
            )


class TestImporterWithMockDB:
    """Tests for DB upsert logic using mocked session."""

    async def test_upsert_with_session_override(self, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip()
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(True,), (False,), (True,)]
        mock_session.execute.return_value = mock_result
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        importer = GtfsImporter(batch_size=100)
        report = await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=False,
            session_override=mock_session,
        )

        # Should have called execute for each table batch
        assert mock_session.execute.call_count > 0
        assert mock_session.commit.call_count > 0
        assert report.counts["stops"]["read"] == 3
        assert report.counts["routes"]["read"] == 2
        assert report.counts["trips"]["read"] == 3
        assert report.counts["stop_times"]["read"] == 8

    async def test_skip_if_unchanged(self, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip()
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        # Pre-compute the feed hash
        import hashlib

        expected_hash = hashlib.sha256(zip_bytes).hexdigest()

        mock_session = AsyncMock()
        # Simulate that the same hash is already stored
        mock_result = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda _self, _idx: expected_hash
        mock_result.fetchone.return_value = mock_row
        mock_session.execute.return_value = mock_result
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        importer = GtfsImporter()
        report = await importer.run(
            source_type="local",
            source=str(zip_file),
            dry_run=False,
            skip_if_unchanged=True,
            session_override=mock_session,
        )

        assert report.skipped_unchanged is True
