"""Tests for GtfsZipReader - ZIP extraction and required file validation."""

from __future__ import annotations

import zipfile

import pytest

from transit_api.services.gtfs_static.reader import (
    GtfsZipReader,
    MissingRequiredFileError,
)

from .fixtures.gtfs_fixture import build_gtfs_zip


class TestGtfsZipReader:
    """Tests for ZIP reader validation and file extraction."""

    def test_valid_zip_opens_successfully(self) -> None:
        zip_bytes = build_gtfs_zip()
        reader = GtfsZipReader(zip_bytes)
        assert "stops.txt" in reader.list_files()
        assert "routes.txt" in reader.list_files()
        assert "trips.txt" in reader.list_files()
        assert "stop_times.txt" in reader.list_files()
        reader.close()

    def test_missing_required_file_raises(self) -> None:
        zip_bytes = build_gtfs_zip(exclude_files={"stops.txt"})
        with pytest.raises(MissingRequiredFileError, match=r"stops\.txt"):
            GtfsZipReader(zip_bytes)

    def test_missing_multiple_files_raises(self) -> None:
        zip_bytes = build_gtfs_zip(exclude_files={"stops.txt", "routes.txt"})
        with pytest.raises(MissingRequiredFileError, match="Missing required"):
            GtfsZipReader(zip_bytes)

    def test_invalid_zip_bytes_raises(self) -> None:
        with pytest.raises(zipfile.BadZipFile):
            GtfsZipReader(b"not a zip file")

    def test_context_manager(self) -> None:
        zip_bytes = build_gtfs_zip()
        with GtfsZipReader(zip_bytes) as reader:
            files = reader.list_files()
        assert len(files) >= 4

    def test_open_file_returns_text(self) -> None:
        zip_bytes = build_gtfs_zip()
        with GtfsZipReader(zip_bytes) as reader:
            text_io = reader.open_file("stops.txt")
            header = text_io.readline()
        assert "stop_id" in header

    def test_extra_files_accepted(self) -> None:
        zip_bytes = build_gtfs_zip(
            extra_files={"agency.txt": "agency_id,agency_name\nTL,TransLink"}
        )
        with GtfsZipReader(zip_bytes) as reader:
            assert "agency.txt" in reader.list_files()
