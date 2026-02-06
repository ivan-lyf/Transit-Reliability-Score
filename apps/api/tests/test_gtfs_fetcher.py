"""Tests for GtfsStaticFetcher - retries, timeouts, ZIP validation."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

if TYPE_CHECKING:
    from pathlib import Path

import httpx
import pytest

from transit_api.services.gtfs_static.fetcher import (
    FetchError,
    GtfsStaticFetcher,
    InvalidZipError,
)

from .fixtures.gtfs_fixture import build_gtfs_zip, build_invalid_zip


class TestFetchLocal:
    """Tests for local file fetching."""

    def test_fetch_local_valid_zip(self, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip()
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        fetcher = GtfsStaticFetcher()
        data, feed_hash = fetcher.fetch_local(str(zip_file))

        assert data == zip_bytes
        assert len(feed_hash) == 64  # SHA-256 hex
        assert feed_hash  # non-empty

    def test_fetch_local_file_not_found(self) -> None:
        fetcher = GtfsStaticFetcher()
        with pytest.raises(FileNotFoundError, match="not found"):
            fetcher.fetch_local("/nonexistent/path/gtfs.zip")

    def test_fetch_local_invalid_zip(self, tmp_path: Path) -> None:
        bad_file = tmp_path / "bad.zip"
        bad_file.write_bytes(build_invalid_zip())

        fetcher = GtfsStaticFetcher()
        with pytest.raises(InvalidZipError, match="not a valid ZIP"):
            fetcher.fetch_local(str(bad_file))

    def test_fetch_local_returns_consistent_hash(self, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip()
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        fetcher = GtfsStaticFetcher()
        _, hash1 = fetcher.fetch_local(str(zip_file))
        _, hash2 = fetcher.fetch_local(str(zip_file))
        assert hash1 == hash2

    def test_fetch_local_pathlib(self, tmp_path: Path) -> None:
        zip_bytes = build_gtfs_zip()
        zip_file = tmp_path / "gtfs.zip"
        zip_file.write_bytes(zip_bytes)

        fetcher = GtfsStaticFetcher()
        data, _ = fetcher.fetch_local(zip_file)  # Pass as Path, not str
        assert data == zip_bytes


class TestFetchRemote:
    """Tests for remote URL fetching with retry logic."""

    async def test_fetch_remote_success(self) -> None:
        zip_bytes = build_gtfs_zip()
        mock_request = httpx.Request("GET", "https://example.com/gtfs.zip")
        mock_response = httpx.Response(200, content=zip_bytes, request=mock_request)

        with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_response):
            fetcher = GtfsStaticFetcher(max_retries=1)
            data, feed_hash = await fetcher.fetch_remote("https://example.com/gtfs.zip")

        assert data == zip_bytes
        assert len(feed_hash) == 64

    async def test_fetch_remote_retries_on_failure(self) -> None:
        zip_bytes = build_gtfs_zip()
        mock_request = httpx.Request("GET", "https://example.com/gtfs.zip")
        mock_response_ok = httpx.Response(200, content=zip_bytes, request=mock_request)
        mock_response_err = httpx.Response(500, request=mock_request)

        call_count = 0

        async def side_effect(*args, **kwargs):  # noqa: ARG001
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.HTTPStatusError("500", request=mock_request, response=mock_response_err)
            return mock_response_ok

        with patch("httpx.AsyncClient.get", side_effect=side_effect):
            fetcher = GtfsStaticFetcher(max_retries=3, backoff_base=0.01)
            data, _ = await fetcher.fetch_remote("https://example.com/gtfs.zip")

        assert data == zip_bytes
        assert call_count == 2

    async def test_fetch_remote_all_retries_exhausted(self) -> None:
        mock_request = httpx.Request("GET", "https://example.com/gtfs.zip")
        mock_response_err = httpx.Response(500, request=mock_request)

        async def side_effect(*args, **kwargs):  # noqa: ARG001
            raise httpx.HTTPStatusError("500", request=mock_request, response=mock_response_err)

        with patch("httpx.AsyncClient.get", side_effect=side_effect):
            fetcher = GtfsStaticFetcher(max_retries=2, backoff_base=0.01)
            with pytest.raises(FetchError, match="Failed to fetch"):
                await fetcher.fetch_remote("https://example.com/gtfs.zip")

    async def test_fetch_remote_invalid_zip_content(self) -> None:
        mock_request = httpx.Request("GET", "https://example.com/gtfs.zip")
        mock_response = httpx.Response(200, content=build_invalid_zip(), request=mock_request)

        with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_response):
            fetcher = GtfsStaticFetcher(max_retries=1)
            with pytest.raises(InvalidZipError, match="not a valid ZIP"):
                await fetcher.fetch_remote("https://example.com/gtfs.zip")


class TestZipValidation:
    """Tests for ZIP magic byte validation."""

    def test_valid_zip_passes(self) -> None:
        GtfsStaticFetcher._validate_zip(build_gtfs_zip())

    def test_invalid_bytes_fail(self) -> None:
        with pytest.raises(InvalidZipError):
            GtfsStaticFetcher._validate_zip(b"not a zip")

    def test_empty_bytes_fail(self) -> None:
        with pytest.raises(InvalidZipError):
            GtfsStaticFetcher._validate_zip(b"")

    def test_short_bytes_fail(self) -> None:
        with pytest.raises(InvalidZipError):
            GtfsStaticFetcher._validate_zip(b"PK")
