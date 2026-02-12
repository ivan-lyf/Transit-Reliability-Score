"""Tests for GTFS-RT feed fetcher."""

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from transit_api.services.gtfs_rt.fetcher import FeedFetchError, GtfsRtFetcher

from .fixtures.gtfs_rt_fixture import build_trip_update_feed


class TestGtfsRtFetcher:
    """Unit tests for GtfsRtFetcher."""

    @pytest.mark.asyncio
    async def test_fetch_success(self) -> None:
        expected_data = build_trip_update_feed()
        fetcher = GtfsRtFetcher(timeout_sec=5, max_retries=1)

        mock_response = AsyncMock()
        mock_response.content = expected_data
        mock_response.raise_for_status = lambda: None

        with patch("transit_api.services.gtfs_rt.fetcher.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.get = AsyncMock(return_value=mock_response)
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = instance

            data, feed_hash = await fetcher.fetch(
                "https://example.com/feed", "trip_updates", "poll-1"
            )

        assert data == expected_data
        assert len(feed_hash) == 64  # sha256 hex

    @pytest.mark.asyncio
    async def test_fetch_empty_response_raises(self) -> None:
        fetcher = GtfsRtFetcher(timeout_sec=5, max_retries=1)

        mock_response = AsyncMock()
        mock_response.content = b""
        mock_response.raise_for_status = lambda: None

        with patch("transit_api.services.gtfs_rt.fetcher.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.get = AsyncMock(return_value=mock_response)
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = instance

            with pytest.raises(FeedFetchError):
                await fetcher.fetch("https://example.com/feed", "trip_updates", "poll-1")

    @pytest.mark.asyncio
    async def test_fetch_http_error_retries(self) -> None:
        fetcher = GtfsRtFetcher(timeout_sec=5, max_retries=2, backoff_base=0.01)

        mock_response = AsyncMock()
        mock_response.status_code = 500
        mock_response.raise_for_status = AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "Server Error",
                request=httpx.Request("GET", "https://example.com/feed"),
                response=httpx.Response(500),
            )
        )

        with patch("transit_api.services.gtfs_rt.fetcher.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.get = AsyncMock(return_value=mock_response)
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = instance

            with pytest.raises(FeedFetchError, match="Failed to fetch"):
                await fetcher.fetch("https://example.com/feed", "trip_updates", "poll-1")

    @pytest.mark.asyncio
    async def test_fetch_network_error_retries(self) -> None:
        fetcher = GtfsRtFetcher(timeout_sec=5, max_retries=2, backoff_base=0.01)

        with patch("transit_api.services.gtfs_rt.fetcher.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.get = AsyncMock(
                side_effect=httpx.RequestError(
                    "Connection refused",
                    request=httpx.Request("GET", "https://example.com/feed"),
                )
            )
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = instance

            with pytest.raises(FeedFetchError, match="Failed to fetch"):
                await fetcher.fetch("https://example.com/feed", "trip_updates", "poll-1")

    @pytest.mark.asyncio
    async def test_fetch_retry_then_success(self) -> None:
        expected_data = build_trip_update_feed()
        fetcher = GtfsRtFetcher(timeout_sec=5, max_retries=3, backoff_base=0.01)

        fail_response = AsyncMock()
        fail_response.raise_for_status = AsyncMock(
            side_effect=httpx.HTTPStatusError(
                "Server Error",
                request=httpx.Request("GET", "https://example.com/feed"),
                response=httpx.Response(500),
            )
        )

        ok_response = AsyncMock()
        ok_response.content = expected_data
        ok_response.raise_for_status = lambda: None

        with patch("transit_api.services.gtfs_rt.fetcher.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.get = AsyncMock(side_effect=[fail_response, ok_response])
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = instance

            data, feed_hash = await fetcher.fetch(
                "https://example.com/feed", "trip_updates", "poll-1"
            )

        assert data == expected_data

    @pytest.mark.asyncio
    async def test_fetch_hash_deterministic(self) -> None:
        data = build_trip_update_feed(feed_timestamp=1700000000)
        fetcher = GtfsRtFetcher(timeout_sec=5, max_retries=1)

        mock_response = AsyncMock()
        mock_response.content = data
        mock_response.raise_for_status = lambda: None

        with patch("transit_api.services.gtfs_rt.fetcher.httpx.AsyncClient") as mock_client:
            instance = AsyncMock()
            instance.get = AsyncMock(return_value=mock_response)
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = instance

            _, hash1 = await fetcher.fetch("https://example.com/feed", "trip_updates", "p1")
            _, hash2 = await fetcher.fetch("https://example.com/feed", "trip_updates", "p2")

        assert hash1 == hash2
