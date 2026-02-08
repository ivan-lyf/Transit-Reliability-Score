"""GTFS-RT feed fetcher with retry and backoff."""

from __future__ import annotations

import asyncio
import hashlib
import inspect

import httpx

from transit_api.logging import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT_SEC = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2.0


class FeedFetchError(Exception):
    """Raised when a GTFS-RT feed fetch fails after all retries."""


class GtfsRtFetcher:
    """Fetches GTFS-RT protobuf feeds from remote URLs."""

    def __init__(
        self,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
    ) -> None:
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    async def fetch(self, url: str, feed_type: str, poll_id: str) -> tuple[bytes, str]:
        """Download a GTFS-RT protobuf feed with retry + exponential backoff.

        Args:
            url: Full URL (with API key) to fetch.
            feed_type: Label for logging (e.g. "trip_updates").
            poll_id: Correlation ID for this poll cycle.

        Returns:
            Tuple of (protobuf_bytes, sha256_hex_digest).

        Raises:
            FeedFetchError: If all retries exhausted.
        """
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                logger.info(
                    "Fetching GTFS-RT feed",
                    feed_type=feed_type,
                    poll_id=poll_id,
                    attempt=attempt + 1,
                    max_retries=self.max_retries,
                )
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(self.timeout_sec),
                    follow_redirects=True,
                ) as client:
                    response = await client.get(url)
                    raise_result = response.raise_for_status()
                    if inspect.isawaitable(raise_result):
                        await raise_result
                    data = response.content

                if not data:
                    msg = "Empty response body"
                    raise FeedFetchError(msg)

                feed_hash = hashlib.sha256(data).hexdigest()
                logger.info(
                    "GTFS-RT feed downloaded",
                    feed_type=feed_type,
                    poll_id=poll_id,
                    size_bytes=len(data),
                    feed_hash=feed_hash[:12],
                )
                return data, feed_hash

            except (httpx.HTTPStatusError, httpx.RequestError, FeedFetchError) as exc:
                last_error = exc
                if attempt < self.max_retries - 1:
                    delay = self.backoff_base ** (attempt + 1)
                    logger.warning(
                        "GTFS-RT fetch failed, retrying",
                        feed_type=feed_type,
                        poll_id=poll_id,
                        attempt=attempt + 1,
                        delay_sec=delay,
                        error=str(exc),
                    )
                    await asyncio.sleep(delay)

        msg = f"Failed to fetch {feed_type} after {self.max_retries} attempts"
        logger.error(msg, feed_type=feed_type, poll_id=poll_id, error=str(last_error))
        raise FeedFetchError(msg) from last_error
