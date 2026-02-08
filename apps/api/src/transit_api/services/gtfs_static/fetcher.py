"""GTFS static feed fetcher with retry and validation."""

from __future__ import annotations

import hashlib
import io
import zipfile
from pathlib import Path

import httpx

from transit_api.logging import get_logger

logger = get_logger(__name__)

# Default settings
DEFAULT_TIMEOUT_SEC = 120
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2.0

# ZIP magic bytes
ZIP_MAGIC = b"PK\x03\x04"


class FetchError(Exception):
    """Raised when GTFS feed fetch fails after all retries."""


class InvalidZipError(Exception):
    """Raised when downloaded content is not a valid ZIP."""


class GtfsStaticFetcher:
    """Fetches static GTFS feed from remote URL or local path."""

    def __init__(
        self,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
    ) -> None:
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    async def fetch_remote(self, url: str) -> tuple[bytes, str]:
        """Download GTFS ZIP from remote URL with retry + exponential backoff.

        Returns:
            Tuple of (zip_bytes, sha256_hex_digest).

        Raises:
            FetchError: If all retries exhausted.
            InvalidZipError: If response is not a valid ZIP.
        """
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                logger.info(
                    "Fetching GTFS static feed",
                    url=url,
                    attempt=attempt + 1,
                    max_retries=self.max_retries,
                )
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(self.timeout_sec),
                    follow_redirects=True,
                ) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    data = response.content

                self._validate_zip(data)
                feed_hash = hashlib.sha256(data).hexdigest()
                logger.info(
                    "GTFS feed downloaded",
                    size_bytes=len(data),
                    feed_hash=feed_hash,
                )
                return data, feed_hash

            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_error = exc
                if attempt < self.max_retries - 1:
                    import asyncio

                    delay = self.backoff_base ** (attempt + 1)
                    logger.warning(
                        "Fetch attempt failed, retrying",
                        attempt=attempt + 1,
                        delay_sec=delay,
                        error=str(exc),
                    )
                    await asyncio.sleep(delay)

        msg = f"Failed to fetch GTFS feed after {self.max_retries} attempts"
        raise FetchError(msg) from last_error

    def fetch_local(self, path: str | Path) -> tuple[bytes, str]:
        """Read GTFS ZIP from local filesystem.

        Returns:
            Tuple of (zip_bytes, sha256_hex_digest).

        Raises:
            FileNotFoundError: If path does not exist.
            InvalidZipError: If file is not a valid ZIP.
        """
        path = Path(path)
        if not path.exists():
            msg = f"Local GTFS file not found: {path}"
            raise FileNotFoundError(msg)

        data = path.read_bytes()
        self._validate_zip(data)
        feed_hash = hashlib.sha256(data).hexdigest()
        logger.info(
            "GTFS feed loaded from local file",
            path=str(path),
            size_bytes=len(data),
            feed_hash=feed_hash,
        )
        return data, feed_hash

    @staticmethod
    def _validate_zip(data: bytes) -> None:
        """Validate that data starts with ZIP magic bytes."""
        if len(data) < 4 or data[:4] != ZIP_MAGIC:
            msg = "Downloaded content is not a valid ZIP file"
            raise InvalidZipError(msg)
        if not zipfile.is_zipfile(io.BytesIO(data)):
            msg = "Downloaded content is not a valid ZIP file"
            raise InvalidZipError(msg)
