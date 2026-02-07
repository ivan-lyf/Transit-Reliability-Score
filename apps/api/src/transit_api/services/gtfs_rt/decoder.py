"""GTFS-RT protobuf decode layer."""

from __future__ import annotations

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from transit_api.logging import get_logger

logger = get_logger(__name__)


class DecodeError_(Exception):
    """Raised when protobuf decoding fails."""


class GtfsRtDecoder:
    """Decodes raw protobuf bytes into GTFS-RT FeedMessage objects."""

    @staticmethod
    def decode(data: bytes, feed_type: str, poll_id: str) -> gtfs_realtime_pb2.FeedMessage:
        """Decode protobuf bytes into a FeedMessage.

        Args:
            data: Raw protobuf bytes.
            feed_type: Label for logging.
            poll_id: Correlation ID.

        Returns:
            Parsed FeedMessage.

        Raises:
            DecodeError_: If protobuf parsing fails.
        """
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(data)
        except DecodeError as exc:
            msg = f"Failed to decode {feed_type} protobuf"
            logger.error(msg, feed_type=feed_type, poll_id=poll_id, error=str(exc))
            raise DecodeError_(msg) from exc

        entity_count = len(feed.entity)
        feed_ts = feed.header.timestamp if feed.header.timestamp else 0

        logger.info(
            "GTFS-RT feed decoded",
            feed_type=feed_type,
            poll_id=poll_id,
            entity_count=entity_count,
            feed_timestamp=feed_ts,
            gtfs_rt_version=feed.header.gtfs_realtime_version,
        )

        return feed

    @staticmethod
    def get_feed_timestamp(feed: gtfs_realtime_pb2.FeedMessage) -> int:
        """Extract the header timestamp from a FeedMessage.

        Returns:
            Unix timestamp (seconds), or 0 if not set.
        """
        return feed.header.timestamp if feed.header.timestamp else 0

    @staticmethod
    def get_entity_count(feed: gtfs_realtime_pb2.FeedMessage) -> int:
        """Get the number of entities in the feed."""
        return len(feed.entity)
