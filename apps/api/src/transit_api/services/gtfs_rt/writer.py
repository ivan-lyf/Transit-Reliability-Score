"""GTFS-RT database writer with batch idempotent inserts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from transit_api.logging import get_logger

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)

DEFAULT_BATCH_SIZE = 500

# Table definitions for batch insert
_TABLE_DEFS = {
    "rt_trip_updates": {
        "columns": (
            "trip_id",
            "route_id",
            "stop_id",
            "stop_sequence",
            "arrival_delay",
            "arrival_time",
            "departure_delay",
            "departure_time",
            "schedule_relationship",
            "feed_timestamp",
            "recorded_at",
        ),
        "conflict_cols": ("trip_id", "stop_id", "feed_timestamp"),
    },
    "rt_vehicle_positions": {
        "columns": (
            "vehicle_id",
            "trip_id",
            "route_id",
            "latitude",
            "longitude",
            "bearing",
            "speed",
            "current_stop_sequence",
            "current_status",
            "feed_timestamp",
            "recorded_at",
        ),
        "conflict_cols": ("vehicle_id", "feed_timestamp"),
    },
    "rt_alerts": {
        "columns": (
            "alert_id",
            "cause",
            "effect",
            "header_text",
            "description_text",
            "active_period_start",
            "active_period_end",
            "informed_route_id",
            "informed_stop_id",
            "informed_trip_id",
            "feed_timestamp",
            "recorded_at",
        ),
        "conflict_cols": (
            "alert_id",
            "informed_route_id",
            "informed_stop_id",
            "feed_timestamp",
        ),
    },
}


class GtfsRtWriter:
    """Batch writer for GTFS-RT normalized data."""

    def __init__(self, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
        self.batch_size = batch_size

    async def write_trip_updates(
        self, session: AsyncSession, rows: list[dict[str, Any]], poll_id: str
    ) -> int:
        """Batch insert trip updates with ON CONFLICT DO NOTHING."""
        return await self._batch_insert(
            session, "rt_trip_updates", rows, poll_id
        )

    async def write_vehicle_positions(
        self, session: AsyncSession, rows: list[dict[str, Any]], poll_id: str
    ) -> int:
        """Batch insert vehicle positions with ON CONFLICT DO NOTHING."""
        return await self._batch_insert(
            session, "rt_vehicle_positions", rows, poll_id
        )

    async def write_alerts(
        self, session: AsyncSession, rows: list[dict[str, Any]], poll_id: str
    ) -> int:
        """Batch insert alerts with ON CONFLICT DO NOTHING."""
        return await self._batch_insert(
            session, "rt_alerts", rows, poll_id
        )

    async def update_ingest_meta(
        self,
        session: AsyncSession,
        feed_type: str,
        status: str,
        entity_count: int = 0,
        feed_hash: str = "",
        error_message: str = "",
    ) -> None:
        """Upsert the ingest meta row for a feed type."""
        now = datetime.now(timezone.utc)
        last_success = now if status == "ok" else None

        stmt = text("""
            INSERT INTO rt_ingest_meta
                (feed_type, last_success_at, last_attempt_at, status, error_message, feed_hash, entity_count)
            VALUES
                (:feed_type, :last_success_at, :last_attempt_at, :status, :error_message, :feed_hash, :entity_count)
            ON CONFLICT (feed_type) DO UPDATE SET
                last_success_at = CASE
                    WHEN EXCLUDED.status = 'ok' THEN EXCLUDED.last_attempt_at
                    ELSE rt_ingest_meta.last_success_at
                END,
                last_attempt_at = EXCLUDED.last_attempt_at,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                feed_hash = EXCLUDED.feed_hash,
                entity_count = EXCLUDED.entity_count
        """)
        await session.execute(
            stmt,
            {
                "feed_type": feed_type,
                "last_success_at": last_success,
                "last_attempt_at": now,
                "status": status,
                "error_message": error_message[:500] if error_message else "",
                "feed_hash": feed_hash,
                "entity_count": entity_count,
            },
        )
        await session.commit()

    async def _batch_insert(
        self,
        session: AsyncSession,
        table: str,
        rows: list[dict[str, Any]],
        poll_id: str,
    ) -> int:
        """Batch INSERT ... ON CONFLICT DO NOTHING for a given table.

        Returns the total number of rows actually inserted.
        """
        if not rows:
            return 0

        table_def = _TABLE_DEFS[table]
        columns = table_def["columns"]
        conflict_cols = table_def["conflict_cols"]

        column_list = ", ".join(columns)
        conflict_list = ", ".join(conflict_cols)

        total_inserted = 0

        for batch_start in range(0, len(rows), self.batch_size):
            batch = rows[batch_start : batch_start + self.batch_size]

            values_sql = ", ".join(
                "(" + ", ".join(f":{col}_{i}" for col in columns) + ")"
                for i in range(len(batch))
            )
            params: dict[str, Any] = {}
            for i, row in enumerate(batch):
                for col in columns:
                    params[f"{col}_{i}"] = row.get(col)

            stmt = text(f"""
                INSERT INTO {table} ({column_list})
                VALUES {values_sql}
                ON CONFLICT ({conflict_list}) DO NOTHING
            """)

            try:
                result = await session.execute(stmt, params)
                inserted = result.rowcount if result.rowcount else 0
                total_inserted += inserted
                await session.commit()
            except Exception as exc:
                await session.rollback()
                logger.error(
                    "Batch insert failed",
                    table=table,
                    poll_id=poll_id,
                    batch_start=batch_start,
                    batch_size=len(batch),
                    error=str(exc),
                )
                raise

        logger.info(
            "Batch insert complete",
            table=table,
            poll_id=poll_id,
            total_rows=len(rows),
            inserted=total_inserted,
            duplicates_skipped=len(rows) - total_inserted,
        )
        return total_inserted
