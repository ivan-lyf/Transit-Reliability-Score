"""GTFS static importer - orchestrates fetch, parse, normalize, and upsert."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from transit_api.config import get_settings
from transit_api.database import get_session_context

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
from transit_api.logging import get_logger
from transit_api.services.gtfs_static.fetcher import GtfsStaticFetcher
from transit_api.services.gtfs_static.normalizer import (
    GtfsNormalizer,
    NormalizationError,
    TimeParseError,
)
from transit_api.services.gtfs_static.parser import GtfsParser
from transit_api.services.gtfs_static.reader import GtfsZipReader

logger = get_logger(__name__)

DEFAULT_BATCH_SIZE = get_settings().import_batch_size


class ImportReport:
    """Collects import metrics, warnings, and errors."""

    def __init__(self, source: str, feed_hash: str, import_id: str | None = None) -> None:
        self.import_id = import_id or str(uuid.uuid4())
        self.source = source
        self.feed_hash = feed_hash
        self.started_at = datetime.now(timezone.utc)
        self.ended_at: datetime | None = None
        self.duration_ms: int | None = None
        self.counts: dict[str, dict[str, int]] = {}
        self.warnings: list[str] = []
        self.errors: list[str] = []
        self.skipped_unchanged = False

    def init_table(self, table: str) -> None:
        self.counts[table] = {"read": 0, "inserted": 0, "updated": 0, "skipped": 0, "failed": 0}

    def finish(self) -> None:
        self.ended_at = datetime.now(timezone.utc)
        self.duration_ms = int((self.ended_at - self.started_at).total_seconds() * 1000)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "failed" if self.errors else "success",
            "import_id": self.import_id,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_ms": self.duration_ms,
            "source": self.source,
            "feed_hash": self.feed_hash,
            "skipped_unchanged": self.skipped_unchanged,
            "counts": self.counts,
            "warnings": self.warnings[:100],  # cap for response size
            "errors": self.errors[:100],
        }


class GtfsImporter:
    """Orchestrates the full static GTFS import pipeline.

    Supports dry_run mode, strict/lenient error handling,
    and skip-if-unchanged optimization.
    """

    def __init__(
        self,
        batch_size: int | None = None,
        strict: bool | None = None,
    ) -> None:
        settings = get_settings()
        self.batch_size = batch_size if batch_size is not None else settings.import_batch_size
        self.strict = strict if strict is not None else settings.gtfs_import_strict
        self._fetcher = GtfsStaticFetcher()
        self._normalizer = GtfsNormalizer()

    async def run(
        self,
        source_type: str,
        source: str,
        dry_run: bool = False,
        skip_if_unchanged: bool = False,
        session_override: AsyncSession | None = None,
    ) -> ImportReport:
        """Execute the full import pipeline.

        Args:
            source_type: "remote" or "local".
            source: URL or file path.
            dry_run: If True, parse and validate but skip DB writes.
            skip_if_unchanged: If True, skip import if feed hash matches last import.
            session_override: Optional session for testing (skips context manager).

        Returns:
            ImportReport with full metrics.
        """
        import_id = str(uuid.uuid4())
        logger.info(
            "Starting GTFS static import",
            import_id=import_id,
            source_type=source_type,
            source=source,
            dry_run=dry_run,
        )

        # Fetch
        if source_type == "remote":
            zip_bytes, feed_hash = await self._fetcher.fetch_remote(source)
        elif source_type == "local":
            zip_bytes, feed_hash = self._fetcher.fetch_local(source)
        else:
            report = ImportReport(source=source, feed_hash="", import_id=import_id)
            report.errors.append(f"Invalid source_type: {source_type}")
            report.finish()
            return report

        report = ImportReport(source=source, feed_hash=feed_hash, import_id=import_id)

        # Read ZIP
        with GtfsZipReader(zip_bytes) as reader:
            parser = GtfsParser(reader)

            # Parse and normalize all data before DB operations
            stops_data = self._parse_and_normalize(
                parser.parse_stops, self._normalizer.normalize_stop, "stops", report
            )
            routes_data = self._parse_and_normalize(
                parser.parse_routes, self._normalizer.normalize_route, "routes", report
            )
            trips_data = self._parse_and_normalize(
                parser.parse_trips, self._normalizer.normalize_trip, "trips", report
            )
            stop_times_data = self._parse_and_normalize(
                parser.parse_stop_times, self._normalizer.normalize_stop_time, "stop_times", report
            )

        if report.errors and self.strict:
            report.finish()
            return report

        if dry_run:
            logger.info("Dry run complete, skipping DB writes", import_id=import_id)
            report.finish()
            return report

        # DB upsert
        if session_override:
            await self._upsert_all(
                session_override,
                stops_data,
                routes_data,
                trips_data,
                stop_times_data,
                report,
                skip_if_unchanged,
                feed_hash,
            )
        else:
            async with get_session_context() as session:
                await self._upsert_all(
                    session,
                    stops_data,
                    routes_data,
                    trips_data,
                    stop_times_data,
                    report,
                    skip_if_unchanged,
                    feed_hash,
                )

        report.finish()
        logger.info(
            "GTFS static import complete",
            import_id=import_id,
            duration_ms=report.duration_ms,
            counts=report.counts,
            warnings_count=len(report.warnings),
            errors_count=len(report.errors),
        )
        return report

    def _parse_and_normalize(
        self,
        parse_fn: Any,
        normalize_fn: Any,
        table_name: str,
        report: ImportReport,
    ) -> list[dict[str, Any]]:
        """Parse and normalize rows from a GTFS file.

        Collects errors per row; if strict mode, raises on first error.
        """
        report.init_table(table_name)
        results: list[dict[str, Any]] = []

        for row in parse_fn():
            report.counts[table_name]["read"] += 1
            try:
                normalized = normalize_fn(row)
                results.append(normalized)
            except (NormalizationError, TimeParseError) as exc:
                report.counts[table_name]["failed"] += 1
                msg = f"{table_name} row error: {exc}"
                if self.strict:
                    report.errors.append(msg)
                    return results
                report.warnings.append(msg)

        return results

    async def _upsert_all(
        self,
        session: AsyncSession,
        stops_data: list[dict[str, Any]],
        routes_data: list[dict[str, Any]],
        trips_data: list[dict[str, Any]],
        stop_times_data: list[dict[str, Any]],
        report: ImportReport,
        skip_if_unchanged: bool,
        feed_hash: str,
    ) -> None:
        """Upsert all tables in dependency order."""
        if skip_if_unchanged:
            last_hash = await self._get_last_feed_hash(session)
            if last_hash == feed_hash:
                logger.info("Feed hash unchanged, skipping import", feed_hash=feed_hash)
                report.skipped_unchanged = True
                report.finish()
                return

        # Order matters: stops & routes first (no FK deps), then trips, then stop_times
        await self._upsert_stops(session, stops_data, report)
        await self._upsert_routes(session, routes_data, report)
        await self._upsert_trips(session, trips_data, report)
        await self._upsert_stop_times(session, stop_times_data, report)

        # Store feed hash for skip-if-unchanged
        await self._store_feed_hash(session, feed_hash)

    async def _upsert_stops(
        self, session: AsyncSession, data: list[dict[str, Any]], report: ImportReport
    ) -> None:
        """Batch upsert stops using ON CONFLICT (stop_id)."""
        await self._bulk_upsert(
            session=session,
            table="stops",
            columns=("stop_id", "name", "lat", "lon"),
            conflict_cols=("stop_id",),
            update_cols=("name", "lat", "lon"),
            data=data,
            report=report,
            table_name="stops",
        )

    async def _upsert_routes(
        self, session: AsyncSession, data: list[dict[str, Any]], report: ImportReport
    ) -> None:
        """Batch upsert routes using ON CONFLICT (route_id)."""
        await self._bulk_upsert(
            session=session,
            table="routes",
            columns=("route_id", "short_name", "long_name"),
            conflict_cols=("route_id",),
            update_cols=("short_name", "long_name"),
            data=data,
            report=report,
            table_name="routes",
        )

    async def _upsert_trips(
        self, session: AsyncSession, data: list[dict[str, Any]], report: ImportReport
    ) -> None:
        """Batch upsert trips using ON CONFLICT (trip_id)."""
        await self._bulk_upsert(
            session=session,
            table="trips",
            columns=("trip_id", "route_id", "service_id", "direction_id"),
            conflict_cols=("trip_id",),
            update_cols=("route_id", "service_id", "direction_id"),
            data=data,
            report=report,
            table_name="trips",
        )

    async def _upsert_stop_times(
        self, session: AsyncSession, data: list[dict[str, Any]], report: ImportReport
    ) -> None:
        """Batch upsert stop_times using ON CONFLICT (trip_id, stop_sequence).

        Conflict key: unique constraint on (trip_id, stop_sequence).
        Updates stop_id + sched_arrival_sec when changes are detected.
        """
        await self._bulk_upsert(
            session=session,
            table="stop_times",
            columns=("trip_id", "stop_id", "stop_sequence", "sched_arrival_sec"),
            conflict_cols=("trip_id", "stop_sequence"),
            update_cols=("stop_id", "sched_arrival_sec"),
            data=data,
            report=report,
            table_name="stop_times",
        )

    async def _bulk_upsert(
        self,
        session: AsyncSession,
        table: str,
        columns: tuple[str, ...],
        conflict_cols: tuple[str, ...],
        update_cols: tuple[str, ...],
        data: list[dict[str, Any]],
        report: ImportReport,
        table_name: str,
    ) -> None:
        """Bulk upsert with accurate inserted/updated/skipped counts."""
        if not data:
            return

        column_list = ", ".join(columns)
        conflict_list = ", ".join(conflict_cols)
        update_set = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_cols)
        update_where = " OR ".join(
            f"EXCLUDED.{col} IS DISTINCT FROM {table}.{col}" for col in update_cols
        )

        for batch_start in range(0, len(data), self.batch_size):
            batch = data[batch_start : batch_start + self.batch_size]
            values_sql = ", ".join(
                "(" + ", ".join(f":{col}_{i}" for col in columns) + ")" for i in range(len(batch))
            )
            params: dict[str, Any] = {}
            for i, row in enumerate(batch):
                for col in columns:
                    params[f"{col}_{i}"] = row[col]

            stmt = text(
                f"""
                INSERT INTO {table} ({column_list})
                VALUES {values_sql}
                ON CONFLICT ({conflict_list}) DO UPDATE SET
                    {update_set}
                WHERE {update_where}
                RETURNING (xmax = 0) AS inserted
                """
            )

            try:
                result = await session.execute(stmt, params)
                rows = result.fetchall()
                inserted = sum(1 for row in rows if row[0])
                updated = len(rows) - inserted
                skipped = len(batch) - len(rows)

                report.counts[table_name]["inserted"] += inserted
                report.counts[table_name]["updated"] += updated
                report.counts[table_name]["skipped"] += skipped

                await session.commit()
            except Exception as exc:
                await session.rollback()
                msg = f"{table_name} batch upsert failed: {exc}"
                logger.error(msg, exc_info=exc)
                report.errors.append(msg)
                raise

        logger.info(
            "Upserted table",
            table=table_name,
            inserted=report.counts[table_name]["inserted"],
            updated=report.counts[table_name]["updated"],
            skipped=report.counts[table_name]["skipped"],
        )

    async def _get_last_feed_hash(self, session: AsyncSession) -> str | None:
        """Get the last imported feed hash from metadata table (if exists)."""
        try:
            result = await session.execute(
                text("SELECT feed_hash FROM gtfs_import_log ORDER BY imported_at DESC LIMIT 1")
            )
            row = result.fetchone()
            return row[0] if row else None
        except Exception as exc:
            # Table may not exist yet; that's fine, but reset failed transaction.
            logger.warning(
                "Could not read feed hash (table may not exist)",
                error=str(exc),
            )
            await session.rollback()
            return None

    async def _store_feed_hash(self, session: AsyncSession, feed_hash: str) -> None:
        """Store the feed hash in the import log table."""
        try:
            await session.execute(
                text("""
                    INSERT INTO gtfs_import_log (feed_hash, imported_at)
                    VALUES (:feed_hash, :imported_at)
                """),
                {"feed_hash": feed_hash, "imported_at": datetime.now(timezone.utc)},
            )
            await session.commit()
        except Exception as exc:
            logger.warning("Could not store feed hash (table may not exist)", error=str(exc))
            await session.rollback()
