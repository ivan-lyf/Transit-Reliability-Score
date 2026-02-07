"""Core matching engine: maps GTFS-RT trip updates to scheduled stop times."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from transit_api.config import get_settings
from transit_api.database import get_session_context
from transit_api.logging import get_logger

logger = get_logger(__name__)


@dataclass
class MatchingReport:
    """Summary of a matching run."""

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: str = ""
    ended_at: str = ""
    duration_ms: int = 0
    scanned_count: int = 0
    matched_count: int = 0
    unmatched_count: int = 0
    ambiguous_count: int = 0
    deduped_count: int = 0
    error_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_ms": self.duration_ms,
            "scanned_count": self.scanned_count,
            "matched_count": self.matched_count,
            "unmatched_count": self.unmatched_count,
            "ambiguous_count": self.ambiguous_count,
            "deduped_count": self.deduped_count,
            "error_count": self.error_count,
        }


def compute_service_date(
    feed_ts: datetime,
    sched_arrival_sec: int,
) -> date:
    """Derive the GTFS service date from a feed timestamp.

    For overnight trips (sched_arrival_sec >= 86400), the service date is
    the *previous* calendar day, because GTFS encodes times past midnight
    relative to the service day (e.g. 25:30:00 = 1:30 AM the next day).

    Args:
        feed_ts: The feed timestamp (tz-aware, typically UTC).
        sched_arrival_sec: Scheduled arrival in seconds from midnight of service day.

    Returns:
        The service date as a date object.
    """
    d = feed_ts.date()
    if sched_arrival_sec >= 86400:
        d = d - timedelta(days=1)
    return d


def compute_scheduled_ts(
    service_date: date,
    sched_arrival_sec: int,
) -> datetime:
    """Build a tz-aware scheduled timestamp from service_date + sched_arrival_sec.

    Args:
        service_date: The GTFS service date.
        sched_arrival_sec: Seconds from midnight of service day (may be >86400).

    Returns:
        A tz-aware datetime in UTC.
    """
    midnight = datetime(
        service_date.year,
        service_date.month,
        service_date.day,
        tzinfo=timezone.utc,
    )
    return midnight + timedelta(seconds=sched_arrival_sec)


def compute_observed_ts(
    arrival_time_epoch: Optional[int],
    arrival_delay: Optional[int],
    scheduled_ts: datetime,
    feed_ts: datetime,
) -> datetime:
    """Determine the best observed arrival timestamp.

    Priority:
    1. arrival_time (unix epoch) if available and non-zero
    2. scheduled_ts + arrival_delay if delay is available
    3. feed_ts as last resort

    Args:
        arrival_time_epoch: Unix epoch from GTFS-RT arrival.time (may be None/0).
        arrival_delay: Seconds of delay from GTFS-RT arrival.delay (may be None).
        scheduled_ts: The computed scheduled timestamp.
        feed_ts: The feed timestamp as fallback.

    Returns:
        A tz-aware datetime.
    """
    if arrival_time_epoch is not None and arrival_time_epoch > 0:
        return datetime.fromtimestamp(arrival_time_epoch, tz=timezone.utc)
    if arrival_delay is not None:
        return scheduled_ts + timedelta(seconds=arrival_delay)
    return feed_ts


def compute_delay_sec(observed_ts: datetime, scheduled_ts: datetime) -> int:
    """Compute delay in seconds (positive = late, negative = early)."""
    return int((observed_ts - scheduled_ts).total_seconds())


def dedup_rt_updates(
    rows: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int]:
    """Deduplicate RT updates: keep latest feed_timestamp per (trip_id, stop_id, stop_sequence).

    Args:
        rows: List of RT update dicts with at minimum
              trip_id, stop_id, stop_sequence, feed_timestamp.

    Returns:
        (deduped_rows, dedup_count)
    """
    best: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
    for row in rows:
        key = (row["trip_id"], row["stop_id"], row["stop_sequence"])
        existing = best.get(key)
        if existing is None or row["feed_timestamp"] > existing["feed_timestamp"]:
            best[key] = row

    deduped = list(best.values())
    dedup_count = len(rows) - len(deduped)
    return deduped, dedup_count


def _classify_match(
    candidate_count: int,
    strict_mode: bool,
) -> Tuple[str, float]:
    """Classify a match result.

    Args:
        candidate_count: Number of schedule candidates found.
        strict_mode: If true, ambiguous matches are treated as unmatched.

    Returns:
        (match_status, match_confidence)
    """
    if candidate_count == 0:
        return "unmatched", 0.0
    if candidate_count == 1:
        return "matched", 1.0
    # Multiple candidates
    if strict_mode:
        return "unmatched", 0.0
    return "ambiguous", round(1.0 / candidate_count, 4)


class MatchingEngine:
    """Matches RT trip updates to scheduled stop times and persists results."""

    def __init__(
        self,
        *,
        window_minutes: Optional[int] = None,
        max_candidates: Optional[int] = None,
        batch_size: Optional[int] = None,
        strict_mode: Optional[bool] = None,
    ) -> None:
        settings = get_settings()
        self.window_minutes = (
            window_minutes if window_minutes is not None else settings.match_window_minutes
        )
        self.max_candidates = (
            max_candidates if max_candidates is not None else settings.match_max_candidates
        )
        self.batch_size = batch_size if batch_size is not None else settings.match_batch_size
        self.strict_mode = strict_mode if strict_mode is not None else settings.match_strict_mode

    async def run(self, *, session: Optional[AsyncSession] = None) -> MatchingReport:
        """Execute a full matching run.

        Args:
            session: Optional DB session (for testing). If None, creates one.

        Returns:
            MatchingReport with summary statistics.
        """
        report = MatchingReport()
        report.started_at = datetime.now(timezone.utc).isoformat()
        t0 = time.monotonic()

        logger.info(
            "Matching run started",
            run_id=report.run_id,
            window_minutes=self.window_minutes,
            batch_size=self.batch_size,
            strict_mode=self.strict_mode,
        )

        try:
            if session is not None:
                await self._run_with_session(session, report)
            else:
                async with get_session_context() as sess:
                    await self._run_with_session(sess, report)
        except Exception as exc:
            logger.error("Matching run failed", run_id=report.run_id, exc_info=exc)
            raise

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        report.duration_ms = elapsed_ms
        report.ended_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Matching run completed",
            run_id=report.run_id,
            duration_ms=report.duration_ms,
            scanned=report.scanned_count,
            matched=report.matched_count,
            unmatched=report.unmatched_count,
            ambiguous=report.ambiguous_count,
            deduped=report.deduped_count,
            errors=report.error_count,
        )

        return report

    async def _run_with_session(
        self,
        session: AsyncSession,
        report: MatchingReport,
    ) -> None:
        """Core matching logic operating within a session."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=self.window_minutes)

        # 1. Fetch RT trip updates within the time window
        rt_rows = await self._fetch_rt_updates(session, cutoff)
        report.scanned_count = len(rt_rows)

        if not rt_rows:
            logger.info("No RT updates to match", run_id=report.run_id)
            return

        # 2. Deduplicate
        deduped, dedup_count = dedup_rt_updates(rt_rows)
        report.deduped_count = dedup_count

        # 3. Process in batches
        for i in range(0, len(deduped), self.batch_size):
            batch = deduped[i : i + self.batch_size]
            await self._process_batch(session, batch, report)

    async def _fetch_rt_updates(
        self,
        session: AsyncSession,
        cutoff: datetime,
    ) -> List[Dict[str, Any]]:
        """Fetch RT trip updates within the matching window."""
        sql = text("""
            SELECT
                id,
                trip_id,
                stop_id,
                stop_sequence,
                arrival_delay,
                arrival_time,
                schedule_relationship,
                feed_timestamp,
                recorded_at
            FROM rt_trip_updates
            WHERE feed_timestamp >= :cutoff
              AND schedule_relationship = 'SCHEDULED'
            ORDER BY feed_timestamp DESC
        """)
        result = await session.execute(sql, {"cutoff": cutoff})
        rows = result.fetchall()
        return [
            {
                "id": row[0],
                "trip_id": row[1],
                "stop_id": row[2],
                "stop_sequence": row[3],
                "arrival_delay": row[4],
                "arrival_time": row[5],
                "schedule_relationship": row[6],
                "feed_timestamp": row[7],
                "recorded_at": row[8],
            }
            for row in rows
        ]

    async def _process_batch(
        self,
        session: AsyncSession,
        batch: List[Dict[str, Any]],
        report: MatchingReport,
    ) -> None:
        """Match a batch of RT updates against schedule and persist results."""
        # Collect unique trip_ids for schedule lookup
        trip_ids = list({row["trip_id"] for row in batch})

        # Fetch all relevant stop_times in one query
        schedule_map = await self._fetch_schedule_map(session, trip_ids)

        inserts: List[Dict[str, Any]] = []

        for rt_row in batch:
            try:
                result = self._match_single(rt_row, schedule_map)
                if result is not None:
                    inserts.append(result)
                    if result["match_status"] == "matched":
                        report.matched_count += 1
                    elif result["match_status"] == "ambiguous":
                        report.ambiguous_count += 1
                    else:
                        report.unmatched_count += 1
                else:
                    report.unmatched_count += 1
            except Exception:
                logger.warning(
                    "Error matching RT update",
                    rt_id=rt_row.get("id"),
                    trip_id=rt_row.get("trip_id"),
                    stop_id=rt_row.get("stop_id"),
                )
                report.error_count += 1

        if inserts:
            await self._persist_batch(session, inserts)

    async def _fetch_schedule_map(
        self,
        session: AsyncSession,
        trip_ids: List[str],
    ) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
        """Fetch stop_times for the given trips, grouped by (trip_id, stop_id).

        Returns a dict mapping (trip_id, stop_id) -> list of schedule candidates,
        each being {stop_sequence, sched_arrival_sec}.
        """
        if not trip_ids:
            return {}

        # Use ANY for array bind
        sql = text("""
            SELECT trip_id, stop_id, stop_sequence, sched_arrival_sec
            FROM stop_times
            WHERE trip_id = ANY(:trip_ids)
            ORDER BY trip_id, stop_sequence
        """)
        result = await session.execute(sql, {"trip_ids": trip_ids})
        rows = result.fetchall()

        schedule_map: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for row in rows:
            key = (row[0], row[1])
            entry = {"stop_sequence": row[2], "sched_arrival_sec": row[3]}
            if key not in schedule_map:
                schedule_map[key] = []
            schedule_map[key].append(entry)

        return schedule_map

    def _match_single(
        self,
        rt_row: Dict[str, Any],
        schedule_map: Dict[Tuple[str, str], List[Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        """Match a single RT update to the schedule.

        Returns a dict ready for insertion into matched_arrivals, or None
        if the RT row has missing keys.
        """
        trip_id = rt_row.get("trip_id", "")
        stop_id = rt_row.get("stop_id", "")
        rt_stop_seq = rt_row.get("stop_sequence", 0)
        feed_ts = rt_row["feed_timestamp"]

        # Validate required keys
        if not trip_id or not stop_id:
            logger.warning(
                "Skipping RT update with missing trip_id or stop_id",
                rt_id=rt_row.get("id"),
            )
            return None

        # Look up schedule candidates
        key = (trip_id, stop_id)
        candidates = schedule_map.get(key, [])

        # Limit candidates
        candidates = candidates[: self.max_candidates]

        candidate_count = len(candidates)
        match_status, match_confidence = _classify_match(candidate_count, self.strict_mode)

        if candidate_count == 0:
            # No schedule match found — produce an unmatched row
            svc_date = feed_ts.date()
            scheduled_ts = feed_ts  # placeholder
            observed_ts = compute_observed_ts(
                rt_row.get("arrival_time"),
                rt_row.get("arrival_delay"),
                scheduled_ts,
                feed_ts,
            )
            return {
                "trip_id": trip_id,
                "stop_id": stop_id,
                "stop_sequence": rt_stop_seq or 0,
                "service_date": svc_date,
                "scheduled_ts": scheduled_ts,
                "observed_ts": observed_ts,
                "delay_sec": 0,
                "match_status": match_status,
                "match_confidence": match_confidence,
                "source_feed_ts": feed_ts,
                "rt_trip_update_id": rt_row.get("id"),
            }

        # Pick best candidate: exact stop_sequence match, or lowest stop_sequence
        chosen = None
        if rt_stop_seq and rt_stop_seq > 0:
            for c in candidates:
                if c["stop_sequence"] == rt_stop_seq:
                    chosen = c
                    break

        if chosen is None:
            # Deterministic tiebreaker: lowest stop_sequence
            chosen = min(candidates, key=lambda c: c["stop_sequence"])

        sched_arrival_sec = chosen["sched_arrival_sec"]
        svc_date = compute_service_date(feed_ts, sched_arrival_sec)
        scheduled_ts = compute_scheduled_ts(svc_date, sched_arrival_sec)
        observed_ts = compute_observed_ts(
            rt_row.get("arrival_time"),
            rt_row.get("arrival_delay"),
            scheduled_ts,
            feed_ts,
        )
        delay_sec = compute_delay_sec(observed_ts, scheduled_ts)

        return {
            "trip_id": trip_id,
            "stop_id": stop_id,
            "stop_sequence": chosen["stop_sequence"],
            "service_date": svc_date,
            "scheduled_ts": scheduled_ts,
            "observed_ts": observed_ts,
            "delay_sec": delay_sec,
            "match_status": match_status,
            "match_confidence": match_confidence,
            "source_feed_ts": feed_ts,
            "rt_trip_update_id": rt_row.get("id"),
        }

    async def _persist_batch(
        self,
        session: AsyncSession,
        inserts: List[Dict[str, Any]],
    ) -> None:
        """Insert matched arrivals with ON CONFLICT for idempotency."""
        sql = text("""
            INSERT INTO matched_arrivals (
                trip_id, stop_id, stop_sequence, service_date,
                scheduled_ts, observed_ts, delay_sec,
                match_status, match_confidence,
                source_feed_ts, rt_trip_update_id
            ) VALUES (
                :trip_id, :stop_id, :stop_sequence, :service_date,
                :scheduled_ts, :observed_ts, :delay_sec,
                :match_status, :match_confidence,
                :source_feed_ts, :rt_trip_update_id
            )
            ON CONFLICT (trip_id, stop_id, stop_sequence, service_date)
            DO UPDATE SET
                scheduled_ts = EXCLUDED.scheduled_ts,
                observed_ts = EXCLUDED.observed_ts,
                delay_sec = EXCLUDED.delay_sec,
                match_status = EXCLUDED.match_status,
                match_confidence = EXCLUDED.match_confidence,
                source_feed_ts = EXCLUDED.source_feed_ts,
                rt_trip_update_id = EXCLUDED.rt_trip_update_id
        """)

        for row in inserts:
            await session.execute(sql, row)

        await session.commit()
