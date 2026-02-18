"""Aggregation engine for Stage 6.

Reads matched_arrivals (Stage 5 output), groups by
(stop_id, route_id, day_type, hour_bucket), computes reliability metrics,
and upserts into score_agg with idempotent ON CONFLICT semantics.

Design notes
------------
- route_id is derived by JOINing matched_arrivals with trips on trip_id.
- Hour-bucket assignment uses scheduled_ts converted to the service timezone
  (config.service_timezone, default 'America/Vancouver').
- Trips outside the five hour windows are excluded from aggregation.
- A run-log entry is written to agg_run_log for /meta/last-agg.
- Repeated runs over the same data produce identical score_agg rows
  (idempotent UPSERT).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from time import monotonic
from typing import Any

from sqlalchemy import text

from transit_api.config import Settings, get_settings
from transit_api.database import get_session_context
from transit_api.logging import get_logger
from transit_api.services.aggregation.scorer import compute_score

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# SQL: aggregate matched_arrivals → per-bucket metrics
# ---------------------------------------------------------------------------
# Bucketing decisions:
#   • day_type  from service_date DOW (DOW=0 → Sunday in PostgreSQL)
#   • hour_bucket from EXTRACT(HOUR FROM scheduled_ts AT TIME ZONE :tz)
#     using the service IANA timezone.  Hours 6-8 → '6-9', etc.
#   • Only match_status = 'matched' rows are included.
#   • Scoped to service_date >= CURRENT_DATE - :lookback_days.
# PostgreSQL DOW convention: 0=Sunday, 1=Monday, … 6=Saturday.

_AGG_SQL = text("""
WITH base AS (
    SELECT
        ma.stop_id,
        t.route_id,
        CASE EXTRACT(DOW FROM ma.service_date)
            WHEN 0 THEN 'sunday'
            WHEN 6 THEN 'saturday'
            ELSE 'weekday'
        END AS day_type,
        CASE
            WHEN EXTRACT(HOUR FROM ma.scheduled_ts AT TIME ZONE :tz) BETWEEN 6  AND 8  THEN '6-9'
            WHEN EXTRACT(HOUR FROM ma.scheduled_ts AT TIME ZONE :tz) BETWEEN 9  AND 11 THEN '9-12'
            WHEN EXTRACT(HOUR FROM ma.scheduled_ts AT TIME ZONE :tz) BETWEEN 12 AND 14 THEN '12-15'
            WHEN EXTRACT(HOUR FROM ma.scheduled_ts AT TIME ZONE :tz) BETWEEN 15 AND 17 THEN '15-18'
            WHEN EXTRACT(HOUR FROM ma.scheduled_ts AT TIME ZONE :tz) BETWEEN 18 AND 20 THEN '18-21'
        END AS hour_bucket,
        ma.delay_sec
    FROM matched_arrivals ma
    JOIN trips t ON ma.trip_id = t.trip_id
    WHERE
        ma.match_status = 'matched'
        AND ma.service_date >= CURRENT_DATE - :lookback_days
)
SELECT
    stop_id,
    route_id,
    day_type,
    hour_bucket,
    COUNT(*)                                                                        AS sample_n,
    (SUM(CASE WHEN ABS(delay_sec) <= :on_time_threshold THEN 1 ELSE 0 END)::float
     / COUNT(*))                                                                    AS on_time_rate,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY delay_sec)                        AS p50_delay_sec,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delay_sec)                        AS p95_delay_sec
FROM base
WHERE hour_bucket IS NOT NULL
GROUP BY stop_id, route_id, day_type, hour_bucket
ORDER BY stop_id, route_id, day_type, hour_bucket
""")

# ---------------------------------------------------------------------------
# SQL: UPSERT one bucket row into score_agg
# ---------------------------------------------------------------------------
_UPSERT_SQL = text("""
INSERT INTO score_agg
    (stop_id, route_id, day_type, hour_bucket,
     on_time_rate, p50_delay_sec, p95_delay_sec, score, sample_n, updated_at)
VALUES
    (:stop_id, :route_id, :day_type, :hour_bucket,
     :on_time_rate, :p50_delay_sec, :p95_delay_sec, :score, :sample_n, NOW())
ON CONFLICT (stop_id, route_id, day_type, hour_bucket)
DO UPDATE SET
    on_time_rate  = EXCLUDED.on_time_rate,
    p50_delay_sec = EXCLUDED.p50_delay_sec,
    p95_delay_sec = EXCLUDED.p95_delay_sec,
    score         = EXCLUDED.score,
    sample_n      = EXCLUDED.sample_n,
    updated_at    = NOW()
""")

# ---------------------------------------------------------------------------
# SQL: run-log helpers
# ---------------------------------------------------------------------------
_INSERT_RUN_LOG = text("""
INSERT INTO agg_run_log (started_at, lookback_days, status)
VALUES (:started_at, :lookback_days, 'running')
RETURNING id
""")

_UPDATE_RUN_LOG = text("""
UPDATE agg_run_log
SET finished_at     = :finished_at,
    rows_scanned    = :rows_scanned,
    buckets_updated = :buckets_updated,
    status          = :status,
    error_message   = :error_message
WHERE id = :run_id
""")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def run_aggregation(
    lookback_days: int | None = None,
    dry_run: bool = False,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Compute reliability aggregates and upsert into score_agg.

    Args:
        lookback_days: Days of matched_arrivals to include (default from config).
        dry_run:       If True, compute metrics but skip the UPSERT and run-log.
        settings:      Optional Settings override (useful in tests).

    Returns:
        Summary dict with started_at, lookback_days, rows_scanned,
        buckets_updated, duration_ms, dry_run, errors.

    Idempotency
    -----------
    Running this function multiple times over the same matched_arrivals data
    produces identical score_agg rows because the UPSERT replaces all metric
    columns unconditionally.
    """
    if settings is None:
        settings = get_settings()

    lookback_days = lookback_days if lookback_days is not None else settings.agg_lookback_days
    start_ts = datetime.now(timezone.utc)
    t0 = monotonic()

    run_id: int | None = None
    rows_scanned = 0
    buckets_updated = 0
    status = "success"
    error_message = ""

    logger.info(
        "Aggregation run starting",
        lookback_days=lookback_days,
        dry_run=dry_run,
    )

    # 1. Insert run-log entry so we have an ID before the heavy query.
    if not dry_run:
        async with get_session_context() as session:
            result = await session.execute(
                _INSERT_RUN_LOG,
                {"started_at": start_ts, "lookback_days": lookback_days},
            )
            run_id = result.scalar_one()
            await session.commit()

    try:
        async with get_session_context() as session:
            # 2. Run the aggregation query (scoped to the lookback window).
            agg_result = await session.execute(
                _AGG_SQL,
                {
                    "tz": settings.service_timezone,
                    "lookback_days": lookback_days,
                    "on_time_threshold": settings.on_time_threshold_sec,
                },
            )
            rows = agg_result.fetchall()
            rows_scanned = len(rows)

            if not dry_run:
                # 3. Compute scores and batch-upsert in chunks.
                batch: list[dict[str, Any]] = []
                for row in rows:
                    score = compute_score(
                        float(row.on_time_rate),
                        float(row.p95_delay_sec),
                        float(row.p50_delay_sec),
                        weight_on_time=settings.weight_on_time_rate,
                        weight_p95=settings.weight_p95_component,
                        weight_p50=settings.weight_p50_component,
                        p95_cap=float(settings.p95_max_delay_sec),
                        p50_cap=float(settings.p50_max_delay_sec),
                    )
                    batch.append(
                        {
                            "stop_id": row.stop_id,
                            "route_id": row.route_id,
                            "day_type": row.day_type,
                            "hour_bucket": row.hour_bucket,
                            "on_time_rate": float(row.on_time_rate),
                            "p50_delay_sec": int(row.p50_delay_sec),
                            "p95_delay_sec": int(row.p95_delay_sec),
                            "score": score,
                            "sample_n": int(row.sample_n),
                        }
                    )
                    if len(batch) >= settings.agg_batch_size:
                        for params in batch:
                            await session.execute(_UPSERT_SQL, params)
                        buckets_updated += len(batch)
                        batch = []

                if batch:
                    for params in batch:
                        await session.execute(_UPSERT_SQL, params)
                    buckets_updated += len(batch)

                await session.commit()
            else:
                # dry_run: report what *would* be updated
                buckets_updated = rows_scanned

    except Exception as exc:
        status = "error"
        error_message = str(exc)
        logger.error("Aggregation run failed", error=error_message)
        raise

    finally:
        # 4. Update run-log (separate session so it commits even on error).
        if not dry_run and run_id is not None:
            try:
                async with get_session_context() as session:
                    await session.execute(
                        _UPDATE_RUN_LOG,
                        {
                            "run_id": run_id,
                            "finished_at": datetime.now(timezone.utc),
                            "rows_scanned": rows_scanned,
                            "buckets_updated": buckets_updated,
                            "status": status,
                            "error_message": error_message,
                        },
                    )
                    await session.commit()
            except Exception as log_exc:
                logger.error("Failed to update agg run log", error=str(log_exc))

    duration_ms = int((monotonic() - t0) * 1000)
    logger.info(
        "Aggregation run complete",
        rows_scanned=rows_scanned,
        buckets_updated=buckets_updated,
        duration_ms=duration_ms,
        dry_run=dry_run,
        status=status,
    )

    return {
        "started_at": start_ts.isoformat(),
        "lookback_days": lookback_days,
        "rows_scanned": rows_scanned,
        "buckets_updated": buckets_updated,
        "duration_ms": duration_ms,
        "dry_run": dry_run,
        "errors": 0 if status == "success" else 1,
    }


# ---------------------------------------------------------------------------
# Haversine helper (used in nearby-risky SQL generation)
# ---------------------------------------------------------------------------

def haversine_bounding_box(
    lat: float, lon: float, radius_km: float
) -> tuple[float, float, float, float]:
    """Return (lat_min, lat_max, lon_min, lon_max) bounding box.

    Used to pre-filter stops with the btree ix_stops_lat_lon index before
    applying the exact Haversine formula in SQL.
    """
    lat_delta = radius_km / 111.0
    lon_delta = radius_km / max(0.001, 111.0 * math.cos(math.radians(lat)))
    return (
        lat - lat_delta,
        lat + lat_delta,
        lon - lon_delta,
        lon + lon_delta,
    )
