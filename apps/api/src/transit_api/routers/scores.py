"""Stage 6/7: reliability score read endpoints and aggregation admin trigger.

Endpoints
---------
GET  /scores                  – score card for a specific bucket
GET  /scores/nearby-risky     – risky stops near a lat/lon  (paginated)
GET  /scores/trend            – daily score series for stop+route
GET  /meta/last-agg           – last aggregation run summary
POST /admin/agg/run           – trigger an aggregation run
"""

from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Annotated, Any, Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from transit_api.config import get_settings
from transit_api.database import get_session_context
from transit_api.logging import get_logger
from transit_api.services.aggregation.engine import haversine_bounding_box, run_aggregation
from transit_api.services.aggregation.scorer import compute_score

logger = get_logger(__name__)

router = APIRouter(tags=["scores"])

# ---------------------------------------------------------------------------
# Shared type aliases
# ---------------------------------------------------------------------------

DayType = Literal["weekday", "saturday", "sunday"]
HourBucket = Literal["6-9", "9-12", "12-15", "15-18", "18-21"]


# ---------------------------------------------------------------------------
# Smart defaults: infer day_type / hour_bucket from current local time
# ---------------------------------------------------------------------------

try:
    from zoneinfo import ZoneInfo as _ZoneInfo

    def _current_day_type(tz: str) -> DayType:
        now = datetime.now(_ZoneInfo(tz))
        dow = now.weekday()  # 0=Mon … 6=Sun
        if dow < 5:
            return "weekday"
        return "saturday" if dow == 5 else "sunday"

    def _current_hour_bucket(tz: str) -> HourBucket | None:
        hour = datetime.now(_ZoneInfo(tz)).hour
        if 6 <= hour <= 8:
            return "6-9"
        if 9 <= hour <= 11:
            return "9-12"
        if 12 <= hour <= 14:
            return "12-15"
        if 15 <= hour <= 17:
            return "15-18"
        if 18 <= hour <= 20:
            return "18-21"
        return None

except Exception:
    # Fallback when system timezone data is unavailable (e.g. Windows dev without tzdata)
    def _current_day_type(tz: str) -> DayType:  # type: ignore[misc]
        dow = datetime.now(timezone.utc).weekday()
        if dow < 5:
            return "weekday"
        return "saturday" if dow == 5 else "sunday"

    def _current_hour_bucket(tz: str) -> HourBucket | None:  # type: ignore[misc]
        hour = datetime.now(timezone.utc).hour
        if 6 <= hour <= 8:
            return "6-9"
        if 9 <= hour <= 11:
            return "9-12"
        if 12 <= hour <= 14:
            return "12-15"
        if 15 <= hour <= 17:
            return "15-18"
        if 18 <= hour <= 20:
            return "18-21"
        return None


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class ScoreCard(BaseModel):
    stop_id: str
    route_id: str
    day_type: str
    hour_bucket: str
    on_time_rate: float
    p50_delay_sec: int
    p95_delay_sec: int
    score: int
    sample_n: int
    updated_at: datetime
    low_confidence: bool = Field(
        description="True when sample_n < MIN_SAMPLES config value"
    )


class RiskyStop(BaseModel):
    stop_id: str
    stop_name: str
    lat: float
    lon: float
    route_id: str
    day_type: str
    hour_bucket: str
    score: int
    on_time_rate: float
    sample_n: int
    distance_m: float
    updated_at: datetime


class NearbyRiskyResponse(BaseModel):
    items: list[RiskyStop]
    limit: int
    count: int


class TrendPoint(BaseModel):
    service_date: date
    score: int
    sample_n: int
    on_time_rate: float
    p50_delay_sec: int
    p95_delay_sec: int


class TrendResponse(BaseModel):
    stop_id: str
    route_id: str
    days: int
    series: list[TrendPoint]


class LastAggResponse(BaseModel):
    last_run_at: Optional[datetime]
    lookback_days: Optional[int]
    rows_scanned: Optional[int]
    buckets_updated: Optional[int]
    status: Optional[str]
    message: str


class AggRunRequest(BaseModel):
    lookback_days: Optional[int] = Field(
        default=None,
        ge=1,
        le=365,
        description="Override lookback window (default from config)",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, compute but do not write to score_agg",
    )


class AggRunResponse(BaseModel):
    started_at: str
    lookback_days: int
    rows_scanned: int
    buckets_updated: int
    duration_ms: int
    dry_run: bool
    errors: int


# ---------------------------------------------------------------------------
# GET /scores
# ---------------------------------------------------------------------------

@router.get("/scores", response_model=ScoreCard, summary="Get reliability score card")
async def get_score(
    stop_id: str,
    route_id: str,
    day_type: DayType,
    hour_bucket: HourBucket,
) -> dict[str, Any]:
    """Return the pre-computed reliability score for a stop+route+bucket."""
    settings = get_settings()

    async with get_session_context() as session:
        result = await session.execute(
            text("""
                SELECT stop_id, route_id, day_type, hour_bucket,
                       on_time_rate::float, p50_delay_sec, p95_delay_sec,
                       score, sample_n, updated_at
                FROM score_agg
                WHERE stop_id = :stop_id
                  AND route_id = :route_id
                  AND day_type = :day_type
                  AND hour_bucket = :hour_bucket
            """),
            {
                "stop_id": stop_id,
                "route_id": route_id,
                "day_type": day_type,
                "hour_bucket": hour_bucket,
            },
        )
        row = result.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="No score available for this bucket yet.",
        )

    return {
        "stop_id": row.stop_id,
        "route_id": row.route_id,
        "day_type": row.day_type,
        "hour_bucket": row.hour_bucket,
        "on_time_rate": float(row.on_time_rate),
        "p50_delay_sec": int(row.p50_delay_sec),
        "p95_delay_sec": int(row.p95_delay_sec),
        "score": int(row.score),
        "sample_n": int(row.sample_n),
        "updated_at": row.updated_at,
        "low_confidence": int(row.sample_n) < settings.min_samples,
    }


# ---------------------------------------------------------------------------
# GET /scores/nearby-risky
# ---------------------------------------------------------------------------

@router.get(
    "/scores/nearby-risky",
    response_model=NearbyRiskyResponse,
    summary="Find risky stops near a location",
    description=(
        "Return stops near a location ordered by lowest reliability score "
        "(riskiest first), then by distance.  Each stop shows its single "
        "worst-scoring route for the requested bucket. "
        "If day_type/hour_bucket are omitted they default to the current "
        "local time in the service timezone (America/Vancouver)."
    ),
)
async def get_nearby_risky(
    lat: Annotated[float, Query(ge=-90, le=90)],
    lon: Annotated[float, Query(ge=-180, le=180)],
    radius_km: Annotated[float, Query(ge=0.05, le=10.0)] = 1.0,
    limit: Annotated[int, Query(ge=1, le=100)] = 25,
    day_type: Optional[DayType] = None,
    hour_bucket: Optional[HourBucket] = None,
    min_samples: Annotated[
        int,
        Query(ge=0, description="Minimum sample_n to include (filters low-data buckets)"),
    ] = 20,
) -> dict[str, Any]:
    """Return nearby stops ordered by lowest reliability score (riskiest first).

    Uses a bounding-box pre-filter on ix_stops_lat_lon to avoid a full table
    scan, then applies the exact Haversine formula to filter by radius.

    Worst-route per stop
    --------------------
    For stops served by multiple routes, only the route with the lowest score
    is returned.  This keeps the payload focused on the riskiest connection
    and avoids duplicating stops in the response.
    """
    settings = get_settings()

    # Smart defaults based on current local time
    if day_type is None:
        day_type = _current_day_type(settings.service_timezone)
    if hour_bucket is None:
        hour_bucket = _current_hour_bucket(settings.service_timezone)
        if hour_bucket is None:
            # Outside all five service windows — return empty results
            return {"items": [], "limit": limit, "count": 0}

    lat_min, lat_max, lon_min, lon_max = haversine_bounding_box(lat, lon, radius_km)

    async with get_session_context() as session:
        result = await session.execute(
            text("""
                WITH candidates AS (
                    SELECT
                        s.stop_id,
                        s.name        AS stop_name,
                        s.lat::float  AS lat,
                        s.lon::float  AS lon,
                        sa.route_id,
                        sa.day_type,
                        sa.hour_bucket,
                        sa.score,
                        sa.on_time_rate::float AS on_time_rate,
                        sa.sample_n,
                        sa.updated_at,
                        (
                            6371000.0 * 2.0 * ASIN(SQRT(
                                POWER(SIN(RADIANS((s.lat::float - :lat) / 2.0)), 2)
                                + COS(RADIANS(:lat)) * COS(RADIANS(s.lat::float))
                                * POWER(SIN(RADIANS((s.lon::float - :lon) / 2.0)), 2)
                            ))
                        ) AS distance_m
                    FROM stops s
                    JOIN score_agg sa ON s.stop_id = sa.stop_id
                    WHERE
                        sa.day_type    = :day_type
                        AND sa.hour_bucket = :hour_bucket
                        AND sa.sample_n   >= :min_samples
                        AND s.lat BETWEEN :lat_min AND :lat_max
                        AND s.lon BETWEEN :lon_min AND :lon_max
                ),
                -- Keep only the worst route per stop (lowest score, tie-break by route_id)
                ranked AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY stop_id
                               ORDER BY score ASC, route_id ASC
                           ) AS rn
                    FROM candidates
                    WHERE distance_m <= :radius_m
                )
                SELECT
                    stop_id, stop_name, lat, lon, route_id,
                    day_type, hour_bucket, score, on_time_rate,
                    sample_n, distance_m, updated_at
                FROM ranked
                WHERE rn = 1
                ORDER BY score ASC, distance_m ASC
                LIMIT :lim
            """),
            {
                "lat": lat,
                "lon": lon,
                "day_type": day_type,
                "hour_bucket": hour_bucket,
                "min_samples": min_samples,
                "lat_min": lat_min,
                "lat_max": lat_max,
                "lon_min": lon_min,
                "lon_max": lon_max,
                "radius_m": radius_km * 1000.0,
                "lim": limit,
            },
        )
        rows = result.fetchall()

    items = [
        {
            "stop_id": r.stop_id,
            "stop_name": r.stop_name,
            "lat": float(r.lat),
            "lon": float(r.lon),
            "route_id": r.route_id,
            "day_type": r.day_type,
            "hour_bucket": r.hour_bucket,
            "score": int(r.score),
            "on_time_rate": float(r.on_time_rate),
            "sample_n": int(r.sample_n),
            "distance_m": round(float(r.distance_m), 1),
            "updated_at": r.updated_at,
        }
        for r in rows
    ]

    return {"items": items, "limit": limit, "count": len(items)}


# ---------------------------------------------------------------------------
# GET /scores/trend
# ---------------------------------------------------------------------------

@router.get(
    "/scores/trend",
    response_model=TrendResponse,
    summary="Get daily reliability trend for a stop+route",
    description=(
        "Compute per-day reliability metrics from matched_arrivals for the "
        "last N days.  Only days with at least one matched observation appear "
        "in the series.  Scores are computed on-the-fly from raw observations."
    ),
)
async def get_trend(
    stop_id: str,
    route_id: str,
    days: Annotated[int, Query(ge=1, le=30)] = None,
) -> dict[str, Any]:
    """Return daily reliability metrics for the past N days (default 7).

    Computes scores on-the-fly from matched_arrivals so the trend reflects
    actual observed data rather than pre-aggregated buckets.  Only days
    that have at least one matched observation appear in the response.

    Design rationale (Option A chosen)
    -----------------------------------
    Computing trend on-the-fly from matched_arrivals (≤30 day window,
    scoped by stop+route) is fast enough at this scale and avoids
    maintaining a separate daily aggregate table.  If query latency
    becomes an issue at production volume, promote to Option B
    (score_daily table with daily pre-aggregation).
    """
    settings = get_settings()
    if days is None:
        days = settings.trend_default_days

    async with get_session_context() as session:
        result = await session.execute(
            text("""
                SELECT
                    ma.service_date,
                    COUNT(*)                                                        AS sample_n,
                    (SUM(CASE WHEN ABS(ma.delay_sec) <= :threshold THEN 1 ELSE 0 END)::float
                     / COUNT(*))                                                    AS on_time_rate,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ma.delay_sec)     AS p50_delay_sec,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ma.delay_sec)     AS p95_delay_sec
                FROM matched_arrivals ma
                JOIN trips t ON ma.trip_id = t.trip_id
                WHERE
                    ma.stop_id        = :stop_id
                    AND t.route_id    = :route_id
                    AND ma.match_status = 'matched'
                    AND ma.service_date >= CURRENT_DATE - :days
                GROUP BY ma.service_date
                ORDER BY ma.service_date ASC
            """),
            {
                "stop_id": stop_id,
                "route_id": route_id,
                "threshold": settings.on_time_threshold_sec,
                "days": days,
            },
        )
        rows = result.fetchall()

    series = [
        {
            "service_date": r.service_date,
            "score": compute_score(
                float(r.on_time_rate),
                float(r.p95_delay_sec),
                float(r.p50_delay_sec),
                weight_on_time=settings.weight_on_time_rate,
                weight_p95=settings.weight_p95_component,
                weight_p50=settings.weight_p50_component,
                p95_cap=float(settings.p95_max_delay_sec),
                p50_cap=float(settings.p50_max_delay_sec),
            ),
            "sample_n": int(r.sample_n),
            "on_time_rate": float(r.on_time_rate),
            "p50_delay_sec": int(r.p50_delay_sec),
            "p95_delay_sec": int(r.p95_delay_sec),
        }
        for r in rows
    ]

    return {
        "stop_id": stop_id,
        "route_id": route_id,
        "days": days,
        "series": series,
    }


# ---------------------------------------------------------------------------
# GET /meta/last-agg
# ---------------------------------------------------------------------------

@router.get(
    "/meta/last-agg",
    response_model=LastAggResponse,
    summary="Get last aggregation run summary",
)
async def get_last_agg() -> dict[str, Any]:
    """Return the most recently completed (or failed) aggregation run."""
    async with get_session_context() as session:
        result = await session.execute(
            text("""
                SELECT started_at, finished_at, lookback_days,
                       rows_scanned, buckets_updated, status
                FROM agg_run_log
                WHERE status != 'running'
                ORDER BY started_at DESC
                LIMIT 1
            """)
        )
        row = result.fetchone()

    if row is None:
        return {
            "last_run_at": None,
            "lookback_days": None,
            "rows_scanned": None,
            "buckets_updated": None,
            "status": None,
            "message": "No aggregation run recorded yet",
        }

    return {
        "last_run_at": row.finished_at or row.started_at,
        "lookback_days": row.lookback_days,
        "rows_scanned": row.rows_scanned,
        "buckets_updated": row.buckets_updated,
        "status": row.status,
        "message": f"Last run {row.status} at {(row.finished_at or row.started_at).isoformat()}",
    }


# ---------------------------------------------------------------------------
# POST /admin/agg/run
# ---------------------------------------------------------------------------

@router.post(
    "/admin/agg/run",
    response_model=AggRunResponse,
    summary="Trigger a reliability score aggregation run",
)
async def trigger_agg_run(body: AggRunRequest) -> dict[str, Any]:
    """Execute one aggregation cycle and return a run summary.

    Set dry_run=true to compute metrics without writing to score_agg
    (useful for verifying data quality before committing).
    """
    logger.info(
        "Aggregation run triggered via API",
        lookback_days=body.lookback_days,
        dry_run=body.dry_run,
    )
    try:
        summary = await run_aggregation(
            lookback_days=body.lookback_days,
            dry_run=body.dry_run,
        )
    except Exception as exc:
        logger.error("Aggregation run API error", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {exc}") from exc

    return summary
