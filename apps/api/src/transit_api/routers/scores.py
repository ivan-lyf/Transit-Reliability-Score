"""Stage 6: reliability score read endpoints and aggregation admin trigger.

Endpoints
---------
GET  /scores                  – score card for a specific bucket
GET  /scores/nearby-risky     – risky stops near a lat/lon
GET  /scores/trend            – 7-day daily score series
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
# Response schemas
# ---------------------------------------------------------------------------

DayType = Literal["weekday", "saturday", "sunday"]
HourBucket = Literal["6-9", "9-12", "12-15", "15-18", "18-21"]


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
    distance_km: float
    updated_at: datetime


class TrendPoint(BaseModel):
    service_date: date
    score: int
    sample_n: int
    on_time_rate: float
    p50_delay_sec: int
    p95_delay_sec: int


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
            detail=f"No score found for stop={stop_id} route={route_id} "
                   f"day_type={day_type} hour_bucket={hour_bucket}",
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
    response_model=list[RiskyStop],
    summary="Find risky stops near a location",
)
async def get_nearby_risky(
    lat: Annotated[float, Query(ge=-90, le=90)],
    lon: Annotated[float, Query(ge=-180, le=180)],
    radius_km: Annotated[
        float,
        Query(gt=0),
    ] = None,
    day_type: DayType = "weekday",
    hour_bucket: HourBucket = "6-9",
    limit: Annotated[int, Query(ge=1, le=100)] = None,
) -> list[dict[str, Any]]:
    """Return nearby stops ordered by lowest reliability score (riskiest first).

    Uses a bounding-box pre-filter on ix_stops_lat_lon to avoid a full table
    scan, then applies the exact Haversine formula to filter by radius.
    """
    settings = get_settings()
    if radius_km is None:
        radius_km = settings.default_nearby_radius_km
    if radius_km > settings.max_nearby_radius_km:
        raise HTTPException(
            status_code=422,
            detail=f"radius_km must be <= {settings.max_nearby_radius_km}",
        )
    if limit is None:
        limit = settings.risky_stops_default_limit

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
                            6371.0 * 2.0 * ASIN(SQRT(
                                POWER(SIN(RADIANS((s.lat::float - :lat) / 2.0)), 2)
                                + COS(RADIANS(:lat)) * COS(RADIANS(s.lat::float))
                                * POWER(SIN(RADIANS((s.lon::float - :lon) / 2.0)), 2)
                            ))
                        ) AS distance_km
                    FROM stops s
                    JOIN score_agg sa ON s.stop_id = sa.stop_id
                    WHERE
                        sa.day_type    = :day_type
                        AND sa.hour_bucket = :hour_bucket
                        AND s.lat BETWEEN :lat_min AND :lat_max
                        AND s.lon BETWEEN :lon_min AND :lon_max
                )
                SELECT *
                FROM candidates
                WHERE distance_km <= :radius_km
                ORDER BY score ASC, distance_km ASC
                LIMIT :lim
            """),
            {
                "lat": lat,
                "lon": lon,
                "day_type": day_type,
                "hour_bucket": hour_bucket,
                "lat_min": lat_min,
                "lat_max": lat_max,
                "lon_min": lon_min,
                "lon_max": lon_max,
                "radius_km": radius_km,
                "lim": limit,
            },
        )
        rows = result.fetchall()

    return [
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
            "distance_km": round(float(r.distance_km), 3),
            "updated_at": r.updated_at,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# GET /scores/trend
# ---------------------------------------------------------------------------

@router.get(
    "/scores/trend",
    response_model=list[TrendPoint],
    summary="Get 7-day daily reliability trend",
)
async def get_trend(
    stop_id: str,
    route_id: str,
    days: Annotated[int, Query(ge=1, le=90)] = None,
) -> list[dict[str, Any]]:
    """Return daily reliability metrics for the past N days (default 7).

    Computes scores on-the-fly from matched_arrivals so the trend reflects
    actual observed data rather than pre-aggregated buckets.  Only days
    that have at least one matched observation appear in the response.

    Design rationale (Option A chosen)
    -----------------------------------
    Computing trend on-the-fly from matched_arrivals (≤90 day window,
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

    return [
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
