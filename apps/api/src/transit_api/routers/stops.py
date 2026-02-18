"""Stage 7: Public stops endpoints.

Endpoints
---------
GET /stops/nearby              – paginated stops within a radius
GET /stops/{stop_id}/routes    – distinct routes serving a stop
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from transit_api.database import get_session_context
from transit_api.logging import get_logger
from transit_api.services.aggregation.engine import haversine_bounding_box

logger = get_logger(__name__)

router = APIRouter(tags=["stops"])


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class StopNearby(BaseModel):
    stop_id: str
    name: str
    lat: float
    lon: float
    distance_m: float


class NearbyStopsResponse(BaseModel):
    items: list[StopNearby]
    limit: int
    offset: int
    count: int


class RouteInfo(BaseModel):
    route_id: str
    short_name: str
    long_name: str


class StopRoutesResponse(BaseModel):
    stop_id: str
    routes: list[RouteInfo]


# ---------------------------------------------------------------------------
# GET /stops/nearby
# ---------------------------------------------------------------------------


@router.get(
    "/stops/nearby",
    response_model=NearbyStopsResponse,
    summary="Find stops near a location",
    description=(
        "Return transit stops within `radius_km` of the given coordinates, "
        "ordered by distance ascending.  Uses a bounding-box pre-filter on "
        "ix_stops_lat_lon and then applies the exact Haversine formula."
    ),
)
async def get_nearby_stops(
    lat: Annotated[
        float,
        Query(ge=-90, le=90, description="Latitude of the search centre"),
    ],
    lon: Annotated[
        float,
        Query(ge=-180, le=180, description="Longitude of the search centre"),
    ],
    radius_km: Annotated[
        float,
        Query(ge=0.05, le=10.0, description="Search radius in kilometres (0.05–10)"),
    ] = 0.75,
    limit: Annotated[
        int,
        Query(ge=1, le=200, description="Maximum number of stops to return"),
    ] = 50,
    offset: Annotated[
        int,
        Query(ge=0, description="Pagination offset"),
    ] = 0,
) -> dict[str, Any]:
    """Return stops within radius ordered by distance (nearest first)."""
    lat_min, lat_max, lon_min, lon_max = haversine_bounding_box(lat, lon, radius_km)
    radius_m = radius_km * 1000.0

    async with get_session_context() as session:
        result = await session.execute(
            text("""
                WITH candidates AS (
                    SELECT
                        s.stop_id,
                        s.name,
                        s.lat::float  AS lat,
                        s.lon::float  AS lon,
                        (
                            6371000.0 * 2.0 * ASIN(SQRT(
                                POWER(SIN(RADIANS((s.lat::float - :lat) / 2.0)), 2)
                                + COS(RADIANS(:lat)) * COS(RADIANS(s.lat::float))
                                * POWER(SIN(RADIANS((s.lon::float - :lon) / 2.0)), 2)
                            ))
                        ) AS distance_m
                    FROM stops s
                    WHERE
                        s.lat BETWEEN :lat_min AND :lat_max
                        AND s.lon BETWEEN :lon_min AND :lon_max
                )
                SELECT *
                FROM candidates
                WHERE distance_m <= :radius_m
                ORDER BY distance_m ASC
                LIMIT :lim OFFSET :off
            """),
            {
                "lat": lat,
                "lon": lon,
                "lat_min": lat_min,
                "lat_max": lat_max,
                "lon_min": lon_min,
                "lon_max": lon_max,
                "radius_m": radius_m,
                "lim": limit,
                "off": offset,
            },
        )
        rows = result.fetchall()

    items = [
        {
            "stop_id": r.stop_id,
            "name": r.name,
            "lat": float(r.lat),
            "lon": float(r.lon),
            "distance_m": round(float(r.distance_m), 1),
        }
        for r in rows
    ]

    return {
        "items": items,
        "limit": limit,
        "offset": offset,
        "count": len(items),
    }


# ---------------------------------------------------------------------------
# GET /stops/{stop_id}/routes
# ---------------------------------------------------------------------------


@router.get(
    "/stops/{stop_id}/routes",
    response_model=StopRoutesResponse,
    summary="Get routes serving a stop",
    description=(
        "Return the distinct transit routes that serve the given stop, "
        "derived via stop_times → trips → routes.  Ordered by short_name."
    ),
)
async def get_stop_routes(stop_id: str) -> dict[str, Any]:
    """Return distinct routes serving a stop, or 404 if stop not found."""
    async with get_session_context() as session:
        # Verify stop exists
        exists = await session.execute(
            text("SELECT 1 FROM stops WHERE stop_id = :stop_id LIMIT 1"),
            {"stop_id": stop_id},
        )
        if exists.fetchone() is None:
            raise HTTPException(
                status_code=404,
                detail=f"Stop '{stop_id}' not found",
            )

        # Fetch distinct routes via stop_times → trips → routes
        routes_result = await session.execute(
            text("""
                SELECT DISTINCT
                    r.route_id,
                    r.short_name,
                    r.long_name
                FROM stop_times st
                JOIN trips t   ON st.trip_id  = t.trip_id
                JOIN routes r  ON t.route_id  = r.route_id
                WHERE st.stop_id = :stop_id
                ORDER BY r.short_name
            """),
            {"stop_id": stop_id},
        )
        routes = routes_result.fetchall()

    return {
        "stop_id": stop_id,
        "routes": [
            {
                "route_id": r.route_id,
                "short_name": r.short_name,
                "long_name": r.long_name,
            }
            for r in routes
        ],
    }
