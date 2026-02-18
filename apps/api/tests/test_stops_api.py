"""Tests for Stage 7 stops endpoints.

GET /stops/nearby           – nearby stop discovery (paginated)
GET /stops/{stop_id}/routes – routes serving a stop

All tests mock the database session so no live DB is needed.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

_PATCH_SESSION = "transit_api.routers.stops.get_session_context"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _row(**kwargs: Any) -> MagicMock:
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


def _mock_ctx(rows: list[Any], *, fetchone: Any = None) -> Any:
    """Return a factory that yields a mock session.

    If fetchone is explicitly supplied it is used; otherwise the first row
    (if any) is returned by fetchone.
    """
    mock_result = MagicMock()
    mock_result.fetchall.return_value = rows
    mock_result.fetchone.return_value = fetchone if fetchone is not None else (rows[0] if rows else None)

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    @asynccontextmanager
    async def _ctx():
        yield mock_session

    return _ctx


# ---------------------------------------------------------------------------
# GET /stops/nearby
# ---------------------------------------------------------------------------


class TestNearbyStops:
    @pytest.mark.asyncio
    async def test_returns_stops_ordered_by_distance(self, client: AsyncClient) -> None:
        rows = [
            _row(stop_id="SA", name="Stop A", lat=49.2827, lon=-123.1207, distance_m=120.0),
            _row(stop_id="SB", name="Stop B", lat=49.283, lon=-123.121, distance_m=350.0),
        ]
        with patch(_PATCH_SESSION, return_value=_mock_ctx(rows)()):
            response = await client.get(
                "/stops/nearby",
                params={"lat": 49.2827, "lon": -123.1207},
            )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "limit" in data
        assert "offset" in data
        assert "count" in data
        assert data["count"] == 2
        assert data["items"][0]["stop_id"] == "SA"
        assert data["items"][0]["distance_m"] == 120.0
        assert data["items"][1]["stop_id"] == "SB"

    @pytest.mark.asyncio
    async def test_response_schema(self, client: AsyncClient) -> None:
        """Each item must include stop_id, name, lat, lon, distance_m."""
        rows = [
            _row(stop_id="S1", name="Main St", lat=49.28, lon=-123.12, distance_m=50.0),
        ]
        with patch(_PATCH_SESSION, return_value=_mock_ctx(rows)()):
            response = await client.get(
                "/stops/nearby",
                params={"lat": 49.28, "lon": -123.12},
            )

        assert response.status_code == 200
        item = response.json()["items"][0]
        assert "stop_id" in item
        assert "name" in item
        assert "lat" in item
        assert "lon" in item
        assert "distance_m" in item

    @pytest.mark.asyncio
    async def test_empty_result(self, client: AsyncClient) -> None:
        with patch(_PATCH_SESSION, return_value=_mock_ctx([])()):
            response = await client.get(
                "/stops/nearby",
                params={"lat": 49.28, "lon": -123.12},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["count"] == 0

    @pytest.mark.asyncio
    async def test_pagination_params_reflected(self, client: AsyncClient) -> None:
        with patch(_PATCH_SESSION, return_value=_mock_ctx([])()):
            response = await client.get(
                "/stops/nearby",
                params={"lat": 49.28, "lon": -123.12, "limit": 10, "offset": 5},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["limit"] == 10
        assert data["offset"] == 5

    @pytest.mark.asyncio
    async def test_invalid_lat_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/stops/nearby",
            params={"lat": 999, "lon": -123.12},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_invalid_lon_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/stops/nearby",
            params={"lat": 49.28, "lon": 999},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_radius_too_large_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/stops/nearby",
            params={"lat": 49.28, "lon": -123.12, "radius_km": 11},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_radius_too_small_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/stops/nearby",
            params={"lat": 49.28, "lon": -123.12, "radius_km": 0.01},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_limit_too_large_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/stops/nearby",
            params={"lat": 49.28, "lon": -123.12, "limit": 201},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_missing_lat_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/stops/nearby",
            params={"lon": -123.12},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_missing_lon_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/stops/nearby",
            params={"lat": 49.28},
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# GET /stops/{stop_id}/routes
# ---------------------------------------------------------------------------


class TestStopRoutes:
    @pytest.mark.asyncio
    async def test_returns_routes_for_stop(self, client: AsyncClient) -> None:
        # Two execute calls: one EXISTS check, one routes query.
        exists_result = MagicMock()
        exists_result.fetchone.return_value = _row(stop_id="S1")

        routes_result = MagicMock()
        routes_result.fetchall.return_value = [
            _row(route_id="R1", short_name="99-B", long_name="B-Line"),
            _row(route_id="R2", short_name="R2", long_name="Express"),
        ]

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=[exists_result, routes_result])

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch(_PATCH_SESSION, return_value=_ctx()):
            response = await client.get("/stops/S1/routes")

        assert response.status_code == 200
        data = response.json()
        assert data["stop_id"] == "S1"
        assert len(data["routes"]) == 2
        assert data["routes"][0]["route_id"] == "R1"
        assert "short_name" in data["routes"][0]
        assert "long_name" in data["routes"][0]

    @pytest.mark.asyncio
    async def test_stop_not_found_returns_404(self, client: AsyncClient) -> None:
        exists_result = MagicMock()
        exists_result.fetchone.return_value = None

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=exists_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch(_PATCH_SESSION, return_value=_ctx()):
            response = await client.get("/stops/NONEXISTENT/routes")

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_stop_with_no_scheduled_routes_returns_empty(
        self, client: AsyncClient
    ) -> None:
        """A stop that exists but has no stop_times should return empty routes list."""
        exists_result = MagicMock()
        exists_result.fetchone.return_value = _row(stop_id="ISOLATED")

        routes_result = MagicMock()
        routes_result.fetchall.return_value = []

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=[exists_result, routes_result])

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch(_PATCH_SESSION, return_value=_ctx()):
            response = await client.get("/stops/ISOLATED/routes")

        assert response.status_code == 200
        data = response.json()
        assert data["stop_id"] == "ISOLATED"
        assert data["routes"] == []

    @pytest.mark.asyncio
    async def test_response_schema(self, client: AsyncClient) -> None:
        """Each route entry must include route_id, short_name, long_name."""
        exists_result = MagicMock()
        exists_result.fetchone.return_value = _row(stop_id="S2")

        routes_result = MagicMock()
        routes_result.fetchall.return_value = [
            _row(route_id="R3", short_name="3", long_name="Third Avenue"),
        ]

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=[exists_result, routes_result])

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch(_PATCH_SESSION, return_value=_ctx()):
            response = await client.get("/stops/S2/routes")

        assert response.status_code == 200
        route = response.json()["routes"][0]
        assert "route_id" in route
        assert "short_name" in route
        assert "long_name" in route
