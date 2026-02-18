"""Tests for Stage 6/7 score endpoints and aggregation admin trigger.

All tests mock the database session so no live DB is needed.  The mocking
pattern follows the established convention in test_gtfs_rt_api.py.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_ctx(rows: list[Any]) -> Any:
    """Return a mock get_session_context() that yields a session whose
    execute().fetchone() / fetchall() return the provided rows."""
    mock_result = MagicMock()
    if len(rows) == 1:
        mock_result.fetchone.return_value = rows[0]
        mock_result.fetchall.return_value = rows
    else:
        mock_result.fetchone.return_value = rows[0] if rows else None
        mock_result.fetchall.return_value = rows

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)

    @asynccontextmanager
    async def _ctx():
        yield mock_session

    return _ctx


def _row(**kwargs: Any) -> MagicMock:
    """Build a MagicMock row whose attributes mirror kwargs."""
    row = MagicMock()
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


# ---------------------------------------------------------------------------
# GET /scores
# ---------------------------------------------------------------------------


class TestGetScore:
    @pytest.mark.asyncio
    async def test_score_found(self, client: AsyncClient) -> None:
        now = datetime.now(timezone.utc)
        row = _row(
            stop_id="S1", route_id="R1", day_type="weekday", hour_bucket="9-12",
            on_time_rate=0.85, p50_delay_sec=30, p95_delay_sec=240,
            score=79, sample_n=120, updated_at=now,
        )
        with patch(
            "transit_api.routers.scores.get_session_context",
            return_value=_make_ctx([row])(),
        ):
            response = await client.get(
                "/scores",
                params={
                    "stop_id": "S1", "route_id": "R1",
                    "day_type": "weekday", "hour_bucket": "9-12",
                },
            )

        assert response.status_code == 200
        data = response.json()
        assert data["stop_id"] == "S1"
        assert data["route_id"] == "R1"
        assert data["score"] == 79
        assert data["sample_n"] == 120
        assert data["low_confidence"] is False  # 120 >= min_samples (20)

    @pytest.mark.asyncio
    async def test_score_not_found_returns_404(self, client: AsyncClient) -> None:
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            response = await client.get(
                "/scores",
                params={
                    "stop_id": "MISSING", "route_id": "R1",
                    "day_type": "weekday", "hour_bucket": "9-12",
                },
            )

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_low_confidence_flag(self, client: AsyncClient) -> None:
        now = datetime.now(timezone.utc)
        row = _row(
            stop_id="S2", route_id="R1", day_type="saturday", hour_bucket="6-9",
            on_time_rate=0.5, p50_delay_sec=60, p95_delay_sec=400,
            score=50, sample_n=5, updated_at=now,
        )
        with patch(
            "transit_api.routers.scores.get_session_context",
            return_value=_make_ctx([row])(),
        ):
            response = await client.get(
                "/scores",
                params={
                    "stop_id": "S2", "route_id": "R1",
                    "day_type": "saturday", "hour_bucket": "6-9",
                },
            )

        assert response.status_code == 200
        assert response.json()["low_confidence"] is True  # 5 < 20

    @pytest.mark.asyncio
    async def test_invalid_day_type_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/scores",
            params={
                "stop_id": "S1", "route_id": "R1",
                "day_type": "holiday",  # not a valid DayType literal
                "hour_bucket": "9-12",
            },
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_invalid_hour_bucket_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/scores",
            params={
                "stop_id": "S1", "route_id": "R1",
                "day_type": "weekday",
                "hour_bucket": "3-6",  # not a valid HourBucket literal
            },
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# GET /scores/nearby-risky  (Stage 7: paginated response)
# ---------------------------------------------------------------------------


class TestNearbyRisky:
    @pytest.mark.asyncio
    async def test_returns_sorted_by_score(self, client: AsyncClient) -> None:
        now = datetime.now(timezone.utc)
        # Note: rows are returned pre-sorted by SQL; here SA row (score=45) first
        rows = [
            _row(
                stop_id="SA", stop_name="Stop A", lat=49.2827, lon=-123.1207,
                route_id="R1", day_type="weekday", hour_bucket="9-12",
                score=45, on_time_rate=0.6, sample_n=100,
                distance_m=200.0, updated_at=now,
            ),
            _row(
                stop_id="SB", stop_name="Stop B", lat=49.283, lon=-123.121,
                route_id="R2", day_type="weekday", hour_bucket="9-12",
                score=72, on_time_rate=0.8, sample_n=80,
                distance_m=400.0, updated_at=now,
            ),
        ]
        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            response = await client.get(
                "/scores/nearby-risky",
                params={
                    "lat": 49.2827, "lon": -123.1207,
                    "day_type": "weekday", "hour_bucket": "9-12",
                },
            )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "limit" in data
        assert "count" in data
        assert data["count"] == 2
        # First entry should have lowest score (45 < 72)
        assert data["items"][0]["score"] == 45
        assert data["items"][0]["stop_id"] == "SA"

    @pytest.mark.asyncio
    async def test_radius_too_large_returns_422(self, client: AsyncClient) -> None:
        response = await client.get(
            "/scores/nearby-risky",
            params={"lat": 49.28, "lon": -123.12, "radius_km": 999},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_empty_result_is_empty_list(self, client: AsyncClient) -> None:
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            response = await client.get(
                "/scores/nearby-risky",
                params={
                    "lat": 49.28, "lon": -123.12,
                    "day_type": "weekday", "hour_bucket": "9-12",
                },
            )

        assert response.status_code == 200
        data = response.json()
        assert data["items"] == []
        assert data["count"] == 0

    @pytest.mark.asyncio
    async def test_invalid_lat_rejected(self, client: AsyncClient) -> None:
        response = await client.get(
            "/scores/nearby-risky",
            params={"lat": 999, "lon": -123.12},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_distance_m_field_present(self, client: AsyncClient) -> None:
        """Verify response items include distance_m (in metres)."""
        now = datetime.now(timezone.utc)
        rows = [
            _row(
                stop_id="SC", stop_name="Stop C", lat=49.28, lon=-123.12,
                route_id="R1", day_type="weekday", hour_bucket="9-12",
                score=60, on_time_rate=0.7, sample_n=50,
                distance_m=150.5, updated_at=now,
            ),
        ]
        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            response = await client.get(
                "/scores/nearby-risky",
                params={
                    "lat": 49.28, "lon": -123.12,
                    "day_type": "weekday", "hour_bucket": "9-12",
                },
            )

        assert response.status_code == 200
        item = response.json()["items"][0]
        assert "distance_m" in item
        assert item["distance_m"] == 150.5


# ---------------------------------------------------------------------------
# GET /scores/trend  (Stage 7: envelope response)
# ---------------------------------------------------------------------------


class TestTrend:
    @pytest.mark.asyncio
    async def test_returns_trend_envelope(self, client: AsyncClient) -> None:
        rows = [
            _row(
                service_date=date(2026, 2, 10),
                sample_n=50, on_time_rate=0.9,
                p50_delay_sec=20, p95_delay_sec=180,
            ),
            _row(
                service_date=date(2026, 2, 11),
                sample_n=45, on_time_rate=0.75,
                p50_delay_sec=50, p95_delay_sec=300,
            ),
        ]
        mock_result = MagicMock()
        mock_result.fetchall.return_value = rows
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            response = await client.get(
                "/scores/trend",
                params={"stop_id": "S1", "route_id": "R1", "days": 7},
            )

        assert response.status_code == 200
        data = response.json()
        # Envelope fields
        assert data["stop_id"] == "S1"
        assert data["route_id"] == "R1"
        assert data["days"] == 7
        assert "series" in data
        assert len(data["series"]) == 2
        assert data["series"][0]["service_date"] == "2026-02-10"
        assert "score" in data["series"][0]
        assert "sample_n" in data["series"][0]
        assert "on_time_rate" in data["series"][0]
        # scores must be in [0, 100]
        for point in data["series"]:
            assert 0 <= point["score"] <= 100

    @pytest.mark.asyncio
    async def test_empty_trend(self, client: AsyncClient) -> None:
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            response = await client.get(
                "/scores/trend",
                params={"stop_id": "S1", "route_id": "R1"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["stop_id"] == "S1"
        assert data["route_id"] == "R1"
        assert data["series"] == []

    @pytest.mark.asyncio
    async def test_days_param_validated(self, client: AsyncClient) -> None:
        # days must be 1–30
        response = await client.get(
            "/scores/trend",
            params={"stop_id": "S1", "route_id": "R1", "days": 31},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_trend_score_deterministic(self, client: AsyncClient) -> None:
        """Same row data must produce the same score on repeated calls."""
        row = _row(
            service_date=date(2026, 2, 15),
            sample_n=100, on_time_rate=0.8,
            p50_delay_sec=60, p95_delay_sec=300,
        )
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [row]
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            r1 = await client.get(
                "/scores/trend", params={"stop_id": "S1", "route_id": "R1"}
            )
        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            r2 = await client.get(
                "/scores/trend", params={"stop_id": "S1", "route_id": "R1"}
            )

        assert r1.json()["series"][0]["score"] == r2.json()["series"][0]["score"]


# ---------------------------------------------------------------------------
# GET /meta/last-agg
# ---------------------------------------------------------------------------


class TestLastAgg:
    @pytest.mark.asyncio
    async def test_no_runs_yet(self, client: AsyncClient) -> None:
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            response = await client.get("/meta/last-agg")

        assert response.status_code == 200
        data = response.json()
        assert data["last_run_at"] is None
        assert "No aggregation run recorded" in data["message"]

    @pytest.mark.asyncio
    async def test_with_successful_run(self, client: AsyncClient) -> None:
        now = datetime.now(timezone.utc)
        row = _row(
            started_at=now, finished_at=now,
            lookback_days=14, rows_scanned=1500,
            buckets_updated=200, status="success",
        )
        mock_result = MagicMock()
        mock_result.fetchone.return_value = row
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=mock_result)

        @asynccontextmanager
        async def _ctx():
            yield mock_session

        with patch("transit_api.routers.scores.get_session_context", return_value=_ctx()):
            response = await client.get("/meta/last-agg")

        assert response.status_code == 200
        data = response.json()
        assert data["rows_scanned"] == 1500
        assert data["buckets_updated"] == 200
        assert data["status"] == "success"
        assert data["lookback_days"] == 14


# ---------------------------------------------------------------------------
# POST /admin/agg/run
# ---------------------------------------------------------------------------


class TestAggRun:
    @pytest.mark.asyncio
    async def test_dry_run(self, client: AsyncClient) -> None:
        summary = {
            "started_at": "2026-02-18T00:00:00+00:00",
            "lookback_days": 14,
            "rows_scanned": 800,
            "buckets_updated": 800,
            "duration_ms": 350,
            "dry_run": True,
            "errors": 0,
        }
        with patch(
            "transit_api.routers.scores.run_aggregation",
            new=AsyncMock(return_value=summary),
        ):
            response = await client.post(
                "/admin/agg/run",
                json={"dry_run": True},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["dry_run"] is True
        assert data["rows_scanned"] == 800
        assert data["errors"] == 0

    @pytest.mark.asyncio
    async def test_real_run(self, client: AsyncClient) -> None:
        summary = {
            "started_at": "2026-02-18T00:00:00+00:00",
            "lookback_days": 7,
            "rows_scanned": 500,
            "buckets_updated": 120,
            "duration_ms": 900,
            "dry_run": False,
            "errors": 0,
        }
        with patch(
            "transit_api.routers.scores.run_aggregation",
            new=AsyncMock(return_value=summary),
        ):
            response = await client.post(
                "/admin/agg/run",
                json={"lookback_days": 7, "dry_run": False},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["buckets_updated"] == 120
        assert data["lookback_days"] == 7

    @pytest.mark.asyncio
    async def test_lookback_days_validation(self, client: AsyncClient) -> None:
        # lookback_days must be 1–365
        response = await client.post("/admin/agg/run", json={"lookback_days": 0})
        assert response.status_code == 422

        response = await client.post("/admin/agg/run", json={"lookback_days": 366})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_engine_error_returns_500(self, client: AsyncClient) -> None:
        with patch(
            "transit_api.routers.scores.run_aggregation",
            new=AsyncMock(side_effect=RuntimeError("DB connection lost")),
        ):
            response = await client.post("/admin/agg/run", json={"dry_run": True})

        assert response.status_code == 500
        assert "DB connection lost" in response.json()["detail"]
