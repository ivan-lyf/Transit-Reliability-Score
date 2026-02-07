"""Tests for GTFS-RT API endpoints."""

from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient


class TestLastIngestEndpoint:
    """Tests for GET /meta/last-ingest."""

    @pytest.mark.asyncio
    async def test_last_ingest_returns_200(self, client: AsyncClient) -> None:
        with patch("transit_api.routers.ingest.get_session_context") as mock_ctx:
            mock_session = AsyncMock()
            mock_result = AsyncMock()
            mock_result.fetchall.return_value = []
            mock_session.execute = AsyncMock(return_value=mock_result)
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            response = await client.get("/meta/last-ingest")

        assert response.status_code == 200
        data = response.json()
        assert "feeds" in data
        assert "stale_threshold_sec" in data
        assert isinstance(data["feeds"], list)

    @pytest.mark.asyncio
    async def test_last_ingest_with_data(self, client: AsyncClient) -> None:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)

        with patch("transit_api.routers.ingest.get_session_context") as mock_ctx:
            mock_session = AsyncMock()
            mock_result = AsyncMock()
            mock_result.fetchall.return_value = [
                ("trip_updates", now, now, "ok", "", 42, "abc123"),
                ("vehicle_positions", now, now, "ok", "", 15, "def456"),
            ]
            mock_session.execute = AsyncMock(return_value=mock_result)
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            response = await client.get("/meta/last-ingest")

        data = response.json()
        assert len(data["feeds"]) == 2
        assert data["feeds"][0]["feed_type"] == "trip_updates"
        assert data["feeds"][0]["status"] == "ok"
        assert data["feeds"][0]["entity_count"] == 42
        assert data["feeds"][0]["is_fresh"] is True

    @pytest.mark.asyncio
    async def test_last_ingest_stale_feed(self, client: AsyncClient) -> None:
        from datetime import datetime, timedelta, timezone

        # Feed that succeeded 5 minutes ago (> default 120s threshold)
        old = datetime.now(timezone.utc) - timedelta(minutes=5)

        with patch("transit_api.routers.ingest.get_session_context") as mock_ctx:
            mock_session = AsyncMock()
            mock_result = AsyncMock()
            mock_result.fetchall.return_value = [
                ("trip_updates", old, old, "ok", "", 10, "abc"),
            ]
            mock_session.execute = AsyncMock(return_value=mock_result)
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            response = await client.get("/meta/last-ingest")

        data = response.json()
        assert data["feeds"][0]["is_fresh"] is False

    @pytest.mark.asyncio
    async def test_last_ingest_handles_missing_table(self, client: AsyncClient) -> None:
        with patch("transit_api.routers.ingest.get_session_context") as mock_ctx:
            mock_session = AsyncMock()
            mock_session.execute = AsyncMock(side_effect=Exception("relation does not exist"))
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            response = await client.get("/meta/last-ingest")

        assert response.status_code == 200
        data = response.json()
        assert data["feeds"] == []


class TestWorkerControlEndpoints:
    """Tests for admin worker control endpoints."""

    @pytest.mark.asyncio
    async def test_worker_status(self, client: AsyncClient) -> None:
        response = await client.get("/admin/ingest/gtfs-rt/status")
        assert response.status_code == 200
        data = response.json()
        assert "running" in data
        assert "poll_count" in data
        assert "poll_interval_sec" in data

    @pytest.mark.asyncio
    async def test_start_worker(self, client: AsyncClient) -> None:
        with patch("transit_api.routers.ingest.get_worker") as mock_get:
            mock_worker = AsyncMock()
            mock_worker.start = AsyncMock()
            mock_worker.get_status = AsyncMock(return_value={
                "running": True,
                "poll_count": 0,
                "last_poll_at": None,
                "poll_interval_sec": 30,
                "stale_threshold_sec": 120,
            })
            mock_get.return_value = mock_worker

            response = await client.post("/admin/ingest/gtfs-rt/start")

        assert response.status_code == 200
        data = response.json()
        assert data["running"] is True

    @pytest.mark.asyncio
    async def test_stop_worker(self, client: AsyncClient) -> None:
        with patch("transit_api.routers.ingest.get_worker") as mock_get:
            mock_worker = AsyncMock()
            mock_worker.stop = AsyncMock()
            mock_worker.get_status = AsyncMock(return_value={
                "running": False,
                "poll_count": 5,
                "last_poll_at": "2026-02-06T12:00:00+00:00",
                "poll_interval_sec": 30,
                "stale_threshold_sec": 120,
            })
            mock_get.return_value = mock_worker

            response = await client.post("/admin/ingest/gtfs-rt/stop")

        assert response.status_code == 200
        data = response.json()
        assert data["running"] is False

    @pytest.mark.asyncio
    async def test_run_once(self, client: AsyncClient) -> None:
        with patch("transit_api.routers.ingest.get_worker") as mock_get:
            mock_worker = AsyncMock()
            mock_worker.run_once = AsyncMock(return_value={
                "poll_id": "abc12345",
                "poll_count": 1,
                "started_at": "2026-02-06T12:00:00+00:00",
                "ended_at": "2026-02-06T12:00:02+00:00",
                "feeds": {
                    "trip_updates": {"status": "ok", "entity_count": 10, "rows_written": 10},
                    "vehicle_positions": {"status": "ok", "entity_count": 5, "rows_written": 5},
                    "service_alerts": {"status": "ok", "entity_count": 2, "rows_written": 2},
                },
            })
            mock_get.return_value = mock_worker

            response = await client.post("/admin/ingest/gtfs-rt/run-once")

        assert response.status_code == 200
        data = response.json()
        assert data["poll_id"] == "abc12345"
        assert "feeds" in data
        assert data["feeds"]["trip_updates"]["status"] == "ok"


class TestHealthEndpointWithWorker:
    """Tests for health endpoint with RT worker integration."""

    @pytest.mark.asyncio
    async def test_health_includes_rt_status(self, client: AsyncClient) -> None:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "gtfsRt" in data["checks"]
        assert "workerRunning" in data["checks"]["gtfsRt"]
        assert "pollCount" in data["checks"]["gtfsRt"]
        assert "lastPollAt" in data["checks"]["gtfsRt"]
