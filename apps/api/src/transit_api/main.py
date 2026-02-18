"""FastAPI application entry point."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from transit_api.config import get_settings
from transit_api.database import check_database_connection, close_database
from transit_api.logging import (
    bind_request_context,
    clear_request_context,
    get_logger,
    setup_logging,
)
from transit_api.routers.admin import router as admin_router
from transit_api.routers.ingest import router as ingest_router
from transit_api.services.gtfs_rt.worker import get_worker, reset_worker

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""
    setup_logging()
    logger.info("Starting Transit Reliability Score API")

    # Auto-start RT worker if configured
    settings = get_settings()
    if settings.gtfs_rt_auto_start:
        worker = get_worker()
        await worker.start()

    yield

    # Shutdown RT worker if running
    worker = get_worker()
    if worker.is_running:
        await worker.stop()
    reset_worker()

    logger.info("Shutting down Transit Reliability Score API")
    await close_database()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description=(
            "API for Transit Reliability Score - "
            "providing reliability metrics for Metro Vancouver (TransLink) transit"
        ),
        lifespan=lifespan,
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url="/redoc" if settings.environment != "production" else None,
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.environment == "development" else [],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Request ID middleware
    @app.middleware("http")
    async def request_context_middleware(request: Request, call_next: Any) -> Any:
        import uuid

        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        bind_request_context(request_id=request_id, path=request.url.path)

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

        clear_request_context()
        return response

    # Include routers
    app.include_router(admin_router)
    app.include_router(ingest_router)

    # Health endpoint
    @app.get("/health", tags=["meta"])
    async def health_check() -> dict[str, Any]:
        """Health check endpoint returning application status."""
        settings = get_settings()
        missing_env = settings.missing_required_env()
        db_healthy = await check_database_connection()

        # RT worker status
        worker = get_worker()
        worker_status = await worker.get_status()
        rt_healthy = worker_status["running"] or not settings.gtfs_rt_auto_start

        status = (
            "unhealthy" if missing_env else "healthy" if (db_healthy and rt_healthy) else "degraded"
        )

        issues: list[str] = []
        if missing_env:
            issues.append("Missing required environment variables: " + ", ".join(missing_env))
        if settings.gtfs_rt_auto_start and not worker_status["running"]:
            issues.append("GTFS-RT worker is not running")

        return {
            "service": settings.app_name,
            "status": status,
            "version": settings.app_version,
            "environment": settings.environment,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": {
                "database": db_healthy,
                "gtfsRt": {
                    "workerRunning": worker_status["running"],
                    "pollCount": worker_status["poll_count"],
                    "lastPollAt": worker_status["last_poll_at"],
                },
            },
            "issues": issues,
        }

    # Attribution endpoint
    @app.get("/meta/attribution", tags=["meta"])
    async def get_attribution() -> dict[str, str]:
        """Get data attribution information as required by TransLink."""
        settings = get_settings()
        return {
            "attribution": settings.data_attribution,
            "termsUrl": settings.translink_terms_url,
        }

    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error("Unhandled exception", exc_info=exc, path=request.url.path)
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_server_error",
                "message": "An unexpected error occurred",
            },
        )

    return app


app = create_app()
