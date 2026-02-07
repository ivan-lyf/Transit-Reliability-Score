"""Admin routes for GTFS import and matching operations."""

from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from transit_api.config import get_settings
from transit_api.logging import get_logger
from transit_api.services.gtfs_static.fetcher import FetchError, InvalidZipError
from transit_api.services.gtfs_static.importer import GtfsImporter
from transit_api.services.gtfs_static.parser import MissingColumnError
from transit_api.services.gtfs_static.reader import MissingRequiredFileError
from transit_api.services.matching.engine import MatchingEngine

logger = get_logger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


class StaticGtfsImportRequest(BaseModel):
    """Request body for static GTFS import."""

    source_type: Literal["remote", "local"] = Field(
        default="remote",
        description="Source type: 'remote' for URL download, 'local' for filesystem path",
    )
    source: str = Field(
        default="",
        description="URL or local file path. Empty uses configured default.",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, parse and validate without writing to DB.",
    )
    strict: Optional[bool] = Field(
        default=None,
        description="If true, fail on first data integrity error. Defaults to env GTFS_IMPORT_STRICT.",
    )
    skip_if_unchanged: bool = Field(
        default=False,
        description="If true, skip import when feed hash matches previous.",
    )
    batch_size: Optional[int] = Field(
        default=None,
        ge=1,
        le=10000,
        description="Batch size for DB upserts. Defaults to env IMPORT_BATCH_SIZE.",
    )


class StaticGtfsImportResponse(BaseModel):
    """Response body for static GTFS import."""

    status: Literal["success", "failed"]
    import_id: str
    started_at: str
    ended_at: Optional[str] = None
    duration_ms: Optional[int] = None
    source: str
    feed_hash: str
    skipped_unchanged: bool
    counts: Dict[str, Dict[str, int]]
    warnings: List[str]
    errors: List[str]


# TODO: Add proper auth middleware when Supabase auth is wired up.
# For now this endpoint is unprotected - suitable for development only.
# In production, wrap with admin-role check middleware.
@router.post(
    "/import/static-gtfs",
    response_model=StaticGtfsImportResponse,
    summary="Import static GTFS feed",
    description=(
        "Trigger an idempotent import of a static GTFS feed. "
        "Supports remote URL download or local file path. "
        "Admin-only (auth stub - protect in production)."
    ),
)
async def import_static_gtfs(body: StaticGtfsImportRequest) -> Dict[str, Any]:
    """Import static GTFS feed into the database."""
    settings = get_settings()
    # Resolve source: use configured default if empty
    source = body.source
    if not source:
        if body.source_type == "remote":
            source = settings.gtfs_static_url
        else:
            raise HTTPException(
                status_code=400,
                detail="source is required when source_type is 'local'",
            )

    strict = body.strict if body.strict is not None else settings.gtfs_import_strict
    batch_size = body.batch_size if body.batch_size is not None else settings.import_batch_size

    importer = GtfsImporter(batch_size=batch_size, strict=strict)

    try:
        report = await importer.run(
            source_type=body.source_type,
            source=source,
            dry_run=body.dry_run,
            skip_if_unchanged=body.skip_if_unchanged,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (FetchError, InvalidZipError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except MissingRequiredFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except MissingColumnError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Unexpected import error", exc_info=exc)
        raise HTTPException(
            status_code=500,
            detail=f"Import failed unexpectedly: {type(exc).__name__}: {exc}",
        ) from exc

    # If strict mode produced errors, return 400
    if report.errors:
        raise HTTPException(status_code=400, detail=report.to_dict())

    return report.to_dict()


# --- Stage 5: Matching ---


class MatchingRunRequest(BaseModel):
    """Request body for matching run."""

    window_minutes: Optional[int] = Field(
        default=None,
        ge=1,
        le=1440,
        description="How far back (minutes) to scan RT updates. Defaults to MATCH_WINDOW_MINUTES.",
    )
    max_candidates: Optional[int] = Field(
        default=None,
        ge=1,
        le=100,
        description="Max schedule candidates per RT update. Defaults to MATCH_MAX_CANDIDATES.",
    )
    batch_size: Optional[int] = Field(
        default=None,
        ge=1,
        le=10000,
        description="Batch size for DB inserts. Defaults to MATCH_BATCH_SIZE.",
    )
    strict_mode: Optional[bool] = Field(
        default=None,
        description="Reject ambiguous matches. Defaults to MATCH_STRICT_MODE.",
    )


class MatchingRunResponse(BaseModel):
    """Response body for matching run."""

    run_id: str
    started_at: str
    ended_at: str
    duration_ms: int
    scanned_count: int
    matched_count: int
    unmatched_count: int
    ambiguous_count: int
    deduped_count: int
    error_count: int


@router.post(
    "/matching/run",
    response_model=MatchingRunResponse,
    summary="Run schedule-to-observation matching",
    description=(
        "Match GTFS-RT trip updates to static scheduled stop times "
        "and persist matched arrivals with delay computation. "
        "Idempotent — reruns update existing matches."
    ),
)
async def run_matching(body: MatchingRunRequest) -> Dict[str, Any]:
    """Execute a schedule-to-observation matching run."""
    engine = MatchingEngine(
        window_minutes=body.window_minutes,
        max_candidates=body.max_candidates,
        batch_size=body.batch_size,
        strict_mode=body.strict_mode,
    )

    try:
        report = await engine.run()
    except Exception as exc:
        logger.error("Matching run failed", exc_info=exc)
        raise HTTPException(
            status_code=500,
            detail=f"Matching run failed: {type(exc).__name__}: {exc}",
        ) from exc

    return report.to_dict()
