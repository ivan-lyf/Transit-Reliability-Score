"""GTFS import log model for tracking feed imports."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 - SQLAlchemy needs this at runtime for Mapped[datetime]

from sqlalchemy import DateTime, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from transit_api.models.base import Base


class GtfsImportLog(Base):
    """Tracks GTFS static feed imports for audit and skip-if-unchanged."""

    __tablename__ = "gtfs_import_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feed_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    imported_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (Index("ix_gtfs_import_log_imported_at", "imported_at"),)
