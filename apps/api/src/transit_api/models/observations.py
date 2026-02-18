"""Realtime observation and aggregation models."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from transit_api.models.base import Base


class RealtimeObservation(Base):
    """Individual realtime observation of a vehicle at a stop."""

    __tablename__ = "rt_observations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trip_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("trips.trip_id", ondelete="CASCADE"),
        nullable=False,
    )
    stop_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("stops.stop_id", ondelete="CASCADE"),
        nullable=False,
    )
    observed_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    delay_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    source_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        # Index for querying observations by stop and time
        Index("ix_rt_observations_stop_observed", "stop_id", "observed_ts"),
        # Index for deduplication checks
        Index("ix_rt_observations_trip_stop_observed", "trip_id", "stop_id", "observed_ts"),
    )


class ScoreAggregate(Base):
    """Aggregated reliability scores by stop, route, day type, and hour bucket."""

    __tablename__ = "score_agg"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stop_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("stops.stop_id", ondelete="CASCADE"),
        nullable=False,
    )
    route_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("routes.route_id", ondelete="CASCADE"),
        nullable=False,
    )
    day_type: Mapped[str] = mapped_column(String(16), nullable=False)
    hour_bucket: Mapped[str] = mapped_column(String(8), nullable=False)

    # Metrics
    on_time_rate: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    p50_delay_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    p95_delay_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_n: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    __table_args__ = (
        # Unique constraint for the aggregation key
        UniqueConstraint("stop_id", "route_id", "day_type", "hour_bucket", name="uq_score_agg_key"),
        # Primary lookup index
        Index("ix_score_agg_lookup", "stop_id", "route_id", "day_type", "hour_bucket"),
        # Index for finding risky stops
        Index("ix_score_agg_stop_score", "stop_id", "score"),
        # Constraints
        CheckConstraint("day_type IN ('weekday', 'saturday', 'sunday')", name="ck_day_type"),
        CheckConstraint(
            "hour_bucket IN ('6-9', '9-12', '12-15', '15-18', '18-21')",
            name="ck_hour_bucket",
        ),
        CheckConstraint("score >= 0 AND score <= 100", name="ck_score_range"),
        CheckConstraint("on_time_rate >= 0 AND on_time_rate <= 1", name="ck_on_time_rate"),
        CheckConstraint("sample_n >= 0", name="ck_sample_n"),
    )


class AggRunLog(Base):
    """Record of a single aggregation job run (Stage 6)."""

    __tablename__ = "agg_run_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    lookback_days: Mapped[int] = mapped_column(Integer, nullable=False)
    rows_scanned: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    buckets_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    error_message: Mapped[str] = mapped_column(Text, nullable=False, default="")

    __table_args__ = (Index("ix_agg_run_log_started_at", "started_at"),)
