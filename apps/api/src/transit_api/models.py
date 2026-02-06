"""SQLAlchemy models for the Transit Reliability Score schema."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime
from uuid import uuid4

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
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base declarative class."""


class Stop(Base):
    """Static GTFS stop metadata."""

    __tablename__ = "stops"

    stop_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    lat: Mapped[float] = mapped_column(Numeric(10, 7), nullable=False)
    lon: Mapped[float] = mapped_column(Numeric(10, 7), nullable=False)

    __table_args__ = (Index("ix_stops_lat_lon", "lat", "lon"),)


class Route(Base):
    """Route metadata."""

    __tablename__ = "routes"

    route_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    short_name: Mapped[str] = mapped_column(String(64), nullable=False)
    long_name: Mapped[str] = mapped_column(String(255), nullable=False)


class Trip(Base):
    """Scheduled trip metadata."""

    __tablename__ = "trips"

    trip_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    route_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("routes.route_id", ondelete="CASCADE"), nullable=False
    )
    service_id: Mapped[str] = mapped_column(String(64), nullable=False)
    direction_id: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    __table_args__ = (Index("ix_trips_route_id", "route_id"),)


class StopTime(Base):
    """Scheduled stop times for trips."""

    __tablename__ = "stop_times"

    trip_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("trips.trip_id", ondelete="CASCADE"), primary_key=True
    )
    stop_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("stops.stop_id", ondelete="CASCADE"), primary_key=True
    )
    stop_sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    sched_arrival_sec: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        Index("ix_stop_times_stop_id", "stop_id"),
        Index("ix_stop_times_trip_id", "trip_id"),
    )


class RealtimeObservation(Base):
    """Realtime GTFS observation events."""

    __tablename__ = "rt_observations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trip_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("trips.trip_id", ondelete="CASCADE"), nullable=False
    )
    stop_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("stops.stop_id", ondelete="CASCADE"), nullable=False
    )
    observed_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    delay_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    source_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_rt_observations_stop_observed", "stop_id", "observed_ts"),
        Index(
            "ix_rt_observations_trip_stop_observed",
            "trip_id",
            "stop_id",
            "observed_ts",
        ),
    )


class ScoreAggregate(Base):
    """Aggregated reliability scores."""

    __tablename__ = "score_agg"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stop_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("stops.stop_id", ondelete="CASCADE"), nullable=False
    )
    route_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("routes.route_id", ondelete="CASCADE"), nullable=False
    )
    day_type: Mapped[str] = mapped_column(String(16), nullable=False)
    hour_bucket: Mapped[str] = mapped_column(String(8), nullable=False)
    on_time_rate: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    p50_delay_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    p95_delay_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_n: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index(
            "ix_score_agg_lookup",
            "stop_id",
            "route_id",
            "day_type",
            "hour_bucket",
        ),
        Index("ix_score_agg_stop_score", "stop_id", "score"),
        UniqueConstraint(
            "stop_id",
            "route_id",
            "day_type",
            "hour_bucket",
            name="uq_score_agg_key",
        ),
        CheckConstraint("day_type IN ('weekday', 'saturday', 'sunday')", name="ck_day_type"),
        CheckConstraint(
            "hour_bucket IN ('6-9', '9-12', '12-15', '15-18', '18-21')",
            name="ck_hour_bucket",
        ),
        CheckConstraint("score >= 0 AND score <= 100", name="ck_score_range"),
        CheckConstraint("on_time_rate >= 0 AND on_time_rate <= 1", name="ck_on_time_rate"),
        CheckConstraint("sample_n >= 0", name="ck_sample_n"),
    )


class User(Base):
    """User profile and favorites."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    auth_id: Mapped[str] = mapped_column(String(64), nullable=False)
    favorites_json: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default='{"stops": [], "routes": []}',
        server_default='{"stops": [], "routes": []}',
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("auth_id"),
        Index("ix_users_auth_id", "auth_id"),
    )

    def get_favorites(self) -> dict[str, list[str]]:
        """Return favorites parsed from JSON storage."""
        try:
            favorites = json.loads(self.favorites_json)
        except json.JSONDecodeError:
            favorites = {"stops": [], "routes": []}

        if not isinstance(favorites, dict):
            return {"stops": [], "routes": []}

        return {
            "stops": list(favorites.get("stops", [])),
            "routes": list(favorites.get("routes", [])),
        }

    def set_favorites(self, favorites: dict[str, list[str]]) -> None:
        """Persist favorites to JSON storage."""
        payload: dict[str, Any] = {
            "stops": list(favorites.get("stops", [])),
            "routes": list(favorites.get("routes", [])),
        }
        self.favorites_json = json.dumps(payload)
