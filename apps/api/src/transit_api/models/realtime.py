"""GTFS-Realtime data models for Stage 4."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from transit_api.models.base import Base


class RtTripUpdate(Base):
    """Normalized trip update from GTFS-RT TripUpdate feed."""

    __tablename__ = "rt_trip_updates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trip_id: Mapped[str] = mapped_column(String(128), nullable=False)
    route_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    stop_id: Mapped[str] = mapped_column(String(64), nullable=False)
    stop_sequence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    arrival_delay: Mapped[int] = mapped_column(Integer, nullable=True)
    arrival_time: Mapped[int] = mapped_column(Integer, nullable=True)
    departure_delay: Mapped[int] = mapped_column(Integer, nullable=True)
    departure_time: Mapped[int] = mapped_column(Integer, nullable=True)
    schedule_relationship: Mapped[str] = mapped_column(
        String(32), nullable=False, default="SCHEDULED"
    )
    feed_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_rt_trip_updates_trip_id", "trip_id"),
        Index("ix_rt_trip_updates_stop_id", "stop_id"),
        Index("ix_rt_trip_updates_feed_ts", "feed_timestamp"),
        Index(
            "ix_rt_trip_updates_dedup",
            "trip_id",
            "stop_id",
            "feed_timestamp",
            unique=True,
        ),
    )


class RtVehiclePosition(Base):
    """Normalized vehicle position from GTFS-RT VehiclePosition feed."""

    __tablename__ = "rt_vehicle_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    vehicle_id: Mapped[str] = mapped_column(String(64), nullable=False)
    trip_id: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    route_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    bearing: Mapped[float] = mapped_column(Float, nullable=True)
    speed: Mapped[float] = mapped_column(Float, nullable=True)
    current_stop_sequence: Mapped[int] = mapped_column(Integer, nullable=True)
    current_status: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    feed_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_rt_vehicle_pos_vehicle_id", "vehicle_id"),
        Index("ix_rt_vehicle_pos_trip_id", "trip_id"),
        Index("ix_rt_vehicle_pos_route_id", "route_id"),
        Index("ix_rt_vehicle_pos_feed_ts", "feed_timestamp"),
        Index(
            "ix_rt_vehicle_pos_dedup",
            "vehicle_id",
            "feed_timestamp",
            unique=True,
        ),
    )


class RtAlert(Base):
    """Normalized service alert from GTFS-RT Alert feed."""

    __tablename__ = "rt_alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alert_id: Mapped[str] = mapped_column(String(128), nullable=False)
    cause: Mapped[str] = mapped_column(String(64), nullable=False, default="UNKNOWN_CAUSE")
    effect: Mapped[str] = mapped_column(String(64), nullable=False, default="UNKNOWN_EFFECT")
    header_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    description_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    active_period_start: Mapped[int] = mapped_column(Integer, nullable=True)
    active_period_end: Mapped[int] = mapped_column(Integer, nullable=True)
    informed_route_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    informed_stop_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    informed_trip_id: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    feed_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_rt_alerts_alert_id", "alert_id"),
        Index("ix_rt_alerts_route_id", "informed_route_id"),
        Index("ix_rt_alerts_feed_ts", "feed_timestamp"),
        Index(
            "ix_rt_alerts_dedup",
            "alert_id",
            "informed_route_id",
            "informed_stop_id",
            "feed_timestamp",
            unique=True,
        ),
    )


class RtIngestMeta(Base):
    """Tracks last successful ingest per GTFS-RT feed type."""

    __tablename__ = "rt_ingest_meta"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    feed_type: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    last_success_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    last_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown")
    error_message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    feed_hash: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    entity_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (Index("ix_rt_ingest_meta_feed_type", "feed_type"),)
