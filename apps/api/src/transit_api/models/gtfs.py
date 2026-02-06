"""GTFS static data models."""

from __future__ import annotations

from sqlalchemy import ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from transit_api.models.base import Base


class Stop(Base):
    """Transit stop/station."""

    __tablename__ = "stops"

    stop_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    lat: Mapped[float] = mapped_column(Numeric(10, 7), nullable=False)
    lon: Mapped[float] = mapped_column(Numeric(10, 7), nullable=False)

    # Relationships
    stop_times: Mapped[list[StopTime]] = relationship(
        "StopTime", back_populates="stop", lazy="selectin"
    )

    __table_args__ = (
        # Spatial index for nearby queries (using btree on lat/lon)
        # For production, consider PostGIS with GIST index
        Index("ix_stops_lat_lon", "lat", "lon"),
    )


class Route(Base):
    """Transit route."""

    __tablename__ = "routes"

    route_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    short_name: Mapped[str] = mapped_column(String(64), nullable=False)
    long_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Relationships
    trips: Mapped[list[Trip]] = relationship("Trip", back_populates="route", lazy="selectin")


class Trip(Base):
    """Transit trip (a specific run of a route)."""

    __tablename__ = "trips"

    trip_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    route_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("routes.route_id", ondelete="CASCADE"), nullable=False
    )
    service_id: Mapped[str] = mapped_column(String(64), nullable=False)
    direction_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Relationships
    route: Mapped[Route] = relationship("Route", back_populates="trips")
    stop_times: Mapped[list[StopTime]] = relationship(
        "StopTime", back_populates="trip", lazy="selectin"
    )

    __table_args__ = (Index("ix_trips_route_id", "route_id"),)


class StopTime(Base):
    """Scheduled stop time for a trip."""

    __tablename__ = "stop_times"

    # Composite primary key
    trip_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("trips.trip_id", ondelete="CASCADE"),
        primary_key=True,
    )
    stop_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("stops.stop_id", ondelete="CASCADE"),
        primary_key=True,
    )
    stop_sequence: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Scheduled arrival in seconds from midnight
    sched_arrival_sec: Mapped[int] = mapped_column(Integer, nullable=False)

    # Relationships
    trip: Mapped[Trip] = relationship("Trip", back_populates="stop_times")
    stop: Mapped[Stop] = relationship("Stop", back_populates="stop_times")

    __table_args__ = (
        UniqueConstraint("trip_id", "stop_sequence", name="uq_stop_times_trip_sequence"),
        Index("ix_stop_times_stop_id", "stop_id"),
        Index("ix_stop_times_trip_id", "trip_id"),
    )
