"""Matched arrival model for Stage 5 schedule-to-observation matching."""

from __future__ import annotations

import datetime  # noqa: TC003

from sqlalchemy import Date, DateTime, Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from transit_api.models.base import Base


class MatchedArrival(Base):
    """Result of matching an RT trip update to a scheduled stop time."""

    __tablename__ = "matched_arrivals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trip_id: Mapped[str] = mapped_column(String(128), nullable=False)
    stop_id: Mapped[str] = mapped_column(String(64), nullable=False)
    stop_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    service_date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    scheduled_ts: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observed_ts: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    delay_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    match_status: Mapped[str] = mapped_column(String(16), nullable=False, default="matched")
    match_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    source_feed_ts: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    rt_trip_update_id: Mapped[int] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index(
            "uq_matched_arrival_key",
            "trip_id",
            "stop_id",
            "stop_sequence",
            "service_date",
            unique=True,
        ),
        Index("ix_matched_trip_stop_date", "trip_id", "stop_id", "service_date"),
        Index("ix_matched_stop_observed", "stop_id", "observed_ts"),
        Index("ix_matched_date_trip", "service_date", "trip_id"),
    )
