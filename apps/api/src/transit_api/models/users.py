"""User model for authentication and favorites."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import DateTime, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from transit_api.models.base import Base


class User(Base):
    """Application user linked to Supabase Auth."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    auth_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    favorites_json: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default='{"stops": [], "routes": []}',
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    def get_favorites(self) -> dict[str, Any]:
        """Parse favorites JSON into a dictionary."""
        import json

        return json.loads(self.favorites_json)  # type: ignore[no-any-return]

    def set_favorites(self, favorites: dict[str, Any]) -> None:
        """Serialize favorites dictionary to JSON."""
        import json

        self.favorites_json = json.dumps(favorites)
