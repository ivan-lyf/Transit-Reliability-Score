"""Application configuration via environment variables."""

from functools import lru_cache
from typing import Literal, Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "Transit Reliability Score API"
    app_version: str = "0.1.0"
    environment: Literal["development", "staging", "production"] = "development"
    debug: bool = False
    log_level: str = "INFO"

    # Database
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/transit"
    )

    # Redis (optional)
    redis_url: Optional[str] = None
    cache_ttl_seconds: int = 300

    # TransLink API
    translink_api_key: str = Field(default="")

    # GTFS Feed URLs (with configurable base)
    gtfs_trip_updates_url: str = "https://gtfsapi.translink.ca/v3/gtfsrealtime"
    gtfs_vehicle_positions_url: str = "https://gtfsapi.translink.ca/v3/gtfsposition"
    gtfs_service_alerts_url: str = "https://gtfsapi.translink.ca/v3/gtfsalerts"
    gtfs_static_url: str = Field(
        default="https://gtfs-static.translink.ca",
        validation_alias=AliasChoices("STATIC_GTFS_URL", "GTFS_STATIC_URL"),
    )

    # Static GTFS import settings (Stage 3)
    import_batch_size: int = Field(
        default=1000,
        ge=1,
        le=10000,
        validation_alias=AliasChoices("IMPORT_BATCH_SIZE", "GTFS_IMPORT_BATCH_SIZE"),
    )
    gtfs_import_strict: bool = Field(
        default=False,
        validation_alias=AliasChoices("GTFS_IMPORT_STRICT"),
    )

    # Polling
    gtfs_rt_poll_interval_sec: int = 30
    stale_feed_threshold_sec: int = 120

    # Scoring configuration
    on_time_threshold_sec: int = 120
    p95_max_delay_sec: int = 900
    p50_max_delay_sec: int = 300
    weight_on_time_rate: float = 0.6
    weight_p95_component: float = 0.25
    weight_p50_component: float = 0.15

    # API limits
    default_nearby_radius_km: float = 0.5
    max_nearby_radius_km: float = 5.0
    risky_stops_default_limit: int = 10
    trend_default_days: int = 7
    default_page_size: int = 20
    max_page_size: int = 100

    # Data attribution (configurable per TransLink requirements)
    data_attribution: str = (
        "Transit data provided by TransLink. This data is provided 'as is' without warranty."
    )
    translink_terms_url: str = (
        "https://www.translink.ca/about-us/doing-business-with-translink/"
        "app-developer-resources/translink-open-api"
    )

    # Supabase Auth
    supabase_url: str = ""
    supabase_anon_key: str = ""
    supabase_service_role_key: str = ""

    def missing_required_env(self) -> list[str]:
        """Return required environment variables that are missing or empty."""
        missing: list[str] = []

        if not self.database_url:
            missing.append("DATABASE_URL")

        return missing

    @property
    def gtfs_trip_updates_full_url(self) -> str:
        """Get full trip updates URL with API key."""
        if self.translink_api_key:
            return f"{self.gtfs_trip_updates_url}?apikey={self.translink_api_key}"
        return self.gtfs_trip_updates_url

    @property
    def gtfs_vehicle_positions_full_url(self) -> str:
        """Get full vehicle positions URL with API key."""
        if self.translink_api_key:
            return f"{self.gtfs_vehicle_positions_url}?apikey={self.translink_api_key}"
        return self.gtfs_vehicle_positions_url

    @property
    def gtfs_service_alerts_full_url(self) -> str:
        """Get full service alerts URL with API key."""
        if self.translink_api_key:
            return f"{self.gtfs_service_alerts_url}?apikey={self.translink_api_key}"
        return self.gtfs_service_alerts_url


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
