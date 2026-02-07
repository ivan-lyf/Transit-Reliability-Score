"""Application configuration via environment variables."""

from functools import lru_cache
from typing import Literal, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

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
    gtfs_trip_updates_url: str = Field(
        default="https://gtfsapi.translink.ca/v3/gtfsrealtime",
        validation_alias=AliasChoices("TRIP_UPDATES_URL", "GTFS_TRIP_UPDATES_URL"),
    )
    gtfs_vehicle_positions_url: str = Field(
        default="https://gtfsapi.translink.ca/v3/gtfsposition",
        validation_alias=AliasChoices("VEHICLE_POSITIONS_URL", "GTFS_VEHICLE_POSITIONS_URL"),
    )
    gtfs_service_alerts_url: str = Field(
        default="https://gtfsapi.translink.ca/v3/gtfsalerts",
        validation_alias=AliasChoices("SERVICE_ALERTS_URL", "GTFS_SERVICE_ALERTS_URL"),
    )
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

    # GTFS-RT polling / worker (Stage 4)
    gtfs_rt_poll_interval_sec: int = 30
    stale_feed_threshold_sec: int = 120
    gtfs_rt_fetch_timeout_sec: int = 30
    gtfs_rt_max_retries: int = 3
    gtfs_rt_backoff_base: float = 2.0
    gtfs_rt_batch_size: int = 500
    gtfs_rt_auto_start: bool = False

    # Stage 5: Matching engine
    match_window_minutes: int = 90
    match_max_candidates: int = 5
    match_batch_size: int = 1000
    match_strict_mode: bool = False

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
        return _with_api_key(self.gtfs_trip_updates_url, self.translink_api_key)

    @property
    def gtfs_vehicle_positions_full_url(self) -> str:
        """Get full vehicle positions URL with API key."""
        return _with_api_key(self.gtfs_vehicle_positions_url, self.translink_api_key)

    @property
    def gtfs_service_alerts_full_url(self) -> str:
        """Get full service alerts URL with API key."""
        return _with_api_key(self.gtfs_service_alerts_url, self.translink_api_key)


def _with_api_key(url: str, api_key: str) -> str:
    """Return URL with api key injected unless already present."""
    if not api_key:
        return url

    if "${TRANSLINK_API_KEY}" in url:
        url = url.replace("${TRANSLINK_API_KEY}", api_key)

    parsed = urlparse(url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    if any(key.lower() == "apikey" for key, _ in query_pairs):
        return url

    query_pairs.append(("apikey", api_key))
    new_query = urlencode(query_pairs)
    return urlunparse(parsed._replace(query=new_query))


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
