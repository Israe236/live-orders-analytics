"""Runtime configuration, read from environment variables prefixed with ``RAD_``."""

from datetime import timedelta

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RAD_", extra="ignore")

    database_url: str = "postgresql://rad:rad@localhost:5432/rad"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10

    # --- Ingestion limits -------------------------------------------------------------------
    max_request_bytes: int = 5_000_000
    max_events_per_request: int = 5_000
    # Producers' clocks are never perfectly in sync with ours: tolerate a little skew.
    max_future_skew_seconds: int = 300
    # Events older than this are treated as bad data rather than "late" data.
    max_event_age_seconds: int = 7 * 24 * 3600
    dead_letter_payload_max_chars: int = 8_000

    # --- Write path (group commit) ----------------------------------------------------------
    # Upper bound on events waiting in memory; beyond it the API answers 429.
    ingest_queue_max_events: int = 50_000
    # Upper bound on events written in a single SQL statement.
    writer_batch_max_events: int = 5_000

    @property
    def max_future_skew(self) -> timedelta:
        return timedelta(seconds=self.max_future_skew_seconds)

    @property
    def max_event_age(self) -> timedelta:
        return timedelta(seconds=self.max_event_age_seconds)
