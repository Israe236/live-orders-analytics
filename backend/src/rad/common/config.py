"""Runtime configuration, read from environment variables prefixed with ``RAD_``."""

from datetime import timedelta

from pydantic_settings import BaseSettings, SettingsConfigDict

from rad.alerts.rules import Thresholds


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

    # --- Live push (WebSocket) --------------------------------------------------------------
    ws_tick_interval_s: float = 1.0
    # Updates carry only the tail of the time series; a full snapshot every N ticks corrects
    # older buckets changed by late events.
    ws_full_snapshot_every_ticks: int = 30
    ws_max_clients: int = 500
    # A client that cannot accept one message within this time is disconnected.
    ws_send_timeout_s: float = 5.0
    ws_feed_max_events: int = 20

    # --- Alerting ---------------------------------------------------------------------------
    alert_window_minutes: int = 3
    # Defaults chosen with the alert backtest (python -m rad.alerts.backtest).
    alert_cancellation_rate: float = 0.20
    alert_cancellation_min_orders: int = 30
    alert_revenue_drop_ratio: float = 0.6
    alert_revenue_min_baseline_mad_per_min: float = 5_000.0
    alert_orders_drop_ratio: float = 0.6
    alert_orders_min_baseline_per_min: float = 10.0
    alert_dead_letter_ratio: float = 0.05
    alert_dead_letter_min_events: int = 100
    alert_stall_after_s: float = 30.0
    # Hysteresis in time: breached this long before firing, healthy this long before resolving.
    alert_fire_after_s: float = 30.0
    alert_resolve_after_s: float = 30.0

    def alert_thresholds(self) -> Thresholds:
        return Thresholds(
            window_minutes=self.alert_window_minutes,
            cancellation_rate=self.alert_cancellation_rate,
            cancellation_min_orders=self.alert_cancellation_min_orders,
            revenue_drop_ratio=self.alert_revenue_drop_ratio,
            revenue_min_baseline_mad_per_min=self.alert_revenue_min_baseline_mad_per_min,
            orders_drop_ratio=self.alert_orders_drop_ratio,
            orders_min_baseline_per_min=self.alert_orders_min_baseline_per_min,
            dead_letter_ratio=self.alert_dead_letter_ratio,
            dead_letter_min_events=self.alert_dead_letter_min_events,
            stall_after_s=self.alert_stall_after_s,
        )

    @property
    def max_future_skew(self) -> timedelta:
        return timedelta(seconds=self.max_future_skew_seconds)

    @property
    def max_event_age(self) -> timedelta:
        return timedelta(seconds=self.max_event_age_seconds)
