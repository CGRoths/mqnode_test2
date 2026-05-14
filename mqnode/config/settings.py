from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_name: str = 'MQNODE'
    app_env: str = 'dev'
    log_level: str = 'INFO'

    postgres_host: str = Field('postgres', alias='POSTGRES_HOST')
    postgres_port: int = Field(5432, alias='POSTGRES_PORT')
    postgres_db: str = Field('mqnode', alias='POSTGRES_DB')
    postgres_user: str = Field('mqnode', alias='POSTGRES_USER')
    postgres_password: str = Field('mqnode', alias='POSTGRES_PASSWORD')
    postgres_pool_minconn: int = Field(1, alias='POSTGRES_POOL_MINCONN')
    postgres_pool_maxconn: int = Field(10, alias='POSTGRES_POOL_MAXCONN')

    redis_host: str = Field('redis', alias='REDIS_HOST')
    redis_port: int = Field(6379, alias='REDIS_PORT')

    btc_rpc_host: str = Field('bitcoin', alias='BTC_RPC_HOST')
    btc_rpc_port: int = Field(8332, alias='BTC_RPC_PORT')
    btc_rpc_user: str = Field('bitcoin', alias='BTC_RPC_USER')
    btc_rpc_password: str = Field('bitcoin', alias='BTC_RPC_PASSWORD')
    btc_rpc_timeout: int = Field(30, alias='BTC_RPC_TIMEOUT')

    btc_listener_sleep_seconds: int = Field(10, alias='BTC_LISTENER_SLEEP_SECONDS')
    btc_listener_error_sleep_seconds: int = Field(10, alias='BTC_LISTENER_ERROR_SLEEP_SECONDS')
    btc_price_table: str = Field('mq_btc_price_10m', alias='BTC_PRICE_TABLE')

    worker_retry_sleep_seconds: int = Field(10, alias='WORKER_RETRY_SLEEP_SECONDS')
    worker_heartbeat_seconds: int = Field(60, alias='WORKER_HEARTBEAT_SECONDS')
    worker_stale_after_seconds: int = Field(180, alias='WORKER_STALE_AFTER_SECONDS')
    worker_startup_replay: bool = Field(True, alias='WORKER_STARTUP_REPLAY')
    price_composer_sleep_seconds: int = Field(30, alias='PRICE_COMPOSER_SLEEP_SECONDS')
    price_composer_timeline_backbone: str = Field('btc_primitive_10m', alias='PRICE_COMPOSER_TIMELINE_BACKBONE')
    price_composer_write_null_buckets: bool = Field(True, alias='PRICE_COMPOSER_WRITE_NULL_BUCKETS')
    price_source_ingestion_sleep_seconds: int = Field(60, alias='PRICE_SOURCE_INGESTION_SLEEP_SECONDS')
    price_request_timeout_seconds: int = Field(30, alias='PRICE_REQUEST_TIMEOUT_SECONDS')
    price_ws_enabled: bool = Field(False, alias='PRICE_WS_ENABLED')
    price_ws_source: str = Field('bybit', alias='PRICE_WS_SOURCE')
    price_ws_symbol: str = Field('BTCUSDT', alias='PRICE_WS_SYMBOL')
    price_ws_child_interval: str = Field('1m', alias='PRICE_WS_CHILD_INTERVAL')
    price_ws_target_interval: str = Field('10m', alias='PRICE_WS_TARGET_INTERVAL')
    price_ws_bucket_grace_seconds: int = Field(5, alias='PRICE_WS_BUCKET_GRACE_SECONDS')
    price_ws_stale_seconds: int = Field(30, alias='PRICE_WS_STALE_SECONDS')
    price_ws_reconnect_backoff_seconds: str = Field('1,2,5,10', alias='PRICE_WS_RECONNECT_BACKOFF_SECONDS')
    price_ws_rest_fallback_enabled: bool = Field(True, alias='PRICE_WS_REST_FALLBACK_ENABLED')
    price_ws_rest_confirm_delay_seconds: int = Field(30, alias='PRICE_WS_REST_CONFIRM_DELAY_SECONDS')
    price_ws_price_tolerance_bps: int = Field(1, alias='PRICE_WS_PRICE_TOLERANCE_BPS')
    price_ws_volume_tolerance_bps: int = Field(10, alias='PRICE_WS_VOLUME_TOLERANCE_BPS')
    price_ws_output_mode: str = Field('local_db', alias='PRICE_WS_OUTPUT_MODE')
    price_ws_remote_ingest_url: str | None = Field(None, alias='PRICE_WS_REMOTE_INGEST_URL')
    price_ws_remote_ingest_token: str | None = Field(None, alias='PRICE_WS_REMOTE_INGEST_TOKEN')
    price_ws_remote_timeout_seconds: int = Field(10, alias='PRICE_WS_REMOTE_TIMEOUT_SECONDS')
    price_ws_remote_max_attempts: int = Field(3, alias='PRICE_WS_REMOTE_MAX_ATTEMPTS')
    price_source_lifecycle_enabled: bool = Field(False, alias='PRICE_SOURCE_LIFECYCLE_ENABLED')
    price_source_rest_sync_allowed_lag_minutes: int = Field(
        20,
        alias='PRICE_SOURCE_REST_SYNC_ALLOWED_LAG_MINUTES',
    )
    price_source_lifecycle_autostart_ws: bool = Field(False, alias='PRICE_SOURCE_LIFECYCLE_AUTOSTART_WS')
    price_source_lifecycle_sources: str = Field(
        'bybit,okx,binance,coinbase,kraken,bitstamp',
        alias='PRICE_SOURCE_LIFECYCLE_SOURCES',
    )
    price_source_worker_poll_seconds: int = Field(60, alias='PRICE_SOURCE_WORKER_POLL_SECONDS')
    price_source_worker_confirmation_poll_seconds: int = Field(
        1,
        alias='PRICE_SOURCE_WORKER_CONFIRMATION_POLL_SECONDS',
    )
    internal_ingest_token: str | None = Field(None, alias='MQNODE_INTERNAL_INGEST_TOKEN')

    @property
    def postgres_dsn(self) -> str:
        return (
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password} host={self.postgres_host} port={self.postgres_port}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/0"

    @property
    def btc_rpc_url(self) -> str:
        return f"http://{self.btc_rpc_host}:{self.btc_rpc_port}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
