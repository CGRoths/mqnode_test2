from __future__ import annotations

from dataclasses import dataclass

REST_ONLY = 'rest_only'
WS_CONFIRMED_CANDLE = 'ws_confirmed_candle'
WS_CANDIDATE_REST_FINAL = 'ws_candidate_rest_final'
CLOUD_WS_REMOTE_PUSH = 'cloud_ws_remote_push'
DISABLED_MODE = 'disabled'

LIFECYCLE_DISABLED = 'disabled'
BOOTSTRAP_REST = 'bootstrap_rest'
REST_BACKFILLING = 'rest_backfilling'
REST_SYNCED = 'rest_synced'
WS_STARTING = 'ws_starting'
WS_LIVE = 'ws_live'
WS_DIRTY = 'ws_dirty'
REST_FALLBACK = 'rest_fallback'
REST_CONFIRMING = 'rest_confirming'
LIVE_HEALTHY = 'live_healthy'
WAITING_REMOTE_PUSH = 'waiting_remote_push'
ERROR = 'error'

HISTORICAL_REST = 'rest_historical'


@dataclass(frozen=True)
class PriceSourceLifecycleProfile:
    source_name: str
    symbol: str
    table_name: str
    historical_mode: str
    live_mode: str
    enabled: bool
    can_write_ws_closed: bool
    rest_fallback_enabled: bool
    rest_confirm_enabled: bool
    remote_push_enabled: bool
    ws_source_name: str | None
    rest_source_name: str | None
    bucket_grace_seconds: int
    rest_confirm_delay_seconds: int
    target_interval: str = '10m'
    child_interval: str = '1m'


@dataclass(frozen=True)
class LifecycleDecision:
    source_name: str
    symbol: str
    current_state: str
    recommended_action: str
    reason: str
    should_start_ws: bool
    should_keep_rest_live: bool
    should_disable_ws: bool
    expects_remote_push: bool
