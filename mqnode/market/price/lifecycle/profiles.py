from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

from mqnode.config.settings import get_settings
from mqnode.market.price.lifecycle.models import (
    CLOUD_WS_REMOTE_PUSH,
    DISABLED_MODE,
    HISTORICAL_REST,
    REST_ONLY,
    WS_CANDIDATE_REST_FINAL,
    WS_CONFIRMED_CANDLE,
    PriceSourceLifecycleProfile,
)
from mqnode.market.price.registry import PriceSourceSpec, get_price_source

DEFAULT_SOURCE_PROFILES = {
    'bybit': {
        'enabled': True,
        'symbol': 'BTCUSDT',
        'live_mode': WS_CONFIRMED_CANDLE,
        'can_write_ws_closed': True,
        'rest_fallback_enabled': True,
        'rest_confirm_enabled': True,
        'remote_push_enabled': False,
    },
    'okx': {
        'enabled': True,
        'symbol': 'BTC-USDT',
        'live_mode': WS_CONFIRMED_CANDLE,
        'can_write_ws_closed': True,
        'rest_fallback_enabled': True,
        'rest_confirm_enabled': True,
        'remote_push_enabled': False,
    },
    'binance': {
        'enabled': True,
        'symbol': 'BTCUSDT',
        'live_mode': CLOUD_WS_REMOTE_PUSH,
        'can_write_ws_closed': True,
        'rest_fallback_enabled': True,
        'rest_confirm_enabled': True,
        'remote_push_enabled': True,
    },
    'coinbase': {
        'enabled': True,
        'symbol': 'BTC-USD',
        'live_mode': WS_CANDIDATE_REST_FINAL,
        'can_write_ws_closed': False,
        'rest_fallback_enabled': True,
        'rest_confirm_enabled': True,
        'remote_push_enabled': False,
    },
    'kraken': {
        'enabled': True,
        'symbol': 'BTC/USD',
        'live_mode': WS_CANDIDATE_REST_FINAL,
        'can_write_ws_closed': False,
        'rest_fallback_enabled': True,
        'rest_confirm_enabled': True,
        'remote_push_enabled': False,
    },
    'bitstamp': {
        'enabled': True,
        'symbol': 'btcusd',
        'live_mode': WS_CANDIDATE_REST_FINAL,
        'can_write_ws_closed': False,
        'rest_fallback_enabled': True,
        'rest_confirm_enabled': True,
        'remote_push_enabled': False,
    },
}


def _row_get(row: PriceSourceSpec | dict[str, Any] | None, field: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(field, default)
    return getattr(row, field, default)


def _row_config(row: PriceSourceSpec | dict[str, Any] | None) -> dict[str, Any]:
    config = _row_get(row, 'config')
    if isinstance(config, dict):
        return config
    config_json = _row_get(row, 'config_json')
    if isinstance(config_json, dict):
        return config_json
    if isinstance(config_json, str) and config_json:
        return json.loads(config_json)
    return {}


def _bool_config(config: dict[str, Any], key: str, default: bool) -> bool:
    value = config.get(key, default)
    if isinstance(value, str):
        return value.lower() in {'1', 'true', 'yes', 'on'}
    return bool(value)


def get_price_source_lifecycle_profile(
    source_name: str,
    settings=None,
    price_source_row: PriceSourceSpec | dict[str, Any] | None = None,
) -> PriceSourceLifecycleProfile:
    settings = settings or get_settings()
    source_name = source_name.lower()
    defaults = DEFAULT_SOURCE_PROFILES.get(source_name)
    if defaults is None:
        raise NotImplementedError(f'No lifecycle profile configured for price source: {source_name}')

    if price_source_row is None:
        try:
            price_source_row = get_price_source(source_name)
        except KeyError:
            price_source_row = None

    config = _row_config(price_source_row)
    enabled = defaults.get('enabled', True)
    if 'enabled' in config:
        enabled = _bool_config(config, 'enabled', bool(enabled))
    mode = str(config.get('mode') or '').lower()
    if mode == DISABLED_MODE:
        enabled = False

    profile = PriceSourceLifecycleProfile(
        source_name=source_name,
        symbol=str(config.get('lifecycle_symbol') or defaults['symbol']),
        table_name=str(_row_get(price_source_row, 'table_name', f'{source_name}_price_10m')),
        historical_mode=str(config.get('historical_mode') or HISTORICAL_REST),
        live_mode=str(config.get('live_mode') or defaults['live_mode']),
        enabled=bool(enabled),
        can_write_ws_closed=_bool_config(config, 'can_write_ws_closed', defaults['can_write_ws_closed']),
        rest_fallback_enabled=_bool_config(config, 'rest_fallback_enabled', defaults['rest_fallback_enabled']),
        rest_confirm_enabled=_bool_config(config, 'rest_confirm_enabled', defaults['rest_confirm_enabled']),
        remote_push_enabled=_bool_config(config, 'remote_push_enabled', defaults['remote_push_enabled']),
        ws_source_name=config.get('ws_source_name') or source_name,
        rest_source_name=config.get('rest_source_name') or source_name,
        bucket_grace_seconds=int(
            config.get('bucket_grace_seconds') or getattr(settings, 'price_ws_bucket_grace_seconds', 5)
        ),
        rest_confirm_delay_seconds=int(
            config.get('rest_confirm_delay_seconds')
            or getattr(settings, 'price_ws_rest_confirm_delay_seconds', 30)
        ),
        target_interval=str(config.get('target_interval') or _row_get(price_source_row, 'target_interval', '10m')),
        child_interval=str(config.get('child_interval') or getattr(settings, 'price_ws_child_interval', '1m')),
    )
    if not profile.enabled:
        return replace(profile, live_mode=str(config.get('live_mode') or profile.live_mode))
    if profile.live_mode == REST_ONLY:
        return replace(profile, can_write_ws_closed=False, remote_push_enabled=False)
    return profile
