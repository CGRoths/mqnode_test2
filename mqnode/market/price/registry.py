from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PriceSourceSpec:
    source_name: str
    table_name: str
    priority_rank: int
    notes: str
    asset_symbol: str
    base_asset: str = 'BTC'
    quote_asset: str = 'USD'
    module_path: str = ''
    interval: str = '10m'
    source_interval: str = '10m'
    target_interval: str = '10m'
    market_type: str = 'spot'
    native_10m: bool = True
    supports_full_historical_replay: bool = True
    default_enabled: bool = True
    is_optional: bool = False
    config: dict[str, Any] | None = None


PRICE_SOURCES: tuple[PriceSourceSpec, ...] = (
    PriceSourceSpec(
        'bitstamp',
        'bitstamp_price_10m',
        1,
        'Long-history anchor venue.',
        asset_symbol='BTCUSD',
        module_path='mqnode.market.price.sources.bitstamp',
        config={
            'historical_start_utc': '2011-09-13T00:00:00Z',
            'auto_historical_backfill': True,
        },
    ),
    PriceSourceSpec(
        'coinbase',
        'coinbase_price_10m',
        2,
        'USD spot benchmark venue using replay-safe 5m aggregation.',
        asset_symbol='BTC-USD',
        module_path='mqnode.market.price.sources.coinbase',
        source_interval='5m',
        native_10m=False,
        config={
            'historical_start_utc': '2015-01-01T00:00:00Z',
            'auto_historical_backfill': True,
        },
    ),
    PriceSourceSpec(
        'binance',
        'binance_price_10m',
        3,
        'Major global liquidity source, cloud-fed from Binance Spot by default.',
        asset_symbol='BTCUSDT',
        quote_asset='USDT',
        module_path='mqnode.market.price.sources.binance',
        source_interval='5m',
        market_type='spot',
        native_10m=False,
        is_optional=True,
        config={
            'mode': 'remote',
            'api_base': 'https://api.binance.com',
            'endpoint': '/api/v3/klines',
            'market_type': 'spot',
            'historical_start_utc': '2017-08-17T00:00:00Z',
            'auto_historical_backfill': True,
        },
    ),
    PriceSourceSpec(
        'bybit',
        'bybit_price_10m',
        4,
        'Crypto-native spot venue.',
        asset_symbol='BTCUSDT',
        quote_asset='USDT',
        module_path='mqnode.market.price.sources.bybit',
        config={
            'historical_start_utc': '2018-11-01T00:00:00Z',
            'auto_historical_backfill': True,
        },
    ),
    PriceSourceSpec(
        'okx',
        'okx_price_10m',
        5,
        'Supplementary major spot venue using replay-safe 5m aggregation.',
        asset_symbol='BTC-USDT',
        quote_asset='USDT',
        module_path='mqnode.market.price.sources.okx',
        source_interval='5m',
        native_10m=False,
        config={
            'historical_start_utc': '2018-01-01T00:00:00Z',
            'auto_historical_backfill': True,
        },
    ),
    PriceSourceSpec(
        'bitfinex',
        'bitfinex_price_10m',
        6,
        'Supplementary BTC/USD liquidity source.',
        asset_symbol='tBTCUSD',
        module_path='mqnode.market.price.sources.bitfinex',
        config={
            'historical_start_utc': '2013-01-01T00:00:00Z',
            'auto_historical_backfill': True,
        },
    ),
    PriceSourceSpec(
        'kraken',
        'kraken_price_10m',
        7,
        'USD spot venue. REST OHLC is recent-only, so replay is best-effort rather than full historical.',
        asset_symbol='XBT/USD',
        module_path='mqnode.market.price.sources.kraken',
        source_interval='5m',
        native_10m=False,
        supports_full_historical_replay=False,
        default_enabled=False,
        is_optional=True,
        config={
            'auto_historical_backfill': False,
            'reason': 'disabled_recent_only_or_unverified_history',
        },
    ),
    PriceSourceSpec(
        'gemini',
        'gemini_price_10m',
        8,
        'US spot venue using recent-only 5m candle aggregation.',
        asset_symbol='BTCUSD',
        module_path='mqnode.market.price.sources.gemini',
        source_interval='5m',
        native_10m=False,
        supports_full_historical_replay=False,
        default_enabled=False,
        is_optional=True,
        config={
            'auto_historical_backfill': False,
            'reason': 'disabled_recent_only_or_unverified_history',
        },
    ),
)
PRICE_SOURCE_MAP = {source.source_name: source for source in PRICE_SOURCES}


def _merge_config(raw_config: Any, fallback: PriceSourceSpec | None) -> dict[str, Any]:
    config = dict(fallback.config or {}) if fallback and fallback.config else {}
    if isinstance(raw_config, dict):
        config.update(raw_config)
    elif isinstance(raw_config, str) and raw_config:
        config.update(json.loads(raw_config))
    return config


def _materialize_price_source(row: dict[str, Any], fallback: PriceSourceSpec | None = None) -> PriceSourceSpec:
    source_name = row.get('source_name') or (fallback.source_name if fallback else None)
    if source_name is None:
        raise KeyError('source_name')
    table_name = row.get('table_name') or (fallback.table_name if fallback else None)
    if table_name is None:
        raise KeyError(f'No table_name configured for source {source_name}')
    priority_rank = row.get('priority_rank')
    if priority_rank is None:
        priority_rank = fallback.priority_rank if fallback else 999
    notes = row.get('notes')
    if notes is None:
        notes = fallback.notes if fallback else ''
    asset_symbol = row.get('asset_symbol') or (fallback.asset_symbol if fallback else source_name.upper())
    base_asset = row.get('base_asset') or (fallback.base_asset if fallback else 'BTC')
    quote_asset = row.get('quote_asset') or (fallback.quote_asset if fallback else 'USD')
    interval = row.get('interval') or (fallback.interval if fallback else '10m')
    source_interval = row.get('source_interval') or (fallback.source_interval if fallback else interval)
    target_interval = row.get('target_interval') or (fallback.target_interval if fallback else interval)
    module_path = row.get('module_path') or (
        fallback.module_path if fallback else f'mqnode.market.price.sources.{source_name}'
    )
    market_type = row.get('market_type') or (fallback.market_type if fallback else 'spot')
    supports_full_historical_replay = row.get('supports_full_historical_replay')
    if supports_full_historical_replay is None:
        supports_full_historical_replay = (
            fallback.supports_full_historical_replay if fallback else True
        )
    default_enabled = row.get('enabled')
    if default_enabled is None:
        default_enabled = fallback.default_enabled if fallback else True
    is_optional = row.get('is_optional')
    if is_optional is None:
        is_optional = fallback.is_optional if fallback else False
    native_10m = fallback.native_10m if fallback else source_interval == target_interval == '10m'
    return PriceSourceSpec(
        source_name=source_name,
        table_name=table_name,
        priority_rank=int(priority_rank),
        notes=notes,
        asset_symbol=asset_symbol,
        base_asset=base_asset,
        quote_asset=quote_asset,
        module_path=module_path,
        interval=interval,
        source_interval=source_interval,
        target_interval=target_interval,
        market_type=market_type,
        native_10m=native_10m,
        supports_full_historical_replay=bool(supports_full_historical_replay),
        default_enabled=bool(default_enabled),
        is_optional=bool(is_optional),
        config=_merge_config(row.get('config_json'), fallback),
    )


def get_price_sources() -> tuple[PriceSourceSpec, ...]:
    return PRICE_SOURCES


def get_price_source(source_name: str, cur=None) -> PriceSourceSpec:
    fallback = PRICE_SOURCE_MAP.get(source_name)
    if cur is not None:
        try:
            cur.execute(
                '''
                SELECT
                  source_name,
                  table_name,
                  asset_symbol,
                  base_asset,
                  quote_asset,
                  module_path,
                  interval,
                  source_interval,
                  target_interval,
                  market_type,
                  priority_rank,
                  enabled,
                  is_optional,
                  supports_full_historical_replay,
                  notes,
                  config_json
                FROM mq_price_source_registry
                WHERE source_name = %s
                ''',
                (source_name,),
            )
            row = cur.fetchone()
            if row:
                return _materialize_price_source(row, fallback)
        except Exception:
            pass
    if fallback is None:
        raise KeyError(source_name)
    return fallback


def get_enabled_price_sources(cur=None) -> tuple[PriceSourceSpec, ...]:
    default_enabled = tuple(source for source in PRICE_SOURCES if source.default_enabled)
    if cur is None:
        return default_enabled
    try:
        cur.execute(
            '''
            SELECT
              source_name,
              table_name,
              asset_symbol,
              base_asset,
              quote_asset,
              module_path,
              interval,
              source_interval,
              target_interval,
              market_type,
              priority_rank,
              enabled,
              is_optional,
              supports_full_historical_replay,
              notes,
              config_json
            FROM mq_price_source_registry
            WHERE enabled = true
            ORDER BY priority_rank ASC, source_name ASC
            '''
        )
        rows: list[dict[str, Any]] = cur.fetchall()
    except Exception:
        return default_enabled

    enabled = tuple(_materialize_price_source(row, PRICE_SOURCE_MAP.get(row['source_name'])) for row in rows)
    return enabled or default_enabled
