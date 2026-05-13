from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import aggregate_small_candles_to_10m, normalize_ohlcv_bucket
from mqnode.market.price.registry import get_price_source
from mqnode.market.price.source_support import (
    default_db,
    get_ingestion_window,
    log_price_source_chunk_empty,
    log_price_source_chunk_upserted,
    request_json,
    upsert_source_rows,
)

DEFAULT_API_BASE = 'https://papi.binance.com'
ENDPOINT = '/papi/v1/um/klines'
API_URL = f'{DEFAULT_API_BASE}{ENDPOINT}'
CHUNK_SIZE = timedelta(days=3)
MARKET_TYPE = 'um_futures'
NATIVE_10M = False
SOURCE_NAME = 'binance'
SOURCE_INTERVAL = '5m'
TARGET_INTERVAL = '10m'
TABLE_NAME = 'binance_price_10m'
SYMBOL = 'BTCUSDT'
logger = logging.getLogger(__name__)


def _get_binance_config(db) -> dict:
    config = {
        'mode': 'local',
        'api_base': DEFAULT_API_BASE,
        'endpoint': ENDPOINT,
        'market_type': MARKET_TYPE,
    }
    try:
        with db.cursor() as cur:
            source = get_price_source(SOURCE_NAME, cur)
            config.update(source.config or {})
    except Exception:
        logger.warning('binance_source_config_fallback', exc_info=True)
    return config


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch Binance PAPI USD-M 5m klines and upsert complete 10m buckets."""
    settings = settings or get_settings()
    db = default_db(db)
    config = _get_binance_config(db)
    mode = str(config.get('mode', 'local')).lower()
    if mode in {'remote', 'disabled'}:
        logger.info('binance_local_fetch_skipped_mode mode=%s', mode)
        return 0
    api_base = str(config.get('api_base') or DEFAULT_API_BASE).rstrip('/')
    endpoint = str(config.get('endpoint') or ENDPOINT)
    if not endpoint.startswith('/'):
        endpoint = f'/{endpoint}'
    market_type = str(config.get('market_type') or MARKET_TYPE)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    chunk_start = start_bucket
    empty_chunks = 0
    processed = 0

    while chunk_start < end_bucket:
        chunk_end = min(chunk_start + CHUNK_SIZE, end_bucket)
        rows = []
        candles = request_json(
            f'{api_base}{endpoint}',
            params={
                'symbol': SYMBOL,
                'interval': '5m',
                'startTime': int(chunk_start.timestamp() * 1000),
                'endTime': int(chunk_end.timestamp() * 1000),
                'limit': 1000,
            },
            timeout=getattr(settings, 'price_request_timeout_seconds', 30),
        )
        if not candles:
            empty_chunks += 1
            log_price_source_chunk_empty(logger, SOURCE_NAME, chunk_start, chunk_end, empty_chunks)
            chunk_start = chunk_end
            continue
        empty_chunks = 0

        for candle in sorted(candles, key=lambda row: int(row[0])):
            open_time_ms = int(candle[0])
            bucket_time = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
            if bucket_time < chunk_start or bucket_time >= chunk_end:
                continue
            rows.append(
                normalize_ohlcv_bucket(
                    SOURCE_NAME,
                    bucket_time,
                    symbol=SYMBOL,
                    open_price_usd=float(candle[1]),
                    high_price_usd=float(candle[2]),
                    low_price_usd=float(candle[3]),
                    close_price_usd=float(candle[4]),
                    volume_btc=float(candle[5]),
                    volume_usd=float(candle[7]),
                    trade_count=int(candle[8]),
                    raw_payload={
                        'kline': candle,
                        'api_base': api_base,
                        'endpoint': endpoint,
                        'market_type': market_type,
                        'fetch_mode': 'local',
                    },
                    source_updated_at=datetime.now(timezone.utc),
                    normalize_bucket_start=False,
                )
            )
        aggregated_rows = aggregate_small_candles_to_10m(
            SOURCE_NAME,
            SYMBOL,
            rows,
            child_minutes=5,
            require_complete_buckets=True,
        )
        upserted = upsert_source_rows(db, SOURCE_NAME, TABLE_NAME, aggregated_rows)
        processed += upserted
        log_price_source_chunk_upserted(logger, SOURCE_NAME, chunk_start, chunk_end, upserted, processed)
        chunk_start = chunk_end

    return processed
