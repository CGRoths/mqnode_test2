from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import normalize_ohlcv_bucket
from mqnode.market.price.source_support import (
    default_db,
    get_ingestion_window,
    log_price_source_chunk_empty,
    log_price_source_chunk_upserted,
    request_json,
    upsert_source_rows,
)

API_URL = 'https://www.bitstamp.net/api/v2/ohlc/btcusd/'
CHUNK_SIZE = timedelta(seconds=600 * 1000)
NATIVE_10M = True
SOURCE_NAME = 'bitstamp'
SOURCE_INTERVAL = '10m'
TABLE_NAME = 'bitstamp_price_10m'
SYMBOL = 'BTCUSD'
logger = logging.getLogger(__name__)


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch native 10m Bitstamp candles in bounded replay-safe windows."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    chunk_start = start_bucket
    processed = 0
    empty_chunks = 0

    while chunk_start < end_bucket:
        chunk_end = min(chunk_start + CHUNK_SIZE, end_bucket)
        rows_by_bucket: dict[datetime, dict] = {}
        payload = request_json(
            API_URL,
            params={
                'step': 600,
                'limit': 1000,
                'start': int(chunk_start.timestamp()),
                'end': int(chunk_end.timestamp()),
                'exclude_current_candle': 'true',
            },
            timeout=getattr(settings, 'price_request_timeout_seconds', 30),
        )
        candles = sorted(payload.get('data', {}).get('ohlc', []), key=lambda row: int(row['timestamp']))
        if not candles:
            empty_chunks += 1
            log_price_source_chunk_empty(logger, SOURCE_NAME, chunk_start, chunk_end, empty_chunks)
            chunk_start = chunk_end
            continue
        empty_chunks = 0
        for candle in candles:
            bucket_time = datetime.fromtimestamp(int(candle['timestamp']), tz=timezone.utc)
            if bucket_time < chunk_start or bucket_time >= chunk_end:
                continue
            rows_by_bucket[bucket_time] = normalize_ohlcv_bucket(
                SOURCE_NAME,
                bucket_time,
                symbol=SYMBOL,
                open_price_usd=float(candle['open']),
                high_price_usd=float(candle['high']),
                low_price_usd=float(candle['low']),
                close_price_usd=float(candle['close']),
                volume_btc=float(candle['volume']),
                trade_count=int(candle['trades']) if candle.get('trades') is not None else None,
                raw_payload=candle,
                source_updated_at=datetime.now(timezone.utc),
            )
        upserted = upsert_source_rows(db, SOURCE_NAME, TABLE_NAME, list(rows_by_bucket.values()))
        processed += upserted
        log_price_source_chunk_upserted(logger, SOURCE_NAME, chunk_start, chunk_end, upserted, processed)
        chunk_start = chunk_end

    return processed
