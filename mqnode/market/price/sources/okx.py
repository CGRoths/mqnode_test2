from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import aggregate_small_candles_to_10m, normalize_ohlcv_bucket
from mqnode.market.price.source_support import (
    default_db,
    get_ingestion_window,
    log_price_source_chunk_empty,
    log_price_source_chunk_upserted,
    request_json,
    upsert_source_rows,
)

API_URL = 'https://www.okx.com/api/v5/market/history-candles'
FIVE_MINUTE_CHUNK = timedelta(hours=24)
NATIVE_10M = False
SOURCE_NAME = 'okx'
SOURCE_INTERVAL = '5m'
TABLE_NAME = 'okx_price_10m'
SYMBOL = 'BTC-USDT'
logger = logging.getLogger(__name__)


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch replay-safe OKX 5m spot candles and aggregate complete 10m buckets."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    chunk_start = start_bucket
    processed = 0
    empty_chunks = 0

    while chunk_start < end_bucket:
        chunk_end = min(chunk_start + FIVE_MINUTE_CHUNK, end_bucket)
        rows = []
        payload = request_json(
            API_URL,
            params={
                'instId': SYMBOL,
                'bar': '5m',
                'after': str(int(chunk_start.timestamp() * 1000)),
                'before': str(int(chunk_end.timestamp() * 1000)),
                'limit': 300,
            },
            timeout=getattr(settings, 'price_request_timeout_seconds', 30),
        )
        candles = sorted(payload.get('data', []), key=lambda row: int(row[0]))
        if not candles:
            empty_chunks += 1
            log_price_source_chunk_empty(logger, SOURCE_NAME, chunk_start, chunk_end, empty_chunks)
            chunk_start = chunk_end
            continue
        empty_chunks = 0
        for candle in candles:
            if len(candle) > 8 and candle[8] == '0':
                continue
            open_time_ms = int(candle[0])
            bucket_time = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
            if bucket_time < chunk_start or bucket_time >= chunk_end:
                continue
            volume_usd = None
            if len(candle) > 7 and candle[7] not in (None, ''):
                volume_usd = float(candle[7])
            elif len(candle) > 6 and candle[6] not in (None, ''):
                volume_usd = float(candle[6])
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
                    volume_usd=volume_usd,
                    raw_payload={'kline': candle},
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
