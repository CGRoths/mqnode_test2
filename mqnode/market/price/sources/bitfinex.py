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

API_URL = 'https://api-pub.bitfinex.com/v2/candles/trade:10m:tBTCUSD/hist'
CHUNK_SIZE = timedelta(minutes=10 * 1000)
NATIVE_10M = True
SOURCE_NAME = 'bitfinex'
SOURCE_INTERVAL = '10m'
TABLE_NAME = 'bitfinex_price_10m'
SYMBOL = 'tBTCUSD'
logger = logging.getLogger(__name__)


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch direct 10-minute Bitfinex candles and upsert them idempotently."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    chunk_start = start_bucket
    processed = 0
    empty_chunks = 0

    while chunk_start < end_bucket:
        chunk_end = min(chunk_start + CHUNK_SIZE, end_bucket)
        rows = []
        candles = request_json(
            API_URL,
            params={
                'sort': 1,
                'start': int(chunk_start.timestamp() * 1000),
                'end': int(chunk_end.timestamp() * 1000),
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

        for candle in candles:
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
                    high_price_usd=float(candle[3]),
                    low_price_usd=float(candle[4]),
                    close_price_usd=float(candle[2]),
                    volume_btc=float(candle[5]),
                    raw_payload={'candle': candle},
                    source_updated_at=datetime.now(timezone.utc),
                )
            )

        upserted = upsert_source_rows(db, SOURCE_NAME, TABLE_NAME, rows)
        processed += upserted
        log_price_source_chunk_upserted(logger, SOURCE_NAME, chunk_start, chunk_end, upserted, processed)
        chunk_start = chunk_end

    return processed
