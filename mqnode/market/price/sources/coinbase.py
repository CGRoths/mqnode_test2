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

API_URL = 'https://api.exchange.coinbase.com/products/BTC-USD/candles'
FIVE_MINUTE_CHUNK = timedelta(hours=24)
NATIVE_10M = False
SOURCE_NAME = 'coinbase'
SOURCE_INTERVAL = '5m'
TABLE_NAME = 'coinbase_price_10m'
SYMBOL = 'BTC-USD'
logger = logging.getLogger(__name__)


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch 5m Coinbase candles in bounded windows and aggregate complete 10m buckets."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    chunk_start = start_bucket
    empty_chunks = 0
    processed = 0

    while chunk_start < end_bucket:
        chunk_end = min(chunk_start + FIVE_MINUTE_CHUNK, end_bucket)
        rows = []
        candles = request_json(
            API_URL,
            params={
                'start': chunk_start.isoformat(),
                'end': chunk_end.isoformat(),
                'granularity': 300,
            },
            timeout=getattr(settings, 'price_request_timeout_seconds', 30),
        )
        if not candles:
            empty_chunks += 1
            log_price_source_chunk_empty(logger, SOURCE_NAME, chunk_start, chunk_end, empty_chunks)
            # Empty slices can happen on sparse history edges; move forward instead of aborting the replay.
            chunk_start = chunk_end
            continue
        empty_chunks = 0
        for candle in sorted(candles, key=lambda row: int(row[0])):
            open_time = datetime.fromtimestamp(int(candle[0]), tz=timezone.utc)
            if open_time < chunk_start or open_time >= chunk_end:
                continue
            rows.append(
                normalize_ohlcv_bucket(
                    SOURCE_NAME,
                    open_time,
                    symbol=SYMBOL,
                    open_price_usd=float(candle[3]),
                    high_price_usd=float(candle[2]),
                    low_price_usd=float(candle[1]),
                    close_price_usd=float(candle[4]),
                    volume_btc=float(candle[5]),
                    raw_payload={'candle': candle},
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
