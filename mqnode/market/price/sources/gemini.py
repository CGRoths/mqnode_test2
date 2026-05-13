from __future__ import annotations

import logging
from datetime import datetime, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import aggregate_small_candles_to_10m, normalize_ohlcv_bucket
from mqnode.market.price.source_support import default_db, get_ingestion_window, request_json, upsert_source_rows

API_URL = 'https://api.gemini.com/v2/candles/btcusd/5m'
NATIVE_10M = False
SOURCE_NAME = 'gemini'
SOURCE_INTERVAL = '5m'
SUPPORTS_FULL_HISTORICAL_REPLAY = False
TABLE_NAME = 'gemini_price_10m'
SYMBOL = 'BTCUSD'
logger = logging.getLogger(__name__)


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch recent-only Gemini 5m candles and aggregate complete 10m buckets."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    payload = request_json(
        API_URL,
        timeout=getattr(settings, 'price_request_timeout_seconds', 30),
    )

    rows = []
    earliest_available = None
    for candle in sorted(payload, key=lambda row: int(row[0])):
        open_time = datetime.fromtimestamp(int(candle[0]) / 1000, tz=timezone.utc)
        earliest_available = earliest_available or open_time
        if open_time < start_bucket or open_time >= end_bucket:
            continue
        rows.append(
            normalize_ohlcv_bucket(
                SOURCE_NAME,
                open_time,
                symbol=SYMBOL,
                open_price_usd=float(candle[1]),
                high_price_usd=float(candle[2]),
                low_price_usd=float(candle[3]),
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
    can_advance_checkpoint = bool(aggregated_rows)
    if earliest_available is not None and start_bucket < earliest_available:
        logger.warning(
            'gemini_recent_only_gap requested_start=%s earliest_available=%s',
            start_bucket.isoformat(),
            earliest_available.isoformat(),
        )
        can_advance_checkpoint = False
    return upsert_source_rows(
        db,
        SOURCE_NAME,
        TABLE_NAME,
        aggregated_rows,
        advance_checkpoint=can_advance_checkpoint,
    )
