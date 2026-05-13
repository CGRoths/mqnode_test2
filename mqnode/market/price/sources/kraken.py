from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from mqnode.config.settings import get_settings
from mqnode.market.price.normalize import aggregate_small_candles_to_10m, normalize_ohlcv_bucket
from mqnode.market.price.source_support import default_db, get_ingestion_window, request_json, upsert_source_rows

API_URL = 'https://api.kraken.com/0/public/OHLC'
AVAILABLE_RECENT_CANDLES = 720
RECENT_WINDOW = timedelta(minutes=AVAILABLE_RECENT_CANDLES * 5)
NATIVE_10M = False
SOURCE_NAME = 'kraken'
SOURCE_INTERVAL = '5m'
SUPPORTS_FULL_HISTORICAL_REPLAY = False
TABLE_NAME = 'kraken_price_10m'
SYMBOL = 'XBT/USD'
logger = logging.getLogger(__name__)


def fetch_buckets(db=None, settings=None) -> int:
    """Fetch recent-only Kraken 5m candles and aggregate the closed 10m buckets we can cover."""
    settings = settings or get_settings()
    db = default_db(db)
    start_bucket, end_bucket = get_ingestion_window(db, SOURCE_NAME)
    recent_start = max(start_bucket, end_bucket - RECENT_WINDOW)
    payload = request_json(
        API_URL,
        params={
            'pair': 'XBTUSD',
            'interval': 5,
            'since': int(recent_start.timestamp()),
        },
        timeout=getattr(settings, 'price_request_timeout_seconds', 30),
    )

    result = payload.get('result', {})
    pair_key = next((key for key in result if key != 'last'), None)
    candles = sorted(result.get(pair_key, []) if pair_key else [], key=lambda row: int(float(row[0])))
    if candles:
        # Kraken includes the current building interval as the last row for REST OHLC responses.
        candles = candles[:-1]
    rows = []
    earliest_available = None
    for candle in candles:
        open_time = datetime.fromtimestamp(int(float(candle[0])), tz=timezone.utc)
        earliest_available = earliest_available or open_time
        if open_time < start_bucket or open_time >= end_bucket:
            continue
        trade_count = int(float(candle[7])) if len(candle) > 7 else None
        rows.append(
            normalize_ohlcv_bucket(
                SOURCE_NAME,
                open_time,
                symbol=SYMBOL,
                open_price_usd=float(candle[1]),
                high_price_usd=float(candle[2]),
                low_price_usd=float(candle[3]),
                close_price_usd=float(candle[4]),
                volume_btc=float(candle[6]),
                trade_count=trade_count,
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
            'kraken_recent_only_gap requested_start=%s earliest_available=%s',
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
