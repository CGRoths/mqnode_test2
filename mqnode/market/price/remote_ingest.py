from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from mqnode.market.price.source_support import upsert_source_rows
from mqnode.market.price.ws.repository import SOURCE_QUALITY_COLUMNS, PriceWsRepository

SOURCE_NAME = 'binance'
TABLE_NAME = 'binance_price_10m'
SYMBOL = 'BTCUSDT'
MARKET_TYPE = 'spot'
MAX_REMOTE_ROWS = 1000
SOURCE_QUALITY_DATETIME_COLUMNS = {
    'ws_closed_at',
    'rest_confirmed_at',
    'first_received_at',
    'last_received_at',
}
SOURCE_QUALITY_INT_COLUMNS = {
    'ws_disconnect_count',
    'ws_gap_count',
    'expected_child_candle_count',
    'actual_child_candle_count',
    'revision_count',
}
SOURCE_QUALITY_BOOL_COLUMNS = {'is_repaired', 'is_revised'}


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError('datetime values must include timezone information')
    return value.astimezone(timezone.utc)


def _is_10m_aligned(value: datetime) -> bool:
    return value.minute % 10 == 0 and value.second == 0 and value.microsecond == 0


def _validate_ohlc(row: dict[str, Any]) -> tuple[float, float, float, float]:
    open_price = float(row['open_price_usd'])
    high_price = float(row['high_price_usd'])
    low_price = float(row['low_price_usd'])
    close_price = float(row['close_price_usd'])
    if min(open_price, high_price, low_price, close_price) <= 0:
        raise ValueError('OHLC prices must be positive')
    if high_price < max(open_price, close_price, low_price):
        raise ValueError('high_price_usd must be greater than or equal to open/close/low')
    if low_price > min(open_price, close_price, high_price):
        raise ValueError('low_price_usd must be less than or equal to open/close/high')
    return open_price, high_price, low_price, close_price


def _json_safe_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key, value in payload.items():
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
    return payload


def _optional_utc(row: dict[str, Any], key: str) -> datetime | None:
    value = row.get(key)
    if value is None:
        return None
    return _as_utc(value)


def _optional_iso(row: dict[str, Any], key: str) -> str | None:
    value = _optional_utc(row, key)
    return value.isoformat() if value is not None else None


def _quality_metadata(row: dict[str, Any]) -> dict[str, Any]:
    metadata = {}
    for key in SOURCE_QUALITY_COLUMNS:
        if key not in row or row[key] is None:
            continue
        value = row[key]
        if key in SOURCE_QUALITY_DATETIME_COLUMNS:
            value = _as_utc(value)
        elif key in SOURCE_QUALITY_INT_COLUMNS:
            value = int(value)
        elif key in SOURCE_QUALITY_BOOL_COLUMNS:
            value = bool(value)
        metadata[key] = value
    return metadata


def normalize_remote_binance_row(row: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize one cloud-fed Binance 10m row for raw-table upsert."""
    source_name = row.get('source_name', SOURCE_NAME)
    if source_name != SOURCE_NAME:
        raise ValueError('source_name must be binance')
    bucket_start_utc = _as_utc(row['bucket_start_utc'])
    if not _is_10m_aligned(bucket_start_utc):
        raise ValueError('bucket_start_utc must be aligned to a 10m boundary')
    open_price, high_price, low_price, close_price = _validate_ohlc(row)
    volume_btc = row.get('volume_btc')
    volume_usd = row.get('volume_usd')
    trade_count = row.get('trade_count')
    local_received_at = datetime.now(timezone.utc)
    source_updated_at = _optional_utc(row, 'source_updated_at') or local_received_at
    raw_payload = row.get('raw_payload') if isinstance(row.get('raw_payload'), dict) else None
    return {
        'source_name': SOURCE_NAME,
        'symbol': row.get('symbol') or SYMBOL,
        'bucket_start_utc': bucket_start_utc,
        'open_price_usd': open_price,
        'high_price_usd': high_price,
        'low_price_usd': low_price,
        'close_price_usd': close_price,
        'volume_btc': float(volume_btc) if volume_btc is not None else None,
        'volume_usd': float(volume_usd) if volume_usd is not None else None,
        'trade_count': int(trade_count) if trade_count is not None else None,
        'raw_payload': {
            'fetch_mode': 'remote_cloud_api',
            'market_type': MARKET_TYPE,
            'exchange_close_time': _optional_iso(row, 'exchange_close_time'),
            'cloud_fetched_at': _optional_iso(row, 'cloud_fetched_at'),
            'cloud_sent_at': _optional_iso(row, 'cloud_sent_at'),
            'local_received_at': local_received_at.isoformat(),
            'collector_raw_payload': _json_safe_payload(raw_payload) if raw_payload is not None else None,
            'remote_payload': _json_safe_payload(row),
        },
        'source_updated_at': source_updated_at,
        **_quality_metadata(row),
    }


def ingest_remote_binance_rows(
    db,
    rows: list[dict[str, Any]],
    *,
    advance_checkpoint: bool = False,
    repository: PriceWsRepository | None = None,
) -> int:
    """Upsert cloud-fed Binance 10m rows without claiming local source replay progress."""
    if not rows:
        raise ValueError('rows must be non-empty')
    if len(rows) > MAX_REMOTE_ROWS:
        raise ValueError(f'rows must contain at most {MAX_REMOTE_ROWS} items')
    normalized_rows = [normalize_remote_binance_row(row) for row in rows]
    rows_upserted = upsert_source_rows(
        db,
        SOURCE_NAME,
        TABLE_NAME,
        normalized_rows,
        advance_checkpoint=advance_checkpoint,
    )
    quality_rows = [
        row
        for row in normalized_rows
        if any(column in row for column in SOURCE_QUALITY_COLUMNS)
    ]
    if quality_rows:
        repository = repository or PriceWsRepository()
        with db.cursor() as cur:
            for row in quality_rows:
                metadata = {column: row[column] for column in SOURCE_QUALITY_COLUMNS if column in row}
                repository.mark_source_quality(cur, TABLE_NAME, row['bucket_start_utc'], metadata)
    return rows_upserted
