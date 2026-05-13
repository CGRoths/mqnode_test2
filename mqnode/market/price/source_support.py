from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from mqnode.config.settings import get_settings
from mqnode.core.utils import to_bucket_start_10m, utc_now
from mqnode.db.connection import DB
from mqnode.db.repositories import get_checkpoint
from mqnode.market.price.checkpoints import (
    price_source_checkpoint_error,
    price_source_checkpoint_ok,
    price_source_component,
)
from mqnode.market.price.registry import get_price_source

IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
logger = logging.getLogger(__name__)


def request_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
) -> Any:
    attempt = 1
    while True:
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in TRANSIENT_STATUS_CODES or attempt >= max_attempts:
                raise
            logger.warning(
                'price_request_retry url=%s status=%s attempt=%s/%s',
                url,
                status_code,
                attempt,
                max_attempts,
            )
        except requests.RequestException:
            if attempt >= max_attempts:
                raise
            logger.warning('price_request_retry url=%s attempt=%s/%s', url, attempt, max_attempts)
        time.sleep(backoff_seconds * attempt)
        attempt += 1


def _parse_utc_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError('historical_start_utc must be timezone-aware')
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError('historical_start_utc must be timezone-aware')
        return parsed.astimezone(timezone.utc)
    raise ValueError('Unsupported historical_start_utc type')


def _fallback_replay_start(lookback_hours: int) -> datetime:
    return to_bucket_start_10m(utc_now() - timedelta(hours=lookback_hours))


def log_price_source_chunk_upserted(
    chunk_logger: logging.Logger,
    source_name: str,
    chunk_start: datetime,
    chunk_end: datetime,
    upserted: int,
    total_upserted: int,
) -> None:
    chunk_logger.info(
        'price_source_chunk_upserted source=%s start=%s end=%s rows=%s total=%s',
        source_name,
        chunk_start.isoformat(),
        chunk_end.isoformat(),
        upserted,
        total_upserted,
    )


def log_price_source_chunk_empty(
    chunk_logger: logging.Logger,
    source_name: str,
    chunk_start: datetime,
    chunk_end: datetime,
    empty_streak: int,
) -> None:
    chunk_logger.warning(
        'price_source_chunk_empty source=%s start=%s end=%s empty_streak=%s',
        source_name,
        chunk_start.isoformat(),
        chunk_end.isoformat(),
        empty_streak,
    )


def get_ingestion_window(db, source_name: str, *, lookback_hours: int = 48) -> tuple[datetime, datetime]:
    end_bucket = to_bucket_start_10m(utc_now())
    with db.cursor() as cur:
        checkpoint = get_checkpoint(cur, 'BTC', price_source_component(source_name), '10m')
        last_bucket_time = checkpoint.get('last_bucket_time')
        if last_bucket_time is not None:
            start_bucket = to_bucket_start_10m(last_bucket_time - timedelta(minutes=10))
            logger.info(
                'price_source_live_replay_start source=%s start=%s end=%s',
                source_name,
                start_bucket.isoformat(),
                end_bucket.isoformat(),
            )
            return start_bucket, end_bucket

        source = get_price_source(source_name, cur)
        historical_start_utc = (source.config or {}).get('historical_start_utc')

    try:
        configured_start = _parse_utc_datetime(historical_start_utc)
    except (TypeError, ValueError) as exc:
        logger.warning(
            'price_source_historical_start_invalid source=%s value=%s error=%s',
            source_name,
            historical_start_utc,
            exc,
        )
        configured_start = None

    if configured_start is not None:
        start_bucket = to_bucket_start_10m(configured_start)
    else:
        start_bucket = _fallback_replay_start(lookback_hours)
    logger.info(
        'price_source_historical_bootstrap_start source=%s start=%s end=%s',
        source_name,
        start_bucket.isoformat(),
        end_bucket.isoformat(),
    )
    return start_bucket, end_bucket


def filter_closed_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    current_bucket = to_bucket_start_10m(utc_now())
    deduped = {
        row['bucket_start_utc']: row
        for row in rows
        if row.get('bucket_start_utc') is not None and row['bucket_start_utc'] < current_bucket
    }
    return [deduped[bucket] for bucket in sorted(deduped)]


def upsert_source_rows(
    db,
    source_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    advance_checkpoint: bool = True,
    checkpoint_bucket_time: datetime | None = None,
) -> int:
    if not IDENTIFIER_RE.fullmatch(table_name):
        raise ValueError(f'Invalid price table name: {table_name}')

    rows = filter_closed_rows(rows)
    if not rows:
        return 0

    last_bucket_time = checkpoint_bucket_time or max(row['bucket_start_utc'] for row in rows)
    try:
        with db.cursor() as cur:
            for row in rows:
                cur.execute(
                    f'''
                    INSERT INTO {table_name}(
                      bucket_start_utc,
                      symbol,
                      open_price_usd,
                      high_price_usd,
                      low_price_usd,
                      close_price_usd,
                      volume_btc,
                      volume_usd,
                      trade_count,
                      raw_payload,
                      source_updated_at,
                      updated_at
                    ) VALUES (
                      %(bucket_start_utc)s,
                      %(symbol)s,
                      %(open_price_usd)s,
                      %(high_price_usd)s,
                      %(low_price_usd)s,
                      %(close_price_usd)s,
                      %(volume_btc)s,
                      %(volume_usd)s,
                      %(trade_count)s,
                      %(raw_payload)s::jsonb,
                      %(source_updated_at)s,
                      now()
                    )
                    ON CONFLICT (bucket_start_utc) DO UPDATE SET
                      symbol = EXCLUDED.symbol,
                      open_price_usd = EXCLUDED.open_price_usd,
                      high_price_usd = EXCLUDED.high_price_usd,
                      low_price_usd = EXCLUDED.low_price_usd,
                      close_price_usd = EXCLUDED.close_price_usd,
                      volume_btc = EXCLUDED.volume_btc,
                      volume_usd = EXCLUDED.volume_usd,
                      trade_count = EXCLUDED.trade_count,
                      raw_payload = EXCLUDED.raw_payload,
                      source_updated_at = EXCLUDED.source_updated_at,
                      updated_at = now()
                    ''',
                    {
                        **row,
                        'raw_payload': json.dumps(row.get('raw_payload')),
                    },
                )
            # Recent-only sources may write useful rows without claiming a full historical replay checkpoint.
            if advance_checkpoint:
                price_source_checkpoint_ok(cur, source_name, last_bucket_time=last_bucket_time)
    except Exception as exc:
        with db.cursor() as cur:
            price_source_checkpoint_error(cur, source_name, str(exc), last_bucket_time=last_bucket_time)
        raise

    return len(rows)


def default_db(db=None):
    return db or DB(get_settings())
