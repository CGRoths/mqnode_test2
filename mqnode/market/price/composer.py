from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any

from mqnode.config.settings import get_settings
from mqnode.core.utils import iter_bucket_range, safe_div, to_bucket_start_10m
from mqnode.db.repositories import get_checkpoint
from mqnode.market.price.checkpoints import PRICE_CANONICAL_COMPONENT, price_checkpoint_error, price_checkpoint_ok
from mqnode.market.price.registry import get_enabled_price_sources, get_price_sources
from mqnode.market.price.ws.models import ACCEPTABLE_SOURCE_QUALITY_STATUSES

logger = logging.getLogger(__name__)

COMPOSITION_METHOD = 'volume_weighted_ohlc_v1'
NULL_COMPOSITION_METHOD = 'timeline_null_v1'
DETAIL_METRICS = ('open_price', 'high_price', 'low_price', 'close_price', 'volume_btc')
IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
WARNED_MISSING_TABLES: set[str] = set()
WARNED_READ_FAILURES: set[str] = set()
WARNED_BACKBONE_FAILURES: set[str] = set()
TABLE_COLUMN_CACHE: dict[tuple[str, str], bool] = {}


def _table_exists(cur, table_name: str) -> bool:
    if not IDENTIFIER_RE.fullmatch(table_name):
        return False
    cur.execute('SELECT to_regclass(%s) AS table_name', (table_name,))
    row = cur.fetchone() or {}
    return row.get('table_name') is not None


def _table_has_column(cur, table_name: str, column_name: str) -> bool:
    if not IDENTIFIER_RE.fullmatch(table_name) or not IDENTIFIER_RE.fullmatch(column_name):
        return False
    cache_key = (table_name, column_name)
    if cache_key in TABLE_COLUMN_CACHE:
        return TABLE_COLUMN_CACHE[cache_key]
    try:
        cur.execute(
            '''
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.columns
              WHERE table_name = %s
                AND column_name = %s
            ) AS column_exists
            ''',
            (table_name, column_name),
        )
        row = cur.fetchone() or {}
        exists = bool(row.get('column_exists'))
    except Exception:
        exists = False
    TABLE_COLUMN_CACHE[cache_key] = exists
    return exists


def _existing_enabled_sources(cur):
    sources = []
    for source in get_enabled_price_sources(cur):
        if _table_exists(cur, source.table_name):
            sources.append(source)
            continue
        if source.table_name not in WARNED_MISSING_TABLES:
            logger.warning('price_source_table_missing source=%s table=%s', source.source_name, source.table_name)
            WARNED_MISSING_TABLES.add(source.table_name)
    return tuple(sources)


def _fetch_source_rows_for_bucket(cur, bucket_start_utc: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in _existing_enabled_sources(cur):
        has_quality_status = _table_has_column(cur, source.table_name, 'quality_status')
        quality_select = ', quality_status' if has_quality_status else ''
        quality_filter = (
            'AND (quality_status IS NULL OR quality_status = ANY(%s))'
            if has_quality_status
            else ''
        )
        params = (
            (source.source_name, bucket_start_utc, sorted(ACCEPTABLE_SOURCE_QUALITY_STATUSES))
            if has_quality_status
            else (source.source_name, bucket_start_utc)
        )
        try:
            cur.execute(
                f'''
                SELECT
                  %s AS source_name,
                  bucket_start_utc,
                  open_price_usd,
                  high_price_usd,
                  low_price_usd,
                  close_price_usd,
                  volume_btc,
                  volume_usd,
                  trade_count
                  {quality_select}
                FROM {source.table_name}
                WHERE bucket_start_utc = %s
                  {quality_filter}
                ''',
                params,
            )
            row = cur.fetchone()
        except Exception as exc:
            if source.table_name not in WARNED_READ_FAILURES:
                logger.warning(
                    'price_source_bucket_read_skipped source=%s table=%s error=%s',
                    source.source_name,
                    source.table_name,
                    exc,
                )
                WARNED_READ_FAILURES.add(source.table_name)
            continue
        if row:
            rows.append(row)
    return rows


def _is_valid_source_row(row: dict[str, Any]) -> bool:
    required_fields = ('open_price_usd', 'high_price_usd', 'low_price_usd', 'close_price_usd')
    values = [row.get(field) for field in required_fields]
    if any(value is None or float(value) <= 0 for value in values):
        return False

    open_price = float(row['open_price_usd'])
    high_price = float(row['high_price_usd'])
    low_price = float(row['low_price_usd'])
    close_price = float(row['close_price_usd'])
    if high_price < max(open_price, close_price, low_price):
        return False
    if low_price > min(open_price, close_price, high_price):
        return False
    return True


def _weight_for_row(row: dict[str, Any], rows: list[dict[str, Any]]) -> float:
    volume_btc = float(row.get('volume_btc') or 0)
    if any(float(candidate.get('volume_btc') or 0) > 0 for candidate in rows):
        return max(volume_btc, 0.0)
    return 1.0


def _weighted_aggregate(rows: list[dict[str, Any]], field: str) -> float | None:
    values = [
        (float(row[field]), _weight_for_row(row, rows))
        for row in rows
        if row.get(field) is not None
    ]
    if not values:
        return None
    total_weight = sum(weight for _, weight in values)
    if total_weight <= 0:
        return None
    return sum(value * weight for value, weight in values) / total_weight


def _ordered_source_names(rows: list[dict[str, Any]]) -> list[str]:
    seen = {row['source_name'] for row in rows}
    ordered = [source.source_name for source in get_price_sources() if source.source_name in seen]
    for row in rows:
        source_name = row['source_name']
        if source_name not in ordered:
            ordered.append(source_name)
    return ordered


def compose_canonical_price(bucket_start_utc: datetime, source_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid_rows = [row for row in source_rows if _is_valid_source_row(row)]
    if not valid_rows:
        return None

    source_names = _ordered_source_names(valid_rows)
    total_volume_btc = sum(float(row['volume_btc'] or 0) for row in valid_rows)
    total_volume_usd = sum(float(row['volume_usd'] or 0) for row in valid_rows)
    return {
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'source_count': len(valid_rows),
        'source_names': source_names,
        'composition_method': COMPOSITION_METHOD,
        'open_price_usd': _weighted_aggregate(valid_rows, 'open_price_usd'),
        'high_price_usd': _weighted_aggregate(valid_rows, 'high_price_usd'),
        'low_price_usd': _weighted_aggregate(valid_rows, 'low_price_usd'),
        'close_price_usd': _weighted_aggregate(valid_rows, 'close_price_usd'),
        'volume_btc': total_volume_btc if total_volume_btc > 0 else None,
        'volume_usd': total_volume_usd if total_volume_usd > 0 else None,
    }


def _null_canonical_price_payload(bucket_start_utc: datetime) -> dict[str, Any]:
    return {
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'source_count': 0,
        'source_names': [],
        'composition_method': NULL_COMPOSITION_METHOD,
        'open_price_usd': None,
        'high_price_usd': None,
        'low_price_usd': None,
        'close_price_usd': None,
        'volume_btc': None,
        'volume_usd': None,
    }


def compose_price_details(bucket_start_utc: datetime, source_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    canonical_payload = compose_canonical_price(bucket_start_utc, source_rows)
    if canonical_payload is None:
        return None

    row_map = {row['source_name']: row for row in source_rows if _is_valid_source_row(row)}
    payload: dict[str, Any] = {
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'fair_open_price': canonical_payload['open_price_usd'],
        'fair_high_price': canonical_payload['high_price_usd'],
        'fair_low_price': canonical_payload['low_price_usd'],
        'fair_close_price': canonical_payload['close_price_usd'],
        'total_volume_btc': canonical_payload['volume_btc'],
        'source_count': canonical_payload['source_count'],
        'source_names': canonical_payload['source_names'],
        'composition_method': canonical_payload['composition_method'],
    }
    for source_name, row in row_map.items():
        payload[f'{source_name}_open_price'] = row.get('open_price_usd')
        payload[f'{source_name}_high_price'] = row.get('high_price_usd')
        payload[f'{source_name}_low_price'] = row.get('low_price_usd')
        payload[f'{source_name}_close_price'] = row.get('close_price_usd')
        payload[f'{source_name}_volume_btc'] = row.get('volume_btc')
    return payload


def _null_price_details_payload(bucket_start_utc: datetime) -> dict[str, Any]:
    payload: dict[str, Any] = {
        'bucket_start_utc': to_bucket_start_10m(bucket_start_utc),
        'fair_open_price': None,
        'fair_high_price': None,
        'fair_low_price': None,
        'fair_close_price': None,
        'total_volume_btc': None,
        'source_count': 0,
        'source_names': [],
        'composition_method': NULL_COMPOSITION_METHOD,
    }
    for source in get_price_sources():
        for metric in DETAIL_METRICS:
            payload[f'{source.source_name}_{metric}'] = None
    return payload


def _upsert_canonical_price(cur, payload: dict[str, Any]) -> None:
    cur.execute(
        '''
        INSERT INTO mq_btc_price_10m(
          bucket_start_utc,
          source_count,
          source_names,
          composition_method,
          open_price_usd,
          high_price_usd,
          low_price_usd,
          close_price_usd,
          volume_btc,
          volume_usd,
          updated_at
        ) VALUES (
          %(bucket_start_utc)s,
          %(source_count)s,
          %(source_names)s::jsonb,
          %(composition_method)s,
          %(open_price_usd)s,
          %(high_price_usd)s,
          %(low_price_usd)s,
          %(close_price_usd)s,
          %(volume_btc)s,
          %(volume_usd)s,
          now()
        )
        ON CONFLICT (bucket_start_utc) DO UPDATE SET
          source_count = EXCLUDED.source_count,
          source_names = EXCLUDED.source_names,
          composition_method = EXCLUDED.composition_method,
          open_price_usd = EXCLUDED.open_price_usd,
          high_price_usd = EXCLUDED.high_price_usd,
          low_price_usd = EXCLUDED.low_price_usd,
          close_price_usd = EXCLUDED.close_price_usd,
          volume_btc = EXCLUDED.volume_btc,
          volume_usd = EXCLUDED.volume_usd,
          updated_at = now()
        ''',
        {
            **payload,
            'source_names': json.dumps(payload['source_names']),
        },
    )


def _upsert_price_details(cur, payload: dict[str, Any]) -> None:
    if not _table_exists(cur, 'mq_btc_price_fair_10m_details'):
        logger.warning('price_detail_table_missing table=mq_btc_price_fair_10m_details')
        return
    cur.execute(
        '''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'mq_btc_price_fair_10m_details'
        '''
    )
    available_columns = {row['column_name'] for row in cur.fetchall()}
    columns = ['bucket_start_utc']
    values = ['%(bucket_start_utc)s']
    updates = []
    for column in payload:
        if column == 'bucket_start_utc' or column not in available_columns:
            continue
        columns.append(column)
        values.append(f'%({column})s')
        updates.append(f'{column} = EXCLUDED.{column}')

    update_sql = ', '.join([*updates, 'updated_at = now()'])
    cur.execute(
        f'''
        INSERT INTO mq_btc_price_fair_10m_details(
          {', '.join(columns)},
          updated_at
        ) VALUES (
          {', '.join(values)},
          now()
        )
        ON CONFLICT (bucket_start_utc) DO UPDATE SET
          {update_sql}
        ''',
        {
            **payload,
            'source_names': json.dumps(payload['source_names']),
        },
    )


def rebuild_canonical_price_bucket(db, bucket_start_utc: datetime, settings=None) -> dict[str, Any] | None:
    settings = settings or get_settings()
    bucket_start_utc = to_bucket_start_10m(bucket_start_utc)
    try:
        with db.cursor() as cur:
            source_rows = _fetch_source_rows_for_bucket(cur, bucket_start_utc)
            canonical_payload = compose_canonical_price(bucket_start_utc, source_rows)
            detail_payload = compose_price_details(bucket_start_utc, source_rows)
            if canonical_payload is None or detail_payload is None:
                if getattr(settings, 'price_composer_write_null_buckets', True):
                    canonical_payload = _null_canonical_price_payload(bucket_start_utc)
                    detail_payload = _null_price_details_payload(bucket_start_utc)
                    _upsert_canonical_price(cur, canonical_payload)
                    _upsert_price_details(cur, detail_payload)
                else:
                    cur.execute('DELETE FROM mq_btc_price_10m WHERE bucket_start_utc = %s', (bucket_start_utc,))
                    if _table_exists(cur, 'mq_btc_price_fair_10m_details'):
                        cur.execute(
                            'DELETE FROM mq_btc_price_fair_10m_details WHERE bucket_start_utc = %s',
                            (bucket_start_utc,),
                        )
            else:
                _upsert_canonical_price(cur, canonical_payload)
                _upsert_price_details(cur, detail_payload)
            price_checkpoint_ok(cur, last_bucket_time=bucket_start_utc)
            return canonical_payload
    except Exception as exc:
        logger.exception('canonical_price_rebuild_failed bucket=%s error=%s', bucket_start_utc.isoformat(), exc)
        with db.cursor() as cur:
            price_checkpoint_error(cur, str(exc), last_bucket_time=bucket_start_utc)
        raise


def _price_bounds_query(table_names: list[str]) -> str:
    selects = [f'SELECT bucket_start_utc FROM {table_name}' for table_name in table_names]
    return ' UNION ALL '.join(selects)


def _timeline_backbone_bounds(cur, table_name: str) -> tuple[datetime, datetime] | None:
    if not table_name or not IDENTIFIER_RE.fullmatch(table_name):
        logger.warning('price_composer_invalid_timeline_backbone table=%s', table_name)
        return None
    if not _table_exists(cur, table_name):
        return None
    try:
        cur.execute(
            f'''
            SELECT MIN(bucket_start_utc) AS min_bucket, MAX(bucket_start_utc) AS max_bucket
            FROM {table_name}
            '''
        )
        bounds = cur.fetchone() or {}
    except Exception as exc:
        if table_name not in WARNED_BACKBONE_FAILURES:
            logger.warning('price_composer_timeline_backbone_skipped table=%s error=%s', table_name, exc)
            WARNED_BACKBONE_FAILURES.add(table_name)
        return None

    min_bucket = bounds.get('min_bucket')
    max_bucket = bounds.get('max_bucket')
    if min_bucket is None or max_bucket is None:
        return None
    return to_bucket_start_10m(min_bucket), to_bucket_start_10m(max_bucket)


def _raw_price_bounds(cur) -> tuple[datetime, datetime] | None:
    enabled_sources = _existing_enabled_sources(cur)
    table_names = [source.table_name for source in enabled_sources if IDENTIFIER_RE.fullmatch(source.table_name)]
    if not table_names:
        return None

    cur.execute(
        f'''
        SELECT MIN(bucket_start_utc) AS min_bucket, MAX(bucket_start_utc) AS max_bucket
        FROM (
          {_price_bounds_query(table_names)}
        ) price_buckets
        '''
    )
    bounds = cur.fetchone() or {}
    min_bucket = bounds.get('min_bucket')
    max_bucket = bounds.get('max_bucket')
    if min_bucket is None or max_bucket is None:
        return None
    return to_bucket_start_10m(min_bucket), to_bucket_start_10m(max_bucket)


def catch_up_canonical_price_from_checkpoint(db, end_bucket: datetime | None = None, settings=None) -> int:
    settings = settings or get_settings()
    with db.cursor() as cur:
        checkpoint = get_checkpoint(cur, 'BTC', PRICE_CANONICAL_COMPONENT, '10m')
        replay_start = checkpoint.get('last_bucket_time')
        bounds = _timeline_backbone_bounds(
            cur,
            getattr(settings, 'price_composer_timeline_backbone', 'btc_primitive_10m'),
        )
        if bounds is None:
            bounds = _raw_price_bounds(cur)

    if bounds is None:
        return 0

    min_bucket, max_bucket = bounds
    start_bucket = to_bucket_start_10m(replay_start) if replay_start is not None else min_bucket
    if start_bucket < min_bucket:
        start_bucket = min_bucket
    final_bucket = min(to_bucket_start_10m(end_bucket), max_bucket) if end_bucket is not None else max_bucket
    if final_bucket < start_bucket:
        return 0

    rebuilt = 0
    for bucket in iter_bucket_range(start_bucket, final_bucket, 10):
        rebuild_canonical_price_bucket(db, bucket, settings=settings)
        rebuilt += 1
    return rebuilt


def estimate_price_completeness(source_rows: list[dict[str, Any]]) -> float | None:
    if not source_rows:
        return None
    return safe_div(len([row for row in source_rows if _is_valid_source_row(row)]), len(source_rows))
