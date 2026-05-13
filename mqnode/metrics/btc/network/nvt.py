from __future__ import annotations

from datetime import datetime

from mqnode.core.errors import DependencyError
from mqnode.core.utils import SATOSHI_PER_BTC, hour_bounds, safe_div

CANONICAL_PRICE_COLUMN = 'canonical_close_price_usd'


def _calc_row(price_usd, supply_total_sat, transferred_sat):
    if price_usd is None:
        return None, None, None
    transferred_value_usd = ((transferred_sat or 0) / SATOSHI_PER_BTC) * price_usd
    market_cap_usd = ((supply_total_sat or 0) / SATOSHI_PER_BTC) * price_usd
    nvt_raw = safe_div(market_cap_usd, transferred_value_usd)
    return transferred_value_usd, market_cap_usd, nvt_raw


def _last_available_price(rows):
    for row in reversed(rows):
        price_usd = row.get(CANONICAL_PRICE_COLUMN)
        if price_usd is not None:
            return price_usd
    return None


def _sum_transferred_value_usd(rows):
    total = 0
    for row in rows:
        price_usd = row.get(CANONICAL_PRICE_COLUMN)
        if price_usd is None:
            continue
        value = ((row['transferred_sat_10m'] or 0) / SATOSHI_PER_BTC) * price_usd
        total += value
    return total


def _height_min(rows, column):
    heights = [row[column] for row in rows if row[column] is not None]
    return min(heights) if heights else None


def _height_max(rows, column):
    heights = [row[column] for row in rows if row[column] is not None]
    return max(heights) if heights else None


def _dependency_not_ready(bucket_start_utc: datetime, interval: str) -> DependencyError:
    return DependencyError(
        f'Canonical price is not ready for NVT bucket {bucket_start_utc.isoformat()} interval {interval}'
    )


def _missing_price_for_value(row) -> bool:
    return (row['transferred_sat_10m'] or 0) > 0 and row.get(CANONICAL_PRICE_COLUMN) is None


def calculate_nvt(db, bucket_start_utc: datetime, interval: str) -> None:
    with db.cursor() as cur:
        if interval == '10m':
            cur.execute(
                '''
                SELECT p.*, px.close_price_usd AS canonical_close_price_usd
                FROM btc_primitive_10m p
                LEFT JOIN mq_btc_price_10m px
                  ON px.bucket_start_utc = p.bucket_start_utc
                WHERE p.bucket_start_utc = %s
                ''',
                (bucket_start_utc,),
            )
            row = cur.fetchone()
            if not row:
                return
            transferred_sat = row['transferred_sat_10m'] or 0
            supply_total_sat = row['supply_total_sat'] or 0
            price_usd = row.get(CANONICAL_PRICE_COLUMN)
            if price_usd is None:
                raise _dependency_not_ready(bucket_start_utc, interval)
            table = 'btc_nvt_10m'
            source_start = row['first_height']
            source_end = row['last_height']
            transferred_value_usd, market_cap_usd, nvt_raw = _calc_row(price_usd, supply_total_sat, transferred_sat)
        elif interval == '1h':
            h_start, h_end = hour_bounds(bucket_start_utc)
            cur.execute(
                '''
                SELECT p.*, px.close_price_usd AS canonical_close_price_usd
                FROM btc_primitive_10m p
                LEFT JOIN mq_btc_price_10m px
                  ON px.bucket_start_utc = p.bucket_start_utc
                WHERE p.bucket_start_utc >= %s
                  AND p.bucket_start_utc < %s
                ORDER BY p.bucket_start_utc ASC
                ''',
                (h_start, h_end),
            )
            rows = cur.fetchall()
            if len(rows) == 0:
                return
            if any(_missing_price_for_value(row) for row in rows):
                raise _dependency_not_ready(h_start, interval)
            transferred_sat = sum(r['transferred_sat_10m'] or 0 for r in rows)
            supply_total_sat = rows[-1]['supply_total_sat'] or 0
            price_usd = _last_available_price(rows)
            if price_usd is None:
                raise _dependency_not_ready(h_start, interval)
            table = 'btc_nvt_1h'
            source_start = _height_min(rows, 'first_height')
            source_end = _height_max(rows, 'last_height')
            bucket_start_utc = h_start
            transferred_value_usd = _sum_transferred_value_usd(rows)
            market_cap_usd = None if price_usd is None else ((supply_total_sat or 0) / SATOSHI_PER_BTC) * price_usd
            nvt_raw = safe_div(market_cap_usd, transferred_value_usd)
        else:
            raise ValueError(f'Unsupported interval for NVT: {interval}')

        cur.execute(
            f'''
            INSERT INTO {table}(
              bucket_start_utc, price_usd, supply_total_sat, market_cap_usd,
              transferred_sat, transferred_value_usd, nvt_raw,
              source_start_height, source_end_height, version, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (bucket_start_utc) DO UPDATE SET
              price_usd = EXCLUDED.price_usd,
              supply_total_sat = EXCLUDED.supply_total_sat,
              market_cap_usd = EXCLUDED.market_cap_usd,
              transferred_sat = EXCLUDED.transferred_sat,
              transferred_value_usd = EXCLUDED.transferred_value_usd,
              nvt_raw = EXCLUDED.nvt_raw,
              source_start_height = EXCLUDED.source_start_height,
              source_end_height = EXCLUDED.source_end_height,
              version = EXCLUDED.version,
              updated_at = now()
            ''',
            (
                bucket_start_utc,
                price_usd,
                supply_total_sat,
                market_cap_usd,
                transferred_sat,
                transferred_value_usd,
                nvt_raw,
                source_start,
                source_end,
                'v1',
            ),
        )
