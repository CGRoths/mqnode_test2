from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from mqnode.config.settings import Settings
from mqnode.core.utils import (
    SATOSHI_PER_BTC,
    iter_bucket_range,
    median,
    safe_div,
    to_bucket_start_10m,
    to_open_time_ms,
)
from mqnode.db.repositories import get_checkpoint, upsert_checkpoint
from mqnode.queue.producer import enqueue_primitive_ready

logger = logging.getLogger(__name__)

RAW_COMPONENT = 'btc_raw_block_ingestion'
PRIMITIVE_COMPONENT = 'btc_primitive_10m_builder'
PRIMITIVE_SCHEDULER_COMPONENT = 'btc_primitive_10m_scheduler'
PRIMITIVE_INTERVAL = '10m'
PRIMITIVE_INTERVAL_MINUTES = 10
UPSERT_COLUMNS = (
    'bucket_start_utc',
    'open_time_ms',
    'first_height',
    'last_height',
    'block_count',
    'first_block_time_utc',
    'last_block_time_utc',
    'issued_sat_10m',
    'fees_sat_10m',
    'miner_revenue_sat_10m',
    'supply_total_sat',
    'block_reward_sat_avg',
    'halving_epoch',
    'total_out_sat_10m',
    'total_fee_sat_10m',
    'transferred_sat_10m',
    'transferred_btc_10m',
    'tx_count_10m',
    'non_coinbase_tx_count_10m',
    'input_count_10m',
    'output_count_10m',
    'tx_rate_per_sec_10m',
    'block_size_total_bytes_10m',
    'block_size_mean_bytes_10m',
    'block_weight_total_wu_10m',
    'block_weight_mean_wu_10m',
    'block_vsize_total_vb_10m',
    'tx_size_total_bytes_10m',
    'tx_size_mean_bytes_10m',
    'block_interval_mean_sec_10m',
    'block_interval_median_sec_10m',
    'avg_fee_sat_10m',
    'median_fee_sat_10m',
    'avg_feerate_sat_vb_10m',
    'min_feerate_sat_vb_10m',
    'max_feerate_sat_vb_10m',
    'utxo_increase_10m',
    'utxo_count_total',
    'utxo_size_inc_bytes_10m',
    'utxo_set_size_total_bytes',
    'spent_output_count_10m',
    'created_output_count_10m',
    'segwit_tx_count_10m',
    'segwit_share_10m',
    'sw_total_size_bytes_10m',
    'sw_total_weight_wu_10m',
    'difficulty_last',
    'chainwork_last',
    'hashrate_est_last',
    'hashrate_est_mean_10m',
    'best_block_height_last',
    'updated_at',
)
UPSERT_UPDATE_COLUMNS = tuple(column for column in UPSERT_COLUMNS if column != 'bucket_start_utc')


def _event_time(row: dict[str, Any]) -> datetime | None:
    return row.get('event_time') or row.get('median_time') or row.get('block_time')


def _bucket_end(bucket_start_utc: datetime) -> datetime:
    return bucket_start_utc + timedelta(minutes=PRIMITIVE_INTERVAL_MINUTES)


def _bucket_expression() -> str:
    return (
        "date_trunc('hour', COALESCE(median_time, block_time)) + "
        "floor(extract(minute from COALESCE(median_time, block_time)) / 10) * interval '10 minutes'"
    )


def _fetch_bucket_for_height(cur, height: int) -> datetime | None:
    cur.execute(
        '''
        SELECT COALESCE(median_time, block_time) AS event_time
        FROM btc_primitive_block
        WHERE height = %s
        ''',
        (height,),
    )
    row = cur.fetchone()
    if not row or not row['event_time']:
        return None
    return to_bucket_start_10m(row['event_time'])


def _fetch_earliest_raw_bucket(cur) -> datetime | None:
    cur.execute(
        '''
        SELECT MIN(COALESCE(median_time, block_time)) AS event_time
        FROM btc_primitive_block
        '''
    )
    row = cur.fetchone()
    if not row or not row['event_time']:
        return None
    return to_bucket_start_10m(row['event_time'])


def _fetch_blocks_for_bucket(cur, bucket_start_utc: datetime) -> list[dict[str, Any]]:
    cur.execute(
        '''
        SELECT
          height,
          block_hash,
          block_time,
          median_time,
          COALESCE(median_time, block_time) AS event_time,
          tx_count,
          non_coinbase_tx_count,
          total_out_sat,
          total_fee_sat,
          subsidy_sat,
          issued_sat,
          miner_revenue_sat,
          input_count,
          output_count,
          block_size_bytes,
          block_weight_wu,
          block_vsize_vb,
          tx_size_total_bytes,
          tx_vsize_total_vb,
          avg_fee_sat,
          min_feerate_sat_vb,
          max_feerate_sat_vb,
          segwit_tx_count,
          sw_total_size_bytes,
          sw_total_weight_wu,
          difficulty,
          chainwork,
          cumulative_supply_sat
        FROM btc_primitive_block
        WHERE COALESCE(median_time, block_time) >= %s
          AND COALESCE(median_time, block_time) < %s
        ORDER BY height ASC
        ''',
        (bucket_start_utc, _bucket_end(bucket_start_utc)),
    )
    return cur.fetchall()


def _fetch_latest_chain_state(cur, bucket_start_utc: datetime) -> dict[str, Any] | None:
    cur.execute(
        '''
        SELECT
          height,
          COALESCE(median_time, block_time) AS event_time,
          difficulty,
          chainwork,
          cumulative_supply_sat
        FROM btc_primitive_block
        WHERE COALESCE(median_time, block_time) < %s
        ORDER BY height DESC
        LIMIT 1
        ''',
        (_bucket_end(bucket_start_utc),),
    )
    return cur.fetchone()


def build_10m_bucket_payload(
    cur,
    bucket_start_utc: datetime,
    settings: Settings | None = None,
) -> dict[str, Any] | None:
    """Aggregate one canonical 10-minute primitive bucket from block-level primitives."""
    blocks = _fetch_blocks_for_bucket(cur, bucket_start_utc)
    state_row = _fetch_latest_chain_state(cur, bucket_start_utc)
    if not blocks and not state_row:
        return None

    block_count = len(blocks)
    first = blocks[0] if blocks else None
    last = blocks[-1] if blocks else None

    issued_sat = sum(int(block['issued_sat'] or 0) for block in blocks)
    total_fee_sat = sum(int(block['total_fee_sat'] or 0) for block in blocks)
    total_out_sat = sum(int(block['total_out_sat'] or 0) for block in blocks)
    tx_count = sum(int(block['tx_count'] or 0) for block in blocks)
    non_coinbase_tx_count = sum(int(block['non_coinbase_tx_count'] or 0) for block in blocks)
    input_count = sum(int(block['input_count'] or 0) for block in blocks)
    output_count = sum(int(block['output_count'] or 0) for block in blocks)
    block_size_total = sum(int(block['block_size_bytes'] or 0) for block in blocks)
    block_weight_total = sum(int(block['block_weight_wu'] or 0) for block in blocks)
    block_vsize_total = sum(int(block['block_vsize_vb'] or 0) for block in blocks)
    tx_size_total = sum(int(block['tx_size_total_bytes'] or 0) for block in blocks)
    tx_vsize_total = sum(int(block['tx_vsize_total_vb'] or 0) for block in blocks)
    segwit_tx_count = sum(int(block['segwit_tx_count'] or 0) for block in blocks)
    sw_total_size_bytes = sum(int(block['sw_total_size_bytes'] or 0) for block in blocks)
    sw_total_weight_wu = sum(int(block['sw_total_weight_wu'] or 0) for block in blocks)
    miner_revenue_sat = sum(
        int(block.get('miner_revenue_sat') or ((block.get('issued_sat') or 0) + (block.get('total_fee_sat') or 0)))
        for block in blocks
    )
    # `total_out_sat` includes coinbase outputs, so we remove claimed miner reward to keep transfer volume
    # aligned with non-issuance transaction output flow.
    transferred_sat = max(total_out_sat - miner_revenue_sat, 0)
    block_times = [event_time.timestamp() for block in blocks if (event_time := _event_time(block)) is not None]
    intervals = [right - left for left, right in zip(block_times, block_times[1:])]
    feerate_min_values = [block['min_feerate_sat_vb'] for block in blocks if block['min_feerate_sat_vb'] is not None]
    feerate_max_values = [block['max_feerate_sat_vb'] for block in blocks if block['max_feerate_sat_vb'] is not None]

    supply_total_sat = state_row['cumulative_supply_sat'] if state_row else None
    best_block_height_last = state_row['height'] if state_row else None

    return {
        'bucket_start_utc': bucket_start_utc,
        'open_time_ms': to_open_time_ms(bucket_start_utc),
        'first_height': first['height'] if first else None,
        'last_height': last['height'] if last else None,
        'block_count': block_count,
        'first_block_time_utc': _event_time(first) if first else None,
        'last_block_time_utc': _event_time(last) if last else None,
        'issued_sat_10m': issued_sat,
        'fees_sat_10m': total_fee_sat,
        'miner_revenue_sat_10m': miner_revenue_sat,
        'supply_total_sat': supply_total_sat,
        'block_reward_sat_avg': safe_div(issued_sat, block_count),
        'halving_epoch': int(best_block_height_last // 210000) if best_block_height_last is not None else None,
        'total_out_sat_10m': total_out_sat,
        'total_fee_sat_10m': total_fee_sat,
        'transferred_sat_10m': transferred_sat,
        'transferred_btc_10m': transferred_sat / SATOSHI_PER_BTC,
        'tx_count_10m': tx_count,
        'non_coinbase_tx_count_10m': non_coinbase_tx_count,
        'input_count_10m': input_count,
        'output_count_10m': output_count,
        'tx_rate_per_sec_10m': tx_count / 600,
        'block_size_total_bytes_10m': block_size_total,
        'block_size_mean_bytes_10m': safe_div(block_size_total, block_count),
        'block_weight_total_wu_10m': block_weight_total,
        'block_weight_mean_wu_10m': safe_div(block_weight_total, block_count),
        'block_vsize_total_vb_10m': block_vsize_total,
        'tx_size_total_bytes_10m': tx_size_total,
        'tx_size_mean_bytes_10m': safe_div(tx_size_total, tx_count),
        'block_interval_mean_sec_10m': safe_div(sum(intervals), len(intervals)) if len(intervals) >= 1 else None,
        'block_interval_median_sec_10m': median(intervals) if len(intervals) >= 1 else None,
        'avg_fee_sat_10m': safe_div(total_fee_sat, non_coinbase_tx_count),
        'median_fee_sat_10m': None,
        'avg_feerate_sat_vb_10m': safe_div(total_fee_sat, tx_vsize_total),
        'min_feerate_sat_vb_10m': min(feerate_min_values) if feerate_min_values else None,
        'max_feerate_sat_vb_10m': max(feerate_max_values) if feerate_max_values else None,
        'utxo_increase_10m': output_count - input_count,
        'utxo_count_total': None,
        'utxo_size_inc_bytes_10m': None,
        'utxo_set_size_total_bytes': None,
        'spent_output_count_10m': input_count,
        'created_output_count_10m': output_count,
        'segwit_tx_count_10m': segwit_tx_count,
        'segwit_share_10m': safe_div(segwit_tx_count, non_coinbase_tx_count),
        'sw_total_size_bytes_10m': sw_total_size_bytes,
        'sw_total_weight_wu_10m': sw_total_weight_wu,
        'difficulty_last': state_row['difficulty'] if state_row else None,
        'chainwork_last': state_row['chainwork'] if state_row else None,
        # TODO: populate these from a dedicated network-state snapshot source when available.
        'hashrate_est_last': None,
        'hashrate_est_mean_10m': None,
        'best_block_height_last': best_block_height_last,
        'updated_at': datetime.now(timezone.utc),
    }


def _upsert_10m_bucket(cur, payload: dict[str, Any]) -> None:
    columns = ', '.join(UPSERT_COLUMNS)
    placeholders = ', '.join(f'%({column})s' for column in UPSERT_COLUMNS)
    updates = ', '.join(f'{column} = EXCLUDED.{column}' for column in UPSERT_UPDATE_COLUMNS)
    cur.execute(
        f'''
        INSERT INTO btc_primitive_10m({columns})
        VALUES ({placeholders})
        ON CONFLICT (bucket_start_utc) DO UPDATE SET
          {updates}
        ''',
        payload,
    )


def rebuild_10m_bucket_for_bucket(
    db,
    bucket_start_utc: datetime,
    settings: Settings | None = None,
    emit_queue_event: bool = True,
) -> datetime | None:
    bucket_start_utc = to_bucket_start_10m(bucket_start_utc)
    try:
        with db.cursor() as cur:
            payload = build_10m_bucket_payload(cur, bucket_start_utc, settings=settings)
            if payload is None:
                return None
            _upsert_10m_bucket(cur, payload)
            upsert_checkpoint(
                cur,
                'BTC',
                PRIMITIVE_COMPONENT,
                PRIMITIVE_INTERVAL,
                last_height=payload['best_block_height_last'],
                last_bucket_time=bucket_start_utc,
                status='ok',
            )
    except Exception as exc:
        logger.exception('primitive_bucket_rebuild_failed bucket=%s error=%s', bucket_start_utc.isoformat(), exc)
        with db.cursor() as cur:
            upsert_checkpoint(
                cur,
                'BTC',
                PRIMITIVE_COMPONENT,
                PRIMITIVE_INTERVAL,
                last_bucket_time=bucket_start_utc,
                status='error',
                error_message=str(exc),
            )
        raise

    if emit_queue_event:
        enqueue_primitive_ready(bucket_start_utc, PRIMITIVE_INTERVAL)
    logger.info(
        'primitive_bucket_rebuilt bucket=%s last_height=%s',
        bucket_start_utc.isoformat(),
        payload['best_block_height_last'],
    )
    return bucket_start_utc


def _resolve_replay_start(cur, target_bucket: datetime) -> datetime | None:
    checkpoint = get_checkpoint(cur, 'BTC', PRIMITIVE_COMPONENT, PRIMITIVE_INTERVAL)
    if checkpoint.get('last_bucket_time'):
        return checkpoint['last_bucket_time']
    earliest_bucket = _fetch_earliest_raw_bucket(cur)
    if earliest_bucket is None:
        return None
    return min(earliest_bucket, target_bucket)


def catch_up_10m_to_bucket(
    db,
    target_bucket: datetime,
    settings: Settings | None = None,
    emit_queue_event: bool = True,
) -> int:
    target_bucket = to_bucket_start_10m(target_bucket)
    with db.cursor() as cur:
        start_bucket = _resolve_replay_start(cur, target_bucket)
    if start_bucket is None or start_bucket > target_bucket:
        return 0

    rebuilt = 0
    for bucket_start_utc in iter_bucket_range(start_bucket, target_bucket, PRIMITIVE_INTERVAL_MINUTES):
        rebuilt_bucket = rebuild_10m_bucket_for_bucket(
            db,
            bucket_start_utc,
            settings=settings,
            emit_queue_event=emit_queue_event,
        )
        if rebuilt_bucket is not None:
            rebuilt += 1
    if rebuilt:
        logger.info(
            'primitive_catch_up_complete start_bucket=%s end_bucket=%s rebuilt=%s',
            start_bucket,
            target_bucket,
            rebuilt,
        )
    return rebuilt


def rebuild_10m_buckets_for_height(
    db,
    height: int,
    settings: Settings | None = None,
    emit_queue_event: bool = True,
) -> int:
    with db.cursor() as cur:
        target_bucket = _fetch_bucket_for_height(cur, height)
    if target_bucket is None:
        logger.warning('primitive_bucket_missing_height height=%s', height)
        return 0
    return catch_up_10m_to_bucket(db, target_bucket, settings=settings, emit_queue_event=emit_queue_event)


def catch_up_10m_from_checkpoint(
    db,
    settings: Settings | None = None,
    emit_queue_event: bool = True,
    end_time: datetime | None = None,
) -> int:
    with db.cursor() as cur:
        raw_checkpoint = get_checkpoint(cur, 'BTC', RAW_COMPONENT, 'block')
        raw_last_height = int(raw_checkpoint.get('last_height') or 0)
        if raw_last_height <= 0:
            return 0
        raw_last_bucket = _fetch_bucket_for_height(cur, raw_last_height)
    if raw_last_bucket is None:
        return 0

    target_bucket = raw_last_bucket
    if end_time is not None:
        target_bucket = max(raw_last_bucket, to_bucket_start_10m(end_time))
    return catch_up_10m_to_bucket(db, target_bucket, settings=settings, emit_queue_event=emit_queue_event)
