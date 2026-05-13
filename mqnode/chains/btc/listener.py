from __future__ import annotations

import logging
import time
from typing import Any

from mqnode.chains.btc.ingest import ingest_block
from mqnode.chains.btc.primitive_builder import PRIMITIVE_COMPONENT, PRIMITIVE_INTERVAL, PRIMITIVE_SCHEDULER_COMPONENT
from mqnode.chains.btc.reorg import reconcile_reorg
from mqnode.chains.btc.rpc import BitcoinRPC
from mqnode.config.logging_config import configure_logging
from mqnode.config.settings import Settings, get_settings
from mqnode.core.utils import to_bucket_start_10m, utc_now
from mqnode.db.connection import DB
from mqnode.db.repositories import get_checkpoint, upsert_checkpoint
from mqnode.queue.producer import enqueue_primitive_tick, enqueue_raw_block_ready

logger = logging.getLogger(__name__)

RAW_COMPONENT = 'btc_raw_block_ingestion'


def _load_ingestion_state(db) -> dict[str, Any]:
    with db.cursor() as cur:
        checkpoint = get_checkpoint(cur, 'BTC', RAW_COMPONENT, 'block')
        last_height = int(checkpoint.get('last_height') or 0)
        cur.execute(
            '''
            SELECT COALESCE(MAX(cumulative_supply_sat), 0) AS supply_total_sat
            FROM btc_primitive_block
            WHERE height <= %s
            ''',
            (last_height,),
        )
        row = cur.fetchone() or {}
    return {
        'last_height': last_height,
        'last_supply_sat': int(row.get('supply_total_sat') or 0),
        'status': checkpoint.get('status') or 'idle',
    }


def _ingest_height(db, rpc: BitcoinRPC, height: int, last_supply_sat: int) -> int:
    with db.cursor() as cur:
        next_supply_sat = ingest_block(cur, rpc, height, last_supply_sat)
        upsert_checkpoint(cur, 'BTC', RAW_COMPONENT, 'block', last_height=height, status='ok')
    return next_supply_sat


def _mark_listener_error(db, last_successful_height: int, error_message: str) -> None:
    with db.cursor() as cur:
        upsert_checkpoint(
            cur,
            'BTC',
            RAW_COMPONENT,
            'block',
            last_height=last_successful_height,
            status='error',
            error_message=error_message,
        )


def _maybe_schedule_primitive_tick(db, last_height: int) -> bool:
    if last_height <= 0:
        return False
    current_bucket = to_bucket_start_10m(utc_now())
    with db.cursor() as cur:
        checkpoint = get_checkpoint(cur, 'BTC', PRIMITIVE_SCHEDULER_COMPONENT, PRIMITIVE_INTERVAL)
        last_scheduled_bucket = checkpoint.get('last_bucket_time')
        primitive_checkpoint = get_checkpoint(cur, 'BTC', PRIMITIVE_COMPONENT, PRIMITIVE_INTERVAL)
        primitive_last_bucket = primitive_checkpoint.get('last_bucket_time')
    if last_scheduled_bucket is not None and current_bucket <= last_scheduled_bucket:
        return False
    if primitive_last_bucket is not None and current_bucket <= primitive_last_bucket:
        return False

    enqueue_primitive_tick(current_bucket)
    with db.cursor() as cur:
        upsert_checkpoint(
            cur,
            'BTC',
            PRIMITIVE_SCHEDULER_COMPONENT,
            PRIMITIVE_INTERVAL,
            last_height=last_height,
            last_bucket_time=current_bucket,
            status='ok',
        )
    logger.info('listener_primitive_tick_scheduled bucket=%s last_height=%s', current_bucket.isoformat(), last_height)
    return True


def sync_blocks_once(db, rpc: BitcoinRPC) -> dict[str, Any]:
    state = _load_ingestion_state(db)
    reorg_result = reconcile_reorg(db, rpc, state['last_height'])
    if reorg_result is not None:
        logger.warning(
            'listener_reorg_reconciled previous_checkpoint_height=%s common_height=%s',
            reorg_result['previous_checkpoint_height'],
            reorg_result['common_height'],
        )
        state = _load_ingestion_state(db)
    last_height = state['last_height']
    last_supply_sat = state['last_supply_sat']
    node_tip = rpc.get_block_count()

    if node_tip <= last_height:
        logger.info('listener_no_new_block last_height=%s node_tip=%s', last_height, node_tip)
        _maybe_schedule_primitive_tick(db, last_height)
        return {
            'status': 'idle',
            'last_height': last_height,
            'node_tip': node_tip,
            'processed_heights': [],
        }

    processed_heights: list[int] = []
    last_successful_height = last_height
    current_supply_sat = last_supply_sat

    for height in range(last_height + 1, node_tip + 1):
        try:
            current_supply_sat = _ingest_height(db, rpc, height, current_supply_sat)
            enqueue_raw_block_ready(height)
            processed_heights.append(height)
            last_successful_height = height
            logger.info('listener_ingested_block height=%s node_tip=%s', height, node_tip)
        except Exception as exc:
            logger.exception(
                'listener_ingest_failed height=%s last_successful_height=%s node_tip=%s error=%s',
                height,
                last_successful_height,
                node_tip,
                exc,
            )
            _mark_listener_error(db, last_successful_height, str(exc))
            return {
                'status': 'error',
                'last_height': last_successful_height,
                'failed_height': height,
                'node_tip': node_tip,
                'processed_heights': processed_heights,
            }

    return {
        'status': 'ok',
        'last_height': last_successful_height,
        'node_tip': node_tip,
        'processed_heights': processed_heights,
    }


def run_listener(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    db = DB(settings)
    rpc = BitcoinRPC(settings)

    while True:
        try:
            result = sync_blocks_once(db, rpc)
            sleep_seconds = settings.btc_listener_sleep_seconds
            if result['status'] == 'error':
                sleep_seconds = settings.btc_listener_error_sleep_seconds
            time.sleep(sleep_seconds)
        except Exception as exc:
            logger.exception('listener_loop_error error=%s', exc)
            time.sleep(settings.btc_listener_error_sleep_seconds)


if __name__ == '__main__':
    run_listener()
