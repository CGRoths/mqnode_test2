from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter
from rq import Queue

from mqnode.chains.btc.listener import RAW_COMPONENT
from mqnode.chains.btc.primitive_builder import PRIMITIVE_COMPONENT, PRIMITIVE_INTERVAL
from mqnode.chains.btc.rpc import BitcoinRPC
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.market.price.checkpoints import PRICE_CANONICAL_COMPONENT
from mqnode.queue.jobs import BTC_MARKET_QUEUE, BTC_MINER_QUEUE, BTC_NETWORK_QUEUE, BTC_PRIMITIVE_QUEUE
from mqnode.queue.redis_conn import get_redis

router = APIRouter()


def _serialize_checkpoint(row: dict) -> dict:
    if not row:
        return {}
    return {
        'component': row['component'],
        'interval': row['interval'],
        'last_height': row.get('last_height'),
        'last_bucket_time': row.get('last_bucket_time'),
        'status': row.get('status'),
        'error_message': row.get('error_message'),
        'updated_at': row.get('updated_at'),
        'seconds_since_update': _seconds_since(row.get('updated_at')),
    }


def _seconds_since(ts: datetime | None) -> float | None:
    if ts is None:
        return None
    return max((datetime.now(timezone.utc) - ts).total_seconds(), 0.0)


def _get_queue_depths(settings) -> tuple[dict[str, int], str | None]:
    try:
        redis = get_redis(settings)
        return (
            {
                queue_name: Queue(queue_name, connection=redis).count
                for queue_name in (BTC_PRIMITIVE_QUEUE, BTC_NETWORK_QUEUE, BTC_MINER_QUEUE, BTC_MARKET_QUEUE)
            },
            None,
        )
    except Exception as exc:
        return ({}, str(exc))


@router.get('/health')
@router.get('/api/v1/btc/health')
def health():
    settings = get_settings()
    checkpoints: list[dict] = []
    queue_depths, redis_error = _get_queue_depths(settings)
    with DB(settings).cursor() as cur:
        cur.execute('SELECT * FROM sync_checkpoints WHERE chain = %s ORDER BY component, interval', ('BTC',))
        checkpoints = cur.fetchall()
        checkpoint_map = {(row['component'], row['interval']): row for row in checkpoints}

        cur.execute(
            '''
            SELECT MAX(bucket_start_utc) AS last_bucket_time, MAX(last_height) AS last_height
            FROM btc_primitive_10m
            '''
        )
        primitive_table_state = cur.fetchone() or {}

        cur.execute('SELECT MAX(bucket_start_utc) AS last_bucket_time FROM mq_btc_price_10m')
        price_table_state = cur.fetchone() or {}

    status = 'ok'
    node_tip = None
    rpc_error = None
    try:
        node_tip = BitcoinRPC(settings).get_block_count()
    except Exception as exc:
        status = 'degraded'
        rpc_error = str(exc)
    if redis_error:
        status = 'degraded'

    raw_checkpoint = checkpoint_map.get((RAW_COMPONENT, 'block'), {})
    primitive_checkpoint = checkpoint_map.get((PRIMITIVE_COMPONENT, PRIMITIVE_INTERVAL), {})
    price_checkpoint = checkpoint_map.get((PRICE_CANONICAL_COMPONENT, '10m'), {})
    raw_last_height = raw_checkpoint.get('last_height')
    primitive_last_height = primitive_checkpoint.get('last_height')
    worker_rows = [
        row
        for row in checkpoints
        if row.get('component', '').startswith('worker_') and row.get('interval') == 'heartbeat'
    ]

    workers = []
    for row in worker_rows:
        seconds_since_update = _seconds_since(row.get('updated_at'))
        stale = seconds_since_update is not None and seconds_since_update > settings.worker_stale_after_seconds
        if stale:
            status = 'degraded'
        workers.append(
            {
                'component': row['component'],
                'status': row.get('status'),
                'updated_at': row.get('updated_at'),
                'seconds_since_update': seconds_since_update,
                'stale': stale,
                'error_message': row.get('error_message'),
            }
        )

    checkpoint_errors = [
        _serialize_checkpoint(row)
        for row in checkpoints
        if row.get('status') == 'error'
    ]
    if checkpoint_errors:
        status = 'degraded'

    latest_primitive_bucket_time = (
        primitive_table_state.get('last_bucket_time')
        or primitive_checkpoint.get('last_bucket_time')
    )
    latest_primitive_height = primitive_table_state.get('last_height') or primitive_last_height
    lag_blocks = None
    primitive_lag_blocks = None
    if node_tip is not None and raw_last_height is not None:
        lag_blocks = max(int(node_tip) - int(raw_last_height), 0)
    if raw_last_height is not None and latest_primitive_height is not None:
        primitive_lag_blocks = max(int(raw_last_height) - int(latest_primitive_height), 0)

    return {
        'status': status,
        'service': 'mqnode-api',
        'btc': {
            'node_tip': node_tip,
            'rpc_error': rpc_error,
            'last_raw_ingested_height': raw_last_height,
            'last_primitive_built_height': primitive_last_height,
            'last_primitive_bucket_time': latest_primitive_bucket_time,
            'last_canonical_price_bucket_time': price_table_state.get('last_bucket_time'),
            'lag_blocks': lag_blocks,
            'primitive_lag_blocks': primitive_lag_blocks,
            'queue_depths': queue_depths,
            'redis_error': redis_error,
            'checkpoints': {
                'raw_ingestion': _serialize_checkpoint(raw_checkpoint),
                'primitive_builder': _serialize_checkpoint(primitive_checkpoint),
                'price_composer': _serialize_checkpoint(price_checkpoint),
            },
            'workers': workers,
            'checkpoint_errors': checkpoint_errors,
        },
    }
