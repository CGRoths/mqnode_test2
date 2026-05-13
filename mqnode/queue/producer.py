from __future__ import annotations

from datetime import datetime

from rq import Queue

from mqnode.config.settings import get_settings
from mqnode.queue.jobs import BTC_MARKET_QUEUE, BTC_MINER_QUEUE, BTC_NETWORK_QUEUE, BTC_PRIMITIVE_QUEUE
from mqnode.queue.redis_conn import get_redis


def _queue(name: str) -> Queue:
    return Queue(name, connection=get_redis(get_settings()))


def enqueue_raw_block_ready(height: int) -> None:
    _queue(BTC_PRIMITIVE_QUEUE).enqueue(
        'mqnode.workers.btc_primitive_worker.process_raw_block_job',
        {
            'chain': 'BTC',
            'event': 'raw_block_ready',
            'height': height,
        },
    )


def enqueue_primitive_tick(as_of_time: datetime) -> None:
    _queue(BTC_PRIMITIVE_QUEUE).enqueue(
        'mqnode.workers.btc_primitive_worker.process_raw_block_job',
        {
            'chain': 'BTC',
            'event': 'primitive_tick',
            'as_of_time': as_of_time.isoformat(),
        },
    )


def enqueue_primitive_ready(bucket_start_utc: datetime, interval: str) -> None:
    payload = {
        'chain': 'BTC',
        'event': 'primitive_ready',
        'interval': interval,
        'bucket_start_utc': bucket_start_utc.isoformat(),
    }
    _queue(BTC_NETWORK_QUEUE).enqueue('mqnode.workers.btc_network_worker.process_network_job', payload)
    _queue(BTC_MINER_QUEUE).enqueue('mqnode.workers.btc_miner_worker.process_miner_job', payload)
    _queue(BTC_MARKET_QUEUE).enqueue('mqnode.workers.btc_market_worker.process_market_job', payload)


def enqueue_metric_job(metric_name: str, interval: str, bucket_start_utc: datetime) -> None:
    factor = metric_name.split('_')[0].lower()
    mapping = {'network': BTC_NETWORK_QUEUE, 'miner': BTC_MINER_QUEUE, 'market': BTC_MARKET_QUEUE}
    queue_name = mapping.get(factor, BTC_NETWORK_QUEUE)
    payload = {
        'chain': 'BTC',
        'event': 'metric_run',
        'metric_name': metric_name,
        'interval': interval,
        'bucket_start_utc': bucket_start_utc.isoformat(),
    }
    _queue(queue_name).enqueue(f'mqnode.workers.btc_{factor}_worker.process_{factor}_job', payload)
