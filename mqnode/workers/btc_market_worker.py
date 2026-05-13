from __future__ import annotations

from datetime import datetime

from mqnode.workers.worker_base import WorkerBase


class BTCMarketWorker(WorkerBase):
    factor = 'MARKET'


def process_market_job(payload: dict):
    if payload.get('event') != 'primitive_ready':
        return None
    bucket = datetime.fromisoformat(payload['bucket_start_utc'])
    return BTCMarketWorker().execute_metrics(bucket)


def replay_market_startup() -> int:
    return BTCMarketWorker().replay_from_checkpoint()
