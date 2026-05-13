from __future__ import annotations

from datetime import datetime

from mqnode.workers.worker_base import WorkerBase


class BTCMinerWorker(WorkerBase):
    factor = 'MINER'


def process_miner_job(payload: dict):
    if payload.get('event') != 'primitive_ready':
        return None
    bucket = datetime.fromisoformat(payload['bucket_start_utc'])
    return BTCMinerWorker().execute_metrics(bucket)


def replay_miner_startup() -> int:
    return BTCMinerWorker().replay_from_checkpoint()
