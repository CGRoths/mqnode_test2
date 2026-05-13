from __future__ import annotations

from datetime import datetime

from mqnode.workers.worker_base import WorkerBase


class BTCNetworkWorker(WorkerBase):
    factor = 'NETWORK'


def process_network_job(payload: dict):
    if payload.get('event') != 'primitive_ready':
        return None
    bucket = datetime.fromisoformat(payload['bucket_start_utc'])
    return BTCNetworkWorker().execute_metrics(bucket)


def replay_network_startup() -> int:
    return BTCNetworkWorker().replay_from_checkpoint()
