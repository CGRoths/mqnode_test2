from __future__ import annotations

import logging
from datetime import datetime

from mqnode.checkpoints.checkpoint_service import checkpoint_error, checkpoint_ok
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.db.repositories import get_checkpoint
from mqnode.registry.dependency_validator import validate_metric_dependencies
from mqnode.registry.dynamic_loader import load_function
from mqnode.registry.metric_registry import get_enabled_metrics

logger = logging.getLogger(__name__)

PRIMITIVE_COMPONENT = 'btc_primitive_10m_builder'


class WorkerBase:
    chain = 'BTC'
    factor = 'NETWORK'

    def __init__(self):
        self.settings = get_settings()
        self.db = DB(self.settings)

    def _metric_component(self, metric_name: str, interval: str) -> str:
        return f'btc_metric_{metric_name}_{interval}'

    def execute_metrics(self, source_bucket_start_utc: datetime) -> int:
        metrics = get_enabled_metrics(self.db, self.chain, self.factor)
        if not metrics:
            logger.info('worker_no_enabled_metrics chain=%s factor=%s', self.chain, self.factor)
            return 0

        executed = 0
        for metric in metrics:
            metric_interval = metric['interval']
            component = self._metric_component(metric['metric_name'], metric_interval)
            try:
                validate_metric_dependencies(self.db, metric, source_bucket_start_utc)
                fn = load_function(metric['module_path'], metric['function_name'])
                fn(self.db, source_bucket_start_utc, metric_interval)
                with self.db.cursor() as cur:
                    checkpoint_ok(cur, self.chain, component, metric_interval, last_bucket_time=source_bucket_start_utc)
                executed += 1
                logger.info(
                    'worker_metric_ok factor=%s metric=%s metric_interval=%s source_bucket=%s',
                    self.factor,
                    metric['metric_name'],
                    metric_interval,
                    source_bucket_start_utc.isoformat(),
                )
            except Exception as exc:
                logger.exception(
                    'worker_metric_failed factor=%s metric=%s metric_interval=%s source_bucket=%s error=%s',
                    self.factor,
                    metric['metric_name'],
                    metric_interval,
                    source_bucket_start_utc.isoformat(),
                    exc,
                )
                with self.db.cursor() as cur:
                    checkpoint_error(
                        cur,
                        self.chain,
                        component,
                        metric_interval,
                        str(exc),
                        last_bucket_time=source_bucket_start_utc,
                    )
        return executed

    def replay_from_checkpoint(self) -> int:
        metrics = get_enabled_metrics(self.db, self.chain, self.factor)
        if not metrics:
            return 0

        with self.db.cursor() as cur:
            primitive_checkpoint = get_checkpoint(cur, self.chain, PRIMITIVE_COMPONENT, '10m')
            primitive_last_bucket = primitive_checkpoint.get('last_bucket_time')
            if primitive_last_bucket is None:
                return 0

            # TODO: add an explicit historical backfill mode for newly enabled metrics.
            replay_start = primitive_last_bucket
            for metric in metrics:
                metric_checkpoint = get_checkpoint(
                    cur,
                    self.chain,
                    self._metric_component(metric['metric_name'], metric['interval']),
                    metric['interval'],
                )
                if metric_checkpoint.get('last_bucket_time'):
                    replay_start = min(replay_start, metric_checkpoint['last_bucket_time'])

            cur.execute(
                '''
                SELECT bucket_start_utc
                FROM btc_primitive_10m
                WHERE bucket_start_utc >= %s
                  AND bucket_start_utc <= %s
                ORDER BY bucket_start_utc ASC
                ''',
                (replay_start, primitive_last_bucket),
            )
            buckets = [row['bucket_start_utc'] for row in cur.fetchall()]

        replayed = 0
        for bucket_start_utc in buckets:
            self.execute_metrics(bucket_start_utc)
            replayed += 1

        if replayed:
            logger.info(
                'worker_replay_complete factor=%s replayed_buckets=%s replay_start=%s replay_end=%s',
                self.factor,
                replayed,
                replay_start.isoformat(),
                primitive_last_bucket.isoformat(),
            )
        return replayed
