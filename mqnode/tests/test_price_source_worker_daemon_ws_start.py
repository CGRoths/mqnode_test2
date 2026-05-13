from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from mqnode.market.price.lifecycle.models import LifecycleDecision
from mqnode.market.price.source_worker import SourcePriceWorker


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DB:
    def cursor(self):
        return _Cursor()


class _LifecycleManager:
    def evaluate_source(self, db, profile, now=None):
        return LifecycleDecision(
            source_name='bybit',
            symbol='BTCUSDT',
            current_state='ws_starting',
            recommended_action='start_ws_after_rest_sync',
            reason='REST synced',
            should_start_ws=True,
            should_keep_rest_live=True,
            should_disable_ws=False,
            expects_remote_push=False,
        )


def test_daemon_starts_ws_immediately_when_rest_is_already_synced():
    ws_calls = []
    rest_calls = []
    settings = SimpleNamespace(
        price_source_worker_poll_seconds=0,
        price_source_worker_confirmation_poll_seconds=0,
        price_request_timeout_seconds=5,
        price_ws_rest_confirm_delay_seconds=0,
        price_ws_child_interval='1m',
        price_ws_output_mode='local_db',
        price_ws_remote_ingest_url=None,
        price_ws_price_tolerance_bps=1,
        price_ws_volume_tolerance_bps=10,
        price_ws_bucket_grace_seconds=5,
    )

    worker = SourcePriceWorker(
        'bybit',
        db=_DB(),
        settings=settings,
        lifecycle_manager=_LifecycleManager(),
        rest_catch_up=lambda source_name, db, settings: rest_calls.append(source_name),
        ws_runner=lambda **kwargs: ws_calls.append(kwargs),
        compose_fn=None,
        now_fn=lambda: datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc),
    )

    results = worker.run_forever(poll_seconds=0, confirmation_poll_seconds=0, max_cycles=1)

    assert [result.action for result in results] == ['start_ws_live']
    assert rest_calls == []
    assert ws_calls[0]['symbol'] == 'BTCUSDT'
