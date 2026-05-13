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
    def __init__(self, decisions):
        self.decisions = list(decisions)

    def evaluate_source(self, db, profile, now=None):
        return self.decisions.pop(0)


def _settings():
    return SimpleNamespace(
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


def _decision(action, *, start_ws=False, keep_rest=True):
    return LifecycleDecision(
        source_name='bybit',
        symbol='BTCUSDT',
        current_state=action,
        recommended_action=action,
        reason='test',
        should_start_ws=start_ws,
        should_keep_rest_live=keep_rest,
        should_disable_ws=not start_ws,
        expects_remote_push=False,
    )


def test_daemon_repeats_rest_catch_up_until_synced_then_starts_ws():
    rest_calls = []
    ws_calls = []

    def fake_rest_catch_up(source_name, db, settings):
        rest_calls.append(source_name)
        return 3

    def fake_ws_runner(**kwargs):
        ws_calls.append(kwargs)

    worker = SourcePriceWorker(
        'bybit',
        db=_DB(),
        settings=_settings(),
        lifecycle_manager=_LifecycleManager(
            [
                _decision('continue_rest_backfill', start_ws=False),
                _decision('continue_rest_backfill', start_ws=False),
                _decision('start_ws_after_rest_sync', start_ws=True),
            ]
        ),
        rest_catch_up=fake_rest_catch_up,
        ws_runner=fake_ws_runner,
        compose_fn=None,
        now_fn=lambda: datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc),
    )

    results = worker.run_forever(poll_seconds=0, confirmation_poll_seconds=0, max_cycles=3)

    assert [result.action for result in results] == ['rest_catch_up', 'rest_catch_up', 'start_ws_live']
    assert rest_calls == ['bybit', 'bybit']
    assert len(ws_calls) == 1
    assert ws_calls[0]['source_name'] == 'bybit'
