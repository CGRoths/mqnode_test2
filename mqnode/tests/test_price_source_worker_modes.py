from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from mqnode.market.price.source_worker import SourcePriceWorker


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DB:
    def cursor(self):
        return _Cursor()


def _settings(**overrides):
    values = {
        'price_source_rest_sync_allowed_lag_minutes': 20,
        'price_request_timeout_seconds': 5,
        'price_ws_bucket_grace_seconds': 5,
        'price_ws_rest_confirm_delay_seconds': 30,
        'price_ws_child_interval': '1m',
        'price_ws_output_mode': 'local_db',
        'price_ws_remote_ingest_url': None,
        'price_ws_price_tolerance_bps': 1,
        'price_ws_volume_tolerance_bps': 10,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _patch_checkpoint(monkeypatch, bucket):
    monkeypatch.setattr(
        'mqnode.market.price.lifecycle.manager.get_checkpoint',
        lambda cur, chain, component, interval: {'last_bucket_time': bucket},
    )


def _worker(source_name, *, settings=None, ws_calls=None, rest_calls=None):
    ws_calls = ws_calls if ws_calls is not None else []
    rest_calls = rest_calls if rest_calls is not None else []

    def fake_ws_runner(**kwargs):
        ws_calls.append(kwargs)

    def fake_rest_catch_up(source_name, db, settings):
        rest_calls.append((source_name, db, settings))
        return 4

    return SourcePriceWorker(
        source_name,
        db=_DB(),
        settings=settings or _settings(),
        ws_runner=fake_ws_runner,
        rest_catch_up=fake_rest_catch_up,
        compose_fn=None,
    )


@pytest.fixture
def now():
    return datetime(2026, 4, 20, 12, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize('source_name', ['bybit', 'okx'])
def test_synced_confirmed_sources_start_ws(monkeypatch, now, source_name):
    _patch_checkpoint(monkeypatch, now - timedelta(minutes=10))
    ws_calls = []
    rest_calls = []

    result = _worker(source_name, ws_calls=ws_calls, rest_calls=rest_calls).run_once(now=now)

    assert result.action == 'start_ws_live'
    assert ws_calls[0]['source_name'] == source_name
    assert ws_calls[0]['interval'] == '1m'
    assert rest_calls == []


def test_binance_waits_for_remote_push_without_local_ws(monkeypatch, now):
    _patch_checkpoint(monkeypatch, now - timedelta(minutes=10))
    ws_calls = []
    rest_calls = []

    result = _worker('binance', ws_calls=ws_calls, rest_calls=rest_calls).run_once(now=now)

    assert result.action == 'wait_for_remote_push'
    assert result.decision.expects_remote_push is True
    assert ws_calls == []
    assert rest_calls == []


@pytest.mark.parametrize('source_name', ['coinbase', 'kraken', 'bitstamp'])
def test_candidate_sources_stay_rest_official(monkeypatch, now, source_name):
    _patch_checkpoint(monkeypatch, now - timedelta(minutes=10))
    ws_calls = []
    rest_calls = []

    result = _worker(source_name, ws_calls=ws_calls, rest_calls=rest_calls).run_once(now=now)

    assert result.action == 'rest_live_poll'
    assert result.rows_written == 4
    assert ws_calls == []
    assert rest_calls[0][0] == source_name


def test_rest_not_synced_runs_catch_up_before_ws(monkeypatch, now):
    _patch_checkpoint(monkeypatch, now - timedelta(hours=1))
    ws_calls = []
    rest_calls = []

    result = _worker('bybit', ws_calls=ws_calls, rest_calls=rest_calls).run_once(now=now)

    assert result.action == 'rest_catch_up'
    assert result.rows_written == 4
    assert ws_calls == []
    assert rest_calls[0][0] == 'bybit'
