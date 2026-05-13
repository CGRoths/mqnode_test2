from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from mqnode.market.price.lifecycle.manager import PriceSourceLifecycleManager
from mqnode.market.price.lifecycle.models import (
    LIFECYCLE_DISABLED,
    LIVE_HEALTHY,
    REST_BACKFILLING,
    WAITING_REMOTE_PUSH,
    WS_STARTING,
)
from mqnode.market.price.lifecycle.profiles import get_price_source_lifecycle_profile


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DB:
    def cursor(self):
        return _Cursor()


def _manager() -> PriceSourceLifecycleManager:
    return PriceSourceLifecycleManager(SimpleNamespace(price_source_rest_sync_allowed_lag_minutes=20))


@pytest.fixture
def now():
    return datetime(2026, 4, 20, 12, 30, tzinfo=timezone.utc)


def _patch_checkpoint(monkeypatch, bucket):
    monkeypatch.setattr(
        'mqnode.market.price.lifecycle.manager.get_checkpoint',
        lambda cur, chain, component, interval: {'last_bucket_time': bucket},
    )


def test_checkpoint_behind_latest_closed_bucket_stays_rest_backfilling(monkeypatch, now):
    _patch_checkpoint(monkeypatch, now - timedelta(hours=1))
    profile = get_price_source_lifecycle_profile('bybit')

    decision = _manager().evaluate_source(_DB(), profile, now=now)

    assert decision.current_state == REST_BACKFILLING
    assert decision.should_start_ws is False
    assert decision.should_keep_rest_live is True


@pytest.mark.parametrize('source_name', ['bybit', 'okx'])
def test_synced_confirmed_candle_sources_start_ws(monkeypatch, now, source_name):
    _patch_checkpoint(monkeypatch, now - timedelta(minutes=10))
    profile = get_price_source_lifecycle_profile(source_name)

    decision = _manager().evaluate_source(_DB(), profile, now=now)

    assert decision.current_state == WS_STARTING
    assert decision.should_start_ws is True
    assert decision.should_keep_rest_live is True
    assert decision.recommended_action == 'start_ws_after_rest_sync'


@pytest.mark.parametrize('source_name', ['coinbase', 'kraken', 'bitstamp'])
def test_synced_candidate_sources_remain_rest_official(monkeypatch, now, source_name):
    _patch_checkpoint(monkeypatch, now - timedelta(minutes=10))
    profile = get_price_source_lifecycle_profile(source_name)

    decision = _manager().evaluate_source(_DB(), profile, now=now)

    assert decision.current_state == LIVE_HEALTHY
    assert decision.should_start_ws is False
    assert decision.recommended_action == 'rest_official_candidate_ws_optional'


def test_binance_waits_for_remote_push(monkeypatch, now):
    _patch_checkpoint(monkeypatch, now - timedelta(minutes=10))
    profile = get_price_source_lifecycle_profile('binance')

    decision = _manager().evaluate_source(_DB(), profile, now=now)

    assert decision.current_state == WAITING_REMOTE_PUSH
    assert decision.should_start_ws is False
    assert decision.expects_remote_push is True


def test_disabled_profile_stays_disabled(monkeypatch, now):
    _patch_checkpoint(monkeypatch, now - timedelta(minutes=10))
    profile = replace(get_price_source_lifecycle_profile('bybit'), enabled=False)

    decision = _manager().evaluate_source(_DB(), profile, now=now)

    assert decision.current_state == LIFECYCLE_DISABLED
    assert decision.should_start_ws is False
    assert decision.should_keep_rest_live is False
