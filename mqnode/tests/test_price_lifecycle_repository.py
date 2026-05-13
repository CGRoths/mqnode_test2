from __future__ import annotations

from datetime import datetime, timezone

from mqnode.market.price.lifecycle.models import ERROR, REST_SYNCED, WS_LIVE
from mqnode.market.price.lifecycle.profiles import get_price_source_lifecycle_profile
from mqnode.market.price.lifecycle.repository import PriceSourceLifecycleRepository


class _Cursor:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.calls.append((query, params))


class _DB:
    def __init__(self):
        self.cursor_instance = _Cursor()

    def cursor(self):
        return self.cursor_instance


def _last_params(db):
    return db.cursor_instance.calls[-1][1]


def test_upsert_status_inserts_profile_state():
    db = _DB()
    profile = get_price_source_lifecycle_profile('bybit')

    PriceSourceLifecycleRepository(db).upsert_status(profile, 'rest_backfilling')

    query, params = db.cursor_instance.calls[0]
    assert 'INSERT INTO mq_price_source_lifecycle_status' in query
    assert params['source_name'] == 'bybit'
    assert params['symbol'] == 'BTCUSDT'
    assert params['current_state'] == 'rest_backfilling'
    assert params['live_mode'] == 'ws_confirmed_candle'


def test_mark_rest_synced_updates_state_and_timestamp():
    db = _DB()
    bucket = datetime(2026, 4, 20, 0, 20, tzinfo=timezone.utc)
    profile = get_price_source_lifecycle_profile('okx')

    PriceSourceLifecycleRepository(db).mark_rest_synced(profile, last_rest_bucket=bucket)

    params = _last_params(db)
    assert params['current_state'] == REST_SYNCED
    assert params['rest_sync_status'] == 'synced'
    assert params['last_rest_bucket'] == bucket
    assert params['rest_synced_at'] is not None


def test_mark_ws_live_updates_state_and_ws_started_at():
    db = _DB()
    bucket = datetime(2026, 4, 20, 0, 20, tzinfo=timezone.utc)
    profile = get_price_source_lifecycle_profile('bybit')

    PriceSourceLifecycleRepository(db).mark_ws_live(profile, last_ws_bucket=bucket)

    params = _last_params(db)
    assert params['current_state'] == WS_LIVE
    assert params['ws_status'] == 'live'
    assert params['last_ws_bucket'] == bucket
    assert params['ws_started_at'] is not None


def test_mark_error_stores_last_error():
    db = _DB()
    profile = get_price_source_lifecycle_profile('bybit')

    PriceSourceLifecycleRepository(db).mark_error(profile, 'boom')

    params = _last_params(db)
    assert params['current_state'] == ERROR
    assert params['last_error'] == 'boom'
    assert '"error": "boom"' in params['metadata_json']
