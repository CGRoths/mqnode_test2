from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import requests

from mqnode.market.price.ws import remote_push, runtime
from mqnode.market.price.ws.candle_builder import TenMinuteCandleBuilder
from mqnode.market.price.ws.gap_filler import PriceWsGapFiller
from mqnode.market.price.ws.models import REST_REPAIRED, WS_CLOSED, ConfirmedCandleEvent
from mqnode.market.price.ws.remote_push import RemotePriceWsPusher, RemoteWsPushAuthError
from mqnode.market.price.ws.repository import PriceWsRepository


def _event(bucket: datetime, minute: int) -> ConfirmedCandleEvent:
    child_start = bucket + timedelta(minutes=minute)
    return ConfirmedCandleEvent(
        source_name='binance',
        symbol='BTCUSDT',
        bucket_start_utc=child_start,
        open_price_usd=100 + minute,
        high_price_usd=110 + minute,
        low_price_usd=90 - minute,
        close_price_usd=101 + minute,
        volume_btc=minute + 1,
        volume_usd=(minute + 1) * (101 + minute),
        trade_count=minute + 2,
        source_updated_at=child_start + timedelta(minutes=1),
        exchange_close_time=child_start + timedelta(seconds=59, milliseconds=999),
        received_at=child_start + timedelta(minutes=1, seconds=1),
        raw_payload={'minute': minute},
    )


class _Response:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self.payload = payload or {'ok': True}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f'status={self.status_code}', response=self)

    def json(self) -> dict:
        return self.payload


def test_remote_pusher_sends_bearer_auth_and_quality_payload(monkeypatch):
    calls = []

    def fake_post(url, *, json=None, headers=None, timeout=10):
        calls.append({'url': url, 'json': json, 'headers': headers, 'timeout': timeout})
        return _Response(200, {'ok': True, 'rows_upserted': 1})

    monkeypatch.setattr(remote_push.requests, 'post', fake_post)
    pusher = RemotePriceWsPusher(
        ingest_url='https://mqnode.example/api/v1/internal/price/source/binance/10m',
        token='secret',
        timeout_seconds=4,
    )

    result = pusher.push_rows(
        [
            {
                'source_name': 'binance',
                'symbol': 'BTCUSDT',
                'bucket_start_utc': datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc),
                'open_price_usd': 100,
                'high_price_usd': 110,
                'low_price_usd': 90,
                'close_price_usd': 105,
                'quality_status': WS_CLOSED,
                'data_source_mode': 'websocket_confirmed_1m_cloud',
            }
        ]
    )

    assert result == {'ok': True, 'rows_upserted': 1}
    assert calls[0]['url'] == 'https://mqnode.example/api/v1/internal/price/source/binance/10m'
    assert calls[0]['headers'] == {'Authorization': 'Bearer secret'}
    assert calls[0]['timeout'] == 4
    row = calls[0]['json']['rows'][0]
    assert row['bucket_start_utc'] == '2026-04-20T00:00:00+00:00'
    assert row['quality_status'] == WS_CLOSED
    assert row['data_source_mode'] == 'websocket_confirmed_1m_cloud'


def test_remote_pusher_403_raises_auth_error(monkeypatch):
    monkeypatch.setattr(remote_push.requests, 'post', lambda *args, **kwargs: _Response(403))
    pusher = RemotePriceWsPusher(ingest_url='https://mqnode.example/ingest', token='bad')

    with pytest.raises(RemoteWsPushAuthError, match='authentication failed'):
        pusher.push_rows([{'bucket_start_utc': '2026-04-20T00:00:00Z'}])


def test_remote_pusher_retries_transient_5xx(monkeypatch):
    calls = []
    responses = [_Response(500), _Response(200)]

    def fake_post(*args, **kwargs):
        calls.append(kwargs)
        return responses.pop(0)

    monkeypatch.setattr(remote_push.requests, 'post', fake_post)
    pusher = RemotePriceWsPusher(
        ingest_url='https://mqnode.example/ingest',
        token='secret',
        max_attempts=2,
        backoff_seconds=0,
    )

    pusher.push_rows([{'bucket_start_utc': '2026-04-20T00:00:00Z'}])

    assert len(calls) == 2


class _Pusher:
    def __init__(self) -> None:
        self.rows = []

    def push_rows(self, rows):
        self.rows.extend(rows)


class _Fetcher:
    def __init__(self, row):
        self.row = row

    def fetch_bucket(self, source_name, symbol, bucket_start_utc):
        return self.row


def test_clean_binance_bucket_finalizer_remote_pushes_ws_closed_row():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    for minute in range(10):
        builder.accept(_event(bucket, minute))
    pusher = _Pusher()

    runtime._finalize_ready_buckets(
        db=None,
        source_name='binance',
        table_name='binance_price_10m',
        symbol='BTCUSDT',
        builder=builder,
        gap_filler=PriceWsGapFiller(_Fetcher(None)),
        repository=PriceWsRepository(),
        now=bucket + timedelta(minutes=10, seconds=5),
        grace_seconds=5,
        finalized_buckets=set(),
        rest_fallback_enabled=True,
        remote_pusher=pusher,
    )

    assert len(pusher.rows) == 1
    row = pusher.rows[0]
    assert row['quality_status'] == WS_CLOSED
    assert row['data_source_mode'] == 'websocket_confirmed_1m_cloud'
    assert row['expected_child_candle_count'] == 10
    assert row['actual_child_candle_count'] == 10


def test_no_event_binance_bucket_remote_pushes_rest_repaired_row():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    repair_row = {
        'source_name': 'binance',
        'symbol': 'BTCUSDT',
        'bucket_start_utc': bucket,
        'open_price_usd': 100,
        'high_price_usd': 110,
        'low_price_usd': 90,
        'close_price_usd': 105,
        'volume_btc': 2,
        'volume_usd': 210,
        'trade_count': 5,
        'raw_payload': {'rest_fallback': 'binance_klines_10m'},
        'source_updated_at': bucket + timedelta(minutes=10),
    }
    pusher = _Pusher()

    runtime._finalize_ready_buckets(
        db=None,
        source_name='binance',
        table_name='binance_price_10m',
        symbol='BTCUSDT',
        builder=TenMinuteCandleBuilder(),
        gap_filler=PriceWsGapFiller(_Fetcher(repair_row)),
        repository=PriceWsRepository(),
        now=bucket + timedelta(minutes=10, seconds=5),
        grace_seconds=5,
        finalized_buckets=set(),
        rest_fallback_enabled=True,
        remote_pusher=pusher,
    )

    assert len(pusher.rows) == 1
    row = pusher.rows[0]
    assert row['quality_status'] == REST_REPAIRED
    assert row['data_source_mode'] == 'rest_live_fallback_cloud'
    assert row['is_repaired'] is True
