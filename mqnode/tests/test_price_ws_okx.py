from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mqnode.market.price.ws import gap_filler
from mqnode.market.price.ws.gap_filler import OkxRestBucketFetcher
from mqnode.market.price.ws.sources.okx import OkxWsSource


def _okx_candle(start: datetime, minute: int, *, confirm: str = '1') -> list[str]:
    return [
        str(int((start + timedelta(minutes=minute)).timestamp() * 1000)),
        str(100 + minute),
        str(110 + minute),
        str(90 - minute),
        str(101 + minute),
        str(minute + 1),
        str((minute + 1) * 100),
        str((minute + 1) * (101 + minute)),
        confirm,
    ]


def test_okx_websocket_url_and_subscription_payload_are_business_candles():
    source = OkxWsSource()

    assert source.websocket_url == 'wss://ws.okx.com:8443/ws/v5/business'
    assert source.subscription_payload(symbol='BTC-USDT', interval='1m') == {
        'op': 'subscribe',
        'args': [{'channel': 'candle1m', 'instId': 'BTC-USDT'}],
    }
    assert source.subscription_payload(symbol='BTCUSDT', interval='1m') == {
        'op': 'subscribe',
        'args': [{'channel': 'candle1m', 'instId': 'BTC-USDT'}],
    }


def test_okx_parser_ignores_partial_confirm_zero():
    bucket = datetime(2026, 4, 20, 22, 35, tzinfo=timezone.utc)
    received_at = bucket + timedelta(minutes=1, seconds=3)
    payload = {
        'arg': {'channel': 'candle1m', 'instId': 'BTC-USDT'},
        'data': [_okx_candle(bucket, 0, confirm='0')],
    }

    assert OkxWsSource().parse_message(payload, received_at=received_at) == []


def test_okx_parser_emits_confirmed_candle_with_ohlcv_and_close_latency():
    bucket = datetime(2026, 4, 20, 22, 35, tzinfo=timezone.utc)
    received_at = bucket + timedelta(minutes=1, seconds=3, milliseconds=97)
    payload = {
        'arg': {'channel': 'candle1m', 'instId': 'BTC-USDT'},
        'data': [_okx_candle(bucket, 0, confirm='1')],
    }

    events = OkxWsSource().parse_message(payload, received_at=received_at)

    assert len(events) == 1
    event = events[0]
    assert event.source_name == 'okx'
    assert event.symbol == 'BTC-USDT'
    assert event.bucket_start_utc == bucket
    assert event.source_updated_at == bucket
    assert event.exchange_close_time == bucket + timedelta(minutes=1)
    assert event.open_price_usd == pytest.approx(100)
    assert event.high_price_usd == pytest.approx(110)
    assert event.low_price_usd == pytest.approx(90)
    assert event.close_price_usd == pytest.approx(101)
    assert event.volume_btc == pytest.approx(1)
    assert event.volume_usd == pytest.approx(101)
    assert event.raw_payload['timestamp_semantics'] == 'okx_ts_is_bucket_start'
    assert event.raw_payload['latency_reference'] == 'bucket_start_plus_interval'
    assert event.raw_payload['close_latency_ms'] == 3097


def test_okx_parser_prefers_exchange_inst_id_over_runtime_symbol_override():
    bucket = datetime(2026, 4, 20, 22, 35, tzinfo=timezone.utc)
    received_at = bucket + timedelta(minutes=1, seconds=3)
    payload = {
        'arg': {'channel': 'candle1m', 'instId': 'BTC-USDT'},
        'data': [_okx_candle(bucket, 0, confirm='1')],
    }

    events = OkxWsSource().parse_message(payload, received_at=received_at, symbol='BTCUSDT')

    assert len(events) == 1
    assert events[0].symbol == 'BTC-USDT'


def test_okx_rest_fallback_aggregates_exact_ten_confirmed_1m_children(monkeypatch):
    bucket = datetime(2026, 4, 20, 22, 30, tzinfo=timezone.utc)
    fetched_at = datetime(2026, 4, 20, 22, 40, 5, tzinfo=timezone.utc)
    captured = {}

    def fake_request_json(url, *, params=None, timeout=30, max_attempts=1, **kwargs):
        captured['url'] = url
        captured['params'] = params
        return {'code': '0', 'data': [_okx_candle(bucket, minute) for minute in reversed(range(10))]}

    monkeypatch.setattr(gap_filler, 'request_json', fake_request_json)
    monkeypatch.setattr(gap_filler, 'utc_now', lambda: fetched_at)

    row = OkxRestBucketFetcher(timeout_seconds=7).fetch_bucket('okx', 'BTC-USDT', bucket)

    assert captured['url'] == 'https://www.okx.com/api/v5/market/candles'
    assert captured['params']['bar'] == '1m'
    assert row is not None
    assert row['bucket_start_utc'] == bucket
    assert row['symbol'] == 'BTC-USDT'
    assert row['open_price_usd'] == pytest.approx(100)
    assert row['high_price_usd'] == pytest.approx(119)
    assert row['low_price_usd'] == pytest.approx(81)
    assert row['close_price_usd'] == pytest.approx(110)
    assert row['volume_btc'] == pytest.approx(55)
    assert row['volume_usd'] == pytest.approx(sum((minute + 1) * (101 + minute) for minute in range(10)))
    assert row['trade_count'] is None
    assert row['source_updated_at'] == fetched_at
    assert row['raw_payload']['rest_fallback'] == 'okx_candles_1m_to_10m'


def test_okx_rest_fallback_returns_none_when_child_rows_missing(monkeypatch):
    bucket = datetime(2026, 4, 20, 22, 30, tzinfo=timezone.utc)

    monkeypatch.setattr(
        gap_filler,
        'request_json',
        lambda *args, **kwargs: {'code': '0', 'data': [_okx_candle(bucket, minute) for minute in range(9)]},
    )

    assert OkxRestBucketFetcher().fetch_bucket('okx', 'BTC-USDT', bucket) is None
