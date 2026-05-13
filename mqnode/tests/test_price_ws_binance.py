from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mqnode.market.price.ws import gap_filler
from mqnode.market.price.ws.gap_filler import BinanceRestBucketFetcher, get_rest_bucket_fetcher
from mqnode.market.price.ws.sources.binance import BinanceWsSource


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _kline_payload(*, closed: bool = True) -> dict:
    start = datetime(2026, 4, 20, 0, 1, tzinfo=timezone.utc)
    close = datetime(2026, 4, 20, 0, 1, 59, 999000, tzinfo=timezone.utc)
    event_time = datetime(2026, 4, 20, 0, 2, 0, 500000, tzinfo=timezone.utc)
    return {
        'e': 'kline',
        'E': _ms(event_time),
        's': 'BTCUSDT',
        'k': {
            't': _ms(start),
            'T': _ms(close),
            's': 'BTCUSDT',
            'i': '1m',
            'o': '100.5',
            'c': '101.5',
            'h': '102.5',
            'l': '99.5',
            'v': '3.25',
            'n': 42,
            'x': closed,
            'q': '330.125',
        },
    }


def test_binance_closed_kline_emits_confirmed_candle_event():
    received_at = datetime(2026, 4, 20, 0, 2, 1, tzinfo=timezone.utc)
    events = BinanceWsSource().parse_message(_kline_payload(), received_at=received_at)

    assert len(events) == 1
    event = events[0]
    assert event.source_name == 'binance'
    assert event.symbol == 'BTCUSDT'
    assert event.bucket_start_utc == datetime(2026, 4, 20, 0, 1, tzinfo=timezone.utc)
    assert event.exchange_close_time == datetime(2026, 4, 20, 0, 1, 59, 999000, tzinfo=timezone.utc)
    assert event.source_updated_at == datetime(2026, 4, 20, 0, 2, 0, 500000, tzinfo=timezone.utc)
    assert event.open_price_usd == pytest.approx(100.5)
    assert event.high_price_usd == pytest.approx(102.5)
    assert event.low_price_usd == pytest.approx(99.5)
    assert event.close_price_usd == pytest.approx(101.5)
    assert event.volume_btc == pytest.approx(3.25)
    assert event.volume_usd == pytest.approx(330.125)
    assert event.trade_count == 42
    assert event.raw_payload['k']['x'] is True


def test_binance_open_kline_is_ignored():
    events = BinanceWsSource().parse_message(
        _kline_payload(closed=False),
        received_at=datetime(2026, 4, 20, 0, 2, tzinfo=timezone.utc),
    )

    assert events == []


def test_binance_parser_accepts_combined_stream_wrapper():
    payload = {
        'stream': 'btcusdt@kline_1m',
        'data': _kline_payload(),
    }

    events = BinanceWsSource().parse_message(
        payload,
        received_at=datetime(2026, 4, 20, 0, 2, tzinfo=timezone.utc),
    )

    assert len(events) == 1
    assert events[0].bucket_start_utc == datetime(2026, 4, 20, 0, 1, tzinfo=timezone.utc)


def test_binance_uses_raw_stream_url_without_subscription_payload():
    source = BinanceWsSource()

    assert source.websocket_url == 'wss://stream.binance.com:9443/ws/btcusdt@kline_1m'
    assert source.subscription_payload(symbol='BTCUSDT', interval='1m') is None


def test_binance_rest_bucket_fetcher_normalizes_exact_10m_kline(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    close_time = bucket + timedelta(minutes=9, seconds=59, milliseconds=999)
    request = {}
    kline = [
        _ms(bucket),
        '100',
        '105',
        '99',
        '104',
        '2.5',
        _ms(close_time),
        '260',
        77,
        '1.0',
        '104',
        '0',
    ]

    def fake_request_json(url, *, params=None, timeout=30, max_attempts=3, **kwargs):
        request['url'] = url
        request['params'] = params
        request['timeout'] = timeout
        request['max_attempts'] = max_attempts
        return [kline]

    monkeypatch.setattr(gap_filler, 'request_json', fake_request_json)

    row = BinanceRestBucketFetcher(timeout_seconds=7).fetch_bucket('binance', 'BTCUSDT', bucket)

    assert row is not None
    assert request['url'] == 'https://api.binance.com/api/v3/klines'
    assert request['params'] == {
        'symbol': 'BTCUSDT',
        'interval': '10m',
        'startTime': _ms(bucket),
        'endTime': _ms(bucket + timedelta(minutes=10)),
        'limit': 1,
    }
    assert request['timeout'] == 7
    assert request['max_attempts'] == 1
    assert row['bucket_start_utc'] == bucket
    assert row['volume_usd'] == pytest.approx(260)
    assert row['trade_count'] == 77
    assert row['source_updated_at'] == close_time
    assert row['raw_payload'] == {'rest_fallback': 'binance_klines_10m', 'kline': kline}


def test_binance_rest_bucket_fetcher_returns_none_for_empty_or_nonmatching_kline(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(gap_filler, 'request_json', lambda *args, **kwargs: [])
    assert BinanceRestBucketFetcher().fetch_bucket('binance', 'BTCUSDT', bucket) is None

    other_bucket = bucket + timedelta(minutes=10)
    monkeypatch.setattr(
        gap_filler,
        'request_json',
        lambda *args, **kwargs: [[_ms(other_bucket), '1', '1', '1', '1', '1', _ms(other_bucket), '1', 1]],
    )
    assert BinanceRestBucketFetcher().fetch_bucket('binance', 'BTCUSDT', bucket) is None


def test_rest_bucket_fetcher_factory_supports_binance_cloud_fallback():
    assert isinstance(get_rest_bucket_fetcher('binance'), BinanceRestBucketFetcher)
