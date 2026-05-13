from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mqnode.market.price.ws.candle_builder import TenMinuteCandleBuilder
from mqnode.market.price.ws.gap_detector import PriceWsGapDetector
from mqnode.market.price.ws.models import REST_REQUIRED, WS_CLOSED, ConfirmedCandleEvent
from mqnode.market.price.ws.state import (
    DIRTY_WS_DISCONNECT,
    InMemoryPriceWsStateStore,
)


def _event(bucket: datetime, minute: int, *, received_offset_seconds: int | None = None) -> ConfirmedCandleEvent:
    child_start = bucket + timedelta(minutes=minute)
    received_at = None
    if received_offset_seconds is not None:
        received_at = child_start + timedelta(minutes=1, seconds=received_offset_seconds)
    return ConfirmedCandleEvent(
        source_name='bybit',
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
        received_at=received_at,
        raw_payload={'minute': minute},
    )


def test_clean_confirmed_1m_candles_aggregate_into_10m_ws_closed():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()

    for minute in range(10):
        builder.accept(_event(bucket, minute, received_offset_seconds=1))

    result = builder.finalize_bucket('bybit', 'BTCUSDT', bucket, closed_at=bucket + timedelta(minutes=10, seconds=5))

    assert result.quality_status == WS_CLOSED
    assert result.candle is not None
    candle = result.candle
    assert candle.bucket_start_utc == bucket
    assert candle.open_price_usd == 100
    assert candle.close_price_usd == 110
    assert candle.high_price_usd == 119
    assert candle.low_price_usd == 81
    assert candle.volume_btc == 55
    assert candle.volume_usd == sum((minute + 1) * (101 + minute) for minute in range(10))
    assert candle.trade_count == sum(minute + 2 for minute in range(10))
    assert candle.expected_child_candle_count == 10
    assert candle.actual_child_candle_count == 10


def test_missing_one_1m_candle_marks_bucket_rest_required():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()

    for minute in range(10):
        if minute != 4:
            builder.accept(_event(bucket, minute))

    result = builder.finalize_bucket('bybit', 'BTCUSDT', bucket, closed_at=bucket + timedelta(minutes=10, seconds=5))

    assert result.quality_status == REST_REQUIRED
    assert result.candle is None
    assert result.rest_required
    assert result.missing_child_buckets == (bucket + timedelta(minutes=4),)
    assert builder.state_for('bybit', 'BTCUSDT', bucket).is_dirty is True


def test_dirty_bucket_does_not_produce_ws_closed_candle():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()

    for minute in range(10):
        builder.accept(_event(bucket, minute))
    builder.mark_dirty('bybit', 'BTCUSDT', bucket, DIRTY_WS_DISCONNECT)

    result = builder.finalize_bucket('bybit', 'BTCUSDT', bucket, closed_at=bucket + timedelta(minutes=10, seconds=5))

    assert result.quality_status == REST_REQUIRED
    assert result.candle is None
    assert result.rest_required_reason == DIRTY_WS_DISCONNECT


def test_out_of_order_but_complete_candles_are_sorted_safely():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()

    for minute in reversed(range(10)):
        builder.accept(_event(bucket, minute))

    result = builder.finalize_bucket('bybit', 'BTCUSDT', bucket, closed_at=bucket + timedelta(minutes=10, seconds=5))

    assert result.candle is not None
    assert result.candle.open_price_usd == 100
    assert result.candle.close_price_usd == 110


def test_reconnect_or_disconnect_flag_makes_current_bucket_dirty():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    state_store = InMemoryPriceWsStateStore()
    builder = TenMinuteCandleBuilder(state_store)
    detector = PriceWsGapDetector(state_store, stale_after=timedelta(seconds=30))
    builder.accept(_event(bucket, 0))

    state = detector.record_disconnect('bybit', 'BTCUSDT')

    assert state is not None
    assert state.is_dirty is True
    assert state.ws_disconnect_count == 1
    assert state.dirty_reason == DIRTY_WS_DISCONNECT


def test_next_bucket_after_reconnect_starts_clean():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    state_store = InMemoryPriceWsStateStore()
    builder = TenMinuteCandleBuilder(state_store)
    detector = PriceWsGapDetector(state_store, stale_after=timedelta(seconds=30))
    builder.accept(_event(bucket, 0))

    detector.record_disconnect('bybit', 'BTCUSDT')
    next_state = detector.record_reconnect('bybit', 'BTCUSDT', bucket + timedelta(minutes=10))

    assert next_state.is_dirty is False
    assert next_state.bucket_start_utc == bucket + timedelta(minutes=10)
    assert state_store.current('bybit', 'BTCUSDT') is next_state


def test_bybit_parser_ignores_partial_and_emits_confirmed_event():
    from mqnode.market.price.ws.sources.bybit import BybitWsSource

    received_at = datetime(2026, 4, 20, 0, 1, 1, tzinfo=timezone.utc)
    payload = {
        'topic': 'kline.1.BTCUSDT',
        'ts': int(received_at.timestamp() * 1000),
        'data': [
            {
                'start': int(datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'end': int(datetime(2026, 4, 20, 0, 0, 59, 999000, tzinfo=timezone.utc).timestamp() * 1000),
                'interval': '1',
                'open': '100',
                'close': '101',
                'high': '102',
                'low': '99',
                'volume': '3',
                'turnover': '303',
                'confirm': False,
                'timestamp': int(received_at.timestamp() * 1000),
            },
            {
                'start': int(datetime(2026, 4, 20, 0, 1, tzinfo=timezone.utc).timestamp() * 1000),
                'end': int(datetime(2026, 4, 20, 0, 1, 59, 999000, tzinfo=timezone.utc).timestamp() * 1000),
                'interval': '1',
                'open': '101',
                'close': '102',
                'high': '103',
                'low': '100',
                'volume': '4',
                'turnover': '408',
                'confirm': True,
                'timestamp': int(received_at.timestamp() * 1000),
            },
        ],
    }

    events = BybitWsSource().parse_message(payload, received_at=received_at)

    assert len(events) == 1
    assert events[0].symbol == 'BTCUSDT'
    assert events[0].bucket_start_utc == datetime(2026, 4, 20, 0, 1, tzinfo=timezone.utc)
    assert events[0].close_price_usd == pytest.approx(102)
