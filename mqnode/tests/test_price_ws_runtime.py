from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from mqnode.market.price.ws import runtime
from mqnode.market.price.ws.candle_builder import TenMinuteCandleBuilder
from mqnode.market.price.ws.gap_detector import PriceWsGapDetector
from mqnode.market.price.ws.models import REST_REPAIRED, WS_CLOSED, ConfirmedCandleEvent
from mqnode.market.price.ws.runtime import (
    _bucket_finalizer_loop,
    _ensure_expected_bucket_states,
    _finalize_ready_buckets,
    _load_source_connector,
    _mark_ws_disconnected,
    _parse_reconnect_backoff_seconds,
)


class _Repository:
    def __init__(self):
        self.upserts = []
        self.statuses = []

    def upsert_source_price_row(self, db, source_name, table_name, row):
        self.upserts.append((source_name, table_name, row))
        return 1

    def mark_source_status(self, db, **kwargs):
        self.statuses.append(kwargs)


class _GapFiller:
    def __init__(self, quality_status=REST_REPAIRED):
        self.quality_status = quality_status
        self.calls = []

    def fill_dirty_bucket(self, db, **kwargs):
        self.calls.append(kwargs)
        return self.quality_status


class _FlakyGapFiller:
    def __init__(self):
        self.calls = []

    def fill_dirty_bucket(self, db, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            raise RuntimeError('temporary REST fallback failure')
        return REST_REPAIRED


def _event(bucket: datetime, minute: int) -> ConfirmedCandleEvent:
    child_start = bucket + timedelta(minutes=minute)
    return ConfirmedCandleEvent(
        source_name='bybit',
        symbol='BTCUSDT',
        bucket_start_utc=child_start,
        open_price_usd=100 + minute,
        high_price_usd=110 + minute,
        low_price_usd=90 - minute,
        close_price_usd=101 + minute,
        volume_btc=1,
        volume_usd=101 + minute,
        received_at=child_start + timedelta(minutes=1, seconds=1),
        source_updated_at=child_start + timedelta(minutes=1),
    )


def _finalize(builder, repository, gap_filler, now):
    finalized = set()
    _finalize_ready_buckets(
        db=object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        builder=builder,
        gap_filler=gap_filler,
        repository=repository,
        now=now,
        grace_seconds=5,
        finalized_buckets=finalized,
        rest_fallback_enabled=True,
    )
    return finalized


def test_runtime_finalizer_writes_clean_bucket_as_ws_closed():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    repository = _Repository()
    gap_filler = _GapFiller()

    for minute in range(10):
        builder.accept(_event(bucket, minute))
    _finalize(builder, repository, gap_filler, bucket + timedelta(minutes=10, seconds=5))

    assert len(repository.upserts) == 1
    assert repository.upserts[0][2]['quality_status'] == WS_CLOSED
    assert gap_filler.calls == []


def test_runtime_finalizer_calls_rest_for_dirty_bucket_at_close_plus_grace():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    repository = _Repository()
    gap_filler = _GapFiller()

    builder.state_for('bybit', 'BTCUSDT', bucket).mark_dirty('ws_disconnect')
    _finalize(builder, repository, gap_filler, bucket + timedelta(minutes=10, seconds=5))

    assert repository.upserts == []
    assert len(gap_filler.calls) == 1
    assert gap_filler.calls[0]['reason'] == 'ws_disconnect'


def test_runtime_scheduler_creates_no_event_bucket_and_rest_fallbacks_immediately():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    repository = _Repository()
    gap_filler = _GapFiller()

    finalized = _finalize(builder, repository, gap_filler, bucket + timedelta(minutes=10, seconds=5))

    assert ('bybit', 'BTCUSDT', bucket) in finalized
    assert repository.upserts == []
    assert len(gap_filler.calls) == 1
    assert gap_filler.calls[0]['bucket_start_utc'] == bucket
    assert gap_filler.calls[0]['reason'] == 'missing_1m_candle'


def test_runtime_disconnect_marks_current_bucket_dirty_status():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    detector = PriceWsGapDetector(builder.state_store, stale_after=timedelta(seconds=30))
    repository = _Repository()

    _mark_ws_disconnected(
        db=object(),
        source_name='bybit',
        symbol='BTCUSDT',
        builder=builder,
        detector=detector,
        repository=repository,
        now=bucket + timedelta(minutes=1),
        error='socket closed',
    )

    state = builder.state_store.current('bybit', 'BTCUSDT')
    assert state is not None
    assert state.is_dirty is True
    assert state.ws_disconnect_count == 1
    assert repository.statuses[0]['status'] == 'ws_disconnected'
    assert repository.statuses[0]['current_bucket_dirty'] is True


def test_runtime_reconnect_does_not_make_dirty_bucket_clean_and_next_bucket_can_start_clean():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    detector = PriceWsGapDetector(builder.state_store, stale_after=timedelta(seconds=30))

    _ensure_expected_bucket_states(builder, source_name='bybit', symbol='BTCUSDT', now=bucket + timedelta(minutes=1))
    detector.record_disconnect('bybit', 'BTCUSDT')
    dirty_state = detector.record_reconnect('bybit', 'BTCUSDT')
    next_state = builder.state_store.start_next_bucket('bybit', 'BTCUSDT', bucket + timedelta(minutes=10))

    assert dirty_state is not None
    assert dirty_state.is_dirty is True
    assert next_state.is_dirty is False


def test_runtime_finalizer_continues_after_disconnect_and_falls_back():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    detector = PriceWsGapDetector(builder.state_store, stale_after=timedelta(seconds=30))
    repository = _Repository()
    gap_filler = _GapFiller()

    _mark_ws_disconnected(
        db=object(),
        source_name='bybit',
        symbol='BTCUSDT',
        builder=builder,
        detector=detector,
        repository=repository,
        now=bucket + timedelta(minutes=1),
        error='socket closed',
    )
    _finalize(builder, repository, gap_filler, bucket + timedelta(minutes=10, seconds=5))

    assert len(gap_filler.calls) == 1
    assert gap_filler.calls[0]['reason'] == 'ws_disconnect'


def test_runtime_backoff_parser_keeps_configured_sequence():
    assert _parse_reconnect_backoff_seconds('1,2,5,10') == (1.0, 2.0, 5.0, 10.0)


def test_runtime_loads_available_ws_connectors():
    assert _load_source_connector('bybit').source_name == 'bybit'
    assert _load_source_connector('okx').source_name == 'okx'
    assert _load_source_connector('binance').source_name == 'binance'

    for source_name in ('missing_source',):
        with pytest.raises(NotImplementedError, match='has no connector module'):
            _load_source_connector(source_name)


def test_runtime_finalizer_loop_survives_transient_tick_exception(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = bucket + timedelta(minutes=10, seconds=5)
    builder = TenMinuteCandleBuilder()
    detector = PriceWsGapDetector(builder.state_store, stale_after=timedelta(seconds=30))
    repository = _Repository()
    gap_filler = _FlakyGapFiller()
    sleep_count = 0

    async def fake_sleep(seconds):
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(runtime, 'utc_now', lambda: now)
    monkeypatch.setattr(runtime.asyncio, 'sleep', fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            _bucket_finalizer_loop(
                db=object(),
                source_name='bybit',
                table_name='bybit_price_10m',
                symbol='BTCUSDT',
                builder=builder,
                detector=detector,
                gap_filler=gap_filler,
                repository=repository,
                settings=SimpleNamespace(
                    price_ws_bucket_grace_seconds=5,
                    price_ws_rest_fallback_enabled=True,
                ),
                finalized_buckets=set(),
                earliest_bucket_start_utc=bucket,
            )
        )

    assert len(gap_filler.calls) == 2
    assert gap_filler.calls[0]['bucket_start_utc'] == bucket
    assert gap_filler.calls[1]['bucket_start_utc'] == bucket
