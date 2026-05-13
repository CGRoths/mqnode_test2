from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mqnode.market.price.ws.candle_builder import TenMinuteCandleBuilder
from mqnode.market.price.ws.gap_filler import PriceWsGapFiller
from mqnode.market.price.ws.models import REST_REPAIRED, REST_REQUIRED, WS_CLOSED, ConfirmedCandleEvent
from mqnode.market.price.ws.repository import PriceWsRepository
from mqnode.market.price.ws.runtime import _finalize_ready_buckets, initialize_startup_bucket_guard
from mqnode.market.price.ws.state import DIRTY_STARTUP_PARTIAL_BUCKET


class _Fetcher:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def fetch_bucket(self, source_name, symbol, bucket_start_utc):
        self.calls.append((source_name, symbol, bucket_start_utc))
        return self.row


class _Repository:
    def __init__(self):
        self.upserts = []
        self.gaps = []
        self.statuses = []

    def upsert_source_price_row(self, db, source_name, table_name, row):
        self.upserts.append((source_name, table_name, row))
        return 1

    def record_gap(self, db, **kwargs):
        self.gaps.append(kwargs)

    def mark_source_status(self, db, **kwargs):
        self.statuses.append(kwargs)


class _Pusher:
    def __init__(self):
        self.rows = []

    def push_rows(self, rows):
        self.rows.extend(rows)


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
        volume_btc=minute + 1,
        volume_usd=(minute + 1) * (101 + minute),
        trade_count=minute + 2,
        source_updated_at=child_start + timedelta(minutes=1),
        received_at=child_start + timedelta(minutes=1, seconds=1),
        raw_payload={'minute': minute},
    )


def _rest_row(bucket: datetime) -> dict:
    return {
        'source_name': 'bybit',
        'symbol': 'BTCUSDT',
        'bucket_start_utc': bucket,
        'open_price_usd': 100,
        'high_price_usd': 110,
        'low_price_usd': 90,
        'close_price_usd': 105,
        'volume_btc': 2,
        'volume_usd': 210,
        'trade_count': 12,
        'raw_payload': {'rest': True},
        'source_updated_at': bucket + timedelta(minutes=10),
    }


def test_startup_bucket_guard_marks_current_open_bucket_dirty():
    startup_now = datetime(2026, 4, 20, 22, 36, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()

    guard = initialize_startup_bucket_guard(
        builder,
        source_name='bybit',
        symbol='BTCUSDT',
        now=startup_now,
    )

    state = builder.state_for('bybit', 'BTCUSDT', datetime(2026, 4, 20, 22, 30, tzinfo=timezone.utc))
    assert state.is_dirty is True
    assert state.dirty_reason == DIRTY_STARTUP_PARTIAL_BUCKET
    assert guard.startup_bucket_start == datetime(2026, 4, 20, 22, 30, tzinfo=timezone.utc)
    assert guard.earliest_bucket_start_utc == datetime(2026, 4, 20, 22, 30, tzinfo=timezone.utc)
    assert guard.earliest_ws_closed_eligible_bucket_start == datetime(2026, 4, 20, 22, 40, tzinfo=timezone.utc)


def test_startup_bucket_cannot_finalize_ws_closed_even_if_all_children_exist():
    bucket = datetime(2026, 4, 20, 22, 30, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    initialize_startup_bucket_guard(
        builder,
        source_name='bybit',
        symbol='BTCUSDT',
        now=bucket + timedelta(minutes=6),
    )
    for minute in range(10):
        builder.accept(_event(bucket, minute))

    result = builder.finalize_bucket('bybit', 'BTCUSDT', bucket, closed_at=bucket + timedelta(minutes=10, seconds=5))

    assert result.quality_status == REST_REQUIRED
    assert result.candle is None
    assert result.rest_required_reason == DIRTY_STARTUP_PARTIAL_BUCKET


def test_next_full_bucket_can_finalize_ws_closed():
    startup_now = datetime(2026, 4, 20, 22, 36, tzinfo=timezone.utc)
    eligible_bucket = datetime(2026, 4, 20, 22, 40, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    initialize_startup_bucket_guard(builder, source_name='bybit', symbol='BTCUSDT', now=startup_now)
    for minute in range(10):
        builder.accept(_event(eligible_bucket, minute))

    result = builder.finalize_bucket(
        'bybit',
        'BTCUSDT',
        eligible_bucket,
        closed_at=eligible_bucket + timedelta(minutes=10, seconds=5),
    )

    assert result.quality_status == WS_CLOSED
    assert result.candle is not None
    assert result.candle.bucket_start_utc == eligible_bucket


def test_finalizer_uses_rest_fallback_for_startup_bucket():
    startup_now = datetime(2026, 4, 20, 22, 36, tzinfo=timezone.utc)
    startup_bucket = datetime(2026, 4, 20, 22, 30, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    guard = initialize_startup_bucket_guard(builder, source_name='bybit', symbol='BTCUSDT', now=startup_now)
    repository = _Repository()
    fetcher = _Fetcher(_rest_row(startup_bucket))

    _finalize_ready_buckets(
        db=object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        builder=builder,
        gap_filler=PriceWsGapFiller(fetcher, repository),
        repository=repository,
        now=startup_bucket + timedelta(minutes=10, seconds=5),
        grace_seconds=5,
        finalized_buckets=set(),
        rest_fallback_enabled=True,
        earliest_bucket_start_utc=guard.earliest_bucket_start_utc,
        earliest_ws_closed_eligible_bucket_start=guard.earliest_ws_closed_eligible_bucket_start,
    )

    assert fetcher.calls == [('bybit', 'BTCUSDT', startup_bucket)]
    assert len(repository.upserts) == 1
    assert repository.upserts[0][2]['quality_status'] == REST_REPAIRED
    assert repository.gaps[0]['reason'] == DIRTY_STARTUP_PARTIAL_BUCKET


def test_remote_push_startup_bucket_pushes_rest_repaired_not_ws_closed():
    startup_now = datetime(2026, 4, 20, 22, 36, tzinfo=timezone.utc)
    startup_bucket = datetime(2026, 4, 20, 22, 30, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    guard = initialize_startup_bucket_guard(builder, source_name='bybit', symbol='BTCUSDT', now=startup_now)
    pusher = _Pusher()

    _finalize_ready_buckets(
        db=None,
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        builder=builder,
        gap_filler=PriceWsGapFiller(_Fetcher(_rest_row(startup_bucket)), PriceWsRepository()),
        repository=PriceWsRepository(),
        now=startup_bucket + timedelta(minutes=10, seconds=5),
        grace_seconds=5,
        finalized_buckets=set(),
        rest_fallback_enabled=True,
        earliest_bucket_start_utc=guard.earliest_bucket_start_utc,
        earliest_ws_closed_eligible_bucket_start=guard.earliest_ws_closed_eligible_bucket_start,
        remote_pusher=pusher,
    )

    assert len(pusher.rows) == 1
    assert pusher.rows[0]['bucket_start_utc'] == startup_bucket
    assert pusher.rows[0]['quality_status'] == REST_REPAIRED
    assert pusher.rows[0]['data_source_mode'] == 'rest_live_fallback_cloud'


def test_clean_eligible_bucket_row_format_is_unchanged():
    eligible_bucket = datetime(2026, 4, 20, 22, 40, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    for minute in range(10):
        builder.accept(_event(eligible_bucket, minute))

    result = builder.finalize_bucket(
        'bybit',
        'BTCUSDT',
        eligible_bucket,
        closed_at=eligible_bucket + timedelta(minutes=10, seconds=5),
    )

    row = result.candle.to_source_row()
    assert {
        'source_name',
        'symbol',
        'bucket_start_utc',
        'open_price_usd',
        'high_price_usd',
        'low_price_usd',
        'close_price_usd',
        'volume_btc',
        'volume_usd',
        'trade_count',
        'raw_payload',
        'source_updated_at',
        'data_source_mode',
        'quality_status',
        'ws_closed_at',
        'first_received_at',
        'last_received_at',
        'ws_disconnect_count',
        'ws_gap_count',
        'expected_child_candle_count',
        'actual_child_candle_count',
    } <= set(row)
    assert row['quality_status'] == WS_CLOSED
    assert row['data_source_mode'] == 'websocket_confirmed_1m'


def test_startup_dirty_reason_does_not_leak_into_next_bucket():
    startup_now = datetime(2026, 4, 20, 22, 36, tzinfo=timezone.utc)
    next_bucket = datetime(2026, 4, 20, 22, 40, tzinfo=timezone.utc)
    builder = TenMinuteCandleBuilder()
    initialize_startup_bucket_guard(builder, source_name='bybit', symbol='BTCUSDT', now=startup_now)

    next_state = builder.state_for('bybit', 'BTCUSDT', next_bucket)

    assert next_state.is_dirty is False
    assert next_state.dirty_reason is None
