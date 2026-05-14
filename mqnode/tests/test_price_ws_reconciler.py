from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mqnode.market.price.ws.models import REST_CONFIRMED, WS_CLOSED
from mqnode.market.price.ws.reconciler import PriceWsReconciler


class _Repository:
    def __init__(self):
        self.confirmed = []
        self.revisions = []

    def mark_rest_confirmed(self, db, table_name, bucket_start_utc, confirmed_at):
        self.confirmed.append((table_name, bucket_start_utc, confirmed_at))

    def revise_source_price_row(self, db, **kwargs):
        self.revisions.append(kwargs)


def _row(bucket, close=100, volume_btc=5):
    return {
        'source_name': 'bybit',
        'symbol': 'BTCUSDT',
        'bucket_start_utc': bucket,
        'open_price_usd': 100,
        'high_price_usd': 102,
        'low_price_usd': 99,
        'close_price_usd': close,
        'volume_btc': volume_btc,
        'volume_usd': 500,
        'raw_payload': {'row': close},
        'quality_status': WS_CLOSED,
    }


def test_ws_candle_equals_rest_within_tolerance_marks_rest_confirmed():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 20, 0, 10, 30, tzinfo=timezone.utc)
    repository = _Repository()
    reconciler = PriceWsReconciler(price_tolerance_bps=1, volume_tolerance_bps=10, repository=repository)

    result = reconciler.reconcile(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        ws_row=_row(bucket, close=100.00001),
        rest_row=_row(bucket, close=100),
        now=now,
    )

    assert result.quality_status == REST_CONFIRMED
    assert result.revised is False
    assert repository.confirmed == [('bybit_price_10m', bucket, now)]
    assert repository.revisions == []


def test_rest_diff_beyond_tolerance_updates_row_and_records_revision_log():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 20, 0, 10, 30, tzinfo=timezone.utc)
    repository = _Repository()
    reconciler = PriceWsReconciler(price_tolerance_bps=1, volume_tolerance_bps=10, repository=repository)

    result = reconciler.reconcile(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        ws_row={**_row(bucket, close=100), 'revision_count': 0},
        rest_row=_row(bucket, close=101),
        now=now,
    )

    assert result.quality_status == REST_CONFIRMED
    assert result.revised is True
    assert result.reason == 'rest_confirmation_mismatch'
    assert repository.confirmed == []
    assert len(repository.revisions) == 1
    revision = repository.revisions[0]
    assert revision['source_name'] == 'bybit'
    assert revision['table_name'] == 'bybit_price_10m'
    assert revision['revision_reason'] == 'rest_confirmation_mismatch'
    assert revision['old_row']['close_price_usd'] == 100
    assert revision['new_row']['close_price_usd'] == 101


def test_incomplete_rest_row_does_not_confirm_or_revise():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 20, 0, 10, 30, tzinfo=timezone.utc)
    repository = _Repository()
    reconciler = PriceWsReconciler(price_tolerance_bps=1, volume_tolerance_bps=10, repository=repository)
    rest_row = {**_row(bucket, close=101), 'volume_btc': None}

    result = reconciler.reconcile(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        ws_row={**_row(bucket, close=100), 'revision_count': 0},
        rest_row=rest_row,
        now=now,
    )

    assert result.quality_status == WS_CLOSED
    assert result.revised is False
    assert result.reason == 'rest_row_incomplete'
    assert repository.confirmed == []
    assert repository.revisions == []


def test_rest_bucket_mismatch_does_not_confirm_or_revise():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    rest_bucket = datetime(2026, 4, 20, 8, 10, tzinfo=timezone(timedelta(hours=8)))
    now = datetime(2026, 4, 20, 0, 10, 30, tzinfo=timezone.utc)
    repository = _Repository()
    reconciler = PriceWsReconciler(price_tolerance_bps=1, volume_tolerance_bps=10, repository=repository)

    result = reconciler.reconcile(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        ws_row={**_row(bucket, close=100), 'revision_count': 0},
        rest_row=_row(rest_bucket, close=101),
        now=now,
    )

    assert result.quality_status == WS_CLOSED
    assert result.revised is False
    assert result.reason == 'rest_bucket_mismatch'
    assert repository.confirmed == []
    assert repository.revisions == []
