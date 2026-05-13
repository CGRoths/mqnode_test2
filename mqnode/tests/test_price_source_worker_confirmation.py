from __future__ import annotations

from datetime import datetime, timezone

from mqnode.market.price.source_worker import confirm_ws_bucket
from mqnode.market.price.ws.models import REST_CONFIRMED, REST_REPAIRED, WS_CLOSED
from mqnode.market.price.ws.reconciler import PriceWsReconciler


class _Repository:
    def __init__(self, source_row):
        self.source_row = source_row
        self.confirmed = []
        self.revisions = []

    def get_source_price_row(self, db, table_name, bucket_start_utc):
        return self.source_row

    def mark_rest_confirmed(self, db, table_name, bucket_start_utc, confirmed_at):
        self.confirmed.append((table_name, bucket_start_utc, confirmed_at))

    def revise_source_price_row(self, db, **kwargs):
        self.revisions.append(kwargs)


class _Fetcher:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def fetch_bucket(self, source_name, symbol, bucket_start_utc):
        self.calls.append((source_name, symbol, bucket_start_utc))
        return self.row


def _row(bucket, *, close=100, quality_status=WS_CLOSED):
    return {
        'source_name': 'bybit',
        'symbol': 'BTCUSDT',
        'bucket_start_utc': bucket,
        'open_price_usd': 100,
        'high_price_usd': 102,
        'low_price_usd': 99,
        'close_price_usd': close,
        'volume_btc': 5,
        'volume_usd': 500,
        'trade_count': 7,
        'raw_payload': {'close': close},
        'source_updated_at': bucket,
        'quality_status': quality_status,
        'revision_count': 0,
    }


def test_ws_closed_matching_rest_row_marks_confirmed_and_recomposes():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 20, 0, 11, tzinfo=timezone.utc)
    repository = _Repository(_row(bucket))
    fetcher = _Fetcher(_row(bucket))
    recomposed = []

    result = confirm_ws_bucket(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        bucket_start_utc=bucket,
        rest_fetcher=fetcher,
        repository=repository,
        reconciler=PriceWsReconciler(repository=repository),
        compose_fn=lambda db, bucket_start_utc, settings=None: recomposed.append(bucket_start_utc),
        now=now,
    )

    assert result.quality_status == REST_CONFIRMED
    assert result.revised is False
    assert result.recomposed is True
    assert repository.confirmed == [('bybit_price_10m', bucket, now)]
    assert repository.revisions == []
    assert recomposed == [bucket]


def test_ws_closed_mismatching_rest_row_revises_and_recomposes():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    repository = _Repository(_row(bucket, close=100))
    fetcher = _Fetcher(_row(bucket, close=101))
    recomposed = []

    result = confirm_ws_bucket(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        bucket_start_utc=bucket,
        rest_fetcher=fetcher,
        repository=repository,
        reconciler=PriceWsReconciler(repository=repository),
        compose_fn=lambda db, bucket_start_utc, settings=None: recomposed.append(bucket_start_utc),
        now=datetime(2026, 4, 20, 0, 11, tzinfo=timezone.utc),
    )

    assert result.quality_status == REST_CONFIRMED
    assert result.revised is True
    assert result.reason == 'rest_confirmation_mismatch'
    assert repository.confirmed == []
    assert repository.revisions[0]['new_row']['close_price_usd'] == 101
    assert recomposed == [bucket]


def test_non_ws_closed_row_is_not_confirmed():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    repository = _Repository(_row(bucket, quality_status=REST_REPAIRED))
    fetcher = _Fetcher(_row(bucket))
    recomposed = []

    result = confirm_ws_bucket(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        bucket_start_utc=bucket,
        rest_fetcher=fetcher,
        repository=repository,
        compose_fn=lambda db, bucket_start_utc, settings=None: recomposed.append(bucket_start_utc),
    )

    assert result.attempted is False
    assert result.quality_status == REST_REPAIRED
    assert fetcher.calls == []
    assert recomposed == []
