from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from mqnode.market.price.lifecycle.profiles import get_price_source_lifecycle_profile
from mqnode.market.price.source_worker import SourcePriceWorker
from mqnode.market.price.ws.models import REST_CONFIRMED, WS_CLOSED


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DB:
    def cursor(self):
        return _Cursor()


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

    def fetch_bucket(self, source_name, symbol, bucket_start_utc):
        return self.row


def _settings():
    return SimpleNamespace(
        price_source_worker_poll_seconds=0,
        price_source_worker_confirmation_poll_seconds=0,
        price_request_timeout_seconds=5,
        price_ws_rest_confirm_delay_seconds=0,
        price_ws_child_interval='1m',
        price_ws_output_mode='local_db',
        price_ws_remote_ingest_url=None,
        price_ws_price_tolerance_bps=1,
        price_ws_volume_tolerance_bps=10,
        price_ws_bucket_grace_seconds=5,
    )


def _row(bucket, *, close=100):
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
        'quality_status': WS_CLOSED,
        'revision_count': 0,
    }


def test_confirmation_loop_confirms_matching_ws_bucket_while_ws_is_active():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 20, 0, 11, tzinfo=timezone.utc)
    repository = _Repository(_row(bucket))
    composed = []
    ws_calls = []
    settings = _settings()

    async def fake_ws_runner(**kwargs):
        ws_calls.append(kwargs)
        kwargs['on_source_row_written']({'bucket_start_utc': bucket, 'quality_status': WS_CLOSED})
        await asyncio.sleep(0.01)

    worker = SourcePriceWorker(
        'bybit',
        db=_DB(),
        settings=settings,
        repository=repository,
        rest_fetcher_factory=lambda source_name, timeout_seconds=30: _Fetcher(_row(bucket)),
        ws_runner=fake_ws_runner,
        compose_fn=lambda db, bucket_start_utc, settings=None: composed.append(bucket_start_utc),
        now_fn=lambda: now,
    )

    asyncio.run(
        worker.start_ws_runtime_async(
            _DB(),
            get_price_source_lifecycle_profile('bybit', settings=settings),
            confirmation_poll_seconds=0,
        )
    )

    assert ws_calls
    assert repository.confirmed == [('bybit_price_10m', bucket, now)]
    assert repository.revisions == []
    assert composed == [bucket, bucket]


def test_confirm_due_buckets_revises_mismatching_ws_bucket_and_recomposes():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    repository = _Repository(_row(bucket, close=100))
    composed = []
    worker = SourcePriceWorker(
        'bybit',
        db=_DB(),
        settings=_settings(),
        repository=repository,
        rest_fetcher_factory=lambda source_name, timeout_seconds=30: _Fetcher(_row(bucket, close=101)),
        compose_fn=lambda db, bucket_start_utc, settings=None: composed.append(bucket_start_utc),
        now_fn=lambda: datetime(2026, 4, 20, 0, 11, tzinfo=timezone.utc),
    )

    worker.schedule_confirmation(
        bucket,
        delay_seconds=0,
        now=datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc),
    )
    results = worker.confirm_due_buckets(_DB(), now=datetime(2026, 4, 20, 0, 11, tzinfo=timezone.utc))

    assert results[0].quality_status == REST_CONFIRMED
    assert results[0].revised is True
    assert repository.confirmed == []
    assert repository.revisions[0]['new_row']['close_price_usd'] == 101
    assert composed == [bucket]
