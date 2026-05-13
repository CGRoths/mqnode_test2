from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mqnode.market.price.ws.gap_filler import (
    BinanceRestBucketFetcher,
    BybitRestBucketFetcher,
    OkxRestBucketFetcher,
    PriceWsGapFiller,
    get_rest_bucket_fetcher,
)
from mqnode.market.price.ws.models import MISSING, REST_REPAIRED


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


def _row(bucket):
    return {
        'source_name': 'bybit',
        'symbol': 'BTCUSDT',
        'bucket_start_utc': bucket,
        'open_price_usd': 100,
        'high_price_usd': 105,
        'low_price_usd': 99,
        'close_price_usd': 104,
        'volume_btc': 2,
        'volume_usd': 208,
        'trade_count': 10,
        'raw_payload': {'rest': True},
        'source_updated_at': bucket,
    }


def test_dirty_bucket_calls_rest_fallback_at_bucket_close():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 20, 0, 10, 5, tzinfo=timezone.utc)
    fetcher = _Fetcher(_row(bucket))
    repository = _Repository()
    filler = PriceWsGapFiller(fetcher, repository)

    result = filler.fill_dirty_bucket(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        bucket_start_utc=bucket,
        reason='missing_1m_candle',
        now=now,
    )

    assert fetcher.calls == [('bybit', 'BTCUSDT', bucket)]
    assert result.quality_status == REST_REPAIRED


def test_rest_fallback_writes_one_source_row_with_rest_repaired_quality():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 20, 0, 10, 5, tzinfo=timezone.utc)
    repository = _Repository()
    filler = PriceWsGapFiller(_Fetcher(_row(bucket)), repository)

    filler.fill_dirty_bucket(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        bucket_start_utc=bucket,
        reason='ws_disconnect',
        now=now,
    )

    assert len(repository.upserts) == 1
    source_name, table_name, row = repository.upserts[0]
    assert source_name == 'bybit'
    assert table_name == 'bybit_price_10m'
    assert row['quality_status'] == REST_REPAIRED
    assert row['data_source_mode'] == 'rest_live_fallback'
    assert row['is_repaired'] is True
    assert repository.gaps[0]['repair_status'] == REST_REPAIRED
    assert repository.statuses[0]['status'] == REST_REPAIRED


def test_rest_returns_no_data_leaves_bucket_missing():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 20, 0, 10, 5, tzinfo=timezone.utc)
    repository = _Repository()
    filler = PriceWsGapFiller(_Fetcher(None), repository)

    result = filler.fill_dirty_bucket(
        object(),
        source_name='bybit',
        table_name='bybit_price_10m',
        symbol='BTCUSDT',
        bucket_start_utc=bucket,
        reason='heartbeat_timeout',
        now=now,
    )

    assert result.quality_status == MISSING
    assert result.row is None
    assert len(repository.upserts) == 1
    source_name, table_name, row = repository.upserts[0]
    assert source_name == 'bybit'
    assert table_name == 'bybit_price_10m'
    assert row['quality_status'] == MISSING
    assert row['data_source_mode'] == 'rest_live_fallback'
    assert row['open_price_usd'] is None
    assert row['raw_payload'] == {
        'rest_fallback': 'missing',
        'reason': 'heartbeat_timeout',
        'error': 'REST fallback returned no candle',
    }
    assert repository.gaps[0]['repair_status'] == MISSING
    assert repository.statuses[0]['status'] == MISSING


def test_rest_bucket_fetcher_factory_supports_only_phase1_sources():
    assert isinstance(get_rest_bucket_fetcher('bybit'), BybitRestBucketFetcher)
    assert isinstance(get_rest_bucket_fetcher('okx'), OkxRestBucketFetcher)
    assert isinstance(get_rest_bucket_fetcher('binance'), BinanceRestBucketFetcher)

    for source_name in ('coinbase', 'kraken', 'bitstamp'):
        with pytest.raises(NotImplementedError, match='No phase-1 WS REST fallback fetcher'):
            get_rest_bucket_fetcher(source_name)
