from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from mqnode.market.price import composer
from mqnode.market.price.composer import compose_canonical_price, compose_price_details, estimate_price_completeness
from mqnode.market.price.normalize import aggregate_small_candles_to_10m
from mqnode.market.price.ws.models import MISSING, REST_REPAIRED, REST_REQUIRED, WS_CLOSED


def test_compose_canonical_price_uses_volume_weighted_fair_price():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    payload = compose_canonical_price(
        bucket,
        [
            {
                'source_name': 'bitstamp',
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 10,
                'volume_usd': 880000,
            },
            {
                'source_name': 'bybit',
                'open_price_usd': 87980,
                'high_price_usd': 88200,
                'low_price_usd': 87890,
                'close_price_usd': 88040,
                'volume_btc': 12,
                'volume_usd': 1056000,
            },
            {
                'source_name': 'binance',
                'open_price_usd': 88020,
                'high_price_usd': 88120,
                'low_price_usd': 87920,
                'close_price_usd': 88060,
                'volume_btc': 20,
                'volume_usd': 1760000,
            },
        ],
    )

    assert payload is not None
    assert payload['source_count'] == 3
    assert payload['source_names'] == ['bitstamp', 'binance', 'bybit']
    assert payload['composition_method'] == 'volume_weighted_ohlc_v1'
    assert payload['open_price_usd'] == pytest.approx((88000 * 10 + 87980 * 12 + 88020 * 20) / 42)
    assert payload['close_price_usd'] == pytest.approx((88050 * 10 + 88040 * 12 + 88060 * 20) / 42)
    assert payload['volume_btc'] == 42
    assert payload['volume_usd'] == 3696000


def test_compose_price_details_keeps_exchange_level_fields():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    payload = compose_price_details(
        bucket,
        [
            {
                'source_name': 'bitstamp',
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 10,
                'volume_usd': 880000,
            },
            {
                'source_name': 'coinbase',
                'open_price_usd': 88010,
                'high_price_usd': 88110,
                'low_price_usd': 87910,
                'close_price_usd': 88020,
                'volume_btc': 5,
                'volume_usd': 440100,
            },
        ],
    )

    assert payload is not None
    assert payload['bitstamp_close_price'] == 88050
    assert payload['coinbase_close_price'] == 88020
    assert payload['fair_close_price'] == pytest.approx((88050 * 10 + 88020 * 5) / 15)
    assert payload['total_volume_btc'] == 15


def test_aggregate_small_candles_to_10m_rolls_5m_rows_up_cleanly():
    rows = [
        {
            'bucket_start_utc': datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc),
            'open_price_usd': 88000,
            'high_price_usd': 88100,
            'low_price_usd': 87950,
            'close_price_usd': 88020,
            'volume_btc': 3,
            'volume_usd': 264060,
            'trade_count': 10,
            'raw_payload': {'id': 1},
            'source_updated_at': datetime(2026, 4, 20, 0, 5, tzinfo=timezone.utc),
        },
        {
            'bucket_start_utc': datetime(2026, 4, 20, 0, 5, tzinfo=timezone.utc),
            'open_price_usd': 88020,
            'high_price_usd': 88200,
            'low_price_usd': 88000,
            'close_price_usd': 88150,
            'volume_btc': 4,
            'volume_usd': 352600,
            'trade_count': 8,
            'raw_payload': {'id': 2},
            'source_updated_at': datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc),
        },
    ]

    aggregated = aggregate_small_candles_to_10m('coinbase', 'BTC-USD', rows)

    assert len(aggregated) == 1
    assert aggregated[0]['bucket_start_utc'] == datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    assert aggregated[0]['open_price_usd'] == 88000
    assert aggregated[0]['close_price_usd'] == 88150
    assert aggregated[0]['high_price_usd'] == 88200
    assert aggregated[0]['low_price_usd'] == 87950
    assert aggregated[0]['volume_btc'] == 7
    assert aggregated[0]['trade_count'] == 18


def test_price_completeness_is_none_without_sources():
    assert estimate_price_completeness([]) is None


def test_compose_price_details_accepts_unknown_source_names():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)

    payload = compose_price_details(
        bucket,
        [
            {
                'source_name': 'test_source',
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 3,
                'volume_usd': 264150,
            }
        ],
    )

    assert payload is not None
    assert payload['source_names'] == ['test_source']
    assert payload['test_source_close_price'] == 88050


def test_fetch_source_rows_for_bucket_skips_missing_table(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    source = type('Source', (), {'source_name': 'optional_source', 'table_name': 'optional_source_price_10m'})()

    class _Cursor:
        def execute(self, query, params):
            raise RuntimeError('relation does not exist')

    monkeypatch.setattr(composer, '_existing_enabled_sources', lambda cur: (source,))

    rows = composer._fetch_source_rows_for_bucket(_Cursor(), bucket)

    assert rows == []


@pytest.mark.parametrize('quality_status', [WS_CLOSED, REST_REPAIRED])
def test_fetch_source_rows_for_bucket_accepts_usable_quality_status(monkeypatch, quality_status):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    source = type('Source', (), {'source_name': 'bybit', 'table_name': 'bybit_price_10m'})()

    class _Cursor:
        def __init__(self, row):
            self.row = row
            self.last_query = ''
            self.last_params = None

        def execute(self, query, params=None):
            self.last_query = query
            self.last_params = params

        def fetchone(self):
            if 'information_schema.columns' in self.last_query:
                return {'column_exists': True}
            accepted_statuses = set(self.last_params[2])
            return self.row if self.row['quality_status'] in accepted_statuses else None

    monkeypatch.setattr(composer, '_existing_enabled_sources', lambda cur: (source,))
    composer.TABLE_COLUMN_CACHE.clear()

    clean_rows = composer._fetch_source_rows_for_bucket(
        _Cursor(
            {
                'source_name': 'bybit',
                'bucket_start_utc': bucket,
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 10,
                'volume_usd': 880500,
                'trade_count': 1,
                'quality_status': quality_status,
            }
        ),
        bucket,
    )

    assert clean_rows[0]['quality_status'] == quality_status


@pytest.mark.parametrize('quality_status', [REST_REQUIRED, MISSING])
def test_fetch_source_rows_for_bucket_filters_unusable_quality_status(monkeypatch, quality_status):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    source = type('Source', (), {'source_name': 'bybit', 'table_name': 'bybit_price_10m'})()

    class _Cursor:
        def __init__(self, row):
            self.row = row
            self.last_query = ''
            self.last_params = None

        def execute(self, query, params=None):
            self.last_query = query
            self.last_params = params

        def fetchone(self):
            if 'information_schema.columns' in self.last_query:
                return {'column_exists': True}
            accepted_statuses = set(self.last_params[2])
            return self.row if self.row['quality_status'] in accepted_statuses else None

    monkeypatch.setattr(composer, '_existing_enabled_sources', lambda cur: (source,))
    composer.TABLE_COLUMN_CACHE.clear()

    rows = composer._fetch_source_rows_for_bucket(
        _Cursor(
            {
                'source_name': 'bybit',
                'bucket_start_utc': bucket,
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 10,
                'volume_usd': 880500,
                'trade_count': 1,
                'quality_status': quality_status,
            }
        ),
        bucket,
    )

    assert rows == []


def test_fetch_source_rows_for_bucket_without_quality_status_remains_backward_compatible(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    source = type('Source', (), {'source_name': 'bybit', 'table_name': 'bybit_price_10m'})()
    row = {
        'source_name': 'bybit',
        'bucket_start_utc': bucket,
        'open_price_usd': 88000,
        'high_price_usd': 88100,
        'low_price_usd': 87900,
        'close_price_usd': 88050,
        'volume_btc': 10,
        'volume_usd': 880500,
        'trade_count': 1,
    }

    class _Cursor:
        def __init__(self):
            self.last_query = ''
            self.last_params = None

        def execute(self, query, params=None):
            self.last_query = query
            self.last_params = params

        def fetchone(self):
            if 'information_schema.columns' in self.last_query:
                return {'column_exists': False}
            return row

    monkeypatch.setattr(composer, '_existing_enabled_sources', lambda cur: (source,))
    composer.TABLE_COLUMN_CACHE.clear()

    rows = composer._fetch_source_rows_for_bucket(_Cursor(), bucket)

    assert rows == [row]


def test_rebuild_canonical_price_bucket_writes_null_row_without_source_rows(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    captured = {}

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _DB:
        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(composer, '_fetch_source_rows_for_bucket', lambda cur, bucket_start_utc: [])
    monkeypatch.setattr(
        composer,
        '_upsert_canonical_price',
        lambda cur, payload: captured.setdefault('canonical', payload),
    )
    monkeypatch.setattr(composer, '_upsert_price_details', lambda cur, payload: captured.setdefault('details', payload))
    monkeypatch.setattr(composer, 'price_checkpoint_ok', lambda cur, last_bucket_time=None: None)

    payload = composer.rebuild_canonical_price_bucket(
        _DB(),
        bucket,
        settings=SimpleNamespace(price_composer_write_null_buckets=True),
    )

    assert payload == captured['canonical']
    assert payload['source_count'] == 0
    assert payload['source_names'] == []
    assert payload['composition_method'] == 'timeline_null_v1'
    assert payload['open_price_usd'] is None
    assert payload['close_price_usd'] is None
    assert captured['details']['source_count'] == 0
    assert captured['details']['fair_close_price'] is None


def test_catch_up_uses_timeline_backbone_and_writes_null_gaps(monkeypatch):
    bucket_0 = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    bucket_1 = datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc)
    bucket_2 = datetime(2026, 4, 20, 0, 20, tzinfo=timezone.utc)
    source_rows_by_bucket = {
        bucket_1: [
            {
                'source_name': 'bitstamp',
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 10,
                'volume_usd': 880500,
            }
        ]
    }
    canonical_rows = {}

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _DB:
        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(
        composer,
        'get_checkpoint',
        lambda cur, chain, component, interval: {'last_bucket_time': None},
    )
    monkeypatch.setattr(composer, '_timeline_backbone_bounds', lambda cur, table_name: (bucket_0, bucket_2))
    monkeypatch.setattr(
        composer,
        '_raw_price_bounds',
        lambda cur: (_ for _ in ()).throw(AssertionError('raw bounds should not be used')),
    )
    monkeypatch.setattr(
        composer,
        '_fetch_source_rows_for_bucket',
        lambda cur, bucket_start_utc: source_rows_by_bucket.get(bucket_start_utc, []),
    )
    monkeypatch.setattr(
        composer,
        '_upsert_canonical_price',
        lambda cur, payload: canonical_rows.__setitem__(payload['bucket_start_utc'], payload),
    )
    monkeypatch.setattr(composer, '_upsert_price_details', lambda cur, payload: None)
    monkeypatch.setattr(composer, 'price_checkpoint_ok', lambda cur, last_bucket_time=None: None)

    rebuilt = composer.catch_up_canonical_price_from_checkpoint(
        _DB(),
        settings=SimpleNamespace(
            price_composer_timeline_backbone='btc_primitive_10m',
            price_composer_write_null_buckets=True,
        ),
    )

    assert rebuilt == 3
    assert canonical_rows[bucket_0]['source_count'] == 0
    assert canonical_rows[bucket_0]['close_price_usd'] is None
    assert canonical_rows[bucket_1]['source_count'] == 1
    assert canonical_rows[bucket_1]['close_price_usd'] == pytest.approx(88050)
    assert canonical_rows[bucket_2]['source_count'] == 0
    assert canonical_rows[bucket_2]['composition_method'] == 'timeline_null_v1'


def test_catch_up_falls_back_to_raw_price_bounds_without_backbone(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc)
    rebuilt_buckets = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _DB:
        def cursor(self):
            return _Cursor()

    monkeypatch.setattr(
        composer,
        'get_checkpoint',
        lambda cur, chain, component, interval: {'last_bucket_time': None},
    )
    monkeypatch.setattr(composer, '_timeline_backbone_bounds', lambda cur, table_name: None)
    monkeypatch.setattr(composer, '_raw_price_bounds', lambda cur: (bucket, bucket))
    monkeypatch.setattr(
        composer,
        'rebuild_canonical_price_bucket',
        lambda db, bucket_start_utc, settings=None: rebuilt_buckets.append(bucket_start_utc),
    )

    rebuilt = composer.catch_up_canonical_price_from_checkpoint(
        _DB(),
        settings=SimpleNamespace(
            price_composer_timeline_backbone='btc_primitive_10m',
            price_composer_write_null_buckets=True,
        ),
    )

    assert rebuilt == 1
    assert rebuilt_buckets == [bucket]
