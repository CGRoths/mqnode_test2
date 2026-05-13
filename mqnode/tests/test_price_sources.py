from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from mqnode.api.routes import internal_price_ingest
from mqnode.market.price import runtime, source_support
from mqnode.market.price.normalize import aggregate_small_candles_to_10m
from mqnode.market.price.registry import PriceSourceSpec, get_enabled_price_sources, get_price_source
from mqnode.market.price.remote_ingest import ingest_remote_binance_rows
from mqnode.market.price.sources import binance, bitstamp, coinbase, gemini, kraken, okx


def _settings():
    return SimpleNamespace(price_request_timeout_seconds=5)


class _DummyCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyDB:
    def cursor(self):
        return _DummyCursor()


def _should_not_import(module_path):
    raise AssertionError(f'should not import {module_path}')


def test_get_ingestion_window_no_checkpoint_uses_registry_historical_start(monkeypatch):
    now = datetime(2026, 4, 20, 0, 27, tzinfo=timezone.utc)
    source = SimpleNamespace(config={'historical_start_utc': '2011-09-13T00:04:00Z'})

    monkeypatch.setattr(source_support, 'utc_now', lambda: now)
    monkeypatch.setattr(source_support, 'get_checkpoint', lambda cur, chain, component, interval: {})
    monkeypatch.setattr(source_support, 'get_price_source', lambda source_name, cur=None: source)

    start_bucket, end_bucket = source_support.get_ingestion_window(_DummyDB(), 'bitstamp')

    assert start_bucket == datetime(2011, 9, 13, 0, 0, tzinfo=timezone.utc)
    assert end_bucket == datetime(2026, 4, 20, 0, 20, tzinfo=timezone.utc)


def test_get_ingestion_window_checkpoint_uses_checkpoint_overlap(monkeypatch):
    now = datetime(2026, 4, 20, 0, 37, tzinfo=timezone.utc)
    checkpoint = {'last_bucket_time': datetime(2026, 4, 20, 0, 20, tzinfo=timezone.utc)}

    monkeypatch.setattr(source_support, 'utc_now', lambda: now)
    monkeypatch.setattr(source_support, 'get_checkpoint', lambda cur, chain, component, interval: checkpoint)
    monkeypatch.setattr(
        source_support,
        'get_price_source',
        lambda source_name, cur=None: (_ for _ in ()).throw(AssertionError('historical config should be ignored')),
    )

    start_bucket, end_bucket = source_support.get_ingestion_window(_DummyDB(), 'bitstamp')

    assert start_bucket == datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc)
    assert end_bucket == datetime(2026, 4, 20, 0, 30, tzinfo=timezone.utc)


def test_get_ingestion_window_invalid_historical_start_falls_back(monkeypatch, caplog):
    now = datetime(2026, 4, 20, 0, 27, tzinfo=timezone.utc)
    source = SimpleNamespace(config={'historical_start_utc': '2011-09-13T00:00:00'})

    monkeypatch.setattr(source_support, 'utc_now', lambda: now)
    monkeypatch.setattr(source_support, 'get_checkpoint', lambda cur, chain, component, interval: {})
    monkeypatch.setattr(source_support, 'get_price_source', lambda source_name, cur=None: source)

    with caplog.at_level(logging.WARNING):
        start_bucket, end_bucket = source_support.get_ingestion_window(_DummyDB(), 'bitstamp', lookback_hours=2)

    assert start_bucket == datetime(2026, 4, 19, 22, 20, tzinfo=timezone.utc)
    assert end_bucket == datetime(2026, 4, 20, 0, 20, tzinfo=timezone.utc)
    assert 'price_source_historical_start_invalid' in caplog.text


def test_aggregate_small_candles_to_10m_skips_partial_bucket_when_required():
    aggregated = aggregate_small_candles_to_10m(
        'coinbase',
        'BTC-USD',
        [
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
            }
        ],
        child_minutes=5,
        require_complete_buckets=True,
    )

    assert aggregated == []


def test_default_enabled_price_sources_match_current_active_set():
    source_names = [source.source_name for source in get_enabled_price_sources()]

    assert 'binance' in source_names
    assert 'bitstamp' in source_names
    assert 'kraken' not in source_names
    assert 'gemini' not in source_names


def test_default_binance_registry_config_is_remote_spot_cloud_mode():
    source = get_price_source('binance')

    assert source.table_name == 'binance_price_10m'
    assert source.market_type == 'spot'
    assert source.config == {
        'mode': 'remote',
        'api_base': 'https://api.binance.com',
        'endpoint': '/api/v3/klines',
        'market_type': 'spot',
        'historical_start_utc': '2017-08-17T00:00:00Z',
        'auto_historical_backfill': True,
    }


def test_okx_fetch_buckets_aggregates_confirmed_5m_rows(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 20, 0, 20, tzinfo=timezone.utc)
    request_params = []
    captured = {}

    monkeypatch.setattr(okx, 'get_ingestion_window', lambda db, source_name: (start, end))

    def fake_request_json(url, *, params=None, timeout=30, **kwargs):
        request_params.append(params)
        return {
            'data': [
                [str(int(start.timestamp() * 1000)), '1', '4', '1', '2', '3', '6', '6', '1'],
                [str(int((start + timedelta(minutes=5)).timestamp() * 1000)), '2', '5', '2', '4', '5', '20', '20', '1'],
                [str(int((start + timedelta(minutes=10)).timestamp() * 1000)), '4', '6', '4', '5', '1', '5', '5', '0'],
            ]
        }

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        captured['rows'] = rows
        return len(rows)

    monkeypatch.setattr(okx, 'request_json', fake_request_json)
    monkeypatch.setattr(okx, 'upsert_source_rows', fake_upsert)

    processed = okx.fetch_buckets(db=object(), settings=_settings())

    assert request_params[0]['bar'] == '5m'
    assert processed == 1
    assert len(captured['rows']) == 1
    assert captured['rows'][0]['bucket_start_utc'] == start
    assert captured['rows'][0]['volume_btc'] == 8


def test_bitstamp_fetch_buckets_uses_native_10m_params(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc)
    captured = {}

    monkeypatch.setattr(bitstamp, 'get_ingestion_window', lambda db, source_name: (start, end))

    def fake_request_json(url, *, params=None, timeout=30, **kwargs):
        captured['params'] = params
        return {
            'data': {
                'ohlc': [
                    {
                        'timestamp': str(int(start.timestamp())),
                        'open': '88000',
                        'high': '88100',
                        'low': '87900',
                        'close': '88050',
                        'volume': '12.5',
                        'trades': '42',
                    }
                ]
            }
        }

    monkeypatch.setattr(bitstamp, 'request_json', fake_request_json)
    monkeypatch.setattr(bitstamp, 'upsert_source_rows', lambda db, source_name, table_name, rows, **kwargs: len(rows))

    processed = bitstamp.fetch_buckets(db=object(), settings=_settings())

    assert processed == 1
    assert captured['params']['step'] == 600
    assert captured['params']['exclude_current_candle'] == 'true'


def test_bitstamp_fetch_buckets_upserts_each_chunk(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    second_chunk = start + timedelta(minutes=10)
    end = start + timedelta(minutes=20)
    responses = [
        {
            'data': {
                'ohlc': [
                    {
                        'timestamp': str(int(start.timestamp())),
                        'open': '88000',
                        'high': '88100',
                        'low': '87900',
                        'close': '88050',
                        'volume': '12.5',
                        'trades': '42',
                    },
                    {
                        'timestamp': str(int(second_chunk.timestamp())),
                        'open': '88100',
                        'high': '88200',
                        'low': '88000',
                        'close': '88150',
                        'volume': '9',
                        'trades': '20',
                    },
                ]
            }
        },
        {
            'data': {
                'ohlc': [
                    {
                        'timestamp': str(int(second_chunk.timestamp())),
                        'open': '88100',
                        'high': '88200',
                        'low': '88000',
                        'close': '88150',
                        'volume': '9',
                        'trades': '20',
                    }
                ]
            }
        },
    ]
    upsert_calls = []

    monkeypatch.setattr(bitstamp, 'CHUNK_SIZE', timedelta(minutes=10))
    monkeypatch.setattr(bitstamp, 'get_ingestion_window', lambda db, source_name: (start, end))

    def fake_request_json(url, *, params=None, timeout=30, **kwargs):
        return responses.pop(0)

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        if not upsert_calls:
            assert len(responses) == 1
        upsert_calls.append(rows)
        return len(rows)

    monkeypatch.setattr(bitstamp, 'request_json', fake_request_json)
    monkeypatch.setattr(bitstamp, 'upsert_source_rows', fake_upsert)

    processed = bitstamp.fetch_buckets(db=object(), settings=_settings())

    assert processed == 2
    assert len(upsert_calls) == 2
    assert [row['bucket_start_utc'] for row in upsert_calls[0]] == [start]
    assert [row['bucket_start_utc'] for row in upsert_calls[1]] == [second_chunk]


def test_binance_fetch_buckets_uses_papi_5m_and_aggregates_complete_10m(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 20, 0, 20, tzinfo=timezone.utc)
    request = {}
    captured = {}

    monkeypatch.setattr(binance, 'get_ingestion_window', lambda db, source_name: (start, end))

    def fake_request_json(url, *, params=None, timeout=30, **kwargs):
        request['url'] = url
        request['params'] = params
        return [
            [int(start.timestamp() * 1000), '1', '4', '1', '2', '3', 0, '6', 10, '0', '0', '0'],
            [
                int((start + timedelta(minutes=5)).timestamp() * 1000),
                '2',
                '5',
                '2',
                '4',
                '5',
                0,
                '20',
                12,
                '0',
                '0',
                '0',
            ],
            [
                int((start + timedelta(minutes=10)).timestamp() * 1000),
                '4',
                '6',
                '4',
                '5',
                '1',
                0,
                '5',
                5,
                '0',
                '0',
                '0',
            ],
        ]

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        captured['source_name'] = source_name
        captured['table_name'] = table_name
        captured['rows'] = rows
        return len(rows)

    monkeypatch.setattr(binance, 'request_json', fake_request_json)
    monkeypatch.setattr(binance, 'upsert_source_rows', fake_upsert)
    monkeypatch.setattr(binance, '_get_binance_config', lambda db: {})

    processed = binance.fetch_buckets(db=object(), settings=_settings())

    assert 'api.binance.com/api/v3/klines' not in request['url']
    assert request['url'] == 'https://papi.binance.com/papi/v1/um/klines'
    assert request['params']['interval'] == '5m'
    assert processed == 1
    assert captured['source_name'] == 'binance'
    assert captured['table_name'] == 'binance_price_10m'
    assert captured['rows'][0]['bucket_start_utc'] == start
    assert captured['rows'][0]['close_price_usd'] == 4.0
    assert captured['rows'][0]['volume_btc'] == 8.0
    assert captured['rows'][0]['trade_count'] == 22


def test_binance_fetch_buckets_aggregates_and_upserts_each_chunk(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    second_chunk = start + timedelta(minutes=10)
    end = start + timedelta(minutes=20)
    responses = [
        [
            [int(start.timestamp() * 1000), '1', '4', '1', '2', '3', 0, '6', 10, '0', '0', '0'],
            [
                int((start + timedelta(minutes=5)).timestamp() * 1000),
                '2',
                '5',
                '2',
                '4',
                '5',
                0,
                '20',
                12,
                '0',
                '0',
                '0',
            ],
            [
                int(second_chunk.timestamp() * 1000),
                '9',
                '9',
                '9',
                '9',
                '9',
                0,
                '9',
                9,
                '0',
                '0',
                '0',
            ],
        ],
        [
            [int(second_chunk.timestamp() * 1000), '4', '6', '4', '5', '1', 0, '5', 5, '0', '0', '0'],
            [
                int((second_chunk + timedelta(minutes=5)).timestamp() * 1000),
                '5',
                '7',
                '5',
                '6',
                '2',
                0,
                '12',
                6,
                '0',
                '0',
                '0',
            ],
        ],
    ]
    upsert_calls = []

    monkeypatch.setattr(binance, 'CHUNK_SIZE', timedelta(minutes=10))
    monkeypatch.setattr(binance, 'get_ingestion_window', lambda db, source_name: (start, end))
    monkeypatch.setattr(binance, '_get_binance_config', lambda db: {})

    def fake_request_json(url, *, params=None, timeout=30, **kwargs):
        return responses.pop(0)

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        if not upsert_calls:
            assert len(responses) == 1
        upsert_calls.append(rows)
        return len(rows)

    monkeypatch.setattr(binance, 'request_json', fake_request_json)
    monkeypatch.setattr(binance, 'upsert_source_rows', fake_upsert)

    processed = binance.fetch_buckets(db=object(), settings=_settings())

    assert processed == 2
    assert len(upsert_calls) == 2
    assert [row['bucket_start_utc'] for row in upsert_calls[0]] == [start]
    assert upsert_calls[0][0]['close_price_usd'] == 4.0
    assert [row['bucket_start_utc'] for row in upsert_calls[1]] == [second_chunk]
    assert upsert_calls[1][0]['close_price_usd'] == 6.0


def test_binance_fetch_buckets_reads_registry_config(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc)
    request = {}
    captured = {}

    monkeypatch.setattr(binance, 'get_ingestion_window', lambda db, source_name: (start, end))
    monkeypatch.setattr(
        binance,
        '_get_binance_config',
        lambda db: {
            'mode': 'local',
            'api_base': 'https://example-binance.test',
            'endpoint': '/custom/klines',
            'market_type': 'custom_market',
        },
    )

    def fake_request_json(url, *, params=None, timeout=30, **kwargs):
        request['url'] = url
        return [
            [int(start.timestamp() * 1000), '1', '4', '1', '2', '3', 0, '6', 10, '0', '0', '0'],
            [
                int((start + timedelta(minutes=5)).timestamp() * 1000),
                '2',
                '5',
                '2',
                '4',
                '5',
                0,
                '20',
                12,
                '0',
                '0',
                '0',
            ],
        ]

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        captured['rows'] = rows
        return len(rows)

    monkeypatch.setattr(binance, 'request_json', fake_request_json)
    monkeypatch.setattr(binance, 'upsert_source_rows', fake_upsert)

    processed = binance.fetch_buckets(db=object(), settings=_settings())

    assert processed == 1
    assert request['url'] == 'https://example-binance.test/custom/klines'
    raw_payload = captured['rows'][0]['raw_payload']['candles'][0]['kline']
    first_child_payload = captured['rows'][0]['raw_payload']['candles'][0]
    assert raw_payload[0] == int(start.timestamp() * 1000)
    assert first_child_payload['api_base'] == 'https://example-binance.test'
    assert first_child_payload['endpoint'] == '/custom/klines'
    assert first_child_payload['market_type'] == 'custom_market'


def test_binance_source_level_remote_mode_skips_request(monkeypatch):
    monkeypatch.setattr(binance, '_get_binance_config', lambda db: {'mode': 'remote'})
    monkeypatch.setattr(binance, 'request_json', lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError))

    assert binance.fetch_buckets(db=object(), settings=_settings()) == 0


def test_kraken_recent_only_does_not_advance_checkpoint_over_missing_history(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 20, 1, 0, tzinfo=timezone.utc)
    captured = {}

    monkeypatch.setattr(kraken, 'get_ingestion_window', lambda db, source_name: (start, end))
    monkeypatch.setattr(
        kraken,
        'request_json',
        lambda url, *, params=None, timeout=30, **kwargs: {
            'result': {
                'XXBTZUSD': [
                    [str(int((start + timedelta(minutes=30)).timestamp())), '1', '4', '1', '2', '0', '3', '10'],
                    [str(int((start + timedelta(minutes=35)).timestamp())), '2', '5', '2', '4', '0', '5', '12'],
                    [str(int((start + timedelta(minutes=40)).timestamp())), '4', '6', '4', '5', '0', '1', '5'],
                ]
            }
        },
    )

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        captured['advance_checkpoint'] = kwargs.get('advance_checkpoint')
        captured['rows'] = rows
        return len(rows)

    monkeypatch.setattr(kraken, 'upsert_source_rows', fake_upsert)

    processed = kraken.fetch_buckets(db=object(), settings=_settings())

    assert processed == 1
    assert captured['advance_checkpoint'] is False
    assert captured['rows'][0]['bucket_start_utc'] == start + timedelta(minutes=30)


def test_gemini_recent_only_does_not_advance_checkpoint_over_missing_history(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 20, 1, 0, tzinfo=timezone.utc)
    captured = {}

    monkeypatch.setattr(gemini, 'get_ingestion_window', lambda db, source_name: (start, end))
    monkeypatch.setattr(
        gemini,
        'request_json',
        lambda url, *, timeout=30, **kwargs: [
            [int((start + timedelta(minutes=30)).timestamp() * 1000), '1', '4', '1', '2', '3'],
            [int((start + timedelta(minutes=35)).timestamp() * 1000), '2', '5', '2', '4', '5'],
            [int((start + timedelta(minutes=40)).timestamp() * 1000), '4', '6', '4', '5', '1'],
        ],
    )

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        captured['advance_checkpoint'] = kwargs.get('advance_checkpoint')
        captured['rows'] = rows
        return len(rows)

    monkeypatch.setattr(gemini, 'upsert_source_rows', fake_upsert)

    processed = gemini.fetch_buckets(db=object(), settings=_settings())

    assert processed == 1
    assert captured['advance_checkpoint'] is False
    assert captured['rows'][0]['bucket_start_utc'] == start + timedelta(minutes=30)


def test_coinbase_empty_chunk_does_not_stop_historical_loop(monkeypatch):
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=1, minutes=10)
    responses = [
        [],
        [
            [int((start + timedelta(days=1)).timestamp()), 10, 14, 11, 13, 2],
            [int((start + timedelta(days=1, minutes=5)).timestamp()), 11, 15, 12, 14, 3],
        ],
    ]
    captured = {}

    monkeypatch.setattr(coinbase, 'get_ingestion_window', lambda db, source_name: (start, end))
    monkeypatch.setattr(
        coinbase,
        'request_json',
        lambda url, *, params=None, timeout=30, **kwargs: responses.pop(0),
    )

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        captured['rows'] = rows
        return len(rows)

    monkeypatch.setattr(coinbase, 'upsert_source_rows', fake_upsert)

    processed = coinbase.fetch_buckets(db=object(), settings=_settings())

    assert processed == 1
    assert captured['rows'][0]['bucket_start_utc'] == start + timedelta(days=1)
    assert captured['rows'][0]['volume_btc'] == 5


def test_runtime_imports_source_via_registry_module_path(monkeypatch):
    source = PriceSourceSpec(
        source_name='test_source',
        table_name='test_source_price_10m',
        priority_rank=1,
        notes='test',
        asset_symbol='BTCUSD',
        module_path='mqnode.market.price.sources.test_source',
        is_optional=True,
    )
    imported = {}

    monkeypatch.setattr(runtime, 'DB', lambda settings: _DummyDB())
    monkeypatch.setattr(runtime, 'get_price_source', lambda source_name, cur=None: source)
    monkeypatch.setattr(runtime, 'get_enabled_price_sources', lambda cur=None: (source,))

    class _FakeModule:
        @staticmethod
        def fetch_buckets(db=None, settings=None):
            return 7

    def fake_import_module(module_path):
        imported['path'] = module_path
        return _FakeModule

    monkeypatch.setattr(runtime, 'import_module', fake_import_module)

    processed = runtime.run_price_source_ingestion_once('test_source', settings=SimpleNamespace())

    assert processed == 7
    assert imported['path'] == 'mqnode.market.price.sources.test_source'


def test_runtime_price_sources_loop_runs_batch_and_sleeps(monkeypatch):
    class StopLoop(Exception):
        pass

    settings = SimpleNamespace(log_level='INFO', price_source_ingestion_sleep_seconds=17)
    calls = []
    sleeps = []

    def fake_run_enabled_price_sources_once(settings=None):
        calls.append(settings)
        return {'binance': 3}

    def fake_sleep(seconds):
        sleeps.append(seconds)
        raise StopLoop

    monkeypatch.setattr(runtime, 'configure_logging', lambda log_level: None)
    monkeypatch.setattr(runtime, 'run_enabled_price_sources_once', fake_run_enabled_price_sources_once)
    monkeypatch.setattr(runtime.time, 'sleep', fake_sleep)

    with pytest.raises(StopLoop):
        runtime.run_price_sources_loop(settings=settings)

    assert calls == [settings]
    assert sleeps == [17]


def test_runtime_cli_all_enabled_without_once_routes_to_source_loop(monkeypatch):
    settings = SimpleNamespace(log_level='INFO')
    called = {}

    def fake_run_price_sources_loop(loop_settings):
        called['settings'] = loop_settings

    monkeypatch.setattr(sys, 'argv', ['runtime', 'ingest-source', '--all-enabled'])
    monkeypatch.setattr(runtime, 'get_settings', lambda: settings)
    monkeypatch.setattr(runtime, 'configure_logging', lambda log_level: None)
    monkeypatch.setattr(runtime, 'run_price_sources_loop', fake_run_price_sources_loop)
    monkeypatch.setattr(
        runtime,
        'run_enabled_price_sources_once',
        lambda settings=None: (_ for _ in ()).throw(AssertionError('should not run once')),
    )

    runtime.main()

    assert called['settings'] is settings


def test_runtime_cli_all_enabled_once_still_runs_one_batch(monkeypatch):
    settings = SimpleNamespace(log_level='INFO')
    called = {}

    def fake_run_enabled_price_sources_once(settings=None):
        called['settings'] = settings
        return {'binance': 3}

    monkeypatch.setattr(sys, 'argv', ['runtime', 'ingest-source', '--all-enabled', '--once'])
    monkeypatch.setattr(runtime, 'get_settings', lambda: settings)
    monkeypatch.setattr(runtime, 'configure_logging', lambda log_level: None)
    monkeypatch.setattr(runtime, 'run_enabled_price_sources_once', fake_run_enabled_price_sources_once)
    monkeypatch.setattr(
        runtime,
        'run_price_sources_loop',
        lambda settings=None: (_ for _ in ()).throw(AssertionError('should not loop')),
    )

    runtime.main()

    assert called['settings'] is settings


def test_runtime_cli_source_once_still_runs_single_source_once(monkeypatch):
    settings = SimpleNamespace(log_level='INFO')
    called = {}

    def fake_run_price_source_ingestion_once(source_name, settings=None):
        called['source_name'] = source_name
        called['settings'] = settings
        return 5

    monkeypatch.setattr(sys, 'argv', ['runtime', 'ingest-source', '--source', 'binance', '--once'])
    monkeypatch.setattr(runtime, 'get_settings', lambda: settings)
    monkeypatch.setattr(runtime, 'configure_logging', lambda log_level: None)
    monkeypatch.setattr(runtime, 'run_price_source_ingestion_once', fake_run_price_source_ingestion_once)
    monkeypatch.setattr(
        runtime,
        'run_single_price_source_loop',
        lambda source_name, settings=None: (_ for _ in ()).throw(AssertionError('should not loop')),
    )

    runtime.main()

    assert called == {'source_name': 'binance', 'settings': settings}


def test_runtime_cli_source_without_once_routes_to_single_source_loop(monkeypatch):
    settings = SimpleNamespace(log_level='INFO')
    called = {}

    def fake_run_single_price_source_loop(source_name, loop_settings):
        called['source_name'] = source_name
        called['settings'] = loop_settings

    monkeypatch.setattr(sys, 'argv', ['runtime', 'ingest-source', '--source', 'binance'])
    monkeypatch.setattr(runtime, 'get_settings', lambda: settings)
    monkeypatch.setattr(runtime, 'configure_logging', lambda log_level: None)
    monkeypatch.setattr(runtime, 'run_single_price_source_loop', fake_run_single_price_source_loop)
    monkeypatch.setattr(
        runtime,
        'run_price_source_ingestion_once',
        lambda source_name, settings=None: (_ for _ in ()).throw(AssertionError('should not run once')),
    )

    runtime.main()

    assert called == {'source_name': 'binance', 'settings': settings}


def test_runtime_skips_disabled_registry_source(monkeypatch):
    source = PriceSourceSpec(
        source_name='gemini',
        table_name='gemini_price_10m',
        priority_rank=8,
        notes='test',
        asset_symbol='BTCUSD',
        module_path='mqnode.market.price.sources.gemini',
        default_enabled=False,
        is_optional=True,
    )

    monkeypatch.setattr(runtime, 'DB', lambda settings: _DummyDB())
    monkeypatch.setattr(runtime, 'get_price_source', lambda source_name, cur=None: source)
    monkeypatch.setattr(runtime, 'get_enabled_price_sources', lambda cur=None: ())
    monkeypatch.setattr(runtime, 'import_module', _should_not_import)

    processed = runtime.run_price_source_ingestion_once('gemini', settings=SimpleNamespace())

    assert processed == 0


def test_runtime_skips_binance_remote_mode(monkeypatch):
    source = PriceSourceSpec(
        source_name='binance',
        table_name='binance_price_10m',
        priority_rank=3,
        notes='test',
        asset_symbol='BTCUSDT',
        module_path='mqnode.market.price.sources.binance',
        is_optional=True,
        config={'mode': 'remote'},
    )

    monkeypatch.setattr(runtime, 'DB', lambda settings: _DummyDB())
    monkeypatch.setattr(runtime, 'get_price_source', lambda source_name, cur=None: source)
    monkeypatch.setattr(runtime, 'get_enabled_price_sources', lambda cur=None: (source,))
    monkeypatch.setattr(runtime, 'import_module', _should_not_import)

    processed = runtime.run_price_source_ingestion_once('binance', settings=SimpleNamespace())

    assert processed == 0


def test_remote_binance_ingest_validates_and_upserts_without_checkpoint(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    source_updated_at = datetime(2026, 4, 20, 0, 11, tzinfo=timezone.utc)
    exchange_close_time = datetime(2026, 4, 20, 0, 9, 59, tzinfo=timezone.utc)
    cloud_fetched_at = datetime(2026, 4, 20, 0, 10, 5, tzinfo=timezone.utc)
    cloud_sent_at = datetime(2026, 4, 20, 0, 10, 8, tzinfo=timezone.utc)
    captured = {}

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        captured['source_name'] = source_name
        captured['table_name'] = table_name
        captured['rows'] = rows
        captured['advance_checkpoint'] = kwargs.get('advance_checkpoint')
        return len(rows)

    monkeypatch.setattr('mqnode.market.price.remote_ingest.upsert_source_rows', fake_upsert)

    processed = ingest_remote_binance_rows(
        object(),
        [
            {
                'source_name': 'binance',
                'bucket_start_utc': bucket,
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'volume_btc': 2.5,
                'volume_usd': 220125,
                'trade_count': 42,
                'source_updated_at': source_updated_at,
                'exchange_close_time': exchange_close_time,
                'cloud_fetched_at': cloud_fetched_at,
                'cloud_sent_at': cloud_sent_at,
            }
        ],
    )

    assert processed == 1
    assert captured['source_name'] == 'binance'
    assert captured['table_name'] == 'binance_price_10m'
    assert captured['advance_checkpoint'] is False
    normalized_row = captured['rows'][0]
    raw_payload = normalized_row['raw_payload']
    assert normalized_row['source_updated_at'] == source_updated_at
    assert raw_payload['fetch_mode'] == 'remote_cloud_api'
    assert raw_payload['market_type'] == 'spot'
    assert raw_payload['exchange_close_time'] == exchange_close_time.isoformat()
    assert raw_payload['cloud_fetched_at'] == cloud_fetched_at.isoformat()
    assert raw_payload['cloud_sent_at'] == cloud_sent_at.isoformat()
    assert raw_payload['local_received_at'] is not None
    assert raw_payload['remote_payload']['source_updated_at'] == source_updated_at.isoformat()


def test_remote_binance_ingest_accepts_optional_ws_quality_metadata(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    ws_closed_at = datetime(2026, 4, 20, 0, 10, 5, tzinfo=timezone.utc)
    captured = {}

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _DB:
        def cursor(self):
            return _Cursor()

    class _Repository:
        def mark_source_quality(self, cur, table_name, bucket_start_utc, metadata):
            captured['quality'] = {
                'table_name': table_name,
                'bucket_start_utc': bucket_start_utc,
                'metadata': metadata,
            }

    def fake_upsert(db, source_name, table_name, rows, **kwargs):
        captured['rows'] = rows
        return len(rows)

    monkeypatch.setattr('mqnode.market.price.remote_ingest.upsert_source_rows', fake_upsert)

    processed = ingest_remote_binance_rows(
        _DB(),
        [
            {
                'source_name': 'binance',
                'bucket_start_utc': bucket,
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
                'source_updated_at': ws_closed_at,
                'data_source_mode': 'websocket_confirmed_1m_cloud',
                'quality_status': 'ws_closed',
                'ws_closed_at': ws_closed_at,
                'first_received_at': ws_closed_at - timedelta(seconds=9),
                'last_received_at': ws_closed_at,
                'ws_disconnect_count': 0,
                'ws_gap_count': 0,
                'expected_child_candle_count': 10,
                'actual_child_candle_count': 10,
                'raw_payload': {'aggregation': 'ws_confirmed_1m_to_10m'},
            }
        ],
        repository=_Repository(),
    )

    assert processed == 1
    normalized = captured['rows'][0]
    assert normalized['quality_status'] == 'ws_closed'
    assert normalized['data_source_mode'] == 'websocket_confirmed_1m_cloud'
    assert normalized['expected_child_candle_count'] == 10
    assert normalized['actual_child_candle_count'] == 10
    assert normalized['raw_payload']['collector_raw_payload'] == {'aggregation': 'ws_confirmed_1m_to_10m'}
    assert captured['quality']['table_name'] == 'binance_price_10m'
    assert captured['quality']['metadata']['quality_status'] == 'ws_closed'


def test_remote_binance_ingest_rejects_bad_source_ohlc_and_alignment():
    bucket = datetime(2026, 4, 20, 0, 5, tzinfo=timezone.utc)
    row = {
        'source_name': 'binance',
        'bucket_start_utc': bucket,
        'open_price_usd': 88000,
        'high_price_usd': 87900,
        'low_price_usd': 87800,
        'close_price_usd': 88050,
    }

    try:
        ingest_remote_binance_rows(object(), [row])
    except ValueError as exc:
        assert '10m boundary' in str(exc)
    else:
        raise AssertionError('expected invalid alignment to raise')

    row['bucket_start_utc'] = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    try:
        ingest_remote_binance_rows(object(), [row])
    except ValueError as exc:
        assert 'high_price_usd' in str(exc)
    else:
        raise AssertionError('expected invalid OHLC to raise')


def test_internal_binance_route_rejects_missing_or_invalid_token(monkeypatch):
    app = FastAPI()
    app.include_router(internal_price_ingest.router)
    client = TestClient(app)
    payload = {
        'rows': [
            {
                'bucket_start_utc': '2026-04-20T00:00:00Z',
                'open_price_usd': 88000,
                'high_price_usd': 88100,
                'low_price_usd': 87900,
                'close_price_usd': 88050,
            }
        ]
    }

    monkeypatch.setattr(internal_price_ingest, 'get_settings', lambda: SimpleNamespace(internal_ingest_token=None))
    assert client.post('/api/v1/internal/price/source/binance/10m', json=payload).status_code == 403

    monkeypatch.setattr(internal_price_ingest, 'get_settings', lambda: SimpleNamespace(internal_ingest_token='secret'))
    assert client.post('/api/v1/internal/price/source/binance/10m', json=payload).status_code == 401
    response = client.post(
        '/api/v1/internal/price/source/binance/10m',
        headers={'Authorization': 'Bearer wrong'},
        json=payload,
    )
    assert response.status_code == 403


def test_internal_binance_route_accepts_valid_token(monkeypatch):
    app = FastAPI()
    app.include_router(internal_price_ingest.router)
    client = TestClient(app)
    captured = {}

    monkeypatch.setattr(internal_price_ingest, 'get_settings', lambda: SimpleNamespace(internal_ingest_token='secret'))
    monkeypatch.setattr(internal_price_ingest, 'DB', lambda settings: object())

    def fake_ingest(db, rows, *, advance_checkpoint=False):
        captured['rows'] = rows
        captured['advance_checkpoint'] = advance_checkpoint
        return len(rows)

    monkeypatch.setattr(internal_price_ingest, 'ingest_remote_binance_rows', fake_ingest)

    response = client.post(
        '/api/v1/internal/price/source/binance/10m',
        headers={'Authorization': 'Bearer secret'},
        json={
            'rows': [
                {
                    'bucket_start_utc': '2026-04-20T00:00:00Z',
                    'open_price_usd': 88000,
                    'high_price_usd': 88100,
                    'low_price_usd': 87900,
                    'close_price_usd': 88050,
                    'source_updated_at': '2026-04-20T00:11:00Z',
                    'exchange_close_time': '2026-04-20T00:09:59Z',
                    'cloud_fetched_at': '2026-04-20T00:10:05Z',
                    'cloud_sent_at': '2026-04-20T00:10:08Z',
                    'data_source_mode': 'websocket_confirmed_1m_cloud',
                    'quality_status': 'ws_closed',
                    'ws_closed_at': '2026-04-20T00:10:05Z',
                    'expected_child_candle_count': 10,
                    'actual_child_candle_count': 10,
                    'raw_payload': {'aggregation': 'ws_confirmed_1m_to_10m'},
                }
            ]
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        'ok': True,
        'source': 'binance',
        'rows_received': 1,
        'rows_upserted': 1,
    }
    assert captured['advance_checkpoint'] is False
    assert captured['rows'][0]['source_updated_at'] == datetime(2026, 4, 20, 0, 11, tzinfo=timezone.utc)
    assert captured['rows'][0]['cloud_sent_at'] == datetime(2026, 4, 20, 0, 10, 8, tzinfo=timezone.utc)
    assert captured['rows'][0]['quality_status'] == 'ws_closed'
    assert captured['rows'][0]['data_source_mode'] == 'websocket_confirmed_1m_cloud'
    assert captured['rows'][0]['raw_payload'] == {'aggregation': 'ws_confirmed_1m_to_10m'}
