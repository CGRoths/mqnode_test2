from __future__ import annotations

from types import SimpleNamespace

import pytest

from mqnode.market.price.ws.runtime import LOCAL_DB_OUTPUT_MODE, REMOTE_PUSH_OUTPUT_MODE, _validate_lifecycle_ws_runtime


def _settings(**overrides):
    values = {
        'price_ws_bucket_grace_seconds': 5,
        'price_ws_rest_confirm_delay_seconds': 30,
        'price_ws_child_interval': '1m',
        'price_ws_remote_ingest_url': None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(('source_name', 'symbol'), [('bybit', 'BTCUSDT'), ('okx', 'BTC-USDT')])
def test_confirmed_candle_sources_are_allowed(source_name, symbol):
    profile = _validate_lifecycle_ws_runtime(
        source_name=source_name,
        symbol=symbol,
        settings=_settings(),
        output_mode=LOCAL_DB_OUTPUT_MODE,
    )

    assert profile.source_name == source_name
    assert profile.can_write_ws_closed is True


@pytest.mark.parametrize(
    ('source_name', 'symbol'),
    [('coinbase', 'BTC-USD'), ('kraken', 'BTC/USD'), ('bitstamp', 'btcusd')],
)
def test_candidate_sources_are_refused_by_default(source_name, symbol):
    with pytest.raises(RuntimeError, match='not official ws_closed mode'):
        _validate_lifecycle_ws_runtime(
            source_name=source_name,
            symbol=symbol,
            settings=_settings(),
            output_mode=LOCAL_DB_OUTPUT_MODE,
        )


@pytest.mark.parametrize(
    ('source_name', 'symbol'),
    [('coinbase', 'BTC-USD'), ('kraken', 'BTC/USD'), ('bitstamp', 'btcusd')],
)
def test_candidate_sources_can_be_explicitly_bypassed(source_name, symbol):
    profile = _validate_lifecycle_ws_runtime(
        source_name=source_name,
        symbol=symbol,
        settings=_settings(),
        output_mode=LOCAL_DB_OUTPUT_MODE,
        candidate=True,
    )

    assert profile.source_name == source_name
    assert profile.can_write_ws_closed is False


def test_binance_local_official_runtime_is_refused():
    with pytest.raises(RuntimeError, match='requires PRICE_WS_OUTPUT_MODE=remote_push'):
        _validate_lifecycle_ws_runtime(
            source_name='binance',
            symbol='BTCUSDT',
            settings=_settings(),
            output_mode=LOCAL_DB_OUTPUT_MODE,
        )


def test_binance_cloud_remote_push_runtime_is_allowed():
    profile = _validate_lifecycle_ws_runtime(
        source_name='binance',
        symbol='BTCUSDT',
        settings=_settings(price_ws_remote_ingest_url='https://mqnode.example/ingest'),
        output_mode=REMOTE_PUSH_OUTPUT_MODE,
    )

    assert profile.source_name == 'binance'
    assert profile.remote_push_enabled is True
