from __future__ import annotations

import pytest

from mqnode.market.price.lifecycle.models import (
    CLOUD_WS_REMOTE_PUSH,
    WS_CANDIDATE_REST_FINAL,
    WS_CONFIRMED_CANDLE,
)
from mqnode.market.price.lifecycle.profiles import get_price_source_lifecycle_profile


def test_bybit_profile_is_confirmed_candle_ws():
    profile = get_price_source_lifecycle_profile('bybit')

    assert profile.live_mode == WS_CONFIRMED_CANDLE
    assert profile.can_write_ws_closed is True
    assert profile.symbol == 'BTCUSDT'


def test_okx_profile_is_confirmed_candle_ws():
    profile = get_price_source_lifecycle_profile('okx')

    assert profile.live_mode == WS_CONFIRMED_CANDLE
    assert profile.can_write_ws_closed is True
    assert profile.symbol == 'BTC-USDT'


def test_binance_profile_waits_for_cloud_remote_push():
    profile = get_price_source_lifecycle_profile('binance')

    assert profile.live_mode == CLOUD_WS_REMOTE_PUSH
    assert profile.can_write_ws_closed is True
    assert profile.remote_push_enabled is True


@pytest.mark.parametrize('source_name', ['coinbase', 'kraken', 'bitstamp'])
def test_candidate_sources_cannot_write_ws_closed(source_name):
    profile = get_price_source_lifecycle_profile(source_name)

    assert profile.live_mode == WS_CANDIDATE_REST_FINAL
    assert profile.can_write_ws_closed is False


def test_unknown_source_raises_clear_error():
    with pytest.raises(NotImplementedError, match='No lifecycle profile configured'):
        get_price_source_lifecycle_profile('unknown_exchange')
