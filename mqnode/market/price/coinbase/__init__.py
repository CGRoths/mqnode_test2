from __future__ import annotations

from mqnode.market.price.coinbase.rest import build_rest_fetcher, fetch_buckets
from mqnode.market.price.coinbase.ws_candidate import CoinbaseWsSource, build_connector

SOURCE_NAME = 'coinbase'
DEFAULT_SYMBOL = 'BTC-USD'
build_ws_connector = build_connector

__all__ = [
    'SOURCE_NAME',
    'DEFAULT_SYMBOL',
    'CoinbaseWsSource',
    'build_connector',
    'build_rest_fetcher',
    'build_ws_connector',
    'fetch_buckets',
]
