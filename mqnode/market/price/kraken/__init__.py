from __future__ import annotations

from mqnode.market.price.kraken.rest import build_rest_fetcher, fetch_buckets
from mqnode.market.price.kraken.ws_candidate import KrakenWsSource, build_connector

SOURCE_NAME = 'kraken'
DEFAULT_SYMBOL = 'BTC/USD'
build_ws_connector = build_connector

__all__ = [
    'SOURCE_NAME',
    'DEFAULT_SYMBOL',
    'KrakenWsSource',
    'build_connector',
    'build_rest_fetcher',
    'build_ws_connector',
    'fetch_buckets',
]
