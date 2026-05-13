from __future__ import annotations

from mqnode.market.price.bitstamp.rest import build_rest_fetcher, fetch_buckets
from mqnode.market.price.bitstamp.ws_candidate import BitstampWsSource, build_connector

SOURCE_NAME = 'bitstamp'
DEFAULT_SYMBOL = 'BTCUSD'
build_ws_connector = build_connector

__all__ = [
    'SOURCE_NAME',
    'DEFAULT_SYMBOL',
    'BitstampWsSource',
    'build_connector',
    'build_rest_fetcher',
    'build_ws_connector',
    'fetch_buckets',
]
