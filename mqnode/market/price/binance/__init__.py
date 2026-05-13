from __future__ import annotations

from mqnode.market.price.binance.rest import BinanceRestBucketFetcher, build_rest_fetcher, fetch_buckets
from mqnode.market.price.binance.ws import BinanceWsSource, build_connector

SOURCE_NAME = 'binance'
DEFAULT_SYMBOL = 'BTCUSDT'
build_ws_connector = build_connector

__all__ = [
    'SOURCE_NAME',
    'DEFAULT_SYMBOL',
    'BinanceRestBucketFetcher',
    'BinanceWsSource',
    'build_connector',
    'build_rest_fetcher',
    'build_ws_connector',
    'fetch_buckets',
]
