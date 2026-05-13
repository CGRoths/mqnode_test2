from __future__ import annotations

from mqnode.market.price.okx.rest import OkxRestBucketFetcher, build_rest_fetcher, fetch_buckets
from mqnode.market.price.okx.ws import OkxWsSource, build_connector

SOURCE_NAME = 'okx'
DEFAULT_SYMBOL = 'BTC-USDT'
build_ws_connector = build_connector

__all__ = [
    'SOURCE_NAME',
    'DEFAULT_SYMBOL',
    'OkxRestBucketFetcher',
    'OkxWsSource',
    'build_connector',
    'build_rest_fetcher',
    'build_ws_connector',
    'fetch_buckets',
]
