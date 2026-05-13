from __future__ import annotations

from mqnode.market.price.binance.rest import BinanceRestBucketFetcher
from mqnode.market.price.bybit.rest import BybitRestBucketFetcher
from mqnode.market.price.bybit.ws import BybitWsSource, build_connector
from mqnode.market.price.okx.rest import OkxRestBucketFetcher
from mqnode.market.price.ws.gap_filler import get_rest_bucket_fetcher
from mqnode.market.price.ws.sources.bybit import BybitWsSource as LegacyBybitWsSource
from mqnode.market.price.ws.sources.bybit import build_connector as legacy_build_connector


def test_source_packages_expose_official_fetchers_and_connectors():
    assert BybitRestBucketFetcher.__name__ == 'BybitRestBucketFetcher'
    assert OkxRestBucketFetcher.__name__ == 'OkxRestBucketFetcher'
    assert BinanceRestBucketFetcher.__name__ == 'BinanceRestBucketFetcher'
    assert build_connector().__class__ is BybitWsSource


def test_legacy_bybit_ws_import_reexports_new_package():
    assert LegacyBybitWsSource is BybitWsSource
    assert legacy_build_connector().__class__ is BybitWsSource


def test_rest_bucket_fetcher_factory_returns_source_package_fetchers():
    assert isinstance(get_rest_bucket_fetcher('bybit'), BybitRestBucketFetcher)
    assert isinstance(get_rest_bucket_fetcher('okx'), OkxRestBucketFetcher)
    assert isinstance(get_rest_bucket_fetcher('binance'), BinanceRestBucketFetcher)
