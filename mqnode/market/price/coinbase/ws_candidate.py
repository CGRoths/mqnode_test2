from __future__ import annotations

from mqnode.market.price.ws.sources.base import NotImplementedPriceWsSource


class CoinbaseWsSource(NotImplementedPriceWsSource):
    source_name = 'coinbase'


def build_connector() -> CoinbaseWsSource:
    return CoinbaseWsSource()
