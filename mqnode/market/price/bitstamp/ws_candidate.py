from __future__ import annotations

from mqnode.market.price.ws.sources.base import NotImplementedPriceWsSource


class BitstampWsSource(NotImplementedPriceWsSource):
    source_name = 'bitstamp'


def build_connector() -> BitstampWsSource:
    return BitstampWsSource()
