from __future__ import annotations

from mqnode.market.price.ws.sources.base import NotImplementedPriceWsSource


class KrakenWsSource(NotImplementedPriceWsSource):
    source_name = 'kraken'


def build_connector() -> KrakenWsSource:
    return KrakenWsSource()
