from __future__ import annotations

from mqnode.market.price.ws.sources.binance import BinanceWsSource


class BinanceRemoteWsSource(BinanceWsSource):
    source_name = 'binance'


def build_connector() -> BinanceRemoteWsSource:
    return BinanceRemoteWsSource()
