from __future__ import annotations

from mqnode.market.price.source_worker import SourcePriceWorker

SOURCE_NAME = 'coinbase'


def build_worker(**kwargs) -> SourcePriceWorker:
    return SourcePriceWorker(SOURCE_NAME, **kwargs)


def run_once(**kwargs):
    return build_worker(**kwargs).run_once()
