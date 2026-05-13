from __future__ import annotations

from mqnode.market.price.sources.coinbase import (
    fetch_buckets,
)


def build_rest_fetcher(*, timeout_seconds: int = 30):
    raise NotImplementedError('Coinbase WS REST confirmation fetcher is not implemented.')


def run_rest_once(db=None, settings=None) -> int:
    return fetch_buckets(db=db, settings=settings)
