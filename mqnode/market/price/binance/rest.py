from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from mqnode.market.price.normalize import normalize_ohlcv_bucket
from mqnode.market.price.source_support import request_json as default_request_json
from mqnode.market.price.sources.binance import (
    fetch_buckets,
)
from mqnode.market.price.ws.models import ensure_utc


class BinanceRestBucketFetcher:
    api_url = 'https://api.binance.com/api/v3/klines'

    def __init__(
        self,
        *,
        timeout_seconds: int = 30,
        request_json_func: Callable | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.request_json = request_json_func or default_request_json

    def fetch_bucket(self, source_name: str, symbol: str, bucket_start_utc: datetime) -> dict | None:
        bucket_start_utc = ensure_utc(bucket_start_utc)
        bucket_end_utc = bucket_start_utc + timedelta(minutes=10)
        candles = self.request_json(
            self.api_url,
            params={
                'symbol': symbol,
                'interval': '10m',
                'startTime': int(bucket_start_utc.timestamp() * 1000),
                'endTime': int(bucket_end_utc.timestamp() * 1000),
                'limit': 1,
            },
            timeout=self.timeout_seconds,
            max_attempts=1,
        )
        for candle in sorted(candles or [], key=lambda item: int(item[0])):
            candle_start = datetime.fromtimestamp(int(candle[0]) / 1000, tz=timezone.utc)
            if candle_start != bucket_start_utc:
                continue
            return normalize_ohlcv_bucket(
                source_name,
                bucket_start_utc,
                symbol=symbol,
                open_price_usd=float(candle[1]),
                high_price_usd=float(candle[2]),
                low_price_usd=float(candle[3]),
                close_price_usd=float(candle[4]),
                volume_btc=float(candle[5]),
                volume_usd=float(candle[7]),
                trade_count=int(candle[8]),
                raw_payload={'rest_fallback': 'binance_klines_10m', 'kline': candle},
                source_updated_at=datetime.fromtimestamp(int(candle[6]) / 1000, tz=timezone.utc),
                normalize_bucket_start=False,
            )
        return None


def build_rest_fetcher(*, timeout_seconds: int = 30) -> BinanceRestBucketFetcher:
    return BinanceRestBucketFetcher(timeout_seconds=timeout_seconds)


def run_rest_once(db=None, settings=None) -> int:
    return fetch_buckets(db=db, settings=settings)
