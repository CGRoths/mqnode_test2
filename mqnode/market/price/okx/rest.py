from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from mqnode.core.utils import utc_now
from mqnode.market.price.normalize import normalize_ohlcv_bucket
from mqnode.market.price.source_support import request_json as default_request_json
from mqnode.market.price.sources.okx import (
    fetch_buckets,
)
from mqnode.market.price.ws.models import ensure_utc


class OkxRestBucketFetcher:
    api_url = 'https://www.okx.com/api/v5/market/candles'

    def __init__(
        self,
        *,
        timeout_seconds: int = 30,
        request_json_func: Callable | None = None,
        utc_now_func: Callable | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.request_json = request_json_func or default_request_json
        self.utc_now = utc_now_func or utc_now

    def fetch_bucket(self, source_name: str, symbol: str, bucket_start_utc: datetime) -> dict | None:
        bucket_start_utc = ensure_utc(bucket_start_utc)
        bucket_end_utc = bucket_start_utc + timedelta(minutes=10)
        payload = self.request_json(
            self.api_url,
            params={
                'instId': symbol,
                'bar': '1m',
                'after': str(int(bucket_start_utc.timestamp() * 1000)),
                'before': str(int(bucket_end_utc.timestamp() * 1000)),
                'limit': 100,
            },
            timeout=self.timeout_seconds,
            max_attempts=1,
        )
        child_rows = self._normalize_child_rows(source_name, symbol, bucket_start_utc, bucket_end_utc, payload)
        expected_buckets = tuple(bucket_start_utc + timedelta(minutes=offset) for offset in range(10))
        child_by_bucket = {row['bucket_start_utc']: row for row in child_rows}
        if tuple(sorted(child_by_bucket)) != expected_buckets:
            return None

        children = [child_by_bucket[bucket] for bucket in expected_buckets]
        volume_btc = sum(float(child['volume_btc'] or 0) for child in children)
        volume_usd = sum(float(child['volume_usd'] or 0) for child in children)
        return normalize_ohlcv_bucket(
            source_name,
            bucket_start_utc,
            symbol=symbol,
            open_price_usd=children[0]['open_price_usd'],
            high_price_usd=max(float(child['high_price_usd']) for child in children),
            low_price_usd=min(float(child['low_price_usd']) for child in children),
            close_price_usd=children[-1]['close_price_usd'],
            volume_btc=volume_btc if volume_btc > 0 else None,
            volume_usd=volume_usd if volume_usd > 0 else None,
            trade_count=None,
            raw_payload={
                'rest_fallback': 'okx_candles_1m_to_10m',
                'child_interval': '1m',
                'candles': [child['raw_payload'] for child in children],
            },
            source_updated_at=self.utc_now(),
        )

    def _normalize_child_rows(
        self,
        source_name: str,
        symbol: str,
        bucket_start_utc: datetime,
        bucket_end_utc: datetime,
        payload: dict,
    ) -> list[dict]:
        rows = []
        candles = sorted(payload.get('data', []), key=lambda item: int(item[0]))
        for candle in candles:
            if len(candle) > 8 and str(candle[8]) == '0':
                continue
            child_start = datetime.fromtimestamp(int(candle[0]) / 1000, tz=timezone.utc)
            if child_start < bucket_start_utc or child_start >= bucket_end_utc:
                continue
            rows.append(
                normalize_ohlcv_bucket(
                    source_name,
                    child_start,
                    symbol=symbol,
                    open_price_usd=float(candle[1]),
                    high_price_usd=float(candle[2]),
                    low_price_usd=float(candle[3]),
                    close_price_usd=float(candle[4]),
                    volume_btc=float(candle[5]) if candle[5] not in (None, '') else None,
                    volume_usd=float(candle[7]) if len(candle) > 7 and candle[7] not in (None, '') else None,
                    trade_count=None,
                    raw_payload={'kline': candle},
                    source_updated_at=child_start,
                    normalize_bucket_start=False,
                )
            )
        return rows


def build_rest_fetcher(*, timeout_seconds: int = 30) -> OkxRestBucketFetcher:
    return OkxRestBucketFetcher(timeout_seconds=timeout_seconds)


def run_rest_once(db=None, settings=None) -> int:
    return fetch_buckets(db=db, settings=settings)
