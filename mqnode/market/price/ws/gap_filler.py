from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from mqnode.core.utils import utc_now
from mqnode.market.price.binance.rest import BinanceRestBucketFetcher as _BinanceRestBucketFetcher
from mqnode.market.price.bybit.rest import BybitRestBucketFetcher as _BybitRestBucketFetcher
from mqnode.market.price.okx.rest import OkxRestBucketFetcher as _OkxRestBucketFetcher
from mqnode.market.price.source_support import request_json
from mqnode.market.price.ws.models import MISSING, REST_REPAIRED, ensure_utc
from mqnode.market.price.ws.repository import PriceWsRepository


class RestBucketFetcher(Protocol):
    def fetch_bucket(self, source_name: str, symbol: str, bucket_start_utc: datetime) -> dict | None:
        ...


@dataclass(frozen=True)
class GapFillResult:
    source_name: str
    symbol: str
    bucket_start_utc: datetime
    quality_status: str
    row: dict | None = None
    error: str | None = None


class BybitRestBucketFetcher(_BybitRestBucketFetcher):
    def __init__(self, *, timeout_seconds: int = 30) -> None:
        super().__init__(timeout_seconds=timeout_seconds, request_json_func=request_json)


class OkxRestBucketFetcher(_OkxRestBucketFetcher):
    def __init__(self, *, timeout_seconds: int = 30) -> None:
        super().__init__(timeout_seconds=timeout_seconds, request_json_func=request_json, utc_now_func=utc_now)


class BinanceRestBucketFetcher(_BinanceRestBucketFetcher):
    def __init__(self, *, timeout_seconds: int = 30) -> None:
        super().__init__(timeout_seconds=timeout_seconds, request_json_func=request_json)


def get_rest_bucket_fetcher(source_name: str, *, timeout_seconds: int = 30) -> RestBucketFetcher:
    source_name = source_name.lower()
    if source_name == 'bybit':
        return BybitRestBucketFetcher(timeout_seconds=timeout_seconds)
    if source_name == 'okx':
        return OkxRestBucketFetcher(timeout_seconds=timeout_seconds)
    if source_name == 'binance':
        return BinanceRestBucketFetcher(timeout_seconds=timeout_seconds)
    raise NotImplementedError(f'No phase-1 WS REST fallback fetcher for source: {source_name}')


def _missing_source_row(
    *,
    source_name: str,
    symbol: str,
    bucket_start_utc: datetime,
    reason: str,
    error: str | None,
    now: datetime,
) -> dict:
    return {
        'source_name': source_name,
        'symbol': symbol,
        'bucket_start_utc': bucket_start_utc,
        'open_price_usd': None,
        'high_price_usd': None,
        'low_price_usd': None,
        'close_price_usd': None,
        'volume_btc': None,
        'volume_usd': None,
        'trade_count': None,
        'raw_payload': {
            'rest_fallback': 'missing',
            'reason': reason,
            'error': error,
        },
        'source_updated_at': now,
        'data_source_mode': 'rest_live_fallback',
        'quality_status': MISSING,
        'is_repaired': False,
    }


class PriceWsGapFiller:
    def __init__(self, fetcher: RestBucketFetcher, repository: PriceWsRepository | None = None) -> None:
        self.fetcher = fetcher
        self.repository = repository or PriceWsRepository()

    def resolve_dirty_bucket(
        self,
        *,
        source_name: str,
        symbol: str,
        bucket_start_utc: datetime,
        reason: str,
        now: datetime,
        data_source_mode: str = 'rest_live_fallback',
    ) -> GapFillResult:
        bucket_start_utc = ensure_utc(bucket_start_utc)
        now = ensure_utc(now)
        row = self.fetcher.fetch_bucket(source_name, symbol, bucket_start_utc)
        if row is None:
            return GapFillResult(
                source_name,
                symbol,
                bucket_start_utc,
                MISSING,
                error='REST fallback returned no candle',
            )

        row = {
            **row,
            'data_source_mode': data_source_mode,
            'quality_status': REST_REPAIRED,
            'is_repaired': True,
            'rest_confirmed_at': now,
        }
        return GapFillResult(source_name, symbol, bucket_start_utc, REST_REPAIRED, row=row)

    def fill_dirty_bucket(
        self,
        db,
        *,
        source_name: str,
        table_name: str,
        symbol: str,
        bucket_start_utc: datetime,
        reason: str,
        now: datetime,
    ) -> GapFillResult:
        result = self.resolve_dirty_bucket(
            source_name=source_name,
            symbol=symbol,
            bucket_start_utc=bucket_start_utc,
            reason=reason,
            now=now,
        )
        if result.row is None:
            missing_row = _missing_source_row(
                source_name=source_name,
                symbol=symbol,
                bucket_start_utc=result.bucket_start_utc,
                reason=reason,
                error=result.error,
                now=ensure_utc(now),
            )
            self.repository.upsert_source_price_row(db, source_name, table_name, missing_row)
            self.repository.record_gap(
                db,
                source_name=source_name,
                symbol=symbol,
                bucket_start_utc=result.bucket_start_utc,
                reason=reason,
                repair_status=MISSING,
            )
            self.repository.mark_source_status(
                db,
                source_name=source_name,
                symbol=symbol,
                status=MISSING,
                current_bucket_start_utc=result.bucket_start_utc,
                current_bucket_dirty=True,
                dirty_reason=reason,
                last_error=result.error,
            )
            return result

        row = result.row
        self.repository.upsert_source_price_row(db, source_name, table_name, row)
        self.repository.record_gap(
            db,
            source_name=source_name,
            symbol=symbol,
            bucket_start_utc=result.bucket_start_utc,
            reason=reason,
            repair_status=REST_REPAIRED,
            repaired_at=ensure_utc(now),
        )
        self.repository.mark_source_status(
            db,
            source_name=source_name,
            symbol=symbol,
            status=REST_REPAIRED,
            last_closed_bucket_start_utc=result.bucket_start_utc,
            current_bucket_start_utc=result.bucket_start_utc,
            current_bucket_dirty=False,
        )
        return result
