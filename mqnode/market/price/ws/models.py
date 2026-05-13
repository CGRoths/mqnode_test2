from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from mqnode.core.utils import to_bucket_start_10m

REST_HISTORICAL = 'rest_historical'
WS_CLOSED = 'ws_closed'
REST_CONFIRMED = 'rest_confirmed'
REST_REPAIRED = 'rest_repaired'
REST_REQUIRED = 'rest_required'
MISSING = 'missing'
REVISED = 'revised'

ACCEPTABLE_SOURCE_QUALITY_STATUSES = frozenset(
    {
        REST_HISTORICAL,
        WS_CLOSED,
        REST_CONFIRMED,
        REST_REPAIRED,
    }
)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError('datetime must be timezone-aware')
    return value.astimezone(timezone.utc)


def one_minute_bucket_start(value: datetime) -> datetime:
    value = ensure_utc(value)
    return value.replace(second=0, microsecond=0)


def expected_child_buckets(bucket_start_utc: datetime, *, child_minutes: int = 1) -> tuple[datetime, ...]:
    bucket_start_utc = to_bucket_start_10m(ensure_utc(bucket_start_utc))
    return tuple(bucket_start_utc + timedelta(minutes=offset) for offset in range(0, 10, child_minutes))


@dataclass(frozen=True)
class ConfirmedCandleEvent:
    source_name: str
    symbol: str
    bucket_start_utc: datetime
    open_price_usd: float
    high_price_usd: float
    low_price_usd: float
    close_price_usd: float
    volume_btc: float | None = None
    volume_usd: float | None = None
    trade_count: int | None = None
    interval: str = '1m'
    confirmed: bool = True
    source_updated_at: datetime | None = None
    exchange_close_time: datetime | None = None
    received_at: datetime | None = None
    sequence: int | None = None
    raw_payload: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, 'bucket_start_utc', one_minute_bucket_start(self.bucket_start_utc))
        if self.source_updated_at is not None:
            object.__setattr__(self, 'source_updated_at', ensure_utc(self.source_updated_at))
        if self.exchange_close_time is not None:
            object.__setattr__(self, 'exchange_close_time', ensure_utc(self.exchange_close_time))
        if self.received_at is not None:
            object.__setattr__(self, 'received_at', ensure_utc(self.received_at))

    @property
    def target_bucket_start_utc(self) -> datetime:
        return to_bucket_start_10m(self.bucket_start_utc)


@dataclass(frozen=True)
class FinalizedPriceBucket:
    source_name: str
    symbol: str
    bucket_start_utc: datetime
    open_price_usd: float
    high_price_usd: float
    low_price_usd: float
    close_price_usd: float
    volume_btc: float | None
    volume_usd: float | None
    trade_count: int | None
    quality_status: str
    data_source_mode: str
    source_updated_at: datetime | None
    ws_closed_at: datetime
    first_received_at: datetime | None
    last_received_at: datetime | None
    ws_disconnect_count: int
    ws_gap_count: int
    expected_child_candle_count: int
    actual_child_candle_count: int
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def to_source_row(self) -> dict[str, Any]:
        return {
            'source_name': self.source_name,
            'symbol': self.symbol,
            'bucket_start_utc': self.bucket_start_utc,
            'open_price_usd': self.open_price_usd,
            'high_price_usd': self.high_price_usd,
            'low_price_usd': self.low_price_usd,
            'close_price_usd': self.close_price_usd,
            'volume_btc': self.volume_btc,
            'volume_usd': self.volume_usd,
            'trade_count': self.trade_count,
            'raw_payload': self.raw_payload,
            'source_updated_at': self.source_updated_at,
            'data_source_mode': self.data_source_mode,
            'quality_status': self.quality_status,
            'ws_closed_at': self.ws_closed_at,
            'first_received_at': self.first_received_at,
            'last_received_at': self.last_received_at,
            'ws_disconnect_count': self.ws_disconnect_count,
            'ws_gap_count': self.ws_gap_count,
            'expected_child_candle_count': self.expected_child_candle_count,
            'actual_child_candle_count': self.actual_child_candle_count,
        }


@dataclass(frozen=True)
class BucketFinalizationResult:
    source_name: str
    symbol: str
    bucket_start_utc: datetime
    quality_status: str
    candle: FinalizedPriceBucket | None = None
    rest_required_reason: str | None = None
    missing_child_buckets: tuple[datetime, ...] = ()

    @property
    def rest_required(self) -> bool:
        return self.candle is None and self.quality_status in {REST_REQUIRED, MISSING}

