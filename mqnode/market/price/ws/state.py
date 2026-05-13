from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from mqnode.core.utils import to_bucket_start_10m
from mqnode.market.price.ws.models import ensure_utc, expected_child_buckets, one_minute_bucket_start

DIRTY_WS_DISCONNECT = 'ws_disconnect'
DIRTY_WS_RECONNECT = 'ws_reconnect'
DIRTY_HEARTBEAT_TIMEOUT = 'heartbeat_timeout'
DIRTY_MISSING_1M_CANDLE = 'missing_1m_candle'
DIRTY_STARTUP_PARTIAL_BUCKET = 'startup_partial_bucket'
DIRTY_SEQUENCE_GAP = 'sequence_gap'
DIRTY_LATE_CONFIRMED_CANDLE = 'late_confirmed_candle'
DIRTY_OUT_OF_ORDER_UNSAFE = 'out_of_order_unsafe'


@dataclass
class PriceWsBucketState:
    source_name: str
    symbol: str
    bucket_start_utc: datetime
    expected_1m_buckets: tuple[datetime, ...]
    received_1m_buckets: set[datetime] = field(default_factory=set)
    ws_disconnect_count: int = 0
    ws_gap_count: int = 0
    heartbeat_timeout_count: int = 0
    reconnect_count_inside_bucket: int = 0
    is_dirty: bool = False
    dirty_reason: str | None = None
    first_received_at: datetime | None = None
    last_received_at: datetime | None = None

    @classmethod
    def start(cls, source_name: str, symbol: str, bucket_start_utc: datetime) -> PriceWsBucketState:
        bucket_start_utc = to_bucket_start_10m(ensure_utc(bucket_start_utc))
        return cls(
            source_name=source_name,
            symbol=symbol,
            bucket_start_utc=bucket_start_utc,
            expected_1m_buckets=expected_child_buckets(bucket_start_utc),
        )

    def record_child_candle(self, bucket_start_utc: datetime, received_at: datetime | None = None) -> None:
        child_bucket = one_minute_bucket_start(bucket_start_utc)
        self.received_1m_buckets.add(child_bucket)
        if received_at is None:
            return
        received_at = ensure_utc(received_at)
        if self.first_received_at is None or received_at < self.first_received_at:
            self.first_received_at = received_at
        if self.last_received_at is None or received_at > self.last_received_at:
            self.last_received_at = received_at

    def missing_child_buckets(self) -> tuple[datetime, ...]:
        return tuple(bucket for bucket in self.expected_1m_buckets if bucket not in self.received_1m_buckets)

    def mark_dirty(self, reason: str) -> None:
        self.is_dirty = True
        if self.dirty_reason is None:
            self.dirty_reason = reason
        if reason in {DIRTY_MISSING_1M_CANDLE, DIRTY_SEQUENCE_GAP, DIRTY_OUT_OF_ORDER_UNSAFE}:
            self.ws_gap_count += 1
        elif reason == DIRTY_HEARTBEAT_TIMEOUT:
            self.heartbeat_timeout_count += 1
        elif reason == DIRTY_WS_DISCONNECT:
            self.ws_disconnect_count += 1
        elif reason == DIRTY_WS_RECONNECT:
            self.reconnect_count_inside_bucket += 1


class InMemoryPriceWsStateStore:
    def __init__(self) -> None:
        self._states: dict[tuple[str, str, datetime], PriceWsBucketState] = {}
        self._current: dict[tuple[str, str], datetime] = {}

    def get_or_start(self, source_name: str, symbol: str, bucket_start_utc: datetime) -> PriceWsBucketState:
        bucket_start_utc = to_bucket_start_10m(ensure_utc(bucket_start_utc))
        key = (source_name, symbol, bucket_start_utc)
        if key not in self._states:
            self._states[key] = PriceWsBucketState.start(source_name, symbol, bucket_start_utc)
        current_key = (source_name, symbol)
        if self._current.get(current_key) is None or bucket_start_utc >= self._current[current_key]:
            self._current[current_key] = bucket_start_utc
        return self._states[key]

    def current(self, source_name: str, symbol: str) -> PriceWsBucketState | None:
        bucket_start_utc = self._current.get((source_name, symbol))
        if bucket_start_utc is None:
            return None
        return self._states[(source_name, symbol, bucket_start_utc)]

    def iter_states(self) -> tuple[PriceWsBucketState, ...]:
        return tuple(self._states.values())

    def start_next_bucket(self, source_name: str, symbol: str, bucket_start_utc: datetime) -> PriceWsBucketState:
        bucket_start_utc = to_bucket_start_10m(ensure_utc(bucket_start_utc))
        self._current[(source_name, symbol)] = bucket_start_utc
        return self.get_or_start(source_name, symbol, bucket_start_utc)

    def mark_current_dirty(self, source_name: str, symbol: str, reason: str) -> PriceWsBucketState | None:
        state = self.current(source_name, symbol)
        if state is not None:
            state.mark_dirty(reason)
        return state
