from __future__ import annotations

from datetime import datetime, timedelta

from mqnode.market.price.ws.models import ensure_utc
from mqnode.market.price.ws.state import (
    DIRTY_HEARTBEAT_TIMEOUT,
    DIRTY_WS_DISCONNECT,
    DIRTY_WS_RECONNECT,
    InMemoryPriceWsStateStore,
    PriceWsBucketState,
)


class PriceWsGapDetector:
    def __init__(self, state_store: InMemoryPriceWsStateStore, *, stale_after: timedelta) -> None:
        self.state_store = state_store
        self.stale_after = stale_after

    def record_disconnect(self, source_name: str, symbol: str) -> PriceWsBucketState | None:
        return self.state_store.mark_current_dirty(source_name, symbol, DIRTY_WS_DISCONNECT)

    def record_reconnect(self, source_name: str, symbol: str, next_bucket_start_utc: datetime | None = None):
        state = self.state_store.current(source_name, symbol)
        if state is not None:
            state.mark_dirty(DIRTY_WS_RECONNECT)
        if next_bucket_start_utc is None:
            return state
        return self.state_store.start_next_bucket(source_name, symbol, next_bucket_start_utc)

    def detect_stale(self, source_name: str, symbol: str, now: datetime) -> bool:
        now = ensure_utc(now)
        state = self.state_store.current(source_name, symbol)
        if state is None or state.last_received_at is None:
            return False
        if now - state.last_received_at <= self.stale_after:
            return False
        state.mark_dirty(DIRTY_HEARTBEAT_TIMEOUT)
        return True

