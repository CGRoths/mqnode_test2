from __future__ import annotations

from datetime import datetime

from mqnode.core.utils import to_bucket_start_10m
from mqnode.market.price.ws.models import (
    REST_REQUIRED,
    WS_CLOSED,
    BucketFinalizationResult,
    ConfirmedCandleEvent,
    FinalizedPriceBucket,
    ensure_utc,
)
from mqnode.market.price.ws.state import (
    DIRTY_MISSING_1M_CANDLE,
    InMemoryPriceWsStateStore,
    PriceWsBucketState,
)


class TenMinuteCandleBuilder:
    def __init__(self, state_store: InMemoryPriceWsStateStore | None = None) -> None:
        self.state_store = state_store or InMemoryPriceWsStateStore()
        self._candles: dict[tuple[str, str, datetime], dict[datetime, ConfirmedCandleEvent]] = {}

    def accept(self, event: ConfirmedCandleEvent) -> PriceWsBucketState:
        bucket_start = event.target_bucket_start_utc
        state = self.state_store.get_or_start(event.source_name, event.symbol, bucket_start)
        if not event.confirmed:
            return state

        key = (event.source_name, event.symbol, bucket_start)
        self._candles.setdefault(key, {})[event.bucket_start_utc] = event
        state.record_child_candle(event.bucket_start_utc, event.received_at)
        return state

    def mark_dirty(self, source_name: str, symbol: str, bucket_start_utc: datetime, reason: str) -> PriceWsBucketState:
        state = self.state_store.get_or_start(source_name, symbol, bucket_start_utc)
        state.mark_dirty(reason)
        return state

    def state_for(self, source_name: str, symbol: str, bucket_start_utc: datetime) -> PriceWsBucketState:
        return self.state_store.get_or_start(source_name, symbol, bucket_start_utc)

    def has_bucket(self, source_name: str, symbol: str, bucket_start_utc: datetime) -> bool:
        bucket_start_utc = to_bucket_start_10m(ensure_utc(bucket_start_utc))
        return (source_name, symbol, bucket_start_utc) in self._candles

    def finalize_bucket(
        self,
        source_name: str,
        symbol: str,
        bucket_start_utc: datetime,
        *,
        closed_at: datetime,
    ) -> BucketFinalizationResult:
        bucket_start_utc = to_bucket_start_10m(ensure_utc(bucket_start_utc))
        closed_at = ensure_utc(closed_at)
        state = self.state_store.get_or_start(source_name, symbol, bucket_start_utc)
        key = (source_name, symbol, bucket_start_utc)

        if state.is_dirty:
            return BucketFinalizationResult(
                source_name=source_name,
                symbol=symbol,
                bucket_start_utc=bucket_start_utc,
                quality_status=REST_REQUIRED,
                rest_required_reason=state.dirty_reason,
            )

        missing = state.missing_child_buckets()
        if missing:
            state.mark_dirty(DIRTY_MISSING_1M_CANDLE)
            return BucketFinalizationResult(
                source_name=source_name,
                symbol=symbol,
                bucket_start_utc=bucket_start_utc,
                quality_status=REST_REQUIRED,
                rest_required_reason=DIRTY_MISSING_1M_CANDLE,
                missing_child_buckets=missing,
            )

        candles_by_start = self._candles.get(key, {})
        candles = [candles_by_start[bucket] for bucket in sorted(candles_by_start)]
        if len(candles) != len(state.expected_1m_buckets):
            state.mark_dirty(DIRTY_MISSING_1M_CANDLE)
            return BucketFinalizationResult(
                source_name=source_name,
                symbol=symbol,
                bucket_start_utc=bucket_start_utc,
                quality_status=REST_REQUIRED,
                rest_required_reason=DIRTY_MISSING_1M_CANDLE,
                missing_child_buckets=state.missing_child_buckets(),
            )

        volume_btc = sum(float(item.volume_btc or 0) for item in candles)
        volume_usd_parts = []
        for item in candles:
            if item.volume_usd is not None:
                volume_usd_parts.append(float(item.volume_usd))
            elif item.volume_btc is not None and item.close_price_usd is not None:
                volume_usd_parts.append(float(item.volume_btc) * float(item.close_price_usd))
        trade_counts = [item.trade_count for item in candles if item.trade_count is not None]
        source_updated_at = max((item.source_updated_at for item in candles if item.source_updated_at), default=None)
        raw_children = [
            {
                'bucket_start_utc': item.bucket_start_utc.isoformat(),
                'source_updated_at': item.source_updated_at.isoformat() if item.source_updated_at else None,
                'received_at': item.received_at.isoformat() if item.received_at else None,
                'raw_payload': item.raw_payload,
            }
            for item in candles
        ]
        candle = FinalizedPriceBucket(
            source_name=source_name,
            symbol=symbol,
            bucket_start_utc=bucket_start_utc,
            open_price_usd=float(candles[0].open_price_usd),
            high_price_usd=max(float(item.high_price_usd) for item in candles),
            low_price_usd=min(float(item.low_price_usd) for item in candles),
            close_price_usd=float(candles[-1].close_price_usd),
            volume_btc=volume_btc if volume_btc > 0 else None,
            volume_usd=sum(volume_usd_parts) if volume_usd_parts else None,
            trade_count=sum(trade_counts) if trade_counts else None,
            quality_status=WS_CLOSED,
            data_source_mode='websocket_confirmed_1m',
            source_updated_at=source_updated_at,
            ws_closed_at=closed_at,
            first_received_at=state.first_received_at,
            last_received_at=state.last_received_at,
            ws_disconnect_count=state.ws_disconnect_count,
            ws_gap_count=state.ws_gap_count,
            expected_child_candle_count=len(state.expected_1m_buckets),
            actual_child_candle_count=len(candles),
            raw_payload={
                'aggregation': 'ws_confirmed_1m_to_10m',
                'child_interval': '1m',
                'child_candles': raw_children,
            },
        )
        return BucketFinalizationResult(
            source_name=source_name,
            symbol=symbol,
            bucket_start_utc=bucket_start_utc,
            quality_status=WS_CLOSED,
            candle=candle,
        )
