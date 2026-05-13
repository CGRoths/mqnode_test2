from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from mqnode.market.price.ws.models import ConfirmedCandleEvent, ensure_utc
from mqnode.market.price.ws.sources.base import PriceWsSource

WS_URL = 'wss://ws.okx.com:8443/ws/v5/business'
CHANNEL_BY_INTERVAL = {'1m': 'candle1m'}


def _datetime_from_ms(value: int | str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def _okx_inst_id(symbol: str) -> str:
    if '-' in symbol:
        return symbol.upper()
    upper_symbol = symbol.upper()
    for quote in ('USDT', 'USD'):
        if upper_symbol.endswith(quote) and len(upper_symbol) > len(quote):
            return f'{upper_symbol[: -len(quote)]}-{quote}'
    return upper_symbol


class OkxWsSource(PriceWsSource):
    source_name = 'okx'
    websocket_url = WS_URL

    def subscription_payload(self, *, symbol: str, interval: str = '1m') -> dict:
        channel = CHANNEL_BY_INTERVAL.get(interval)
        if channel is None:
            raise ValueError(f'Unsupported OKX WS interval: {interval}')
        return {'op': 'subscribe', 'args': [{'channel': channel, 'instId': _okx_inst_id(symbol)}]}

    def parse_message(
        self,
        message: str | dict,
        *,
        received_at: datetime,
        symbol: str | None = None,
    ) -> list[ConfirmedCandleEvent]:
        payload: dict[str, Any] = json.loads(message) if isinstance(message, str) else message
        arg = payload.get('arg') or {}
        channel = arg.get('channel')
        if channel != 'candle1m':
            return []
        event_symbol = arg.get('instId') or (_okx_inst_id(symbol) if symbol else None)
        if not event_symbol:
            return []

        received_at = ensure_utc(received_at)
        events: list[ConfirmedCandleEvent] = []
        for candle in payload.get('data') or []:
            if len(candle) < 9 or str(candle[8]) != '1':
                continue
            bucket_start_utc = _datetime_from_ms(candle[0])
            exchange_close_time = bucket_start_utc + timedelta(minutes=1)
            close_latency_ms = int((received_at - exchange_close_time).total_seconds() * 1000)
            events.append(
                ConfirmedCandleEvent(
                    source_name=self.source_name,
                    symbol=event_symbol,
                    bucket_start_utc=bucket_start_utc,
                    interval='1m',
                    open_price_usd=float(candle[1]),
                    high_price_usd=float(candle[2]),
                    low_price_usd=float(candle[3]),
                    close_price_usd=float(candle[4]),
                    volume_btc=float(candle[5]) if candle[5] not in (None, '') else None,
                    volume_usd=float(candle[7]) if candle[7] not in (None, '') else None,
                    source_updated_at=bucket_start_utc,
                    exchange_close_time=exchange_close_time,
                    received_at=received_at,
                    raw_payload={
                        'arg': arg,
                        'candle': candle,
                        'timestamp_semantics': 'okx_ts_is_bucket_start',
                        'latency_reference': 'bucket_start_plus_interval',
                        'close_latency_ms': close_latency_ms,
                    },
                )
            )
        return events


def build_connector() -> OkxWsSource:
    return OkxWsSource()
