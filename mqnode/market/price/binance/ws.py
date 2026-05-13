from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from mqnode.market.price.ws.models import ConfirmedCandleEvent, ensure_utc
from mqnode.market.price.ws.sources.base import PriceWsSource

DEFAULT_SYMBOL = 'BTCUSDT'
DEFAULT_INTERVAL = '1m'
WS_BASE_URL = 'wss://stream.binance.com:9443'


def _stream_symbol(symbol: str) -> str:
    return symbol.replace('-', '').replace('/', '').lower()


def _datetime_from_ms(value: int | str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def _raw_stream_url(symbol: str = DEFAULT_SYMBOL, interval: str = DEFAULT_INTERVAL) -> str:
    return f'{WS_BASE_URL}/ws/{_stream_symbol(symbol)}@kline_{interval}'


class BinanceWsSource(PriceWsSource):
    source_name = 'binance'
    websocket_url = _raw_stream_url()

    def subscription_payload(self, *, symbol: str, interval: str = DEFAULT_INTERVAL) -> dict | None:
        return None

    def parse_message(
        self,
        message: str | dict,
        *,
        received_at: datetime,
        symbol: str | None = None,
    ) -> list[ConfirmedCandleEvent]:
        payload: dict[str, Any] = json.loads(message) if isinstance(message, str) else message
        if 'stream' in payload and isinstance(payload.get('data'), dict):
            payload = payload['data']
        if payload.get('e') != 'kline':
            return []

        kline = payload.get('k')
        if not isinstance(kline, dict) or kline.get('x') is not True:
            return []

        received_at = ensure_utc(received_at)
        event_time = payload.get('E')
        event_symbol = str(kline.get('s') or payload.get('s') or symbol or DEFAULT_SYMBOL).upper()
        output_symbol = symbol or event_symbol
        return [
            ConfirmedCandleEvent(
                source_name=self.source_name,
                symbol=output_symbol,
                bucket_start_utc=_datetime_from_ms(kline['t']),
                interval=str(kline.get('i') or DEFAULT_INTERVAL),
                open_price_usd=float(kline['o']),
                high_price_usd=float(kline['h']),
                low_price_usd=float(kline['l']),
                close_price_usd=float(kline['c']),
                volume_btc=float(kline['v']) if kline.get('v') is not None else None,
                volume_usd=float(kline['q']) if kline.get('q') is not None else None,
                trade_count=int(kline['n']) if kline.get('n') is not None else None,
                source_updated_at=_datetime_from_ms(event_time) if event_time is not None else received_at,
                exchange_close_time=_datetime_from_ms(kline['T']),
                received_at=received_at,
                raw_payload=dict(payload),
            )
        ]


def build_connector() -> BinanceWsSource:
    return BinanceWsSource()
