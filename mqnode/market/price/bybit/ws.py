from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from mqnode.market.price.ws.models import ConfirmedCandleEvent, ensure_utc
from mqnode.market.price.ws.sources.base import PriceWsSource

WS_URL = 'wss://stream.bybitglobal.com/v5/public/spot'


def _bybit_interval(interval: str) -> str:
    return interval[:-1] if interval.endswith('m') else interval


def _datetime_from_ms(value: int | str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def _is_confirmed(value: Any) -> bool:
    return value is True or str(value).lower() == 'true'


class BybitWsSource(PriceWsSource):
    source_name = 'bybit'
    websocket_url = WS_URL

    def subscription_payload(self, *, symbol: str, interval: str = '1m') -> dict:
        return {'op': 'subscribe', 'args': [f'kline.{_bybit_interval(interval)}.{symbol}']}

    def parse_message(
        self,
        message: str | dict,
        *,
        received_at: datetime,
        symbol: str | None = None,
    ) -> list[ConfirmedCandleEvent]:
        payload: dict[str, Any] = json.loads(message) if isinstance(message, str) else message
        topic = str(payload.get('topic') or '')
        if not topic.startswith('kline.'):
            return []
        topic_parts = topic.split('.')
        topic_interval = f'{topic_parts[1]}m' if len(topic_parts) > 1 and topic_parts[1].isdigit() else '1m'
        topic_symbol = symbol or (topic_parts[2] if len(topic_parts) > 2 else None)
        if topic_symbol is None:
            return []

        events: list[ConfirmedCandleEvent] = []
        received_at = ensure_utc(received_at)
        payload_ts = payload.get('ts')
        source_updated_at = _datetime_from_ms(payload_ts) if payload_ts else received_at
        for item in payload.get('data') or []:
            if not _is_confirmed(item.get('confirm')):
                continue
            start = _datetime_from_ms(item['start'])
            exchange_close_time = _datetime_from_ms(item.get('end') or item.get('timestamp') or payload_ts)
            events.append(
                ConfirmedCandleEvent(
                    source_name=self.source_name,
                    symbol=topic_symbol,
                    bucket_start_utc=start,
                    interval=topic_interval,
                    open_price_usd=float(item['open']),
                    high_price_usd=float(item['high']),
                    low_price_usd=float(item['low']),
                    close_price_usd=float(item['close']),
                    volume_btc=float(item['volume']) if item.get('volume') is not None else None,
                    volume_usd=float(item['turnover']) if item.get('turnover') is not None else None,
                    source_updated_at=source_updated_at,
                    exchange_close_time=exchange_close_time,
                    received_at=received_at,
                    raw_payload={'topic': topic, 'data': item, 'ts': payload_ts},
                )
            )
        return events


def build_connector() -> BybitWsSource:
    return BybitWsSource()
