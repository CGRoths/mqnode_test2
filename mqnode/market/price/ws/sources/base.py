from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from mqnode.market.price.ws.models import ConfirmedCandleEvent


class PriceWsSource(ABC):
    source_name: str
    websocket_url: str

    @abstractmethod
    def subscription_payload(self, *, symbol: str, interval: str = '1m') -> dict | None:
        raise NotImplementedError

    @abstractmethod
    def parse_message(
        self,
        message: str | dict,
        *,
        received_at: datetime,
        symbol: str | None = None,
    ) -> list[ConfirmedCandleEvent]:
        raise NotImplementedError

    def heartbeat_payload(self) -> dict:
        return {'op': 'ping'}


class NotImplementedPriceWsSource(PriceWsSource):
    source_name = 'unimplemented'
    websocket_url = ''

    def subscription_payload(self, *, symbol: str, interval: str = '1m') -> dict | None:
        raise NotImplementedError(f'{self.source_name} WS source is not enabled in the MVP.')

    def parse_message(
        self,
        message: str | dict,
        *,
        received_at: datetime,
        symbol: str | None = None,
    ) -> list[ConfirmedCandleEvent]:
        raise NotImplementedError(f'{self.source_name} WS source is not enabled in the MVP.')
