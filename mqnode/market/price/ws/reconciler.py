from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import inf

from mqnode.market.price.ws.models import REST_CONFIRMED, ensure_utc
from mqnode.market.price.ws.repository import PriceWsRepository


@dataclass(frozen=True)
class ReconciliationResult:
    source_name: str
    bucket_start_utc: datetime
    quality_status: str
    revised: bool
    reason: str | None = None


def _bps_delta(old_value, new_value) -> float:
    if old_value is None and new_value is None:
        return 0.0
    if old_value is None or new_value is None:
        return inf
    old_value = float(old_value)
    new_value = float(new_value)
    if new_value == 0:
        return 0.0 if old_value == 0 else inf
    return abs(old_value - new_value) / abs(new_value) * 10_000


class PriceWsReconciler:
    def __init__(
        self,
        *,
        price_tolerance_bps: float = 1.0,
        volume_tolerance_bps: float = 10.0,
        repository: PriceWsRepository | None = None,
    ) -> None:
        self.price_tolerance_bps = price_tolerance_bps
        self.volume_tolerance_bps = volume_tolerance_bps
        self.repository = repository or PriceWsRepository()

    def is_within_tolerance(self, ws_row: dict, rest_row: dict) -> bool:
        for field in ('open_price_usd', 'high_price_usd', 'low_price_usd', 'close_price_usd'):
            if _bps_delta(ws_row.get(field), rest_row.get(field)) > self.price_tolerance_bps:
                return False
        return _bps_delta(ws_row.get('volume_btc'), rest_row.get('volume_btc')) <= self.volume_tolerance_bps

    def reconcile(
        self,
        db,
        *,
        source_name: str,
        table_name: str,
        ws_row: dict,
        rest_row: dict,
        now: datetime,
    ) -> ReconciliationResult:
        bucket_start_utc = ensure_utc(ws_row['bucket_start_utc'])
        now = ensure_utc(now)
        if self.is_within_tolerance(ws_row, rest_row):
            self.repository.mark_rest_confirmed(db, table_name, bucket_start_utc, now)
            return ReconciliationResult(source_name, bucket_start_utc, REST_CONFIRMED, revised=False)

        reason = 'rest_confirmation_mismatch'
        revised_row = {
            **rest_row,
            'bucket_start_utc': bucket_start_utc,
            'rest_confirmed_at': now,
        }
        self.repository.revise_source_price_row(
            db,
            source_name=source_name,
            table_name=table_name,
            old_row=ws_row,
            new_row=revised_row,
            revision_reason=reason,
        )
        return ReconciliationResult(source_name, bucket_start_utc, REST_CONFIRMED, revised=True, reason=reason)

