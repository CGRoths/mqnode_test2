from __future__ import annotations

import secrets
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel, Field

from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.market.price.remote_ingest import MAX_REMOTE_ROWS, ingest_remote_binance_rows

router = APIRouter()


class RemoteBinancePriceRow(BaseModel):
    bucket_start_utc: datetime
    open_price_usd: float
    high_price_usd: float
    low_price_usd: float
    close_price_usd: float
    source_name: str = 'binance'
    symbol: str = 'BTCUSDT'
    volume_btc: float | None = None
    volume_usd: float | None = None
    trade_count: int | None = None
    source_updated_at: datetime | None = None
    exchange_close_time: datetime | None = None
    cloud_fetched_at: datetime | None = None
    cloud_sent_at: datetime | None = None
    data_source_mode: str | None = None
    quality_status: str | None = None
    ws_closed_at: datetime | None = None
    rest_confirmed_at: datetime | None = None
    is_repaired: bool | None = None
    is_revised: bool | None = None
    revision_count: int | None = None
    first_received_at: datetime | None = None
    last_received_at: datetime | None = None
    ws_disconnect_count: int | None = None
    ws_gap_count: int | None = None
    expected_child_candle_count: int | None = None
    actual_child_candle_count: int | None = None
    raw_payload: dict[str, Any] | None = None


class RemoteBinancePriceRequest(BaseModel):
    rows: list[RemoteBinancePriceRow] = Field(min_length=1, max_length=MAX_REMOTE_ROWS)


def _require_internal_token(authorization: str | None) -> None:
    expected_token = get_settings().internal_ingest_token
    if not expected_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Internal ingest token is not configured.',
        )
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Missing bearer token.')
    supplied_token = authorization.removeprefix('Bearer ').strip()
    if not secrets.compare_digest(supplied_token, expected_token):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Invalid bearer token.')


@router.post('/api/v1/internal/price/source/binance/10m')
def ingest_remote_binance_10m(payload: RemoteBinancePriceRequest, authorization: str | None = Header(default=None)):
    _require_internal_token(authorization)
    rows = [row.model_dump() for row in payload.rows]
    try:
        rows_upserted = ingest_remote_binance_rows(DB(get_settings()), rows, advance_checkpoint=False)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    return {
        'ok': True,
        'source': 'binance',
        'rows_received': len(rows),
        'rows_upserted': rows_upserted,
    }
