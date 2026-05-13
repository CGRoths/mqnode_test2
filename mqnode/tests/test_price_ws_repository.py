from __future__ import annotations

from datetime import datetime, timezone

from mqnode.market.price.ws.repository import PriceWsRepository


class _Cursor:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.calls.append((query, params))


class _DB:
    def __init__(self):
        self.cursor_instance = _Cursor()

    def cursor(self):
        return self.cursor_instance


def test_source_price_upsert_writes_ohlcv_and_quality_in_one_statement():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    db = _DB()

    upserted = PriceWsRepository().upsert_source_price_row(
        db,
        'bybit',
        'bybit_price_10m',
        {
            'bucket_start_utc': bucket,
            'symbol': 'BTCUSDT',
            'open_price_usd': 100,
            'high_price_usd': 110,
            'low_price_usd': 90,
            'close_price_usd': 105,
            'volume_btc': 2,
            'volume_usd': 210,
            'trade_count': None,
            'raw_payload': {'source': 'ws'},
            'source_updated_at': bucket,
            'data_source_mode': 'websocket_confirmed_1m',
            'quality_status': 'ws_closed',
            'ws_closed_at': bucket,
            'expected_child_candle_count': 10,
            'actual_child_candle_count': 10,
        },
    )

    assert upserted == 1
    assert len(db.cursor_instance.calls) == 1
    query, params = db.cursor_instance.calls[0]
    assert 'quality_status' in query
    assert 'data_source_mode' in query
    assert params['quality_status'] == 'ws_closed'
    assert params['data_source_mode'] == 'websocket_confirmed_1m'
    assert params['raw_payload'] == '{"source": "ws"}'
