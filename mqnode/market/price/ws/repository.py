from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

SOURCE_QUALITY_COLUMNS = {
    'data_source_mode',
    'quality_status',
    'ws_closed_at',
    'rest_confirmed_at',
    'is_repaired',
    'is_revised',
    'revision_count',
    'first_received_at',
    'last_received_at',
    'ws_disconnect_count',
    'ws_gap_count',
    'expected_child_candle_count',
    'actual_child_candle_count',
}


def _validate_table_name(table_name: str) -> None:
    if not IDENTIFIER_RE.fullmatch(table_name):
        raise ValueError(f'Invalid table name: {table_name}')


class PriceWsRepository:
    def upsert_source_price_row(self, db, source_name: str, table_name: str, row: dict[str, Any]) -> int:
        _validate_table_name(table_name)
        with db.cursor() as cur:
            cur.execute(
                f'''
                INSERT INTO {table_name}(
                  bucket_start_utc,
                  symbol,
                  open_price_usd,
                  high_price_usd,
                  low_price_usd,
                  close_price_usd,
                  volume_btc,
                  volume_usd,
                  trade_count,
                  raw_payload,
                  source_updated_at,
                  data_source_mode,
                  quality_status,
                  ws_closed_at,
                  rest_confirmed_at,
                  is_repaired,
                  is_revised,
                  revision_count,
                  first_received_at,
                  last_received_at,
                  ws_disconnect_count,
                  ws_gap_count,
                  expected_child_candle_count,
                  actual_child_candle_count,
                  updated_at
                ) VALUES (
                  %(bucket_start_utc)s,
                  %(symbol)s,
                  %(open_price_usd)s,
                  %(high_price_usd)s,
                  %(low_price_usd)s,
                  %(close_price_usd)s,
                  %(volume_btc)s,
                  %(volume_usd)s,
                  %(trade_count)s,
                  %(raw_payload)s::jsonb,
                  %(source_updated_at)s,
                  %(data_source_mode)s,
                  %(quality_status)s,
                  %(ws_closed_at)s,
                  %(rest_confirmed_at)s,
                  %(is_repaired)s,
                  %(is_revised)s,
                  %(revision_count)s,
                  %(first_received_at)s,
                  %(last_received_at)s,
                  %(ws_disconnect_count)s,
                  %(ws_gap_count)s,
                  %(expected_child_candle_count)s,
                  %(actual_child_candle_count)s,
                  now()
                )
                ON CONFLICT (bucket_start_utc) DO UPDATE SET
                  symbol = EXCLUDED.symbol,
                  open_price_usd = EXCLUDED.open_price_usd,
                  high_price_usd = EXCLUDED.high_price_usd,
                  low_price_usd = EXCLUDED.low_price_usd,
                  close_price_usd = EXCLUDED.close_price_usd,
                  volume_btc = EXCLUDED.volume_btc,
                  volume_usd = EXCLUDED.volume_usd,
                  trade_count = EXCLUDED.trade_count,
                  raw_payload = EXCLUDED.raw_payload,
                  source_updated_at = EXCLUDED.source_updated_at,
                  data_source_mode = EXCLUDED.data_source_mode,
                  quality_status = EXCLUDED.quality_status,
                  ws_closed_at = EXCLUDED.ws_closed_at,
                  rest_confirmed_at = EXCLUDED.rest_confirmed_at,
                  is_repaired = EXCLUDED.is_repaired,
                  is_revised = EXCLUDED.is_revised,
                  revision_count = EXCLUDED.revision_count,
                  first_received_at = EXCLUDED.first_received_at,
                  last_received_at = EXCLUDED.last_received_at,
                  ws_disconnect_count = EXCLUDED.ws_disconnect_count,
                  ws_gap_count = EXCLUDED.ws_gap_count,
                  expected_child_candle_count = EXCLUDED.expected_child_candle_count,
                  actual_child_candle_count = EXCLUDED.actual_child_candle_count,
                  updated_at = now()
                ''',
                self._source_row_params(row),
            )
        return 1

    def _source_row_params(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            'bucket_start_utc': row['bucket_start_utc'],
            'symbol': row['symbol'],
            'open_price_usd': row.get('open_price_usd'),
            'high_price_usd': row.get('high_price_usd'),
            'low_price_usd': row.get('low_price_usd'),
            'close_price_usd': row.get('close_price_usd'),
            'volume_btc': row.get('volume_btc'),
            'volume_usd': row.get('volume_usd'),
            'trade_count': row.get('trade_count'),
            'raw_payload': json.dumps(row.get('raw_payload')),
            'source_updated_at': row.get('source_updated_at'),
            'data_source_mode': row.get('data_source_mode'),
            'quality_status': row.get('quality_status'),
            'ws_closed_at': row.get('ws_closed_at'),
            'rest_confirmed_at': row.get('rest_confirmed_at'),
            'is_repaired': bool(row.get('is_repaired', False)),
            'is_revised': bool(row.get('is_revised', False)),
            'revision_count': int(row.get('revision_count') or 0),
            'first_received_at': row.get('first_received_at'),
            'last_received_at': row.get('last_received_at'),
            'ws_disconnect_count': int(row.get('ws_disconnect_count') or 0),
            'ws_gap_count': int(row.get('ws_gap_count') or 0),
            'expected_child_candle_count': row.get('expected_child_candle_count'),
            'actual_child_candle_count': row.get('actual_child_candle_count'),
        }

    def get_source_price_row(self, db, table_name: str, bucket_start_utc: datetime) -> dict[str, Any] | None:
        _validate_table_name(table_name)
        with db.cursor() as cur:
            cur.execute(
                f'''
                SELECT
                  bucket_start_utc,
                  symbol,
                  open_price_usd,
                  high_price_usd,
                  low_price_usd,
                  close_price_usd,
                  volume_btc,
                  volume_usd,
                  trade_count,
                  raw_payload,
                  source_updated_at,
                  data_source_mode,
                  quality_status,
                  ws_closed_at,
                  rest_confirmed_at,
                  is_repaired,
                  is_revised,
                  revision_count,
                  first_received_at,
                  last_received_at,
                  ws_disconnect_count,
                  ws_gap_count,
                  expected_child_candle_count,
                  actual_child_candle_count
                FROM {table_name}
                WHERE bucket_start_utc = %s
                ''',
                (bucket_start_utc,),
            )
            return cur.fetchone()

    def mark_source_quality(
        self,
        cur,
        table_name: str,
        bucket_start_utc: datetime,
        metadata: dict[str, Any],
    ) -> None:
        _validate_table_name(table_name)
        updates = []
        params: dict[str, Any] = {'bucket_start_utc': bucket_start_utc}
        for column in sorted(SOURCE_QUALITY_COLUMNS):
            if column not in metadata:
                continue
            updates.append(f'{column} = %({column})s')
            params[column] = metadata[column]
        if not updates:
            return
        cur.execute(
            f'''
            UPDATE {table_name}
            SET {', '.join(updates)},
                updated_at = now()
            WHERE bucket_start_utc = %(bucket_start_utc)s
            ''',
            params,
        )

    def mark_source_status(
        self,
        db,
        *,
        source_name: str,
        symbol: str,
        status: str,
        last_message_at: datetime | None = None,
        last_closed_bucket_start_utc: datetime | None = None,
        current_bucket_start_utc: datetime | None = None,
        current_bucket_dirty: bool = False,
        dirty_reason: str | None = None,
        reconnect_count: int = 0,
        last_error: str | None = None,
    ) -> None:
        with db.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO mq_price_ws_source_status(
                  source_name,
                  symbol,
                  status,
                  last_message_at,
                  last_closed_bucket_start_utc,
                  current_bucket_start_utc,
                  current_bucket_dirty,
                  dirty_reason,
                  reconnect_count,
                  last_error,
                  updated_at
                ) VALUES (
                  %(source_name)s,
                  %(symbol)s,
                  %(status)s,
                  %(last_message_at)s,
                  %(last_closed_bucket_start_utc)s,
                  %(current_bucket_start_utc)s,
                  %(current_bucket_dirty)s,
                  %(dirty_reason)s,
                  %(reconnect_count)s,
                  %(last_error)s,
                  now()
                )
                ON CONFLICT (source_name, symbol) DO UPDATE SET
                  status = EXCLUDED.status,
                  last_message_at = EXCLUDED.last_message_at,
                  last_closed_bucket_start_utc = EXCLUDED.last_closed_bucket_start_utc,
                  current_bucket_start_utc = EXCLUDED.current_bucket_start_utc,
                  current_bucket_dirty = EXCLUDED.current_bucket_dirty,
                  dirty_reason = EXCLUDED.dirty_reason,
                  reconnect_count = mq_price_ws_source_status.reconnect_count + EXCLUDED.reconnect_count,
                  last_error = EXCLUDED.last_error,
                  updated_at = now()
                ''',
                {
                    'source_name': source_name,
                    'symbol': symbol,
                    'status': status,
                    'last_message_at': last_message_at,
                    'last_closed_bucket_start_utc': last_closed_bucket_start_utc,
                    'current_bucket_start_utc': current_bucket_start_utc,
                    'current_bucket_dirty': current_bucket_dirty,
                    'dirty_reason': dirty_reason,
                    'reconnect_count': reconnect_count,
                    'last_error': last_error,
                },
            )

    def record_gap(
        self,
        db,
        *,
        source_name: str,
        symbol: str,
        bucket_start_utc: datetime,
        reason: str,
        repair_status: str,
        gap_start_utc: datetime | None = None,
        gap_end_utc: datetime | None = None,
        repaired_at: datetime | None = None,
    ) -> None:
        with db.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO mq_price_source_gaps(
                  source_name,
                  symbol,
                  bucket_start_utc,
                  gap_start_utc,
                  gap_end_utc,
                  reason,
                  repair_status,
                  repaired_at
                ) VALUES (
                  %(source_name)s,
                  %(symbol)s,
                  %(bucket_start_utc)s,
                  %(gap_start_utc)s,
                  %(gap_end_utc)s,
                  %(reason)s,
                  %(repair_status)s,
                  %(repaired_at)s
                )
                ''',
                {
                    'source_name': source_name,
                    'symbol': symbol,
                    'bucket_start_utc': bucket_start_utc,
                    'gap_start_utc': gap_start_utc,
                    'gap_end_utc': gap_end_utc,
                    'reason': reason,
                    'repair_status': repair_status,
                    'repaired_at': repaired_at,
                },
            )

    def mark_rest_confirmed(self, db, table_name: str, bucket_start_utc: datetime, confirmed_at: datetime) -> None:
        _validate_table_name(table_name)
        with db.cursor() as cur:
            cur.execute(
                f'''
                UPDATE {table_name}
                SET quality_status = 'rest_confirmed',
                    rest_confirmed_at = %s,
                    updated_at = now()
                WHERE bucket_start_utc = %s
                ''',
                (confirmed_at, bucket_start_utc),
            )

    def revise_source_price_row(
        self,
        db,
        *,
        source_name: str,
        table_name: str,
        old_row: dict[str, Any],
        new_row: dict[str, Any],
        revision_reason: str,
    ) -> None:
        row = {
            **new_row,
            'quality_status': 'rest_confirmed',
            'data_source_mode': 'rest_confirmation',
            'is_revised': True,
            'revision_count': int(old_row.get('revision_count') or 0) + 1,
        }
        self.upsert_source_price_row(db, source_name, table_name, row)
        self.insert_revision_log(
            db,
            source_name=source_name,
            old_row=old_row,
            new_row=row,
            revision_reason=revision_reason,
        )

    def insert_revision_log(
        self,
        db,
        *,
        source_name: str,
        old_row: dict[str, Any],
        new_row: dict[str, Any],
        revision_reason: str,
    ) -> None:
        with db.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO mq_price_revision_log(
                  source_name,
                  symbol,
                  bucket_start_utc,
                  old_open_price_usd,
                  old_high_price_usd,
                  old_low_price_usd,
                  old_close_price_usd,
                  old_volume_btc,
                  new_open_price_usd,
                  new_high_price_usd,
                  new_low_price_usd,
                  new_close_price_usd,
                  new_volume_btc,
                  revision_reason,
                  old_payload,
                  new_payload
                ) VALUES (
                  %(source_name)s,
                  %(symbol)s,
                  %(bucket_start_utc)s,
                  %(old_open_price_usd)s,
                  %(old_high_price_usd)s,
                  %(old_low_price_usd)s,
                  %(old_close_price_usd)s,
                  %(old_volume_btc)s,
                  %(new_open_price_usd)s,
                  %(new_high_price_usd)s,
                  %(new_low_price_usd)s,
                  %(new_close_price_usd)s,
                  %(new_volume_btc)s,
                  %(revision_reason)s,
                  %(old_payload)s::jsonb,
                  %(new_payload)s::jsonb
                )
                ''',
                {
                    'source_name': source_name,
                    'symbol': new_row.get('symbol') or old_row.get('symbol'),
                    'bucket_start_utc': new_row['bucket_start_utc'],
                    'old_open_price_usd': old_row.get('open_price_usd'),
                    'old_high_price_usd': old_row.get('high_price_usd'),
                    'old_low_price_usd': old_row.get('low_price_usd'),
                    'old_close_price_usd': old_row.get('close_price_usd'),
                    'old_volume_btc': old_row.get('volume_btc'),
                    'new_open_price_usd': new_row.get('open_price_usd'),
                    'new_high_price_usd': new_row.get('high_price_usd'),
                    'new_low_price_usd': new_row.get('low_price_usd'),
                    'new_close_price_usd': new_row.get('close_price_usd'),
                    'new_volume_btc': new_row.get('volume_btc'),
                    'revision_reason': revision_reason,
                    'old_payload': json.dumps(old_row.get('raw_payload')),
                    'new_payload': json.dumps(new_row.get('raw_payload')),
                },
            )
