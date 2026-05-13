from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from mqnode.core.utils import utc_now


def get_checkpoint(cur, chain: str, component: str, interval: str) -> dict[str, Any]:
    cur.execute(
        '''
        SELECT * FROM sync_checkpoints
        WHERE chain = %s AND component = %s AND interval = %s
        ''',
        (chain, component, interval),
    )
    row = cur.fetchone()
    return row or {
        'chain': chain,
        'component': component,
        'interval': interval,
        'last_height': 0,
        'last_bucket_time': None,
        'status': 'idle',
    }


def upsert_checkpoint(
    cur,
    chain: str,
    component: str,
    interval: str,
    last_height: Optional[int] = None,
    last_bucket_time: Optional[datetime] = None,
    status: str = 'ok',
    error_message: Optional[str] = None,
) -> None:
    cur.execute(
        '''
        INSERT INTO sync_checkpoints(
          chain, component, interval, last_height, last_bucket_time, status, error_message, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (chain, component, interval)
        DO UPDATE SET
          last_height = COALESCE(EXCLUDED.last_height, sync_checkpoints.last_height),
          last_bucket_time = COALESCE(EXCLUDED.last_bucket_time, sync_checkpoints.last_bucket_time),
          status = EXCLUDED.status,
          error_message = EXCLUDED.error_message,
          updated_at = EXCLUDED.updated_at
        ''',
        (chain, component, interval, last_height, last_bucket_time, status, error_message, utc_now()),
    )


def get_enabled_metrics(cur, chain: str, factor: Optional[str] = None):
    if factor:
        cur.execute(
            '''
            SELECT * FROM metric_registry
            WHERE enabled = true AND chain = %s AND factor = %s
            ORDER BY id ASC
            ''',
            (chain, factor),
        )
    else:
        cur.execute(
            'SELECT * FROM metric_registry WHERE enabled = true AND chain = %s ORDER BY id ASC',
            (chain,),
        )
    return cur.fetchall()
