from __future__ import annotations

import logging
import re
from collections.abc import Callable
from datetime import datetime

from mqnode.chains.btc.primitive_builder import PRIMITIVE_COMPONENT, PRIMITIVE_SCHEDULER_COMPONENT
from mqnode.core.utils import hour_bounds, to_bucket_start_10m
from mqnode.db.repositories import upsert_checkpoint

logger = logging.getLogger(__name__)

IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
RAW_COMPONENT = 'btc_raw_block_ingestion'


def find_common_ancestor_height(
    start_height: int,
    get_local_hash: Callable[[int], str | None],
    get_canonical_hash: Callable[[int], str],
) -> int:
    for height in range(max(start_height, 0), -1, -1):
        local_hash = get_local_hash(height)
        if local_hash is None:
            continue
        if local_hash == get_canonical_hash(height):
            return height
    return 0


def _get_local_hash(cur, height: int) -> str | None:
    cur.execute('SELECT block_hash FROM btc_blocks_raw WHERE height = %s', (height,))
    row = cur.fetchone()
    return row['block_hash'] if row else None


def _get_bucket_for_height(cur, height: int) -> datetime | None:
    if height <= 0:
        return None
    cur.execute(
        '''
        SELECT COALESCE(median_time, block_time) AS event_time
        FROM btc_primitive_block
        WHERE height = %s
        ''',
        (height,),
    )
    row = cur.fetchone()
    if not row or not row['event_time']:
        return None
    return to_bucket_start_10m(row['event_time'])


def _get_first_orphan_bucket(cur, common_height: int) -> datetime | None:
    cur.execute(
        '''
        SELECT COALESCE(median_time, block_time) AS event_time
        FROM btc_primitive_block
        WHERE height > %s
        ORDER BY height ASC
        LIMIT 1
        ''',
        (common_height,),
    )
    row = cur.fetchone()
    if not row or not row['event_time']:
        return None
    return to_bucket_start_10m(row['event_time'])


def _delete_metric_outputs(cur, affected_bucket: datetime | None) -> None:
    if affected_bucket is None:
        return

    cur.execute(
        '''
        SELECT DISTINCT output_table, interval
        FROM metric_registry
        WHERE chain = %s
        ''',
        ('BTC',),
    )
    for row in cur.fetchall():
        output_table = row['output_table']
        interval = row['interval']
        if not IDENTIFIER_RE.fullmatch(output_table):
            logger.warning('reorg_skip_output_table_cleanup invalid_table=%s', output_table)
            continue
        delete_from = affected_bucket
        if interval == '1h':
            delete_from = hour_bounds(affected_bucket)[0]
        cur.execute(f'DELETE FROM {output_table} WHERE bucket_start_utc >= %s', (delete_from,))


def reconcile_reorg(db, rpc, previous_checkpoint_height: int) -> dict | None:
    """Rewind raw and derived BTC state if the checkpoint no longer matches the canonical chain."""
    if previous_checkpoint_height <= 0:
        return None
    if not hasattr(db, 'cursor') or not hasattr(rpc, 'get_block_hash'):
        logger.debug('btc_reorg_skip unsupported_runtime previous_checkpoint_height=%s', previous_checkpoint_height)
        return None

    with db.cursor() as cur:
        local_hash = _get_local_hash(cur, previous_checkpoint_height)
    if local_hash is None:
        return None

    canonical_hash = rpc.get_block_hash(previous_checkpoint_height)
    if local_hash == canonical_hash:
        return None

    with db.cursor() as cur:
        common_height = find_common_ancestor_height(
            previous_checkpoint_height - 1,
            lambda height: _get_local_hash(cur, height),
            rpc.get_block_hash,
        )
        affected_bucket = _get_first_orphan_bucket(cur, common_height)
        if affected_bucket is None:
            affected_bucket = _get_bucket_for_height(cur, common_height)

        cur.execute(
            '''
            INSERT INTO btc_reorg_events(
              previous_checkpoint_height,
              common_height,
              diverged_height,
              rollback_bucket_start_utc,
              old_block_hash,
              canonical_block_hash,
              notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
            (
                previous_checkpoint_height,
                common_height,
                common_height + 1,
                affected_bucket,
                local_hash,
                canonical_hash,
                'Automatic rollback after canonical chain mismatch at the raw ingestion checkpoint.',
            ),
        )

        cur.execute('DELETE FROM btc_blocks_raw WHERE height > %s', (common_height,))
        cur.execute('DELETE FROM btc_primitive_block WHERE height > %s', (common_height,))
        if affected_bucket is not None:
            cur.execute('DELETE FROM btc_primitive_10m WHERE bucket_start_utc >= %s', (affected_bucket,))
            _delete_metric_outputs(cur, affected_bucket)
            cur.execute(
                '''
                UPDATE sync_checkpoints
                SET last_bucket_time = %s,
                    status = %s,
                    error_message = %s,
                    updated_at = now()
                WHERE chain = %s
                  AND interval <> %s
                  AND last_bucket_time IS NOT NULL
                  AND last_bucket_time >= %s
                ''',
                (
                    affected_bucket,
                    'reorg',
                    f'Rollback to common height {common_height}',
                    'BTC',
                    'heartbeat',
                    affected_bucket,
                ),
            )

        cur.execute(
            '''
            UPDATE sync_checkpoints
            SET last_height = CASE
                WHEN last_height IS NULL THEN NULL
                WHEN last_height > %s THEN %s
                ELSE last_height
              END,
              status = %s,
              error_message = %s,
              updated_at = now()
            WHERE chain = %s
              AND component IN (%s, %s, %s)
            ''',
            (
                common_height,
                common_height,
                'reorg',
                f'Rollback to common height {common_height}',
                'BTC',
                RAW_COMPONENT,
                PRIMITIVE_COMPONENT,
                PRIMITIVE_SCHEDULER_COMPONENT,
            ),
        )
        upsert_checkpoint(
            cur,
            'BTC',
            RAW_COMPONENT,
            'block',
            last_height=common_height,
            status='reorg',
            error_message=f'Rollback to common height {common_height}',
        )

    logger.warning(
        'btc_reorg_reconciled previous_checkpoint_height=%s common_height=%s affected_bucket=%s',
        previous_checkpoint_height,
        common_height,
        affected_bucket,
    )
    return {
        'previous_checkpoint_height': previous_checkpoint_height,
        'common_height': common_height,
        'affected_bucket': affected_bucket,
    }
