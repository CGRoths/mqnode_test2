from __future__ import annotations

import argparse

from mqnode.chains.btc.ingest import ingest_block
from mqnode.chains.btc.listener import RAW_COMPONENT
from mqnode.chains.btc.rpc import BitcoinRPC
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.db.repositories import get_checkpoint, upsert_checkpoint
from mqnode.queue.producer import enqueue_raw_block_ready


def _resolve_height_range(args, db, rpc: BitcoinRPC) -> tuple[int, int]:
    node_tip = rpc.get_block_count()
    if args.mode == 'full':
        return 0, node_tip

    if args.mode == 'resume':
        with db.cursor() as cur:
            checkpoint = get_checkpoint(cur, 'BTC', RAW_COMPONENT, 'block')
        start_height = int(checkpoint.get('last_height') or 0) + 1
        return start_height, node_tip

    if args.start_height is None or args.end_height is None:
        raise ValueError('--start-height and --end-height are required for --mode range')
    return args.start_height, args.end_height


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['full', 'resume', 'range'], default='resume')
    parser.add_argument('--start-height', type=int)
    parser.add_argument('--end-height', type=int)
    args = parser.parse_args()

    settings = get_settings()
    db = DB(settings)
    rpc = BitcoinRPC(settings)
    start_height, end_height = _resolve_height_range(args, db, rpc)

    if start_height > end_height:
        print(f'No blocks to process for mode={args.mode}. start_height={start_height} end_height={end_height}')
        raise SystemExit(0)

    with db.cursor() as cur:
        cur.execute(
            '''
            SELECT COALESCE(MAX(cumulative_supply_sat), 0) AS supply_total_sat
            FROM btc_primitive_block
            WHERE height < %s
            ''',
            (start_height,),
        )
        supply = int((cur.fetchone() or {}).get('supply_total_sat') or 0)

    for height in range(start_height, end_height + 1):
        try:
            with db.cursor() as cur:
                supply = ingest_block(cur, rpc, height, supply)
                upsert_checkpoint(cur, 'BTC', RAW_COMPONENT, 'block', last_height=height, status='ok')
            enqueue_raw_block_ready(height)
            print(f'Backfilled block {height}')
        except Exception as exc:
            with db.cursor() as cur:
                upsert_checkpoint(
                    cur,
                    'BTC',
                    RAW_COMPONENT,
                    'block',
                    last_height=height - 1,
                    status='error',
                    error_message=str(exc),
                )
            raise


if __name__ == '__main__':
    main()
