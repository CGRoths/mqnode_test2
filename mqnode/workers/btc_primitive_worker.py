from __future__ import annotations

from datetime import datetime

from mqnode.chains.btc.primitive_builder import catch_up_10m_from_checkpoint, rebuild_10m_buckets_for_height
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB


def process_raw_block_job(payload: dict):
    db = DB(get_settings())
    if payload.get('event') == 'raw_block_ready':
        height = int(payload['height'])
        return rebuild_10m_buckets_for_height(db, height)
    if payload.get('event') == 'primitive_tick':
        as_of_time = datetime.fromisoformat(payload['as_of_time'])
        return catch_up_10m_from_checkpoint(db, settings=get_settings(), emit_queue_event=True, end_time=as_of_time)
    return None


def replay_primitive_startup() -> int:
    settings = get_settings()
    db = DB(settings)
    return catch_up_10m_from_checkpoint(db, settings=settings, emit_queue_event=True)
