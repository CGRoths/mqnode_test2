from __future__ import annotations

from dataclasses import dataclass

from mqnode.config.settings import Settings, get_settings
from mqnode.db.connection import DB
from mqnode.queue.redis_conn import get_redis


@dataclass
class AppContext:
    settings: Settings
    db: DB


def build_context() -> AppContext:
    settings = get_settings()
    _ = get_redis(settings)
    return AppContext(settings=settings, db=DB(settings))
