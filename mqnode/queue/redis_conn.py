from __future__ import annotations

from redis import Redis

from mqnode.config.settings import Settings


def get_redis(settings: Settings) -> Redis:
    return Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=True)
