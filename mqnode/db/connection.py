from __future__ import annotations

from contextlib import contextmanager
from threading import Lock

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from mqnode.config.settings import Settings


class DB:
    _pools: dict[str, ThreadedConnectionPool] = {}
    _pool_lock = Lock()

    def __init__(self, settings: Settings):
        self.dsn = settings.postgres_dsn
        self.minconn = settings.postgres_pool_minconn
        self.maxconn = settings.postgres_pool_maxconn

    def _get_pool(self) -> ThreadedConnectionPool:
        pool = self._pools.get(self.dsn)
        if pool is not None:
            return pool

        with self._pool_lock:
            pool = self._pools.get(self.dsn)
            if pool is None:
                pool = ThreadedConnectionPool(self.minconn, self.maxconn, self.dsn)
                self._pools[self.dsn] = pool
        return pool

    @contextmanager
    def cursor(self):
        pool = self._get_pool()
        conn = pool.getconn()
        try:
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    yield cur
        finally:
            pool.putconn(conn)
