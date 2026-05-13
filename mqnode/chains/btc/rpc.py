from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

from mqnode.config.settings import Settings
from mqnode.core.errors import RpcError

logger = logging.getLogger(__name__)


class BitcoinRPC:
    def __init__(self, settings: Settings):
        self.url = settings.btc_rpc_url
        self.auth = (settings.btc_rpc_user, settings.btc_rpc_password)
        self.timeout = settings.btc_rpc_timeout

    def call(self, method: str, params: list[Any] | None = None, retries: int = 5) -> Any:
        payload = {'jsonrpc': '1.0', 'id': 'mqnode', 'method': method, 'params': params or []}
        for attempt in range(retries):
            try:
                r = requests.post(self.url, json=payload, auth=self.auth, timeout=self.timeout)
                r.raise_for_status()
                body = r.json()
                if body.get('error'):
                    raise RpcError(str(body['error']))
                return body['result']
            except Exception as exc:
                sleep = 2**attempt
                logger.warning('rpc_retry method=%s attempt=%s error=%s sleep=%s', method, attempt + 1, exc, sleep)
                time.sleep(sleep)
        raise RpcError(f'RPC call failed after retries: {method}')

    def get_block_count(self) -> int:
        return int(self.call('getblockcount'))

    def get_block_hash(self, height: int) -> str:
        return str(self.call('getblockhash', [height]))

    def get_block(self, block_hash: str) -> dict[str, Any]:
        return self.call('getblock', [block_hash, 2])


def ts_from_unix(ts: int | None) -> datetime | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)
