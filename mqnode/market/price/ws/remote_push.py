from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import requests

from mqnode.market.price.source_support import TRANSIENT_STATUS_CODES


class RemoteWsPushError(RuntimeError):
    pass


class RemoteWsPushAuthError(RemoteWsPushError):
    pass


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


class RemotePriceWsPusher:
    def __init__(
        self,
        *,
        ingest_url: str | None,
        token: str | None,
        timeout_seconds: int = 10,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:
        if not ingest_url:
            raise RemoteWsPushError('PRICE_WS_REMOTE_INGEST_URL must be set for remote_push output mode.')
        if not token:
            raise RemoteWsPushAuthError(
                'PRICE_WS_REMOTE_INGEST_TOKEN or MQNODE_INTERNAL_INGEST_TOKEN must be set for remote_push output mode.'
            )
        self.ingest_url = ingest_url
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max(1, max_attempts)
        self.backoff_seconds = backoff_seconds

    def push_rows(self, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not rows:
            return None
        payload = {'rows': [_json_safe(row) for row in rows]}
        headers = {'Authorization': f'Bearer {self.token}'}
        attempt = 1
        while True:
            try:
                response = requests.post(
                    self.ingest_url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt >= self.max_attempts:
                    raise RemoteWsPushError('remote price WS ingest POST failed') from exc
            else:
                if response.status_code in {401, 403}:
                    raise RemoteWsPushAuthError(
                        f'remote price WS ingest authentication failed with status {response.status_code}'
                    )
                if response.status_code not in TRANSIENT_STATUS_CODES or attempt >= self.max_attempts:
                    response.raise_for_status()
                    try:
                        return response.json()
                    except ValueError:
                        return None
            time.sleep(self.backoff_seconds * attempt)
            attempt += 1
