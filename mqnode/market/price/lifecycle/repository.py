from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from mqnode.core.utils import utc_now
from mqnode.market.price.lifecycle.models import (
    ERROR,
    LIVE_HEALTHY,
    REST_BACKFILLING,
    REST_CONFIRMING,
    REST_FALLBACK,
    REST_SYNCED,
    WS_DIRTY,
    WS_LIVE,
    WS_STARTING,
    PriceSourceLifecycleProfile,
)

STATUS_FIELDS = {
    'rest_sync_status',
    'ws_status',
    'last_rest_bucket',
    'last_ws_bucket',
    'last_canonical_bucket',
    'rest_synced_at',
    'ws_started_at',
    'last_error',
    'metadata_json',
}


class PriceSourceLifecycleRepository:
    def __init__(self, db=None) -> None:
        self.db = db

    def _db(self, db=None):
        target = db or self.db
        if target is None:
            raise ValueError('PriceSourceLifecycleRepository requires a db.')
        return target

    def upsert_status(
        self,
        profile: PriceSourceLifecycleProfile,
        current_state: str,
        *,
        db=None,
        **kwargs,
    ) -> None:
        values = self._status_params(profile, current_state, **kwargs)
        with self._db(db).cursor() as cur:
            cur.execute(
                '''
                INSERT INTO mq_price_source_lifecycle_status(
                  source_name,
                  symbol,
                  table_name,
                  enabled,
                  historical_mode,
                  live_mode,
                  current_state,
                  rest_sync_status,
                  ws_status,
                  last_rest_bucket,
                  last_ws_bucket,
                  last_canonical_bucket,
                  rest_synced_at,
                  ws_started_at,
                  last_transition_at,
                  last_error,
                  metadata_json,
                  updated_at
                ) VALUES (
                  %(source_name)s,
                  %(symbol)s,
                  %(table_name)s,
                  %(enabled)s,
                  %(historical_mode)s,
                  %(live_mode)s,
                  %(current_state)s,
                  %(rest_sync_status)s,
                  %(ws_status)s,
                  %(last_rest_bucket)s,
                  %(last_ws_bucket)s,
                  %(last_canonical_bucket)s,
                  %(rest_synced_at)s,
                  %(ws_started_at)s,
                  %(last_transition_at)s,
                  %(last_error)s,
                  %(metadata_json)s::jsonb,
                  %(updated_at)s
                )
                ON CONFLICT (source_name, symbol) DO UPDATE SET
                  table_name = EXCLUDED.table_name,
                  enabled = EXCLUDED.enabled,
                  historical_mode = EXCLUDED.historical_mode,
                  live_mode = EXCLUDED.live_mode,
                  current_state = EXCLUDED.current_state,
                  rest_sync_status = EXCLUDED.rest_sync_status,
                  ws_status = EXCLUDED.ws_status,
                  last_rest_bucket = COALESCE(
                    EXCLUDED.last_rest_bucket,
                    mq_price_source_lifecycle_status.last_rest_bucket
                  ),
                  last_ws_bucket = COALESCE(
                    EXCLUDED.last_ws_bucket,
                    mq_price_source_lifecycle_status.last_ws_bucket
                  ),
                  last_canonical_bucket = COALESCE(
                    EXCLUDED.last_canonical_bucket,
                    mq_price_source_lifecycle_status.last_canonical_bucket
                  ),
                  rest_synced_at = COALESCE(EXCLUDED.rest_synced_at, mq_price_source_lifecycle_status.rest_synced_at),
                  ws_started_at = COALESCE(EXCLUDED.ws_started_at, mq_price_source_lifecycle_status.ws_started_at),
                  last_transition_at = EXCLUDED.last_transition_at,
                  last_error = EXCLUDED.last_error,
                  metadata_json = EXCLUDED.metadata_json,
                  updated_at = EXCLUDED.updated_at
                ''',
                values,
            )

    def _status_params(
        self,
        profile: PriceSourceLifecycleProfile,
        current_state: str,
        **kwargs,
    ) -> dict[str, Any]:
        now = utc_now()
        metadata_json = kwargs.get('metadata_json')
        if metadata_json is None:
            metadata_json = {}
        return {
            'source_name': profile.source_name,
            'symbol': profile.symbol,
            'table_name': profile.table_name,
            'enabled': profile.enabled,
            'historical_mode': profile.historical_mode,
            'live_mode': profile.live_mode,
            'current_state': current_state,
            'rest_sync_status': kwargs.get('rest_sync_status'),
            'ws_status': kwargs.get('ws_status'),
            'last_rest_bucket': kwargs.get('last_rest_bucket'),
            'last_ws_bucket': kwargs.get('last_ws_bucket'),
            'last_canonical_bucket': kwargs.get('last_canonical_bucket'),
            'rest_synced_at': kwargs.get('rest_synced_at'),
            'ws_started_at': kwargs.get('ws_started_at'),
            'last_transition_at': kwargs.get('last_transition_at') or now,
            'last_error': kwargs.get('last_error'),
            'metadata_json': json.dumps(metadata_json),
            'updated_at': now,
        }

    def get_status(self, source_name: str, symbol: str, *, db=None) -> dict[str, Any] | None:
        with self._db(db).cursor() as cur:
            cur.execute(
                '''
                SELECT *
                FROM mq_price_source_lifecycle_status
                WHERE source_name = %s AND symbol = %s
                ''',
                (source_name, symbol),
            )
            return cur.fetchone()

    def list_statuses(self, *, db=None) -> list[dict[str, Any]]:
        with self._db(db).cursor() as cur:
            cur.execute(
                '''
                SELECT *
                FROM mq_price_source_lifecycle_status
                ORDER BY source_name ASC, symbol ASC
                '''
            )
            return list(cur.fetchall())

    def mark_rest_backfilling(
        self,
        profile: PriceSourceLifecycleProfile,
        last_rest_bucket: datetime | None = None,
        *,
        db=None,
    ) -> None:
        self.upsert_status(
            profile,
            REST_BACKFILLING,
            db=db,
            rest_sync_status='backfilling',
            last_rest_bucket=last_rest_bucket,
        )

    def mark_rest_synced(
        self,
        profile: PriceSourceLifecycleProfile,
        last_rest_bucket: datetime | None = None,
        *,
        db=None,
    ) -> None:
        self.upsert_status(
            profile,
            REST_SYNCED,
            db=db,
            rest_sync_status='synced',
            last_rest_bucket=last_rest_bucket,
            rest_synced_at=utc_now(),
        )

    def mark_ws_starting(self, profile: PriceSourceLifecycleProfile, *, db=None) -> None:
        self.upsert_status(profile, WS_STARTING, db=db, ws_status='starting', ws_started_at=utc_now())

    def mark_ws_live(
        self,
        profile: PriceSourceLifecycleProfile,
        last_ws_bucket: datetime | None = None,
        *,
        db=None,
    ) -> None:
        self.upsert_status(
            profile,
            WS_LIVE,
            db=db,
            ws_status='live',
            last_ws_bucket=last_ws_bucket,
            ws_started_at=utc_now(),
        )

    def mark_ws_dirty(self, profile: PriceSourceLifecycleProfile, reason: str | None = None, *, db=None) -> None:
        self.upsert_status(
            profile,
            WS_DIRTY,
            db=db,
            ws_status='dirty',
            last_error=reason,
            metadata_json={'reason': reason},
        )

    def mark_rest_fallback(self, profile: PriceSourceLifecycleProfile, reason: str | None = None, *, db=None) -> None:
        self.upsert_status(
            profile,
            REST_FALLBACK,
            db=db,
            rest_sync_status='fallback',
            last_error=reason,
            metadata_json={'reason': reason},
        )

    def mark_rest_confirming(
        self,
        profile: PriceSourceLifecycleProfile,
        bucket_start: datetime | None = None,
        *,
        db=None,
    ) -> None:
        self.upsert_status(
            profile,
            REST_CONFIRMING,
            db=db,
            rest_sync_status='confirming',
            metadata_json={'bucket_start': bucket_start.isoformat() if bucket_start else None},
        )

    def mark_live_healthy(self, profile: PriceSourceLifecycleProfile, *, db=None) -> None:
        self.upsert_status(profile, LIVE_HEALTHY, db=db, rest_sync_status='synced', ws_status='healthy')

    def mark_error(self, profile: PriceSourceLifecycleProfile, error: str, *, db=None) -> None:
        self.upsert_status(profile, ERROR, db=db, last_error=error, metadata_json={'error': error})
