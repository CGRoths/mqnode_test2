from __future__ import annotations

from datetime import datetime, timezone

from mqnode.market.price.lifecycle.profiles import get_price_source_lifecycle_profile
from mqnode.market.price.source_worker import SourcePriceWorker
from mqnode.market.price.ws.models import REST_REPAIRED, WS_CLOSED


def test_source_row_write_hook_recomposes_ws_closed_and_schedules_confirmation():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    composed = []

    def fake_compose(db, bucket_start_utc, settings=None):
        composed.append((db, bucket_start_utc, settings))
        return {'bucket_start_utc': bucket_start_utc}

    worker = SourcePriceWorker('bybit', db=object(), compose_fn=fake_compose)
    profile = get_price_source_lifecycle_profile('bybit')

    worker.on_source_row_written(
        {'bucket_start_utc': bucket, 'quality_status': WS_CLOSED},
        db='db',
        profile=profile,
    )

    assert composed[0][1] == bucket
    assert bucket in worker._confirmations


def test_source_row_write_hook_recomposes_rest_repaired_without_confirmation():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    composed = []

    worker = SourcePriceWorker(
        'bybit',
        db=object(),
        compose_fn=lambda db, bucket_start_utc, settings=None: composed.append(bucket_start_utc),
    )
    profile = get_price_source_lifecycle_profile('bybit')

    worker.on_source_row_written(
        {'bucket_start_utc': bucket, 'quality_status': REST_REPAIRED},
        db='db',
        profile=profile,
    )

    assert composed == [bucket]
    assert worker._confirmations == {}
