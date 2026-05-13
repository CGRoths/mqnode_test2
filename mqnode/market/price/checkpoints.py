from __future__ import annotations

from datetime import timedelta

from mqnode.checkpoints.checkpoint_service import checkpoint_error, checkpoint_ok
from mqnode.core.utils import to_bucket_start_10m, utc_now
from mqnode.db.repositories import get_checkpoint

PRICE_CANONICAL_COMPONENT = 'btc_price_canonical_composer'
PRICE_SOURCE_COMPONENT_PREFIX = 'btc_price_source'


def price_checkpoint_ok(cur, component: str = PRICE_CANONICAL_COMPONENT, last_bucket_time=None):
    checkpoint_ok(cur, 'BTC', component, '10m', last_bucket_time=last_bucket_time)


def price_checkpoint_error(cur, error_message: str, component: str = PRICE_CANONICAL_COMPONENT, last_bucket_time=None):
    checkpoint_error(cur, 'BTC', component, '10m', error_message, last_bucket_time=last_bucket_time)


def price_source_component(source_name: str) -> str:
    return f'{PRICE_SOURCE_COMPONENT_PREFIX}_{source_name}'


def price_source_checkpoint_ok(cur, source_name: str, last_bucket_time=None):
    checkpoint_ok(cur, 'BTC', price_source_component(source_name), '10m', last_bucket_time=last_bucket_time)


def price_source_checkpoint_error(cur, source_name: str, error_message: str, last_bucket_time=None):
    checkpoint_error(
        cur,
        'BTC',
        price_source_component(source_name),
        '10m',
        error_message,
        last_bucket_time=last_bucket_time,
    )


def price_source_replay_start(cur, source_name: str, lookback_hours: int = 48):
    checkpoint = get_checkpoint(cur, 'BTC', price_source_component(source_name), '10m')
    last_bucket_time = checkpoint.get('last_bucket_time')
    if last_bucket_time is not None:
        return to_bucket_start_10m(last_bucket_time - timedelta(minutes=10))
    return to_bucket_start_10m(utc_now() - timedelta(hours=lookback_hours))
