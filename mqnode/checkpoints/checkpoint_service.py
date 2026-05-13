from __future__ import annotations

from mqnode.db.repositories import upsert_checkpoint


def checkpoint_ok(
    cur,
    chain: str,
    component: str,
    interval: str,
    last_height=None,
    last_bucket_time=None,
):
    upsert_checkpoint(
        cur,
        chain,
        component,
        interval,
        last_height=last_height,
        last_bucket_time=last_bucket_time,
        status='ok',
    )


def checkpoint_error(
    cur,
    chain: str,
    component: str,
    interval: str,
    error_message: str,
    last_height=None,
    last_bucket_time=None,
):
    upsert_checkpoint(
        cur,
        chain,
        component,
        interval,
        last_height=last_height,
        last_bucket_time=last_bucket_time,
        status='error',
        error_message=error_message,
    )
