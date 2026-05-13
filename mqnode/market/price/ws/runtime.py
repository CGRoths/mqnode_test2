from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib import import_module
from typing import Any, Callable

from mqnode.config.logging_config import configure_logging
from mqnode.config.settings import get_settings
from mqnode.core.utils import to_bucket_start_10m, utc_now
from mqnode.db.connection import DB
from mqnode.market.price.lifecycle.models import CLOUD_WS_REMOTE_PUSH, WS_CONFIRMED_CANDLE
from mqnode.market.price.lifecycle.profiles import get_price_source_lifecycle_profile
from mqnode.market.price.registry import get_price_source
from mqnode.market.price.ws.candle_builder import TenMinuteCandleBuilder
from mqnode.market.price.ws.gap_detector import PriceWsGapDetector
from mqnode.market.price.ws.gap_filler import PriceWsGapFiller, get_rest_bucket_fetcher
from mqnode.market.price.ws.models import REST_REPAIRED, WS_CLOSED
from mqnode.market.price.ws.remote_push import RemotePriceWsPusher
from mqnode.market.price.ws.repository import PriceWsRepository
from mqnode.market.price.ws.state import DIRTY_MISSING_1M_CANDLE, DIRTY_STARTUP_PARTIAL_BUCKET

logger = logging.getLogger(__name__)
BucketKey = tuple[str, str, datetime]
LOCAL_DB_OUTPUT_MODE = 'local_db'
REMOTE_PUSH_OUTPUT_MODE = 'remote_push'


@dataclass(frozen=True)
class StartupBucketGuard:
    startup_bucket_start: datetime
    earliest_bucket_start_utc: datetime
    earliest_ws_closed_eligible_bucket_start: datetime
    dirty_reason: str = DIRTY_STARTUP_PARTIAL_BUCKET


def _load_source_connector(source_name: str):
    source_name = source_name.lower()
    module_path = f'mqnode.market.price.ws.sources.{source_name}'
    try:
        module = import_module(module_path)
    except ModuleNotFoundError as exc:
        if exc.name == module_path:
            raise NotImplementedError(f'WS source {source_name} has no connector module.') from exc
        raise
    factory = getattr(module, 'build_connector', None)
    if factory is None:
        raise NotImplementedError(f'WS source {source_name} has no build_connector()')
    return factory()


def _validate_lifecycle_ws_runtime(
    *,
    source_name: str,
    symbol: str,
    settings,
    output_mode: str,
    force: bool = False,
    candidate: bool = False,
    profile=None,
):
    profile = profile or get_price_source_lifecycle_profile(source_name, settings=settings)
    if force or candidate:
        return profile
    if not profile.enabled:
        raise RuntimeError(f'{source_name} WS runtime refused: source lifecycle profile is disabled.')
    if profile.live_mode == CLOUD_WS_REMOTE_PUSH:
        if output_mode != REMOTE_PUSH_OUTPUT_MODE:
            raise RuntimeError(
                f'{source_name} WS runtime refused: cloud mode requires PRICE_WS_OUTPUT_MODE=remote_push.'
            )
        if not getattr(settings, 'price_ws_remote_ingest_url', None):
            raise RuntimeError(f'{source_name} WS runtime refused: PRICE_WS_REMOTE_INGEST_URL is required.')
        return profile
    if profile.live_mode != WS_CONFIRMED_CANDLE:
        raise RuntimeError(
            f'{source_name} WS runtime refused: live_mode={profile.live_mode} is not official ws_closed mode.'
        )
    if not profile.can_write_ws_closed:
        raise RuntimeError(f'{source_name} WS runtime refused: can_write_ws_closed=false for symbol={symbol}.')
    return profile


def _bucket_key(source_name: str, symbol: str, bucket_start_utc: datetime) -> BucketKey:
    return (source_name, symbol, bucket_start_utc)


def _parse_reconnect_backoff_seconds(raw_backoff: str | list[int] | tuple[int, ...]) -> tuple[float, ...]:
    if isinstance(raw_backoff, str):
        values = tuple(float(item.strip()) for item in raw_backoff.split(',') if item.strip())
    else:
        values = tuple(float(item) for item in raw_backoff)
    return values or (1.0,)


def _price_ws_output_mode(settings) -> str:
    output_mode = str(getattr(settings, 'price_ws_output_mode', LOCAL_DB_OUTPUT_MODE)).lower()
    if output_mode not in {LOCAL_DB_OUTPUT_MODE, REMOTE_PUSH_OUTPUT_MODE}:
        raise ValueError(f'Unsupported PRICE_WS_OUTPUT_MODE={output_mode!r}')
    return output_mode


def _row_for_remote_push(row: dict) -> dict:
    row = dict(row)
    if row.get('quality_status') == WS_CLOSED:
        row['data_source_mode'] = 'websocket_confirmed_1m_cloud'
    elif row.get('quality_status') == REST_REPAIRED:
        row['data_source_mode'] = 'rest_live_fallback_cloud'
    return row


def initialize_startup_bucket_guard(
    builder: TenMinuteCandleBuilder,
    *,
    source_name: str,
    symbol: str,
    now: datetime,
) -> StartupBucketGuard:
    startup_bucket_start = to_bucket_start_10m(now)
    builder.mark_dirty(source_name, symbol, startup_bucket_start, DIRTY_STARTUP_PARTIAL_BUCKET)
    return StartupBucketGuard(
        startup_bucket_start=startup_bucket_start,
        earliest_bucket_start_utc=startup_bucket_start,
        earliest_ws_closed_eligible_bucket_start=startup_bucket_start + timedelta(minutes=10),
    )


def _ensure_expected_bucket_states(
    builder: TenMinuteCandleBuilder,
    *,
    source_name: str,
    symbol: str,
    now: datetime,
    earliest_bucket_start_utc: datetime | None = None,
) -> tuple[datetime, datetime]:
    current_bucket = to_bucket_start_10m(now)
    previous_bucket = current_bucket - timedelta(minutes=10)
    if earliest_bucket_start_utc is None or previous_bucket >= earliest_bucket_start_utc:
        builder.state_for(source_name, symbol, previous_bucket)
    builder.state_for(source_name, symbol, current_bucket)
    return previous_bucket, current_bucket


def _ensure_current_bucket_state(
    builder: TenMinuteCandleBuilder,
    *,
    source_name: str,
    symbol: str,
    now: datetime,
) -> datetime:
    current_bucket = to_bucket_start_10m(now)
    builder.state_for(source_name, symbol, current_bucket)
    return current_bucket


def _finalize_ready_buckets(
    *,
    db,
    source_name: str,
    table_name: str,
    symbol: str,
    builder: TenMinuteCandleBuilder,
    gap_filler: PriceWsGapFiller,
    repository: PriceWsRepository,
    now: datetime,
    grace_seconds: int,
    finalized_buckets: set[BucketKey],
    rest_fallback_enabled: bool,
    earliest_bucket_start_utc: datetime | None = None,
    earliest_ws_closed_eligible_bucket_start: datetime | None = None,
    remote_pusher: RemotePriceWsPusher | None = None,
    on_source_row_written: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    _ensure_expected_bucket_states(
        builder,
        source_name=source_name,
        symbol=symbol,
        now=now,
        earliest_bucket_start_utc=earliest_bucket_start_utc,
    )
    for state in builder.state_store.iter_states():
        if state.source_name != source_name or state.symbol != symbol:
            continue
        bucket_key = _bucket_key(state.source_name, state.symbol, state.bucket_start_utc)
        if bucket_key in finalized_buckets:
            continue
        if (
            earliest_ws_closed_eligible_bucket_start is not None
            and state.bucket_start_utc < earliest_ws_closed_eligible_bucket_start
            and not state.is_dirty
        ):
            state.mark_dirty(DIRTY_STARTUP_PARTIAL_BUCKET)
        bucket_ready_at = state.bucket_start_utc + timedelta(minutes=10, seconds=grace_seconds)
        if now < bucket_ready_at:
            continue

        result = builder.finalize_bucket(
            state.source_name,
            state.symbol,
            state.bucket_start_utc,
            closed_at=now,
        )
        if result.candle is not None:
            row = result.candle.to_source_row()
            if remote_pusher is not None:
                remote_pusher.push_rows([_row_for_remote_push(row)])
            else:
                repository.upsert_source_price_row(db, state.source_name, table_name, row)
                if on_source_row_written is not None:
                    on_source_row_written(row)
                repository.mark_source_status(
                    db,
                    source_name=state.source_name,
                    symbol=state.symbol,
                    status=result.quality_status,
                    last_closed_bucket_start_utc=state.bucket_start_utc,
                    current_bucket_start_utc=state.bucket_start_utc,
                    current_bucket_dirty=False,
                )
        elif result.rest_required and rest_fallback_enabled:
            reason = result.rest_required_reason or DIRTY_MISSING_1M_CANDLE
            if remote_pusher is not None:
                repair = gap_filler.resolve_dirty_bucket(
                    source_name=state.source_name,
                    symbol=state.symbol,
                    bucket_start_utc=state.bucket_start_utc,
                    reason=reason,
                    now=now,
                    data_source_mode='rest_live_fallback_cloud',
                )
                if repair.row is not None:
                    remote_pusher.push_rows([_row_for_remote_push(repair.row)])
            else:
                repair = gap_filler.fill_dirty_bucket(
                    db,
                    source_name=state.source_name,
                    table_name=table_name,
                    symbol=state.symbol,
                    bucket_start_utc=state.bucket_start_utc,
                    reason=reason,
                    now=now,
                )
                if on_source_row_written is not None:
                    on_source_row_written(
                        repair.row
                        or {
                            'source_name': state.source_name,
                            'symbol': state.symbol,
                            'bucket_start_utc': state.bucket_start_utc,
                            'quality_status': repair.quality_status,
                        }
                    )
        finalized_buckets.add(bucket_key)


def _mark_ws_disconnected(
    *,
    db,
    source_name: str,
    symbol: str,
    builder: TenMinuteCandleBuilder,
    detector: PriceWsGapDetector,
    repository: PriceWsRepository,
    now: datetime,
    error: str,
) -> None:
    _ensure_current_bucket_state(builder, source_name=source_name, symbol=symbol, now=now)
    state = detector.record_disconnect(source_name, symbol) or builder.state_store.current(source_name, symbol)
    if db is None:
        return
    repository.mark_source_status(
        db,
        source_name=source_name,
        symbol=symbol,
        status='ws_disconnected',
        last_message_at=now,
        current_bucket_start_utc=state.bucket_start_utc if state else None,
        current_bucket_dirty=True,
        dirty_reason=state.dirty_reason if state else 'ws_disconnect',
        last_error=error,
    )


async def _bucket_finalizer_loop(
    *,
    db,
    source_name: str,
    table_name: str,
    symbol: str,
    builder: TenMinuteCandleBuilder,
    detector: PriceWsGapDetector,
    gap_filler: PriceWsGapFiller,
    repository: PriceWsRepository,
    settings,
    finalized_buckets: set[BucketKey],
    earliest_bucket_start_utc: datetime,
    earliest_ws_closed_eligible_bucket_start: datetime | None = None,
    remote_pusher: RemotePriceWsPusher | None = None,
    on_source_row_written: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    while True:
        try:
            now = utc_now()
            _ensure_current_bucket_state(builder, source_name=source_name, symbol=symbol, now=now)
            detector.detect_stale(source_name, symbol, now)
            _finalize_ready_buckets(
                db=db,
                source_name=source_name,
                table_name=table_name,
                symbol=symbol,
                builder=builder,
                gap_filler=gap_filler,
                repository=repository,
                now=now,
                grace_seconds=settings.price_ws_bucket_grace_seconds,
                finalized_buckets=finalized_buckets,
                rest_fallback_enabled=settings.price_ws_rest_fallback_enabled,
                earliest_bucket_start_utc=earliest_bucket_start_utc,
                earliest_ws_closed_eligible_bucket_start=earliest_ws_closed_eligible_bucket_start,
                remote_pusher=remote_pusher,
                on_source_row_written=on_source_row_written,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('price_ws_finalizer_tick_failed source=%s symbol=%s', source_name, symbol)
        await asyncio.sleep(1)


async def run_ws_price_collector(
    *,
    source_name: str,
    symbol: str,
    interval: str,
    settings=None,
    force: bool = False,
    candidate: bool = False,
    on_source_row_written: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    settings = settings or get_settings()
    source_name = source_name.lower()
    if not settings.price_ws_enabled:
        logger.info('price_ws_disabled source=%s symbol=%s', source_name, symbol)
        return

    output_mode = _price_ws_output_mode(settings)
    remote_pusher = None
    if output_mode == REMOTE_PUSH_OUTPUT_MODE:
        db = None
        source = get_price_source(source_name)
        remote_ingest_token = getattr(settings, 'price_ws_remote_ingest_token', None) or getattr(
            settings,
            'internal_ingest_token',
            None,
        )
        remote_pusher = RemotePriceWsPusher(
            ingest_url=getattr(settings, 'price_ws_remote_ingest_url', None),
            token=remote_ingest_token,
            timeout_seconds=getattr(settings, 'price_ws_remote_timeout_seconds', 10),
            max_attempts=getattr(settings, 'price_ws_remote_max_attempts', 3),
        )
    else:
        db = DB(settings)
        with db.cursor() as cur:
            source = get_price_source(source_name, cur)
    profile = get_price_source_lifecycle_profile(source_name, settings=settings, price_source_row=source)
    _validate_lifecycle_ws_runtime(
        source_name=source_name,
        symbol=symbol,
        settings=settings,
        output_mode=output_mode,
        force=force,
        candidate=candidate,
        profile=profile,
    )

    try:
        websockets = import_module('websockets')
    except ImportError as exc:
        raise RuntimeError('Install the websockets package before running the live WS price collector.') from exc

    connector = _load_source_connector(source_name)
    repository = PriceWsRepository()
    builder = TenMinuteCandleBuilder()
    detector = PriceWsGapDetector(
        builder.state_store,
        stale_after=timedelta(seconds=settings.price_ws_stale_seconds),
    )
    gap_filler = PriceWsGapFiller(
        get_rest_bucket_fetcher(source.source_name, timeout_seconds=settings.price_request_timeout_seconds),
        repository,
    )
    finalized_buckets: set[BucketKey] = set()
    startup_guard = initialize_startup_bucket_guard(
        builder,
        source_name=source.source_name,
        symbol=symbol,
        now=utc_now(),
    )
    if db is not None:
        repository.mark_source_status(
            db,
            source_name=source.source_name,
            symbol=symbol,
            status='startup_partial',
            current_bucket_start_utc=startup_guard.startup_bucket_start,
            current_bucket_dirty=True,
            dirty_reason=startup_guard.dirty_reason,
        )
    reconnect_backoff = _parse_reconnect_backoff_seconds(settings.price_ws_reconnect_backoff_seconds)
    finalizer_task = asyncio.create_task(
        _bucket_finalizer_loop(
            db=db,
            source_name=source.source_name,
            table_name=source.table_name,
            symbol=symbol,
            builder=builder,
            detector=detector,
            gap_filler=gap_filler,
            repository=repository,
            settings=settings,
            finalized_buckets=finalized_buckets,
            earliest_bucket_start_utc=startup_guard.earliest_bucket_start_utc,
            earliest_ws_closed_eligible_bucket_start=startup_guard.earliest_ws_closed_eligible_bucket_start,
            remote_pusher=remote_pusher,
            on_source_row_written=on_source_row_written,
        )
    )
    has_connected_once = False
    backoff_index = 0
    try:
        while True:
            try:
                async with websockets.connect(connector.websocket_url) as ws:
                    now = utc_now()
                    _ensure_current_bucket_state(builder, source_name=source.source_name, symbol=symbol, now=now)
                    if has_connected_once:
                        state = detector.record_reconnect(source.source_name, symbol)
                        if db is not None:
                            repository.mark_source_status(
                                db,
                                source_name=source.source_name,
                                symbol=symbol,
                                status='ws_reconnected',
                                last_message_at=now,
                                current_bucket_start_utc=state.bucket_start_utc if state else None,
                                current_bucket_dirty=state.is_dirty if state else True,
                                dirty_reason=state.dirty_reason if state else 'ws_reconnect',
                                reconnect_count=1,
                            )
                    has_connected_once = True
                    backoff_index = 0
                    subscription_payload = connector.subscription_payload(symbol=symbol, interval=interval)
                    if subscription_payload is not None:
                        await ws.send(json.dumps(subscription_payload))
                    async for raw_message in ws:
                        received_at = utc_now()
                        _ensure_current_bucket_state(
                            builder,
                            source_name=source.source_name,
                            symbol=symbol,
                            now=received_at,
                        )
                        for event in connector.parse_message(raw_message, received_at=received_at, symbol=symbol):
                            state = builder.accept(event)
                            if db is not None:
                                repository.mark_source_status(
                                    db,
                                    source_name=event.source_name,
                                    symbol=event.symbol,
                                    status='receiving',
                                    last_message_at=received_at,
                                    current_bucket_start_utc=state.bucket_start_utc,
                                    current_bucket_dirty=state.is_dirty,
                                    dirty_reason=state.dirty_reason,
                                )
                            detector.detect_stale(event.source_name, event.symbol, received_at)
                    _mark_ws_disconnected(
                        db=db,
                        source_name=source.source_name,
                        symbol=symbol,
                        builder=builder,
                        detector=detector,
                        repository=repository,
                        now=utc_now(),
                        error='websocket closed',
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    'price_ws_connection_error source=%s symbol=%s error=%s',
                    source.source_name,
                    symbol,
                    exc,
                )
                _mark_ws_disconnected(
                    db=db,
                    source_name=source.source_name,
                    symbol=symbol,
                    builder=builder,
                    detector=detector,
                    repository=repository,
                    now=utc_now(),
                    error=str(exc),
                )
            sleep_seconds = reconnect_backoff[min(backoff_index, len(reconnect_backoff) - 1)]
            backoff_index += 1
            await asyncio.sleep(sleep_seconds)
    finally:
        finalizer_task.cancel()
        try:
            await finalizer_task
        except asyncio.CancelledError:
            pass


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', default=None)
    parser.add_argument('--symbol', default=None)
    parser.add_argument('--interval', default=None)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--candidate', action='store_true')
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)
    asyncio.run(
        run_ws_price_collector(
            source_name=args.source or settings.price_ws_source,
            symbol=args.symbol or settings.price_ws_symbol,
            interval=args.interval or settings.price_ws_child_interval,
            settings=settings,
            force=args.force,
            candidate=args.candidate,
        )
    )


if __name__ == '__main__':
    main()
