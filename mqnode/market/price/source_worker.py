from __future__ import annotations

import asyncio
import inspect
import logging
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from importlib import import_module
from typing import Any, Callable

from mqnode.config.settings import get_settings
from mqnode.core.utils import to_bucket_start_10m, utc_now
from mqnode.db.connection import DB
from mqnode.market.price.composer import rebuild_canonical_price_bucket
from mqnode.market.price.lifecycle.manager import PriceSourceLifecycleManager
from mqnode.market.price.lifecycle.models import (
    CLOUD_WS_REMOTE_PUSH,
    LifecycleDecision,
    PriceSourceLifecycleProfile,
)
from mqnode.market.price.lifecycle.profiles import get_price_source_lifecycle_profile
from mqnode.market.price.registry import get_price_source
from mqnode.market.price.ws.gap_filler import RestBucketFetcher, get_rest_bucket_fetcher
from mqnode.market.price.ws.models import WS_CLOSED, ensure_utc
from mqnode.market.price.ws.reconciler import PriceWsReconciler, ReconciliationResult
from mqnode.market.price.ws.repository import PriceWsRepository
from mqnode.market.price.ws.runtime import REMOTE_PUSH_OUTPUT_MODE, run_ws_price_collector

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceWorkerRunResult:
    source_name: str
    action: str
    decision: LifecycleDecision
    rows_written: int = 0


@dataclass(frozen=True)
class BucketConfirmationResult:
    source_name: str
    bucket_start_utc: datetime
    attempted: bool
    quality_status: str | None
    revised: bool = False
    reason: str | None = None
    recomposed: bool = False


def compose_after_source_write(db, bucket_start_utc: datetime, settings=None) -> dict[str, Any] | None:
    return rebuild_canonical_price_bucket(db, bucket_start_utc, settings=settings)


def catch_up_price_source(source_name: str, db, settings=None) -> int:
    source_name = source_name.lower()
    module_paths = [f'mqnode.market.price.{source_name}.rest']
    try:
        source = get_price_source(source_name)
        module_paths.append(source.module_path)
    except KeyError:
        pass

    last_error: Exception | None = None
    for module_path in module_paths:
        try:
            module = import_module(module_path)
        except ModuleNotFoundError as exc:
            if exc.name == module_path:
                last_error = exc
                continue
            raise
        runner = getattr(module, 'run_rest_once', None) or getattr(module, 'fetch_buckets', None)
        if runner is None:
            continue
        return int(runner(db=db, settings=settings))
    raise NotImplementedError(f'No REST catch-up callable configured for price source: {source_name}') from last_error


run_price_source_rest_once = catch_up_price_source


def confirm_ws_bucket(
    db,
    *,
    source_name: str,
    table_name: str,
    symbol: str,
    bucket_start_utc: datetime,
    rest_fetcher: RestBucketFetcher,
    reconciler: PriceWsReconciler | None = None,
    repository: PriceWsRepository | None = None,
    settings=None,
    compose_fn: Callable[[Any, datetime, Any], dict[str, Any] | None] | None = compose_after_source_write,
    now: datetime | None = None,
) -> BucketConfirmationResult:
    repository = repository or PriceWsRepository()
    reconciler = reconciler or PriceWsReconciler(repository=repository)
    bucket_start_utc = to_bucket_start_10m(ensure_utc(bucket_start_utc))
    source_row = repository.get_source_price_row(db, table_name, bucket_start_utc)
    if source_row is None:
        return BucketConfirmationResult(
            source_name,
            bucket_start_utc,
            attempted=False,
            quality_status=None,
            reason='source_row_missing',
        )
    if source_row.get('quality_status') != WS_CLOSED:
        return BucketConfirmationResult(
            source_name,
            bucket_start_utc,
            attempted=False,
            quality_status=source_row.get('quality_status'),
            reason='source_row_not_ws_closed',
        )

    rest_row = rest_fetcher.fetch_bucket(source_name, symbol, bucket_start_utc)
    if rest_row is None:
        return BucketConfirmationResult(
            source_name,
            bucket_start_utc,
            attempted=True,
            quality_status=WS_CLOSED,
            reason='rest_confirmation_row_missing',
        )

    reconciled: ReconciliationResult = reconciler.reconcile(
        db,
        source_name=source_name,
        table_name=table_name,
        ws_row=source_row,
        rest_row=rest_row,
        now=now or utc_now(),
    )
    recomposed = False
    if compose_fn is not None:
        compose_fn(db, bucket_start_utc, settings=settings)
        recomposed = True
    return BucketConfirmationResult(
        source_name,
        bucket_start_utc,
        attempted=True,
        quality_status=reconciled.quality_status,
        revised=reconciled.revised,
        reason=reconciled.reason,
        recomposed=recomposed,
    )


class SourcePriceWorker:
    def __init__(
        self,
        source_name: str,
        *,
        db=None,
        settings=None,
        symbol: str | None = None,
        lifecycle_manager: PriceSourceLifecycleManager | None = None,
        rest_catch_up: Callable[[str, Any, Any], int] | None = None,
        ws_runner: Callable[..., Any] | None = None,
        repository: PriceWsRepository | None = None,
        rest_fetcher_factory: Callable[..., RestBucketFetcher] | None = None,
        compose_fn: Callable[[Any, datetime, Any], dict[str, Any] | None] | None = compose_after_source_write,
        now_fn: Callable[[], datetime] = utc_now,
    ) -> None:
        self.source_name = source_name.lower()
        self.db = db
        self.settings = settings or get_settings()
        self.symbol = symbol
        self.lifecycle_manager = lifecycle_manager or PriceSourceLifecycleManager(self.settings)
        self.rest_catch_up = rest_catch_up or catch_up_price_source
        self.ws_runner = ws_runner or run_ws_price_collector
        self.repository = repository or PriceWsRepository()
        self.rest_fetcher_factory = rest_fetcher_factory or get_rest_bucket_fetcher
        self.compose_fn = compose_fn
        self.now_fn = now_fn
        self._confirmations: dict[datetime, datetime] = {}

    def _resolve_db(self, db=None):
        return db or self.db or DB(self.settings)

    def load_profile(self, db=None) -> PriceSourceLifecycleProfile:
        source_row = None
        db = db or self.db
        if db is not None:
            with db.cursor() as cur:
                source_row = get_price_source(self.source_name, cur)
        profile = get_price_source_lifecycle_profile(
            self.source_name,
            settings=self.settings,
            price_source_row=source_row,
        )
        if self.symbol is not None:
            profile = replace(profile, symbol=self.symbol)
        return profile

    def evaluate(self, db=None, now: datetime | None = None) -> tuple[PriceSourceLifecycleProfile, LifecycleDecision]:
        db = self._resolve_db(db)
        profile = self.load_profile(db)
        decision = self.lifecycle_manager.evaluate_source(db, profile, now=now or self.now_fn())
        return profile, decision

    def run_once(self, db=None, now: datetime | None = None) -> SourceWorkerRunResult:
        db = self._resolve_db(db)
        profile, decision = self.evaluate(db, now=now)

        if decision.recommended_action == 'continue_rest_backfill':
            rows_written = self.run_rest_catch_up(db)
            return SourceWorkerRunResult(self.source_name, 'rest_catch_up', decision, rows_written)

        if decision.should_start_ws:
            self.start_ws_runtime(db, profile)
            return SourceWorkerRunResult(self.source_name, 'start_ws_live', decision, 0)

        if decision.expects_remote_push:
            if self._should_start_cloud_remote_push_ws(profile):
                self.start_ws_runtime(db, profile, force_remote_push=True)
                return SourceWorkerRunResult(self.source_name, 'start_cloud_remote_push_ws', decision, 0)
            return SourceWorkerRunResult(self.source_name, 'wait_for_remote_push', decision, 0)

        if decision.should_keep_rest_live:
            rows_written = self.run_rest_catch_up(db)
            return SourceWorkerRunResult(self.source_name, 'rest_live_poll', decision, rows_written)

        return SourceWorkerRunResult(self.source_name, decision.recommended_action, decision, 0)

    def run_forever(
        self,
        poll_seconds: int | None = None,
        confirmation_poll_seconds: int | None = None,
        max_cycles: int | None = None,
    ) -> list[SourceWorkerRunResult]:
        return asyncio.run(
            self.async_run_forever(
                poll_seconds=poll_seconds,
                confirmation_poll_seconds=confirmation_poll_seconds,
                max_cycles=max_cycles,
            )
        )

    async def async_run_forever(
        self,
        poll_seconds: int | None = None,
        confirmation_poll_seconds: int | None = None,
        max_cycles: int | None = None,
    ) -> list[SourceWorkerRunResult]:
        db = self._resolve_db()
        poll_seconds = self._poll_seconds(poll_seconds)
        confirmation_poll_seconds = self._confirmation_poll_seconds(confirmation_poll_seconds)
        results: list[SourceWorkerRunResult] = []
        cycles = 0

        while max_cycles is None or cycles < max_cycles:
            cycles += 1
            result: SourceWorkerRunResult | None = None
            try:
                profile, decision = self.evaluate(db)
                if decision.recommended_action == 'continue_rest_backfill':
                    rows_written = self.run_rest_catch_up(db)
                    logger.info(
                        'price_source_worker_rest_catch_up source=%s rows=%s',
                        self.source_name,
                        rows_written,
                    )
                    result = SourceWorkerRunResult(self.source_name, 'rest_catch_up', decision, rows_written)
                elif decision.should_start_ws:
                    logger.info('price_source_worker_start_ws source=%s symbol=%s', self.source_name, profile.symbol)
                    await self.start_ws_runtime_async(
                        db,
                        profile,
                        confirmation_poll_seconds=confirmation_poll_seconds,
                    )
                    result = SourceWorkerRunResult(self.source_name, 'start_ws_live', decision, 0)
                elif decision.expects_remote_push:
                    if self._should_start_cloud_remote_push_ws(profile):
                        logger.info(
                            'price_source_worker_start_cloud_remote_push_ws source=%s symbol=%s',
                            self.source_name,
                            profile.symbol,
                        )
                        await self.start_ws_runtime_async(
                            db,
                            profile,
                            force_remote_push=True,
                            confirmation_poll_seconds=confirmation_poll_seconds,
                        )
                        result = SourceWorkerRunResult(self.source_name, 'start_cloud_remote_push_ws', decision, 0)
                    else:
                        rows_written = self.run_rest_catch_up(db) if decision.should_keep_rest_live else 0
                        result = SourceWorkerRunResult(self.source_name, 'wait_for_remote_push', decision, rows_written)
                elif decision.should_keep_rest_live:
                    rows_written = self.run_rest_catch_up(db)
                    logger.info(
                        'price_source_worker_rest_live_poll source=%s rows=%s',
                        self.source_name,
                        rows_written,
                    )
                    result = SourceWorkerRunResult(self.source_name, 'rest_live_poll', decision, rows_written)
                else:
                    result = SourceWorkerRunResult(self.source_name, decision.recommended_action, decision, 0)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('price_source_worker_cycle_failed source=%s', self.source_name)

            if result is not None:
                results.append(result)
            if max_cycles is not None and cycles >= max_cycles:
                break
            await asyncio.sleep(poll_seconds)

        return results

    def _poll_seconds(self, poll_seconds: int | None) -> int:
        if poll_seconds is not None:
            return int(poll_seconds)
        return int(
            getattr(
                self.settings,
                'price_source_worker_poll_seconds',
                getattr(self.settings, 'price_source_ingestion_sleep_seconds', 60),
            )
        )

    def _confirmation_poll_seconds(self, confirmation_poll_seconds: int | None) -> int:
        if confirmation_poll_seconds is not None:
            return int(confirmation_poll_seconds)
        return int(getattr(self.settings, 'price_source_worker_confirmation_poll_seconds', 1))

    def run_rest_catch_up(self, db=None) -> int:
        return int(self.rest_catch_up(self.source_name, db or self._resolve_db(), self.settings))

    def start_ws_runtime(
        self,
        db,
        profile: PriceSourceLifecycleProfile,
        *,
        force_remote_push: bool = False,
    ) -> None:
        def on_source_row_written(row: dict[str, Any]) -> None:
            self.on_source_row_written(row, db=db, profile=profile)

        result = self.ws_runner(
            source_name=profile.ws_source_name or profile.source_name,
            symbol=profile.symbol,
            interval=profile.child_interval,
            settings=self.settings,
            on_source_row_written=on_source_row_written,
        )
        if inspect.isawaitable(result):
            asyncio.run(result)

    async def start_ws_runtime_async(
        self,
        db,
        profile: PriceSourceLifecycleProfile,
        *,
        force_remote_push: bool = False,
        confirmation_poll_seconds: int | None = None,
    ) -> None:
        confirmation_task = asyncio.create_task(
            self.confirmation_loop(
                db,
                poll_seconds=self._confirmation_poll_seconds(confirmation_poll_seconds),
            )
        )
        ws_task = asyncio.create_task(self._run_ws_runner_async(db, profile, force_remote_push=force_remote_push))
        try:
            await ws_task
        finally:
            confirmation_task.cancel()
            with suppress(asyncio.CancelledError):
                await confirmation_task

    async def _run_ws_runner_async(
        self,
        db,
        profile: PriceSourceLifecycleProfile,
        *,
        force_remote_push: bool = False,
    ) -> None:
        def on_source_row_written(row: dict[str, Any]) -> None:
            self.on_source_row_written(row, db=db, profile=profile)

        result = self.ws_runner(
            source_name=profile.ws_source_name or profile.source_name,
            symbol=profile.symbol,
            interval=profile.child_interval,
            settings=self.settings,
            on_source_row_written=on_source_row_written,
        )
        if inspect.isawaitable(result):
            await result

    def _should_start_cloud_remote_push_ws(self, profile: PriceSourceLifecycleProfile) -> bool:
        if profile.live_mode != CLOUD_WS_REMOTE_PUSH:
            return False
        output_mode = str(getattr(self.settings, 'price_ws_output_mode', '')).lower()
        return output_mode == REMOTE_PUSH_OUTPUT_MODE and bool(
            getattr(self.settings, 'price_ws_remote_ingest_url', None)
        )

    def on_source_row_written(
        self,
        row: dict[str, Any],
        *,
        db=None,
        profile: PriceSourceLifecycleProfile | None = None,
    ) -> dict[str, Any] | None:
        if row.get('bucket_start_utc') is None:
            return None
        db = self._resolve_db(db)
        bucket_start_utc = to_bucket_start_10m(ensure_utc(row['bucket_start_utc']))
        composed = self.compose_fn(db, bucket_start_utc, settings=self.settings) if self.compose_fn else None
        profile = profile or self.load_profile(db)
        if row.get('quality_status') == WS_CLOSED and profile.rest_confirm_enabled:
            self.schedule_confirmation(
                bucket_start_utc,
                delay_seconds=profile.rest_confirm_delay_seconds,
                now=self.now_fn(),
            )
        return composed

    def schedule_confirmation(
        self,
        bucket_start_utc: datetime,
        *,
        delay_seconds: int | None = None,
        now: datetime | None = None,
    ) -> datetime:
        bucket_start_utc = to_bucket_start_10m(ensure_utc(bucket_start_utc))
        delay_seconds = (
            int(delay_seconds)
            if delay_seconds is not None
            else int(getattr(self.settings, 'price_ws_rest_confirm_delay_seconds', 30))
        )
        due_at = ensure_utc(now or self.now_fn()) + timedelta(seconds=delay_seconds)
        self._confirmations[bucket_start_utc] = due_at
        return due_at

    def confirm_due_buckets(self, db=None, *, now: datetime | None = None) -> list[BucketConfirmationResult]:
        db = self._resolve_db(db)
        now = ensure_utc(now or self.now_fn())
        due_buckets = sorted(bucket for bucket, due_at in self._confirmations.items() if due_at <= now)
        results = [self.confirm_bucket(db, bucket) for bucket in due_buckets]
        for bucket in due_buckets:
            self._confirmations.pop(bucket, None)
        return results

    async def confirmation_loop(self, db, *, poll_seconds: int = 1) -> None:
        while True:
            self.confirm_due_buckets(db)
            await asyncio.sleep(poll_seconds)

    def confirm_bucket(self, db, bucket_start_utc: datetime) -> BucketConfirmationResult:
        profile = self.load_profile(db)
        fetcher = self.rest_fetcher_factory(
            profile.rest_source_name or profile.source_name,
            timeout_seconds=getattr(self.settings, 'price_request_timeout_seconds', 30),
        )
        reconciler = PriceWsReconciler(
            price_tolerance_bps=getattr(self.settings, 'price_ws_price_tolerance_bps', 1),
            volume_tolerance_bps=getattr(self.settings, 'price_ws_volume_tolerance_bps', 10),
            repository=self.repository,
        )
        return confirm_ws_bucket(
            db,
            source_name=profile.source_name,
            table_name=profile.table_name,
            symbol=profile.symbol,
            bucket_start_utc=bucket_start_utc,
            rest_fetcher=fetcher,
            reconciler=reconciler,
            repository=self.repository,
            settings=self.settings,
            compose_fn=self.compose_fn,
            now=self.now_fn(),
        )
