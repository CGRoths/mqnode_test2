from __future__ import annotations

from datetime import datetime, timedelta

from mqnode.config.settings import get_settings
from mqnode.core.utils import to_bucket_start_10m, utc_now
from mqnode.db.repositories import get_checkpoint
from mqnode.market.price.checkpoints import price_source_component
from mqnode.market.price.lifecycle.models import (
    CLOUD_WS_REMOTE_PUSH,
    DISABLED_MODE,
    LIFECYCLE_DISABLED,
    LIVE_HEALTHY,
    REST_BACKFILLING,
    REST_ONLY,
    WAITING_REMOTE_PUSH,
    WS_CANDIDATE_REST_FINAL,
    WS_CONFIRMED_CANDLE,
    WS_STARTING,
    LifecycleDecision,
    PriceSourceLifecycleProfile,
)


class PriceSourceLifecycleManager:
    def __init__(self, settings=None) -> None:
        self.settings = settings or get_settings()

    def latest_closed_10m_bucket(self, now: datetime | None = None) -> datetime:
        now = now or utc_now()
        return to_bucket_start_10m(now) - timedelta(minutes=10)

    def rest_sync_threshold(self, now: datetime | None = None) -> datetime:
        allowed_lag = int(getattr(self.settings, 'price_source_rest_sync_allowed_lag_minutes', 20))
        return self.latest_closed_10m_bucket(now) - timedelta(minutes=allowed_lag)

    def source_checkpoint(self, db, profile: PriceSourceLifecycleProfile) -> dict:
        with db.cursor() as cur:
            return get_checkpoint(cur, 'BTC', price_source_component(profile.source_name), profile.target_interval)

    def is_rest_synced(self, db, profile: PriceSourceLifecycleProfile, now: datetime | None = None) -> bool:
        checkpoint = self.source_checkpoint(db, profile)
        last_bucket_time = checkpoint.get('last_bucket_time')
        if last_bucket_time is None:
            return False
        return to_bucket_start_10m(last_bucket_time) >= self.rest_sync_threshold(now)

    def evaluate_source(
        self,
        db,
        profile: PriceSourceLifecycleProfile,
        now: datetime | None = None,
    ) -> LifecycleDecision:
        if not profile.enabled or profile.live_mode == DISABLED_MODE:
            return LifecycleDecision(
                source_name=profile.source_name,
                symbol=profile.symbol,
                current_state=LIFECYCLE_DISABLED,
                recommended_action='disabled',
                reason='source disabled by lifecycle profile',
                should_start_ws=False,
                should_keep_rest_live=False,
                should_disable_ws=True,
                expects_remote_push=False,
            )

        if not self.is_rest_synced(db, profile, now=now):
            return LifecycleDecision(
                source_name=profile.source_name,
                symbol=profile.symbol,
                current_state=REST_BACKFILLING,
                recommended_action='continue_rest_backfill',
                reason='REST checkpoint is behind latest closed bucket',
                should_start_ws=False,
                should_keep_rest_live=True,
                should_disable_ws=True,
                expects_remote_push=False,
            )

        if profile.live_mode == REST_ONLY:
            return LifecycleDecision(
                source_name=profile.source_name,
                symbol=profile.symbol,
                current_state=LIVE_HEALTHY,
                recommended_action='rest_live_only',
                reason='REST is synced and profile is REST_ONLY',
                should_start_ws=False,
                should_keep_rest_live=True,
                should_disable_ws=True,
                expects_remote_push=False,
            )

        if profile.live_mode == WS_CONFIRMED_CANDLE:
            return LifecycleDecision(
                source_name=profile.source_name,
                symbol=profile.symbol,
                current_state=WS_STARTING,
                recommended_action='start_ws_after_rest_sync',
                reason='REST is synced and source has confirmed candle WS mode',
                should_start_ws=True,
                should_keep_rest_live=True,
                should_disable_ws=False,
                expects_remote_push=False,
            )

        if profile.live_mode == WS_CANDIDATE_REST_FINAL:
            return LifecycleDecision(
                source_name=profile.source_name,
                symbol=profile.symbol,
                current_state=LIVE_HEALTHY,
                recommended_action='rest_official_candidate_ws_optional',
                reason='REST is official; WS candidate data cannot write ws_closed',
                should_start_ws=False,
                should_keep_rest_live=True,
                should_disable_ws=True,
                expects_remote_push=False,
            )

        if profile.live_mode == CLOUD_WS_REMOTE_PUSH:
            return LifecycleDecision(
                source_name=profile.source_name,
                symbol=profile.symbol,
                current_state=WAITING_REMOTE_PUSH,
                recommended_action='wait_for_cloud_remote_push',
                reason='REST is synced and source expects cloud WS remote push',
                should_start_ws=False,
                should_keep_rest_live=True,
                should_disable_ws=True,
                expects_remote_push=True,
            )

        return LifecycleDecision(
            source_name=profile.source_name,
            symbol=profile.symbol,
            current_state=LIFECYCLE_DISABLED,
            recommended_action='unsupported_live_mode',
            reason=f'Unsupported lifecycle live_mode: {profile.live_mode}',
            should_start_ws=False,
            should_keep_rest_live=True,
            should_disable_ws=True,
            expects_remote_push=False,
        )
