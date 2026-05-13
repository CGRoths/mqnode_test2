from __future__ import annotations

import argparse

from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.market.price.lifecycle.manager import PriceSourceLifecycleManager
from mqnode.market.price.lifecycle.profiles import get_price_source_lifecycle_profile
from mqnode.market.price.lifecycle.repository import PriceSourceLifecycleRepository
from mqnode.market.price.registry import get_price_source


def _configured_sources(settings) -> list[str]:
    raw_sources = getattr(settings, 'price_source_lifecycle_sources', '')
    return [item.strip().lower() for item in raw_sources.split(',') if item.strip()]


def _print_decision(decision, *, rest_synced: bool) -> None:
    print(f'{decision.source_name}:')
    print(f'  state={decision.current_state}')
    print(f'  rest_synced={str(rest_synced).lower()}')
    print(f'  should_start_ws={str(decision.should_start_ws).lower()}')
    print(f'  should_keep_rest_live={str(decision.should_keep_rest_live).lower()}')
    print(f'  expects_remote_push={str(decision.expects_remote_push).lower()}')
    print(f'  action={decision.recommended_action}')


def evaluate_lifecycle_sources(source_names: list[str], *, settings=None, db=None) -> int:
    settings = settings or get_settings()
    db = db or DB(settings)
    manager = PriceSourceLifecycleManager(settings)
    repository = PriceSourceLifecycleRepository(db)
    evaluated = 0
    for source_name in source_names:
        with db.cursor() as cur:
            price_source = get_price_source(source_name, cur)
        profile = get_price_source_lifecycle_profile(source_name, settings=settings, price_source_row=price_source)
        rest_synced = manager.is_rest_synced(db, profile)
        decision = manager.evaluate_source(db, profile)
        repository.upsert_status(
            profile,
            decision.current_state,
            rest_sync_status='synced' if rest_synced else 'backfilling',
            ws_status='starting' if decision.should_start_ws else None,
            metadata_json={
                'recommended_action': decision.recommended_action,
                'reason': decision.reason,
                'should_start_ws': decision.should_start_ws,
                'should_keep_rest_live': decision.should_keep_rest_live,
                'expects_remote_push': decision.expects_remote_push,
            },
        )
        _print_decision(decision, rest_synced=rest_synced)
        evaluated += 1
    return evaluated


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--source')
    parser.add_argument('--all', action='store_true')
    args = parser.parse_args(argv)

    settings = get_settings()
    if args.source:
        sources = [args.source.lower()]
    elif args.all:
        sources = _configured_sources(settings)
    else:
        parser.error('Pass --source SOURCE or --all.')
    evaluate_lifecycle_sources(sources, settings=settings)


if __name__ == '__main__':
    main()
