from __future__ import annotations

import argparse
import logging
import time
from importlib import import_module

from mqnode.config.logging_config import configure_logging
from mqnode.config.settings import Settings, get_settings
from mqnode.db.connection import DB
from mqnode.market.price.checkpoints import price_source_checkpoint_error
from mqnode.market.price.composer import catch_up_canonical_price_from_checkpoint
from mqnode.market.price.registry import get_enabled_price_sources, get_price_source

logger = logging.getLogger(__name__)


def compose_prices_once(db) -> int:
    rebuilt = catch_up_canonical_price_from_checkpoint(db)
    logger.info('price_composer_once rebuilt_buckets=%s', rebuilt)
    return rebuilt


def run_price_composer(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    db = DB(settings)

    while True:
        try:
            compose_prices_once(db)
        except Exception as exc:
            logger.exception('price_composer_loop_error error=%s', exc)
        time.sleep(settings.price_composer_sleep_seconds)


def run_price_source_ingestion_once(source_name: str, settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    db = DB(settings)
    with db.cursor() as cur:
        source = get_price_source(source_name, cur)
        enabled_source_names = {item.source_name for item in get_enabled_price_sources(cur)}
    if source.source_name not in enabled_source_names:
        logger.info('price_source_skipped_disabled source=%s', source_name)
        return 0

    mode = str((source.config or {}).get('mode', 'local')).lower()
    if source.source_name == 'binance' and mode in {'disabled', 'remote'}:
        logger.info('price_source_skipped_mode source=%s mode=%s', source_name, mode)
        return 0

    try:
        module = import_module(source.module_path)
        fetch_buckets = getattr(module, 'fetch_buckets', None)
        if fetch_buckets is None:
            raise NotImplementedError(
                f'Price source {source_name} has no fetch_buckets() implementation at {source.module_path}.'
            )
        return int(fetch_buckets(db=db, settings=settings))
    except Exception as exc:
        if source.is_optional:
            logger.warning('optional_price_source_failed source=%s error=%s', source_name, exc)
            with db.cursor() as cur:
                price_source_checkpoint_error(cur, source_name, str(exc))
            return 0
        raise


def run_enabled_price_sources_once(settings: Settings | None = None) -> dict[str, int]:
    settings = settings or get_settings()
    db = DB(settings)
    with db.cursor() as cur:
        enabled_sources = get_enabled_price_sources(cur)
    results: dict[str, int] = {}
    for source in enabled_sources:
        results[source.source_name] = run_price_source_ingestion_once(source.source_name, settings=settings)
    return results


def run_price_sources_loop(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    while True:
        try:
            results = run_enabled_price_sources_once(settings=settings)
            logger.info('price_source_ingestion_loop results=%s', results)
        except Exception as exc:
            logger.exception('price_source_ingestion_loop_error error=%s', exc)
        time.sleep(settings.price_source_ingestion_sleep_seconds)


def run_single_price_source_loop(source_name: str, settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    while True:
        try:
            processed = run_price_source_ingestion_once(source_name, settings=settings)
            logger.info('price_source_ingestion_loop source=%s processed=%s', source_name, processed)
        except Exception as exc:
            logger.exception('price_source_ingestion_loop_error source=%s error=%s', source_name, exc)
        time.sleep(settings.price_source_ingestion_sleep_seconds)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['compose', 'ingest-source'])
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--source')
    parser.add_argument('--all-enabled', action='store_true')
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)

    if args.mode == 'compose':
        if args.once:
            compose_prices_once(DB(settings))
            return
        run_price_composer(settings)
        return

    if args.all_enabled:
        if args.once:
            results = run_enabled_price_sources_once(settings=settings)
            logger.info('price_source_ingestion_batch results=%s', results)
            return
        run_price_sources_loop(settings)
        return
    if not args.source:
        raise ValueError('--source is required for ingest-source mode unless --all-enabled is used')
    if args.once:
        processed = run_price_source_ingestion_once(args.source, settings=settings)
        logger.info('price_source_ingestion_once source=%s processed=%s', args.source, processed)
        return
    run_single_price_source_loop(args.source, settings)


if __name__ == '__main__':
    main()
