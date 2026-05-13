from __future__ import annotations

import argparse

from mqnode.config.logging_config import configure_logging
from mqnode.config.settings import get_settings
from mqnode.market.price.source_worker import SourcePriceWorker


def main_for_source(source_name: str, argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--daemon', action='store_true')
    mode.add_argument('--once', action='store_true')
    parser.add_argument('--source', default=source_name)
    parser.add_argument('--symbol', default=None)
    parser.add_argument('--poll-seconds', type=int, default=None)
    parser.add_argument('--confirmation-poll-seconds', type=int, default=None)
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)
    worker = SourcePriceWorker(args.source, settings=settings, symbol=args.symbol)
    if args.daemon:
        worker.run_forever(
            poll_seconds=args.poll_seconds,
            confirmation_poll_seconds=args.confirmation_poll_seconds,
        )
        return
    worker.run_once()


def main(argv: list[str] | None = None) -> None:
    main_for_source('bybit', argv=argv)


if __name__ == '__main__':
    main()
