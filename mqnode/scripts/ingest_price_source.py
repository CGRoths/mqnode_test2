from __future__ import annotations

import argparse

from mqnode.market.price.runtime import run_enabled_price_sources_once, run_price_source_ingestion_once


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--source')
    parser.add_argument('--all-enabled', action='store_true')
    args = parser.parse_args()

    if args.all_enabled:
        results = run_enabled_price_sources_once()
        print(f'Enabled price sources processed: {results}')
        return
    if not args.source:
        raise ValueError('--source is required unless --all-enabled is used')
    processed = run_price_source_ingestion_once(args.source)
    print(f'Price source processed: source={args.source} count={processed}')


if __name__ == '__main__':
    main()
