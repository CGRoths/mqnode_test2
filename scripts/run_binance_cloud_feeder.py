from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description='Placeholder for future Binance cloud price feeder.')
    parser.add_argument('--once', action='store_true', help='Reserved for one-shot cloud feeder execution.')
    parser.parse_args()
    raise NotImplementedError(
        'Cloud Binance feeder is intentionally not wired yet. Planned env: '
        'BINANCE_API_BASE=https://papi.binance.com, MQNODE_INTERNAL_INGEST_URL, '
        'MQNODE_INTERNAL_INGEST_TOKEN. Future flow: fetch Binance PAPI 5m, aggregate '
        'complete 10m buckets, then POST to /api/v1/internal/price/source/binance/10m.'
    )


if __name__ == '__main__':
    main()
