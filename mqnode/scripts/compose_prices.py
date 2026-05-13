from __future__ import annotations

from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.market.price.runtime import compose_prices_once


def main() -> None:
    rebuilt = compose_prices_once(DB(get_settings()))
    print(f'Canonical price buckets composed: {rebuilt}')


if __name__ == '__main__':
    main()
