from __future__ import annotations

from mqnode.chains.btc.listener import RAW_COMPONENT
from mqnode.chains.btc.reorg import reconcile_reorg
from mqnode.chains.btc.rpc import BitcoinRPC
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.db.repositories import get_checkpoint


def main() -> None:
    settings = get_settings()
    db = DB(settings)
    rpc = BitcoinRPC(settings)

    with db.cursor() as cur:
        checkpoint = get_checkpoint(cur, 'BTC', RAW_COMPONENT, 'block')
        last_height = int(checkpoint.get('last_height') or 0)

    result = reconcile_reorg(db, rpc, last_height)
    if result is None:
        print('No BTC reorg detected.')
    else:
        print(
            'BTC reorg reconciled: '
            f'previous_checkpoint_height={result["previous_checkpoint_height"]} '
            f'common_height={result["common_height"]} '
            f'affected_bucket={result["affected_bucket"]}'
        )


if __name__ == '__main__':
    main()
