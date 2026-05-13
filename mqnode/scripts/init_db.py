from __future__ import annotations

from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.db.migrations import run_migrations


def main() -> None:
    run_migrations(DB(get_settings()))
    print('Migrations applied.')


if __name__ == '__main__':
    main()
