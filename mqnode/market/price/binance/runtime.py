from __future__ import annotations

from mqnode.market.price.source_worker_runtime import main_for_source


def main(argv: list[str] | None = None) -> None:
    main_for_source('binance', argv=argv)


if __name__ == '__main__':
    main()
