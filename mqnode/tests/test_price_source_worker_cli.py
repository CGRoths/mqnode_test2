from __future__ import annotations

from types import SimpleNamespace

from mqnode.market.price import source_worker


def test_source_worker_cli_runs_forever_with_source_and_max_cycles(monkeypatch):
    calls = []
    settings = SimpleNamespace(log_level='INFO')

    class _Worker:
        def __init__(self, *, source_name, symbol=None, settings=None):
            calls.append(('init', source_name, symbol, settings))

        def run_forever(self, **kwargs):
            calls.append(('run_forever', kwargs))

    monkeypatch.setattr(source_worker, 'get_settings', lambda: settings)
    monkeypatch.setattr(source_worker, 'configure_logging', lambda log_level: calls.append(('log', log_level)))
    monkeypatch.setattr(source_worker, 'SourcePriceWorker', _Worker)

    source_worker.main(['--source', 'bybit', '--max-cycles', '1'])

    assert calls == [
        ('log', 'INFO'),
        ('init', 'bybit', None, settings),
        (
            'run_forever',
            {
                'poll_seconds': None,
                'confirmation_poll_seconds': None,
                'max_cycles': 1,
            },
        ),
    ]
