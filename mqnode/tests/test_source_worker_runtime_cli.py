from __future__ import annotations

from types import SimpleNamespace

from mqnode.market.price import source_worker_runtime
from mqnode.market.price.bybit import runtime as bybit_runtime


def test_runtime_cli_once_calls_run_once(monkeypatch):
    calls = []

    class _Worker:
        def __init__(self, source_name, *, settings=None, symbol=None):
            calls.append(('init', source_name, settings, symbol))

        def run_once(self):
            calls.append(('once',))

        def run_forever(self, **kwargs):
            calls.append(('daemon', kwargs))

    settings = SimpleNamespace(log_level='INFO')
    monkeypatch.setattr(source_worker_runtime, 'get_settings', lambda: settings)
    monkeypatch.setattr(source_worker_runtime, 'configure_logging', lambda log_level: calls.append(('log', log_level)))
    monkeypatch.setattr(source_worker_runtime, 'SourcePriceWorker', _Worker)

    source_worker_runtime.main_for_source('bybit', argv=['--once', '--symbol', 'BTCUSDT'])

    assert calls == [
        ('log', 'INFO'),
        ('init', 'bybit', settings, 'BTCUSDT'),
        ('once',),
    ]


def test_runtime_cli_defaults_to_once(monkeypatch):
    calls = []

    class _Worker:
        def __init__(self, source_name, *, settings=None, symbol=None):
            self.source_name = source_name

        def run_once(self):
            calls.append(('once', self.source_name))

        def run_forever(self, **kwargs):
            calls.append(('daemon', kwargs))

    monkeypatch.setattr(source_worker_runtime, 'get_settings', lambda: SimpleNamespace(log_level='INFO'))
    monkeypatch.setattr(source_worker_runtime, 'configure_logging', lambda log_level: None)
    monkeypatch.setattr(source_worker_runtime, 'SourcePriceWorker', _Worker)

    source_worker_runtime.main_for_source('bybit', argv=[])

    assert calls == [('once', 'bybit')]


def test_runtime_cli_daemon_calls_run_forever_with_poll_settings(monkeypatch):
    calls = []

    class _Worker:
        def __init__(self, source_name, *, settings=None, symbol=None):
            calls.append(('init', source_name, symbol))

        def run_once(self):
            calls.append(('once',))

        def run_forever(self, **kwargs):
            calls.append(('daemon', kwargs))

    monkeypatch.setattr(source_worker_runtime, 'get_settings', lambda: SimpleNamespace(log_level='INFO'))
    monkeypatch.setattr(source_worker_runtime, 'configure_logging', lambda log_level: None)
    monkeypatch.setattr(source_worker_runtime, 'SourcePriceWorker', _Worker)

    source_worker_runtime.main_for_source(
        'bybit',
        argv=['--daemon', '--poll-seconds', '7', '--confirmation-poll-seconds', '2'],
    )

    assert calls == [
        ('init', 'bybit', None),
        ('daemon', {'poll_seconds': 7, 'confirmation_poll_seconds': 2}),
    ]


def test_bybit_runtime_passes_argv_to_source_worker_runtime(monkeypatch):
    calls = []
    monkeypatch.setattr(
        bybit_runtime,
        'main_for_source',
        lambda source_name, argv=None: calls.append((source_name, argv)),
    )

    bybit_runtime.main(['--daemon'])

    assert calls == [('bybit', ['--daemon'])]
