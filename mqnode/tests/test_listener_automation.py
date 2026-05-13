from __future__ import annotations

from datetime import datetime, timezone

from mqnode.chains.btc import listener


class DummyRPC:
    def __init__(self, node_tip: int):
        self.node_tip = node_tip

    def get_block_count(self) -> int:
        return self.node_tip


def test_sync_blocks_once_resumes_from_checkpoint(monkeypatch):
    processed = []
    enqueued = []
    checkpoint_errors = []

    monkeypatch.setattr(
        listener,
        '_load_ingestion_state',
        lambda db: {'last_height': 5, 'last_supply_sat': 500, 'status': 'ok'},
    )

    def fake_ingest_height(db, rpc, height, last_supply_sat):
        processed.append((height, last_supply_sat))
        return last_supply_sat + 50

    monkeypatch.setattr(listener, '_ingest_height', fake_ingest_height)
    monkeypatch.setattr(listener, 'enqueue_raw_block_ready', lambda height: enqueued.append(height))
    monkeypatch.setattr(
        listener,
        '_mark_listener_error',
        lambda db, last_successful_height, error_message: checkpoint_errors.append(
            (last_successful_height, error_message)
        ),
    )

    result = listener.sync_blocks_once(object(), DummyRPC(node_tip=7))

    assert result['status'] == 'ok'
    assert result['processed_heights'] == [6, 7]
    assert processed == [(6, 500), (7, 550)]
    assert enqueued == [6, 7]
    assert checkpoint_errors == []


def test_sync_blocks_once_marks_last_safe_height_on_failure(monkeypatch):
    attempted = []
    enqueued = []
    checkpoint_errors = []

    monkeypatch.setattr(
        listener,
        '_load_ingestion_state',
        lambda db: {'last_height': 2, 'last_supply_sat': 200, 'status': 'ok'},
    )

    def fake_ingest_height(db, rpc, height, last_supply_sat):
        attempted.append(height)
        if height == 4:
            raise RuntimeError('boom')
        return last_supply_sat + 100

    monkeypatch.setattr(listener, '_ingest_height', fake_ingest_height)
    monkeypatch.setattr(listener, 'enqueue_raw_block_ready', lambda height: enqueued.append(height))
    monkeypatch.setattr(
        listener,
        '_mark_listener_error',
        lambda db, last_successful_height, error_message: checkpoint_errors.append(
            (last_successful_height, error_message)
        ),
    )

    result = listener.sync_blocks_once(object(), DummyRPC(node_tip=5))

    assert result['status'] == 'error'
    assert result['failed_height'] == 4
    assert result['last_height'] == 3
    assert result['processed_heights'] == [3]
    assert attempted == [3, 4]
    assert enqueued == [3]
    assert checkpoint_errors == [(3, 'boom')]


def test_idle_listener_schedules_single_primitive_tick(monkeypatch):
    scheduled = []
    checkpoint_writes = []

    monkeypatch.setattr(listener, 'utc_now', lambda: datetime(2026, 4, 21, 1, 4, tzinfo=timezone.utc))

    checkpoints = {
        (
            listener.PRIMITIVE_SCHEDULER_COMPONENT,
            listener.PRIMITIVE_INTERVAL,
        ): {'last_bucket_time': datetime(2026, 4, 21, 0, 50, tzinfo=timezone.utc)},
        (
            listener.PRIMITIVE_COMPONENT,
            listener.PRIMITIVE_INTERVAL,
        ): {'last_bucket_time': datetime(2026, 4, 21, 0, 50, tzinfo=timezone.utc)},
    }

    monkeypatch.setattr(
        listener,
        'get_checkpoint',
        lambda cur, chain, component, interval: checkpoints.get((component, interval), {'last_bucket_time': None}),
    )
    monkeypatch.setattr(listener, 'enqueue_primitive_tick', lambda bucket: scheduled.append(bucket))
    monkeypatch.setattr(listener, 'upsert_checkpoint', lambda cur, *args, **kwargs: checkpoint_writes.append(kwargs))

    class DummyCursor:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyDB:
        def cursor(self):
            return DummyCursor()

    scheduled_now = listener._maybe_schedule_primitive_tick(DummyDB(), 900000)

    assert scheduled_now is True
    assert scheduled == [datetime(2026, 4, 21, 1, 0, tzinfo=timezone.utc)]
    assert checkpoint_writes == [
        {'last_height': 900000, 'last_bucket_time': datetime(2026, 4, 21, 1, 0, tzinfo=timezone.utc), 'status': 'ok'}
    ]
