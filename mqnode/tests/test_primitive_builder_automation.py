from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

from mqnode.chains.btc import primitive_builder

DEPRECATED_PRICE_FIELDS = {
    'open_price_usd',
    'high_price_usd',
    'low_price_usd',
    'close_price_usd',
    'market_cap_usd',
    'onchain_volume_usd_raw_10m',
}


class DummyCursor:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyDB:
    def cursor(self):
        return DummyCursor()


def _sample_blocks():
    return [
        {
            'height': 100,
            'block_hash': 'a',
            'block_time': datetime(2026, 4, 20, 0, 2, tzinfo=timezone.utc),
            'median_time': datetime(2026, 4, 20, 0, 1, tzinfo=timezone.utc),
            'event_time': datetime(2026, 4, 20, 0, 1, tzinfo=timezone.utc),
            'tx_count': 10,
            'non_coinbase_tx_count': 9,
            'total_out_sat': 312501100,
            'total_fee_sat': 100,
            'issued_sat': 312500000,
            'miner_revenue_sat': 312500100,
            'input_count': 20,
            'output_count': 30,
            'block_size_bytes': 1000,
            'block_weight_wu': 4000,
            'block_vsize_vb': 1000,
            'tx_size_total_bytes': 800,
            'tx_vsize_total_vb': 700,
            'min_feerate_sat_vb': 2.0,
            'max_feerate_sat_vb': 8.0,
            'segwit_tx_count': 5,
            'sw_total_size_bytes': 500,
            'sw_total_weight_wu': 2000,
            'difficulty': 1,
            'chainwork': 'abc',
            'cumulative_supply_sat': 100000000000,
        },
        {
            'height': 101,
            'block_hash': 'b',
            'block_time': datetime(2026, 4, 20, 0, 8, tzinfo=timezone.utc),
            'median_time': datetime(2026, 4, 20, 0, 7, tzinfo=timezone.utc),
            'event_time': datetime(2026, 4, 20, 0, 7, tzinfo=timezone.utc),
            'tx_count': 12,
            'non_coinbase_tx_count': 11,
            'total_out_sat': 312501320,
            'total_fee_sat': 120,
            'issued_sat': 312500000,
            'miner_revenue_sat': 312500120,
            'input_count': 24,
            'output_count': 32,
            'block_size_bytes': 1200,
            'block_weight_wu': 4200,
            'block_vsize_vb': 1050,
            'tx_size_total_bytes': 900,
            'tx_vsize_total_vb': 750,
            'min_feerate_sat_vb': 1.5,
            'max_feerate_sat_vb': 10.0,
            'segwit_tx_count': 7,
            'sw_total_size_bytes': 650,
            'sw_total_weight_wu': 2500,
            'difficulty': 2,
            'chainwork': 'def',
            'cumulative_supply_sat': 100312500000,
        },
    ]


def _state_row():
    return {
        'height': 101,
        'event_time': datetime(2026, 4, 20, 0, 8, tzinfo=timezone.utc),
        'difficulty': 2,
        'chainwork': 'def',
        'cumulative_supply_sat': 100312500000,
    }


def test_rebuild_10m_bucket_is_idempotent(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    writes = []
    checkpoints = []
    enqueued = []

    monkeypatch.setattr(primitive_builder, 'build_10m_bucket_payload', lambda cur, value, settings=None: {
        'bucket_start_utc': bucket,
        'open_time_ms': 1,
        'first_height': 100,
        'last_height': 101,
        'block_count': 2,
        'first_block_time_utc': datetime(2026, 4, 20, 0, 1, tzinfo=timezone.utc),
        'last_block_time_utc': datetime(2026, 4, 20, 0, 7, tzinfo=timezone.utc),
        'issued_sat_10m': 625000000,
        'fees_sat_10m': 220,
        'miner_revenue_sat_10m': 625000220,
        'supply_total_sat': 100312500000,
        'block_reward_sat_avg': 312500000,
        'halving_epoch': 0,
        'total_out_sat_10m': 2200,
        'total_fee_sat_10m': 220,
        'transferred_sat_10m': 2200,
        'transferred_btc_10m': 0.000022,
        'tx_count_10m': 22,
        'non_coinbase_tx_count_10m': 20,
        'input_count_10m': 44,
        'output_count_10m': 62,
        'tx_rate_per_sec_10m': 22 / 600,
        'block_size_total_bytes_10m': 2200,
        'block_size_mean_bytes_10m': 1100,
        'block_weight_total_wu_10m': 8200,
        'block_weight_mean_wu_10m': 4100,
        'block_vsize_total_vb_10m': 2050,
        'tx_size_total_bytes_10m': 1700,
        'tx_size_mean_bytes_10m': 1700 / 22,
        'block_interval_mean_sec_10m': 360,
        'block_interval_median_sec_10m': 360,
        'avg_fee_sat_10m': 11,
        'median_fee_sat_10m': None,
        'avg_feerate_sat_vb_10m': 220 / 1450,
        'min_feerate_sat_vb_10m': 1.5,
        'max_feerate_sat_vb_10m': 10.0,
        'utxo_increase_10m': 18,
        'utxo_count_total': None,
        'utxo_size_inc_bytes_10m': None,
        'utxo_set_size_total_bytes': None,
        'spent_output_count_10m': 44,
        'created_output_count_10m': 62,
        'segwit_tx_count_10m': 12,
        'segwit_share_10m': 12 / 20,
        'sw_total_size_bytes_10m': 1150,
        'sw_total_weight_wu_10m': 4500,
        'difficulty_last': 2,
        'chainwork_last': 'def',
        'hashrate_est_last': None,
        'hashrate_est_mean_10m': None,
        'best_block_height_last': 101,
        'updated_at': datetime(2026, 4, 20, 0, 9, tzinfo=timezone.utc),
    })
    monkeypatch.setattr(primitive_builder, '_upsert_10m_bucket', lambda cur, payload: writes.append(dict(payload)))
    monkeypatch.setattr(primitive_builder, 'upsert_checkpoint', lambda cur, *args, **kwargs: checkpoints.append(kwargs))
    monkeypatch.setattr(
        primitive_builder,
        'enqueue_primitive_ready',
        lambda value, interval: enqueued.append((value, interval)),
    )

    settings = SimpleNamespace(btc_price_table='mq_btc_price_10m')
    db = DummyDB()

    primitive_builder.rebuild_10m_bucket_for_bucket(db, bucket, settings=settings)
    primitive_builder.rebuild_10m_bucket_for_bucket(db, bucket, settings=settings)

    assert len(writes) == 2
    normalized_first = {key: value for key, value in writes[0].items() if key != 'updated_at'}
    normalized_second = {key: value for key, value in writes[1].items() if key != 'updated_at'}
    assert normalized_first == normalized_second
    assert checkpoints == [
        {'last_height': 101, 'last_bucket_time': bucket, 'status': 'ok'},
        {'last_height': 101, 'last_bucket_time': bucket, 'status': 'ok'},
    ]
    assert enqueued == [(bucket, '10m'), (bucket, '10m')]


def test_build_10m_bucket_zero_fills_and_carries_state_without_blocks(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 10, tzinfo=timezone.utc)
    monkeypatch.setattr(primitive_builder, '_fetch_blocks_for_bucket', lambda cur, value: [])
    monkeypatch.setattr(primitive_builder, '_fetch_latest_chain_state', lambda cur, value: deepcopy(_state_row()))

    payload = primitive_builder.build_10m_bucket_payload(
        object(),
        bucket,
        settings=SimpleNamespace(btc_price_table='mq_btc_price_10m'),
    )

    assert payload is not None
    assert payload['block_count'] == 0
    assert payload['tx_count_10m'] == 0
    assert payload['issued_sat_10m'] == 0
    assert payload['block_size_total_bytes_10m'] == 0
    assert payload['supply_total_sat'] == 100312500000
    assert payload['difficulty_last'] == 2
    assert payload['best_block_height_last'] == 101
    assert payload['block_size_mean_bytes_10m'] is None
    assert payload['block_interval_mean_sec_10m'] is None
    assert payload['segwit_share_10m'] is None
    assert DEPRECATED_PRICE_FIELDS.isdisjoint(payload)


def test_build_10m_bucket_payload_is_chain_native(monkeypatch):
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(primitive_builder, '_fetch_blocks_for_bucket', lambda cur, value: deepcopy(_sample_blocks()))
    monkeypatch.setattr(primitive_builder, '_fetch_latest_chain_state', lambda cur, value: deepcopy(_state_row()))

    payload = primitive_builder.build_10m_bucket_payload(
        object(),
        bucket,
        settings=SimpleNamespace(btc_price_table='mq_btc_price_10m'),
    )

    assert payload['tx_count_10m'] == 22
    assert payload['supply_total_sat'] == 100312500000
    assert payload['transferred_sat_10m'] == 2200
    assert payload['miner_revenue_sat_10m'] == 625000220
    assert payload['segwit_share_10m'] == 12 / 20
    assert DEPRECATED_PRICE_FIELDS.isdisjoint(payload)
    assert DEPRECATED_PRICE_FIELDS.isdisjoint(primitive_builder.UPSERT_COLUMNS)
