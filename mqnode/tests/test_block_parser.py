from __future__ import annotations

import pytest

from mqnode.chains.btc.block_parser import parse_block


def test_parse_block_separates_subsidy_fees_and_coinbase_counters():
    block = {
        'height': 840000,
        'hash': 'block-840000',
        'time': 1_700_000_000,
        'mediantime': 1_700_000_100,
        'size': 1200,
        'weight': 3600,
        'vsize': 900,
        'difficulty': 1,
        'chainwork': 'abc',
        'tx': [
            {
                'vin': [{'coinbase': 'abcd', 'txinwitness': ['reserved']}],
                'vout': [{'value': 3.12500150}],
                'size': 100,
                'weight': 400,
                'vsize': 100,
            },
            {
                'vin': [{'txid': 'prev-1', 'vout': 0, 'txinwitness': ['witness']}],
                'vout': [{'value': 0.10000000}],
                'fee': 0.00000100,
                'size': 200,
                'weight': 500,
                'vsize': 125,
            },
            {
                'vin': [{'txid': 'prev-2', 'vout': 1}],
                'vout': [{'value': 0.05000000}, {'value': 0.14999950}],
                'fee': 0.00000050,
                'size': 180,
                'weight': 480,
                'vsize': 120,
            },
        ],
    }

    raw, primitive = parse_block(block, cumulative_supply_sat_prev=2_000_000_000_000_000)

    assert raw['previous_block_hash'] is None
    assert primitive['subsidy_sat'] == 312_500_000
    assert primitive['issued_sat'] == 312_500_000
    assert primitive['total_fee_sat'] == 150
    assert primitive['miner_revenue_sat'] == 312_500_150
    assert primitive['cumulative_supply_sat'] == 2_000_000_312_500_000
    assert primitive['input_count'] == 2
    assert primitive['output_count'] == 4
    assert primitive['segwit_tx_count'] == 1
    assert primitive['avg_fee_sat'] == 75
    assert primitive['min_feerate_sat_vb'] == pytest.approx(50 / 120)
    assert primitive['max_feerate_sat_vb'] == pytest.approx(100 / 125)


def test_parse_block_uses_claimed_subsidy_when_miner_burns_reward():
    block = {
        'height': 840001,
        'hash': 'block-840001',
        'time': 1_700_000_600,
        'mediantime': 1_700_000_650,
        'size': 900,
        'weight': 2800,
        'vsize': 700,
        'difficulty': 1,
        'chainwork': 'def',
        'tx': [
            {
                'vin': [{'coinbase': 'efgh'}],
                'vout': [{'value': 3.12499900}],
                'size': 90,
                'weight': 360,
                'vsize': 90,
            },
            {
                'vin': [{'txid': 'prev-3', 'vout': 0}],
                'vout': [{'value': 0.02000000}],
                'fee': 0.00000100,
                'size': 160,
                'weight': 420,
                'vsize': 105,
            },
        ],
    }

    _, primitive = parse_block(block, cumulative_supply_sat_prev=100_000_000_000)

    assert primitive['total_fee_sat'] == 100
    assert primitive['miner_revenue_sat'] == 312_499_900
    assert primitive['subsidy_sat'] == 312_499_800
    assert primitive['issued_sat'] == 312_499_800
    assert primitive['cumulative_supply_sat'] == 100_312_499_800
