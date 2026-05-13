from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from statistics import mean
from typing import Any

SATOSHIS_PER_BTC = Decimal("100000000")


class ValidationError(RuntimeError):
    """Raised when a block payload violates parser invariants."""


def _btc_to_sat(value: Any) -> int:
    """Convert a BTC-denominated RPC numeric value into satoshis."""
    return int(
        (Decimal(str(value or 0)) * SATOSHIS_PER_BTC).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )
    )


def _is_coinbase_tx(tx: dict[str, Any]) -> bool:
    """
    Return True if the transaction is coinbase.

    Bitcoin consensus requires exactly one coinbase transaction per block,
    and it must be the first transaction.
    """
    vin = tx.get("vin", [])
    return (
        len(vin) == 1
        and vin[0].get("coinbase") is not None
    )


def _block_subsidy_sat(height: int) -> int:
    """
    Return the maximum consensus subsidy for a block height in satoshis.

    Bitcoin starts at 50 BTC and halves every 210,000 blocks.
    """
    halvings = height // 210_000
    if halvings >= 64:
        return 0
    return 50 * 100_000_000 >> halvings


def parse_block(
    block: dict[str, Any],
    cumulative_supply_sat_prev: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Parse one Bitcoin block RPC payload into:
    1) a raw block record for audit/replay
    2) a primitive block record for downstream metrics

    Accounting identity used here:
        coinbase_reward_sat = subsidy_sat + total_fee_sat

    Therefore:
        issued_sat = true newly minted BTC = subsidy only
        miner_revenue_sat = full coinbase payout = subsidy + fees
        cumulative_supply_sat increases only by issued_sat
    """
    txs = block.get("tx", [])
    tx_count = len(txs)

    if tx_count == 0:
        raise ValidationError(
            f"Block at height={block.get('height')} has no transactions."
        )

    # Defensive validation:
    # valid Bitcoin blocks must have coinbase as the first transaction.
    if not _is_coinbase_tx(txs[0]):
        raise ValidationError(
            f"Block at height={block.get('height')} does not start with a coinbase transaction."
        )

    coinbase_count = sum(1 for tx in txs if _is_coinbase_tx(tx))
    if coinbase_count != 1:
        raise ValidationError(
            f"Block at height={block.get('height')} has invalid coinbase count={coinbase_count}."
        )

    non_coinbase_count = sum(1 for tx in txs if not _is_coinbase_tx(tx))

    total_out_sat = 0
    observed_fee_sat_total = 0
    input_count = 0
    output_count = 0
    tx_size_total_bytes = 0
    tx_vsize_total_vb = 0
    segwit_tx_count = 0
    sw_total_size_bytes = 0
    sw_total_weight_wu = 0
    fee_values_sat: list[int] = []
    feerate_values_sat_vb: list[float] = []

    # Coinbase reward is the full payout to the miner:
    # subsidy + transaction fees.
    coinbase_reward_sat = sum(
        _btc_to_sat(vout.get("value", 0))
        for vout in txs[0].get("vout", [])
    )

    for tx in txs:
        is_coinbase = _is_coinbase_tx(tx)
        vin = tx.get("vin", [])
        vout = tx.get("vout", [])

        # Coinbase inputs are synthetic, so exclude them from input_count.
        if not is_coinbase:
            input_count += len(vin)

        # Output count and output value include coinbase outputs.
        output_count += len(vout)
        total_out_sat += sum(_btc_to_sat(v.get("value", 0)) for v in vout)

        tx_size = int(tx.get("size") or 0)
        tx_weight = int(tx.get("weight") or 0)
        tx_vsize = int(
            tx.get("vsize")
            or ((tx_weight + 3) // 4 if tx_weight else 0)
        )

        tx_size_total_bytes += tx_size
        tx_vsize_total_vb += tx_vsize

        # SegWit adoption is tracked only for spend transactions, not coinbase.
        if not is_coinbase and any(vin_row.get("txinwitness") for vin_row in vin):
            segwit_tx_count += 1
            sw_total_size_bytes += tx_size
            sw_total_weight_wu += tx_weight

        if not is_coinbase:
            fee = tx.get("fee")
            if fee is not None:
                fee_sat = _btc_to_sat(fee)
                observed_fee_sat_total += fee_sat
                fee_values_sat.append(fee_sat)

                if tx_vsize > 0:
                    feerate_values_sat_vb.append(fee_sat / tx_vsize)

    max_subsidy_sat = _block_subsidy_sat(int(block["height"]))
    fee_observations_complete = len(fee_values_sat) == non_coinbase_count

    # Prefer observed transaction fees when complete.
    # Otherwise infer fees from coinbase payout minus max allowed subsidy.
    if fee_observations_complete:
        total_fee_sat = observed_fee_sat_total
    else:
        total_fee_sat = max(coinbase_reward_sat - max_subsidy_sat, 0)

    # New supply should only reflect the subsidy component, never fees.
    issued_sat = min(
        max(coinbase_reward_sat - total_fee_sat, 0),
        max_subsidy_sat,
    )
    subsidy_sat = issued_sat
    miner_revenue_sat = coinbase_reward_sat
    cumulative_supply_sat = cumulative_supply_sat_prev + issued_sat

    block_weight_wu = int(block.get("weight") or 0)
    if block.get("vsize") is not None:
        block_vsize_vb = int(block["vsize"])
    elif block_weight_wu:
        block_vsize_vb = (block_weight_wu + 3) // 4
    else:
        # Last-resort approximation if neither vsize nor weight is present.
        block_vsize_vb = int(block.get("strippedsize") or 0)

    primitive = {
        "height": block["height"],
        "block_hash": block["hash"],
        "block_time": block.get("time"),
        "median_time": block.get("mediantime"),
        "tx_count": tx_count,
        "non_coinbase_tx_count": non_coinbase_count,
        "total_out_sat": total_out_sat,
        "total_fee_sat": total_fee_sat,
        "subsidy_sat": subsidy_sat,
        "issued_sat": issued_sat,
        "miner_revenue_sat": miner_revenue_sat,
        "fee_observations_complete": fee_observations_complete,
        "input_count": input_count,
        "output_count": output_count,
        "block_size_bytes": int(block.get("size") or 0),
        "block_weight_wu": block_weight_wu,
        "block_vsize_vb": block_vsize_vb,
        "tx_size_total_bytes": tx_size_total_bytes,
        "tx_vsize_total_vb": tx_vsize_total_vb,
        "avg_fee_sat": mean(fee_values_sat) if fee_values_sat else None,
        "min_feerate_sat_vb": (
            min(feerate_values_sat_vb)
            if fee_observations_complete and feerate_values_sat_vb
            else None
        ),
        "max_feerate_sat_vb": (
            max(feerate_values_sat_vb)
            if fee_observations_complete and feerate_values_sat_vb
            else None
        ),
        "segwit_tx_count": segwit_tx_count,
        "sw_total_size_bytes": sw_total_size_bytes,
        "sw_total_weight_wu": sw_total_weight_wu,
        "difficulty": block.get("difficulty"),
        "chainwork": block.get("chainwork"),
        "cumulative_supply_sat": cumulative_supply_sat,
    }

    raw = {
        "height": block["height"],
        "block_hash": block["hash"],
        "previous_block_hash": block.get("previousblockhash"),
        "block_time": block.get("time"),
        "median_time": block.get("mediantime"),
        "tx_count": tx_count,
        "size": int(block.get("size") or 0),
        "stripped_size": int(block.get("strippedsize") or 0),
        "weight": block_weight_wu,
        "difficulty": block.get("difficulty"),
        "chainwork": block.get("chainwork"),
        "version": block.get("version"),
        "merkle_root": block.get("merkleroot"),
        "raw_json": block,
    }

    return raw, primitive