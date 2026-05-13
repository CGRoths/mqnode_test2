from __future__ import annotations

from datetime import datetime, timezone

from psycopg2.extras import Json

from mqnode.chains.btc.block_parser import ValidationError, parse_block
from mqnode.chains.btc.rpc import BitcoinRPC


def _to_dt(ts: int | None) -> datetime | None:
    """Convert a Unix timestamp into a timezone-aware UTC datetime."""
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def ingest_block(cur, rpc: BitcoinRPC, height: int, last_supply_sat: int) -> int:
    """
    Fetch, validate, parse, and persist one Bitcoin block.

    Returns the updated cumulative BTC supply in satoshis so the caller can
    continue deterministic block-by-block ingestion.
    """
    block_hash = rpc.get_block_hash(height)
    block = rpc.get_block(block_hash)

    # Defensive guard: the RPC payload must match the requested height.
    if int(block["height"]) != int(height):
        raise ValidationError(
            f"Block height mismatch: requested height={height}, "
            f"rpc returned height={block['height']}"
        )

    raw, primitive = parse_block(
        block,
        cumulative_supply_sat_prev=last_supply_sat,
    )

    # Defensive chain continuity check against the previously stored block.
    if height > 0:
        cur.execute(
            "SELECT block_hash FROM btc_blocks_raw WHERE height = %s",
            (height - 1,),
        )
        previous_row = cur.fetchone()
        expected_previous_hash = raw.get("previous_block_hash")

        if (
            previous_row
            and expected_previous_hash
            and previous_row["block_hash"] != expected_previous_hash
        ):
            raise ValidationError(
                f"Block continuity check failed at height={height}: "
                f"expected previous hash {expected_previous_hash}, "
                f"found {previous_row['block_hash']}"
            )

    # psycopg2 needs Json(...) wrapper for Python dict/list going into JSONB columns.
    cur.execute(
        """
        INSERT INTO btc_blocks_raw(
            height,
            block_hash,
            previous_block_hash,
            block_time,
            median_time,
            tx_count,
            size,
            stripped_size,
            weight,
            difficulty,
            chainwork,
            version,
            merkle_root,
            raw_json,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
        ON CONFLICT (height) DO UPDATE SET
            block_hash = EXCLUDED.block_hash,
            previous_block_hash = EXCLUDED.previous_block_hash,
            block_time = EXCLUDED.block_time,
            median_time = EXCLUDED.median_time,
            tx_count = EXCLUDED.tx_count,
            size = EXCLUDED.size,
            stripped_size = EXCLUDED.stripped_size,
            weight = EXCLUDED.weight,
            difficulty = EXCLUDED.difficulty,
            chainwork = EXCLUDED.chainwork,
            version = EXCLUDED.version,
            merkle_root = EXCLUDED.merkle_root,
            raw_json = EXCLUDED.raw_json
        """,
        (
            raw["height"],
            raw["block_hash"],
            raw["previous_block_hash"],
            _to_dt(raw["block_time"]),
            _to_dt(raw["median_time"]),
            raw["tx_count"],
            raw["size"],
            raw["stripped_size"],
            raw["weight"],
            raw["difficulty"],
            raw["chainwork"],
            raw["version"],
            raw["merkle_root"],
            Json(raw["raw_json"]),
        ),
    )

    cur.execute(
        """
        INSERT INTO btc_primitive_block(
            height,
            block_hash,
            block_time,
            median_time,
            tx_count,
            non_coinbase_tx_count,
            total_out_sat,
            total_fee_sat,
            subsidy_sat,
            issued_sat,
            miner_revenue_sat,
            input_count,
            output_count,
            block_size_bytes,
            block_weight_wu,
            block_vsize_vb,
            tx_size_total_bytes,
            tx_vsize_total_vb,
            avg_fee_sat,
            min_feerate_sat_vb,
            max_feerate_sat_vb,
            segwit_tx_count,
            sw_total_size_bytes,
            sw_total_weight_wu,
            difficulty,
            chainwork,
            cumulative_supply_sat,
            created_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
        )
        ON CONFLICT (height) DO UPDATE SET
            block_hash = EXCLUDED.block_hash,
            block_time = EXCLUDED.block_time,
            median_time = EXCLUDED.median_time,
            tx_count = EXCLUDED.tx_count,
            non_coinbase_tx_count = EXCLUDED.non_coinbase_tx_count,
            total_out_sat = EXCLUDED.total_out_sat,
            total_fee_sat = EXCLUDED.total_fee_sat,
            subsidy_sat = EXCLUDED.subsidy_sat,
            issued_sat = EXCLUDED.issued_sat,
            miner_revenue_sat = EXCLUDED.miner_revenue_sat,
            input_count = EXCLUDED.input_count,
            output_count = EXCLUDED.output_count,
            block_size_bytes = EXCLUDED.block_size_bytes,
            block_weight_wu = EXCLUDED.block_weight_wu,
            block_vsize_vb = EXCLUDED.block_vsize_vb,
            tx_size_total_bytes = EXCLUDED.tx_size_total_bytes,
            tx_vsize_total_vb = EXCLUDED.tx_vsize_total_vb,
            avg_fee_sat = EXCLUDED.avg_fee_sat,
            min_feerate_sat_vb = EXCLUDED.min_feerate_sat_vb,
            max_feerate_sat_vb = EXCLUDED.max_feerate_sat_vb,
            segwit_tx_count = EXCLUDED.segwit_tx_count,
            sw_total_size_bytes = EXCLUDED.sw_total_size_bytes,
            sw_total_weight_wu = EXCLUDED.sw_total_weight_wu,
            difficulty = EXCLUDED.difficulty,
            chainwork = EXCLUDED.chainwork,
            cumulative_supply_sat = EXCLUDED.cumulative_supply_sat
        """,
        (
            primitive["height"],
            primitive["block_hash"],
            _to_dt(primitive["block_time"]),
            _to_dt(primitive["median_time"]),
            primitive["tx_count"],
            primitive["non_coinbase_tx_count"],
            primitive["total_out_sat"],
            primitive["total_fee_sat"],
            primitive["subsidy_sat"],
            primitive["issued_sat"],
            primitive["miner_revenue_sat"],
            primitive["input_count"],
            primitive["output_count"],
            primitive["block_size_bytes"],
            primitive["block_weight_wu"],
            primitive["block_vsize_vb"],
            primitive["tx_size_total_bytes"],
            primitive["tx_vsize_total_vb"],
            primitive["avg_fee_sat"],
            primitive["min_feerate_sat_vb"],
            primitive["max_feerate_sat_vb"],
            primitive["segwit_tx_count"],
            primitive["sw_total_size_bytes"],
            primitive["sw_total_weight_wu"],
            primitive["difficulty"],
            primitive["chainwork"],
            primitive["cumulative_supply_sat"],
        ),
    )

    return int(primitive["cumulative_supply_sat"])
