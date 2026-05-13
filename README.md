# MQNODE (MamakQuantNode) - Phase 0 (Bitcoin)

Self-owned crypto data infrastructure with raw chain ingestion, reusable primitive caches, canonical market reference prices, derived metrics, and API delivery.

## Project Direction

MQNODE is being structured as a checkpoint-safe, event-driven data platform:

Bitcoin Core RPC  
`->` raw block ingest  
`->` `btc_primitive_block`  
`->` `btc_primitive_10m`  
`->` `primitive_ready` queue event  
`->` metric workers  
`->` metric tables  
`->` API / downstream systems

The canonical primitive base is `10m`.
Higher intervals such as `1h` and `24h` should be derived from `10m`, not built as separate ingestion bases.

## Current Repo Tree

```text
mqnode/
  api/
    main.py
    routes/
      health.py
      btc_metrics.py
      registry.py
      checkpoints.py
  chains/
    btc/
      rpc.py
      listener.py
      ingest.py
      primitive_builder.py
      block_parser.py
  checkpoints/
    checkpoint_service.py
  config/
    settings.py
    logging_config.py
  core/
    app_context.py
    errors.py
    utils.py
  db/
    connection.py
    schema.sql
    migrations.py
    repositories.py
    sql_versions/
  market/
    price/
      checkpoints.py
      composer.py
      normalize.py
      registry.py
      runtime.py
      sources/
        bitstamp.py
        bybit.py
        binance.py
        okx.py
  metrics/
    btc/
      network/
        nvt.py
      miner/
        miner_revenue.py
      market/
        market_cap.py
      fee/
        fee_metrics.py
  queue/
    redis_conn.py
    jobs.py
    producer.py
  registry/
    metric_registry.py
    dynamic_loader.py
  scripts/
    compose_prices.py
    ingest_price_source.py
    init_db.py
    reconcile_btc.py
    seed_registry.py
    backfill_btc.py
  tests/
    test_checkpoint.py
    test_listener_automation.py
    test_nvt.py
    test_price_composer.py
    test_primitive_builder_automation.py
    test_registry.py
  workers/
    worker_base.py
    btc_primitive_worker.py
    btc_network_worker.py
    btc_miner_worker.py
    btc_market_worker.py
    run_worker.py
Dockerfile
docker-compose.yml
docker-compose.dev.yml
nginx/
requirements.txt
.env.example
README.md
```

## Primitive Philosophy

- `btc_primitive_block` stores reusable block-level facts from RPC ingest.
- `btc_primitive_10m` stores reusable 10-minute on-chain facts and latest known state.
- Final advanced analytics do not belong in primitive tables.

Examples that do **not** belong in `btc_primitive_10m`:
- `nvt_raw`
- `mvrv`
- `sopr`
- z-scores
- rolling statistics
- strategy signals

## 10m Bucket Semantics

MQNODE uses continuous 10-minute buckets, even when no block lands in a bucket.

### Flow Fields

Flow fields represent activity during the bucket and are zero-filled when the bucket is empty.

Examples:
- `block_count`
- `issued_sat_10m`
- `fees_sat_10m`
- `miner_revenue_sat_10m`
- `total_out_sat_10m`
- `total_fee_sat_10m`
- `transferred_sat_10m`
- `transferred_btc_10m`
- `tx_count_10m`
- `input_count_10m`
- `output_count_10m`
- `segwit_tx_count_10m`
- `block_size_total_bytes_10m`
- `block_weight_total_wu_10m`
- `block_vsize_total_vb_10m`

### State Fields

State fields represent the latest known chain state as of bucket end and are forward-filled from the latest known block state.

Examples:
- `supply_total_sat`
- `difficulty_last`
- `chainwork_last`
- `best_block_height_last`
- `halving_epoch`

### Undefined Derived Fields

Undefined fields stay `NULL`, never fake `0`.

Examples:
- `block_interval_mean_sec_10m` when fewer than 2 blocks exist
- `block_interval_median_sec_10m` when fewer than 2 blocks exist
- `avg_fee_sat_10m` when no denominator exists
- `median_fee_sat_10m` when not enough observations exist
- `avg_feerate_sat_vb_10m` when not enough observations exist
- `segwit_share_10m` when `tx_count_10m = 0`
- `tx_size_mean_bytes_10m` when `tx_count_10m = 0`
- `block_size_mean_bytes_10m` when `block_count = 0`

## Price Subsystem

Price data is separated into raw venue tables plus a canonical composed table.

### Raw Venue Tables

- `bitstamp_price_10m`
- `coinbase_price_10m`
- `bybit_price_10m`
- `binance_price_10m`
- `okx_price_10m`
- `bitfinex_price_10m`

### Canonical Table

- `mq_btc_price_10m`

### Source Registry

- `mq_price_source_registry`

### Cloud-Fed Binance

`mqnode_cloud` posts Binance Spot 10-minute rows into local MQNODE through the
internal authenticated ingest API:

```text
Production: https://api.mamakquant.com/api/v1/internal/price/source/binance/10m
Local dev:  http://localhost:8000/api/v1/internal/price/source/binance/10m
```

The POST request must include `Authorization: Bearer <MQNODE_INTERNAL_INGEST_TOKEN>`.
Local MQNODE validates that token with `MQNODE_INTERNAL_INGEST_TOKEN`.

The default seeded Binance registry entry is remote/cloud-fed Spot mode:

```json
{
  "mode": "remote",
  "api_base": "https://api.binance.com",
  "endpoint": "/api/v3/klines",
  "market_type": "spot",
  "historical_start_utc": "2017-08-17T00:00:00Z",
  "auto_historical_backfill": true
}
```

When Binance is in `mode=remote`, the local price-source runtime skips the local
`binance.py` exchange fetch. The cloud feeder owns its own checkpoint, so remote
POST rows upsert into `binance_price_10m` without advancing the local Binance
source checkpoint.

For the Binance WebSocket speed lane, run the collector on a cloud/VPS host
with Binance connectivity and push finalized 10m rows back to the same internal
route:

```text
PRICE_WS_SOURCE=binance
PRICE_WS_SYMBOL=BTCUSDT
PRICE_WS_CHILD_INTERVAL=1m
PRICE_WS_OUTPUT_MODE=remote_push
PRICE_WS_REMOTE_INGEST_URL=https://api.mamakquant.com/api/v1/internal/price/source/binance/10m
PRICE_WS_REMOTE_INGEST_TOKEN=<same secret as MQNODE_INTERNAL_INGEST_TOKEN>
```

The Binance collector uses confirmed Spot kline events only (`k.x == true`),
aggregates 10 confirmed 1m candles into a 10m row, and posts quality metadata
such as `quality_status`, `data_source_mode`, child candle counts, and receive
timing with the OHLCV payload. Local Malaysia MQNODE does not need direct
Binance WS connectivity.

For an existing database that was seeded before this default changed, rerun
`python -m mqnode.scripts.seed_registry` or update only the Binance registry row:

```sql
UPDATE mq_price_source_registry
SET
  market_type = 'spot',
  config_json = '{
    "mode": "remote",
    "api_base": "https://api.binance.com",
    "endpoint": "/api/v3/klines",
    "market_type": "spot",
    "historical_start_utc": "2017-08-17T00:00:00Z",
    "auto_historical_backfill": true
  }'::jsonb,
  updated_at = now()
WHERE source_name = 'binance';
```

### Rules

- Metrics do not call exchange APIs directly.
- Raw venue fetchers must write normalized OHLCV to venue tables first.
- Raw venue tables are sparse and source-factual; they do not receive fake `NULL` rows.
- The canonical composer builds `mq_btc_price_10m` from available venue buckets.
- When `btc_primitive_10m` exists, `mq_btc_price_10m` is timeline-complete against that backbone.
- Primitive tables join only the canonical price table.
- Missing price never blocks primitive writes.
- Missing canonical price leaves price fields `NULL` with `source_count = 0`; it never writes fake `0`.

The phase-0 canonical composer uses volume-weighted OHLC across valid venue rows and sums reported volumes. This is intentionally simple and replaceable later.

## Automation Workflow

### Raw Ingest

1. `mqnode.chains.btc.listener` reads `btc_raw_block_ingestion:block`.
2. It compares checkpoint height with the current Bitcoin Core node tip.
3. If the node tip is ahead, it ingests blocks from `checkpoint + 1`.
4. Each block transaction writes `btc_blocks_raw` and `btc_primitive_block`.
5. The raw checkpoint advances only after the DB write succeeds.
6. `raw_block_ready` is enqueued only after the ingest transaction succeeds.
7. If one block fails, the listener marks the checkpoint `error` at the last safe height and retries cleanly on the next loop.

### Primitive Build

1. The primitive worker receives `raw_block_ready`.
2. It resolves the target 10-minute bucket from `btc_primitive_block`.
3. It rebuilds continuously from the last primitive checkpoint through the target bucket.
4. Missing intermediate buckets are still written as empty 10-minute buckets.
5. Flow fields are zero-filled, state fields are carried forward, and undefined derived fields stay `NULL`.
6. `btc_primitive_10m` is written with UPSERT, so replaying the latest bucket is safe.
7. The primitive checkpoint updates only after the bucket write succeeds.
8. `primitive_ready` is enqueued after commit.

When the listener is idle, it also schedules a primitive tick so the current 10-minute timeline can continue materializing even if no new block arrives.

### Price Composition

1. `btc-price-source-ingestor` runs enabled exchange source fetchers continuously.
2. On first run for a source with no checkpoint, source ingestion starts from `mq_price_source_registry.config_json.historical_start_utc`.
3. After a source checkpoint exists, source ingestion resumes from `checkpoint - 10m`.
4. Raw venue fetchers normalize exchange OHLCV into sparse raw `*_price_10m` tables.
5. `btc-price-composer` uses `btc_primitive_10m` as the timeline backbone when available.
6. It combines each timeline bucket with available raw source rows and writes `mq_btc_price_10m` with UPSERT.
7. If no valid venue rows exist for a bucket, it writes a canonical `NULL` price row with `source_count = 0`.
8. The price composer checkpoint advances only after the write succeeds.

Flow:

```text
First run: source config historical_start_utc -> raw source table -> source checkpoint
Live: checkpoint - 10m -> raw source table -> source checkpoint
Canonical: btc_primitive_10m timeline + raw source rows -> mq_btc_price_10m
```

### Metric Workers

1. Factor workers consume `primitive_ready`.
2. They load enabled metric rows from `metric_registry`.
3. Each metric runs in isolation and writes its own output table.
4. Worker startup includes replay from the latest primitive checkpoint so crash recovery does not depend only on queued jobs.
5. Metric dependencies declared in `metric_registry.dependencies` are checked before execution.

## Checkpoints

Checkpoints are keyed by `(chain, component, interval)`.

Important components:
- `btc_raw_block_ingestion:block`
- `btc_primitive_10m_builder:10m`
- `btc_primitive_10m_scheduler:10m`
- `btc_price_canonical_composer:10m`
- `btc_metric_<metric_name>_<interval>:<interval>`
- `worker_<queue_name>:heartbeat`

On failure, checkpoint status becomes `error` with `error_message`.

## Migration Discipline

- `mqnode/db/migrations.py` now applies a tracked schema snapshot plus numbered SQL files from `mqnode/db/sql_versions/`.
- `scripts/init_db.py` applies migrations instead of relying on an untracked bootstrap only.
- New additive schema changes should be introduced through a new numbered SQL file.

## Health / Observability

`/health` and `/api/v1/btc/health` surface:
- current node tip
- last raw ingested height
- last primitive built height
- last primitive bucket time
- last canonical price bucket time
- lag vs node tip
- checkpoint error visibility
- worker stale visibility through heartbeat checkpoints
- queue depth visibility for the active RQ lanes

## Manual Commands

Start production-style stack with nginx public on port 80 and internal service ports:
```bash
docker compose up -d --build
```

Start local dev stack with the API also published directly on port 8000:
```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

View logs:
```bash
docker compose logs -f btc-listener
docker compose logs -f btc-primitive-worker
docker compose logs -f btc-network-worker
docker compose logs -f btc-price-source-ingestor
docker compose logs -f btc-price-composer
docker compose logs -f mqnode-api
```

Initialize DB:
```bash
python scripts/init_db.py
```

Seed metric registry and price source registry:
```bash
python scripts/seed_registry.py
```

Run continuous price source ingestion:
```bash
python -m mqnode.market.price.runtime ingest-source --all-enabled
```

Run one-time source ingestion:
```bash
python -m mqnode.market.price.runtime ingest-source --all-enabled --once
python -m mqnode.market.price.runtime ingest-source --source binance --once
```

Compose canonical prices from raw venue tables:
```bash
python scripts/compose_prices.py
```

Docker service:
```bash
btc-price-source-ingestor
```

Check and reconcile BTC reorg drift:
```bash
python scripts/reconcile_btc.py
```

Run listener locally:
```bash
python -m mqnode.chains.btc.listener
```

Run a worker locally:
```bash
python -m mqnode.workers.run_worker --queue btc_primitive
python -m mqnode.workers.run_worker --queue btc_network
```

Backfill raw BTC blocks:
```bash
python scripts/backfill_btc.py --mode resume
python scripts/backfill_btc.py --mode full
python scripts/backfill_btc.py --mode range --start-height 0 --end-height 1000
```

Check API through nginx:
```bash
curl http://localhost/health
curl "http://localhost/api/v1/btc/primitive?interval=10m"
curl "http://localhost/api/v1/btc/metrics/nvt?interval=10m"
curl "http://localhost/api/v1/btc/price/canonical?interval=10m"
```

When using `docker-compose.dev.yml`, the API is also reachable directly:
```bash
curl http://localhost:8000/health
curl "http://localhost:8000/api/v1/btc/primitive?interval=10m"
curl "http://localhost:8000/api/v1/btc/metrics/nvt?interval=10m"
curl "http://localhost:8000/api/v1/btc/price/canonical?interval=10m"
```

## Example Seeded Metric

`mqnode/scripts/seed_registry.py` currently seeds:
- `nvt_raw` for `10m` -> `btc_nvt_10m`
- `nvt_raw` for `1h` -> `btc_nvt_1h`

This is only an example derived metric path. The current focus remains the correctness of raw ingest, primitive build, price composition, checkpoints, and worker automation.
