CREATE TABLE IF NOT EXISTS sync_checkpoints (
  chain TEXT NOT NULL,
  component TEXT NOT NULL,
  interval TEXT NOT NULL,
  last_height BIGINT,
  last_bucket_time TIMESTAMPTZ,
  status TEXT,
  error_message TEXT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (chain, component, interval)
);

CREATE TABLE IF NOT EXISTS metric_registry (
  id SERIAL PRIMARY KEY,
  metric_name TEXT NOT NULL,
  chain TEXT NOT NULL,
  factor TEXT NOT NULL,
  module_path TEXT NOT NULL,
  function_name TEXT NOT NULL,
  interval TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  version TEXT NOT NULL DEFAULT 'v1',
  output_table TEXT NOT NULL,
  dependencies JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(metric_name, chain, interval, version)
);

CREATE TABLE IF NOT EXISTS mq_price_source_registry (
  source_name TEXT PRIMARY KEY,
  table_name TEXT NOT NULL UNIQUE,
  asset_symbol TEXT NOT NULL,
  base_asset TEXT NOT NULL,
  quote_asset TEXT NOT NULL,
  module_path TEXT NOT NULL,
  interval TEXT NOT NULL,
  source_interval TEXT NOT NULL DEFAULT '10m',
  target_interval TEXT NOT NULL DEFAULT '10m',
  market_type TEXT NOT NULL DEFAULT 'spot',
  priority_rank INTEGER NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  is_optional BOOLEAN NOT NULL DEFAULT FALSE,
  supports_full_historical_replay BOOLEAN NOT NULL DEFAULT TRUE,
  config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS btc_blocks_raw (
  height BIGINT PRIMARY KEY,
  block_hash TEXT UNIQUE,
  previous_block_hash TEXT,
  block_time TIMESTAMPTZ,
  median_time TIMESTAMPTZ,
  tx_count INTEGER,
  size BIGINT,
  stripped_size BIGINT,
  weight BIGINT,
  difficulty NUMERIC,
  chainwork TEXT,
  version INTEGER,
  merkle_root TEXT,
  raw_json JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS btc_primitive_block (
  height BIGINT PRIMARY KEY,
  block_hash TEXT,
  block_time TIMESTAMPTZ,
  median_time TIMESTAMPTZ,
  tx_count INTEGER,
  non_coinbase_tx_count INTEGER,
  total_out_sat BIGINT,
  total_fee_sat BIGINT,
  subsidy_sat BIGINT,
  issued_sat BIGINT,
  miner_revenue_sat BIGINT,
  input_count INTEGER,
  output_count INTEGER,
  block_size_bytes BIGINT,
  block_weight_wu BIGINT,
  block_vsize_vb BIGINT,
  tx_size_total_bytes BIGINT,
  tx_vsize_total_vb BIGINT,
  avg_fee_sat NUMERIC,
  min_feerate_sat_vb NUMERIC,
  max_feerate_sat_vb NUMERIC,
  segwit_tx_count INTEGER,
  sw_total_size_bytes BIGINT,
  sw_total_weight_wu BIGINT,
  difficulty NUMERIC,
  chainwork TEXT,
  cumulative_supply_sat NUMERIC,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS btc_primitive_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  open_time_ms BIGINT NOT NULL,
  first_height BIGINT,
  last_height BIGINT,
  block_count INTEGER NOT NULL DEFAULT 0,
  first_block_time_utc TIMESTAMPTZ,
  last_block_time_utc TIMESTAMPTZ,
  issued_sat_10m NUMERIC NOT NULL DEFAULT 0,
  fees_sat_10m NUMERIC NOT NULL DEFAULT 0,
  miner_revenue_sat_10m NUMERIC NOT NULL DEFAULT 0,
  supply_total_sat NUMERIC,
  block_reward_sat_avg NUMERIC,
  halving_epoch INTEGER,
  total_out_sat_10m NUMERIC NOT NULL DEFAULT 0,
  total_fee_sat_10m NUMERIC NOT NULL DEFAULT 0,
  transferred_sat_10m NUMERIC NOT NULL DEFAULT 0,
  transferred_btc_10m NUMERIC NOT NULL DEFAULT 0,
  tx_count_10m INTEGER NOT NULL DEFAULT 0,
  non_coinbase_tx_count_10m INTEGER NOT NULL DEFAULT 0,
  input_count_10m INTEGER NOT NULL DEFAULT 0,
  output_count_10m INTEGER NOT NULL DEFAULT 0,
  tx_rate_per_sec_10m NUMERIC NOT NULL DEFAULT 0,
  block_size_total_bytes_10m NUMERIC NOT NULL DEFAULT 0,
  block_size_mean_bytes_10m NUMERIC,
  block_weight_total_wu_10m NUMERIC NOT NULL DEFAULT 0,
  block_weight_mean_wu_10m NUMERIC,
  block_vsize_total_vb_10m NUMERIC NOT NULL DEFAULT 0,
  tx_size_total_bytes_10m NUMERIC NOT NULL DEFAULT 0,
  tx_size_mean_bytes_10m NUMERIC,
  block_interval_mean_sec_10m NUMERIC,
  block_interval_median_sec_10m NUMERIC,
  avg_fee_sat_10m NUMERIC,
  median_fee_sat_10m NUMERIC,
  avg_feerate_sat_vb_10m NUMERIC,
  min_feerate_sat_vb_10m NUMERIC,
  max_feerate_sat_vb_10m NUMERIC,
  utxo_increase_10m NUMERIC NOT NULL DEFAULT 0,
  utxo_count_total NUMERIC,
  utxo_size_inc_bytes_10m NUMERIC,
  utxo_set_size_total_bytes NUMERIC,
  spent_output_count_10m NUMERIC NOT NULL DEFAULT 0,
  created_output_count_10m NUMERIC NOT NULL DEFAULT 0,
  segwit_tx_count_10m INTEGER NOT NULL DEFAULT 0,
  segwit_share_10m NUMERIC,
  sw_total_size_bytes_10m NUMERIC NOT NULL DEFAULT 0,
  sw_total_weight_wu_10m NUMERIC NOT NULL DEFAULT 0,
  difficulty_last NUMERIC,
  chainwork_last TEXT,
  hashrate_est_last NUMERIC,
  hashrate_est_mean_10m NUMERIC,
  best_block_height_last BIGINT,
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  market_cap_usd NUMERIC,
  onchain_volume_usd_raw_10m NUMERIC,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bitstamp_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  symbol TEXT NOT NULL DEFAULT 'BTCUSD',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  trade_count BIGINT,
  raw_payload JSONB,
  source_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bybit_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  symbol TEXT NOT NULL DEFAULT 'BTCUSD',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  trade_count BIGINT,
  raw_payload JSONB,
  source_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS binance_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  symbol TEXT NOT NULL DEFAULT 'BTCUSD',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  trade_count BIGINT,
  raw_payload JSONB,
  source_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS okx_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  symbol TEXT NOT NULL DEFAULT 'BTCUSD',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  trade_count BIGINT,
  raw_payload JSONB,
  source_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS coinbase_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  symbol TEXT NOT NULL DEFAULT 'BTC-USD',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  trade_count BIGINT,
  raw_payload JSONB,
  source_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kraken_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  symbol TEXT NOT NULL DEFAULT 'XBT/USD',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  trade_count BIGINT,
  raw_payload JSONB,
  source_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bitfinex_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  symbol TEXT NOT NULL DEFAULT 'tBTCUSD',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  trade_count BIGINT,
  raw_payload JSONB,
  source_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gemini_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  symbol TEXT NOT NULL DEFAULT 'BTCUSD',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  trade_count BIGINT,
  raw_payload JSONB,
  source_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mq_btc_price_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  source_count INTEGER NOT NULL DEFAULT 0,
  source_names JSONB NOT NULL DEFAULT '[]'::jsonb,
  composition_method TEXT NOT NULL DEFAULT 'median',
  open_price_usd NUMERIC,
  high_price_usd NUMERIC,
  low_price_usd NUMERIC,
  close_price_usd NUMERIC,
  volume_btc NUMERIC,
  volume_usd NUMERIC,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mq_btc_price_fair_10m_details (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  bitstamp_open_price NUMERIC,
  bitstamp_high_price NUMERIC,
  bitstamp_low_price NUMERIC,
  bitstamp_close_price NUMERIC,
  bitstamp_volume_btc NUMERIC,
  coinbase_open_price NUMERIC,
  coinbase_high_price NUMERIC,
  coinbase_low_price NUMERIC,
  coinbase_close_price NUMERIC,
  coinbase_volume_btc NUMERIC,
  binance_open_price NUMERIC,
  binance_high_price NUMERIC,
  binance_low_price NUMERIC,
  binance_close_price NUMERIC,
  binance_volume_btc NUMERIC,
  bybit_open_price NUMERIC,
  bybit_high_price NUMERIC,
  bybit_low_price NUMERIC,
  bybit_close_price NUMERIC,
  bybit_volume_btc NUMERIC,
  okx_open_price NUMERIC,
  okx_high_price NUMERIC,
  okx_low_price NUMERIC,
  okx_close_price NUMERIC,
  okx_volume_btc NUMERIC,
  kraken_open_price NUMERIC,
  kraken_high_price NUMERIC,
  kraken_low_price NUMERIC,
  kraken_close_price NUMERIC,
  kraken_volume_btc NUMERIC,
  bitfinex_open_price NUMERIC,
  bitfinex_high_price NUMERIC,
  bitfinex_low_price NUMERIC,
  bitfinex_close_price NUMERIC,
  bitfinex_volume_btc NUMERIC,
  gemini_open_price NUMERIC,
  gemini_high_price NUMERIC,
  gemini_low_price NUMERIC,
  gemini_close_price NUMERIC,
  gemini_volume_btc NUMERIC,
  fair_open_price NUMERIC,
  fair_high_price NUMERIC,
  fair_low_price NUMERIC,
  fair_close_price NUMERIC,
  total_volume_btc NUMERIC,
  source_count INTEGER NOT NULL DEFAULT 0,
  source_names JSONB NOT NULL DEFAULT '[]'::jsonb,
  composition_method TEXT NOT NULL DEFAULT 'volume_weighted_ohlc_v1',
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS btc_reorg_events (
  id BIGSERIAL PRIMARY KEY,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  previous_checkpoint_height BIGINT NOT NULL,
  common_height BIGINT NOT NULL,
  diverged_height BIGINT NOT NULL,
  rollback_bucket_start_utc TIMESTAMPTZ,
  old_block_hash TEXT,
  canonical_block_hash TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS btc_nvt_10m (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  price_usd NUMERIC,
  supply_total_sat NUMERIC,
  market_cap_usd NUMERIC,
  transferred_sat NUMERIC,
  transferred_value_usd NUMERIC,
  nvt_raw NUMERIC,
  source_start_height BIGINT,
  source_end_height BIGINT,
  version TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS btc_nvt_1h (
  bucket_start_utc TIMESTAMPTZ PRIMARY KEY,
  price_usd NUMERIC,
  supply_total_sat NUMERIC,
  market_cap_usd NUMERIC,
  transferred_sat NUMERIC,
  transferred_value_usd NUMERIC,
  nvt_raw NUMERIC,
  source_start_height BIGINT,
  source_end_height BIGINT,
  version TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_metric_registry_chain_factor_enabled
ON metric_registry(chain, factor, enabled);

CREATE INDEX IF NOT EXISTS idx_sync_checkpoints_chain_component
ON sync_checkpoints(chain, component, interval, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_btc_primitive_block_bucket_time
ON btc_primitive_block((COALESCE(median_time, block_time)));

CREATE INDEX IF NOT EXISTS idx_btc_primitive_10m_last_height
ON btc_primitive_10m(last_height);

CREATE INDEX IF NOT EXISTS idx_mq_btc_price_10m_updated_at
ON mq_btc_price_10m(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_mq_btc_price_fair_10m_details_updated_at
ON mq_btc_price_fair_10m_details(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_btc_reorg_events_detected_at
ON btc_reorg_events(detected_at DESC);
