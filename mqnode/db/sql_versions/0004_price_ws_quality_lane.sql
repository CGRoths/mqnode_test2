ALTER TABLE bitstamp_price_10m
ADD COLUMN IF NOT EXISTS data_source_mode TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS quality_status TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS is_revised BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS revision_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER,
ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER;

ALTER TABLE bybit_price_10m
ADD COLUMN IF NOT EXISTS data_source_mode TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS quality_status TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS is_revised BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS revision_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER,
ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER;

ALTER TABLE binance_price_10m
ADD COLUMN IF NOT EXISTS data_source_mode TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS quality_status TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS is_revised BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS revision_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER,
ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER;

ALTER TABLE okx_price_10m
ADD COLUMN IF NOT EXISTS data_source_mode TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS quality_status TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS is_revised BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS revision_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER,
ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER;

ALTER TABLE coinbase_price_10m
ADD COLUMN IF NOT EXISTS data_source_mode TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS quality_status TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS is_revised BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS revision_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER,
ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER;

ALTER TABLE kraken_price_10m
ADD COLUMN IF NOT EXISTS data_source_mode TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS quality_status TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS is_revised BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS revision_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER,
ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER;

ALTER TABLE bitfinex_price_10m
ADD COLUMN IF NOT EXISTS data_source_mode TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS quality_status TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS is_revised BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS revision_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER,
ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER;

ALTER TABLE gemini_price_10m
ADD COLUMN IF NOT EXISTS data_source_mode TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS quality_status TEXT DEFAULT 'rest_historical',
ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS is_revised BOOLEAN DEFAULT false,
ADD COLUMN IF NOT EXISTS revision_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER,
ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER;

CREATE TABLE IF NOT EXISTS mq_price_ws_source_status (
  source_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  last_message_at TIMESTAMPTZ,
  last_closed_bucket_start_utc TIMESTAMPTZ,
  current_bucket_start_utc TIMESTAMPTZ,
  current_bucket_dirty BOOLEAN DEFAULT false,
  dirty_reason TEXT,
  reconnect_count BIGINT DEFAULT 0,
  last_error TEXT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY(source_name, symbol)
);

CREATE TABLE IF NOT EXISTS mq_price_source_gaps (
  id BIGSERIAL PRIMARY KEY,
  source_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  bucket_start_utc TIMESTAMPTZ,
  gap_start_utc TIMESTAMPTZ,
  gap_end_utc TIMESTAMPTZ,
  reason TEXT,
  repair_status TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  repaired_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS mq_price_revision_log (
  id BIGSERIAL PRIMARY KEY,
  source_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  bucket_start_utc TIMESTAMPTZ NOT NULL,
  old_open_price_usd NUMERIC,
  old_high_price_usd NUMERIC,
  old_low_price_usd NUMERIC,
  old_close_price_usd NUMERIC,
  old_volume_btc NUMERIC,
  new_open_price_usd NUMERIC,
  new_high_price_usd NUMERIC,
  new_low_price_usd NUMERIC,
  new_close_price_usd NUMERIC,
  new_volume_btc NUMERIC,
  revision_reason TEXT,
  old_payload JSONB,
  new_payload JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mq_latency_events (
  id BIGSERIAL PRIMARY KEY,
  bucket_start_utc TIMESTAMPTZ,
  source_name TEXT,
  symbol TEXT,
  stage TEXT NOT NULL,
  stage_time_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  latency_ms_from_bucket_end BIGINT,
  metadata_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_mq_price_source_gaps_source_bucket
ON mq_price_source_gaps(source_name, symbol, bucket_start_utc);

CREATE INDEX IF NOT EXISTS idx_mq_price_revision_log_source_bucket
ON mq_price_revision_log(source_name, symbol, bucket_start_utc);

CREATE INDEX IF NOT EXISTS idx_mq_latency_events_bucket_stage
ON mq_latency_events(bucket_start_utc, stage);
