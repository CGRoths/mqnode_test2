DO $$
DECLARE
  source_table TEXT;
BEGIN
  FOREACH source_table IN ARRAY ARRAY[
    'bitstamp_price_10m',
    'bybit_price_10m',
    'binance_price_10m',
    'okx_price_10m',
    'coinbase_price_10m',
    'kraken_price_10m',
    'bitfinex_price_10m',
    'gemini_price_10m'
  ]
  LOOP
    IF to_regclass(source_table) IS NOT NULL THEN
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS data_source_mode TEXT', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS quality_status TEXT', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS ws_closed_at TIMESTAMPTZ', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS rest_confirmed_at TIMESTAMPTZ', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS is_repaired BOOLEAN NOT NULL DEFAULT FALSE', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS is_revised BOOLEAN NOT NULL DEFAULT FALSE', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS revision_count INTEGER NOT NULL DEFAULT 0', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS first_received_at TIMESTAMPTZ', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS ws_disconnect_count INTEGER NOT NULL DEFAULT 0', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS ws_gap_count INTEGER NOT NULL DEFAULT 0', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS expected_child_candle_count INTEGER', source_table);
      EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS actual_child_candle_count INTEGER', source_table);

      EXECUTE format('ALTER TABLE %I ALTER COLUMN is_repaired SET DEFAULT FALSE', source_table);
      EXECUTE format('UPDATE %I SET is_repaired = FALSE WHERE is_repaired IS NULL', source_table);
      EXECUTE format('ALTER TABLE %I ALTER COLUMN is_repaired SET NOT NULL', source_table);

      EXECUTE format('ALTER TABLE %I ALTER COLUMN is_revised SET DEFAULT FALSE', source_table);
      EXECUTE format('UPDATE %I SET is_revised = FALSE WHERE is_revised IS NULL', source_table);
      EXECUTE format('ALTER TABLE %I ALTER COLUMN is_revised SET NOT NULL', source_table);

      EXECUTE format('ALTER TABLE %I ALTER COLUMN revision_count SET DEFAULT 0', source_table);
      EXECUTE format('UPDATE %I SET revision_count = 0 WHERE revision_count IS NULL', source_table);
      EXECUTE format('ALTER TABLE %I ALTER COLUMN revision_count SET NOT NULL', source_table);

      EXECUTE format('ALTER TABLE %I ALTER COLUMN ws_disconnect_count SET DEFAULT 0', source_table);
      EXECUTE format('UPDATE %I SET ws_disconnect_count = 0 WHERE ws_disconnect_count IS NULL', source_table);
      EXECUTE format('ALTER TABLE %I ALTER COLUMN ws_disconnect_count SET NOT NULL', source_table);

      EXECUTE format('ALTER TABLE %I ALTER COLUMN ws_gap_count SET DEFAULT 0', source_table);
      EXECUTE format('UPDATE %I SET ws_gap_count = 0 WHERE ws_gap_count IS NULL', source_table);
      EXECUTE format('ALTER TABLE %I ALTER COLUMN ws_gap_count SET NOT NULL', source_table);

      EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (quality_status, bucket_start_utc DESC)',
        'idx_' || source_table || '_quality_bucket',
        source_table
      );
      EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (updated_at DESC)',
        'idx_' || source_table || '_updated_at',
        source_table
      );
    END IF;
  END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS mq_price_source_lifecycle_status (
  source_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  table_name TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  historical_mode TEXT NOT NULL,
  live_mode TEXT NOT NULL,
  current_state TEXT NOT NULL,
  rest_sync_status TEXT,
  ws_status TEXT,
  last_rest_bucket TIMESTAMPTZ,
  last_ws_bucket TIMESTAMPTZ,
  last_canonical_bucket TIMESTAMPTZ,
  rest_synced_at TIMESTAMPTZ,
  ws_started_at TIMESTAMPTZ,
  last_transition_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error TEXT,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_name, symbol)
);

ALTER TABLE mq_price_source_lifecycle_status
ADD COLUMN IF NOT EXISTS table_name TEXT,
ADD COLUMN IF NOT EXISTS enabled BOOLEAN DEFAULT TRUE,
ADD COLUMN IF NOT EXISTS historical_mode TEXT,
ADD COLUMN IF NOT EXISTS live_mode TEXT,
ADD COLUMN IF NOT EXISTS rest_sync_status TEXT,
ADD COLUMN IF NOT EXISTS ws_status TEXT,
ADD COLUMN IF NOT EXISTS last_rest_bucket TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_ws_bucket TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_canonical_bucket TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rest_synced_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS ws_started_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_transition_at TIMESTAMPTZ DEFAULT now(),
ADD COLUMN IF NOT EXISTS last_error TEXT,
ADD COLUMN IF NOT EXISTS metadata_json JSONB DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now(),
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

UPDATE mq_price_source_lifecycle_status
SET enabled = TRUE
WHERE enabled IS NULL;

UPDATE mq_price_source_lifecycle_status
SET last_transition_at = now()
WHERE last_transition_at IS NULL;

UPDATE mq_price_source_lifecycle_status
SET metadata_json = '{}'::jsonb
WHERE metadata_json IS NULL;

UPDATE mq_price_source_lifecycle_status
SET created_at = COALESCE(updated_at, now())
WHERE created_at IS NULL;

UPDATE mq_price_source_lifecycle_status
SET updated_at = now()
WHERE updated_at IS NULL;

ALTER TABLE mq_price_source_lifecycle_status
ALTER COLUMN enabled SET DEFAULT TRUE,
ALTER COLUMN enabled SET NOT NULL,
ALTER COLUMN last_transition_at SET DEFAULT now(),
ALTER COLUMN last_transition_at SET NOT NULL,
ALTER COLUMN metadata_json SET DEFAULT '{}'::jsonb,
ALTER COLUMN metadata_json SET NOT NULL,
ALTER COLUMN created_at SET DEFAULT now(),
ALTER COLUMN created_at SET NOT NULL,
ALTER COLUMN updated_at SET DEFAULT now(),
ALTER COLUMN updated_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mq_price_source_lifecycle_status_state_updated_at
ON mq_price_source_lifecycle_status(current_state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_mq_price_source_lifecycle_status_updated_at_desc
ON mq_price_source_lifecycle_status(updated_at DESC);

CREATE TABLE IF NOT EXISTS mq_price_ws_source_status (
  source_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  last_message_at TIMESTAMPTZ,
  last_closed_bucket_start_utc TIMESTAMPTZ,
  current_bucket_start_utc TIMESTAMPTZ,
  current_bucket_dirty BOOLEAN NOT NULL DEFAULT FALSE,
  dirty_reason TEXT,
  reconnect_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_name, symbol)
);

ALTER TABLE mq_price_ws_source_status
ADD COLUMN IF NOT EXISTS last_message_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_closed_bucket_start_utc TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS current_bucket_start_utc TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS current_bucket_dirty BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS dirty_reason TEXT,
ADD COLUMN IF NOT EXISTS reconnect_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS last_error TEXT,
ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now(),
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

UPDATE mq_price_ws_source_status
SET current_bucket_dirty = FALSE
WHERE current_bucket_dirty IS NULL;

UPDATE mq_price_ws_source_status
SET reconnect_count = 0
WHERE reconnect_count IS NULL;

UPDATE mq_price_ws_source_status
SET created_at = COALESCE(updated_at, now())
WHERE created_at IS NULL;

UPDATE mq_price_ws_source_status
SET updated_at = now()
WHERE updated_at IS NULL;

ALTER TABLE mq_price_ws_source_status
ALTER COLUMN current_bucket_dirty SET DEFAULT FALSE,
ALTER COLUMN current_bucket_dirty SET NOT NULL,
ALTER COLUMN reconnect_count SET DEFAULT 0,
ALTER COLUMN reconnect_count SET NOT NULL,
ALTER COLUMN created_at SET DEFAULT now(),
ALTER COLUMN created_at SET NOT NULL,
ALTER COLUMN updated_at SET DEFAULT now(),
ALTER COLUMN updated_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mq_price_ws_source_status_status_updated_at
ON mq_price_ws_source_status(status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_mq_price_ws_source_status_current_bucket
ON mq_price_ws_source_status(current_bucket_start_utc DESC);

CREATE TABLE IF NOT EXISTS mq_price_source_gaps (
  id BIGSERIAL PRIMARY KEY,
  source_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  bucket_start_utc TIMESTAMPTZ NOT NULL,
  gap_start_utc TIMESTAMPTZ,
  gap_end_utc TIMESTAMPTZ,
  reason TEXT NOT NULL,
  repair_status TEXT NOT NULL,
  repaired_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mq_price_source_gaps_source_symbol_bucket
ON mq_price_source_gaps(source_name, symbol, bucket_start_utc DESC);

CREATE INDEX IF NOT EXISTS idx_mq_price_source_gaps_repair_status_created_at
ON mq_price_source_gaps(repair_status, created_at DESC);

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
  revision_reason TEXT NOT NULL,
  old_payload JSONB,
  new_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mq_price_revision_log_source_symbol_bucket
ON mq_price_revision_log(source_name, symbol, bucket_start_utc DESC);

CREATE INDEX IF NOT EXISTS idx_mq_price_revision_log_created_at_desc
ON mq_price_revision_log(created_at DESC);
