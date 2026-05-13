CREATE TABLE IF NOT EXISTS mq_price_source_lifecycle_status (
  source_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  table_name TEXT,
  enabled BOOLEAN DEFAULT true,
  historical_mode TEXT,
  live_mode TEXT,
  current_state TEXT NOT NULL,
  rest_sync_status TEXT,
  ws_status TEXT,
  last_rest_bucket TIMESTAMPTZ,
  last_ws_bucket TIMESTAMPTZ,
  last_canonical_bucket TIMESTAMPTZ,
  rest_synced_at TIMESTAMPTZ,
  ws_started_at TIMESTAMPTZ,
  last_transition_at TIMESTAMPTZ DEFAULT now(),
  last_error TEXT,
  metadata_json JSONB,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY(source_name, symbol)
);

CREATE INDEX IF NOT EXISTS idx_mq_price_source_lifecycle_status_source_state
ON mq_price_source_lifecycle_status(source_name, current_state);

CREATE INDEX IF NOT EXISTS idx_mq_price_source_lifecycle_status_live_mode
ON mq_price_source_lifecycle_status(live_mode);

CREATE INDEX IF NOT EXISTS idx_mq_price_source_lifecycle_status_updated_at
ON mq_price_source_lifecycle_status(updated_at);
