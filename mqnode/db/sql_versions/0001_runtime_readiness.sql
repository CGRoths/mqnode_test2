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

CREATE INDEX IF NOT EXISTS idx_btc_reorg_events_detected_at
ON btc_reorg_events(detected_at DESC);
