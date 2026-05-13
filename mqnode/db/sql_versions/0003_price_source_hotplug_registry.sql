ALTER TABLE mq_price_source_registry
ADD COLUMN IF NOT EXISTS module_path TEXT;

ALTER TABLE mq_price_source_registry
ADD COLUMN IF NOT EXISTS source_interval TEXT;

ALTER TABLE mq_price_source_registry
ADD COLUMN IF NOT EXISTS target_interval TEXT;

ALTER TABLE mq_price_source_registry
ADD COLUMN IF NOT EXISTS market_type TEXT;

ALTER TABLE mq_price_source_registry
ADD COLUMN IF NOT EXISTS is_optional BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE mq_price_source_registry
ADD COLUMN IF NOT EXISTS supports_full_historical_replay BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE mq_price_source_registry
ADD COLUMN IF NOT EXISTS config_json JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE mq_price_source_registry
SET
  module_path = COALESCE(module_path, 'mqnode.market.price.sources.' || source_name),
  source_interval = COALESCE(source_interval, interval),
  target_interval = COALESCE(target_interval, interval),
  market_type = COALESCE(market_type, 'spot'),
  config_json = COALESCE(config_json, '{}'::jsonb);

ALTER TABLE mq_price_source_registry
ALTER COLUMN module_path SET NOT NULL;

ALTER TABLE mq_price_source_registry
ALTER COLUMN source_interval SET NOT NULL;

ALTER TABLE mq_price_source_registry
ALTER COLUMN target_interval SET NOT NULL;

ALTER TABLE mq_price_source_registry
ALTER COLUMN market_type SET NOT NULL;
