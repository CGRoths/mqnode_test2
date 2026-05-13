from __future__ import annotations

import json

from mqnode.config.settings import get_settings
from mqnode.db.connection import DB
from mqnode.market.price.registry import get_price_sources

SQL = '''
INSERT INTO metric_registry(
  metric_name, chain, factor, module_path, function_name, interval, enabled, version, output_table, dependencies
) VALUES
  (
    'nvt_raw', 'BTC', 'NETWORK', 'mqnode.metrics.btc.network.nvt', 'calculate_nvt',
    '10m', true, 'v1', 'btc_nvt_10m', '["btc_primitive_10m", "mq_btc_price_10m"]'::jsonb
  ),
  (
    'nvt_raw', 'BTC', 'NETWORK', 'mqnode.metrics.btc.network.nvt', 'calculate_nvt',
    '1h', true, 'v1', 'btc_nvt_1h', '["btc_primitive_10m", "mq_btc_price_10m"]'::jsonb
  )
ON CONFLICT (metric_name, chain, interval, version)
DO UPDATE SET
  enabled = EXCLUDED.enabled,
  dependencies = EXCLUDED.dependencies,
  updated_at = now();
'''

PRICE_SOURCE_SQL = '''
INSERT INTO mq_price_source_registry(
  source_name,
  table_name,
  asset_symbol,
  base_asset,
  quote_asset,
  module_path,
  interval,
  source_interval,
  target_interval,
  market_type,
  priority_rank,
  enabled,
  is_optional,
  supports_full_historical_replay,
  config_json,
  notes
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
ON CONFLICT (source_name) DO UPDATE SET
  table_name = EXCLUDED.table_name,
  asset_symbol = EXCLUDED.asset_symbol,
  base_asset = EXCLUDED.base_asset,
  quote_asset = EXCLUDED.quote_asset,
  module_path = EXCLUDED.module_path,
  interval = EXCLUDED.interval,
  source_interval = EXCLUDED.source_interval,
  target_interval = EXCLUDED.target_interval,
  market_type = EXCLUDED.market_type,
  priority_rank = EXCLUDED.priority_rank,
  enabled = EXCLUDED.enabled,
  is_optional = EXCLUDED.is_optional,
  supports_full_historical_replay = EXCLUDED.supports_full_historical_replay,
  config_json = EXCLUDED.config_json,
  notes = EXCLUDED.notes,
  updated_at = now();
'''

def main() -> None:
    with DB(get_settings()).cursor() as cur:
        cur.execute(SQL)
        for source in get_price_sources():
            cur.execute(
                PRICE_SOURCE_SQL,
                (
                    source.source_name,
                    source.table_name,
                    source.asset_symbol,
                    source.base_asset,
                    source.quote_asset,
                    source.module_path,
                    source.interval,
                    source.source_interval,
                    source.target_interval,
                    source.market_type,
                    source.priority_rank,
                    source.default_enabled,
                    source.is_optional,
                    source.supports_full_historical_replay,
                    json.dumps(source.config or {}),
                    source.notes,
                ),
            )
    print('Metric registry seeded.')


if __name__ == '__main__':
    main()
