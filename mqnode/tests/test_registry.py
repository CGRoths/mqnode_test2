from mqnode.registry.dynamic_loader import load_function
from mqnode.scripts.seed_registry import SQL


def test_dynamic_loader():
    fn = load_function('mqnode.metrics.btc.network.nvt', '_calc_row')
    assert callable(fn)


def test_seed_registry_nvt_dependencies_include_canonical_price():
    assert '["btc_primitive_10m", "mq_btc_price_10m"]' in SQL
    assert 'dependencies = EXCLUDED.dependencies' in SQL
