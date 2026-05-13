from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mqnode.core.errors import DependencyError
from mqnode.metrics.btc.network.nvt import _calc_row, calculate_nvt


class DummyCursor:
    def __init__(self, *, row=None, rows=None):
        self.row = row
        self.rows = rows or []
        self.statements = []
        self.params = []
        self.insert_params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.statements.append(sql)
        self.params.append(params)
        if 'INSERT INTO' in sql:
            self.insert_params = params

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class DummyDB:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_calc_row_zero_safe():
    transferred_value_usd, market_cap_usd, nvt_raw = _calc_row(0, 1, 1)
    assert transferred_value_usd == 0
    assert market_cap_usd == 0
    assert nvt_raw is None


def test_calculate_10m_nvt_joins_canonical_price():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    cursor = DummyCursor(
        row={
            'bucket_start_utc': bucket,
            'first_height': 100,
            'last_height': 101,
            'transferred_sat_10m': 200_000_000,
            'supply_total_sat': 1_000_000_000,
            'close_price_usd': 1,
            'canonical_close_price_usd': 50_000,
        }
    )

    calculate_nvt(DummyDB(cursor), bucket, '10m')

    assert 'LEFT JOIN mq_btc_price_10m' in cursor.statements[0]
    assert cursor.insert_params == (
        bucket,
        50_000,
        1_000_000_000,
        500_000.0,
        200_000_000,
        100_000.0,
        5.0,
        100,
        101,
        'v1',
    )


def test_calculate_10m_nvt_missing_price_raises_dependency_error():
    bucket = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    cursor = DummyCursor(
        row={
            'bucket_start_utc': bucket,
            'first_height': 100,
            'last_height': 101,
            'transferred_sat_10m': 200_000_000,
            'supply_total_sat': 1_000_000_000,
            'canonical_close_price_usd': None,
        }
    )

    with pytest.raises(DependencyError, match='Canonical price is not ready for NVT bucket .* interval 10m'):
        calculate_nvt(DummyDB(cursor), bucket, '10m')

    assert cursor.insert_params is None


def test_calculate_1h_nvt_aggregates_primitive_rows_with_price_join():
    hour = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    rows = []
    prices = [10, 20, 30, 40, 50, 60]
    for index, price in enumerate(prices):
        rows.append(
            {
                'bucket_start_utc': hour + timedelta(minutes=index * 10),
                'first_height': 100 + index,
                'last_height': 100 + index,
                'transferred_sat_10m': 100_000_000,
                'supply_total_sat': 1_000_000_000,
                'canonical_close_price_usd': price,
            }
        )
    cursor = DummyCursor(rows=rows)

    calculate_nvt(DummyDB(cursor), hour + timedelta(minutes=30), '1h')

    assert 'LEFT JOIN mq_btc_price_10m' in cursor.statements[0]
    assert cursor.params[0] == (hour, hour + timedelta(hours=1))
    assert cursor.insert_params == (
        hour,
        60,
        1_000_000_000,
        600.0,
        600_000_000,
        210.0,
        pytest.approx(600 / 210),
        100,
        105,
        'v1',
    )


def test_calculate_1h_nvt_partial_missing_price_raises_dependency_error():
    hour = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    rows = []
    prices = [10, 20, None, 40, 50, 60]
    for index, price in enumerate(prices):
        rows.append(
            {
                'bucket_start_utc': hour + timedelta(minutes=index * 10),
                'first_height': 100 + index,
                'last_height': 100 + index,
                'transferred_sat_10m': 100_000_000,
                'supply_total_sat': 1_000_000_000,
                'canonical_close_price_usd': price,
            }
        )
    cursor = DummyCursor(rows=rows)

    with pytest.raises(DependencyError, match='Canonical price is not ready for NVT bucket .* interval 1h'):
        calculate_nvt(DummyDB(cursor), hour, '1h')

    assert cursor.insert_params is None


def test_calculate_1h_nvt_allows_missing_price_for_zero_volume_rows():
    hour = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    rows = []
    transfers = [100_000_000, 0, 100_000_000, 0, 0, 0]
    prices = [10, None, 30, None, 40, None]
    for index, (transferred_sat, price) in enumerate(zip(transfers, prices)):
        rows.append(
            {
                'bucket_start_utc': hour + timedelta(minutes=index * 10),
                'first_height': 100 + index,
                'last_height': 100 + index,
                'transferred_sat_10m': transferred_sat,
                'supply_total_sat': 1_000_000_000,
                'canonical_close_price_usd': price,
            }
        )
    cursor = DummyCursor(rows=rows)

    calculate_nvt(DummyDB(cursor), hour, '1h')

    assert cursor.insert_params == (
        hour,
        40,
        1_000_000_000,
        400.0,
        200_000_000,
        40.0,
        10.0,
        100,
        105,
        'v1',
    )


def test_calculate_1h_nvt_zero_volume_keeps_nvt_null():
    hour = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    rows = []
    for index in range(6):
        rows.append(
            {
                'bucket_start_utc': hour + timedelta(minutes=index * 10),
                'first_height': 100 + index,
                'last_height': 100 + index,
                'transferred_sat_10m': 0,
                'supply_total_sat': 1_000_000_000,
                'canonical_close_price_usd': 60,
            }
        )
    cursor = DummyCursor(rows=rows)

    calculate_nvt(DummyDB(cursor), hour, '1h')

    assert cursor.insert_params == (
        hour,
        60,
        1_000_000_000,
        600.0,
        0,
        0,
        None,
        100,
        105,
        'v1',
    )
