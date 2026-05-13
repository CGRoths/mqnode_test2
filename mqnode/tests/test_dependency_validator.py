from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mqnode.core.errors import DependencyError
from mqnode.registry.dependency_validator import validate_metric_dependencies


class DummyCursor:
    def __init__(self):
        self.result = None

    def execute(self, query, params=None):
        if 'to_regclass' in query:
            self.result = {'table_ref': 'btc_primitive_10m'}
        elif 'information_schema.columns' in query:
            self.result = {'ok': 1}
        elif 'SELECT 1 FROM btc_primitive_10m' in query:
            self.result = None
        else:
            self.result = None

    def fetchone(self):
        return self.result

    def fetchall(self):
        return [self.result] if self.result else []


class DummyCursorContext:
    def __enter__(self):
        return DummyCursor()

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyDB:
    def cursor(self):
        return DummyCursorContext()


def test_validate_metric_dependencies_raises_when_bucket_is_missing():
    metric = {'dependencies': ['btc_primitive_10m']}
    with pytest.raises(DependencyError):
        validate_metric_dependencies(DummyDB(), metric, datetime(2026, 4, 21, 0, 0, tzinfo=timezone.utc))
