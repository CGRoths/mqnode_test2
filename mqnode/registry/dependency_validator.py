from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from mqnode.core.errors import DependencyError

IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$')


def _normalize_dependency(dep: Any) -> str:
    if isinstance(dep, str):
        return dep
    if isinstance(dep, dict) and isinstance(dep.get('table'), str):
        return dep['table']
    raise DependencyError(f'Unsupported dependency format: {dep!r}')


def _table_exists(cur, table_name: str) -> bool:
    cur.execute('SELECT to_regclass(%s) AS table_ref', (table_name,))
    return (cur.fetchone() or {}).get('table_ref') is not None


def _has_bucket_column(cur, schema_name: str, table_name: str) -> bool:
    cur.execute(
        '''
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = 'bucket_start_utc'
        LIMIT 1
        ''',
        (schema_name, table_name),
    )
    return cur.fetchone() is not None


def validate_metric_dependencies(db, metric: dict[str, Any], bucket_start_utc: datetime) -> None:
    dependencies = metric.get('dependencies') or []
    if not dependencies:
        return

    with db.cursor() as cur:
        for dep in dependencies:
            table_name = _normalize_dependency(dep)
            if not IDENTIFIER_RE.fullmatch(table_name):
                raise DependencyError(f'Invalid dependency table name: {table_name}')
            if not _table_exists(cur, table_name):
                raise DependencyError(f'Metric dependency table does not exist: {table_name}')

            schema_name, raw_table_name = ('public', table_name)
            if '.' in table_name:
                schema_name, raw_table_name = table_name.split('.', 1)
            if not _has_bucket_column(cur, schema_name, raw_table_name):
                continue

            cur.execute(f'SELECT 1 FROM {table_name} WHERE bucket_start_utc = %s LIMIT 1', (bucket_start_utc,))
            if cur.fetchone() is None:
                raise DependencyError(
                    f'Metric dependency {table_name} is not ready for bucket {bucket_start_utc.isoformat()}'
                )
