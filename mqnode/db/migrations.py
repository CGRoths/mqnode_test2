from __future__ import annotations

import hashlib
from pathlib import Path

from mqnode.db.connection import DB

SCHEMA_SNAPSHOT_VERSION = '0000_schema_snapshot'
MIGRATIONS_DIR = Path(__file__).with_name('sql_versions')


def _migration_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _ensure_migration_table(cur) -> None:
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version TEXT PRIMARY KEY,
          checksum TEXT NOT NULL,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        '''
    )


def _get_applied_versions(cur) -> set[str]:
    _ensure_migration_table(cur)
    cur.execute('SELECT version FROM schema_migrations ORDER BY version ASC')
    return {row['version'] for row in cur.fetchall()}


def _record_applied_version(cur, version: str, checksum: str) -> None:
    cur.execute(
        '''
        INSERT INTO schema_migrations(version, checksum)
        VALUES (%s, %s)
        ON CONFLICT (version) DO UPDATE SET checksum = EXCLUDED.checksum
        ''',
        (version, checksum),
    )


def _bootstrap_schema_snapshot(cur) -> None:
    """Apply the baseline schema once so later changes can be tracked incrementally."""
    schema_path = Path(__file__).with_name('schema.sql')
    cur.execute(schema_path.read_text(encoding='utf-8'))
    _record_applied_version(cur, SCHEMA_SNAPSHOT_VERSION, _migration_checksum(schema_path))


def run_migrations(db: DB) -> None:
    """Apply the schema snapshot and any pending versioned SQL migrations."""
    migration_files = sorted(MIGRATIONS_DIR.glob('*.sql'))
    with db.cursor() as cur:
        applied_versions = _get_applied_versions(cur)
        if SCHEMA_SNAPSHOT_VERSION not in applied_versions:
            _bootstrap_schema_snapshot(cur)
            applied_versions = _get_applied_versions(cur)

        for migration_file in migration_files:
            version = migration_file.stem
            if version in applied_versions:
                continue
            cur.execute(migration_file.read_text(encoding='utf-8'))
            _record_applied_version(cur, version, _migration_checksum(migration_file))


def run_schema(db: DB) -> None:
    run_migrations(db)
