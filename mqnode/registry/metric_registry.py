from __future__ import annotations


def get_enabled_metrics(db, chain: str, factor: str):
    with db.cursor() as cur:
        cur.execute(
            '''
            SELECT * FROM metric_registry
            WHERE enabled = true AND chain = %s AND factor = %s
            ORDER BY id ASC
            ''',
            (chain, factor),
        )
        return cur.fetchall()
