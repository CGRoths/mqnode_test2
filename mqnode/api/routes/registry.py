from fastapi import APIRouter, Query

from mqnode.api.schemas import ListResponse
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB

router = APIRouter(prefix='/api/v1/btc')


@router.get('/registry', response_model=ListResponse)
def registry(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    with DB(get_settings()).cursor() as cur:
        cur.execute('SELECT COUNT(*) AS count FROM metric_registry WHERE enabled = true')
        count = int(cur.fetchone()['count'])
        cur.execute(
            '''
            SELECT metric_name, chain, factor, interval, version, output_table
            FROM metric_registry
            WHERE enabled = true
            ORDER BY id ASC
            LIMIT %s OFFSET %s
            ''',
            (limit, offset),
        )
        rows = cur.fetchall()
    return {'count': count, 'limit': limit, 'offset': offset, 'interval': None, 'items': rows}
