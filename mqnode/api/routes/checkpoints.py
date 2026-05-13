from fastapi import APIRouter, Query

from mqnode.api.schemas import ListResponse
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB

router = APIRouter(prefix='/api/v1/btc')


@router.get('/checkpoints', response_model=ListResponse)
def checkpoints(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    with DB(get_settings()).cursor() as cur:
        cur.execute('SELECT COUNT(*) AS count FROM sync_checkpoints WHERE chain = %s', ('BTC',))
        count = int(cur.fetchone()['count'])
        cur.execute(
            '''
            SELECT * FROM sync_checkpoints
            WHERE chain = %s
            ORDER BY component, interval
            LIMIT %s OFFSET %s
            ''',
            ('BTC', limit, offset),
        )
        rows = cur.fetchall()
    return {'count': count, 'limit': limit, 'offset': offset, 'interval': None, 'items': rows}
