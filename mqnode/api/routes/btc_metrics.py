from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Query

from mqnode.api.schemas import ListResponse
from mqnode.config.settings import get_settings
from mqnode.db.connection import DB

router = APIRouter(prefix='/api/v1/btc')


@router.get('/metrics/nvt', response_model=ListResponse)
def nvt(
    interval: Literal['10m', '1h'] = Query('10m'),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    table = 'btc_nvt_10m' if interval == '10m' else 'btc_nvt_1h'
    start = start or datetime.now(timezone.utc) - timedelta(days=1)
    end = end or datetime.now(timezone.utc)
    with DB(get_settings()).cursor() as cur:
        cur.execute(
            f'''
            SELECT COUNT(*) AS count
            FROM {table}
            WHERE bucket_start_utc >= %s
              AND bucket_start_utc <= %s
            ''',
            (start, end),
        )
        count = int(cur.fetchone()['count'])
        cur.execute(
            f'''
            SELECT *
            FROM {table}
            WHERE bucket_start_utc >= %s
              AND bucket_start_utc <= %s
            ORDER BY bucket_start_utc
            LIMIT %s OFFSET %s
            ''',
            (start, end, limit, offset),
        )
        rows = cur.fetchall()
    return {
        'count': count,
        'limit': limit,
        'offset': offset,
        'interval': interval,
        'items': rows,
    }


@router.get('/primitive', response_model=ListResponse)
def primitive(
    interval: Literal['10m'] = Query('10m'),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    start = start or datetime.now(timezone.utc) - timedelta(days=1)
    end = end or datetime.now(timezone.utc)
    with DB(get_settings()).cursor() as cur:
        cur.execute(
            '''
            SELECT COUNT(*) AS count
            FROM btc_primitive_10m
            WHERE bucket_start_utc >= %s
              AND bucket_start_utc <= %s
            ''',
            (start, end),
        )
        count = int(cur.fetchone()['count'])
        cur.execute(
            '''
            SELECT *
            FROM btc_primitive_10m
            WHERE bucket_start_utc >= %s
              AND bucket_start_utc <= %s
            ORDER BY bucket_start_utc
            LIMIT %s OFFSET %s
            ''',
            (start, end, limit, offset),
        )
        rows = cur.fetchall()
    return {
        'count': count,
        'limit': limit,
        'offset': offset,
        'interval': interval,
        'items': rows,
    }


@router.get('/price/canonical', response_model=ListResponse)
def canonical_price(
    interval: Literal['10m'] = Query('10m'),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    start = start or datetime.now(timezone.utc) - timedelta(days=1)
    end = end or datetime.now(timezone.utc)
    with DB(get_settings()).cursor() as cur:
        cur.execute(
            '''
            SELECT COUNT(*) AS count
            FROM mq_btc_price_10m
            WHERE bucket_start_utc >= %s
              AND bucket_start_utc <= %s
            ''',
            (start, end),
        )
        count = int(cur.fetchone()['count'])
        cur.execute(
            '''
            SELECT *
            FROM mq_btc_price_10m
            WHERE bucket_start_utc >= %s
              AND bucket_start_utc <= %s
            ORDER BY bucket_start_utc
            LIMIT %s OFFSET %s
            ''',
            (start, end, limit, offset),
        )
        rows = cur.fetchall()
    return {
        'count': count,
        'limit': limit,
        'offset': offset,
        'interval': interval,
        'items': rows,
    }
