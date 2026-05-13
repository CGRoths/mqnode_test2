from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator, Optional

SATOSHI_PER_BTC = 100_000_000


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_bucket_start(ts: datetime, minutes: int) -> datetime:
    if minutes <= 0 or 60 % minutes != 0:
        raise ValueError(f'Unsupported bucket size: {minutes}')
    ts = ts.astimezone(timezone.utc)
    minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=minute, second=0, microsecond=0)


def to_bucket_start_10m(ts: datetime) -> datetime:
    return to_bucket_start(ts, 10)


def to_bucket_start_30m(ts: datetime) -> datetime:
    return to_bucket_start(ts, 30)


def to_open_time_ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def safe_div(num: Optional[float], den: Optional[float]) -> Optional[float]:
    if num is None or den in (None, 0):
        return None
    return num / den


def median(values: Iterable[float]) -> Optional[float]:
    s = sorted(values)
    if not s:
        return None
    n = len(s)
    mid = n // 2
    return float(s[mid]) if n % 2 else float((s[mid - 1] + s[mid]) / 2)


def iter_bucket_range(start: datetime, end: datetime, minutes: int) -> Iterator[datetime]:
    start = to_bucket_start(start, minutes)
    end = to_bucket_start(end, minutes)
    step = timedelta(minutes=minutes)
    current = start
    while current <= end:
        yield current
        current += step


def hour_bounds(ts: datetime) -> tuple[datetime, datetime]:
    start = ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return start, start + timedelta(hours=1)
