from datetime import datetime, timezone

from mqnode.core.utils import iter_bucket_range, to_bucket_start_10m


def test_bucket_rounding():
    dt = datetime(2025, 1, 1, 12, 44, 4, tzinfo=timezone.utc)
    rounded = to_bucket_start_10m(dt)
    assert rounded.minute == 40
    assert rounded.second == 0


def test_bucket_iteration_includes_gaps():
    start = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    end = datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc)
    buckets = list(iter_bucket_range(start, end, 10))
    assert [bucket.minute for bucket in buckets] == [0, 10, 20, 30]
