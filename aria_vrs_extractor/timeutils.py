"""Time helper utilities."""

from __future__ import annotations

import datetime as _dt
from typing import Iterable, Optional


def ns_to_iso8601(ns: int) -> str:
    """Convert nanoseconds since epoch to ISO 8601 string."""
    seconds, nanoseconds = divmod(ns, 1_000_000_000)
    dt = _dt.datetime.utcfromtimestamp(seconds).replace(tzinfo=_dt.timezone.utc)
    return dt.replace(microsecond=nanoseconds // 1000).isoformat()


def timestamp_range_ns(timestamps: Iterable[int]) -> Optional[dict]:
    values = list(timestamps)
    if not values:
        return None
    start = min(values)
    end = max(values)
    return {"start": int(start), "end": int(end)}


def derive_partition_dt(ns: Optional[int]) -> Optional[str]:
    if ns is None:
        return None
    seconds = ns / 1_000_000_000
    dt = _dt.datetime.utcfromtimestamp(seconds)
    return dt.strftime("%Y/%m/%d")


__all__ = ["ns_to_iso8601", "timestamp_range_ns", "derive_partition_dt"]
