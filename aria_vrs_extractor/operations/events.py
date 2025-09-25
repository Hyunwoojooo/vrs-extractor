"""Event merging implementation."""

from __future__ import annotations

import heapq
import json
import logging
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from ..constants import DONE_DIRNAME, EVENTS_FILE
from ..io import Filesystem, ensure_directory, is_remote, join_uri
from ..logger import LogTimer
from ..status import clear_done, is_done, mark_done, step_done_path


def _read_done_summary(fs: Filesystem, path: str) -> Optional[Dict]:
    if not fs.exists(path):
        return None
    with fs.open(path, "rt") as handle:
        content = handle.read().strip()
        if not content:
            return None
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return None


def _iter_jsonl(fs: Filesystem, path: str) -> Iterator[Tuple[int, Dict]]:
    with fs.open(path, "rt") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = payload.get("ts_ns")
            if ts is None:
                continue
            yield int(ts), payload


def merge_events(
    *,
    fs: Filesystem,
    root: str,
    force: bool,
    logger: logging.Logger,
) -> None:
    step_name = "merge_events"
    if not force and is_done(fs, root, step_name):
        logger.info(
            "Event merge skipped (already complete)",
            extra={"step": step_name, "root": root},
        )
        return

    if force:
        clear_done(fs, root, step_name)

    done_dir = join_uri(root, DONE_DIRNAME)
    candidate_steps = [
        "extract_rgb",
        "extract_et",
        "extract_audio",
        "extract_imu",
        "extract_gps",
        "extract_wifi",
        "extract_bt",
    ]

    sensor_files: List[str] = []
    for step in candidate_steps:
        marker_path = join_uri(done_dir, f"{step}.done")
        summary = _read_done_summary(fs, marker_path)
        if summary and "jsonl" in summary:
            sensor_files.append(summary["jsonl"])

    if not sensor_files:
        logger.warning("No sensor JSONL files discovered for event merge", extra={"root": root})
        return

    first_jsonl = sensor_files[0]
    if "/" in first_jsonl:
        sensors_dir = first_jsonl.rsplit("/", 1)[0]
    else:
        sensors_dir = join_uri(root, "sensors")
    events_path = join_uri(sensors_dir, EVENTS_FILE)
    ensure_directory(fs, sensors_dir)

    if force and fs.exists(events_path):
        fs.remove(events_path)

    def initial_entry(idx: int, iterator: Iterator[Tuple[int, Dict]]):
        try:
            ts, payload = next(iterator)
        except StopIteration:
            return None
        return (ts, idx, payload, iterator)

    iterators: List[Iterator[Tuple[int, Dict]]] = [
        _iter_jsonl(fs, path) for path in sensor_files
    ]
    heap: List[Tuple[int, int, Dict, Iterator[Tuple[int, Dict]]]] = []
    for idx, iterator in enumerate(iterators):
        entry = initial_entry(idx, iterator)
        if entry:
            heapq.heappush(heap, entry)

    summary = {
        "sensor": "events",
        "jsonl": events_path,
        "sources": sensor_files,
        "count": 0,
        "ts_first": None,
        "ts_last": None,
    }

    with LogTimer(
        logger,
        "Event merge completed",
        step=step_name,
        device_id=None,
        recording_id=None,
        extra={"events": events_path},
    ):
        with fs.open(events_path, "wt") as output:
            while heap:
                ts, idx, payload, iterator = heapq.heappop(heap)
                summary["count"] = int(summary["count"]) + 1
                summary["ts_first"] = ts if summary["ts_first"] is None else min(summary["ts_first"], ts)
                summary["ts_last"] = ts if summary["ts_last"] is None else max(summary["ts_last"], ts)
                output.write(json.dumps(payload, ensure_ascii=False) + "\n")
                next_entry = initial_entry(idx, iterator)
                if next_entry:
                    heapq.heappush(heap, next_entry)

    mark_done(fs, root, step_name, json.dumps(summary, ensure_ascii=False))
