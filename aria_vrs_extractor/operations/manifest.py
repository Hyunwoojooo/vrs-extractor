"""Manifest writing implementation."""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse

from ..constants import DONE_DIRNAME
from ..io import FileInfo, Filesystem, ensure_directory, join_uri
from ..logger import LogTimer
from ..status import clear_done, is_done, mark_done
from ..timeutils import derive_partition_dt


def _read_summary(fs: Filesystem, marker_path: str) -> Optional[Dict]:
    if not fs.exists(marker_path):
        return None
    with fs.open(marker_path, "rt") as handle:
        content = handle.read().strip()
        if not content:
            return None
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return None


def _logical_path(root: str, uri: str) -> str:
    if not uri:
        return ""
    root_str = root.rstrip("/")
    if uri.startswith(root_str):
        relative = uri[len(root_str):].lstrip("/")
        return relative
    parsed_root = urlparse(root)
    parsed_uri = urlparse(uri)
    if parsed_root.scheme and parsed_root.scheme == parsed_uri.scheme:
        rel = parsed_uri.path[len(parsed_root.path):].lstrip("/")
        return rel
    return uri


def _collect_directory_checksums(fs: Filesystem, uri: str) -> Dict[str, object]:
    import hashlib

    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    total_bytes = 0
    count = 0
    for file_path in sorted(fs.list_files(uri)):
        info = fs.compute_checksums(file_path)
        sha256.update(f"{file_path}:{info.checksum_sha256}".encode("utf-8"))
        md5.update(f"{file_path}:{info.checksum_md5}".encode("utf-8"))
        total_bytes += info.size
        count += 1
    return {
        "count": count,
        "bytes": total_bytes,
        "checksum": {"sha256": sha256.hexdigest(), "md5": md5.hexdigest()},
    }


def write_manifest(
    *,
    fs: Filesystem,
    root: str,
    owner: str,
    tool_version: str,
    upstream: List[str],
    transform: str,
    device_id: Optional[str],
    recording_id: Optional[str],
    partition_dt: Optional[str],
    logger: logging.Logger,
) -> None:
    step_name = "write_manifest"
    if is_done(fs, root, step_name):
        logger.info("Manifest already generated", extra={"root": root})
        return

    done_dir = join_uri(root, DONE_DIRNAME)
    summaries: List[Dict] = []
    for step in [
        "extract_rgb",
        "extract_et",
        "extract_audio",
        "extract_imu",
        "extract_gps",
        "extract_wifi",
        "extract_bt",
        "merge_events",
    ]:
        marker_path = join_uri(done_dir, f"{step}.done")
        summary = _read_summary(fs, marker_path)
        if summary:
            summary["_step"] = step
            summaries.append(summary)

    if not summaries:
        logger.warning("No extraction summaries found; manifest not written", extra={"root": root})
        return

    jsonl_summaries = [s for s in summaries if "jsonl" in s]
    if jsonl_summaries:
        first_jsonl = jsonl_summaries[0]["jsonl"]
        sensors_dir = first_jsonl.rsplit("/", 1)[0]
    else:
        sensors_dir = join_uri(root, "sensors")

    manifest_dir = join_uri(root, "manifest")
    manifest_path = join_uri(manifest_dir, "manifest.json")
    ensure_directory(fs, manifest_dir)

    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

    if not recording_id:
        parsed = urlparse(root)
        if parsed.scheme:
            parts = [p for p in parsed.path.split("/") if p]
        else:
            parts = Path(root).parts
        recording_id = parts[-1] if parts else "unknown_recording"
    if not device_id:
        device_id = "unknown_device"

    ts_candidates = [s.get("ts_first") for s in summaries if s.get("ts_first") is not None]
    auto_dt = derive_partition_dt(int(min(ts_candidates)) if ts_candidates else None)
    partition_keys = {
        "dt": partition_dt or auto_dt or "unknown",
        "device_id": device_id,
        "recording_id": recording_id,
    }

    file_entries: List[Dict[str, object]] = []

    def add_jsonl_entry(summary: Dict) -> None:
        jsonl_uri = summary.get("jsonl")
        if not jsonl_uri or not fs.exists(jsonl_uri):
            return
        info = fs.compute_checksums(jsonl_uri)
        entry = {
            "logical_path": _logical_path(root, jsonl_uri),
            "physical_uri": jsonl_uri,
            "stream_type": summary.get("sensor", "events"),
            "bytes": info.size,
            "count": summary.get("count", 0),
            "checksum": {
                "sha256": info.checksum_sha256,
                "md5": info.checksum_md5,
            },
            "ts_range_ns": {
                "start": summary.get("ts_first"),
                "end": summary.get("ts_last"),
            },
            "tool_version": tool_version,
            "source": "extracted_from_vrs",
            "notes": "",
        }
        file_entries.append(entry)

    def add_artifact_entry(summary: Dict) -> None:
        artifacts = summary.get("artifacts") or []
        for artifact in artifacts:
            uri = artifact.get("uri")
            if not uri:
                continue
            if not fs.exists(uri):
                continue
            metrics = _collect_directory_checksums(fs, uri)
            entry = {
                "logical_path": _logical_path(root, uri) + "/",
                "physical_uri": uri,
                "stream_type": summary.get("sensor"),
                "bytes": metrics["bytes"],
                "count": metrics["count"],
                "checksum": metrics["checksum"],
                "ts_range_ns": {
                    "start": summary.get("ts_first"),
                    "end": summary.get("ts_last"),
                },
                "tool_version": tool_version,
                "source": "extracted_from_vrs",
                "notes": "",
            }
            file_entries.append(entry)

    for summary in summaries:
        add_jsonl_entry(summary)
        add_artifact_entry(summary)

    manifest = {
        "schema_version": "1.0.0",
        "created_utc": now,
        "session": {
            "project": "Context2Text",
            "recording_id": recording_id,
            "device_id": device_id,
            "session_id": recording_id,
        },
        "files": file_entries,
        "partition_keys": partition_keys,
        "lineage": {
            "upstream": upstream,
            "transform": transform,
            "owner": owner,
        },
    }

    with LogTimer(
        logger,
        "Manifest written",
        step=step_name,
        device_id=device_id,
        recording_id=recording_id,
        extra={"manifest": manifest_path},
    ):
        with fs.open(manifest_path, "wt") as handle:
            json.dump(manifest, handle, indent=2, ensure_ascii=False)
            handle.write("\n")

    mark_done(
        fs,
        root,
        step_name,
        json.dumps({"manifest": manifest_path, "files": len(file_entries)}, ensure_ascii=False),
    )
