"""Eye-tracking extraction implementation."""

from __future__ import annotations

import io
import json
import logging
from typing import Dict

import numpy as np
from PIL import Image

from ..config import ExtractorConfig
from ..constants import SENSOR_FILES
from ..io import Filesystem, ensure_directory, join_uri
from ..logger import LogTimer
from ..paths import OutputLayout
from ..provider import AriaVrsProvider, StreamInfo
from ..quality import QualityFlagger
from ..status import clear_done, is_done, mark_done


def _save_eye_frame(fs: Filesystem, array: np.ndarray, uri: str) -> int:
    buffer = io.BytesIO()
    Image.fromarray(array).save(buffer, format="JPEG", quality=95)
    payload = buffer.getvalue()
    parent = uri.rsplit("/", 1)[0] if "/" in uri else ""
    if parent:
        ensure_directory(fs, parent)
    with fs.open(uri, "wb") as handle:
        handle.write(payload)
    return len(payload)


def extract_et(
    *,
    fs: Filesystem,
    config: ExtractorConfig,
    layout: OutputLayout,
    quality_flagger: QualityFlagger,
    vrs_path: str,
    force: bool,
    logger: logging.Logger,
) -> None:
    if not config.et.export:
        logger.info(
            "ET export disabled by configuration",
            extra={"step": "extract_et", "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    step_name = "extract_et"
    if not force and is_done(fs, layout.root, step_name):
        logger.info(
            "ET extraction skipped (already complete)",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    if force:
        clear_done(fs, layout.root, step_name)

    provider = AriaVrsProvider(vrs_path)
    discovered = provider.resolve_et_streams()

    selection: Dict[str, StreamInfo] = {}
    if "left" in discovered and config.et.left:
        selection["left"] = discovered["left"]
    if "right" in discovered and config.et.right:
        selection["right"] = discovered["right"]
    if "mono" in discovered and not selection:
        # mono stream delivering both eyes
        selection["mono"] = discovered["mono"]
    if not selection:
        logger.warning(
            "No ET streams selected for export",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    dir_map = {
        "left": layout.et_left_dir,
        "right": layout.et_right_dir,
        "mono": layout.et_left_dir,
    }
    for eye, directory in dir_map.items():
        if eye in selection:
            ensure_directory(fs, directory)
    ensure_directory(fs, layout.sensors_dir)

    jsonl_path = layout.sensor_file(SENSOR_FILES["et"])

    artifacts = []
    for eye, info in selection.items():
        artifacts.append(
            {
                "kind": "image_dir",
                "eye": eye,
                "uri": dir_map[eye],
                "stream_id": info.numeric_name,
                "count": 0,
                "bytes": 0,
            }
        )

    summary: Dict[str, object] = {
        "sensor": "et",
        "jsonl": jsonl_path,
        "count": 0,
        "bytes": 0,
        "ts_first": None,
        "ts_last": None,
        "artifacts": artifacts,
    }

    with LogTimer(
        logger,
        "ET extraction completed",
        step=step_name,
        device_id=config.device_id,
        recording_id=config.recording_id,
        extra={"streams": [info.numeric_name for info in selection.values()], "jsonl": jsonl_path},
    ):
        with fs.open(jsonl_path, "wt") as jsonl_file:
            for eye, info in selection.items():
                output_dir = dir_map[eye]
                artifact = next(item for item in artifacts if item["stream_id"] == info.numeric_name)
                iterator = provider.deliver_stream([info.stream_id])
                for data in iterator:
                    image, record = data.image_data_and_record()
                    if not image.is_valid():
                        continue
                    array = image.to_numpy_array()
                    if config.et.downscale:
                        width, height = config.et.downscale
                        array = np.array(Image.fromarray(array).resize((width, height), resample=Image.BILINEAR))
                    height_px, width_px = array.shape[:2]

                    record_frame = getattr(record, "frame_number", None)
                    frame_id = int(record_frame) if record_frame is not None else int(summary["count"])  # type: ignore[arg-type]
                    filename = f"frame_{frame_id:06d}.jpg"
                    frame_uri = join_uri(output_dir, filename)

                    bytes_written = _save_eye_frame(fs, array, frame_uri)
                    summary["bytes"] = int(summary["bytes"]) + bytes_written  # type: ignore[arg-type]
                    summary["count"] = int(summary["count"]) + 1  # type: ignore[arg-type]
                    artifact["bytes"] = int(artifact["bytes"]) + bytes_written
                    artifact["count"] = int(artifact["count"]) + 1

                    ts_ns = int(getattr(record, "capture_timestamp_ns"))
                    summary["ts_first"] = ts_ns if summary["ts_first"] is None else min(summary["ts_first"], ts_ns)  # type: ignore[arg-type]
                    summary["ts_last"] = ts_ns if summary["ts_last"] is None else max(summary["ts_last"], ts_ns)  # type: ignore[arg-type]

                    payload = {
                        "ts_ns": ts_ns,
                        "sensor": "et",
                        "eye": eye,
                        "frame_id": frame_id,
                        "uri": frame_uri,
                        "width": int(width_px),
                        "height": int(height_px),
                        "stream_id": info.numeric_name,
                        "gaze_vector": getattr(record, "gaze_vector", None),
                        "confidence": getattr(record, "gaze_confidence", None),
                    }
                    payload["quality_flags"] = quality_flagger.evaluate(payload)
                    jsonl_file.write(json.dumps(payload, ensure_ascii=False) + "\n")

    mark_done(fs, layout.root, step_name, json.dumps(summary, ensure_ascii=False))
