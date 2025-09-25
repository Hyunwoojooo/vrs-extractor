"""RGB extraction implementation."""

from __future__ import annotations

import io
import json
import logging
from typing import Dict, Optional

import numpy as np
from PIL import Image

from ..config import ExtractorConfig
from ..constants import SENSOR_FILES
from ..io import Filesystem, ensure_directory, join_uri
from ..logger import LogTimer
from ..paths import OutputLayout
from ..provider import AriaVrsProvider
from ..quality import QualityFlagger
from ..status import clear_done, is_done, mark_done


def _save_jpeg(
    fs: Filesystem,
    image_array: np.ndarray,
    path: str,
    *,
    quality: int = 95,
) -> int:
    image = Image.fromarray(image_array)
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=quality)
    payload = buffer.getvalue()
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    if parent:
        ensure_directory(fs, parent)
    with fs.open(path, "wb") as handle:
        handle.write(payload)
    return len(payload)


def extract_rgb(
    *,
    fs: Filesystem,
    config: ExtractorConfig,
    layout: OutputLayout,
    quality_flagger: QualityFlagger,
    vrs_path: str,
    force: bool,
    logger: logging.Logger,
) -> None:
    if not config.rgb.export:
        logger.info(
            "RGB export disabled by configuration",
            extra={"step": "extract_rgb", "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    step_name = "extract_rgb"
    if not force and is_done(fs, layout.root, step_name):
        logger.info(
            "RGB extraction skipped (already complete)",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    if force:
        clear_done(fs, layout.root, step_name)

    ensure_directory(fs, layout.rgb_dir)
    ensure_directory(fs, layout.sensors_dir)

    provider = AriaVrsProvider(vrs_path)
    rgb_stream = provider.resolve_rgb_stream()

    jsonl_path = layout.sensor_file(SENSOR_FILES["rgb"])

    summary: Dict[str, object] = {
        "sensor": "rgb",
        "jsonl": jsonl_path,
        "count": 0,
        "bytes": 0,
        "ts_first": None,
        "ts_last": None,
        "artifacts": [
            {
                "kind": "image_dir",
                "uri": layout.rgb_dir,
                "stream_id": rgb_stream.numeric_name,
                "count": 0,
                "bytes": 0,
            }
        ],
    }

    timer_extra = {
        "stream": rgb_stream.numeric_name,
        "jsonl": jsonl_path,
    }

    artifact = summary["artifacts"][0]  # type: ignore[index]

    with LogTimer(
        logger,
        "RGB extraction completed",
        step=step_name,
        device_id=config.device_id,
        recording_id=config.recording_id,
        extra=timer_extra,
    ):
        with fs.open(jsonl_path, "wt") as jsonl_file:
            iterator = provider.deliver_stream([rgb_stream.stream_id])
            for data in iterator:
                image, record = data.image_data_and_record()
                if not image.is_valid():
                    continue

                frame_array = image.to_numpy_array()
                if config.rgb.downscale:
                    width, height = config.rgb.downscale
                    pil_image = Image.fromarray(frame_array)
                    frame_array = np.array(pil_image.resize((width, height), resample=Image.BILINEAR))
                height_px, width_px = frame_array.shape[:2]

                frame_id: int
                record_frame = getattr(record, "frame_number", None)
                if record_frame is None:
                    frame_id = int(summary["count"])  # type: ignore[arg-type]
                else:
                    frame_id = int(record_frame)
                filename = f"frame_{frame_id:06d}.jpg"
                frame_uri = join_uri(layout.rgb_dir, filename)

                bytes_written = _save_jpeg(fs, frame_array, frame_uri)
                summary["bytes"] = int(summary["bytes"]) + bytes_written  # type: ignore[arg-type]
                summary["count"] = int(summary["count"]) + 1  # type: ignore[arg-type]
                artifact["bytes"] = int(artifact["bytes"]) + bytes_written
                artifact["count"] = int(artifact["count"]) + 1

                ts_ns = int(getattr(record, "capture_timestamp_ns"))
                summary["ts_first"] = ts_ns if summary["ts_first"] is None else min(summary["ts_first"], ts_ns)  # type: ignore[arg-type]
                summary["ts_last"] = ts_ns if summary["ts_last"] is None else max(summary["ts_last"], ts_ns)  # type: ignore[arg-type]

                payload = {
                    "ts_ns": ts_ns,
                    "sensor": "rgb",
                    "frame_id": frame_id,
                    "uri": frame_uri,
                    "width": int(width_px),
                    "height": int(height_px),
                    "stream_id": rgb_stream.numeric_name,
                }
                flags = quality_flagger.evaluate(payload)
                payload["quality_flags"] = flags
                jsonl_file.write(json.dumps(payload, ensure_ascii=False) + "\n")

    mark_done(fs, layout.root, step_name, json.dumps(summary, ensure_ascii=False))
