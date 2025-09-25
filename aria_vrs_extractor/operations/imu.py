"""IMU extraction implementation."""

from __future__ import annotations

import json
import logging
from typing import Dict

from _core_pybinds.sensor_data import TimeDomain

from ..config import ExtractorConfig
from ..constants import SENSOR_FILES
from ..io import Filesystem
from ..logger import LogTimer
from ..paths import OutputLayout
from ..provider import AriaVrsProvider
from ..quality import QualityFlagger
from ..status import clear_done, is_done, mark_done


def extract_imu(
    *,
    fs: Filesystem,
    config: ExtractorConfig,
    layout: OutputLayout,
    quality_flagger: QualityFlagger,
    vrs_path: str,
    force: bool,
    logger: logging.Logger,
) -> None:
    if not config.imu.export:
        logger.info(
            "IMU export disabled by configuration",
            extra={"step": "extract_imu", "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    step_name = "extract_imu"
    if not force and is_done(fs, layout.root, step_name):
        logger.info(
            "IMU extraction skipped (already complete)",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    if force:
        clear_done(fs, layout.root, step_name)

    provider = AriaVrsProvider(vrs_path)
    imu_streams = provider.resolve_imu_streams()

    jsonl_path = layout.sensor_file(SENSOR_FILES["imu"])

    summary: Dict[str, object] = {
        "sensor": "imu",
        "jsonl": jsonl_path,
        "count": 0,
        "bytes": 0,
        "ts_first": None,
        "ts_last": None,
        "streams": [info.numeric_name for info in imu_streams],
    }

    with LogTimer(
        logger,
        "IMU extraction completed",
        step=step_name,
        device_id=config.device_id,
        recording_id=config.recording_id,
        extra={"streams": summary["streams"], "jsonl": jsonl_path},
    ):
        with fs.open(jsonl_path, "wt") as jsonl_file:
            for info in imu_streams:
                iterator = provider.deliver_stream([info.stream_id])
                for data in iterator:
                    motion = data.imu_data()
                    ts_ns = int(data.get_time_ns(TimeDomain.DEVICE_TIME))
                    summary["ts_first"] = ts_ns if summary["ts_first"] is None else min(summary["ts_first"], ts_ns)  # type: ignore[arg-type]
                    summary["ts_last"] = ts_ns if summary["ts_last"] is None else max(summary["ts_last"], ts_ns)  # type: ignore[arg-type]

                    payload = {
                        "ts_ns": ts_ns,
                        "sensor": "imu",
                        "stream_id": info.numeric_name,
                        "acc": list(motion.accel_msec2) if getattr(motion, "accel_valid", False) else None,
                        "gyro": list(motion.gyro_radsec) if getattr(motion, "gyro_valid", False) else None,
                        "mag": list(motion.mag_tesla) if getattr(motion, "mag_valid", False) else None,
                    }
                    flags = quality_flagger.evaluate(payload)
                    payload["quality_flags"] = flags

                    record = json.dumps(payload, ensure_ascii=False)
                    jsonl_file.write(record + "\n")
                    summary["count"] = int(summary["count"]) + 1  # type: ignore[arg-type]
                    summary["bytes"] = int(summary["bytes"]) + len(record) + 1  # include newline

    mark_done(fs, layout.root, step_name, json.dumps(summary, ensure_ascii=False))
