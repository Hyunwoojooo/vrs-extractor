"""Bluetooth extraction implementation."""

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


def extract_bluetooth(
    *,
    fs: Filesystem,
    config: ExtractorConfig,
    layout: OutputLayout,
    quality_flagger: QualityFlagger,
    vrs_path: str,
    force: bool,
    logger: logging.Logger,
) -> None:
    if not config.bt.export:
        logger.info(
            "Bluetooth export disabled by configuration",
            extra={"step": "extract_bt", "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    step_name = "extract_bt"
    if not force and is_done(fs, layout.root, step_name):
        logger.info(
            "Bluetooth extraction skipped (already complete)",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    if force:
        clear_done(fs, layout.root, step_name)

    provider = AriaVrsProvider(vrs_path)
    bt_streams = provider.resolve_bt_streams()
    if not bt_streams:
        logger.warning(
            "No Bluetooth streams found",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    jsonl_path = layout.sensor_file(SENSOR_FILES["bt"])

    summary: Dict[str, object] = {
        "sensor": "bt",
        "jsonl": jsonl_path,
        "count": 0,
        "bytes": 0,
        "ts_first": None,
        "ts_last": None,
        "streams": [info.numeric_name for info in bt_streams],
    }

    with LogTimer(
        logger,
        "Bluetooth extraction completed",
        step=step_name,
        device_id=config.device_id,
        recording_id=config.recording_id,
        extra={"streams": summary["streams"], "jsonl": jsonl_path},
    ):
        with fs.open(jsonl_path, "wt") as jsonl_file:
            for info in bt_streams:
                iterator = provider.deliver_stream([info.stream_id])
                for data in iterator:
                    bt = data.bluetooth_data()
                    ts_ns = int(data.get_time_ns(TimeDomain.DEVICE_TIME))
                    summary["ts_first"] = ts_ns if summary["ts_first"] is None else min(summary["ts_first"], ts_ns)  # type: ignore[arg-type]
                    summary["ts_last"] = ts_ns if summary["ts_last"] is None else max(summary["ts_last"], ts_ns)  # type: ignore[arg-type]

                    payload = {
                        "ts_ns": ts_ns,
                        "sensor": "bt",
                        "stream_id": info.numeric_name,
                        "beacon_id": getattr(bt, "unique_id", None),
                        "rssi": getattr(bt, "rssi", None),
                        "tx_power": getattr(bt, "tx_power", None),
                        "freq_mhz": getattr(bt, "freq_mhz", None),
                    }
                    payload["quality_flags"] = quality_flagger.evaluate(payload)

                    record = json.dumps(payload, ensure_ascii=False)
                    jsonl_file.write(record + "\n")
                    summary["count"] = int(summary["count"]) + 1  # type: ignore[arg-type]
                    summary["bytes"] = int(summary["bytes"]) + len(record) + 1

    mark_done(fs, layout.root, step_name, json.dumps(summary, ensure_ascii=False))
