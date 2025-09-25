"""Wi-Fi extraction implementation."""

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


def extract_wifi(
    *,
    fs: Filesystem,
    config: ExtractorConfig,
    layout: OutputLayout,
    quality_flagger: QualityFlagger,
    vrs_path: str,
    force: bool,
    logger: logging.Logger,
) -> None:
    if not config.wifi.export:
        logger.info(
            "Wi-Fi export disabled by configuration",
            extra={"step": "extract_wifi", "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    step_name = "extract_wifi"
    if not force and is_done(fs, layout.root, step_name):
        logger.info(
            "Wi-Fi extraction skipped (already complete)",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    if force:
        clear_done(fs, layout.root, step_name)

    provider = AriaVrsProvider(vrs_path)
    wifi_streams = provider.resolve_wifi_streams()
    if not wifi_streams:
        logger.warning(
            "No Wi-Fi streams found",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    jsonl_path = layout.sensor_file(SENSOR_FILES["wifi"])

    summary: Dict[str, object] = {
        "sensor": "wifi",
        "jsonl": jsonl_path,
        "count": 0,
        "bytes": 0,
        "ts_first": None,
        "ts_last": None,
        "streams": [info.numeric_name for info in wifi_streams],
    }

    with LogTimer(
        logger,
        "Wi-Fi extraction completed",
        step=step_name,
        device_id=config.device_id,
        recording_id=config.recording_id,
        extra={"streams": summary["streams"], "jsonl": jsonl_path},
    ):
        with fs.open(jsonl_path, "wt") as jsonl_file:
            for info in wifi_streams:
                iterator = provider.deliver_stream([info.stream_id])
                for data in iterator:
                    wifi = data.wps_data()
                    ts_ns = int(data.get_time_ns(TimeDomain.DEVICE_TIME))
                    summary["ts_first"] = ts_ns if summary["ts_first"] is None else min(summary["ts_first"], ts_ns)  # type: ignore[arg-type]
                    summary["ts_last"] = ts_ns if summary["ts_last"] is None else max(summary["ts_last"], ts_ns)  # type: ignore[arg-type]

                    payload = {
                        "ts_ns": ts_ns,
                        "sensor": "wifi",
                        "stream_id": info.numeric_name,
                        "ap_mac": getattr(wifi, "bssid_mac", None),
                        "ssid": getattr(wifi, "ssid", None),
                        "rssi": getattr(wifi, "rssi", None),
                        "freq_mhz": getattr(wifi, "freq_mhz", None),
                    }
                    payload["quality_flags"] = quality_flagger.evaluate(payload)

                    record = json.dumps(payload, ensure_ascii=False)
                    jsonl_file.write(record + "\n")
                    summary["count"] = int(summary["count"]) + 1  # type: ignore[arg-type]
                    summary["bytes"] = int(summary["bytes"]) + len(record) + 1

    mark_done(fs, layout.root, step_name, json.dumps(summary, ensure_ascii=False))
