"""Audio extraction implementation."""

from __future__ import annotations

import json
import logging
import wave
from typing import Dict, List

import numpy as np

from _core_pybinds.sensor_data import TimeDomain

from ..config import ExtractorConfig
from ..constants import SENSOR_FILES
from ..io import Filesystem, ensure_directory, join_uri
from ..logger import LogTimer
from ..paths import OutputLayout
from ..provider import AriaVrsProvider
from ..quality import QualityFlagger
from ..status import clear_done, is_done, mark_done


def _write_wav(
    fs: Filesystem,
    uri: str,
    *,
    sample_rate: int,
    num_channels: int,
    payload: bytes,
) -> int:
    parent = uri.rsplit("/", 1)[0] if "/" in uri else ""
    if parent:
        ensure_directory(fs, parent)
    with fs.open(uri, "wb") as handle:
        with wave.open(handle, "wb") as wav_file:
            wav_file.setnchannels(num_channels)
            wav_file.setsampwidth(4)  # 32-bit samples
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(payload)
    return len(payload)


def extract_audio(
    *,
    fs: Filesystem,
    config: ExtractorConfig,
    layout: OutputLayout,
    quality_flagger: QualityFlagger,
    vrs_path: str,
    force: bool,
    logger: logging.Logger,
) -> None:
    if not config.audio.export:
        logger.info(
            "Audio export disabled by configuration",
            extra={"step": "extract_audio", "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    step_name = "extract_audio"
    if not force and is_done(fs, layout.root, step_name):
        logger.info(
            "Audio extraction skipped (already complete)",
            extra={"step": step_name, "device_id": config.device_id, "recording_id": config.recording_id},
        )
        return

    if force:
        clear_done(fs, layout.root, step_name)

    ensure_directory(fs, layout.audio_dir)
    ensure_directory(fs, layout.sensors_dir)

    provider = AriaVrsProvider(vrs_path)
    audio_stream = provider.resolve_audio_stream()
    config_record = provider.provider.get_audio_configuration(audio_stream.stream_id)
    sample_rate = int(getattr(config_record, "sample_rate", 48000))
    num_channels = int(getattr(config_record, "num_channels", 7))

    jsonl_path = layout.sensor_file(SENSOR_FILES["audio"])

    summary: Dict[str, object] = {
        "sensor": "mic",
        "jsonl": jsonl_path,
        "count": 0,
        "bytes": 0,
        "ts_first": None,
        "ts_last": None,
        "artifacts": [
            {
                "kind": "audio_dir",
                "uri": layout.audio_dir,
                "stream_id": audio_stream.numeric_name,
                "count": 0,
                "bytes": 0,
            }
        ],
    }
    artifact = summary["artifacts"][0]  # type: ignore[index]

    enabled_flags = {flag for flag in quality_flagger.enabled_flags}

    with LogTimer(
        logger,
        "Audio extraction completed",
        step=step_name,
        device_id=config.device_id,
        recording_id=config.recording_id,
        extra={"stream": audio_stream.numeric_name, "jsonl": jsonl_path},
    ):
        with fs.open(jsonl_path, "wt") as jsonl_file:
            iterator = provider.deliver_stream([audio_stream.stream_id])
            chunk_index = 0
            for data in iterator:
                audio_data, record = data.audio_data_and_record()
                if not audio_data.data:
                    continue

                payload_array = np.array(audio_data.data, dtype=np.int32)
                try:
                    sample_count = len(payload_array) // num_channels
                except ZeroDivisionError:  # pragma: no cover - defensive
                    continue

                chunk_samples = sample_count
                expected_samples = config.audio.chunk_samples
                if expected_samples and chunk_samples != expected_samples:
                    logger.warning(
                        "Audio chunk sample count %s differs from requested %s",
                        chunk_samples,
                        expected_samples,
                    )

                chunk_bytes = _write_wav(
                    fs,
                    join_uri(layout.audio_dir, f"chunk_{chunk_index:06d}.wav"),
                    sample_rate=sample_rate,
                    num_channels=num_channels,
                    payload=payload_array.tobytes(),
                )
                clip_uri = join_uri(layout.audio_dir, f"chunk_{chunk_index:06d}.wav")
                chunk_index += 1

                summary["bytes"] = int(summary["bytes"]) + chunk_bytes  # type: ignore[arg-type]
                summary["count"] = int(summary["count"]) + 1  # type: ignore[arg-type]
                artifact["bytes"] = int(artifact["bytes"]) + chunk_bytes
                artifact["count"] = int(artifact["count"]) + 1

                device_ts = int(data.get_time_ns(TimeDomain.DEVICE_TIME))
                summary["ts_first"] = device_ts if summary["ts_first"] is None else min(summary["ts_first"], device_ts)  # type: ignore[arg-type]
                summary["ts_last"] = device_ts if summary["ts_last"] is None else max(summary["ts_last"], device_ts)  # type: ignore[arg-type]

                duration_ns = int(chunk_samples / sample_rate * 1_000_000_000)
                duration_ms = duration_ns / 1_000_000

                clipping = bool(np.max(np.abs(payload_array)) >= np.iinfo(np.int32).max)

                payload = {
                    "ts_ns": device_ts,
                    "sensor": "mic",
                    "clip_uri": clip_uri,
                    "duration_ms": duration_ms,
                    "channels": num_channels,
                    "chunk_samples": chunk_samples,
                    "stream_id": audio_stream.numeric_name,
                }
                flags: List[str] = quality_flagger.evaluate(payload)
                if clipping and "audio_clipping" in enabled_flags and "audio_clipping" not in flags:
                    flags.append("audio_clipping")
                payload["quality_flags"] = flags
                jsonl_file.write(json.dumps(payload, ensure_ascii=False) + "\n")

    mark_done(fs, layout.root, step_name, json.dumps(summary, ensure_ascii=False))
