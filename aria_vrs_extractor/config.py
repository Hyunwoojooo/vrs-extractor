"""Configuration loading for aria_vrs_extractor."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import yaml


@dataclass(slots=True)
class StreamToggle:
    export: bool = True

    @classmethod
    def from_dict(cls, payload: Optional[Dict]) -> "StreamToggle":
        payload = payload or {}
        return cls(export=bool(payload.get("export", True)))


@dataclass(slots=True)
class RgbConfig(StreamToggle):
    downscale: Optional[Tuple[int, int]] = None

    @classmethod
    def from_dict(cls, payload: Optional[Dict]) -> "RgbConfig":
        payload = payload or {}
        downscale_value = payload.get("downscale")
        if downscale_value is not None:
            if isinstance(downscale_value, Sequence) and len(downscale_value) == 2:
                downscale = (int(downscale_value[0]), int(downscale_value[1]))
            else:
                raise ValueError("rgb.downscale must be a sequence of two integers or null")
        else:
            downscale = None
        return cls(export=bool(payload.get("export", True)), downscale=downscale)


@dataclass(slots=True)
class EyeTrackingConfig(StreamToggle):
    left: bool = True
    right: bool = True
    downscale: Optional[Tuple[int, int]] = None

    @classmethod
    def from_dict(cls, payload: Optional[Dict]) -> "EyeTrackingConfig":
        payload = payload or {}
        downsample_value = payload.get("downscale")
        if downsample_value is not None:
            if isinstance(downsample_value, Sequence) and len(downsample_value) == 2:
                downscale = (int(downsample_value[0]), int(downsample_value[1]))
            else:
                raise ValueError("et.downscale must be a sequence of two integers or null")
        else:
            downscale = None
        return cls(
            export=bool(payload.get("export", True)),
            left=bool(payload.get("left", True)),
            right=bool(payload.get("right", True)),
            downscale=downscale,
        )


@dataclass(slots=True)
class AudioConfig(StreamToggle):
    chunk_samples: int = 4096

    @classmethod
    def from_dict(cls, payload: Optional[Dict]) -> "AudioConfig":
        payload = payload or {}
        chunk_samples = int(payload.get("chunk_samples", 4096))
        if chunk_samples not in (2048, 4096):
            raise ValueError("audio.chunk_samples must be either 2048 or 4096")
        return cls(export=bool(payload.get("export", True)), chunk_samples=chunk_samples)


@dataclass(slots=True)
class PathsConfig:
    rgb_frames: str = "rgb/frames"
    et_left: str = "et/left"
    et_right: str = "et/right"
    audio_chunks: str = "audio"
    sensors_dir: str = "sensors"
    manifest_dir: str = "manifest"

    @classmethod
    def from_dict(cls, payload: Optional[Dict]) -> "PathsConfig":
        payload = payload or {}
        kwargs = {}
        for field_name in ("rgb_frames", "et_left", "et_right", "audio_chunks", "sensors_dir", "manifest_dir"):
            if field_name in payload:
                kwargs[field_name] = str(payload[field_name])
        return cls(**kwargs)


@dataclass(slots=True)
class PartitionKeys:
    dt: str = "auto"
    device_id: Optional[str] = None
    recording_id: Optional[str] = None

    @classmethod
    def from_dict(cls, payload: Optional[Dict]) -> "PartitionKeys":
        payload = payload or {}
        return cls(
            dt=str(payload.get("dt", "auto")),
            device_id=payload.get("device_id"),
            recording_id=payload.get("recording_id"),
        )


@dataclass(slots=True)
class QualityFlags:
    enabled: List[str] = field(default_factory=lambda: ["blur", "drop_frame", "audio_clipping"])

    @classmethod
    def from_dict(cls, payload: Optional[Dict]) -> "QualityFlags":
        payload = payload or {}
        enabled = payload.get("enabled")
        if enabled is None:
            return cls()
        if not isinstance(enabled, Iterable):
            raise ValueError("quality_flags.enabled must be an iterable of strings")
        return cls(enabled=[str(flag) for flag in enabled])


@dataclass(slots=True)
class ExtractorConfig:
    device_id: str
    recording_id: str
    output_root: str
    rgb: RgbConfig = field(default_factory=RgbConfig)
    et: EyeTrackingConfig = field(default_factory=EyeTrackingConfig)
    audio: AudioConfig = field(default_factory=AudioConfig)
    imu: StreamToggle = field(default_factory=StreamToggle)
    gps: StreamToggle = field(default_factory=StreamToggle)
    wifi: StreamToggle = field(default_factory=StreamToggle)
    bt: StreamToggle = field(default_factory=StreamToggle)
    paths: PathsConfig = field(default_factory=PathsConfig)
    partition_keys: PartitionKeys = field(default_factory=PartitionKeys)
    quality_flags: QualityFlags = field(default_factory=QualityFlags)

    @classmethod
    def from_dict(cls, data: Dict) -> "ExtractorConfig":
        if not data:
            raise ValueError("config dictionary cannot be empty")
        try:
            device_id = str(data["device_id"])
            recording_id = str(data["recording_id"])
            output_root = str(data["output_root"])
        except KeyError as exc:  # pragma: no cover - defensive programming
            raise KeyError(f"Missing required config key: {exc.args[0]}") from exc

        return cls(
            device_id=device_id,
            recording_id=recording_id,
            output_root=output_root,
            rgb=RgbConfig.from_dict(data.get("rgb")),
            et=EyeTrackingConfig.from_dict(data.get("et")),
            audio=AudioConfig.from_dict(data.get("audio")),
            imu=StreamToggle.from_dict(data.get("imu")),
            gps=StreamToggle.from_dict(data.get("gps")),
            wifi=StreamToggle.from_dict(data.get("wifi")),
            bt=StreamToggle.from_dict(data.get("bt")),
            paths=PathsConfig.from_dict(data.get("paths")),
            partition_keys=PartitionKeys.from_dict(data.get("partition_keys")),
            quality_flags=QualityFlags.from_dict(data.get("quality_flags")),
        )

    @classmethod
    def from_yaml(cls, path: Path | str) -> "ExtractorConfig":
        path = Path(path)
        content = path.read_text(encoding="utf-8")
        data = yaml.safe_load(content)
        if not isinstance(data, dict):
            raise ValueError("YAML configuration must evaluate to a dictionary")
        return cls.from_dict(data)


__all__ = [
    "ExtractorConfig",
    "StreamToggle",
    "RgbConfig",
    "EyeTrackingConfig",
    "AudioConfig",
    "PathsConfig",
    "PartitionKeys",
    "QualityFlags",
]
