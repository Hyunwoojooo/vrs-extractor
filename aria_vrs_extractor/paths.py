"""Helpers for computing output layout paths."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .config import ExtractorConfig
from .io import join_uri, is_remote


@dataclass(slots=True)
class OutputLayout:
    root: str
    sensors_dir: str
    rgb_dir: str
    et_left_dir: str
    et_right_dir: str
    audio_dir: str
    manifest_dir: str

    @classmethod
    def from_config(cls, config: ExtractorConfig) -> "OutputLayout":
        root = config.output_root.rstrip("/")
        sensors_dir = join_uri(root, config.paths.sensors_dir)
        return cls(
            root=root,
            sensors_dir=sensors_dir,
            rgb_dir=join_uri(root, config.paths.rgb_frames),
            et_left_dir=join_uri(root, config.paths.et_left),
            et_right_dir=join_uri(root, config.paths.et_right),
            audio_dir=join_uri(root, config.paths.audio_chunks),
            manifest_dir=join_uri(root, config.paths.manifest_dir),
        )

    def sensor_file(self, filename: str) -> str:
        return join_uri(self.sensors_dir, filename)

    def manifest_file(self, filename: str) -> str:
        return join_uri(self.manifest_dir, filename)

    def local_root_path(self) -> Path:
        if is_remote(self.root):
            raise ValueError("Local root requested for remote output layout")
        return Path(self.root)


__all__ = ["OutputLayout"]
