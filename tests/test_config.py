import json
from pathlib import Path

import pytest

from aria_vrs_extractor.config import ExtractorConfig


def test_config_from_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
        device_id: devA
        recording_id: rec01
        output_root: /tmp/out
        rgb:
          export: true
          downscale: [640, 480]
        audio:
          chunk_samples: 2048
        """
    )

    cfg = ExtractorConfig.from_yaml(config_path)
    assert cfg.device_id == "devA"
    assert cfg.recording_id == "rec01"
    assert cfg.output_root == "/tmp/out"
    assert cfg.rgb.downscale == (640, 480)
    assert cfg.audio.chunk_samples == 2048
    assert cfg.quality_flags.enabled == ["blur", "drop_frame", "audio_clipping"]


@pytest.mark.parametrize(
    "downscale",
    [None, [320, 240]],
)
def test_override_downscale(downscale):
    cfg = ExtractorConfig.from_dict(
        {
            "device_id": "dev",
            "recording_id": "rec",
            "output_root": "/out",
            "et": {"downscale": downscale},
        }
    )
    if downscale:
        assert cfg.et.downscale == (320, 240)
    else:
        assert cfg.et.downscale is None
