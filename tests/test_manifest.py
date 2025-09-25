import json
import logging
from pathlib import Path

from aria_vrs_extractor.constants import DONE_DIRNAME
from aria_vrs_extractor.io import Filesystem
from aria_vrs_extractor.operations.manifest import write_manifest


def write_summary(path: Path, summary: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary), encoding="utf-8")


def create_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_write_manifest(tmp_path: Path) -> None:
    root = tmp_path
    sensors_dir = root / "sensors"
    rgb_dir = root / "rgb" / "frames"
    audio_dir = root / "audio"

    rgb_jsonl = sensors_dir / "rgb.jsonl"
    rgb_jsonl.parent.mkdir(parents=True, exist_ok=True)
    rgb_jsonl.write_text(
        json.dumps({"ts_ns": 1, "sensor": "rgb", "uri": "rgb"}) + "\n",
        encoding="utf-8",
    )
    create_file(rgb_dir / "frame_000001.jpg", b"rgbdata")

    audio_jsonl = sensors_dir / "mic.jsonl"
    audio_jsonl.write_text(
        json.dumps({"ts_ns": 2, "sensor": "mic", "clip_uri": "audio"}) + "\n",
        encoding="utf-8",
    )
    create_file(audio_dir / "chunk_000001.wav", b"audiodata")

    status_dir = root / DONE_DIRNAME
    write_summary(
        status_dir / "extract_rgb.done",
        {
            "sensor": "rgb",
            "jsonl": str(rgb_jsonl),
            "ts_first": 1,
            "ts_last": 1,
            "count": 1,
            "artifacts": [
                {
                    "kind": "image_dir",
                    "uri": str(rgb_dir),
                    "stream_id": "rgb_stream",
                    "count": 1,
                    "bytes": len(b"rgbdata"),
                }
            ],
        },
    )
    write_summary(
        status_dir / "extract_audio.done",
        {
            "sensor": "mic",
            "jsonl": str(audio_jsonl),
            "ts_first": 2,
            "ts_last": 2,
            "count": 1,
            "artifacts": [
                {
                    "kind": "audio_dir",
                    "uri": str(audio_dir),
                    "stream_id": "mic_stream",
                    "count": 1,
                    "bytes": len(b"audiodata"),
                }
            ],
        },
    )

    fs = Filesystem()
    write_manifest(
        fs=fs,
        root=str(root),
        owner="tester",
        tool_version="unit-test",
        upstream=["vrs://example"],
        transform="extract",
        device_id="devA",
        recording_id="rec01",
        partition_dt="2024/01/01",
        logger=logging.getLogger("manifest-test"),
    )

    manifest_path = root / "manifest" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["session"]["recording_id"] == "rec01"
    assert manifest["partition_keys"]["dt"] == "2024/01/01"
    assert any(entry["stream_type"] == "rgb" for entry in manifest["files"])
