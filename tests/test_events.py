import json
from pathlib import Path

from aria_vrs_extractor.logger import get_logger
from aria_vrs_extractor.operations.events import merge_events
from aria_vrs_extractor.io import Filesystem, join_uri
from aria_vrs_extractor.constants import DONE_DIRNAME, SENSOR_FILES, EVENTS_FILE


def write_summary(path: Path, summary: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary), encoding="utf-8")


def test_merge_events_sorted(tmp_path: Path) -> None:
    root = tmp_path
    sensors_dir = root / "sensors"
    sensors_dir.mkdir()

    rgb_jsonl = sensors_dir / SENSOR_FILES["rgb"]
    rgb_jsonl.write_text(
        "\n".join(
            [
                json.dumps({"ts_ns": 2, "sensor": "rgb", "value": 1}),
                json.dumps({"ts_ns": 5, "sensor": "rgb", "value": 2}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    imu_jsonl = sensors_dir / SENSOR_FILES["imu"]
    imu_jsonl.write_text(
        "\n".join(
            [
                json.dumps({"ts_ns": 1, "sensor": "imu", "value": 10}),
                json.dumps({"ts_ns": 3, "sensor": "imu", "value": 11}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    status_dir = root / DONE_DIRNAME
    write_summary(
        status_dir / "extract_rgb.done",
        {
            "sensor": "rgb",
            "jsonl": str(rgb_jsonl),
            "count": 2,
            "ts_first": 2,
            "ts_last": 5,
        },
    )
    write_summary(
        status_dir / "extract_imu.done",
        {
            "sensor": "imu",
            "jsonl": str(imu_jsonl),
            "count": 2,
            "ts_first": 1,
            "ts_last": 3,
        },
    )

    fs = Filesystem()
    merge_events(
        fs=fs,
        root=str(root),
        force=True,
        logger=get_logger("test"),
    )

    events_path = sensors_dir / EVENTS_FILE
    lines = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    timestamps = [row["ts_ns"] for row in lines]
    assert timestamps == [1, 2, 3, 5]
