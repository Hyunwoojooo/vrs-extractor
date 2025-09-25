"""Shared constants for aria_vrs_extractor."""

SENSOR_FILES = {
    "rgb": "rgb.jsonl",
    "et": "et.jsonl",
    "audio": "mic.jsonl",
    "imu": "imu.jsonl",
    "gps": "gps.jsonl",
    "wifi": "wifi.jsonl",
    "bt": "bt.jsonl",
}

EVENTS_FILE = "events.jsonl"

DEFAULT_QUALITY_FLAGS = ["blur", "drop_frame", "audio_clipping"]

DONE_DIRNAME = "_status"

__all__ = ["SENSOR_FILES", "EVENTS_FILE", "DEFAULT_QUALITY_FLAGS", "DONE_DIRNAME"]
