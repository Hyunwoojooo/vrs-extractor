"""Minimal JSON logger helpers."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional


class JsonFormatter(logging.Formatter):
    """Serialize log records as JSON."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401 - inherited docstring
        payload = {
            "level": record.levelname,
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "message": record.getMessage(),
        }
        if record.args and isinstance(record.args, dict):
            payload.update(record.args)
        # propagate user supplied extras
        for attr in ("device_id", "recording_id", "step", "counts", "duration_ms"):
            value = getattr(record, attr, None)
            if value is not None:
                payload[attr] = value
        for key, value in getattr(record, "extra_fields", {}).items():
            payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


def get_logger(name: str = "aria_vrs_extractor") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    return logger


class LogTimer:
    """Context manager that measures execution duration and logs on exit."""

    def __init__(
        self,
        logger: logging.Logger,
        message: str,
        *,
        step: Optional[str] = None,
        device_id: Optional[str] = None,
        recording_id: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
        level: int = logging.INFO,
    ) -> None:
        self.logger = logger
        self.message = message
        self.step = step
        self.device_id = device_id
        self.recording_id = recording_id
        self.extra = extra or {}
        self.level = level
        self.start = 0.0

    def __enter__(self) -> "LogTimer":
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: D401 - standard context manager signature
        duration_ms = int((time.perf_counter() - self.start) * 1000)
        msg_extra = {
            "extra_fields": self.extra,
            "step": self.step,
            "device_id": self.device_id,
            "recording_id": self.recording_id,
            "duration_ms": duration_ms,
        }
        if exc:
            self.logger.log(
                logging.ERROR,
                f"{self.message} failed: {exc}",
                extra={**msg_extra, "error": str(exc)},
            )
        else:
            self.logger.log(
                self.level,
                self.message,
                extra=msg_extra,
            )


__all__ = ["get_logger", "LogTimer"]
