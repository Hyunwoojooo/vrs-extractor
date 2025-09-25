"""Idempotency markers for extraction steps."""

from __future__ import annotations

from pathlib import Path

from .constants import DONE_DIRNAME
from .io import Filesystem, join_uri, is_remote


def step_done_path(root: str | Path, step_name: str) -> str:
    base = str(root)
    if is_remote(base):
        return join_uri(base, DONE_DIRNAME, f"{step_name}.done")
    return str(Path(base) / DONE_DIRNAME / f"{step_name}.done")


def mark_done(fs: Filesystem, root: str | Path, step_name: str, payload: str | None = None) -> None:
    marker = step_done_path(root, step_name)
    done_dir = marker.rsplit("/", 1)[0]
    fs.makedirs(done_dir)
    with fs.open(marker, "wt") as handle:
        handle.write(payload or "ok")


def is_done(fs: Filesystem, root: str | Path, step_name: str) -> bool:
    marker = step_done_path(root, step_name)
    return fs.exists(marker)


def clear_done(fs: Filesystem, root: str | Path, step_name: str) -> None:
    marker = step_done_path(root, step_name)
    if fs.exists(marker):
        fs.remove(marker)
