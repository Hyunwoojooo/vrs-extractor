"""Filesystem helpers supporting local paths and S3-style URIs."""

from __future__ import annotations

import hashlib
import io
import os
import posixpath
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Tuple
from urllib.parse import urlparse

try:  # pragma: no cover - optional dependency
    import fsspec
except ModuleNotFoundError:  # pragma: no cover - fallback
    fsspec = None  # type: ignore


def is_remote(path: str | os.PathLike[str]) -> bool:
    parsed = urlparse(str(path))
    return bool(parsed.scheme and parsed.scheme != "file")


def join_uri(base: str | os.PathLike[str], *parts: str | os.PathLike[str]) -> str:
    base_str = str(base)
    new_parts = [str(part).strip("/") for part in parts if str(part)]
    if is_remote(base_str):
        prefix = base_str.rstrip("/")
        joined = "/".join(new_parts)
        if not joined:
            return prefix
        return f"{prefix}/{joined}"
    return str(Path(base_str).joinpath(*new_parts))


@dataclass(slots=True)
class FileInfo:
    path: str
    size: int
    checksum_sha256: str
    checksum_md5: str


class Filesystem:
    """Thin wrapper over local filesystem and optional fsspec backends."""

    def __init__(self) -> None:
        self._fs_cache: Dict[str, "fsspec.AbstractFileSystem"] = {}

    def _get_remote_fs(self, path: str):
        if not fsspec:  # pragma: no cover - fallback when dependency missing
            raise RuntimeError("fsspec is required for remote paths but is not installed")
        parsed = urlparse(path)
        protocol = parsed.scheme
        if protocol in self._fs_cache:
            return self._fs_cache[protocol]
        fs = fsspec.filesystem(protocol)
        self._fs_cache[protocol] = fs
        return fs

    def exists(self, path: str | Path) -> bool:
        path_str = str(path)
        if is_remote(path_str):
            fs = self._get_remote_fs(path_str)
            return bool(fs.exists(path_str))
        return Path(path_str).exists()

    def makedirs(self, path: str | Path) -> None:
        path_str = str(path)
        if is_remote(path_str):
            fs = self._get_remote_fs(path_str)
            if hasattr(fs, "makedirs"):
                fs.makedirs(path_str, exist_ok=True)
            else:  # pragma: no cover - older fsspec fallback
                fs.mkdir(path_str, exist_ok=True)
        else:
            Path(path_str).mkdir(parents=True, exist_ok=True)

    @contextmanager
    def open(self, path: str | Path, mode: str = "rb") -> Iterator[io.IOBase]:
        path_str = str(path)
        if "b" not in mode and "t" not in mode:
            # default to text mode with utf-8 decoding when not specified explicitly
            mode = f"{mode}t"
        if is_remote(path_str):
            fs = self._get_remote_fs(path_str)
            with fs.open(path_str, mode) as handle:
                yield handle
        else:
            with open(path_str, mode, encoding=None if "b" in mode else "utf-8") as handle:
                yield handle

    def remove(self, path: str | Path) -> None:
        path_str = str(path)
        if is_remote(path_str):
            fs = self._get_remote_fs(path_str)
            fs.rm(path_str, recursive=False)
        else:
            Path(path_str).unlink(missing_ok=True)

    def list_files(self, path: str | Path) -> Iterable[str]:
        path_str = str(path)
        if is_remote(path_str):
            fs = self._get_remote_fs(path_str)
            if not path_str.endswith("/"):
                path_str = f"{path_str}/"
            for item in fs.find(path_str):
                yield item
        else:
            base = Path(path_str)
            if base.is_dir():
                for sub in base.rglob("*"):
                    if sub.is_file():
                        yield str(sub)

    def compute_checksums(self, path: str | Path) -> FileInfo:
        path_str = str(path)
        sha256 = hashlib.sha256()
        md5 = hashlib.md5()
        total = 0
        with self.open(path_str, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                total += len(chunk)
                sha256.update(chunk)
                md5.update(chunk)
        return FileInfo(
            path=path_str,
            size=total,
            checksum_sha256=sha256.hexdigest(),
            checksum_md5=md5.hexdigest(),
        )

    def walk_local(self, path: str | Path) -> Iterator[Tuple[str, Dict[str, FileInfo]]]:
        """Walk a directory tree yielding file info dictionaries."""
        path_str = str(path)
        if is_remote(path_str):
            fs = self._get_remote_fs(path_str)
            for file_path in fs.find(path_str):
                yield file_path, {file_path: self.compute_checksums(file_path)}
        else:
            for root, _, files in os.walk(path_str):
                results: Dict[str, FileInfo] = {}
                for name in files:
                    file_path = Path(root) / name
                    results[str(file_path)] = self.compute_checksums(file_path)
                yield root, results


def ensure_directory(fs: Filesystem, path: str | Path) -> None:
    if not fs.exists(path):
        fs.makedirs(path)


__all__ = [
    "Filesystem",
    "FileInfo",
    "ensure_directory",
    "is_remote",
    "join_uri",
]
