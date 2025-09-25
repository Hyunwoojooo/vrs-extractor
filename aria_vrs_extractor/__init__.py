"""Aria VRS Extractor package."""

from importlib.metadata import version as _pkg_version, PackageNotFoundError

try:
    __version__ = _pkg_version("aria-vrs-extractor")
except PackageNotFoundError:  # pragma: no cover - local editable installs
    __version__ = "0.1.0"

__all__ = ["__version__"]
