"""Typer CLI entrypoint for aria_vrs_extractor."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import typer

from .config import ExtractorConfig
from .constants import DEFAULT_QUALITY_FLAGS
from .io import Filesystem
from .logger import get_logger
from .operations import (
    extract_audio,
    extract_bluetooth,
    extract_et,
    extract_gps,
    extract_imu,
    extract_rgb,
    extract_wifi,
    merge_events,
    write_manifest,
)
from .paths import OutputLayout
from .quality import QualityFlagger

app = typer.Typer(no_args_is_help=True, add_completion=False)
logger = get_logger()


def resolve_config(
    *,
    config_path: Optional[Path],
    device_id: Optional[str],
    recording_id: Optional[str],
    output_root: Optional[str],
) -> ExtractorConfig:
    if config_path:
        config = ExtractorConfig.from_yaml(config_path)
    else:
        if not (device_id and recording_id and output_root):
            raise typer.BadParameter(
                "When --config is not provided you must specify --device-id, --recording-id and --out"
            )
        config = ExtractorConfig(
            device_id=device_id,
            recording_id=recording_id,
            output_root=output_root,
        )
    if device_id:
        config.device_id = device_id
    if recording_id:
        config.recording_id = recording_id
    if output_root:
        config.output_root = output_root
    return config


def build_quality_flagger(config: ExtractorConfig) -> QualityFlagger:
    enabled = config.quality_flags.enabled or DEFAULT_QUALITY_FLAGS
    return QualityFlagger(enabled_flags=enabled)


@app.command("extract-rgb")
def cli_extract_rgb(
    vrs: Path = typer.Option(..., help="Input VRS file"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration YAML path"),
    out: Optional[str] = typer.Option(None, "--out", help="Output root (overrides config)"),
    device_id: Optional[str] = typer.Option(None, "--device-id", help="Device identifier"),
    recording_id: Optional[str] = typer.Option(None, "--recording-id", help="Recording identifier"),
    downscale: Optional[Tuple[int, int]] = typer.Option(None, "--downscale", help="Optional downscale width height"),
    force: bool = typer.Option(False, "--force", help="Re-run even if step is marked done"),
) -> None:
    cfg = resolve_config(
        config_path=config,
        device_id=device_id,
        recording_id=recording_id,
        output_root=out,
    )
    if downscale:
        cfg.rgb.downscale = (int(downscale[0]), int(downscale[1]))
    fs = Filesystem()
    layout = OutputLayout.from_config(cfg)
    flagger = build_quality_flagger(cfg)
    extract_rgb(
        fs=fs,
        config=cfg,
        layout=layout,
        quality_flagger=flagger,
        vrs_path=str(vrs),
        force=force,
        logger=logger,
    )


@app.command("extract-et")
def cli_extract_et(
    vrs: Path = typer.Option(..., help="Input VRS file"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration YAML path"),
    out: Optional[str] = typer.Option(None, "--out", help="Output root"),
    device_id: Optional[str] = typer.Option(None, "--device-id"),
    recording_id: Optional[str] = typer.Option(None, "--recording-id"),
    left: bool = typer.Option(True, "--left/--no-left", help="Export left eye tracking images"),
    right: bool = typer.Option(True, "--right/--no-right", help="Export right eye tracking images"),
    downscale: Optional[Tuple[int, int]] = typer.Option(None, "--downscale"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    cfg = resolve_config(
        config_path=config,
        device_id=device_id,
        recording_id=recording_id,
        output_root=out,
    )
    cfg.et.left = left
    cfg.et.right = right
    if downscale:
        cfg.et.downscale = (int(downscale[0]), int(downscale[1]))
    fs = Filesystem()
    layout = OutputLayout.from_config(cfg)
    flagger = build_quality_flagger(cfg)
    extract_et(
        fs=fs,
        config=cfg,
        layout=layout,
        quality_flagger=flagger,
        vrs_path=str(vrs),
        force=force,
        logger=logger,
    )


@app.command("extract-audio")
def cli_extract_audio(
    vrs: Path = typer.Option(..., help="Input VRS file"),
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    out: Optional[str] = typer.Option(None, "--out"),
    device_id: Optional[str] = typer.Option(None, "--device-id"),
    recording_id: Optional[str] = typer.Option(None, "--recording-id"),
    chunk_samples: Optional[int] = typer.Option(None, "--chunk-samples", help="Target chunk size"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    cfg = resolve_config(
        config_path=config,
        device_id=device_id,
        recording_id=recording_id,
        output_root=out,
    )
    if chunk_samples:
        cfg.audio.chunk_samples = int(chunk_samples)
    fs = Filesystem()
    layout = OutputLayout.from_config(cfg)
    flagger = build_quality_flagger(cfg)
    extract_audio(
        fs=fs,
        config=cfg,
        layout=layout,
        quality_flagger=flagger,
        vrs_path=str(vrs),
        force=force,
        logger=logger,
    )


@app.command("extract-imu")
def cli_extract_imu(
    vrs: Path = typer.Option(...),
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    out: Optional[str] = typer.Option(None, "--out"),
    device_id: Optional[str] = typer.Option(None, "--device-id"),
    recording_id: Optional[str] = typer.Option(None, "--recording-id"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    cfg = resolve_config(
        config_path=config,
        device_id=device_id,
        recording_id=recording_id,
        output_root=out,
    )
    fs = Filesystem()
    layout = OutputLayout.from_config(cfg)
    flagger = build_quality_flagger(cfg)
    extract_imu(
        fs=fs,
        config=cfg,
        layout=layout,
        quality_flagger=flagger,
        vrs_path=str(vrs),
        force=force,
        logger=logger,
    )


@app.command("extract-gps")
def cli_extract_gps(
    vrs: Path = typer.Option(...),
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    out: Optional[str] = typer.Option(None, "--out"),
    device_id: Optional[str] = typer.Option(None, "--device-id"),
    recording_id: Optional[str] = typer.Option(None, "--recording-id"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    cfg = resolve_config(
        config_path=config,
        device_id=device_id,
        recording_id=recording_id,
        output_root=out,
    )
    fs = Filesystem()
    layout = OutputLayout.from_config(cfg)
    flagger = build_quality_flagger(cfg)
    extract_gps(
        fs=fs,
        config=cfg,
        layout=layout,
        quality_flagger=flagger,
        vrs_path=str(vrs),
        force=force,
        logger=logger,
    )


@app.command("extract-wifi")
def cli_extract_wifi(
    vrs: Path = typer.Option(...),
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    out: Optional[str] = typer.Option(None, "--out"),
    device_id: Optional[str] = typer.Option(None, "--device-id"),
    recording_id: Optional[str] = typer.Option(None, "--recording-id"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    cfg = resolve_config(
        config_path=config,
        device_id=device_id,
        recording_id=recording_id,
        output_root=out,
    )
    fs = Filesystem()
    layout = OutputLayout.from_config(cfg)
    flagger = build_quality_flagger(cfg)
    extract_wifi(
        fs=fs,
        config=cfg,
        layout=layout,
        quality_flagger=flagger,
        vrs_path=str(vrs),
        force=force,
        logger=logger,
    )


@app.command("extract-bt")
def cli_extract_bt(
    vrs: Path = typer.Option(...),
    config: Optional[Path] = typer.Option(None, "--config", "-c"),
    out: Optional[str] = typer.Option(None, "--out"),
    device_id: Optional[str] = typer.Option(None, "--device-id"),
    recording_id: Optional[str] = typer.Option(None, "--recording-id"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    cfg = resolve_config(
        config_path=config,
        device_id=device_id,
        recording_id=recording_id,
        output_root=out,
    )
    fs = Filesystem()
    layout = OutputLayout.from_config(cfg)
    flagger = build_quality_flagger(cfg)
    extract_bluetooth(
        fs=fs,
        config=cfg,
        layout=layout,
        quality_flagger=flagger,
        vrs_path=str(vrs),
        force=force,
        logger=logger,
    )


@app.command("merge-events")
def cli_merge_events(
    root: str = typer.Option(..., "--root", help="Extraction root"),
    force: bool = typer.Option(False, "--force"),
) -> None:
    fs = Filesystem()
    merge_events(fs=fs, root=root, force=force, logger=logger)


@app.command("write-manifest")
def cli_write_manifest(
    root: str = typer.Option(..., "--root", help="Extraction root"),
    owner: str = typer.Option(..., "--owner", help="Owner string for lineage"),
    tool_version: str = typer.Option(..., "--tool-version", help="Tool version identifier"),
    upstream: List[str] = typer.Option([], "--upstream", help="Upstream VRS URIs"),
    transform: str = typer.Option("aria_vrs_extractor", "--transform", help="Transform lineage string"),
    device_id: Optional[str] = typer.Option(None, "--device-id", help="Device identifier"),
    recording_id: Optional[str] = typer.Option(None, "--recording-id", help="Recording identifier"),
    partition_dt: Optional[str] = typer.Option(None, "--partition-dt", help="Partition dt override (YYYY/MM/DD)"),
) -> None:
    fs = Filesystem()
    write_manifest(
        fs=fs,
        root=root,
        owner=owner,
        tool_version=tool_version,
        upstream=list(upstream),
        transform=transform,
        device_id=device_id,
        recording_id=recording_id,
        partition_dt=partition_dt,
        logger=logger,
    )


__all__ = ["app"]
