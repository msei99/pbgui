"""Lightweight lifecycle helpers for the PBCoinData background service."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Iterable

import psutil


def _arg_matches_path(arg: str, expected_path: Path) -> bool:
    """Return whether one process argument points at the expected runtime config."""

    if not arg:
        return False
    expected = str(expected_path)
    expected_alt = expected.replace("/", "\\")
    value = str(arg)
    return value.endswith(expected) or value.endswith(expected_alt)


def running_dynamic_ignore_instances(
    pbgui_dir: Path,
    *,
    process_iter: Callable[[], Iterable[Any]] | None = None,
) -> list[Path]:
    """Return running V7 instance directories that currently need dynamic-ignore data."""

    run_root = Path(pbgui_dir) / "data" / "run_v7"
    if not run_root.is_dir():
        return []

    candidates: list[tuple[Path, Path]] = []
    for instance_dir in run_root.iterdir():
        if not instance_dir.is_dir():
            continue
        config_file = instance_dir / "config.json"
        config_run = instance_dir / "config_run.json"
        if not config_file.is_file() or not config_run.is_file():
            continue
        try:
            payload = json.loads(config_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        pbgui = payload.get("pbgui") if isinstance(payload, dict) else None
        if not isinstance(pbgui, dict) or pbgui.get("dynamic_ignore") is not True:
            continue
        candidates.append((instance_dir, config_run))

    if not candidates:
        return []

    iterator = process_iter or psutil.process_iter
    matched: set[Path] = set()
    for proc in iterator():
        try:
            cmdline = proc.cmdline()
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError):
            continue
        if not any("main.py" in str(arg) for arg in cmdline):
            continue
        for instance_dir, config_run in candidates:
            if any(_arg_matches_path(str(arg), config_run) for arg in cmdline):
                matched.add(instance_dir)
    return sorted(matched, key=lambda path: str(path))


def has_running_dynamic_ignore_bots(
    pbgui_dir: Path,
    *,
    process_iter: Callable[[], Iterable[Any]] | None = None,
) -> bool:
    """Return whether this node has a running V7 bot that needs PBCoinData."""

    return bool(running_dynamic_ignore_instances(pbgui_dir, process_iter=process_iter))


def should_unload(
    pbgui_dir: Path,
    *,
    role: str,
    process_iter: Callable[[], Iterable[Any]] | None = None,
) -> bool:
    """Return whether an idle slave can fully unload PBCoinData."""

    return (
        str(role or "").strip().lower() == "slave"
        and not has_running_dynamic_ignore_bots(pbgui_dir, process_iter=process_iter)
    )


def service_expected(
    pbgui_dir: Path,
    *,
    role: str,
    credential_active: bool | None,
    process_iter: Callable[[], Iterable[Any]] | None = None,
) -> bool | None:
    """Return whether PBCoinData should stay resident on this node.

    Masters preserve the existing credential-driven behavior because Coin Data and
    Market Data UI workflows may need the daemon even without a live bot. Slaves
    only keep the daemon resident while a running V7 dynamic-ignore bot consumes it.
    """

    if should_unload(pbgui_dir, role=role, process_iter=process_iter):
        return False
    return credential_active
