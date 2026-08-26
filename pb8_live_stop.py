"""Stop only PB8 live processes managed by one PBGui installation."""

from __future__ import annotations

import argparse
import platform
import sys
from pathlib import Path
from typing import Any


def _managed_run_root(pbgdir: Path) -> Path:
    """Return a non-symlinked PB8 run root below the supplied PBGui directory."""
    base = pbgdir.expanduser().absolute()
    data_dir = base / "data"
    run_root = data_dir / "run_v8"
    for path in (base, data_dir, run_root):
        if path.is_symlink():
            raise RuntimeError(f"PB8 live process root must not contain symlinks: {path}")
    return run_root


def stop_managed_pb8_live_processes(
    pbgdir: Path,
    pb8dir: Path,
    pb8_python: Path,
    *,
    run_v8_class: type[Any] | None = None,
) -> list[int]:
    """Stop exact PBRun-style live commands without touching PB8 batch jobs."""
    pbgdir = pbgdir.expanduser().absolute()
    pb8dir = pb8dir.expanduser().absolute()
    pb8_python = pb8_python.expanduser().absolute()
    run_root = _managed_run_root(pbgdir)
    if not run_root.is_dir():
        return []
    if run_v8_class is None:
        sys.path.insert(0, str(pbgdir))
        from PBRun import RunV8

        run_v8_class = RunV8

    stopped: list[int] = []
    for instance_dir in sorted(run_root.iterdir(), key=lambda path: path.name):
        config_path = instance_dir / "config.json"
        if (
            instance_dir.is_symlink()
            or not instance_dir.is_dir()
            or config_path.is_symlink()
            or not config_path.is_file()
        ):
            continue
        runner = run_v8_class()
        runner.path = str(instance_dir)
        runner.user = instance_dir.name
        runner.name = platform.node()
        runner.pb8dir = str(pb8dir)
        runner.pb8venv = str(pb8_python)
        runner.pbgdir = str(pbgdir)
        process = runner.pid()
        if process is None:
            continue
        stopped.append(int(process.pid))
        runner.stop(process)
    return stopped


def main(argv: list[str] | None = None) -> int:
    """Run the exact PB8 live stop helper for installer playbooks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pbgdir", type=Path)
    parser.add_argument("pb8dir", type=Path)
    parser.add_argument("pb8_python", type=Path)
    args = parser.parse_args(argv)
    stopped = stop_managed_pb8_live_processes(args.pbgdir, args.pb8dir, args.pb8_python)
    sys.stdout.write(f"stopped_processes={len(stopped)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
