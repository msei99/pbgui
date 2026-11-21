"""Stop only PB8 live processes managed by one PBGui installation."""

from __future__ import annotations

import argparse
import platform
import sys
from pathlib import Path
from typing import Any


def _managed_run_root(pbgdir: Path, runtime: str = 'pb8') -> Path:
    """Return a non-symlinked PB8 run root below the supplied PBGui directory."""
    base = pbgdir.expanduser().absolute()
    data_dir = base / "data"
    if runtime not in {'pb8', 'pb9'}:
        raise ValueError('Unsupported live runtime')
    run_root = data_dir / ('run_v9' if runtime == 'pb9' else 'run_v8')
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
    return stop_managed_live_processes(pbgdir, pb8dir, pb8_python, runner_class=run_v8_class)


def stop_managed_live_processes(pbgdir: Path, pb8dir: Path, pb8_python: Path, *,
                                runtime: str = 'pb8', runner_class: type[Any] | None = None) -> list[int]:
    """Share exact live-process ownership checks across separate PB8/PB9 slots."""
    pbgdir = pbgdir.expanduser().absolute()
    pb8dir = pb8dir.expanduser().absolute()
    pb8_python = pb8_python.expanduser().absolute()
    run_root = _managed_run_root(pbgdir, runtime)
    if not run_root.is_dir():
        return []
    if runner_class is None:
        sys.path.insert(0, str(pbgdir))
        from PBRun import RunV8, RunV9
        runner_class = RunV9 if runtime == 'pb9' else RunV8

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
        runner = runner_class()
        runner.path = str(instance_dir)
        runner.user = instance_dir.name
        runner.name = platform.node()
        setattr(runner, runtime + 'dir', str(pb8dir))
        setattr(runner, runtime + 'venv', str(pb8_python))
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
    parser.add_argument('--runtime', choices=['pb8', 'pb9'], default='pb8')
    args = parser.parse_args(argv)
    stopped = stop_managed_live_processes(args.pbgdir, args.pb8dir, args.pb8_python, runtime=args.runtime)
    sys.stdout.write(f"stopped_processes={len(stopped)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
