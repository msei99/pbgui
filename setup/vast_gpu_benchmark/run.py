"""Run bounded PB8 benchmark cases inside an isolated Linux worker."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

SERVICE = "VastGpuBenchmark"
CASES = ("cpu4", "gpu1", "gpu2", "gpu4")


def verify_bundle(root: Path) -> dict:
    """Verify config/data checksums and reject manifest paths escaping the input."""
    root = root.resolve(strict=True)
    manifest = json.loads((root / "manifest.json").read_text())
    files = [(Path("ohlcv") / entry["path"], entry["sha256"]) for entry in manifest["files"]]
    files.extend((Path("configs") / f"{case}.json", manifest["config_sha256"][case]) for case in CASES)
    for relative, expected in files:
        if relative.is_absolute() or any(part in (".", "..") or "\\" in part or any(ord(char) < 32 for char in part) for part in relative.parts):
            raise ValueError("Invalid bundle path")
        source = (root / relative).resolve(strict=True)
        source.relative_to(root)
        if hashlib.sha256(source.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Bundle checksum mismatch: {relative}")
    return manifest


def interrupt_runner(signum: int, frame: object) -> None:
    """Translate termination into stack unwinding to clean owned processes."""
    raise KeyboardInterrupt


def stop_owned_process(process: subprocess.Popen) -> None:
    """Stop only this benchmark's child process group, with bounded grace."""
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            break
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            continue
    process.wait()


def parse_gpu_profiles(log: Path) -> list[dict]:
    """Extract native structured profiles while tolerating interrupted lines."""
    events = []
    with log.open(errors="replace") as stream:
        for line in stream:
            if "[gpu-profile] " not in line:
                continue
            try:
                event = json.loads(line.split("[gpu-profile] ", 1)[1])
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict):
                events.append(event)
    return events


def run_case(command: list[str], directory: Path, timeout: int) -> dict:
    """Measure wall time and preserve raw profiles and partial results."""
    directory.mkdir(parents=True, exist_ok=False)
    log = directory / "optimizer.log"
    started = time.monotonic()
    timed_out = False
    environment = dict(os.environ, PASSIVBOT_GPU_PROFILE="1", PASSIVBOT_OPTIMIZE_PROFILE="1", OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1", PYTHONUNBUFFERED="1")
    with log.open("wb") as stream:
        process = subprocess.Popen(command, cwd=directory, env=environment, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
        finally:
            # Also clean any descendants left behind by an exited parent.
            stop_owned_process(process)
    return {"exit_code": process.returncode, "timed_out": timed_out, "wall_seconds": time.monotonic() - started, "gpu_profiles": parse_gpu_profiles(log)}


def main() -> int:
    """Run explicitly selected cases; never rent or destroy a cloud instance."""
    os.umask(0o077)
    signal.signal(signal.SIGTERM, interrupt_runner)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=(*CASES, "all"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=600, help="Maximum seconds per case, excluding termination grace")
    args = parser.parse_args()
    if not 10 <= args.timeout <= 3600:
        parser.error("timeout must be between 10 and 3600 seconds")
    selected = CASES if args.case == "all" else (args.case,)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    hardware = {"cpu_count": os.cpu_count(), "cpu_affinity_count": len(os.sched_getaffinity(0)), "python": sys.version, "packages": {}}
    for package in ("passivbot", "numpy", "torch", "cupy-cuda12x"):
        try:
            hardware["packages"][package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            hardware["packages"][package] = None
    if any(case.startswith("gpu") for case in selected):
        import torch
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA is unavailable; GPU results must not silently fall back to CPU")
        hardware["gpu"] = torch.cuda.get_device_name(0)
        hardware["cuda"] = torch.version.cuda
    revision = Path("/opt/pb8-revision").read_text().strip()
    manifest = verify_bundle(Path("/work/input"))
    if revision != manifest["pb8_revision"]:
        raise ValueError("Worker and input require different PB8 revisions")
    report = {"hardware": hardware, "pb8_revision": revision, "cases": {}}
    failed = False
    for case in selected:
        config = Path("/work/input/configs") / f"{case}.json"
        result = run_case([sys.executable, "/opt/passivbot/src/optimize.py", str(config)], output / case, args.timeout)
        report["cases"][case] = result
        failed |= result["timed_out"] or result["exit_code"] != 0
        temporary = output / "report.json.tmp"
        temporary.write_text(json.dumps(report, indent=4) + "\n")
        os.replace(temporary, output / "report.json")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
