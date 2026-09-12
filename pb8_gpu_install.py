"""Select optional PB8 GPU dependencies without importing Torch or changing the host."""

from __future__ import annotations

import argparse
import ast
import platform
import shutil
import subprocess
import sys
from pathlib import Path

SERVICE = "PB8GPUInstall"


def _has_nvidia_device() -> bool:
    """Detect a usable NVIDIA driver, including WSL2's separate binary location."""
    binary = shutil.which("nvidia-smi")
    wsl_binary = Path("/usr/lib/wsl/lib/nvidia-smi")
    if not binary and wsl_binary.is_file():
        binary = str(wsl_binary)
    if not binary:
        return False
    try:
        probe = subprocess.run(
            [binary, "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        # An unavailable driver is an expected negative capability result.
        return False
    return probe.returncode == 0 and bool(probe.stdout.strip())


def pb8_gpu_extras(pb8_dir: Path, gpu: str = "auto") -> str:
    """Choose a full profile supported by both the target host and PB8 revision."""
    if gpu not in {"auto", "cpu", "mps", "cuda"}:
        raise ValueError("PB8 GPU profile must be auto, cpu, mps, or cuda")
    system, machine = platform.system(), platform.machine()
    if gpu == "auto":
        gpu = "mps" if system == "Darwin" and machine == "arm64" else "cpu"
        if system == "Linux" and _has_nvidia_device():
            gpu = "cuda"
            explicit = False
        else:
            explicit = False
    else:
        explicit = True
    if gpu == "cpu":
        return "full"
    if gpu == "mps" and (system != "Darwin" or machine != "arm64"):
        raise ValueError("PB8 gpu-mps requires Apple Silicon")
    if gpu == "cuda" and system != "Linux":
        raise ValueError("PB8 gpu-cuda requires Linux or WSL2")
    setup_file = Path(pb8_dir) / "setup.py"
    if not setup_file.is_file():
        raise ValueError("PB8 setup.py is missing; cannot select optional GPU dependencies")
    try:
        tree = ast.parse(setup_file.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, UnicodeError) as exc:
        raise ValueError("Cannot read PB8 optional dependency declarations") from exc
    extra = "gpu-" + gpu
    supported = any(
        isinstance(node, ast.Dict) and any(
            isinstance(key, ast.Constant) and key.value == extra for key in node.keys
        )
        for node in ast.walk(tree)
    )
    if not supported:
        if explicit:
            raise ValueError(f"This PB8 revision does not declare {extra}; update PB8 first")
        return "full"
    return "full," + extra


def main() -> int:
    """Return a bounded profile token for installer and Ansible callers."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pb8_dir", type=Path)
    parser.add_argument("--gpu", choices=("auto", "cpu", "mps", "cuda"), default="auto")
    args = parser.parse_args()
    try:
        extras = pb8_gpu_extras(args.pb8_dir, args.gpu)
    except ValueError as exc:
        parser.error(str(exc))
    sys.stdout.write(extras + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
