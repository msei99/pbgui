#!/usr/bin/env python3
"""Refresh one PBGui monitor-agent package status cache."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


def collect_package_status() -> dict[str, object]:
    """Return the current Debian/Ubuntu package and reboot status."""
    env = os.environ.copy()
    env["LANG"] = "C"
    result = subprocess.run(
        ["apt-get", "dist-upgrade", "-s"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"apt probe failed rc={result.returncode}")
    match = re.search(r"(\d+) upgraded", result.stdout or "")
    if not match:
        raise RuntimeError("apt output did not contain an upgrade count")
    return {
        "schema_version": 1,
        "source": "monitor-agent",
        "generated_at": time.time(),
        "upgrades": match.group(1),
        "reboot": Path("/var/run/reboot-required").exists(),
    }


def write_package_status(pbgui_dir: Path, payload: dict[str, object]) -> Path:
    """Atomically write package status below one resolved PBGui root."""
    root = pbgui_dir.expanduser().resolve()
    if not root.is_absolute():
        raise ValueError("PBGui directory must be absolute")
    output = root / "data" / "monitor_agent" / "package_status.json"
    if output.is_symlink():
        raise RuntimeError("package status cache must not be a symlink")
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary_name = tempfile.mkstemp(prefix=".package_status.", dir=output.parent, text=True)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return output


def main(argv: list[str] | None = None) -> int:
    """Collect and persist a fresh package status payload."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbgdir", required=True, type=Path)
    args = parser.parse_args(argv)
    payload = collect_package_status()
    write_package_status(args.pbgdir, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
