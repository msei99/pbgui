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


_APT_INSTALL_RE = re.compile(
    r"^Inst\s+(\S+)(?:\s+\[([^\]]*)\])?\s+\((\S+)(?:\s+(.+?))?\)(?:\s+\[[^\]]*\])*$"
)
_APT_ARCH_RE = re.compile(r"\s+\[([^\]]+)\]\s*$")
_APT_REMOVE_RE = re.compile(r"^Remv\s+(\S+)(?:\s+\[([^\]]*)\])?(?:\s+\(([^)]*)\))?")
_KERNEL_PACKAGE_RE = re.compile(r"^(?:linux-(?:image|headers|modules|tools|generic|virtual)|ubuntu-kernel-accessories)")
MAX_PACKAGE_DETAILS = 500


def _parse_package_details(output: str) -> list[dict[str, object]]:
    """Parse bounded package/version/source details from apt simulation output."""
    packages: list[dict[str, object]] = []
    for line in str(output or "").splitlines():
        match = _APT_INSTALL_RE.match(line.strip())
        if not match:
            continue
        name, installed, candidate, source_tail = match.groups()
        source = str(source_tail or "").strip()
        architecture = ""
        architecture_match = _APT_ARCH_RE.search(source)
        if architecture_match:
            architecture = architecture_match.group(1).strip()
            source = source[:architecture_match.start()].strip()
        security = "security" in source.lower()
        kernel = bool(_KERNEL_PACKAGE_RE.match(name.lower()))
        packages.append({
            "name": name[:200],
            "installed_version": str(installed or "")[:256],
            "candidate_version": candidate[:256],
            "source": source[:300],
            "architecture": architecture[:64],
            "security": security,
            "kernel": kernel,
            "removed": False,
        })
        if len(packages) >= MAX_PACKAGE_DETAILS:
            break
    return packages


def _parse_package_removals(output: str) -> list[dict[str, object]]:
    """Parse packages apt plans to remove during the simulated transaction."""
    packages: list[dict[str, object]] = []
    for line in str(output or "").splitlines():
        match = _APT_REMOVE_RE.match(line.strip())
        if not match:
            continue
        name, installed, source = match.groups()
        packages.append({
            "name": name[:200],
            "installed_version": str(installed or "")[:256],
            "candidate_version": "removed",
            "source": str(source or "")[:300],
            "architecture": "",
            "security": False,
            "kernel": bool(_KERNEL_PACKAGE_RE.match(name.lower())),
            "removed": True,
        })
        if len(packages) >= MAX_PACKAGE_DETAILS:
            break
    return packages


def _prepare_apt_lists(pbgui_dir: Path) -> Path:
    """Create a private apt list cache below the resolved PBGui data root."""
    root = pbgui_dir.expanduser().resolve()
    data_dir = root / "data"
    monitor_dir = data_dir / "monitor_agent"
    apt_dir = monitor_dir / "apt"
    lists_dir = apt_dir / "lists"
    cache_dir = apt_dir / "cache"
    for path in (data_dir, monitor_dir, apt_dir, lists_dir, lists_dir / "partial", cache_dir, cache_dir / "archives" / "partial"):
        if path.is_symlink():
            raise RuntimeError("apt list cache path must not contain symlinks")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    for path in (monitor_dir, apt_dir, lists_dir, lists_dir / "partial", cache_dir, cache_dir / "archives", cache_dir / "archives" / "partial"):
        path.chmod(0o700)
    return apt_dir


def collect_package_status(pbgui_dir: Path) -> dict[str, object]:
    """Refresh package indexes and return Debian/Ubuntu package status."""
    env = os.environ.copy()
    env["LANG"] = "C"
    env["LC_ALL"] = "C"
    apt_dir = _prepare_apt_lists(pbgui_dir)
    lists_dir = apt_dir / "lists"
    cache_dir = apt_dir / "cache"
    apt_options = [
        "-o", f"Dir::State::Lists={lists_dir}{os.sep}",
        "-o", f"Dir::Cache={cache_dir}{os.sep}",
        "-o", "Acquire::Languages=none",
        "-o", "Acquire::IndexTargets::deb::DEP-11::DefaultEnabled=false",
        "-o", "Acquire::IndexTargets::deb::DEP-11-icons-small::DefaultEnabled=false",
        "-o", "Acquire::IndexTargets::deb::DEP-11-icons::DefaultEnabled=false",
        "-o", "Acquire::IndexTargets::deb::DEP-11-icons-hidpi::DefaultEnabled=false",
        "-o", "Acquire::IndexTargets::deb::CNF::DefaultEnabled=false",
        "-o", "DPkg::Lock::Timeout=120",
    ]
    update_result = subprocess.run(
        ["apt-get", *apt_options, "update"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        check=False,
    )
    if update_result.returncode != 0:
        raise RuntimeError(f"apt index refresh failed rc={update_result.returncode}")
    result = subprocess.run(
        ["apt-get", *apt_options, "dist-upgrade", "-s"],
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
    upgrade_count = int(match.group(1))
    deferred_match = re.search(r"(\d+) not upgraded", result.stdout or "")
    deferred_count = int(deferred_match.group(1)) if deferred_match else 0
    pending_update_count = upgrade_count + deferred_count
    new_install_match = re.search(r"(\d+) newly installed", result.stdout or "")
    new_install_count = int(new_install_match.group(1)) if new_install_match else 0
    removal_match = re.search(r"(\d+) to remove", result.stdout or "")
    removal_count = int(removal_match.group(1)) if removal_match else 0
    expected_package_changes = upgrade_count + new_install_count + removal_count
    packages = _parse_package_details(result.stdout or "")
    remaining_slots = max(MAX_PACKAGE_DETAILS - len(packages), 0)
    if remaining_slots:
        packages.extend(_parse_package_removals(result.stdout or "")[:remaining_slots])
    security_updates = sum(1 for package in packages if package["security"])
    removal_updates = sum(1 for package in packages if package["removed"] and not package["security"])
    kernel_updates = sum(1 for package in packages if package["kernel"] and not package["security"] and not package["removed"])
    categorized_updates = sum(1 for package in packages if package["security"] or package["kernel"] or package["removed"])
    details_complete = deferred_count == 0 and len(packages) == expected_package_changes
    if security_updates:
        urgency = "security"
    elif not details_complete:
        urgency = "unknown"
    elif removal_updates:
        urgency = "removal"
    elif kernel_updates:
        urgency = "kernel"
    elif pending_update_count or expected_package_changes:
        urgency = "routine"
    else:
        urgency = "none"
    return {
        "schema_version": 1,
        "source": "monitor-agent",
        "generated_at": time.time(),
        "upgrades": str(pending_update_count),
        "installable_updates": upgrade_count,
        "deferred_updates": deferred_count,
        "new_installs": new_install_count,
        "removals": removal_count,
        "reboot": Path("/var/run/reboot-required").exists(),
        "packages": packages,
        "security_updates": security_updates,
        "kernel_updates": kernel_updates,
        "removal_updates": removal_updates,
        "routine_updates": max(len(packages) - categorized_updates, 0),
        "urgency": urgency,
        "details_complete": details_complete,
        "details_truncated": expected_package_changes > len(packages) and len(packages) >= MAX_PACKAGE_DETAILS,
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
    payload = collect_package_status(args.pbgdir)
    write_package_status(args.pbgdir, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
