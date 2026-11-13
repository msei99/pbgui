"""Verify PB7/PB8 wheel receipts and retire unused VPS Rust build tools."""
from __future__ import annotations

import configparser
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

SERVICE = "PBWheelStatus"


def wheel_installed(pbgdir: Path, product: str, repo: Path, python: Path) -> bool:
    """Accept only a receipt tied to this checkout and installed Rust extension."""
    if product not in {"pb7", "pb8"} or not repo.is_dir() or not python.is_file():
        return False
    receipt = pbgdir / "data/state" / f"{product}-wheel.json"
    try:
        if receipt.is_symlink() or receipt.stat().st_size > 4096:
            return False
        data = json.loads(receipt.read_text(encoding="utf-8"))
        if (data.get("format") != 1 or data.get("product") != product
                or not re.fullmatch("[0-9a-f]{40}", data.get("commit", ""))
                or not re.fullmatch("[0-9a-f]{64}", data.get("extension_sha256", ""))):
            return False
        commit = subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            text=True, stderr=subprocess.DEVNULL, timeout=10).strip()
        if commit != data["commit"]:
            return False
        site = python.parent.parent / "lib/python3.12/site-packages/passivbot_rust"
        binaries = list(site.glob("passivbot_rust*.so"))
        if len(binaries) != 1 or binaries[0].is_symlink():
            return False
        with binaries[0].open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest() == data["extension_sha256"]
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        return False


def cleanup_vps_rust(pbgdir: Path) -> dict:
    """Remove PBGui Rust build data only when every installed bot uses wheels."""
    pbgdir = pbgdir.resolve(strict=True)
    home = Path.home().resolve()
    if pbgdir.name != "pbgui" or not pbgdir.is_relative_to(home):
        return {"cleaned": False, "reason": "PBGui is outside the VPS account"}
    config = configparser.ConfigParser()
    config.read(pbgdir / "pbgui.ini")
    role = config.get("main", "role", fallback="").strip().lower()
    if role not in {"vps", "slave"}:
        return {"cleaned": False, "reason": "Rust cleanup is restricted to VPS runners"}
    installed = []
    for product in ("pb7", "pb8"):
        repo_text = config.get("main", f"{product}dir", fallback="").strip()
        python_text = config.get("main", f"{product}venv", fallback="").strip()
        repo = Path(repo_text) if repo_text else pbgdir.parent / product
        executable = Path(python_text) if python_text else pbgdir.parent / f"venv_{product}/bin/python"
        if not repo.exists() and not repo_text:
            continue
        if not repo.is_dir():
            return {"cleaned": False, "reason": f"Configured {product} checkout is missing"}
        if repo.is_symlink() or not repo.resolve().is_relative_to(home):
            return {"cleaned": False, "reason": f"Unsafe {product} checkout path"}
        if not wheel_installed(pbgdir, product, repo, executable):
            return {"cleaned": False, "reason": f"{product} has no verified wheel install"}
        installed.append((product, repo))
    if not installed:
        return {"cleaned": False, "reason": "No PB7/PB8 runtime is installed"}
    try:
        for _, repo in installed:
            rust_source = repo / "passivbot-rust"
            if rust_source.is_symlink() or not rust_source.resolve().is_relative_to(repo.resolve()):
                return {"cleaned": False, "reason": "Unsafe Rust source path"}
            target = rust_source / "target"
            if target.is_symlink():
                target.unlink()
            elif target.is_dir():
                shutil.rmtree(target)
    except OSError as exc:
        return {"cleaned": False, "reason": f"Rust build cleanup failed: {exc.strerror or type(exc).__name__}"}
    rustup = home / ".rustup"
    cargo = home / ".cargo"
    if rustup.is_symlink() or cargo.is_symlink():
        return {"cleaned": False, "reason": "Rust home is a symlink; build targets removed"}
    toolchains = rustup / "toolchains"
    try:
        if toolchains.exists() and any(not p.name.startswith("1.90.0-") for p in toolchains.iterdir()):
            return {"cleaned": False, "reason": "Another Rust toolchain exists; build targets removed"}
    except OSError:
        return {"cleaned": False, "reason": "Cannot inspect Rust toolchains; build targets removed"}
    allowed_bins = {"cargo", "cargo-clippy", "cargo-fmt", "clippy-driver", "rustc",
                    "rustdoc", "rustfmt", "rustup", "rust-gdb", "rust-gdbgui", "rust-lldb"}
    bin_dir = cargo / "bin"
    try:
        if bin_dir.exists() and any(p.name not in allowed_bins for p in bin_dir.iterdir()):
            return {"cleaned": False, "reason": "Another Cargo tool exists; build targets removed"}
    except OSError:
        return {"cleaned": False, "reason": "Cannot inspect Cargo tools; build targets removed"}
    try:
        for directory in (rustup, cargo):
            if directory.is_dir():
                shutil.rmtree(directory)
    except OSError as exc:
        return {"cleaned": False, "reason": f"Rust tool cleanup failed: {exc.strerror or type(exc).__name__}"}
    return {"cleaned": True, "products": [product for product, _ in installed]}


def main() -> None:
    """Inspect a wheel install or run guarded VPS Rust cleanup."""
    parser = argparse.ArgumentParser()
    parser.add_argument("pbgdir", type=Path)
    parser.add_argument("--check-product", choices=["pb7", "pb8"])
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--python", type=Path)
    args = parser.parse_args()
    if args.check_product:
        if args.repo is None or args.python is None:
            parser.error("--check-product requires --repo and --python")
        result = {"installed": wheel_installed(args.pbgdir, args.check_product, args.repo, args.python)}
    else:
        result = cleanup_vps_rust(args.pbgdir)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
