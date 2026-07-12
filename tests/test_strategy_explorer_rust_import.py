"""Tests for Strategy Explorer PB7 Rust import path handling."""

import sys

from api import strategy_explorer_core as core


def test_pb7_venv_site_packages_uses_configured_venv_without_resolving(tmp_path, monkeypatch):
    """Return PB7 venv site-packages from the configured interpreter path."""
    venv_root = tmp_path / "venv_pb7"
    python_bin = venv_root / "bin" / "python"
    site_packages = venv_root / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    python_bin.parent.mkdir(parents=True)
    python_bin.write_text("", encoding="utf-8")
    site_packages.mkdir(parents=True)

    monkeypatch.setattr(core, "pb7venv", lambda: str(python_bin))

    assert core._pb7_venv_site_packages() == [str(site_packages)]


def test_normalize_pb7_src_dir_accepts_pb7_root_and_src(tmp_path):
    """Normalize both PB7 root and PB7 src inputs to the source directory."""
    pb7_root = tmp_path / "pb7"
    pb7_src = pb7_root / "src"
    pb7_src.mkdir(parents=True)
    (pb7_src / "passivbot.py").write_text("", encoding="utf-8")

    assert core._normalize_pb7_src_dir(str(pb7_root)) == str(pb7_src)
    assert core._normalize_pb7_src_dir(str(pb7_src)) == str(pb7_src)
