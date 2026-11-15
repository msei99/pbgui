"""Static, read-only detection of a separate experimental PB9 runtime layout."""

from __future__ import annotations

import configparser
import os
from pathlib import Path
import re
import sys

from passivbot_runtime import classify_passivbot_source


SERVICE = "PB9Runtime"
_SLOT_KEYS = ("pb7dir", "pb7venv", "pb8dir", "pb8venv")


class PB9RuntimePathError(ValueError):
    """Reject a path update with stable codes and no persisted partial state."""

    def __init__(self, status: dict):
        """Keep validation diagnostics available without embedding setting values."""
        self.error_codes = tuple(status["error_codes"])
        self.errors = tuple(status["errors"])
        super().__init__("PB9 paths were not saved: " + ", ".join(self.error_codes))


def _empty_status() -> dict:
    """Return static evidence fields, deliberately without an installation-ready flag."""
    return {
        "configured": False, "detected": False,
        "pb9dir": "", "pb9venv": "", "venv_dir": "",
        "source_detected": False, "source_separate": False, "venv_separate": False,
        "package_version": None, "config_schema": None, "commit": None,
        "commit_available": False, "source_diagnostics": [],
        "python_exists": False, "interpreter_name_ok": False,
        "interpreter_executable": False, "python_version": "",
        "python_version_supported": False,
        "cli_file": "", "cli_exists": False, "cli_executable": False,
        "rust_file": "", "rust_exists": False,
        "rust_stamp_file": "", "rust_stamp_present": False,
        "errors": [], "error_codes": [], "warnings": [], "warning_codes": [],
    }


def _diagnostic(status: dict, code: str, message: str, *, warning: bool = False) -> None:
    """Keep human-readable diagnostics alongside stable machine-readable codes."""
    status["warnings" if warning else "errors"].append(message)
    status["warning_codes" if warning else "error_codes"].append(code)


def _absolute_path(value: str | Path, *, interpreter: bool = False) -> Path:
    """Canonicalize a non-secret configured path without resolving interpreter links."""
    text = str(value)
    if not text.strip() or any(ord(char) < 32 or ord(char) == 127 for char in text):
        raise ValueError("Path is empty or contains control characters")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError("Path must be absolute")
    return path.parent.resolve() / path.name if interpreter else path.resolve()


def _venv_root(interpreter: Path) -> Path:
    """Identify an environment by its directory, not its Python symlink target."""
    if interpreter.parent.name.lower() not in {"bin", "scripts"}:
        raise ValueError("Interpreter must be inside a virtual environment's bin or Scripts directory")
    return interpreter.parent.parent.resolve()


def _overlaps(first: Path, second: Path) -> bool:
    """Recognize equality and nesting in either direction after canonicalization."""
    return first == second or first in second.parents or second in first.parents


def _check_separation(status: dict, source: Path | None, venv: Path | None, other_slots: dict) -> None:
    """Reject source overlap and shared PB7/PB8/PBGui virtual environments."""
    status["source_separate"] = source is not None
    status["venv_separate"] = venv is not None
    for slot in ("pb7", "pb8"):
        for suffix in ("dir", "venv"):
            key = slot + suffix
            value = other_slots.get(key)
            if value is None or not str(value).strip():
                continue
            try:
                other = _absolute_path(value, interpreter=suffix == "venv")
                if suffix == "dir" and source and _overlaps(source, other):
                    status["source_separate"] = False
                    _diagnostic(status, "source_overlap", f"PB9 source overlaps the configured {slot.upper()} source")
                if suffix == "venv" and venv and _venv_root(other) == venv:
                    status["venv_separate"] = False
                    _diagnostic(status, "venv_shared", f"PB9 shares the configured {slot.upper()} environment")
            except (OSError, ValueError, RuntimeError):
                status["source_separate" if suffix == "dir" else "venv_separate"] = False
                _diagnostic(status, "other_slot_path_invalid", f"Cannot verify separation from {key}")
    if venv and venv == Path(sys.prefix).resolve():
        status["venv_separate"] = False
        _diagnostic(status, "venv_shared", "PB9 shares the running PBGui Python environment")


def _python_version(venv: Path) -> str:
    """Read only version declarations from bounded virtual-environment metadata."""
    cfg = venv / "pyvenv.cfg"
    if not cfg.is_file() or not cfg.resolve().is_relative_to(venv):
        raise ValueError("pyvenv.cfg is missing or resolves outside the environment")
    with cfg.open("rb") as stream:
        source = stream.read(65537)
    if len(source) > 65536:
        raise ValueError("pyvenv.cfg exceeds the metadata limit")
    versions = []
    for line in source.decode("utf-8-sig").splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() in {"version", "version_info"}:
            match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:\.[a-z]+\.\d+)?", value.strip())
            if not match:
                raise ValueError("Python version declaration is invalid")
            versions.append(".".join(match.groups()))
    if not versions or len(set(versions)) != 1:
        raise ValueError("Python version is missing or conflicting")
    return versions[0]


def _inspect_artifacts(status: dict, source: Path | None, python: Path, venv: Path) -> None:
    """Check artifact presence and permissions without executing or loading them."""
    status["python_exists"] = python.is_file()
    status["interpreter_name_ok"] = python.name.lower().startswith("python")
    status["interpreter_executable"] = status["python_exists"] and os.access(python, os.X_OK)
    if not status["python_exists"]:
        _diagnostic(status, "python_missing", "PB9 Python interpreter is missing")
    if not status["interpreter_name_ok"]:
        _diagnostic(status, "python_name_invalid", "PB9 interpreter filename must begin with python")
    if status["python_exists"] and not status["interpreter_executable"]:
        _diagnostic(status, "python_not_executable", "PB9 Python interpreter is not executable")
    try:
        version = _python_version(venv)
        status["python_version"] = version
        status["python_version_supported"] = tuple(version.split(".")[:2]) in {("3", "12"), ("3", "14")}
        if not status["python_version_supported"]:
            _diagnostic(status, "python_version_unsupported", "PB9 requires Python 3.12 or 3.14")
    except (OSError, ValueError, RuntimeError):
        _diagnostic(status, "python_metadata_invalid", "Cannot read a valid Python version from pyvenv.cfg")

    cli = python.parent / ("passivbot.exe" if python.parent.name.lower() == "scripts" else "passivbot")
    status.update(cli_file=str(cli), cli_exists=cli.is_file())
    status["cli_executable"] = status["cli_exists"] and os.access(cli, os.X_OK)
    if not status["cli_exists"]:
        _diagnostic(status, "cli_missing", "PB9 CLI is missing")
    elif not status["cli_executable"]:
        _diagnostic(status, "cli_not_executable", "PB9 CLI is not executable")

    bases = [venv / "Lib" / "site-packages"]
    for lib_name in ("lib", "lib64"):
        lib = venv / lib_name
        if lib.is_dir():
            bases.extend(sorted(lib.glob("python*/site-packages")))
    if source:
        bases.append(source / "src")
    roots = [venv] + ([source] if source else [])
    for base in bases:
        for pattern in ("passivbot_rust/passivbot_rust*.so", "passivbot_rust/passivbot_rust*.pyd",
                        "passivbot_rust*.so", "passivbot_rust*.pyd"):
            for artifact in sorted(base.glob(pattern)):
                if artifact.is_file() and any(artifact.resolve().is_relative_to(root) for root in roots):
                    status.update(rust_file=str(artifact), rust_exists=True)
                    stamp = artifact.with_name(artifact.name + ".rust-src-sha256")
                    status["rust_stamp_file"] = str(stamp)
                    status["rust_stamp_present"] = stamp.is_file() and any(
                        stamp.resolve().is_relative_to(root) for root in roots)
                    return
    _diagnostic(status, "rust_missing", "PB9 Rust extension is missing")


def inspect_pb9_runtime(pb9dir: str | Path | None, pb9venv: str | Path | None,
                        other_slots: dict | None = None, *, cancel_event=None) -> dict:
    """Inspect static layout evidence, not executable/ABI compatibility or readiness.

    other_slots contains optional pb7dir/pb7venv/pb8dir/pb8venv paths. No INI is
    read, interpreter started, module imported, file written or client retained.
    Missing Git identity is only a warning once both source declarations are valid.
    rust_stamp_present reports presence only; its content is not verified.
    This synchronous inspection must be offloaded by any future async caller.
    """
    status = _empty_status()
    status["configured"] = any(value is not None and str(value).strip() for value in (pb9dir, pb9venv))
    if not status["configured"]:
        return status
    source = python = venv = None
    for value, key in ((pb9dir, "pb9dir"), (pb9venv, "pb9venv")):
        try:
            if value is None:
                raise ValueError("Path is not configured")
            path = _absolute_path(value, interpreter=key == "pb9venv")
            status[key] = str(path)
            if key == "pb9dir":
                source = path
            else:
                python = path
                venv = _venv_root(python)
                status["venv_dir"] = str(venv)
        except (OSError, ValueError, RuntimeError, TypeError):
            _diagnostic(status, key + "_invalid", f"{key} must be a configured absolute runtime path")
    try:
        _check_separation(status, source, venv, other_slots or {})
        if source:
            identity = (classify_passivbot_source(source, git_timeout=1, cancel_requested=cancel_event.is_set)
                        if cancel_event is not None else classify_passivbot_source(source))
            for key in ("package_version", "config_schema", "commit"):
                status[key] = identity[key]
            status["source_diagnostics"] = identity["errors"]
            status["source_detected"] = identity["generation"] == "pb9" and bool(identity["package_version"])
            status["commit_available"] = identity["commit"] is not None
            if not status["source_detected"]:
                _diagnostic(status, "source_invalid", "PB9 source requires readable package metadata and a v9 config schema")
            elif not status["commit_available"]:
                _diagnostic(status, "commit_unavailable", "PB9 source commit is unavailable; provenance is incomplete", warning=True)
        if python and venv:
            _inspect_artifacts(status, source, python, venv)
    except (OSError, ValueError, RuntimeError):
        _diagnostic(status, "inspection_failed", "PB9 runtime files could not be inspected")
    status["detected"] = bool(status["source_detected"] and status["source_separate"]
                              and status["venv_separate"] and not status["errors"])
    return status


def pb9_runtime_status() -> dict:
    """Read one INI snapshot and inspect only allowlisted non-secret path settings."""
    from pbgui_purefunc import load_ini_snapshot

    try:
        snapshot = load_ini_snapshot()
        values = {key: snapshot.get("main", key) if snapshot.has_option("main", key) else ""
                  for key in ("pb9dir", "pb9venv", *_SLOT_KEYS)}
    except (OSError, configparser.Error, ValueError, RuntimeError):
        status = _empty_status()
        _diagnostic(status, "ini_read_failed", "PB9 path settings could not be read")
        return status
    return inspect_pb9_runtime(values["pb9dir"], values["pb9venv"],
                               {key: values[key] for key in _SLOT_KEYS})


def check_slot_change(parser: configparser.ConfigParser, changed_keys: set[tuple[str, str]], *,
                      requested_values: dict[str, str] | None = None) -> None:
    """Reject changed PB7/PB8 paths overlapping a configured PB9 reservation.

    Inspect paths only, never source metadata, artifacts or Git. Optional raw
    interpreter requests preserve environment identity when a legacy caller's
    normalization follows the executable symlink before this hook runs.
    """
    changes = {key for section, key in changed_keys if section == "main" and key in _SLOT_KEYS}
    if not changes:
        return
    pb9_paths = {key: parser.get("main", key, raw=True, fallback="")
                 for key in ("pb9dir", "pb9venv")}
    if not any(value.strip() for value in pb9_paths.values()):
        return
    status = _empty_status()
    for key in sorted(changes):
        value = parser.get("main", key, raw=True, fallback="")
        if not value.strip():
            continue
        try:
            if key.endswith("dir") and pb9_paths["pb9dir"].strip():
                if _overlaps(_absolute_path(value), _absolute_path(pb9_paths["pb9dir"])):
                    _diagnostic(status, "source_overlap", "Runtime source overlaps the configured PB9 source")
            elif key.endswith("venv") and pb9_paths["pb9venv"].strip():
                roots = {_venv_root(_absolute_path(value, interpreter=True))}
                requested = (requested_values or {}).get(key, "")
                if requested.strip():
                    # Existing PB7 settings also accept cwd-relative paths.
                    requested_path = Path(requested).expanduser().absolute()
                    roots.add(_venv_root(_absolute_path(requested_path, interpreter=True)))
                if _venv_root(_absolute_path(pb9_paths["pb9venv"], interpreter=True)) in roots:
                    _diagnostic(status, "venv_shared", "Runtime shares the configured PB9 environment")
        except (OSError, ValueError, RuntimeError, TypeError):
            _diagnostic(status, "other_slot_path_invalid", "Cannot verify changed runtime path separation from PB9")
    if status["errors"]:
        raise PB9RuntimePathError(status)


def save_pb9_runtime_paths(pb9dir: str | Path | None, pb9venv: str | Path | None) -> dict:
    """Validate and save both PB9 paths in one locked, atomic INI transaction.

    Two empty values explicitly remove both keys. Otherwise the static layout
    must be detected; validation failure raises PB9RuntimePathError before any
    persistence. Other slot paths come from the same locked parser generation.
    Interpreter symlink targets remain unresolved. No interpreter is executed.
    This synchronous operation must be offloaded by any future async caller.
    """
    from pbgui_purefunc import update_ini

    clear = all(value is None or (isinstance(value, str) and not value.strip())
                for value in (pb9dir, pb9venv))

    def mutate(parser: configparser.ConfigParser) -> dict:
        """Check separation before publishing either of the new path values."""
        if clear:
            if parser.has_section("main"):
                for key in ("pb9dir", "pb9venv"):
                    parser.remove_option("main", key)
            return _empty_status()
        other_slots = {key: parser.get("main", key) if parser.has_option("main", key) else ""
                       for key in _SLOT_KEYS}
        status = inspect_pb9_runtime(pb9dir, pb9venv, other_slots)
        if not status["detected"]:
            raise PB9RuntimePathError(status)
        if not parser.has_section("main"):
            parser.add_section("main")
        for key in ("pb9dir", "pb9venv"):
            parser.set("main", key, status[key])
        return status

    return update_ini(mutate)
