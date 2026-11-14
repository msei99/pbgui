"""Inspect Passivbot source identity without importing or executing bot code."""

from __future__ import annotations

import ast
import os
from pathlib import Path
import re
import subprocess


SERVICE = "PassivbotRuntime"
MAX_SOURCE_BYTES = 256 * 1024
_SCHEMA_RE = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
_SCHEMA_GENERATIONS = {"8": "pb8", "9": "pb9"}


def read_source_string_literal(source: str | bytes, name: str, *, source_name: str = "Passivbot source") -> str:
    """Read one literal declaration without executing source, bounded to 256 KiB.

    UTF-8 BOM is accepted. AST Name stores detect ordinary/conditional duplicate
    assignments; this is not a proof against arbitrary dynamic rebinding.
    Only standard-library APIs available with Python 3.8 are used here.
    """
    try:
        encoded = source.encode("utf-8") if isinstance(source, str) else source
        if len(encoded) > MAX_SOURCE_BYTES:
            raise ValueError(f"{source_name} exceeds the 256 KiB source limit")
        tree = ast.parse(encoded.decode("utf-8-sig"))
    except (UnicodeError, SyntaxError, RecursionError) as exc:
        raise ValueError(f"Cannot parse {source_name} ({type(exc).__name__})") from exc

    # Detect AST Name stores, including conditional and compound assignments.
    # This declaration reader does not prove the absence of dynamic rebinding.
    writes = [node for node in ast.walk(tree)
              if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store)
              and node.id == name]
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
        elif isinstance(node, ast.AnnAssign):
            target = node.target
        else:
            continue
        if isinstance(target, ast.Name) and target.id == name and len(writes) == 1:
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                return node.value.value
    raise ValueError(f"{source_name}: {name} must have exactly one top-level string literal assignment")


def generation_from_schema(schema: str) -> str:
    """Classify a literal schema major; never infer a generation from a package."""
    match = _SCHEMA_RE.fullmatch(schema)
    return _SCHEMA_GENERATIONS.get(match[1], "unknown") if match else "unknown"


def _read_string_literal(repo_dir: Path, relative_path: str, name: str) -> tuple[str, bytes]:
    """Read a top-level literal and retain its exact inspected source bytes."""
    path = repo_dir / relative_path
    try:
        path = path.resolve()
        # Keep this helper usable when imported by the system-Python guard.
        try:
            path.relative_to(repo_dir)
        except ValueError as exc:
            raise ValueError(f"{relative_path} resolves outside the source directory") from exc
        if not path.is_file():
            raise ValueError(f"{relative_path} is missing or is not a regular file")
        with path.open("rb") as stream:
            source = stream.read(MAX_SOURCE_BYTES + 1)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"Cannot read {relative_path} ({type(exc).__name__})") from exc
    return read_source_string_literal(source, name, source_name=relative_path), source


def _read_commit(repo_dir: Path, sources: dict[str, bytes]) -> str:
    """Associate inspected bytes with HEAD only within the selected checkout."""
    if not (repo_dir / ".git").exists():
        raise ValueError("Source directory is not a Git checkout; commit is unavailable")
    # Git's repository/config environment must not redirect these local reads.
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update({"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_NO_REPLACE_OBJECTS": "1", "GIT_OPTIONAL_LOCKS": "0"})
    try:
        root = subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "--show-toplevel"],
            check=True, capture_output=True, text=True, timeout=5, env=env,
        ).stdout.strip()
        if Path(root).resolve() != repo_dir:
            raise ValueError("Git checkout root does not match the source directory")
        commit = subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "--verify", "HEAD"],
            check=True, capture_output=True, text=True, timeout=5, env=env,
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit):
            raise ValueError("Source Git commit is not a full commit hash")
        for relative_path, source in sources.items():
            committed_source = subprocess.run(
                ["git", "-C", str(repo_dir), "show", f"{commit}:{relative_path}"],
                check=True, capture_output=True, timeout=5, env=env,
            ).stdout
            # Git may materialize LF blobs as CRLF. Normalize only that pair;
            # do not run configured clean/textconv filters or ignore other edits.
            if committed_source.replace(b"\r\n", b"\n") != source.replace(b"\r\n", b"\n"):
                raise ValueError(f"Inspected source differs from HEAD: {relative_path}; commit is unavailable")
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"Cannot read source Git commit ({type(exc).__name__})") from exc
    return commit


def classify_passivbot_source(repo_dir: str | Path | None) -> dict:
    """Return generation, package_version, config_schema, commit and errors.

    Only literal schema majors 8 and 9 classify as pb8/pb9. Other or unreadable
    schemas classify as unknown; the package version never provides a fallback.
    Missing package/Git metadata is reported independently of classification.
    Commit is unavailable if inspected metadata differs from HEAD after CRLF/LF
    normalization. It identifies these two declarations, not full-tree cleanliness.
    Errors must be checked even when generation or commit is available.
    This source identity is not an installation readiness or compatibility check.
    Git reads are synchronous; async callers must offload this function.
    No inspected Python module is imported and no dependency is installed.
    """
    result = {
        "generation": "unknown", "package_version": None,
        "config_schema": None, "commit": None, "errors": [],
    }
    try:
        if repo_dir is None or (isinstance(repo_dir, str) and not repo_dir.strip()):
            raise ValueError("Source directory must be explicitly set")
        if any(ord(char) < 32 or ord(char) == 127 for char in str(repo_dir)):
            raise ValueError("Source directory contains control characters")
        root = Path(repo_dir).expanduser()
        if not root.is_absolute():
            raise ValueError("Source directory must be an absolute path")
        root = root.resolve()
        if not root.is_dir():
            raise ValueError("Source directory is missing or is not a directory")
    except (OSError, ValueError, RuntimeError, TypeError) as exc:
        result["errors"].append(f"Invalid source directory ({type(exc).__name__}): {exc}")
        return result

    sources = {}
    for key, relative_path, name in (
        ("package_version", "src/passivbot_version.py", "__version__"),
        ("config_schema", "src/config/schema.py", "CONFIG_SCHEMA_VERSION"),
    ):
        try:
            result[key], sources[relative_path] = _read_string_literal(root, relative_path, name)
        except ValueError as exc:
            result["errors"].append(str(exc))

    schema = result["config_schema"]
    if schema is not None:
        generation = generation_from_schema(schema)
        if generation == "unknown":
            result["errors"].append("Unrecognized config schema; expected a v8.x.y or v9.x.y schema")
        else:
            result["generation"] = generation
    try:
        result["commit"] = _read_commit(root, sources)
    except ValueError as exc:
        result["errors"].append(str(exc))
    return result
