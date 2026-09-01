"""Persistent reconstructable SQLite index for compact Backtest result summaries."""

from __future__ import annotations

import copy
import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Callable

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, PBGUI_VERSION
from secure_files import ensure_private_directory


SERVICE = "BacktestResultIndex"
_DB_NAME = "backtest_results.sqlite"
_LOCK = threading.RLock()
_CONFIG_NAMES = ("config.json", "analysis_config.json", "config_used.json", "backtest_config.json")


def _db_path() -> Path:
    """Return the owner-only reconstructable index path."""
    data_root = Path(PBGDIR) / "data"
    cache_root = ensure_private_directory(data_root / "cache")
    return cache_root / _DB_NAME


def _connect() -> sqlite3.Connection:
    """Open and initialize one WAL connection."""
    path = _db_path()
    connection = sqlite3.connect(str(path), timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS result_summaries (
            version TEXT NOT NULL,
            result_path TEXT NOT NULL,
            signature TEXT NOT NULL,
            modified REAL NOT NULL,
            payload TEXT NOT NULL,
            PRIMARY KEY (version, result_path)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_result_summaries_version_modified "
        "ON result_summaries(version, modified DESC)"
    )
    connection.commit()
    for private_path in (path, path.with_name(f"{path.name}-wal"), path.with_name(f"{path.name}-shm")):
        try:
            if private_path.exists():
                os.chmod(private_path, 0o600)
        except OSError:
            pass
    return connection


def _file_signature(path: Path) -> tuple[int, int, int]:
    """Return a cheap identity tuple for one regular file."""
    try:
        stat_result = path.stat()
        if path.is_symlink() or not path.is_file():
            return (0, 0, 0)
        return (int(stat_result.st_ino or 0), int(stat_result.st_size), int(stat_result.st_mtime_ns))
    except OSError:
        return (0, 0, 0)


def result_signature(analysis_path: Path) -> str:
    """Fingerprint summary-bearing files without reading their contents."""
    result_dir = analysis_path.parent
    values = [["pbgui_version", PBGUI_VERSION], [analysis_path.name, *_file_signature(analysis_path)]]
    for name in _CONFIG_NAMES:
        candidate = result_dir / name
        signature = _file_signature(candidate)
        if signature != (0, 0, 0):
            values.append([name, *signature])
    return json.dumps(values, separators=(",", ":"))


def load_indexed_results(
    version: str,
    analysis_paths: list[Path],
    builder: Callable[[list[Path]], list[dict]],
    *,
    prune_missing: bool = False,
) -> list[dict]:
    """Return cached summaries, rebuilding only new or changed result paths."""
    normalized_version = str(version or "").strip().lower()
    if normalized_version not in {"v7", "v8"}:
        raise ValueError("Unsupported Backtest result index version")
    paths = [Path(path).resolve(strict=False) for path in analysis_paths]
    signatures = {str(path.parent): result_signature(path) for path in paths}
    try:
        with _LOCK:
            db_path = _db_path()
            with advisory_file_lock(db_path):
                connection = _connect()
                try:
                    cached = {
                        str(row[0]): {"signature": str(row[1]), "payload": str(row[2])}
                        for row in connection.execute(
                            "SELECT result_path, signature, payload FROM result_summaries WHERE version = ?",
                            (normalized_version,),
                        )
                    }
                    payloads: dict[str, dict] = {}
                    missing = []
                    for analysis_path in paths:
                        result_path = str(analysis_path.parent)
                        entry = cached.get(result_path)
                        payload = None
                        if entry and entry["signature"] == signatures[result_path]:
                            try:
                                payload = json.loads(entry["payload"])
                            except (json.JSONDecodeError, TypeError):
                                payload = None
                        if isinstance(payload, dict):
                            payloads[result_path] = payload
                        else:
                            missing.append(analysis_path)

                    missing_by_result = {str(path.parent): path for path in missing}
                    for payload in builder(missing) if missing else []:
                        if not isinstance(payload, dict) or not payload.get("path"):
                            continue
                        result_path = str(Path(str(payload["path"])).resolve(strict=False))
                        if result_path not in signatures:
                            continue
                        payloads[result_path] = payload
                        analysis_path = missing_by_result[result_path]
                        connection.execute(
                            """
                            INSERT INTO result_summaries(version, result_path, signature, modified, payload)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(version, result_path) DO UPDATE SET
                                signature = excluded.signature,
                                modified = excluded.modified,
                                payload = excluded.payload
                            """,
                            (
                                normalized_version,
                                result_path,
                                signatures[result_path],
                                analysis_path.stat().st_mtime,
                                json.dumps(payload, separators=(",", ":"), allow_nan=False),
                            ),
                        )
                    if prune_missing:
                        stale = [path for path in cached if path not in signatures]
                        if stale:
                            connection.executemany(
                                "DELETE FROM result_summaries WHERE version = ? AND result_path = ?",
                                [(normalized_version, path) for path in stale],
                            )
                    connection.commit()
                finally:
                    connection.close()
        return [copy.deepcopy(payloads[str(path.parent)]) for path in paths if str(path.parent) in payloads]
    except (OSError, OverflowError, RuntimeError, sqlite3.Error, ValueError) as exc:
        _log(SERVICE, f"Backtest result index unavailable; rebuilding summaries from files: {exc}", level="WARNING")
        return builder(paths)


def invalidate_result(version: str, result_path: Path) -> None:
    """Remove one result row after a managed delete or replacement."""
    try:
        with _LOCK:
            db_path = _db_path()
            with advisory_file_lock(db_path):
                connection = _connect()
                try:
                    connection.execute(
                        "DELETE FROM result_summaries WHERE version = ? AND result_path = ?",
                        (str(version).lower(), str(Path(result_path).resolve(strict=False))),
                    )
                    connection.commit()
                finally:
                    connection.close()
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        _log(SERVICE, f"Failed to invalidate Backtest result index row: {exc}", level="WARNING")
