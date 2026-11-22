"""Persistent reconstructable SQLite index for compact Backtest result summaries."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

from file_lock import advisory_file_lock
from backtest_result_metadata import _file_signature, cached_payloads, result_signature as metadata_result_signature
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, PBGUI_VERSION
from secure_files import ensure_private_directory


SERVICE = "BacktestResultIndex"
_DB_NAME = "backtest_results.sqlite"
_SUMMARY_SCHEMA_VERSION = 4
_LOCK = threading.RLock()
_BUILD_LOCKS = {version: threading.RLock() for version in ('v7', 'v8', 'v9')}
_BUILD_CHUNK_SIZE = 100
_CACHE_ERRORS = (OSError, OverflowError, RuntimeError, sqlite3.Error, ValueError)


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


def result_signature(analysis_path: Path, *, version: str = '') -> str:
    """Fingerprint through the shared pure implementation, preserving the cache contract."""
    return metadata_result_signature(analysis_path, version=version,
        gui_version=PBGUI_VERSION, summary_schema=_SUMMARY_SCHEMA_VERSION)


@contextmanager
def _cache_transaction():
    """Keep database ownership short; never run source builders under this lock."""
    with _LOCK:
        with advisory_file_lock(_db_path()):
            connection = _connect()
            try:
                with connection:
                    yield connection
            finally:
                connection.close()


def _cached_payloads(version, signatures):
    """Read only requested summaries, ignoring corrupt or outdated cache rows."""
    with _cache_transaction() as connection:
        return cached_payloads(connection, version, signatures)


@contextmanager
def _build_ownership(version, cancel_check):
    """Serialize cold reads per generation while keeping cancellation responsive."""
    lock = _BUILD_LOCKS[version]
    while not lock.acquire(timeout=0.1):
        if cancel_check:
            cancel_check()
    try:
        if cancel_check:
            cancel_check()
        yield
    finally:
        lock.release()


def load_indexed_results(
    version: str,
    analysis_paths: list[Path],
    builder: Callable[[list[Path]], list[dict]],
    *,
    prune_missing: bool = False,
    cancel_check: Callable[[], None] | None = None,
) -> list[dict]:
    """Build bounded missing chunks without blocking other generations' reads."""
    normalized_version = str(version or "").strip().lower()
    if normalized_version not in {"v7", "v8", "v9"}:
        raise ValueError("Unsupported Backtest result index version")
    paths = [Path(path).resolve(strict=False) for path in analysis_paths]
    signatures = {str(path.parent): result_signature(path, version=normalized_version) for path in paths}
    if cancel_check:
        cancel_check()
    cache_available = True
    try:
        payloads = _cached_payloads(normalized_version, signatures) if signatures else {}
    except _CACHE_ERRORS as exc:
        _log(SERVICE, f"Backtest result index unavailable; reading source summaries: {exc}", level="WARNING")
        payloads, cache_available = {}, False
    if len(payloads) < len(signatures) or prune_missing:
        with _build_ownership(normalized_version, cancel_check):
            # A preceding cold reader may have filled these rows while we waited.
            if cache_available:
                try:
                    payloads = _cached_payloads(normalized_version, signatures)
                except _CACHE_ERRORS as exc:
                    _log(SERVICE, f"Backtest result cache read failed: {exc}", level="WARNING")
                    cache_available = False
            missing = [path for path in paths if str(path.parent) not in payloads]
            for start in range(0, len(missing), _BUILD_CHUNK_SIZE):
                if cancel_check:
                    cancel_check()
                chunk = missing[start:start + _BUILD_CHUNK_SIZE]
                chunk_paths = {str(path.parent): path for path in chunk}
                # No global, file or SQLite lock is held while this reads artifacts.
                built = builder(chunk)
                if cancel_check:
                    cancel_check()
                pending = []
                for payload in built:
                    if not isinstance(payload, dict) or not payload.get('path'):
                        continue
                    result_path = str(Path(str(payload['path'])).resolve(strict=False))
                    path = chunk_paths.get(result_path)
                    if path is None or not path.is_file() or result_signature(path, version=normalized_version) != signatures[result_path]:
                        continue
                    payloads[result_path] = payload
                    try:
                        encoded = json.dumps(payload, separators=(',', ':'), allow_nan=False)
                    except (ValueError, TypeError):
                        # The index is optional; retain the owner's uncached result
                        # contract rather than failing all otherwise valid rows.
                        _log(SERVICE, 'Backtest summary is not JSON-cacheable; retaining source summary', level='WARNING')
                        continue
                    pending.append((result_path, path, payload, encoded))
                if cache_available and pending:
                    try:
                        with _cache_transaction() as connection:
                            for result_path, path, payload, encoded in pending:
                                if not path.is_file() or result_signature(path, version=normalized_version) != signatures[result_path]:
                                    payloads.pop(result_path, None)
                                    continue
                                connection.execute(
                                    'INSERT INTO result_summaries(version, result_path, signature, modified, payload) VALUES (?, ?, ?, ?, ?) '
                                    'ON CONFLICT(version, result_path) DO UPDATE SET signature=excluded.signature, '
                                    'modified=excluded.modified, payload=excluded.payload',
                                    (normalized_version, result_path, signatures[result_path], path.stat().st_mtime, encoded))
                    except _CACHE_ERRORS as exc:
                        _log(SERVICE, f"Backtest result cache write failed; retaining source summaries: {exc}", level="WARNING")
                        cache_available = False
            if cache_available and prune_missing:
                try:
                    with _cache_transaction() as connection:
                        stale = [str(row[0]) for row in connection.execute(
                            'SELECT result_path FROM result_summaries WHERE version = ?', (normalized_version,))
                            if str(row[0]) not in signatures]
                        connection.executemany('DELETE FROM result_summaries WHERE version = ? AND result_path = ?',
                                               [(normalized_version, path) for path in stale])
                except _CACHE_ERRORS as exc:
                    _log(SERVICE, f"Backtest result cache prune failed: {exc}", level="WARNING")
    if cancel_check:
        cancel_check()
    results = [copy.deepcopy(payloads[str(path.parent)]) for path in paths if str(path.parent) in payloads]
    # Match the AI capability registry's non-reversible managed result identity.
    # Enrich warm and fallback reads without rebuilding the persistent index.
    for item in results:
        identity = str(item.get('path') or '')
        if identity:
            opaque = hashlib.sha256(f'backtest\0{normalized_version}\0{identity}'.encode('utf-8')).hexdigest()[:32]
            item['resource'] = f'pbgui://backtest/{normalized_version}/{opaque}'
    return results


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
