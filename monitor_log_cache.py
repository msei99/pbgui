"""Persistent incremental log summaries for the VPS instance collector.

Only numeric events and hashed file guards are retained, never log text.
SQLite transactions and a reentrant process lock cover each collection.
"""

from __future__ import annotations

from contextlib import closing
from datetime import datetime
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import time

from file_lock import advisory_file_lock
from secure_files import ensure_private_directory, secure_private_file

SERVICE = "MonitorLogCache"
WINDOW_HOURS = 24 * 28
MAX_LINE_BYTES = 1024 * 1024
SUMMARY_RE = re.compile(r"\[fill\]\s+(\d+)\s+fills,\s+pnl=([+-]?(?:\d+\.?\d*|\d*\.\d+))\s+\w+")
PNL_RE = re.compile(r"\bpnl=([+-]?(?:\d+\.?\d*|\d*\.\d+))\b")
FEE_RE = re.compile(r"\bfee=([+-]?(?:\d+\.?\d*|\d*\.\d+))\b")
EVENT_RE = re.compile(r"\[fill\]\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z\b")


def _timestamp(value: str) -> int | None:
    """Parse the canonical UTC timestamp without expensive strptime calls."""
    try:
        if len(value) != 20 or value[19] != "Z":
            return None
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def _guard(handle, offset: int) -> tuple[str, str]:
    """Fingerprint bounded ranges at the beginning and last consumed byte."""
    handle.seek(0)
    prefix = hashlib.sha256(handle.read(min(256, offset))).hexdigest()
    handle.seek(max(0, offset - 64))
    tail = hashlib.sha256(handle.read(min(64, offset))).hexdigest()
    return prefix, tail


class LogHistoryCache:
    """Cache one host's log summaries across collector processes and restarts."""

    def __init__(self, path: Path, approved_roots: list[Path]) -> None:
        """Bind cache storage and the only directories containing readable logs."""
        self.path = Path(path)
        self.roots = tuple(Path(root).resolve() for root in approved_roots)

    def _files(self, paths: list[str]) -> list[tuple[Path, object, str]]:
        """Validate paths and collapse rotated/hardlinked/symlink aliases."""
        result, ordering = {}, {}
        for raw in paths:
            path = Path(raw).resolve()
            if not any(path.is_relative_to(root) and path != root for root in self.roots):
                raise ValueError("Monitor log path is outside approved roots")
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            if not path.is_file():
                raise ValueError("Monitor log path is not a regular file")
            identity = f"{stat.st_dev}:{stat.st_ino}"
            if identity not in result:
                result[identity] = (path, stat, identity)
                ordering[identity] = str(raw)
        return sorted(result.values(), key=lambda row: (row[1].st_mtime_ns, ordering[row[2]]))

    @staticmethod
    def _schema(db: sqlite3.Connection) -> None:
        """Create cache tables; SQLite commits all offsets and summaries together."""
        db.executescript("""
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY, size INTEGER, mtime INTEGER, offset INTEGER,
                prefix TEXT, tail TEXT, last_fill INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS hours (
                file_id TEXT REFERENCES files(id) ON DELETE CASCADE,
                hour INTEGER, errors INTEGER, tracebacks INTEGER,
                PRIMARY KEY (file_id, hour)
            );
            CREATE TABLE IF NOT EXISTS events (
                file_id TEXT REFERENCES files(id) ON DELETE CASCADE,
                pos INTEGER, ts INTEGER, kind TEXT, fills INTEGER, pnl REAL,
                PRIMARY KEY (file_id, pos)
            );
            CREATE INDEX IF NOT EXISTS events_time ON events(file_id, kind, ts, pos);
            CREATE TABLE IF NOT EXISTS scopes (
                scope TEXT, file_id TEXT REFERENCES files(id) ON DELETE CASCADE,
                seen INTEGER, PRIMARY KEY (scope, file_id)
            );
        """)

    def _sync(self, db, path, stat, identity: str, from_hour: int) -> None:
        """Read only appended complete lines, rebuilding changed file identities."""
        old = db.execute("SELECT size,mtime,offset,prefix,tail,last_fill FROM files WHERE id=?", (identity,)).fetchone()
        if old and old[0] == stat.st_size and old[1] == stat.st_mtime_ns:
            return
        with path.open("rb") as handle:
            # Revalidate the opened inode so a concurrent rename cannot mix files.
            opened = os.fstat(handle.fileno())
            if (opened.st_dev, opened.st_ino) != (stat.st_dev, stat.st_ino):
                raise OSError("Monitor log rotated during collection; retry next cycle")
            offset = old[2] if old else 0
            reset = bool(old and (stat.st_size < offset or stat.st_size <= old[0]))
            if old and not reset:
                reset = _guard(handle, offset) != (old[3], old[4])
            if reset:
                db.execute("DELETE FROM files WHERE id=?", (identity,))
                old, offset = None, 0
            if not old:
                db.execute("INSERT INTO files(id,size,mtime,offset,prefix,tail) VALUES(?,0,0,0,'','')", (identity,))
            last_fill = old[5] if old else 0
            handle.seek(offset)
            while offset < stat.st_size:
                line = handle.readline(min(MAX_LINE_BYTES, stat.st_size - offset))
                if not line.endswith(b"\n"):
                    if len(line) >= MAX_LINE_BYTES:
                        raise ValueError("Monitor log line exceeds supported size")
                    break  # An in-progress write is retried when the line is complete.
                pos = offset
                offset += len(line)
                if not any(marker in line for marker in (b" ERROR ", b"Traceback", b"[fill]")):
                    continue
                text = line.decode("utf-8", errors="ignore")
                ts = _timestamp(text[:20])
                if ts is None:
                    continue
                hour = ts // 3600
                if hour >= from_hour:
                    db.execute("""INSERT INTO hours VALUES(?,?,?,?)
                        ON CONFLICT(file_id,hour) DO UPDATE SET
                        errors=errors+excluded.errors, tracebacks=tracebacks+excluded.tracebacks""",
                        (identity, hour, int(" ERROR " in text), int("Traceback" in text)))
                if "[fill]" not in text:
                    continue
                summary = SUMMARY_RE.search(text)
                if summary:
                    db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)", (identity, pos, ts, "summary", int(summary[1]), float(summary[2])))
                else:
                    event = EVENT_RE.search(text)
                    event_ts = _timestamp(event[1] + "Z") if event else None
                    if event_ts is None:
                        continue
                    last_fill = max(last_fill, event_ts)
                    pnl, fee = PNL_RE.search(text), FEE_RE.search(text)
                    net = (float(pnl[1]) if pnl else 0.0) + (float(fee[1]) if fee else 0.0)
                    db.execute("INSERT INTO events VALUES(?,?,?,?,?,?)", (identity, pos, ts, "fill", 1, net))
            prefix, tail = _guard(handle, offset)
            db.execute("UPDATE files SET size=?,mtime=?,offset=?,prefix=?,tail=?,last_fill=? WHERE id=?",
                       (stat.st_size, stat.st_mtime_ns, offset, prefix, tail, last_fill, identity))
        # A file's latest summary supersedes all its earlier PNL events.
        latest = db.execute("SELECT ts,pos FROM events WHERE file_id=? AND kind='summary' ORDER BY ts DESC,pos DESC LIMIT 1", (identity,)).fetchone()
        if latest:
            db.execute("DELETE FROM events WHERE file_id=? AND (ts<? OR (ts=? AND pos<?))", (identity, latest[0], latest[0], latest[1]))

    def collect(self, scope: str, native_files: list[str], traceback_files: list[str],
                from_hour: int, to_hour: int) -> dict:
        """Return exact hourly counts and net PNL while updating the durable cache."""
        if to_hour < from_hour or to_hour - from_hour >= WINDOW_HOURS:
            raise ValueError("Invalid monitor history window")
        native, stderr = self._files(native_files), self._files(traceback_files)
        ensure_private_directory(self.path.parent)
        with advisory_file_lock(self.path), closing(sqlite3.connect(self.path, timeout=30)) as db:
            secure_private_file(self.path)
            db.execute("PRAGMA foreign_keys=ON")
            self._schema(db)
            with db:
                now = int(time.time())
                db.execute("DELETE FROM scopes WHERE scope=? OR seen<?", (scope, now - WINDOW_HOURS * 3600))
                for path, stat, identity in {row[2]: row for row in native + stderr}.values():
                    self._sync(db, path, stat, identity, from_hour)
                    db.execute("INSERT INTO scopes VALUES(?,?,?)", (scope, identity, now))
                db.execute("DELETE FROM files WHERE id NOT IN (SELECT file_id FROM scopes)")
                db.execute("DELETE FROM hours WHERE hour<?", (from_hour,))
                errors, tracebacks = {}, {}
                for files, column, result in ((native, "errors", errors), (stderr, "tracebacks", tracebacks)):
                    for _, _, identity in files:
                        for hour, count in db.execute(f"SELECT hour,{column} FROM hours WHERE file_id=? AND hour BETWEEN ? AND ? AND {column}>0", (identity, from_hour, to_hour)):
                            result[str(hour)] = result.get(str(hour), 0) + count
                pnl = self._pnl(db, native)
                return {"from_hour": from_hour, "to_hour": to_hour,
                        "errors": errors, "tracebacks": tracebacks, "pnl": pnl}

    @staticmethod
    def _pnl(db, native) -> dict | None:
        """Combine cached per-file summaries using the original log ordering."""
        latest, last_fill = None, 0
        for rank, (_, _, identity) in enumerate(native):
            row = db.execute("SELECT ts,pos,fills,pnl FROM events WHERE file_id=? AND kind='summary' ORDER BY ts DESC,pos DESC LIMIT 1", (identity,)).fetchone()
            if row:
                candidate = ((row[0], rank, row[1]), row[2], row[3])
                if latest is None or candidate[0] > latest[0]:
                    latest = candidate
            last_fill = max(last_fill, db.execute("SELECT last_fill FROM files WHERE id=?", (identity,)).fetchone()[0])
        if latest is None:
            return None
        (ts, summary_rank, pos), fills, pnl = latest
        for rank, (_, _, identity) in enumerate(native):
            # At equal timestamps file ordering precedes line ordering.
            cutoff = pos if rank == summary_rank else (-1 if rank > summary_rank else 2**63 - 1)
            count, net = db.execute("""SELECT COUNT(*),COALESCE(SUM(pnl),0) FROM events
                WHERE file_id=? AND kind='fill' AND (ts>? OR (ts=? AND pos>?))""", (identity, ts, ts, cutoff)).fetchone()
            fills += count
            pnl += net
        return {"total_pnl": pnl, "total_fills": fills, "last_fill_ts": last_fill,
                "source": "PB8 fill batch summary"}
