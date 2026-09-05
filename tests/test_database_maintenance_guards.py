"""Exercise actual Database/PBData admission bodies without importing application startup."""

import ast
import sqlite3
import threading
from datetime import datetime
from functools import wraps
from pathlib import Path
from unittest.mock import Mock

import pytest

from database_lock import DatabaseBusyError, acquire_database_lock
from secure_files import atomic_write_private_text


@pytest.fixture
def guarded_classes(tmp_path):
    """Compile real guarded Database and PBData entrypoints with an isolated root."""
    root = Path(__file__).resolve().parents[1]
    tree = ast.parse((root / "Database.py").read_text())
    body = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef))
            and node.name in {"Database", "_guard_database_writes"}]
    namespace = {"Path": Path, "PBGDIR": tmp_path, "wraps": wraps, "threading": threading,
                 "sqlite3": sqlite3, "datetime": datetime, "_human_log": Mock(), "SERVICE": "Database",
                 "acquire_database_lock": acquire_database_lock, "DatabaseBusyError": DatabaseBusyError,
                 "Exchange": Mock(side_effect=AssertionError("No network factory may run"))}
    future = ast.parse("from __future__ import annotations").body
    exec(compile(ast.Module(body=future + body, type_ignores=[]), "Database.py", "exec"), namespace)
    tree = ast.parse((root / "PBData.py").read_text())
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "PBData")
    cls.body = [node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name in {"__init__", "run"}]
    exec(compile(ast.Module(body=future + [cls], type_ignores=[]), "PBData.py", "exec"), namespace)
    return namespace


@pytest.mark.parametrize("method,args", [
    ("update_executions", (object(),)), ("update_positions", (object(),)),
    ("update_orders", (object(),)), ("update_balances", (object(),)),
    ("update_prices", (object(),)), ("batch_upsert_prices", ([("alice", "BTCUSDT", 1, 2)],)),
    ("set_last_scan_ts", ("alice", "mock", 1)), ("delete_income_by_ids", ([1],)),
    ("add_history", (None, [])), ("create_tables", ()), ("create_trades_tables", ()),
])
def test_every_writer_defers_before_db_or_network(tmp_path, guarded_classes, method, args):
    """Admission precedes low-level writes and high-level scan/fetch work alike."""
    cls = guarded_classes["Database"]
    db = cls.__new__(cls)
    with acquire_database_lock(tmp_path, exclusive=True):
        with pytest.raises(DatabaseBusyError):
            getattr(db, method)(*args)
    guarded_classes["Exchange"].assert_not_called()
    assert not (tmp_path / "data" / "pbgui.db").exists()


def test_shared_default_and_cached_connection_lifetime_are_unchanged(tmp_path, guarded_classes):
    """Ordinary SH writers coexist; a cached connection owns no idle maintenance lease."""
    db = guarded_classes["Database"]()
    try:
        cached = db._connect()
        with acquire_database_lock(tmp_path):
            db.batch_upsert_prices([("alice", "BTCUSDT", 1, 2)])
        assert db._connect() is cached
        assert cached.execute("SELECT price FROM prices").fetchone() == (2,)
        with acquire_database_lock(tmp_path, exclusive=True):
            with pytest.raises(DatabaseBusyError):
                db.batch_upsert_prices([("alice", "BTCUSDT", 2, 3)])
        assert cached.execute("SELECT price FROM prices").fetchone() == (2,)
    finally:
        db.close_thread_connections()


@pytest.mark.parametrize("entry", ["Database", "PBData", "PBData.run"])
def test_startup_fails_closed_on_invalid_recovery_journal(tmp_path, guarded_classes, entry):
    """Even a malformed purportedly consistent record must not admit startup/writes."""
    journal = tmp_path / "data" / "locks" / "db-tools-recovery.json"
    atomic_write_private_text(journal, '{"version": 1, "phase": "consistent"}')
    cls = guarded_classes[entry.split(".")[0]]
    with pytest.raises(DatabaseBusyError, match="Invalid DB Tools recovery journal"):
        if entry.endswith(".run"):
            cls.run(cls.__new__(cls))
        else:
            cls()
    assert not (tmp_path / "data" / "pbgui.db").exists()
