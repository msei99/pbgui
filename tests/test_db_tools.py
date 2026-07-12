"""Tests for DB tools SQLite cleanup and copy helpers."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace
from pathlib import Path

import pytest

import Database as database_mod
from PBData import PBData
from api import db_tools
from master import async_pool as async_pool_mod
import task_worker


def _bundle(tmp_path: Path, name: str) -> dict[str, Path]:
    """Create an empty DB tools bundle with the production schemas."""

    paths = {
        db_tools.MAIN_DB_NAME: tmp_path / f"{name}-pbgui.db",
        db_tools.TRADES_DB_NAME: tmp_path / f"{name}-pbgui_trades.db",
    }
    for db_name, path in paths.items():
        db_tools._ensure_schema(path, db_name)
    return paths


def _insert_sample(paths: dict[str, Path], user: str, prefix: str, ts: int) -> None:
    """Insert one row in each user-owned table for a user."""

    with sqlite3.connect(paths[db_tools.MAIN_DB_NAME]) as conn:
        conn.execute(
            "INSERT INTO history(symbol,timestamp,income,uniqueid,user) VALUES(?,?,?,?,?)",
            ("BTCUSDT", ts, 1.0, f"hist-{prefix}", user),
        )
        conn.execute(
            "INSERT INTO position(symbol,timestamp,psize,upnl,entry,user,side) VALUES(?,?,?,?,?,?,?)",
            ("BTCUSDT", ts, 0.1, 2.0, 100.0, user, "long"),
        )
        conn.execute(
            "INSERT INTO orders(symbol,timestamp,amount,price,side,uniqueid,user) VALUES(?,?,?,?,?,?,?)",
            ("BTCUSDT", ts, 0.1, 100.0, "buy", f"order-{prefix}", user),
        )
        conn.execute(
            "INSERT INTO prices(symbol,timestamp,price,user) VALUES(?,?,?,?)",
            ("BTCUSDT", ts, 100.0, user),
        )
        conn.execute(
            "INSERT OR REPLACE INTO balances(timestamp,balance,user) VALUES(?,?,?)",
            (ts, 1000.0, user),
        )
        conn.execute(
            "INSERT OR REPLACE INTO history_scan_meta(user,exchange,last_scan_ts) VALUES(?,?,?)",
            (user, "binance", ts),
        )
        conn.commit()
    with sqlite3.connect(paths[db_tools.TRADES_DB_NAME]) as conn:
        conn.execute(
            "INSERT INTO executions(exchange,symbol,timestamp,side,price,qty,fee,realized_pnl,order_id,trade_id,user,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            ("binance", "BTCUSDT", ts, "buy", 100.0, 0.1, 0.01, 1.0, f"oid-{prefix}", f"trade-{prefix}", user, "{}"),
        )
        conn.commit()


def test_delete_user_rows_removes_all_user_tables(tmp_path: Path) -> None:
    """Deleting a user removes rows from every handled table only for that user."""

    paths = _bundle(tmp_path, "delete-all")
    _insert_sample(paths, "alice", "alice", 1000)
    _insert_sample(paths, "bob", "bob", 1000)

    result = db_tools.delete_user_rows(paths, ["alice"])

    assert result["total"] == 7
    assert db_tools.count_user_rows(paths, ["alice"])["total"] == 0
    assert db_tools.count_user_rows(paths, ["bob"])["total"] == 7


def test_delete_user_rows_older_than_uses_timestamp_columns(tmp_path: Path) -> None:
    """Older-than cleanup keeps newer rows for the selected user."""

    paths = _bundle(tmp_path, "delete-older")
    _insert_sample(paths, "alice", "old", 1000)
    _insert_sample(paths, "alice", "new", 3000)

    result = db_tools.delete_user_rows(paths, ["alice"], cutoff_ms=1500)

    assert result["total"] == 5
    assert db_tools.count_user_rows(paths, ["alice"])["total"] == 7


def test_copy_user_rows_add_missing_keeps_existing_target_rows(tmp_path: Path) -> None:
    """Add-missing mode imports absent rows without duplicating existing keys."""

    source = _bundle(tmp_path, "source-add")
    target = _bundle(tmp_path, "target-add")
    _insert_sample(source, "alice", "same", 1000)
    _insert_sample(source, "alice", "new", 2000)
    _insert_sample(target, "alice", "same", 1000)

    result = db_tools.copy_user_rows(source, target, ["alice"], "add_missing")

    counts = db_tools.count_user_rows(target, ["alice"])

    assert result["inserted"] == 4
    assert result["skipped"] == 8
    assert counts["tables"]["pbgui.db:history"] == 2
    assert counts["tables"]["pbgui.db:balances"] == 1
    assert counts["tables"]["pbgui_trades.db:executions"] == 2


def test_copy_user_rows_replace_replaces_target_user_data(tmp_path: Path) -> None:
    """Replace mode deletes the target user first, then imports source rows."""

    source = _bundle(tmp_path, "source-replace")
    target = _bundle(tmp_path, "target-replace")
    _insert_sample(source, "alice", "source", 2000)
    _insert_sample(target, "alice", "target", 1000)
    _insert_sample(target, "bob", "bob", 1000)

    result = db_tools.copy_user_rows(source, target, ["alice"], "replace")

    alice_counts = db_tools.count_user_rows(target, ["alice"])
    bob_counts = db_tools.count_user_rows(target, ["bob"])

    assert result["inserted"] == 7
    assert result["skipped"] == 0
    assert alice_counts["total"] == 7
    assert bob_counts["total"] == 7


def test_incremental_sync_helpers_copy_append_rows_and_replace_state(tmp_path: Path) -> None:
    """Sync helpers copy append rows with overlap and replace small state tables."""

    source = _bundle(tmp_path, "sync-source")
    target = _bundle(tmp_path, "sync-target")
    _insert_sample(source, "alice", "same", 1000)
    _insert_sample(source, "alice", "new", 2000)
    _insert_sample(target, "alice", "same", 1000)

    history_spec = next(spec for spec in db_tools.TABLE_SPECS if spec.table == "history")
    execution_spec = next(spec for spec in db_tools.TABLE_SPECS if spec.table == "executions")
    balance_spec = next(spec for spec in db_tools.TABLE_SPECS if spec.table == "balances")

    history_payload = db_tools._fetch_sync_rows_from_paths(source, history_spec, ["alice"], "append", {"alice": 0})
    history_stats = db_tools._apply_sync_rows_to_paths(target, history_spec, ["alice"], "append", history_payload)
    execution_payload = db_tools._fetch_sync_rows_from_paths(source, execution_spec, ["alice"], "append", {"alice": 0})
    execution_stats = db_tools._apply_sync_rows_to_paths(target, execution_spec, ["alice"], "append", execution_payload)
    balance_payload = db_tools._fetch_sync_rows_from_paths(source, balance_spec, ["alice"], "state")
    balance_stats = db_tools._apply_sync_rows_to_paths(target, balance_spec, ["alice"], "state", balance_payload)

    assert history_stats == {"fetched": 2, "inserted": 1, "skipped": 1, "deleted": 0}
    assert execution_stats == {"fetched": 2, "inserted": 1, "skipped": 1, "deleted": 0}
    assert balance_stats == {"fetched": 1, "inserted": 1, "skipped": 0, "deleted": 1}
    with sqlite3.connect(target[db_tools.MAIN_DB_NAME]) as conn:
        assert conn.execute("SELECT COUNT(*) FROM history WHERE user='alice'").fetchone()[0] == 2
        assert conn.execute("SELECT timestamp, balance FROM balances WHERE user='alice'").fetchone() == (2000, 1000.0)
    with sqlite3.connect(target[db_tools.TRADES_DB_NAME]) as conn:
        assert conn.execute("SELECT COUNT(*) FROM executions WHERE user='alice'").fetchone()[0] == 2


def test_write_json_item_honors_add_missing_and_replace_modes(tmp_path: Path, monkeypatch) -> None:
    """Dashboard/template writes skip existing files unless overwrite is requested."""

    monkeypatch.setattr(db_tools, "PBGDIR", str(tmp_path))

    created = asyncio.run(db_tools._write_json_item("local", "main", False, b'{"rows":1}', False, "test"))
    skipped = asyncio.run(db_tools._write_json_item("local", "main", False, b'{"rows":2}', False, "test"))
    replaced = asyncio.run(db_tools._write_json_item("local", "main", False, b'{"rows":3}', True, "test"))
    template_created = asyncio.run(db_tools._write_json_item("local", "tpl", True, b'{"cols":1}', False, "test"))

    dashboard_path = tmp_path / "data" / "dashboards" / "main.json"
    template_path = tmp_path / "data" / "dashboards" / "templates" / "tpl.json"
    assert created == "created"
    assert skipped == "skipped"
    assert replaced == "replaced"
    assert template_created == "created"
    assert dashboard_path.read_text(encoding="utf-8") == '{"rows":3}'
    assert template_path.read_text(encoding="utf-8") == '{"cols":1}'


def test_sqlite_backup_file_captures_wal_content(tmp_path: Path) -> None:
    """SQLite backup copies a WAL-mode database consistently."""

    src = tmp_path / "source.db"
    dst = tmp_path / "copy.db"
    with sqlite3.connect(src) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items(name) VALUES('wal-row')")
        conn.commit()
        assert Path(str(src) + "-wal").exists()
        db_tools._sqlite_backup_file(src, dst)

    with sqlite3.connect(dst) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 1


def test_remove_sqlite_sidecars_removes_wal_and_shm(tmp_path: Path) -> None:
    """Replacing a DB removes stale WAL/SHM sidecar files."""

    db_path = tmp_path / "pbgui.db"
    Path(str(db_path) + "-wal").write_text("wal", encoding="utf-8")
    Path(str(db_path) + "-shm").write_text("shm", encoding="utf-8")

    db_tools._remove_sqlite_sidecars(db_path)

    assert not Path(str(db_path) + "-wal").exists()
    assert not Path(str(db_path) + "-shm").exists()


def test_list_backup_files_returns_local_db_backups(tmp_path: Path, monkeypatch) -> None:
    """Local backup listing returns only direct DB backup files."""

    monkeypatch.setattr(db_tools, "PBGDIR", str(tmp_path))
    backup_dir = tmp_path / "data" / "backup" / "db-tools"
    backup_dir.mkdir(parents=True)
    main_backup = backup_dir / "db-tools-20260612-120000-cleanup-pbgui.db"
    trades_backup = backup_dir / "db-tools-20260612-120000-cleanup-pbgui_trades.db"
    ignored = backup_dir / "notes.txt"
    main_backup.write_bytes(b"main")
    trades_backup.write_bytes(b"trades")
    ignored.write_text("ignore", encoding="utf-8")

    backups = asyncio.run(db_tools._list_backup_files("local"))

    assert {item["name"] for item in backups} == {main_backup.name, trades_backup.name}
    assert {item["db_name"] for item in backups} == {db_tools.MAIN_DB_NAME, db_tools.TRADES_DB_NAME}


def test_delete_backup_files_deletes_only_selected_local_backups(tmp_path: Path, monkeypatch) -> None:
    """Deleting backups removes selected files from the local DB tools backup folder."""

    monkeypatch.setattr(db_tools, "PBGDIR", str(tmp_path))
    backup_dir = tmp_path / "data" / "backup" / "db-tools"
    backup_dir.mkdir(parents=True)
    selected = backup_dir / "db-tools-20260612-120000-cleanup-pbgui.db"
    kept = backup_dir / "db-tools-20260612-120000-cleanup-pbgui_trades.db"
    selected.write_bytes(b"main")
    kept.write_bytes(b"trades")

    deleted = asyncio.run(db_tools._delete_backup_files("local", [selected.name]))

    assert deleted == [selected.name]
    assert not selected.exists()
    assert kept.exists()


def test_stage_backup_files_rejects_duplicate_db_restore(tmp_path: Path, monkeypatch) -> None:
    """Restore staging allows only one backup per destination DB file."""

    monkeypatch.setattr(db_tools, "PBGDIR", str(tmp_path))
    backup_dir = tmp_path / "data" / "backup" / "db-tools"
    backup_dir.mkdir(parents=True)
    first = backup_dir / "db-tools-20260612-120000-cleanup-pbgui.db"
    second = backup_dir / "db-tools-20260612-120100-copy-users-pbgui.db"
    _bundle(backup_dir, "first")[db_tools.MAIN_DB_NAME].replace(first)
    _bundle(backup_dir, "second")[db_tools.MAIN_DB_NAME].replace(second)
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()

    try:
        asyncio.run(db_tools._stage_backup_files("local", [first.name, second.name], stage_dir))
    except Exception as exc:
        assert "Only one backup" in str(exc)
    else:
        raise AssertionError("duplicate DB restore was not rejected")


def test_validate_sync_job_payload_normalizes_and_rejects_source_target() -> None:
    """Sync job payload validation deduplicates lists and rejects source-as-target."""

    payload = db_tools.SyncJobRequest(
        name="",
        source="local",
        targets=["remote-a", "remote-a"],
        users=["alice", "alice"],
        interval_seconds=10,
        enabled=True,
    )

    job = db_tools._validate_sync_job_payload(payload)

    assert job["name"] == "local sync"
    assert job["targets"] == ["remote-a"]
    assert job["users"] == ["alice"]
    assert job["interval_seconds"] == 30

    try:
        db_tools._validate_sync_job_payload(db_tools.SyncJobRequest(source="local", targets=["local"], users=["alice"]))
    except Exception as exc:
        assert "Source cannot also be" in str(exc)
    else:
        raise AssertionError("source-as-target sync job was not rejected")


def test_sync_jobs_persist_atomically(tmp_path: Path, monkeypatch) -> None:
    """Sync jobs are saved to and loaded from the configured data directory."""

    monkeypatch.setattr(db_tools, "PBGDIR", str(tmp_path))
    original = dict(db_tools._sync_jobs)
    try:
        db_tools._sync_jobs.clear()
        db_tools._sync_jobs["job1"] = {"id": "job1", "name": "Test Job"}

        db_tools._save_sync_jobs()
        db_tools._sync_jobs.clear()
        db_tools._load_sync_jobs()

        assert db_tools._sync_jobs == {"job1": {"id": "job1", "name": "Test Job"}}
        assert (tmp_path / "data" / "db_sync" / "jobs.json").exists()
    finally:
        db_tools._sync_jobs.clear()
        db_tools._sync_jobs.update(original)


def test_sync_job_events_use_standard_job_log_path(tmp_path: Path, monkeypatch) -> None:
    """Sync job events are written through standard logging to a data/logs job file."""

    calls: list[tuple[str, str, str, str]] = []

    def fake_log(service: str, message: str, level: str = "INFO", **kwargs) -> None:
        calls.append((service, message, level, str(kwargs.get("logfile") or "")))

    monkeypatch.setattr(db_tools, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(db_tools, "_log", fake_log)
    monkeypatch.setitem(db_tools._sync_jobs, "job1", {"name": "manibot01_local"})

    db_tools._log_sync_job("job1", "run_start", source="local", targets=["remote-a"])
    db_tools._log_sync_job("job1", "run_error", error="failed")

    expected_log = str(tmp_path / "data" / "logs" / "jobs" / "db-tools-sync-manibot01_local.log")
    assert calls[0] == (
        "DbTools",
        'Sync job "manibot01_local" started (scheduled): source local -> remote-a; users none.',
        "INFO",
        expected_log,
    )
    assert calls[1] == (
        "DbTools",
        'Sync job "manibot01_local" failed: failed.',
        "WARNING",
        expected_log,
    )
    assert (tmp_path / "data" / "logs" / "jobs").exists()


def test_validate_sync_job_safety_blocks_active_pbdata_target(monkeypatch) -> None:
    """Sync safety blocks targets where PBData is actively writing the selected user."""

    async def fake_assert_known(target: str) -> None:
        return None

    async def fake_active_users(target: str) -> dict[str, object]:
        return {"running": True, "active_users": ["alice"]}

    monkeypatch.setattr(db_tools, "_assert_known_target", fake_assert_known)
    monkeypatch.setattr(db_tools, "_pbdata_active_users", fake_active_users)
    job = {"id": "job1", "source": "source", "targets": ["target"], "users": ["alice"], "enabled": True}

    try:
        asyncio.run(db_tools._validate_sync_job_safety(job))
    except Exception as exc:
        assert "PBData is active" in str(exc)
    else:
        raise AssertionError("active PBData target user was not blocked")


def test_validate_sync_job_safety_allows_same_source_for_enabled_user(monkeypatch) -> None:
    """Enabled jobs may share users only when they use the same source master."""

    async def fake_assert_known(target: str) -> None:
        return None

    async def fake_active_users(target: str) -> dict[str, object]:
        return {"running": False, "active_users": []}

    monkeypatch.setattr(db_tools, "_assert_known_target", fake_assert_known)
    monkeypatch.setattr(db_tools, "_pbdata_active_users", fake_active_users)
    original = dict(db_tools._sync_jobs)
    try:
        db_tools._sync_jobs.clear()
        db_tools._sync_jobs["job1"] = {"id": "job1", "source": "source", "users": ["alice"], "enabled": True}
        asyncio.run(db_tools._validate_sync_job_safety({"id": "job2", "source": "source", "targets": ["target"], "users": ["alice"], "enabled": True}))
        try:
            asyncio.run(db_tools._validate_sync_job_safety({"id": "job3", "source": "other", "targets": ["target"], "users": ["alice"], "enabled": True}))
        except Exception as exc:
            assert "another sync source" in str(exc)
        else:
            raise AssertionError("conflicting sync source was not rejected")
    finally:
        db_tools._sync_jobs.clear()
        db_tools._sync_jobs.update(original)


def test_run_sync_job_does_not_create_backups(tmp_path: Path, monkeypatch) -> None:
    """Sync jobs use SQL sync directly and never call the DB backup helper."""

    async def fake_validate(job: dict[str, object]) -> None:
        return None

    async def fake_sync(source: str, target: str, users: list[str], temp_dir: Path, operation=None) -> dict[str, object]:
        assert source == "remote-a"
        assert target == "local"
        assert users == ["alice"]
        assert temp_dir.exists()
        return {
            "source_total": 1,
            "fetched": 1,
            "inserted": 1,
            "skipped": 0,
            "deleted": 0,
            "tables": {},
            "verify": {"ok": True, "tables": {}},
        }

    async def fail_backup(*args, **kwargs):
        raise AssertionError("sync must not create backups")

    original = dict(db_tools._sync_jobs)
    try:
        db_tools._sync_jobs.clear()
        db_tools._sync_jobs["job1"] = {
            "id": "job1",
            "name": "remote_local",
            "source": "remote-a",
            "targets": ["local"],
            "users": ["alice"],
            "interval_seconds": 60,
        }
        monkeypatch.setattr(db_tools, "PBGDIR", str(tmp_path))
        monkeypatch.setattr(db_tools, "_save_sync_jobs", lambda: None)
        monkeypatch.setattr(db_tools, "_validate_sync_job_safety", fake_validate)
        monkeypatch.setattr(db_tools, "sync_user_rows_incremental", fake_sync)
        monkeypatch.setattr(db_tools, "_backup_target_dbs", fail_backup)
        monkeypatch.setattr(db_tools, "_log_sync_job", lambda *args, **kwargs: None)
        monkeypatch.setattr(db_tools, "_log", lambda *args, **kwargs: None)

        result = asyncio.run(db_tools._run_sync_job("job1", manual=True))

        assert result["ok"] is True
        assert "backups" not in result
        assert result["results"]["local"]["verify"]["ok"] is True
    finally:
        db_tools._sync_jobs.clear()
        db_tools._sync_jobs.update(original)
        db_tools._sync_job_locks.discard("job1")


def test_local_pbdata_stop_start_prefers_systemd(monkeypatch) -> None:
    """Local PBData restart helpers use systemd when the PBData user service is active."""

    calls: list[tuple[str, ...]] = []

    def fake_run(args, **kwargs):
        del kwargs
        calls.append(tuple(args))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(db_tools.subprocess, "run", fake_run)

    marker = db_tools._pbdata_stop_local()
    db_tools._pbdata_start_local(marker)

    assert marker == "systemd"
    assert calls == [
        ("systemctl", "--user", "is-active", "--quiet", "pbgui-pbdata.service"),
        ("systemctl", "--user", "stop", "pbgui-pbdata.service"),
        ("systemctl", "--user", "start", "pbgui-pbdata.service"),
    ]


def test_start_target_pbdata_preserves_local_marker(monkeypatch) -> None:
    """Target PBData restart passes the original local stop marker through."""

    markers: list[str] = []
    operation = SimpleNamespace(set_current=lambda label: None, advance=lambda label, detail: None)

    monkeypatch.setattr(db_tools, "_pbdata_start_local", lambda marker: markers.append(marker))

    asyncio.run(db_tools._start_target_pbdata("local", "systemd", operation))

    assert markers == ["systemd"]


def test_remote_list_users_script_returns_counts(tmp_path: Path) -> None:
    """Remote user listing helper returns compact JSON counts without copying DB files."""

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paths = {
        db_tools.MAIN_DB_NAME: data_dir / db_tools.MAIN_DB_NAME,
        db_tools.TRADES_DB_NAME: data_dir / db_tools.TRADES_DB_NAME,
    }
    for db_name, path in paths.items():
        db_tools._ensure_schema(path, db_name)
    _insert_sample(paths, "alice", "alice", 1000)
    _insert_sample(paths, "bob", "bob", 1000)

    result = subprocess.run(
        [sys.executable, "-c", db_tools._REMOTE_LIST_USERS_SCRIPT, db_tools._table_specs_json()],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])

    assert {item["user"] for item in payload["users"]} == {"alice", "bob"}
    assert {item["total"] for item in payload["users"]} == {7}


def test_remote_count_users_script_honors_cutoff(tmp_path: Path) -> None:
    """Remote count helper applies selected users and timestamp cutoff locally."""

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paths = {
        db_tools.MAIN_DB_NAME: data_dir / db_tools.MAIN_DB_NAME,
        db_tools.TRADES_DB_NAME: data_dir / db_tools.TRADES_DB_NAME,
    }
    for db_name, path in paths.items():
        db_tools._ensure_schema(path, db_name)
    _insert_sample(paths, "alice", "old", 1000)
    _insert_sample(paths, "alice", "new", 3000)
    _insert_sample(paths, "bob", "bob", 1000)

    result = subprocess.run(
        [sys.executable, "-c", db_tools._REMOTE_COUNT_USERS_SCRIPT, db_tools._table_specs_json(), json.dumps(["alice"]), "1500"],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])

    assert payload["total"] == 5
    assert payload["tables"]["pbgui.db:history"] == 1
    assert payload["tables"]["pbgui.db:history_scan_meta"] == 0
    assert payload["tables"]["pbgui_trades.db:executions"] == 1


def test_remote_sync_scripts_fetch_apply_and_verify(tmp_path: Path) -> None:
    """Remote sync scripts fetch incremental rows, apply them, and report stats."""

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paths = {
        db_tools.MAIN_DB_NAME: data_dir / db_tools.MAIN_DB_NAME,
        db_tools.TRADES_DB_NAME: data_dir / db_tools.TRADES_DB_NAME,
    }
    for db_name, path in paths.items():
        db_tools._ensure_schema(path, db_name)
    _insert_sample(paths, "alice", "old", 1000)
    _insert_sample(paths, "alice", "new", 3000)

    fetch_result = subprocess.run(
        [
            sys.executable,
            "-c",
            db_tools._REMOTE_SYNC_FETCH_SCRIPT,
            db_tools.MAIN_DB_NAME,
            "history",
            "user",
            "timestamp",
            "append",
            json.dumps(["alice"]),
            json.dumps({"alice": 2000}),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(fetch_result.stdout.strip().splitlines()[-1])
    assert payload["columns"] == ["symbol", "timestamp", "income", "uniqueid", "user"]
    assert [row[3] for row in payload["rows"]] == ["hist-new"]

    target_dir = tmp_path / "target"
    target_data_dir = target_dir / "data"
    target_data_dir.mkdir(parents=True)
    target_paths = {
        db_tools.MAIN_DB_NAME: target_data_dir / db_tools.MAIN_DB_NAME,
        db_tools.TRADES_DB_NAME: target_data_dir / db_tools.TRADES_DB_NAME,
    }
    for db_name, path in target_paths.items():
        db_tools._ensure_schema(path, db_name)
    _insert_sample(target_paths, "alice", "old", 1000)
    payload_file = target_dir / "payload.json"
    payload_file.write_text(json.dumps(payload), encoding="utf-8")

    apply_result = subprocess.run(
        [
            sys.executable,
            "-c",
            db_tools._REMOTE_SYNC_APPLY_SCRIPT,
            db_tools.MAIN_DB_NAME,
            "history",
            "user",
            "append",
            json.dumps(["alice"]),
            str(payload_file),
        ],
        cwd=target_dir,
        text=True,
        capture_output=True,
        check=True,
    )
    apply_payload = json.loads(apply_result.stdout.strip().splitlines()[-1])
    assert apply_payload == {"fetched": 1, "inserted": 1, "skipped": 0, "deleted": 0}

    stats_result = subprocess.run(
        [sys.executable, "-c", db_tools._REMOTE_SYNC_STATS_SCRIPT, db_tools._table_specs_json(), json.dumps(["alice"])],
        cwd=target_dir,
        text=True,
        capture_output=True,
        check=True,
    )
    stats_payload = json.loads(stats_result.stdout.strip().splitlines()[-1])
    assert stats_payload["tables"]["pbgui.db:history"]["count"] == 2
    assert stats_payload["tables"]["pbgui.db:history"]["max_timestamp"] == 3000


def test_remote_read_helpers_use_remote_python(monkeypatch) -> None:
    """Remote read helpers invoke the remote SQL scripts and normalize their JSON output."""

    async def fake_assert_known(target: str) -> None:
        assert target == "remote-a"

    async def fake_run_remote_python(target: str, script: str, args: list[str], timeout: int = 60) -> dict[str, object]:
        assert target == "remote-a"
        assert timeout == 90
        if script == db_tools._REMOTE_LIST_USERS_SCRIPT:
            return {"users": [{"user": "alice", "total": 7, "tables": {}}]}
        assert script == db_tools._REMOTE_COUNT_USERS_SCRIPT
        return {"total": 7, "tables": {"pbgui.db:history": 1}}

    monkeypatch.setattr(db_tools, "_assert_known_target", fake_assert_known)
    monkeypatch.setattr(db_tools, "_run_remote_python", fake_run_remote_python)

    users = asyncio.run(db_tools.list_users_for_target("remote-a"))
    counts = asyncio.run(db_tools.count_user_rows_for_target("remote-a", ["alice"]))

    assert users == [{"user": "alice", "total": 7, "tables": {}}]
    assert counts == {"total": 7, "tables": {"pbgui.db:history": 1}}


def test_known_targets_uses_monitor_state_without_remote_probe(monkeypatch) -> None:
    """Target listing uses existing monitor metadata instead of probing remotes."""

    class FakePool:
        """Small fake SSH pool with one master and one slave."""

        def hostnames(self) -> list[str]:
            return ["master-a", "slave-a"]

        def connected_hosts(self) -> list[str]:
            return ["master-a"]

        def get_connection(self, hostname: str):
            return SimpleNamespace(
                config=SimpleNamespace(remote_pbgui_dir="pbgui"),
                data={"remote_pbgui_dir": "pbgui"},
            )

    async def fail_probe(hostname: str) -> None:
        raise AssertionError("remote probe should not run")

    original_monitor = db_tools._monitor
    monkeypatch.setattr(db_tools, "_probe_remote_master", fail_probe)
    monkeypatch.setattr(
        db_tools,
        "_monitor",
        SimpleNamespace(
            pool=FakePool(),
            store=SimpleNamespace(host_meta={"master-a": {"role": "master", "pbname": "Master A"}, "slave-a": {"role": "slave"}}),
        ),
    )
    try:
        targets = asyncio.run(db_tools._known_targets())
    finally:
        monkeypatch.setattr(db_tools, "_monitor", original_monitor)

    remote_targets = [target for target in targets if target["kind"] == "remote"]
    assert remote_targets == [
        {
            "id": "master-a",
            "label": "Master A",
            "kind": "remote",
            "role": "master",
            "connected": True,
            "remote_pbgui_dir": "pbgui",
        }
    ]


def test_assert_known_target_uses_monitor_state(monkeypatch) -> None:
    """Target validation checks known connected masters from monitor state only."""

    class FakePool:
        """Small fake SSH pool for target validation."""

        def connected_hosts(self) -> list[str]:
            return ["master-a"]

        def get_connection(self, hostname: str):
            if hostname in {"master-a", "slave-a"}:
                return SimpleNamespace(config=SimpleNamespace(remote_pbgui_dir="pbgui"), data={})
            return None

    original_monitor = db_tools._monitor
    monkeypatch.setattr(
        db_tools,
        "_monitor",
        SimpleNamespace(pool=FakePool(), store=SimpleNamespace(host_meta={"master-a": {"role": "master"}, "slave-a": {"role": "slave"}})),
    )
    try:
        asyncio.run(db_tools._assert_known_target("master-a"))
        try:
            asyncio.run(db_tools._assert_known_target("slave-a"))
        except Exception as exc:
            assert "not a known master" in str(exc)
        else:
            raise AssertionError("slave target was not rejected")
        try:
            asyncio.run(db_tools._assert_known_target("missing-a"))
        except Exception as exc:
            assert "not a known VPS host" in str(exc)
        else:
            raise AssertionError("unknown target was not rejected")
    finally:
        monkeypatch.setattr(db_tools, "_monitor", original_monitor)


def test_known_targets_falls_back_to_monitor_snapshot(tmp_path: Path, monkeypatch) -> None:
    """Target listing uses the persisted monitor snapshot when live host metadata is empty."""

    class FakePool:
        """Fake SSH pool whose live store has not populated metadata yet."""

        def hostnames(self) -> list[str]:
            return ["manibot01"]

        def connected_hosts(self) -> list[str]:
            return ["manibot01"]

        def get_connection(self, hostname: str):
            return SimpleNamespace(config=SimpleNamespace(remote_pbgui_dir="pbgui"), data={"remote_pbgui_dir": "pbgui"})

    monkeypatch.setattr(db_tools, "PBGDIR", str(tmp_path))
    snapshot_dir = tmp_path / "data" / "state" / "vps_monitor"
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "snapshot.json").write_text(
        json.dumps({"host_meta": {"manibot01": {"role": "master", "pbname": "manibot01"}}}),
        encoding="utf-8",
    )
    original_monitor = db_tools._monitor
    monkeypatch.setattr(db_tools, "_monitor", SimpleNamespace(pool=FakePool(), store=SimpleNamespace(host_meta={})))
    try:
        targets = asyncio.run(db_tools._known_targets())
    finally:
        monkeypatch.setattr(db_tools, "_monitor", original_monitor)

    assert any(target["id"] == "manibot01" and target["role"] == "master" for target in targets)


class _FailingPriceDatabase:
    """Database stub that exposes and then fails a batch write."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def batch_upsert_prices(self, rows: list) -> None:
        """Block until released, then simulate a database failure."""
        self.started.set()
        self.release.wait(timeout=2)
        raise RuntimeError("database unavailable")


def _pbdata_with_failing_price_database() -> PBData:
    """Build the price-buffer state without initializing runtime services."""
    pbdata = PBData.__new__(PBData)
    pbdata.db = _FailingPriceDatabase()
    pbdata.users = SimpleNamespace(find_user=lambda _name: None)
    pbdata._price_buffer = {}
    pbdata._price_buffer_lock = asyncio.Lock()
    return pbdata


def test_batch_upsert_prices_propagates_database_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """The batch writer reports a failed transaction to its caller."""
    database = database_mod.Database.__new__(database_mod.Database)
    database._write_lock = threading.Lock()
    database._connect = lambda: sqlite3.connect(":memory:")
    monkeypatch.setattr(database_mod, "_human_log", lambda *_args, **_kwargs: None)

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        database.batch_upsert_prices([("alice", "BTCUSDT", 1000, 50_000.0)])


def test_failed_price_flush_restores_buffered_prices() -> None:
    """A failed database write puts the detached price snapshot back."""

    async def exercise() -> None:
        pbdata = _pbdata_with_failing_price_database()
        pbdata._price_buffer[("alice", "BTCUSDT")] = (1000, 50_000.0)
        pbdata.db.release.set()

        with pytest.raises(RuntimeError, match="database unavailable"):
            await pbdata._flush_price_buffer()

        assert pbdata._price_buffer == {("alice", "BTCUSDT"): (1000, 50_000.0)}

    asyncio.run(exercise())


def test_failed_price_flush_keeps_tick_buffered_during_write() -> None:
    """A newer tick buffered during a failed write wins over the snapshot."""

    async def exercise() -> None:
        pbdata = _pbdata_with_failing_price_database()
        pbdata._price_buffer[("alice", "BTCUSDT")] = (1000, 50_000.0)

        flush_task = asyncio.create_task(pbdata._flush_price_buffer())
        assert await asyncio.to_thread(pbdata.db.started.wait, 1)
        await pbdata.buffer_price(SimpleNamespace(name="alice"), "BTCUSDT", 1001, 50_100.0)
        pbdata.db.release.set()

        with pytest.raises(RuntimeError, match="database unavailable"):
            await flush_task

        assert pbdata._price_buffer == {("alice", "BTCUSDT"): (1001, 50_100.0)}

    asyncio.run(exercise())


def test_db_tools_shutdown_cancels_and_awaits_owned_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shutdown stops the scheduler and marks an interrupted operation terminal."""
    monkeypatch.setattr(db_tools, "_operations", {})
    monkeypatch.setattr(db_tools, "_background_tasks", set())
    monkeypatch.setattr(db_tools, "_sync_scheduler_task", None)
    monkeypatch.setattr(db_tools, "_sync_job_locks", {"job-a"})
    monkeypatch.setattr(db_tools, "_log", lambda *_args, **_kwargs: None)

    async def exercise() -> tuple[db_tools.OperationProgress, asyncio.Task]:
        blocker = asyncio.Event()

        async def runner(_operation: db_tools.OperationProgress) -> dict:
            await blocker.wait()
            return {"ok": True}

        operation = db_tools._start_operation("test", 1, runner)
        scheduler = asyncio.create_task(blocker.wait(), name="test-db-tools-scheduler")
        db_tools._sync_scheduler_task = scheduler
        await asyncio.sleep(0)
        assert "DB Tools operation" in db_tools.restart_block_reason()

        await db_tools.shutdown()
        return operation, scheduler

    operation, scheduler = asyncio.run(exercise())

    assert scheduler.cancelled()
    assert operation.status == "error"
    assert operation.error == "Interrupted by API shutdown"
    assert db_tools._background_tasks == set()
    assert db_tools._sync_scheduler_task is None
    assert db_tools._sync_job_locks == set()
    assert db_tools.restart_block_reason() == ""


def test_api_lifespan_awaits_db_tools_and_cluster_shutdown() -> None:
    """FastAPI shutdown invokes both newly tracked module lifecycles."""
    source = Path("PBApiServer.py").read_text(encoding="utf-8")

    assert '("cluster", cluster_shutdown)' in source
    assert '("db-tools", db_tools_shutdown)' in source
    assert "await shutdown_step()" in source
    assert "db_tools_restart_block_reason()" in source
    assert "cluster_restart_block_reason()" in source
    assert source.index('("cluster", cluster_shutdown)') < source.index("await _vps_monitor.stop()")
    assert source.index('("db-tools", db_tools_shutdown)') < source.index("await _vps_monitor.stop()")


def test_dispatch_sync_job_uses_persistent_task_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """Manual and scheduled DB Sync runs persist work outside the API process."""
    job = {
        "id": "sync-a",
        "name": "Sync A",
        "source": "local",
        "targets": ["master-b"],
        "users": ["alice"],
        "interval_seconds": 60,
        "enabled": True,
        "running": False,
    }
    queued_item = {
        "id": "task-a",
        "type": "db_sync",
        "status": "pending",
        "progress": {"total": 10},
    }
    queued = {"created": False}

    def fake_enqueue(**_kwargs):
        queued["created"] = True
        return SimpleNamespace(job_id="task-a", path="/tmp/task-a.json")

    monkeypatch.setattr(db_tools, "enqueue_running_job", fake_enqueue)
    monkeypatch.setattr(db_tools, "_start_db_sync_worker", lambda _path: None)
    monkeypatch.setattr(db_tools, "_task_jobs_by_id", lambda: {"task-a": queued_item} if queued["created"] else {})
    monkeypatch.setattr(db_tools, "_save_sync_jobs", lambda: None)

    operation = db_tools._dispatch_sync_job(job, manual=True)

    assert operation["id"] == "task-a"
    assert operation["status"] == "running"
    assert job["worker_job_id"] == "task-a"
    assert job["running"] is True
    assert job["next_run"] == ""


def test_reconcile_completed_worker_job_schedules_next_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """The API imports persistent worker completion after any intervening restart."""
    job = {
        "id": "sync-a",
        "enabled": True,
        "interval_seconds": 60,
        "running": True,
        "worker_job_id": "task-a",
    }
    monkeypatch.setattr(db_tools, "_sync_jobs", {"sync-a": job})
    monkeypatch.setattr(
        db_tools,
        "_task_jobs_by_id",
        lambda: {"task-a": {"id": "task-a", "type": "db_sync", "status": "done", "result": {"ok": True}}},
    )
    saved = []
    monkeypatch.setattr(db_tools, "_save_sync_jobs", lambda: saved.append(True))

    db_tools._reconcile_sync_worker_jobs()

    assert job["running"] is False
    assert job["worker_job_id"] == ""
    assert job["last_result"] == {"ok": True}
    assert job["last_error"] == ""
    assert job["next_run"]
    assert saved == [True]


def test_db_sync_worker_uses_own_ssh_pool_and_persists_result(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A task_worker DB Sync executes independently from FastAPI's monitor pool."""
    task_obj = {"id": "task-a", "status": "running", "progress": {}}
    disconnected = []

    def fake_update(_path: Path, mutate) -> None:
        mutate(task_obj)

    class FakePool:
        def load_vps_configs(self) -> list[str]:
            return []

        async def connect(self, _hostname: str) -> bool:
            return True

        async def disconnect_all(self) -> None:
            disconnected.append(True)

    async def fake_run(job: dict, operation) -> dict:
        assert job["id"] == "sync-a"
        operation.advance("Synced", {"rows": 2})
        return {"ok": True, "rows": 2}

    monkeypatch.setattr(task_worker, "update_job_file", fake_update)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _path: False)
    monkeypatch.setattr(async_pool_mod, "AsyncSSHPool", FakePool)
    monkeypatch.setattr(db_tools, "run_sync_job_snapshot", fake_run)

    task_worker._run_db_sync(
        tmp_path / "task-a.json",
        {"job": {"id": "sync-a", "source": "local", "targets": [], "users": ["alice"]}, "total": 2},
    )

    assert task_obj["result"] == {"ok": True, "rows": 2}
    assert task_obj["progress"]["completed"] == 2
    assert task_obj["progress"]["current"] == "Completed"
    assert disconnected == [True]
