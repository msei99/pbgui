"""Tests for DB tools SQLite cleanup and copy helpers."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from pathlib import Path

from api import db_tools


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
