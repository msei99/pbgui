"""Offline regressions for issues #140, #141, #142, #144, and #151."""

import asyncio
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

import Exchange as exchange_module
from api import api_keys, db_tools, v8_instances, vps


@pytest.mark.parametrize("operation", ["schema", "integrity", "backup"])
def test_sqlite_helpers_use_sync_lock_timeout(tmp_path, monkeypatch, operation):
    """Writes retain long busy waits; bounded snapshot helpers close short-wait connections."""
    source = tmp_path / "source.db"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE sample (value INTEGER)")
        connection.execute("INSERT INTO sample VALUES (42)")
    connection.close()
    connect = sqlite3.connect
    opened = []
    timeout = 30 if operation == "schema" else 0.01

    def tracked_connect(*args, **kwargs):
        """Require the timeout at open, before any SQL can encounter a lock."""
        assert kwargs["timeout"] == timeout
        conn = connect(*args, **kwargs)
        assert conn.execute("PRAGMA busy_timeout").fetchone() == (int(timeout * 1000),)
        opened.append(conn)
        return conn

    monkeypatch.setattr(db_tools.sqlite3, "connect", tracked_connect)
    try:
        if operation == "schema":
            db_tools._ensure_schema(tmp_path / "new.db", db_tools.MAIN_DB_NAME)
        elif operation == "integrity":
            db_tools._assert_sqlite_integrity(source)
        else:
            db_tools._sqlite_backup_file(source, tmp_path / "backup.db")
            restored = connect(tmp_path / "backup.db")
            try:
                assert restored.execute("SELECT value FROM sample").fetchone() == (42,)
            finally:
                restored.close()
        assert len(opened) == (2 if operation == "backup" else 1)
        for conn in opened:
            if operation == "schema":
                assert conn.execute("PRAGMA busy_timeout").fetchone() == (30000,)
            else:
                with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                    conn.execute("SELECT 1")
    finally:
        for conn in opened:
            conn.close()


@pytest.mark.parametrize("hosts", [["local"], ["remote-a"], ["local", "remote-a"], []])
def test_pb8_delete_checks_running_hosts_before_any_mutation(tmp_path, monkeypatch, hosts):
    """A running PB8 bundle is never backed up, tombstoned, or removed."""
    import PBRun
    from master.async_store import VPSStore

    monkeypatch.setattr(v8_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v8_instances, "_master_hostname", lambda: "local")
    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8dir", lambda: str(tmp_path / "pb8"))
    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8venv", lambda: str(tmp_path / "venv" / "bin" / "python"))
    monkeypatch.setattr(v8_instances, "load_pb8_config", lambda _path: {"pbgui": {"enabled_on": "disabled"}})
    target = tmp_path / "data" / "run_v8" / "alice"
    target.mkdir(parents=True)
    (target / "config.json").write_text("{}", encoding="utf-8")
    command = [str(tmp_path / "venv" / "bin" / "passivbot"), "live", str(target / "config.json"), "--fail-on-stale-rust"]
    process = SimpleNamespace(cmdline=lambda: command, cwd=lambda: str(tmp_path / "pb8"))
    monkeypatch.setattr(PBRun.psutil, "process_iter", lambda: iter([process] if "local" in hosts else []))
    store = VPSStore()
    store.update_v8_instances("remote-b", [{"name": "other", "running": True}])
    if "remote-a" in hosts:
        store.update_v8_instances("remote-a", [{"name": "alice", "running": True}])
    monkeypatch.setattr(v8_instances, "_monitor", SimpleNamespace(store=store))
    snapshot = Mock(return_value="1")
    record = Mock(return_value={"op": "DELETE_PB8_INSTANCE", "op_id": "test-op"})
    monkeypatch.setattr(v8_instances, "_snapshot_v8_bundle", snapshot)
    monkeypatch.setattr(v8_instances, "_record_delete", record)
    monkeypatch.setattr(v8_instances, "_current_version", lambda name: 1)
    monkeypatch.setattr(v8_instances, "_highest_cluster_version", lambda name: 1)
    if hosts:
        with pytest.raises(HTTPException) as error:
            v8_instances.delete_v8_instance("alice", session=None)
        assert error.value.status_code == 409
        assert ("running locally" if "local" in hosts else "running on remote-a") in error.value.detail
        assert "stop it first" in error.value.detail
        assert (target / "config.json").read_text(encoding="utf-8") == "{}"
        snapshot.assert_not_called()
        record.assert_not_called()
    else:
        assert v8_instances.delete_v8_instance("alice", session=None)["ok"] is True
        assert not target.exists()
        snapshot.assert_called_once()
        record.assert_called_once_with("alice", 1)


@pytest.mark.parametrize("runtime", ["run_v7", "run_v8"])
@pytest.mark.parametrize("action", ["delete", "rename"])
def test_credentials_in_either_runtime_are_protected_outside_cwd(tmp_path, monkeypatch, runtime, action):
    """Both mutation routes reject in-use users before touching fake credentials."""
    monkeypatch.setattr(api_keys, "_PBGDIR", str(tmp_path))
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    monkeypatch.chdir(unrelated)
    bundle = tmp_path / "data" / runtime / "alice"
    bundle.mkdir(parents=True)
    # Missing config must not disable protection for a damaged live bundle.
    assert api_keys._get_in_use_names() == {"alice"}
    user = SimpleNamespace(name="alice")
    users = Mock()
    users.find_user.return_value = user
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    with pytest.raises(HTTPException) as error:
        if action == "delete":
            api_keys.delete_user("alice", session=None)
        else:
            api_keys.rename_user(api_keys.RenameRequest(new_name="bob"), "alice", session=None)
    assert error.value.status_code == 409
    assert user.name == "alice"
    users.save.assert_not_called()
    users.remove_user.assert_not_called()


def test_credential_usage_ignores_nonbundles_and_fails_closed(tmp_path, monkeypatch):
    """Hidden entries, files, and links are not instances; unreadable roots block writes."""
    monkeypatch.setattr(api_keys, "_PBGDIR", str(tmp_path))
    root = tmp_path / "data" / "run_v8"
    root.mkdir(parents=True)
    (root / ".staging").mkdir()
    (root / "status.json").write_text("{}", encoding="utf-8")
    (root / "linked").symlink_to(tmp_path, target_is_directory=True)
    assert api_keys._get_in_use_names() == set()
    monkeypatch.setattr(Path, "iterdir", Mock(side_effect=PermissionError("test denied")))
    with pytest.raises(HTTPException) as error:
        api_keys._get_in_use_names()
    assert error.value.status_code == 503


@pytest.mark.parametrize("version", ["7", "8"])
@pytest.mark.parametrize("config", [None, "{broken", '{"pbgui":{"enabled_on":"disabled"}}'])
@pytest.mark.parametrize("running", [True, False])
def test_local_kill_does_not_require_startable_config(tmp_path, monkeypatch, version, config, running):
    """Only validated identity and a matching mocked process are needed to stop."""
    root = tmp_path / "data" / f"run_v{version}"
    if config is not None:
        bundle = root / "alice"
        bundle.mkdir(parents=True)
        (bundle / "config.json").write_text(config, encoding="utf-8")
    supervisor = SimpleNamespace(
        v7_path=tmp_path / "data" / "run_v7", v8_path=tmp_path / "data" / "run_v8",
        name="test-host", pbgdir=tmp_path,
        pb7dir="test-pb7", pb7venv="test-python7", pb8dir="test-pb8", pb8venv="test-python8",
    )
    runner = Mock()
    runner.load.return_value = False
    runner.pid.return_value = SimpleNamespace(pid=4321) if running else None
    constructors = SimpleNamespace(PBRun=Mock(return_value=supervisor),
                                   RunV7=Mock(return_value=runner), RunV8=Mock(return_value=runner))
    monkeypatch.setitem(sys.modules, "PBRun", constructors)
    result = asyncio.run(vps._local_kill_instance("alice", version))
    assert result["success"] is running
    assert result["pid"] == (4321 if running else None)
    assert runner.path == str(root / "alice")
    assert runner.user == "alice"
    runner.load.assert_not_called()
    assert runner.stop.call_count == int(running)
    assert getattr(constructors, f"RunV{version}").call_count == 1


@pytest.mark.parametrize("name,version", [("../alice", "8"), ("alice\x00", "7"), ("alice", "6")])
def test_local_kill_retains_identifier_validation(monkeypatch, name, version):
    """Invalid commands never construct a supervisor or inspect real processes."""
    constructors = SimpleNamespace(PBRun=Mock(), RunV7=Mock(), RunV8=Mock())
    monkeypatch.setitem(sys.modules, "PBRun", constructors)
    assert asyncio.run(vps._local_kill_instance(name, version))["success"] is False
    constructors.PBRun.assert_not_called()
    constructors.RunV7.assert_not_called()
    constructors.RunV8.assert_not_called()


@pytest.mark.parametrize("payload,expected", [
    ({}, 0.0), ({"total": None}, 0.0), ({"total": {}}, 0.0),
    ({"total": {"USDC": None}}, 0.0), ({"total": {"USDC": "12.5"}}, 12.5),
])
@pytest.mark.parametrize("vault", [False, True])
def test_hyperliquid_template_credentials_and_balance_test(monkeypatch, payload, expected, vault):
    """The API test accepts unfunded wallets and sends saved vault routing to CCXT."""
    user = SimpleNamespace(name="alice", exchange="hyperliquid", key="key", secret="secret",
                           wallet_address="test-wallet", private_key="test-private", is_vault=vault)
    client = Mock(options={})
    client.fetch_balance.return_value = payload
    monkeypatch.setattr(exchange_module.ccxt, "hyperliquid", lambda: client)
    monkeypatch.setattr(api_keys, "_get_users", lambda: SimpleNamespace(find_user=lambda name: user))
    result = api_keys.test_connection("alice", override=None, session=None)
    assert result.success is True
    assert result.balance_futures == expected
    assert client.walletAddress == "test-wallet"
    assert client.privateKey == "test-private"
    params = {"type": "swap"}
    if vault:
        params["vaultAddress"] = "test-wallet"
    client.fetch_balance.assert_called_once_with(params=params)
    client.close.assert_called_once()


def test_hyperliquid_network_failure_is_not_a_zero_balance(monkeypatch):
    """A failed request retains the existing error result and closes its client."""
    user = SimpleNamespace(name="alice", exchange="hyperliquid", key="key", is_vault=False)
    client = Mock(options={})
    client.fetch_balance.side_effect = RuntimeError("test connection failed")
    monkeypatch.setattr(exchange_module.ccxt, "hyperliquid", lambda: client)
    monkeypatch.setattr(api_keys, "_get_users", lambda: SimpleNamespace(find_user=lambda name: user))
    result = api_keys.test_connection("alice", override=None, session=None)
    assert result.success is False
    assert result.balance_futures is None
    assert result.error == "test connection failed"
    client.close.assert_called_once()


def test_non_hyperliquid_credentials_and_balance_are_unchanged(monkeypatch):
    """Conventional credentials, USDT balances, and parameters retain their contract."""
    user = SimpleNamespace(key="test-key", secret="test-secret", passphrase="test-pass", is_vault=True)
    client = Mock(options={})
    client.fetch_balance.return_value = {"total": {"USDT": "123.5"}}
    monkeypatch.setattr(exchange_module.ccxt, "okx", lambda: client)
    exchange = exchange_module.Exchange("okx", user)
    assert exchange.fetch_balance("swap") == 123.5
    assert (client.apiKey, client.secret, client.password) == ("test-key", "test-secret", "test-pass")
    client.fetch_balance.assert_called_once_with(params={"type": "swap"})
