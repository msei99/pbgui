"""Offline Bitget Classic/UTA persistence contracts using temporary databases."""

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Database as database_module
from database_lock import DatabaseBusyError, acquire_database_lock


@pytest.fixture
def database(tmp_path, monkeypatch):
    """Isolate both SQLite stores, maintenance leases, logs and exchange calls."""
    monkeypatch.setattr(database_module, "PBGDIR", tmp_path)
    monkeypatch.setattr(database_module, "_human_log", Mock())
    monkeypatch.setattr(database_module, "Exchange", Mock(side_effect=AssertionError("Unexpected exchange access")))
    (tmp_path / "data").mkdir()
    db = database_module.Database()
    yield db
    db.close_thread_connections()


@pytest.mark.parametrize("side,trade_side,expected", [
    ("buy", "close", "sell"), ("sell", "close", "buy"),
    ("buy", "open", "buy"), ("sell", "open", "sell"),
])
def test_side_repair_preserves_uta_and_repairs_classic(database, side, trade_side, expected):
    """Repeated repairs skip provenance/native UTA rows even beside malformed JSON."""
    info = {"side": side, "tradeSide": trade_side}
    payloads = {
        "classic": json.dumps({"info": info}),
        "uta": json.dumps({"info": info, "pbgui_account_mode": "uta"}),
        "native": json.dumps({"info": {**info, "execId": "123"}}),
        "native_integer": json.dumps({"info": {**info, "execId": 123}}),
        "empty_exec_id": json.dumps({"info": {**info, "execId": ""}}),
        "null_exec_id": json.dumps({"info": {**info, "execId": None}}),
        "malformed": '{"info":',
        "empty": "",
        "null": None,
    }
    with database._connect_trades() as conn:
        conn.executemany(
            "INSERT INTO executions(exchange,symbol,timestamp,side,trade_id,user,raw_json) VALUES(?,?,?,?,?,?,?)",
            [("bitget", "BTCUSDT", 1, side, key, "alice", raw) for key, raw in payloads.items()]
            + [("bitget", "BTCUSDT", 1, side, "other_user", "bob", payloads["classic"]),
               ("bybit", "BTCUSDT", 1, side, "other_exchange", "alice", payloads["classic"])],
        )
    for _ in range(2):
        database._repair_bitget_execution_sides("alice")
        rows = dict(database._connect_trades().execute("SELECT trade_id, side FROM executions"))
        assert rows == {key: expected if key in {"classic", "empty_exec_id", "null_exec_id"} else side for key in rows}


def test_side_repair_logs_sqlite_failure(database, monkeypatch):
    """A database repair failure is observable rather than silently swallowed."""
    monkeypatch.setattr(database, "_connect_trades", Mock(side_effect=sqlite3.OperationalError("test failure")))
    database._repair_bitget_execution_sides("alice")
    database_module._human_log.assert_called_once()
    assert database_module._human_log.call_args.kwargs == {"level": "ERROR", "user": "alice"}


def test_uta_execution_import_is_idempotent(database, monkeypatch):
    """Normalized UTA order sides/PnL survive imports and the automatic repair."""
    user = SimpleNamespace(name="alice", exchange="bitget")
    raw = json.dumps({"info": {"execId": "42", "side": "sell", "tradeSide": "close", "execPnl": "12.5"},
                      "pbgui_account_mode": "uta"})
    exchange = Mock(id="bitget")
    exchange.fetch_executions.return_value = [{
        "symbol": "DELISTEDUSDT", "timestamp": 1000, "side": "sell", "price": 10,
        "qty": 2, "fee": 0.1, "realized_pnl": 12.5, "order_id": "order42",
        "trade_id": "bitget:uta:alice:42", "raw_json": raw,
    }]
    monkeypatch.setattr(database_module, "Exchange", Mock(return_value=exchange))
    first = database.update_executions(user)
    second = database.update_executions(user)
    assert first == {"fetched": 1, "prepared": 1, "inserted": 1}
    assert second == {"fetched": 1, "prepared": 1, "inserted": 0}
    assert exchange.fetch_executions.call_args.kwargs["symbols"] == []
    assert database._connect_trades().execute(
        "SELECT symbol, side, realized_pnl, raw_json FROM executions"
    ).fetchall() == [("DELISTEDUSDT", "sell", 12.5, raw)]


@pytest.mark.parametrize("partial_pages", [0, 1])
def test_failed_history_scan_never_advances_checkpoint(database, monkeypatch, partial_pages):
    """Exchange failures before or after a page cannot publish income or a checkpoint."""
    user = SimpleNamespace(name="alice", exchange="bitget")
    previous = 100_000_000
    database.set_last_scan_ts(user.name, user.exchange, previous)
    exchange = Mock(id="bitget")

    def fetch_history(since):
        """Model the Exchange full-list-or-raise contract on later-page failure."""
        assert since == previous - database._HISTORY_SCAN_LOOKBACK_MS
        rows = []
        for _ in range(partial_pages):
            rows.append({"symbol": "BTCUSDT", "timestamp": previous, "income": 1, "uniqueid": "uta:alice:42"})
        raise RuntimeError(f"history scan failed after {len(rows)} pages")

    exchange.fetch_history.side_effect = fetch_history
    monkeypatch.setattr(database_module, "Exchange", Mock(return_value=exchange))
    with pytest.raises(RuntimeError, match="history scan failed"):
        database.fetch_history(user)
    database.update_history(user)
    assert database.get_last_scan_ts(user.name, user.exchange) == previous
    assert database._connect().execute("SELECT COUNT(*) FROM history").fetchone() == (0,)
    assert exchange.close.call_count == 2


def test_history_migration_preserves_classic_and_account_scoped_ids(database, monkeypatch):
    """Existing Classic income survives UTA imports; globally unique IDs isolate accounts."""
    with database._connect() as conn:
        database.add_history(conn, ["BTCUSDT", 1, 3.0, "classic-42", "alice"])
    monkeypatch.setattr(database_module.time, "time", lambda: 200_000.0)
    for name in ("alice", "bob"):
        user = SimpleNamespace(name=name, exchange="bitget")
        exchange = Mock(id="bitget")
        exchange.fetch_history.return_value = [{
            "symbol": "BTCUSDT", "timestamp": 2, "income": 4.0,
            "uniqueid": f"bitget:uta:{name}:42",
        }]
        monkeypatch.setattr(database_module, "Exchange", Mock(return_value=exchange))
        database.update_history(user)
        database.update_history(user)
        assert database.get_last_scan_ts(name, "bitget") == 200_000_000
    assert database._connect().execute(
        "SELECT uniqueid, income, user FROM history ORDER BY id"
    ).fetchall() == [("classic-42", 3.0, "alice"), ("bitget:uta:alice:42", 4.0, "alice"),
                     ("bitget:uta:bob:42", 4.0, "bob")]


def test_successful_empty_scan_advances_checkpoint(database, monkeypatch):
    """A fully scanned empty account is a success, unlike a failed partial scan."""
    exchange = Mock(id="bitget")
    exchange.fetch_history.return_value = []
    monkeypatch.setattr(database_module, "Exchange", Mock(return_value=exchange))
    monkeypatch.setattr(database_module.time, "time", lambda: 200_000.0)
    user = SimpleNamespace(name="alice", exchange="bitget")
    assert database.fetch_history(user) == []
    assert database.get_last_scan_ts("alice", "bitget") is None
    database.update_history(user)
    assert database.get_last_scan_ts("alice", "bitget") == 200_000_000


@pytest.mark.parametrize("failure_target", ["income", "checkpoint"])
def test_failed_persistence_rolls_back_and_retry_imports_all(database, monkeypatch, failure_target):
    """Failures after an insert or at checkpoint persistence roll back both tables."""
    user = SimpleNamespace(name="alice", exchange="bitget")
    previous = 100_000_000
    database.set_last_scan_ts(user.name, user.exchange, previous)
    with database._connect() as conn:
        database.add_history(conn, ["BTCUSDT", 1, 3.0, "classic-42", user.name])
        if failure_target == "income":
            conn.execute("""CREATE TEMP TRIGGER fail_scan BEFORE INSERT ON history
                            WHEN NEW.uniqueid = 'uta:alice:2'
                            BEGIN SELECT RAISE(ABORT, 'test persistence failure'); END""")
        else:
            conn.execute("""CREATE TEMP TRIGGER fail_scan BEFORE UPDATE ON history_scan_meta
                            BEGIN SELECT RAISE(ABORT, 'test persistence failure'); END""")
    exchange = Mock(id="bitget")
    rows = [{"symbol": "BTCUSDT", "timestamp": previous + index, "income": index,
             "uniqueid": f"uta:alice:{index}"} for index in (1, 2)]
    exchange.fetch_history.return_value = rows
    monkeypatch.setattr(database_module, "Exchange", Mock(return_value=exchange))
    monkeypatch.setattr(database_module.time, "time", lambda: 200_000.0)
    database.update_history(user)
    assert database.get_last_scan_ts(user.name, user.exchange) == previous
    assert database._connect().execute("SELECT uniqueid FROM history").fetchall() == [("classic-42",)]
    assert any(call.kwargs.get("level") == "ERROR" for call in database_module._human_log.call_args_list)
    with database._connect() as conn:
        conn.execute("DROP TRIGGER fail_scan")
    database.update_history(user)
    assert [call.args[0] for call in exchange.fetch_history.call_args_list] == [
        previous - database._HISTORY_SCAN_LOOKBACK_MS,
        previous - database._HISTORY_SCAN_LOOKBACK_MS,
    ]
    assert database.get_last_scan_ts(user.name, user.exchange) == 200_000_000
    assert database._connect().execute("SELECT uniqueid FROM history ORDER BY id").fetchall() == [
        ("classic-42",), ("uta:alice:1",), ("uta:alice:2",),
    ]


@pytest.mark.parametrize("invalid", [None, {}, [{"symbol": "BTCUSDT"}], [
    {"symbol": None, "timestamp": 1, "income": 1, "uniqueid": "invalid"},
]])
def test_invalid_history_does_not_publish_checkpoint(database, monkeypatch, invalid):
    """Incomplete results and non-duplicate constraint violations are not successes."""
    exchange = Mock(id="bitget")
    exchange.fetch_history.return_value = invalid
    monkeypatch.setattr(database_module, "Exchange", Mock(return_value=exchange))
    database.update_history(SimpleNamespace(name="alice", exchange="bitget"))
    assert database.get_last_scan_ts("alice", "bitget") is None
    assert database._connect().execute("SELECT COUNT(*) FROM history").fetchone() == (0,)


def test_long_history_scan_checkpoints_start_not_completion(database, monkeypatch):
    """A slow scan cannot skip records arriving after its initial request."""
    now = [200_000.0]
    monkeypatch.setattr(database_module.time, "time", lambda: now[0])

    def fetch(since):
        """Advance the fake clock beyond the overlap window during network work."""
        now[0] += 24 * 60 * 60
        return []

    exchange = Mock(id="bitget", fetch_history=Mock(side_effect=fetch))
    monkeypatch.setattr(database_module, "Exchange", Mock(return_value=exchange))
    database.update_history(SimpleNamespace(name="alice", exchange="bitget"))
    assert database.get_last_scan_ts("alice", "bitget") == 200_000_000


def test_atomic_history_batch_excludes_restore_and_allows_consistent_backup(database, monkeypatch, tmp_path):
    """Mid-batch backups see the old snapshot while restore remains lease-blocked."""
    user = SimpleNamespace(name="alice", exchange="bitget")
    previous = 100_000_000
    database.set_last_scan_ts(user.name, user.exchange, previous)
    entered, release = threading.Event(), threading.Event()
    exchange = Mock(id="bitget")
    exchange.fetch_history.return_value = [
        {"symbol": "BTCUSDT", "timestamp": previous + index, "income": index,
         "uniqueid": f"uta:alice:{index}"} for index in (1, 2)
    ]
    monkeypatch.setattr(database_module, "Exchange", Mock(return_value=exchange))

    def pause():
        """Block the second insert with the first row still uncommitted."""
        entered.set()
        assert release.wait(10), "Atomic history test did not release worker"
        return 0

    def update():
        """Install a trigger on the worker connection and close it deterministically."""
        try:
            conn = database._connect()
            conn.create_function("pause_scan", 0, pause)
            conn.execute("""CREATE TEMP TRIGGER pause_scan BEFORE INSERT ON history
                            WHEN NEW.uniqueid = 'uta:alice:2'
                            BEGIN SELECT pause_scan(); END""")
            database.update_history(user)
        finally:
            database.close_thread_connections()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(update)
        try:
            assert entered.wait(10), "History did not reach its second insert"
            with pytest.raises(DatabaseBusyError):
                with acquire_database_lock(tmp_path, exclusive=True):
                    pass
            backup = database.backup_full_db()
            assert backup is not None
            with closing(sqlite3.connect(backup)) as snapshot:
                assert snapshot.execute("SELECT COUNT(*) FROM history").fetchone() == (0,)
                assert snapshot.execute("SELECT last_scan_ts FROM history_scan_meta").fetchone() == (previous,)
            assert database._connect().execute("SELECT COUNT(*) FROM history").fetchone() == (0,)
            assert database.get_last_scan_ts(user.name, user.exchange) == previous
        finally:
            release.set()
        future.result(timeout=10)
    assert database._connect().execute("SELECT COUNT(*) FROM history").fetchone() == (2,)
    assert database.get_last_scan_ts(user.name, user.exchange) > previous
