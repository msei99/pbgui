"""Isolated regressions for dashboard issues #146, #150, #152, #157 and #158."""

import json
import re
import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import pbgui_purefunc
import sqlite_backup
from api import dashboard, dashboards
from database_lock import DatabaseBusyError, acquire_database_lock
from master_update_lock import MasterUpdateBusyError, acquire_master_update_lock


@pytest.fixture(autouse=True)
def isolated_runtime(monkeypatch, tmp_path):
    """Block real clients, process control and database access in every test."""
    monkeypatch.setattr(pbgui_purefunc, "PBGDIR", tmp_path)
    monkeypatch.setattr(dashboard, "PBGDIR", tmp_path)
    for name in ("_get_db", "_get_users", "_get_exchange", "_pbdata_stop", "_pbdata_start"):
        monkeypatch.setattr(dashboard, name, Mock(side_effect=AssertionError("Unexpected runtime access")))
    monkeypatch.setattr(dashboard, "_start_ohlcv_poller", Mock())
    monkeypatch.setattr(dashboard, "_ohlcv_cache_get", Mock(return_value=None))
    monkeypatch.setattr(dashboard, "_ohlcv_cache_put", Mock())
    monkeypatch.setattr(dashboard, "_log", Mock())


@pytest.fixture
def client():
    """Mount only the dashboard routers without production startup or auth state."""
    app = FastAPI()
    app.include_router(dashboard.router, prefix="/api/dashboard")
    app.include_router(dashboards.router, prefix="/api/dashboards")
    app.dependency_overrides[dashboard.require_auth] = lambda: object()
    with TestClient(app) as test_client:
        yield test_client


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("users,expected", [("alice", 1000.0), ("ALL", 1500.0)])
def test_adg_balance_updates_do_not_accumulate_snapshots(monkeypatch, legacy, users, expected):
    """Real balance writes replace snapshots, including cleanup for a legacy schema."""
    from Database import Database

    db = Database.__new__(Database)
    with closing(sqlite3.connect(":memory:")) as conn:
        monkeypatch.setattr(db, "_connect", lambda: conn)
        if legacy:
            conn.execute("CREATE TABLE balances (id INTEGER PRIMARY KEY, timestamp INTEGER, balance REAL, user TEXT)")
            conn.execute("INSERT INTO balances VALUES (1, 0, 9999, 'alice')")
        db.create_tables()
        for timestamp in range(1, 51):
            db.update_balance(conn, [timestamp, 1000.0, "alice"])
        db.update_balance(conn, [51, 500.0, "bob"])
        assert len(db.fetch_balances(["alice", "bob"])) == 2
        monkeypatch.setattr(db, "select_pnl", lambda *_: [("2026-09-01", 20.0)])
        monkeypatch.setattr(dashboard, "_get_db", lambda: db)
        monkeypatch.setattr(dashboard, "_get_users", lambda: SimpleNamespace(list=lambda: ["alice", "bob"]))
        result = dashboard.get_adg_data(users=users, period="THIS_MONTH", mode="bar", session=object())
    assert result["current_balance"] == expected
    assert result["starting_balance"] == expected - 20
    assert result["total_pnl"] == 20
    assert result["bars"] == [{"date": "2026-09-01", "adg": round(2000 / (expected - 20), 4)}]


@pytest.mark.parametrize("route", ["example", "templates/example"])
@pytest.mark.parametrize("key", ["rows", "cols"])
@pytest.mark.parametrize("value", [0, -1, 1000, "2", None, True, False, [], {}, 1.0])
def test_grid_invalid_dimensions_do_not_overwrite(client, tmp_path, route, key, value):
    """Both save endpoints reject invalid dimensions before replacing saved data."""
    payload = {"rows": 1, "cols": 1}
    assert client.post(f"/api/dashboards/{route}", json=payload).status_code == 200
    saved = tmp_path / "data" / "dashboards" / f"{route}.json"
    original = saved.read_bytes()
    payload[key] = value
    assert client.post(f"/api/dashboards/{route}", json=payload).status_code == 422
    assert saved.read_bytes() == original
    assert not saved.with_suffix(".tmp").exists()


@pytest.mark.parametrize("route", ["example", "templates/example"])
@pytest.mark.parametrize("payload", [{}, {"rows": 1}, {"cols": 1}, {"rows": 11, "cols": 1}, {"rows": 1, "cols": 3}])
def test_grid_missing_or_out_of_range_dimensions_do_not_create(client, tmp_path, route, payload):
    """Missing fields and values just beyond each maximum must not create configs."""
    assert client.post(f"/api/dashboards/{route}", json=payload).status_code == 422
    assert not (tmp_path / "data" / "dashboards" / f"{route}.json").exists()


@pytest.mark.parametrize("route", ["example", "templates/example"])
@pytest.mark.parametrize("rows,cols", [(1, 1), (1, 2), (10, 1), (10, 2), (5, 2)])
def test_grid_valid_dimensions_round_trip(client, tmp_path, route, rows, cols):
    """Valid grid limits preserve all widget settings and the save response shape."""
    payload = {"rows": rows, "cols": cols, "dashboard_type_1_1": "BALANCE"}
    response = client.post(f"/api/dashboards/{route}", json=payload)
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "name": "example"}
    assert json.loads((tmp_path / "data" / "dashboards" / f"{route}.json").read_text()) == payload


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("rows", [0, 10])
def test_template_creation_revalidates_persisted_grid(client, tmp_path, bulk, rows):
    """Simple and bulk creation must not propagate invalid existing templates."""
    template_dir = tmp_path / "data" / "dashboards" / "templates"
    template_dir.mkdir(parents=True)
    payload = {"rows": rows, "cols": 2, "dashboard_balance_users_1_1": ["old"]}
    (template_dir / "source.json").write_text(json.dumps(payload))
    request = {"template": "source"}
    request.update({"prefix": "copy", "users": ["alice"]} if bulk else {"name": "copy"})
    response = client.post("/api/dashboards/from_template", json=request)
    assert response.status_code == (200 if rows else 422)
    saved = template_dir.parent / ("copy_alice.json" if bulk else "copy.json")
    assert saved.exists() == bool(rows)
    if rows:
        config = json.loads(saved.read_text())
        assert config["rows"] == rows
        assert config["dashboard_balance_users_1_1"] == (["alice"] if bulk else ["old"])


@pytest.mark.parametrize("page,variable,param", [
    ("editor_page", "ORIG_NAME", "name"),
    ("templates_page", "CURRENT", "current"),
    ("main_page", "CURRENT", "current"),
])
@pytest.mark.parametrize("value", [
    '\";window.injected=true;//',
    '</script><script>window.injected=true</script>&',
    '\\quote"\n\u2028\u2029',
    '%%DASHBOARD_NAME%%%%CURRENT%%%%VERSION%%%%DASHBOARDS_JSON%%',
])
def test_dashboard_pages_encode_inline_values(client, page, variable, param, value):
    """Query values stay JSON strings without HTML termination or marker substitution."""
    params = {param: value}
    if page != "main_page":
        params["api_base"] = value
    response = client.get(f"/api/dashboard/{page}", params=params)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    html = response.text
    assert '</script><script>window.injected' not in html
    variables = [variable] + (["API_BASE"] if page != "main_page" else [])
    for name in variables:
        assignment = re.search(rf"var {name}\s*=\s*(.*)", html).group(1)
        decoded, end = json.JSONDecoder().raw_decode(assignment)
        assert decoded == ("/api" if name == "API_BASE" else value)
        assert assignment[end:].lstrip().startswith(";")
        assert "<" not in assignment[:end]
        assert "%%" not in assignment[:end]


def test_dashboard_list_names_are_html_safe(client, tmp_path):
    """Persisted dashboard names must use the same script-safe JSON encoding."""
    folder = tmp_path / "data" / "dashboards"
    folder.mkdir(parents=True)
    name = '<img onerror="window.injected=true">&%%VERSION%%'
    (folder / f"{name}.json").write_text("{}")
    html = client.get("/api/dashboard/main_page").text
    assignment = re.search(r"var DASHBOARDS\s*=\s*(.*)", html).group(1)
    decoded, end = json.JSONDecoder().raw_decode(assignment)
    assert decoded == [name]
    assert "<" not in assignment[:end]
    assert "&" not in assignment[:end]


@pytest.mark.parametrize("kind", ["directory", "missing", "empty", "text", "corrupt", "outside", "symlink", "nul"])
def test_income_restore_rejects_invalid_sources_before_runtime(client, tmp_path, kind):
    """Invalid sources must not obtain the active DB or stop PBData."""
    backup_dir = tmp_path / "data" / "backup" / "db"
    backup_dir.mkdir(parents=True)
    live = tmp_path / "data" / "pbgui.db"
    with closing(sqlite3.connect(live)) as conn:
        conn.execute("CREATE TABLE retained (value TEXT)")
        conn.execute("INSERT INTO retained VALUES ('untouched')")
        conn.commit()
    path = backup_dir / "backup.db"
    if kind == "directory":
        path = backup_dir
    elif kind == "empty":
        path.touch()
    elif kind == "text":
        path.write_text("not sqlite")
    elif kind == "corrupt":
        path.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)
    elif kind in {"outside", "symlink"}:
        outside = tmp_path / "outside.db"
        outside.write_text("outside")
        if kind == "symlink":
            path.symlink_to(outside)
        else:
            path = outside
    elif kind == "nul":
        path = str(path) + "\x00"
    response = client.post("/api/dashboard/income/restore", json={"path": str(path)})
    assert response.status_code == 400
    dashboard._get_db.assert_not_called()
    dashboard._pbdata_stop.assert_not_called()
    dashboard._pbdata_start.assert_not_called()
    assert dashboard.restart_block_reason() == ""
    with closing(sqlite3.connect(live)) as conn:
        assert conn.execute("SELECT value FROM retained").fetchall() == [("untouched",)]
    with acquire_master_update_lock(tmp_path):
        pass


@pytest.mark.parametrize("results", [[], [("damaged page",)], [("ok",), ("damaged page",)]])
def test_income_restore_rejects_integrity_findings(client, monkeypatch, tmp_path, results):
    """All integrity results must be clean, and the read-only validator must close."""
    from Database import Database

    backup_dir = tmp_path / "data" / "backup" / "db"
    backup_dir.mkdir(parents=True)
    path = backup_dir / "backup.db"
    with closing(sqlite3.connect(path)) as seed:
        database = Database.__new__(Database)
        monkeypatch.setattr(database, "_connect", lambda: seed)
        database.create_tables()
    (tmp_path / "data" / "pbgui.db").touch()
    opened = []
    checks = []

    class IntegrityConnection(sqlite3.Connection):
        """Preserve real schema validation while injecting only integrity findings."""

        def execute(self, sql, *args, **kwargs):
            """Return all simulated findings after the production schema queries."""
            if sql == "PRAGMA integrity_check":
                checks.append(sql)
                return iter(results)
            return super().execute(sql, *args, **kwargs)

    def open_snapshot(staged, mode, *, immutable=False):
        """Open only the private staged database; never a production connection."""
        assert staged.name == "snapshot.db"
        assert mode == "ro" and immutable
        conn = sqlite3.connect(staged.as_uri() + "?mode=ro&immutable=1", uri=True, factory=IntegrityConnection)
        opened.append(conn)
        return conn

    connect = Mock(side_effect=open_snapshot)
    monkeypatch.setattr(sqlite_backup, "_connect", connect)
    response = client.post("/api/dashboard/income/restore", json={"path": str(path)})
    assert response.status_code == 400
    connect.assert_called_once()
    assert connect.call_args.args[0].name == "snapshot.db"
    assert connect.call_args.args[1:] == ("ro",)
    assert connect.call_args.kwargs == {"immutable": True}
    assert checks == ["PRAGMA integrity_check"]
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError):
        opened[0].execute("SELECT 1")
    dashboard._get_db.assert_not_called()
    dashboard._pbdata_stop.assert_not_called()
    dashboard._pbdata_start.assert_not_called()
    assert dashboard.restart_block_reason() == ""
    with acquire_master_update_lock(tmp_path):
        pass


@pytest.mark.parametrize("outcome,status", [
    (None, 200), (sqlite_backup.InvalidBackupError, 400),
    (sqlite_backup.RestoreBusyError, 409), (DatabaseBusyError, 409), (MasterUpdateBusyError, 409),
    (RuntimeError, 500), (409, 409), (500, 500),
])
def test_income_restore_recovers_process_state(client, monkeypatch, tmp_path, outcome, status):
    """Native restore never controls PBData and clears its blocker on every exit."""
    path = tmp_path / "external-private" / "backup ?#.db"

    def restore(source, destination, approved_root):
        """Observe the blocker, master reservation and DB-exclusive lease in the callback."""
        assert source == path
        assert destination == tmp_path / "data" / "pbgui.db"
        assert approved_root == tmp_path / "data" / "backup" / "db"
        assert dashboard._income_restore_active.is_set()
        assert "Income database restore" in dashboard.restart_block_reason()
        with pytest.raises(MasterUpdateBusyError):
            acquire_master_update_lock(tmp_path)
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(tmp_path)
        if isinstance(outcome, int):
            raise HTTPException(status_code=outcome, detail="Preserved HTTP failure")
        if outcome is not None:
            raise outcome(f"private diagnostic: {path}")

    callback = Mock(side_effect=restore)
    monkeypatch.setattr(sqlite_backup, "restore_sqlite_backup", callback)
    response = client.post("/api/dashboard/income/restore", json={"path": str(path)})
    callback.assert_called_once()
    assert response.status_code == status
    dashboard._get_db.assert_not_called()
    dashboard._pbdata_stop.assert_not_called()
    dashboard._pbdata_start.assert_not_called()
    assert not dashboard._income_restore_active.is_set()
    assert dashboard.restart_block_reason() == ""
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass
    if outcome is None:
        assert response.json() == {"ok": True}
    elif isinstance(outcome, int):
        assert response.json()["detail"] == "Preserved HTTP failure"
        dashboard._log.assert_not_called()
    else:
        dashboard._log.assert_called_once()
        assert dashboard._log.call_args.kwargs["meta"] == {"exception_type": outcome.__name__}
        assert dashboard._log.call_args.kwargs["level"] == ("ERROR" if status == 500 else "WARNING")
    for private in ("private diagnostic", str(path)):
        assert private not in str(dashboard._log.call_args)
        assert private not in response.text


@pytest.mark.parametrize("since", [None, 1000])
@pytest.mark.parametrize("failure", ["connect", "fetch", "http", "none", "cached"])
def test_orders_candle_failures_preserve_db_data(client, monkeypatch, since, failure):
    """Both candle branches handle failures while preserving cached and DB data."""
    user = SimpleNamespace(name="alice", exchange="bybit")
    db = SimpleNamespace(
        fetch_positions=lambda _: [(1, "BTCUSDT", 0, 2, 10, 100, "alice", "long")],
        fetch_prices=lambda _: [(1, "BTCUSDT", 0, 105)],
    )
    monkeypatch.setattr(dashboard, "_get_db", lambda: db)
    monkeypatch.setattr(dashboard, "_get_users", lambda: SimpleNamespace(find_user=lambda _: user))
    orders = [{"side": "sell", "price": 110, "amount": 2}]
    monkeypatch.setattr(dashboard, "_dashboard_orders_for_position", lambda *_, **__: (orders, False, "db"))
    candles = [[1, 100, 110, 90, 105, 5]]
    exchange = Mock()
    exchange.fetch_ohlcv.return_value = candles
    connect = Mock(return_value=exchange)
    monkeypatch.setattr(dashboard, "_get_exchange", connect)
    if failure == "connect":
        connect.side_effect = RuntimeError("private diagnostic")
    elif failure == "fetch":
        exchange.fetch_ohlcv.side_effect = TimeoutError("private diagnostic")
    elif failure == "http":
        connect.side_effect = HTTPException(status_code=409, detail="Blocked")
    elif failure == "cached":
        dashboard._ohlcv_cache_get.return_value = candles
    params = {"user": "alice", "symbol": "BTCUSDT", "live": "false"}
    if since is not None:
        params["since"] = since
    response = client.get("/api/dashboard/orders_data", params=params)
    if failure == "http":
        assert response.status_code == 409
        assert response.json()["detail"] == "Blocked"
        return
    assert response.status_code == 200
    result = response.json()
    assert result["orders"] == orders
    assert result["current_price"] == 105
    assert result["position"] == {"entry": 100, "size": 2, "upnl": 10, "side": "long"}
    if failure in {"connect", "fetch"}:
        assert result["candles"] == []
        dashboard._log.assert_called_once()
        assert "private diagnostic" not in str(dashboard._log.call_args)
        dashboard._ohlcv_cache_put.assert_not_called()
    else:
        assert result["candles"] == [{"t": 1, "o": 100, "h": 110, "l": 90, "c": 105, "v": 5}]
        if failure == "cached" and since is None:
            connect.assert_not_called()
        else:
            connect.assert_called_once_with(user)
            kwargs = {"timeframe": "4h", "limit": 500}
            if since is not None:
                kwargs["since"] = since
            exchange.fetch_ohlcv.assert_called_once_with("BTC/USDT:USDT", "futures", **kwargs)


@pytest.mark.parametrize("since", [None, 1000])
def test_orders_default_live_mode_falls_back_after_connection_failure(client, monkeypatch, since):
    """The default live mode still returns DB positions and orders during an outage."""
    user = SimpleNamespace(name="alice", exchange="bybit")
    db = SimpleNamespace(
        fetch_positions=lambda _: [(1, "BTCUSDT", 0, 2, 10, 100, "alice", "long")],
        fetch_prices=lambda _: [(1, "BTCUSDT", 0, 105)],
        fetch_orders_by_symbol=lambda *_: [(1, "BTCUSDT", 0, 2, 110, "sell")],
    )
    monkeypatch.setattr(dashboard, "_get_db", lambda: db)
    monkeypatch.setattr(dashboard, "_get_users", lambda: SimpleNamespace(find_user=lambda _: user))
    monkeypatch.setattr(dashboard, "_get_exchange", Mock(side_effect=ConnectionError("offline")))
    params = {"user": "alice", "symbol": "BTCUSDT"}
    if since is not None:
        params["since"] = since
    response = client.get("/api/dashboard/orders_data", params=params)
    assert response.status_code == 200
    result = response.json()
    assert result["candles"] == []
    assert result["position"] == {"entry": 100, "size": 2, "upnl": 10, "side": "long"}
    assert result["orders"] == [{"price": 110, "amount": 2, "side": "sell"}]
    assert result["orders_source"] == "db"
    assert result["current_price"] == 105
