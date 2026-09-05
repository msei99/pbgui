"""Isolated HTTP and numeric regressions for calculator issues 173 through 179."""

import copy
import json
import re
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import balance_calc


def _config():
    """Return a finite enabled long side with a disabled short side."""
    return {"live": {"approved_coins": {"long": ["BTC"], "short": []}},
            "bot": {"long": {"n_positions": 1, "total_wallet_exposure_limit": 1,
                             "entry_initial_qty_pct": 1}, "short": {}}}


def _row(coin="BTC", minimum=10, **extra):
    """Return a synthetic eligible mapping row, without exchange IO."""
    return {"coin": coin, "quote": "USDT", "active": True, "swap": True, "linear": True,
            "min_order_price": minimum, **extra}


@pytest.fixture
def client(monkeypatch, tmp_path):
    """Expose only calculator routes with auth, mappings, and run roots isolated."""
    monkeypatch.setattr(balance_calc, "RUN_V7_DIR", tmp_path / "run_v7")
    monkeypatch.setattr(balance_calc, "RUN_V8_DIR", tmp_path / "run_v8")
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: [_row()])
    monkeypatch.setattr(balance_calc, "_draft_store", {})
    app = FastAPI(root_path="/pbgui")
    app.include_router(balance_calc.router, prefix="/api/balance-calc")
    app.dependency_overrides[balance_calc.require_auth] = lambda: object()
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


@pytest.mark.parametrize("parameter", ["instance", "instance_version", "draft_id", "exchange"])
def test_page_query_values_cannot_terminate_scripts_or_inject_later_markers(client, parameter):
    """Actual page responses preserve query values as inert inline JSON."""
    payload = '</script><script>bad()</script>&%%EXCHANGES%%"%%VERSION%%"'
    response = client.get("/api/balance-calc/main_page", params={parameter: payload})
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert "</script><script>bad()" not in response.text
    variable = {"instance": "INIT_INSTANCE", "instance_version": "INIT_VERSION",
                "draft_id": "DRAFT_ID", "exchange": "INIT_EXCHANGE"}[parameter]
    encoded = re.search(rf"var {variable}\s*=\s*(.*);", response.text)[1]
    assert json.loads(encoded) == payload
    assert 'var API_BASE     = "/pbgui/api/balance-calc";' in response.text
    assert 'window.PBGUI_BASE_PREFIX = "/pbgui";' in response.text
    assert 'src="/pbgui/app/pbgui_nav.js?' in response.text


@pytest.mark.parametrize(("age", "status"), [(0, 200), (599.99, 200), (600, 404), (601, 404)])
def test_draft_expiry_is_enforced_without_another_post(client, monkeypatch, age, status):
    """The GET boundary expires drafts even when no new draft triggers cleanup."""
    clock = SimpleNamespace(now=1000)
    monkeypatch.setattr(balance_calc, "time", SimpleNamespace(monotonic=lambda: clock.now))
    draft = client.post("/api/balance-calc/draft", json={"config": {"synthetic": True}}).json()["draft_id"]
    clock.now += age
    response = client.get(f"/api/balance-calc/draft/{draft}")
    assert response.status_code == status
    assert (draft in balance_calc._draft_store) == (status == 200)
    assert client.get(f"/api/balance-calc/draft/{draft}").status_code == status
    assert client.get("/api/balance-calc/draft/unknown").status_code == 404


def test_new_drafts_remove_expired_entries_at_the_same_boundary(client, monkeypatch):
    """POST cleanup and GET validation use the same monotonic TTL boundary."""
    monkeypatch.setattr(balance_calc, "time", SimpleNamespace(monotonic=lambda: 1000))
    balance_calc._draft_store.update({"expired": (400, {}), "fresh": (401, {})})
    assert client.post("/api/balance-calc/draft", json={"config": {}}).status_code == 200
    assert "expired" not in balance_calc._draft_store
    assert "fresh" in balance_calc._draft_store


@pytest.mark.parametrize("version", ["v7", "v8"])
def test_config_file_uses_versioned_loader(client, monkeypatch, version):
    """File inputs below either isolated root use the corresponding config pipeline."""
    root = getattr(balance_calc, "RUN_" + version.upper() + "_DIR")
    path = root / "bot" / "config.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="utf-8")
    calls = []

    def loader(selected_path, **kwargs):
        """Record the selected loader without invoking a bot runtime."""
        calls.append((selected_path, kwargs))
        return _config()

    def forbidden(*args, **kwargs):
        """Fail if the wrong runtime loader is invoked."""
        raise AssertionError("Wrong config loader")

    monkeypatch.setattr(balance_calc, "load_pb7_config", loader if version == "v7" else forbidden)
    monkeypatch.setattr(balance_calc, "load_pb8_config", loader if version == "v8" else forbidden)
    response = client.post("/api/balance-calc/calculate", json={"config_file": str(path), "exchange": "binance"})
    assert response.status_code == 200
    assert response.json()["recommendation"]["symbol"] == "BTC"
    assert calls == [(path, {"neutralize_added": False} if version == "v7" else {})]


@pytest.mark.parametrize("value", [12345, 0, True, False, [], ["bad"], {}, {"bad": 1}, None, "", " ", "a\x00b", "a\\b", "../outside"])
def test_invalid_config_file_types_and_paths_are_client_errors(client, value):
    """Malformed path inputs never become unhandled Path constructor errors."""
    response = client.post("/api/balance-calc/calculate", json={"config_file": value, "exchange": "binance"})
    assert response.status_code == 422
    assert "detail" in response.json()


@pytest.mark.parametrize("kind", ["outside", "missing", "directory", "file_symlink", "parent_symlink", "root_symlink", "traversal"])
def test_config_file_rejects_unapproved_locations_before_loading(client, monkeypatch, tmp_path, kind):
    """All filesystem fixtures stay under tmp_path and rejected files are never loaded."""
    root = balance_calc.RUN_V8_DIR
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "config.json").write_text("{}", encoding="utf-8")
    root.mkdir()
    if kind == "outside":
        path = outside / "config.json"
    elif kind == "file_symlink":
        path = root / "config.json"
        path.symlink_to(outside / "config.json")
    elif kind == "parent_symlink":
        (root / "bot").symlink_to(outside, target_is_directory=True)
        path = root / "bot" / "config.json"
    elif kind == "root_symlink":
        link = tmp_path / "linked_root"
        link.symlink_to(outside, target_is_directory=True)
        monkeypatch.setattr(balance_calc, "RUN_V8_DIR", link)
        path = link / "config.json"
    elif kind == "directory":
        path = root
    elif kind == "traversal":
        path = root / ".." / "outside" / "config.json"
    else:
        path = root / "missing.json"
    response = client.post("/api/balance-calc/calculate", json={"config_file": str(path), "exchange": "binance"})
    assert response.status_code == 422


@pytest.mark.parametrize("status", [409, 422])
def test_config_loader_errors_preserve_expected_http_status(client, monkeypatch, status):
    """A restart blocker is preserved and a parse failure becomes a client error."""
    path = balance_calc.RUN_V8_DIR / "config.json"
    path.parent.mkdir()
    path.write_text("invalid fixture", encoding="utf-8")

    def fail(path):
        """Simulate a loader failure without opening the fixture."""
        if status == 409:
            raise HTTPException(409, "Fixture restart blocker")
        raise ValueError("Fixture parse failure")

    monkeypatch.setattr(balance_calc, "load_pb8_config", fail)
    response = client.post("/api/balance-calc/calculate", json={"config_file": str(path), "exchange": "binance"})
    assert response.status_code == status


@pytest.mark.parametrize("field", ["n_positions", "total_wallet_exposure_limit", "entry_initial_qty_pct"])
@pytest.mark.parametrize("value", ["nan", "inf", "-inf", -1, "bad", True])
@pytest.mark.parametrize("side", ["long", "short"])
def test_bot_parameters_return_serializable_http_validation_errors(client, field, value, side):
    """Finite/range checks cover disabled sides too, preventing serialization-time 500s."""
    config = _config()
    config["bot"][side][field] = value
    response = client.post("/api/balance-calc/calculate", json={"config": config, "exchange": "binance"})
    assert response.status_code == 422
    assert field in response.json()["detail"]


@pytest.mark.parametrize("kind", ["ema_anchor", "trailing_grid_v7"])
def test_pb8_non_finite_active_strategy_is_rejected(client, kind):
    """PB8 tests use the real nested active strategy schema, not a flat strategy dict."""
    config = _config()
    config["live"]["strategy_kind"] = kind
    strategy = {"base_qty_pct": "nan"} if kind == "ema_anchor" else {"entry": {"initial_qty_pct": "inf"}}
    config["bot"]["long"] = {"risk": {"n_positions": 1, "total_wallet_exposure_limit": 1}, "strategy": {kind: strategy}}
    response = client.post("/api/balance-calc/calculate", json={"config": config, "exchange": "binance"})
    assert response.status_code == 422


@pytest.mark.parametrize("field", ["n_positions", "total_wallet_exposure_limit", "entry_initial_qty_pct"])
def test_zero_parameters_keep_sides_disabled(client, field):
    """Zero still disables sizing instead of becoming a validation failure."""
    config = _config()
    config["bot"]["long"][field] = 0
    response = client.post("/api/balance-calc/calculate", json={"config": config, "exchange": "binance"})
    assert response.status_code == 200
    assert response.json()["recommendation"] is None


@pytest.mark.parametrize("layout", ["pb7", "ema_anchor", "trailing_grid_v7"])
@pytest.mark.parametrize("side", ["long", "short"])
@pytest.mark.parametrize("field", ["n_positions", "total_wallet_exposure_limit", "entry_initial_qty_pct"])
@pytest.mark.parametrize(("value", "status"), [
    ("1e-400", 422), ("-1e-400", 422), (0, 200), (0.0, 200),
    ("0e-400", 200), ("-0e-400", 200), (None, 200), (Ellipsis, 200), ("0.01", 200),
])
def test_raw_decimal_sizing_preserves_underflow_sign_and_real_zero(client, layout, side, field, value, status):
    """PB7/PB8 reject nonzero underflow even on otherwise disabled sides, but retain defaults."""
    config = _config()
    target = config["bot"][side]
    key = field
    if layout != "pb7":
        config["live"]["strategy_kind"] = layout
        risk = {name: number for name, number in target.items() if name != "entry_initial_qty_pct"}
        qty = target.get("entry_initial_qty_pct", 0)
        strategy = {"base_qty_pct": qty} if layout == "ema_anchor" else {"entry": {"initial_qty_pct": qty}}
        config["bot"][side] = {"risk": risk, "strategy": {layout: strategy}}
        if field == "entry_initial_qty_pct":
            target = strategy if layout == "ema_anchor" else strategy["entry"]
            key = "base_qty_pct" if layout == "ema_anchor" else "initial_qty_pct"
        else:
            target = risk
    if value is Ellipsis:
        target.pop(key, None)
    else:
        target[key] = value

    response = client.post("/api/balance-calc/calculate", json={"config": config, "exchange": "binance"})
    assert response.status_code == status
    if status == 422:
        assert field in response.json()["detail"]
    else:
        expected = 0 if value is None or value is Ellipsis else float(value)
        assert response.json()["bot_params"][side][field] == expected
        if expected == 0:
            assert response.json()["balance_" + side] == []


@pytest.mark.parametrize(("positions", "exposure", "qty"), [
    (1e308, 1e-308, 1), (1e-308, 1e308, 1), (1, 1e-308, 1), (1, 1, 1e-308),
])
def test_finite_inputs_with_non_finite_derived_sizing_are_rejected(client, positions, exposure, qty):
    """Overflow and underflow are validated after arithmetic, not only on inputs."""
    config = _config()
    config["bot"]["long"] = dict(n_positions=positions, total_wallet_exposure_limit=exposure, entry_initial_qty_pct=qty)
    response = client.post("/api/balance-calc/calculate", json={"config": config, "exchange": "binance"})
    assert response.status_code == 422


@pytest.mark.parametrize(("minimum", "qty"), [(1.7e308, 1), (1e-308, 1e308)])
def test_recommendation_overflow_and_balance_underflow_are_client_errors(client, monkeypatch, minimum, qty):
    """Even finite sizing denominators can produce an unsupported result or buffer."""
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: [_row(minimum=minimum)])
    config = _config()
    config["bot"]["long"]["entry_initial_qty_pct"] = qty
    response = client.post("/api/balance-calc/calculate", json={"config": config, "exchange": "binance"})
    assert response.status_code == 422


def test_mapping_numeric_overflow_and_optional_nan_cannot_escape_as_non_json(client, monkeypatch):
    """Derived mapping values are checked and optional leverage is safely nullable."""
    rows = [_row("BROKEN", 0, price_last=1e308, contract_size=1e308, min_amount=1),
            _row(max_leverage=float("nan"))]
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: rows)
    response = client.post("/api/balance-calc/calculate", json={"config": _config(), "exchange": "binance"})
    assert response.status_code == 200
    assert response.json()["coin_infos"][0]["max_lev"] is None


@pytest.mark.parametrize(("coin", "symbol", "approved"), [
    ("SHIB", "1000SHIBUSDT", "SHIB"), ("SHIB", "1000SHIBUSDT", "1000SHIBUSDT"),
    ("TUSD", "TUSDUSDT", "TUSD"), ("TUSD", "TUSDUSDT", "TUSDUSDT"),
    ("SHIB", "1000SHIB/USDT:USDT", "1000SHIB/USDT:USDT"),
    ("KAVA", "KAVAUSDT", "KAVAUSDT"), ("BONK", "kBONKUSDT", "kBONKUSDT"),
    ("XYZ-AAPL", "xyz:AAPL", "xyz:AAPL"),
])
def test_symbols_resolve_to_mapping_coin_without_prefix_guessing(monkeypatch, coin, symbol, approved):
    """Both canonical names and exchange identities resolve via synthetic mappings."""
    config = _config()
    config["live"]["approved_coins"]["long"] = [approved]
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: [_row(coin, symbol=symbol)])
    result = balance_calc._calculate(config, "binance")
    assert result["balance_long"] == [{"coin": coin, "balance": 10}]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("approved", ["all", ["CATUSDT", "1000CATUSDT"]])
def test_separate_multiplier_markets_and_ignored_aliases_remain_distinct(monkeypatch, reverse, approved):
    """Persisted CAT/1000CAT identities cannot collapse through alias normalization."""
    rows = [_row("CAT", symbol="CATUSDT", base="CAT"),
            _row("1000CAT", minimum=20, symbol="1000CATUSDT", base="1000CAT")]
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: list(reversed(rows)) if reverse else rows)
    config = _config()
    config["live"]["approved_coins"] = approved
    config["live"]["ignored_coins"] = {"long": ["CATUSDT"], "short": ["1000CATUSDT"]}
    result = balance_calc._calculate(config, "binance")
    assert result["balance_long"] == [{"coin": "1000CAT", "balance": 20}]
    assert {row["coin"] for row in result["coin_infos"]} == {"CAT", "1000CAT"}


def test_dynamic_ignore_uses_the_same_mapping_aliases(monkeypatch):
    """The dynamic filter output resolves through the same canonical mapping as config lists."""
    import PBCoinData

    class FakeCoinData:
        """In-memory dynamic approval source."""

        def filter_mapping(self, **kwargs):
            """Return an exchange symbol rather than a canonical base."""
            return ["1000SHIBUSDT"], []

    monkeypatch.setattr(PBCoinData, "CoinData", FakeCoinData)
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: [_row("SHIB", symbol="1000SHIBUSDT")])
    config = _config()
    config["pbgui"] = {"dynamic_ignore": True}
    assert balance_calc._calculate(config, "binance")["recommendation"]["symbol"] == "SHIB"


@pytest.mark.parametrize(("a", "b", "recommended"), [(100.001, 100.004, 120), (99.999, 100.001, 120)])
def test_raw_balances_choose_dominant_coin_and_buffer(monkeypatch, a, b, recommended):
    """Cent ties cannot hide the true dominant coin or cross a buffer threshold."""
    config = _config()
    config["live"]["approved_coins"]["long"] = ["A", "B"]
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: [_row("A", a), _row("B", b)])
    result = balance_calc._calculate(config, "binance")
    assert result["recommendation"]["symbol"] == "B"
    assert result["recommendation"]["recommended_balance"] == recommended
    assert result["balance_long"] == [{"coin": "B", "balance": 100}, {"coin": "A", "balance": 100}]
    assert all(set(item) == {"coin", "balance"} for item in result["balance_long"])


def test_raw_balances_choose_dominant_side(monkeypatch):
    """Long/short dominance is decided before cent rounding, too."""
    config = _config()
    config["live"]["approved_coins"] = {"long": ["BTC"], "short": ["BTC"]}
    config["bot"]["short"] = copy.deepcopy(config["bot"]["long"])
    config["bot"]["short"]["entry_initial_qty_pct"] = 1 / 99.999
    config["bot"]["long"]["entry_initial_qty_pct"] = 1 / 100.001
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: [_row(minimum=1)])
    recommendation = balance_calc._calculate(config, "binance")["recommendation"]
    assert recommendation["side"] == "long"
    assert recommendation["recommended_balance"] == 120


def test_genuine_raw_ties_keep_deterministic_selection(monkeypatch):
    """True coin ties retain alphabetical ordering and true side ties retain short."""
    config = _config()
    config["live"]["approved_coins"] = ["B", "A"]
    config["bot"]["short"] = copy.deepcopy(config["bot"]["long"])
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: [_row("B"), _row("A")])
    result = balance_calc._calculate(config, "binance")
    assert result["recommendation"]["symbol"] == "A"
    assert result["recommendation"]["side"] == "short"


def test_recommendation_does_not_recompute_from_rounded_minimum(monkeypatch):
    """Sub-micro precision in minimum orders remains significant to the buffer."""
    config = _config()
    config["bot"]["long"]["entry_initial_qty_pct"] = 0.01
    monkeypatch.setattr(balance_calc, "_load_mapping", lambda exchange: [_row(minimum=1.0000004)])
    result = balance_calc._calculate(config, "binance")
    assert result["coin_infos"][0]["min_order_price"] == 1
    assert result["recommendation"]["recommended_balance"] == 120
