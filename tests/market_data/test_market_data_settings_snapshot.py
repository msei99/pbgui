"""Offline settings snapshots, consistency, write isolation and latency for #418."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from statistics import median
import time

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
import pytest

import api.market_data as api
import hyperliquid_best_1m as best
import market_data as md
from credential_store import CredentialStore
from market_symbol_mapping import disambiguate_multiplier_market_coins


def write_mapping(root, exchange, rows):
    """Publish one isolated mapping atomically, as Coin Data does."""
    path = root / "data" / "coindata" / exchange / "mapping.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(rows), encoding="utf-8")
    tmp.replace(path)
    return path


def row(coin="BTC", **values):
    """Return one eligible synthetic linear swap."""
    return {"coin": coin, "quote": "USDT", "active": True, "linear": True, "swap": True, **values}


def file_signature(path):
    """Check write identity at nanosecond resolution while allowing read access times."""
    stat = path.stat()
    return stat.st_mtime_ns, stat.st_size, stat.st_ino


@pytest.fixture
def real_coindata():
    """Load real pure filters regardless of other tests' PBCoinData module doubles."""
    source = Path(__file__).resolve().parents[2] / "PBCoinData.py"
    spec = importlib.util.spec_from_file_location("snapshot_real_coindata", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    """All reads/writes and credential stores belong to the temporary directory."""
    import pbgui_purefunc

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(api, "PBGDIR", tmp_path)
    monkeypatch.setattr(md, "_COIN_SNAPSHOT_CACHE", {})
    monkeypatch.setattr(api, "_SETTINGS_TIINGO_CACHE", None)
    monkeypatch.setattr(api, "_SETTINGS_TIINGO_STORE", None)
    monkeypatch.setattr(md, "get_market_data_config_path", lambda: tmp_path / "config.json")
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: tmp_path / "pbgui.ini")
    monkeypatch.setattr(best, "_tiingo_usage_state_path", lambda: tmp_path / "tiingo_usage_state.json")
    monkeypatch.setattr(api, "load_aws_profile_credentials", lambda _: {})
    monkeypatch.setattr(api, "load_aws_profile_region", lambda _: "")
    monkeypatch.setattr(api, "save_aws_profile_credentials", lambda **kwargs: None)
    monkeypatch.setattr(api, "save_aws_profile_region", lambda **kwargs: None)
    return tmp_path


def forbidden(*args, **kwargs):
    """Fail if a lightweight settings read enters a constructor or live path."""
    raise AssertionError("Forbidden constructor/network/bootstrap/write path")


def test_settings_cold_and_warm_are_local_readonly_and_fast(isolated, monkeypatch):
    """Authenticated fixture GETs perform no bootstrap, WAN or usage persistence."""
    import cmc_runtime
    import credential_rolling_bootstrap
    import hyperliquid_api
    import api.auth as auth

    root = isolated
    write_mapping(root, "hyperliquid", [row("xyz:TSLA", quote="USDC", is_hip3=True, dex="xyz", open_interest=10)] + [row(f"TEST{i}") for i in range(400)])
    store = CredentialStore(root / "data" / "credentials")
    store.create_tradfi("tiingo", {"api_key": "synthetic-no-provider-key"})
    files = [root / "pbgui.ini", root / "data/credentials/legacy_shadow/state.json", root / "tiingo_usage_state.json"]
    for path in files:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[market_data]\n" if path.suffix == ".ini" else '{"entries": {}}', encoding="utf-8")
    before = [file_signature(path) for path in files]
    attempts = {"CoinData": 0, "CMC": 0, "bootstrap": 0, "Hyperliquid": 0, "usage_write": 0}

    def counter(name):
        """Record even calls swallowed by a fallback's exception handler."""
        def fail(*args, **kwargs):
            """A forbidden call must remain observable even if its error is caught."""
            attempts[name] += 1
            forbidden()
        return fail

    monkeypatch.setattr(md, "CoinData", counter("CoinData"))
    monkeypatch.setattr(cmc_runtime, "build_cmc_pool_client", counter("CMC"))
    monkeypatch.setattr(credential_rolling_bootstrap, "bootstrap_local_legacy_credentials", counter("bootstrap"))
    monkeypatch.setattr(hyperliquid_api, "resolve_hyperliquid_coin_name", counter("Hyperliquid"))
    monkeypatch.setattr(api, "_active_tiingo_credential", forbidden)
    monkeypatch.setattr(api, "get_tiingo_runtime_usage", forbidden)
    monkeypatch.setattr(best, "_tiingo_usage_file_lock", forbidden)
    monkeypatch.setattr(best, "_tiingo_persist_usage_state", counter("usage_write"))
    app = FastAPI()
    app.include_router(api.router, prefix="/api")
    monkeypatch.setattr(auth, "PBGDIR", root)
    session = auth.generate_token("isolated-settings-test")
    durations = []
    with TestClient(app) as client:
        assert client.get("/api/market-data/settings/hyperliquid").status_code == 401
        client.cookies.set(auth.SESSION_COOKIE_NAME, session.token)
        for _ in range(12):
            start = time.perf_counter()
            response = client.get("/api/market-data/settings/hyperliquid")
            durations.append((time.perf_counter() - start) * 1000)
            assert response.status_code == 200
            payload = response.json()
            assert "xyz:TSLA" in payload["coin_options"]
            assert payload["settings"]["tiingo_configured"] is True
            assert "synthetic-no-provider-key" not in response.text
            assert "usage_id" not in response.text
    assert [file_signature(path) for path in files] == before
    assert attempts == {name: 0 for name in attempts}
    assert not (root / "tiingo_usage_state.json.lock").exists()
    assert median(durations[1:]) < 50, durations
    # Available to pytest's captured evidence without reading any runtime data.
    print(f"isolated settings GET median_ms={median(durations[1:]):.3f}; samples={len(durations)-1}; excluded_calls={attempts}")


@pytest.mark.parametrize("exchange", ["binance", "bybit", "bitget", "okx", "kucoin", "hyperliquid"])
def test_snapshot_matches_active_filter_semantics(isolated, monkeypatch, exchange, real_coindata):
    """Compare the pure path to real CoinData filters without constructing CoinData."""
    real = real_coindata
    monkeypatch.setattr(md, "compute_coin_name", real.compute_coin_name)
    monkeypatch.setattr(md, "normalize_symbol", real.normalize_symbol)
    rows = [row(), row("ETH", quote="USDC"), row("OFF", active=False), row("SPOT", swap=False),
            row("INV", linear=False), row("ZERO", open_interest=0), row("NEG", open_interest=-1),
            row("OTHER", is_hip3=True, dex="flx"), row("xyz:TSLA", is_hip3=True, dex="xyz", open_interest=1),
            row("", symbol="XBTUSDTM" if exchange == "kucoin" else "BTCUSDT"),
            row("CAT", base="CAT"), row("CAT", base="1000CAT")]
    model = object.__new__(real.CoinData)
    model.load_mapping = lambda **kwargs: disambiguate_multiplier_market_coins(rows)
    approved, _ = model.filter_mapping(exchange, market_cap_min_m=0, vol_mcap_max=float("inf"), only_cpt=False,
                                       notices_ignore=False, tags=[], quote_filter=["USDT"] if exchange in {"okx", "bitget"} else None,
                                       active_only=True)
    write_mapping(isolated, exchange, rows)
    expected = sorted({md._canonical_enabled_coin(exchange, coin) for coin in approved})
    assert md.get_local_market_data_coin_snapshot(exchange).coin_options == expected


def test_legacy_hip3_row_aliases(isolated, monkeypatch, real_coindata):
    """Rows without coin use the shared symbol normalizer and canonical xyz names."""
    monkeypatch.setattr(md, "normalize_symbol", real_coindata.normalize_symbol)
    write_mapping(isolated, "hyperliquid", [row("", ccxt_symbol="XYZ-TSLA/USDC:USDC", quote="USDC")])
    assert md.get_local_market_data_coin_snapshot("hyperliquid").coin_options == ["xyz:TSLA"]


@pytest.mark.parametrize("exchange,symbol", [("bybit", "BTCUSDT"), ("kucoin", "XBTUSDTM"),
                                           ("okx", "BTC-USDT-SWAP"), ("bitget", "BTCUSDT")])
def test_empty_coin_snapshot_prefers_native_symbol_outside_hyperliquid(isolated, monkeypatch, real_coindata, exchange, symbol):
    """A CCXT display symbol must not replace the native fallback of standard exchanges."""
    monkeypatch.setattr(md, "compute_coin_name", real_coindata.compute_coin_name)
    write_mapping(isolated, exchange, [row("", symbol=symbol, ccxt_symbol="BTC/USDT:USDT")])
    assert md.get_local_market_data_coin_snapshot(exchange).coin_options == ["BTC"]
    if exchange != "kucoin":  # KuCoin has local snapshots, but no Market Data settings page.
        assert api.get_market_data_settings(exchange, None)["coin_options"] == ["BTC"]


@pytest.mark.parametrize("empty_coin", [False, True])
def test_snapshot_includes_all_active_hip3_dexes(isolated, monkeypatch, real_coindata, empty_coin):
    """The local picker uses complete tickers and explicit rows without live resolution."""
    monkeypatch.setattr(md, "normalize_symbol", real_coindata.normalize_symbol)
    monkeypatch.setattr(md, "CoinData", forbidden)
    monkeypatch.setattr("hyperliquid_api.resolve_hyperliquid_coin_name", forbidden)
    rows = [row("" if empty_coin else f"{dex.upper()}-FOOUSD", quote="USDC", is_hip3=True, dex=dex,
                symbol=str(i), ccxt_symbol=f"{dex.upper()}-FOOUSD/USDC:USDC", open_interest=100)
            for i, dex in enumerate(["xyz", "para", "mkts", "flx", "newdex"])]
    write_mapping(isolated, "hyperliquid", rows)
    expected = ["flx:FOOUSD", "mkts:FOOUSD", "newdex:FOOUSD", "para:FOOUSD", "xyz:FOOUSD"]
    assert md.get_local_market_data_coin_snapshot("hyperliquid").coin_options == expected
    assert api.get_market_data_settings("hyperliquid", None)["coin_options"] == expected
    rows[1]["active"] = False
    write_mapping(isolated, "hyperliquid", rows)
    assert "para:FOOUSD" not in md.get_local_market_data_coin_snapshot("hyperliquid").coin_options


def test_missing_empty_invalid_and_atomic_replacement(isolated):
    """Every on-disk generation is visible immediately, including delete/recreate."""
    root = isolated
    assert md.get_local_market_data_coin_snapshot("okx") == md.MarketDataCoinSnapshot([], False)
    path = write_mapping(root, "okx", [])
    assert md.get_local_market_data_coin_snapshot("okx") == md.MarketDataCoinSnapshot([], True)
    write_mapping(root, "okx", [row()])
    first = md.get_local_market_data_coin_snapshot("okx")
    first.coin_options.append("MUTATED")
    assert md.get_local_market_data_coin_snapshot("okx").coin_options == ["BTC"]
    write_mapping(root, "okx", [row("ETH")])
    assert md.get_local_market_data_coin_snapshot("okx").coin_options == ["ETH"]
    path.unlink()
    assert md.get_local_market_data_coin_snapshot("okx").coin_data_available is False
    path.write_text("invalid", encoding="utf-8")
    with pytest.raises(md.CoinSnapshotReadError):
        md.get_local_market_data_coin_snapshot("okx")
    write_mapping(root, "okx", [row("SOL")])
    assert md.get_local_market_data_coin_snapshot("okx").coin_options == ["SOL"]


@pytest.mark.parametrize("value", [{}, [None], "invalid", [{"coin": {"invalid": True}}]])
def test_invalid_structure_never_stale_success(isolated, value):
    """Invalid mapping structures are errors even after an earlier valid snapshot."""
    write_mapping(isolated, "okx", [row()])
    md.get_local_market_data_coin_snapshot("okx")
    write_mapping(isolated, "okx", value)
    with pytest.raises(md.CoinSnapshotReadError):
        md.get_local_market_data_coin_snapshot("okx")


def test_root_exchange_separation_and_boundary(isolated, monkeypatch):
    """The default root matches the cwd writer; aliases and API root agree."""
    write_mapping(isolated, "binance", [row("BTC")])
    write_mapping(isolated, "bybit", [row("ETH")])
    write_mapping(isolated / "other", "binance", [row("SOL")])
    assert api._settings_coin_snapshot("binanceusdm").coin_options == ["BTC"]
    assert md.get_local_market_data_coin_snapshot("bybit").coin_options == ["ETH"]
    assert md.get_local_market_data_coin_snapshot("binance", root=isolated / "other").coin_options == ["SOL"]
    assert md.get_local_market_data_coin_snapshot("binance-usdm").coin_options == ["BTC"]
    for exchange in ["../x", "binance/x", "..", "bad\x00", "unknown"]:
        with pytest.raises(ValueError):
            md.get_local_market_data_coin_snapshot(exchange)
    target = isolated / "outside.json"
    target.write_text("[]")
    path = isolated / "data/coindata/binance/mapping.json"
    path.unlink()
    path.symlink_to(target)
    with pytest.raises(md.CoinSnapshotReadError):
        md.get_local_market_data_coin_snapshot("binance")


def test_concurrent_misses_are_joined(isolated, monkeypatch):
    """Only one derivation serves simultaneous misses and every result is independent."""
    write_mapping(isolated, "okx", [row()])
    calls = []
    original = md._local_mapping_coin_options

    def derive(*args):
        """Record each cache derivation."""
        calls.append(1)
        return original(*args)

    monkeypatch.setattr(md, "_local_mapping_coin_options", derive)
    with ThreadPoolExecutor(max_workers=8) as pool:
        snapshots = list(pool.map(lambda _: md.get_local_market_data_coin_snapshot("okx"), range(20)))
    assert calls == [1]
    snapshots[0].coin_options.clear()
    assert snapshots[1].coin_options == ["BTC"]


def test_unstable_reads_are_bounded_and_permissions_reported(isolated, monkeypatch):
    """No sleeps or unbounded retry on replacement storms or unreadable files."""
    write_mapping(isolated, "okx", [row()])
    calls = []

    def changing(_):
        """Simulate a different inode at every signature check."""
        calls.append(1)
        return (1, 1, len(calls))

    with monkeypatch.context() as patch:
        patch.setattr(md, "_coin_mapping_signature", changing)
        with pytest.raises(md.CoinSnapshotReadError):
            md.get_local_market_data_coin_snapshot("okx")
    assert len(calls) == 4
    original = Path.read_text

    def unreadable(path, *args, **kwargs):
        """Deny only this temporary mapping."""
        if path.name == "mapping.json":
            raise PermissionError("synthetic")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", unreadable)
    with pytest.raises(md.CoinSnapshotReadError):
        md.get_local_market_data_coin_snapshot("okx")


def test_one_replacement_during_read_uses_second_generation(isolated, monkeypatch):
    """An atomic replacement during the first read triggers exactly one retry."""
    path = write_mapping(isolated, "okx", [row("OLD")])
    original = Path.read_text
    reads = []

    def replace_once(selected, *args, **kwargs):
        """Publish another fixture generation after capturing the old bytes."""
        value = original(selected, *args, **kwargs)
        if selected == path:
            reads.append(1)
            if len(reads) == 1:
                write_mapping(isolated, "okx", [row("NEW")])
        return value

    monkeypatch.setattr(Path, "read_text", replace_once)
    assert md.get_local_market_data_coin_snapshot("okx").coin_options == ["NEW"]
    assert len(reads) == 2


@pytest.mark.parametrize("mapping", [None, []])
@pytest.mark.parametrize("auto", [True, False])
def test_save_without_options_preserves_selection(isolated, mapping, auto):
    """Ordinary saves must not turn unknown availability into a deletion request."""
    md.set_enabled_coins("okx", ["BTC", "ETH"])
    md.set_auto_enable_new_coins("okx", auto)
    if mapping is not None:
        write_mapping(isolated, "okx", mapping)
    payload = api.get_market_data_settings("okx", None)
    assert payload["enabled_coins"] == ["BTC", "ETH"]
    assert payload["missing_saved_coins"] == []
    request = {"settings": {"interval_seconds": 75}, "auto_enable_new_coins": auto}
    if auto or mapping is None:
        request["enabled_coins"] = []
    saved = api.save_market_data_settings("okx", request, None)
    assert saved["success"] is True
    assert md.load_market_data_config().enabled_coins["okx"] == ["BTC", "ETH"]
    assert saved["settings"]["settings"]["interval_seconds"] == 75


def test_explicit_manual_clear_on_valid_empty_mapping(isolated):
    """An explicit manual selection change remains distinct from auto availability."""
    md.set_enabled_coins("okx", ["BTC"])
    write_mapping(isolated, "okx", [])
    api._save_market_data_settings("okx", {"enabled_coins": [], "auto_enable_new_coins": False})
    assert md.load_market_data_config().enabled_coins["okx"] == []


def test_save_prepares_snapshot_and_response_before_mutations(isolated, monkeypatch):
    """A failing preflight cannot partially persist, and response reuse does not reread."""
    write_mapping(isolated, "okx", [row()])
    calls = []
    original = api._settings_coin_snapshot

    def once(exchange):
        """The response must share this exact generation."""
        calls.append(exchange)
        assert len(calls) == 1
        return original(exchange)

    monkeypatch.setattr(api, "_settings_coin_snapshot", once)
    assert api._save_market_data_settings("okx", {"enabled_coins": ["BTC"], "auto_enable_new_coins": False})["enabled_coins"] == ["BTC"]
    assert calls == ["okx"]
    calls.clear()
    before = (isolated / "config.json").read_bytes()
    ini_before = (isolated / "pbgui.ini").read_bytes()
    with pytest.raises(ValueError):
        api._save_market_data_settings("okx", {"enabled_coins": [], "settings": {"interval_seconds": "invalid"}})
    assert (isolated / "config.json").read_bytes() == before
    assert (isolated / "pbgui.ini").read_bytes() == ini_before


def test_snapshot_failure_propagation_and_no_queue_mutation(isolated, monkeypatch):
    """GET/Save preserve 503, while Best 1m uses its established error envelope."""
    write_mapping(isolated, "okx", {})
    for name in ["set_enabled_coins", "set_auto_enable_new_coins", "update_ini", "save_aws_profile_credentials"]:
        monkeypatch.setattr(api, name, forbidden)
    monkeypatch.setattr("task_queue.enqueue_job", forbidden)
    monkeypatch.setattr("task_queue.enqueue_running_job", forbidden)
    monkeypatch.setattr("subprocess.Popen", forbidden)
    for call in [lambda: api.get_market_data_settings("okx", None), lambda: api.save_market_data_settings("okx", {"enabled_coins": []}, None)]:
        with pytest.raises(HTTPException) as caught:
            call()
        assert caught.value.status_code == 503
    for result in [api.get_best_1m_info("okx", None), api.queue_best_1m_job("okx", {"coins": ["BTC"]}, None)]:
        assert result["success"] is False
        assert "local Coin Data" in result["error"]


def test_picker_queue_and_mapping_refresh_share_universe(isolated):
    """A simulated Coin Data publication immediately changes both local consumers."""
    write_mapping(isolated, "okx", [row("BTC")])
    assert api.get_market_data_settings("okx", None)["coin_options"] == api.get_best_1m_info("okx", None)["coins"] == ["BTC"]
    write_mapping(isolated, "okx", [row("ETH")])
    assert api.get_market_data_settings("okx", None)["coin_options"] == api.get_best_1m_info("okx", None)["coins"] == ["ETH"]
    assert api.queue_best_1m_job("okx", {"coins": ["BTC"]}, None)["success"] is False


@pytest.mark.parametrize("settings", [{"api_timeout_seconds": "nan"}, {"coin_pause_seconds": "inf"},
                                     {"aws_access_key_id": "one-half"}, {"aws_profile": "../bad"},
                                     {"l2book_scan_workers": "invalid"}])
def test_hyperliquid_preflight_prevents_partial_persistence(isolated, monkeypatch, settings):
    """Invalid conversions and AWS inputs cannot mutate any settings destination."""
    for name in ["set_enabled_coins", "set_auto_enable_new_coins", "update_ini", "save_aws_profile_credentials", "save_aws_profile_region", "_touch_exchange_refresh_flag"]:
        monkeypatch.setattr(api, name, forbidden)
    result = api.save_market_data_settings("hyperliquid", {"enabled_coins": [], "settings": settings}, None)
    assert result["success"] is False


def test_hyperliquid_save_without_mapping_or_aws_credentials(isolated, monkeypatch):
    """A fresh installation can save ordinary settings without creating AWS credentials."""
    monkeypatch.setattr(api, "save_aws_profile_credentials", forbidden)
    md.set_enabled_coins("hyperliquid", ["BTC"])
    result = api.save_market_data_settings("hyperliquid", {"enabled_coins": [], "auto_enable_new_coins": True, "settings": {"interval_seconds": 90}}, None)
    assert result["success"] is True
    assert result["settings"]["enabled_coins"] == ["BTC"]
    assert result["settings"]["coin_data_available"] is False


def test_persistence_failure_does_not_report_success(isolated, monkeypatch):
    """Actual write failures remain failures, without claiming a cross-file rollback."""
    write_mapping(isolated, "okx", [row()])

    def fail_write(*args, **kwargs):
        """Simulate a filesystem failure only inside the fixture."""
        raise OSError("synthetic disk error")

    monkeypatch.setattr(api, "update_ini", fail_write)
    assert api.save_market_data_settings("okx", {"enabled_coins": ["BTC"]}, None)["success"] is False


@pytest.mark.parametrize("now,expected", [("2026-12-30T23:30:00", (7, 9, 11, 20)), ("2027-01-01T00:00:00", (0, 0, 0, 0)), ("2026-12-31T00:00:00", (0, 0, 11, 0)), ("2026-12-30T23:31:00", (7, 9, 11, 0)), ("2026-12-30T22:00:00", (0, 9, 11, 5420))])
def test_usage_readonly_time_boundaries(isolated, monkeypatch, now, expected):
    """Rollover and 429 countdown are computed without usage or lock writes."""
    fixed = datetime.fromisoformat(now).replace(tzinfo=timezone.utc)

    class Clock(datetime):
        """Freeze only the usage reader's UTC clock."""

        @classmethod
        def now(cls, tz=None):
            """Return the selected fixture instant."""
            return fixed

    monkeypatch.setattr(best, "datetime", Clock)
    path = isolated / "tiingo_usage_state.json"
    state = {"hour_key": "2026123023", "day_key": "20261230", "month_key": "202612", "hour_requests": 7,
             "day_requests": 9, "month_bytes": 11, "server_429_wait_until_ts": int(datetime(2026, 12, 30, 23, 30, 20, tzinfo=timezone.utc).timestamp())}
    path.write_text(json.dumps({"entries": {"synthetic-id": state}}))
    before = file_signature(path)
    usage = best.read_tiingo_usage_snapshot(usage_id="synthetic-id")
    assert tuple(usage[key] for key in ["hour_requests", "day_requests", "month_bytes", "server_429_wait_remaining_s"]) == expected
    assert file_signature(path) == before
    assert not path.with_suffix(".json.lock").exists()
    path.unlink()
    assert best.read_tiingo_usage_snapshot(usage_id="synthetic-id")["hour_requests"] == 0
    assert not path.exists()


def test_tiingo_catalog_and_generation_invalidation(isolated, monkeypatch):
    """Rotation, activation, deletion and recreation invalidate metadata-only cache."""
    import credential_rolling_bootstrap

    monkeypatch.setattr(credential_rolling_bootstrap, "bootstrap_local_legacy_credentials", forbidden)
    assert api._settings_tiingo_identity() == ("", False, "")
    store = CredentialStore(isolated / "data/credentials")
    record = store.create_tradfi("tiingo", {"api_key": "synthetic-one"})
    first = api._settings_tiingo_identity()
    assert first == (record["id"], True, best._tiingo_usage_key_id("synthetic-one"))
    with monkeypatch.context() as patch:
        patch.setattr(api._SETTINGS_TIINGO_STORE, "load_tradfi_credentials", forbidden)
        assert api._settings_tiingo_identity() == first
    store.update_tradfi(record["id"], credentials={"api_key": "synthetic-two"})
    assert api._settings_tiingo_identity()[2] == best._tiingo_usage_key_id("synthetic-two")
    store.update_tradfi(record["id"], active=False)
    assert api._settings_tiingo_identity() == ("", False, "")
    store.update_tradfi(record["id"], active=True)
    assert api._settings_tiingo_identity()[0] == record["id"]
    store.delete_tradfi(record["id"])
    assert api._settings_tiingo_identity() == ("", False, "")
    store.create_tradfi("tiingo", {"api_key": "synthetic-three"})
    assert api._settings_tiingo_identity()[2] == best._tiingo_usage_key_id("synthetic-three")
    catalog = isolated / "data/credentials/catalog.json"
    saved = catalog.read_bytes()
    catalog.unlink()
    assert api._settings_tiingo_identity() == ("", False, "")
    catalog.write_bytes(saved)
    assert api._settings_tiingo_identity()[1] is True
