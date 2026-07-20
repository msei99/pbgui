"""Phase 6 tests for INI apply timing and restart feedback."""

from __future__ import annotations

import asyncio
import configparser
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import PBApiServer
import api.auth as auth_api
import api.logging as logging_api
import api.market_data as market_data_api
import api.services as services
from ini_settings import APPLY_GROUPS, SETTING_APPLY_REGISTRY, apply_metadata
from ini_settings import apply_metadata_for


def test_registry_covers_editable_setting_families_without_values() -> None:
    """The registry classifies required families but stores no setting values."""
    required_sections = {
        "api_server", "main", "vps_monitor", "vps_monitor_alerts", "monitor",
        "vps_monitor_ui", "pbdata", "coinmarketcap", "logging",
        "pareto", "config_archive", "market_data",
    }
    sections = {section for section, _key in SETTING_APPLY_REGISTRY}
    assert required_sections <= sections
    assert {"binance_data", "bybit_data", "bitget_data", "okx_data"} <= sections
    assert "hyperliquid_data" not in sections
    assert SETTING_APPLY_REGISTRY[("pbdata", "latest_1m_interval_seconds")].owner == "PBData"
    assert "tradfi_profiles" not in sections
    assert SETTING_APPLY_REGISTRY[("market_data", "hl_l2book_scan_workers")].owner == "TaskWorker"
    assert SETTING_APPLY_REGISTRY[("config_archive", "my_archive")].owner == "BacktestV7"
    assert all("value" not in vars(item) and "default" not in vars(item) for item in SETTING_APPLY_REGISTRY.values())


def test_apply_metadata_has_stable_shape_and_mixed_group_priority() -> None:
    """Mixed groups expose one deterministic highest-impact timing."""
    payload = apply_metadata("pbdata", "api_server")
    assert set(payload) == {"version", "timing", "restart_required", "message", "owners", "settings"}
    assert payload["version"] == 1
    assert payload["timing"] == "api_restart"
    assert payload["restart_required"] is True
    assert payload["message"] == "API restart required"
    assert APPLY_GROUPS["pbdata"]


def test_welcome_setup_registry_classifies_role_and_empty_changes() -> None:
    """Welcome settings classify role separately and support unchanged saves."""
    role = SETTING_APPLY_REGISTRY[("main", "role")]
    assert role.owner == "PBGui"
    assert role.timing == "next_cycle"
    assert role.restart_required is False
    assert set(APPLY_GROUPS["welcome_setup"]) == {
        ("main", "pbname"),
        ("main", "pb7dir"),
        ("main", "pb7venv"),
        ("main", "pb8dir"),
        ("main", "pb8venv"),
        ("main", "role"),
    }
    assert SETTING_APPLY_REGISTRY[("main", "pb8dir")].timing == "next_operation"
    assert SETTING_APPLY_REGISTRY[("main", "pb8venv")].restart_required is False
    unchanged = apply_metadata_for(())
    assert unchanged["timing"] == "immediate"
    assert unchanged["restart_required"] is False
    assert unchanged["message"] == "No runtime changes"
    assert unchanged["settings"] == []


def test_welcome_setup_returns_apply_metadata_for_changed_fields(monkeypatch) -> None:
    """Welcome setup reports only settings whose persisted values changed."""
    previous = {"pb7dir": "/old/pb7", "pb7venv": "/old/python", "pbname": "master-a", "role": "slave"}
    current = {"pb7dir": "/new/pb7", "pb7venv": "/new/python", "pbname": "master-a", "role": "master"}
    saved: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr(auth_api, "pb7_runtime_status", lambda: previous)
    monkeypatch.setattr(auth_api, "pb8_runtime_status", lambda: {"pb8dir": "", "pb8venv": ""})
    monkeypatch.setattr(auth_api, "save_ini_section", lambda section, values: saved.append((section, values)))
    monkeypatch.setattr(auth_api, "_bootstrap_payload", lambda session: {"setup": {**current, "pb8": {"pb8dir": "", "pb8venv": ""}}})

    result = auth_api.save_setup(
        auth_api.SetupConfigRequest(pb7dir="/new/pb7", pb7venv="/new/python", pbname="master-a", role="master"),
        SimpleNamespace(),
    )

    assert saved == [("main", {"pb7dir": "/new/pb7", "pb7venv": "/new/python", "pbname": "master-a", "role": "master"})]
    assert result["apply"]["timing"] == "service_restart"
    assert result["apply"]["restart_required"] is True
    assert {
        (item["section"], item["key"])
        for item in result["apply"]["settings"]
    } == {("main", "pb7dir"), ("main", "pb7venv"), ("main", "role")}
    assert result["message"] == "Setup saved. Service restart required."


def test_settings_payloads_add_apply_metadata_without_removing_existing_fields(monkeypatch) -> None:
    """GET payloads retain established keys and add the apply contract."""
    class FakeCoinData:
        fetch_limit = 5000
        fetch_interval = 24
        metadata_interval = 1
        mapping_interval = 24

    monkeypatch.setattr("PBCoinData.CoinData", FakeCoinData)
    monkeypatch.setattr(services, "_cmc_pool_payload", lambda: {"ready": False, "active_credentials": 0})
    coin = services.get_pbcoindata_settings(SimpleNamespace())
    assert {"fetch_limit", "fetch_interval", "metadata_interval", "mapping_interval", "cmc_pool"} <= set(coin)
    assert "api_key" not in coin
    assert coin["apply"]["message"] == "Applies next cycle"

    monkeypatch.setattr(logging_api, "get_rotate_defaults", lambda: (1024 * 1024, 1))
    monkeypatch.setattr(logging_api.logging_helpers, "LOG_ROOT", Path("/missing-phase6-log-root"))
    rotation = logging_api.get_rotation(SimpleNamespace())
    assert {"default", "per_service", "managed_scopes"} <= set(rotation)
    assert rotation["apply"]["timing"] == "next_log_write"


def test_pbdata_and_coindata_save_messages_are_additive(monkeypatch) -> None:
    """Existing success flags remain while both live services report next cycle."""
    saved_sections: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr(services, "save_ini_section", lambda section, values: saved_sections.append((section, values)))
    pbdata = services.save_pbdata_settings(services.PBDataSettings(), SimpleNamespace())
    assert pbdata["ok"] is True
    assert pbdata["apply"]["message"] == "Applies next cycle"
    assert saved_sections[0][0] == "pbdata"

    class FakeCoinData:
        def save_config(self) -> None:
            return None

    monkeypatch.setattr("PBCoinData.CoinData", FakeCoinData)
    monkeypatch.setattr(services, "_cmc_pool_payload", lambda: {"ready": False, "active_credentials": 0})
    coin = services.save_pbcoindata_settings(services.PBCoinDataSettings(), SimpleNamespace())
    assert coin["ok"] is True
    assert coin["cmc_pool"]["ready"] is False
    assert coin["apply"]["message"] == "Applies next cycle"


def test_api_server_save_uses_one_ini_generation_and_marks_bind_change(monkeypatch) -> None:
    """The combined API/VPS form is persisted by exactly one update transaction."""
    generations: list[configparser.ConfigParser] = []
    reasons: list[str] = []

    class FakeServer:
        host = "127.0.0.1"
        port = 8000

    fake_module = SimpleNamespace(
        PBApiServer=FakeServer,
        mark_runtime_restart_required=reasons.append,
    )

    def capture_update(mutator) -> None:
        parser = configparser.ConfigParser()
        mutator(parser)
        generations.append(parser)

    monkeypatch.setattr(services.importlib, "import_module", lambda name: fake_module)
    monkeypatch.setattr(services, "update_ini", capture_update)
    body = services.APIServerSettings(
        host="0.0.0.0",
        port=80,
        auto_restart=False,
        enabled_hosts=["vps-b", "vps-a"],
        monitor_config={"cpu_warning_server": 81},
        telegram_token="token",
        telegram_chat_id="chat",
    )

    result = services.save_api_server_settings(body, SimpleNamespace())

    assert result["ok"] is True
    assert len(generations) == 1
    parser = generations[0]
    assert parser.get("api_server", "port") == "1024"
    assert parser.get("vps_monitor", "enabled_hosts") == "vps-a,vps-b"
    assert parser.get("monitor", "cpu_warning_server") == "81.0"
    assert parser.get("vps_monitor_alerts", "offline_gui") == "true"
    assert reasons == ["API host or port settings changed"]
    restart_keys = {
        (item["section"], item["key"])
        for item in result["apply"]["settings"]
        if item["restart_required"]
    }
    assert restart_keys == {("api_server", "host"), ("api_server", "port")}


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_api_server_save_rejects_non_finite_monitor_values_before_publish(monkeypatch, value) -> None:
    """NaN and infinities never reach the combined INI transaction."""
    published = []
    monkeypatch.setattr(services, "update_ini", lambda mutator: published.append(mutator))
    monkeypatch.setattr(
        services.importlib,
        "import_module",
        lambda name: SimpleNamespace(PBApiServer=lambda: SimpleNamespace(host="0.0.0.0", port=8000)),
    )

    with pytest.raises(services.HTTPException) as exc_info:
        services.save_api_server_settings(
            services.APIServerSettings(monitor_config={"cpu_warning_server": value}),
            SimpleNamespace(),
        )

    assert exc_info.value.status_code == 422
    assert published == []


def test_api_server_mixed_metadata_omits_unchanged_restart_fields(monkeypatch) -> None:
    """Only changed bind fields appear as restart-required in a mixed save."""
    fake_module = SimpleNamespace(
        PBApiServer=lambda: SimpleNamespace(host="127.0.0.1", port=8000),
        mark_runtime_restart_required=lambda reason: None,
    )
    monkeypatch.setattr(services.importlib, "import_module", lambda name: fake_module)
    monkeypatch.setattr(services, "update_ini", lambda mutator: mutator(configparser.ConfigParser()))

    result = services.save_api_server_settings(
        services.APIServerSettings(host="127.0.0.1", port=9000),
        SimpleNamespace(),
    )

    restart_keys = {
        (item["section"], item["key"])
        for item in result["apply"]["settings"]
        if item["restart_required"]
    }
    assert restart_keys == {("api_server", "port")}


def test_runtime_restart_reason_lifecycle_and_status(monkeypatch) -> None:
    """Runtime reasons augment serial state and a clean process state has none."""
    PBApiServer._runtime_restart_reasons.clear()
    monkeypatch.setattr(PBApiServer, "_startup_serial", 7)
    monkeypatch.setattr(PBApiServer, "_read_serial", lambda: 7)
    monkeypatch.setattr(PBApiServer, "_restart_block_state", lambda: asyncio.sleep(0, result=(False, "")))
    assert PBApiServer._refresh_restart_state() is False

    PBApiServer.mark_runtime_restart_required("API host or port settings changed")
    PBApiServer.mark_runtime_restart_required("API host or port settings changed")
    payload = asyncio.run(PBApiServer.server_status(SimpleNamespace()))

    assert payload["needs_restart"] is True
    assert payload["serial_restart_required"] is False
    assert payload["runtime_restart_reasons"] == ["API host or port settings changed"]
    PBApiServer._runtime_restart_reasons.clear()
    PBApiServer._refresh_restart_state()


def test_services_ui_uses_safe_apply_messages() -> None:
    """Server-provided apply text is rendered only through textContent."""
    source = Path("frontend/services_monitor.html").read_text(encoding="utf-8")
    post_block = source[source.index("function _post("):source.index("function _val(")]
    assert "d.apply.message" in post_block
    assert "textContent" not in post_block
    flash_block = source[source.index("function _flash("):source.index("/* ── Prices Overlay")]
    assert "el.textContent = text" in flash_block
    assert "innerHTML" not in flash_block
    assert "Applied immediately" in json.dumps(apply_metadata("api_server_full"))
    assert apply_metadata("pbdata")["message"] == "Applies next cycle"
    assert apply_metadata("pbcoindata")["message"] == "Applies next cycle"


def test_market_data_payload_contract_is_additive(monkeypatch) -> None:
    """Market Data GET and save payloads expose timing without replacing fields."""
    monkeypatch.setattr(market_data_api, "_get_exchange_settings_meta", lambda exchange: ("okx", {"label": "OKX", "ini_section": "okx_data", "defaults": {"interval_seconds": 1, "coin_pause_seconds": 1, "api_timeout_seconds": 1, "min_lookback_days": 1, "max_lookback_days": 1}}))
    monkeypatch.setattr(market_data_api, "load_market_data_config", lambda: object())
    monkeypatch.setattr(market_data_api, "_coin_options_for_exchange", lambda exchange: ["BTC"])
    monkeypatch.setattr(market_data_api, "get_effective_enabled_coins", lambda *args, **kwargs: (["BTC"], [], True))
    monkeypatch.setattr(market_data_api, "load_ini", lambda *args: "")

    payload = market_data_api._build_market_data_settings_payload("okx")

    assert {"exchange", "exchange_label", "enabled_coins", "settings"} <= set(payload)
    assert payload["apply"]["timing"] == "next_cycle"
