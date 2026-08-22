"""Unit tests for the isolated PB8 configuration client."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pb8_config
import pb8_config_helper
import pytest
from master_update_lock import MasterUpdateBusyError


def _reset_cache(monkeypatch) -> None:
    """Keep cache tests isolated from process-global PB8 client state."""
    monkeypatch.setattr(pb8_config, "_template_cache", None)
    monkeypatch.setattr(pb8_config, "_result_metrics_cache", None)
    monkeypatch.setattr(pb8_config, "_optimize_metadata_cache", None)
    pb8_config._coin_override_metadata_cache.clear()
    monkeypatch.setattr(pb8_config, "_exchange_metadata_cache", None)
    pb8_config._market_catalog_cache.clear()
    pb8_config._config_cache.clear()


def test_prepare_pb8_config_delegates_to_isolated_helper(monkeypatch) -> None:
    """In-memory configs must be prepared by the PB8 helper, not imported locally."""
    calls = []

    def fake_call(operation: str, **payload) -> dict:
        calls.append((operation, payload))
        return {"config": {"config_version": "v8.0.0"}}

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    result = pb8_config.prepare_pb8_config({"backtest": {}}, base_config_path="/tmp/backtest.json")

    assert result == {"config_version": "v8.0.0"}
    assert calls == [
        (
            "prepare",
            {
                "config": {"backtest": {}},
                "base_config_path": "/tmp/backtest.json",
            },
        )
    ]


def test_optimizer_override_validation_delegates_to_isolated_helper(monkeypatch) -> None:
    """Strategy-dependent optimizer overrides must be checked inside PB8's runtime."""
    calls = []
    monkeypatch.setattr(
        pb8_config,
        "_call_helper",
        lambda operation, **payload: calls.append((operation, payload)) or {"valid": True},
    )
    config = {"live": {"strategy_kind": "ema_anchor"}, "optimize": {"enable_overrides": []}}

    pb8_config.validate_pb8_optimizer_overrides(config, base_config_path="/tmp/optimize.json")

    assert calls == [
        (
            "validate_optimizer_overrides",
            {"config": config, "base_config_path": "/tmp/optimize.json"},
        )
    ]


def test_market_identifier_client_preserves_exact_helper_contract(monkeypatch) -> None:
    """The PBGui client must delegate exact IDs without local normalization."""
    calls = []
    helper_result = {
        "contract_version": 1,
        "exchanges": ["bitget"],
        "symbols": ["BTC", "bitget::1000ABCUSDT"],
        "catalog": [{"config_id": "BTC", "coin": "BTC", "resolutions": []}],
        "statuses": {"1000ABC/USDT:USDT": {"status": "valid"}},
    }

    def fake_call(operation: str, **payload) -> dict:
        calls.append((operation, payload))
        return helper_result

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)
    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: ("runtime",))
    result = pb8_config.get_pb8_market_identifiers(
        ["bitget"], ["1000ABC/USDT:USDT"], quote="USDT"
    )
    result["symbols"].append("mutated")

    assert helper_result["symbols"] == ["BTC", "bitget::1000ABCUSDT"]
    assert calls == [
        (
            "market_identifiers",
            {
                "exchanges": ["bitget"],
                "identifiers": ["1000ABC/USDT:USDT"],
                "quote": "USDT",
            },
        )
    ]


def test_market_identifier_client_rejects_invalid_helper_shape(monkeypatch) -> None:
    """Malformed helper output must fail closed before reaching an editor."""
    monkeypatch.setattr(pb8_config, "_call_helper", lambda *_args, **_kwargs: {"symbols": []})
    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: ("runtime",))

    with pytest.raises(pb8_config.PB8ConfigurationError, match="invalid market identifiers"):
        pb8_config.get_pb8_market_identifiers(["bitget"])


@pytest.mark.parametrize(
    ("exchanges", "identifiers", "quote"),
    [
        ([], None, None),
        ([" bitget "], None, None),
        (["bitget"], [" BTC "], None),
        (["bitget"], ["x" * 257], None),
        (["bitget"], None, "USD/T"),
    ],
)
def test_market_identifier_client_validation_is_422(exchanges, identifiers, quote) -> None:
    """Bounded request validation should remain distinguishable from resolver outages."""
    with pytest.raises(pb8_config.PB8MarketRequestError) as exc_info:
        pb8_config.get_pb8_market_identifiers(exchanges, identifiers, quote=quote)

    assert exc_info.value.status_code == 422


def test_market_catalog_cache_is_bounded_and_identifier_requests_are_uncached(monkeypatch) -> None:
    """Catalog-only calls may cache by runtime while exact-ID status requests always execute."""
    _reset_cache(monkeypatch)
    calls = []
    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: ("runtime",))
    monkeypatch.setattr(
        pb8_config,
        "_call_helper",
        lambda operation, **payload: calls.append((operation, payload)) or {
            "contract_version": 1,
            "symbols": ["BTC"],
            "catalog": [],
            "statuses": {},
        },
    )

    pb8_config.get_pb8_market_identifiers(["bitget"])
    pb8_config.get_pb8_market_identifiers(["bitget"])
    pb8_config.get_pb8_market_identifiers(["bitget"], ["BTC"])
    pb8_config.get_pb8_market_identifiers(["bitget"], ["BTC"])

    assert len(calls) == 3


def test_result_metrics_use_bounded_helper_cache(monkeypatch) -> None:
    """Installed PB8 metric names should be normalized, copied, and cached."""
    _reset_cache(monkeypatch)
    calls = []

    def fake_call(operation: str, **payload) -> dict:
        calls.append((operation, payload))
        return {"metrics": ["sharpe_ratio", "adg", "adg"]}

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    first = pb8_config.get_pb8_result_metrics()
    first.append("mutated")

    assert pb8_config.get_pb8_result_metrics() == ["adg", "sharpe_ratio"]
    assert calls == [("result_metrics", {})]


def test_coin_override_metadata_cache_is_contextual_and_returns_copies(monkeypatch) -> None:
    """Override policy caches must remain isolated by HSL mode and strategy."""
    _reset_cache(monkeypatch)
    calls = []
    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: ("runtime",))

    def fake_call(operation: str, **payload) -> dict:
        calls.append((operation, payload))
        return {
            "contract_version": 1,
            "hsl_signal_mode": payload["hsl_signal_mode"],
            "strategy_kind": payload["strategy_kind"],
            "params": {"bot": {"long": {}, "short": {}}, "live": {}},
        }

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)
    first = pb8_config.get_pb8_coin_override_metadata("coin", "trailing_martingale")
    first["params"]["live"]["mutated"] = True

    cached = pb8_config.get_pb8_coin_override_metadata("coin", "trailing_martingale")
    pb8_config.get_pb8_coin_override_metadata("pside", "trailing_martingale")

    assert "mutated" not in cached["params"]["live"]
    assert len(calls) == 2


def test_validate_override_bundle_delegates_staged_path(monkeypatch, tmp_path: Path) -> None:
    """Definitive override validation must use the exact staged config path."""
    config_path = tmp_path / "backtest.json"
    config_path.write_text("{}", encoding="utf-8")
    calls = []
    monkeypatch.setattr(
        pb8_config,
        "_call_helper",
        lambda operation, **payload: calls.append((operation, payload)) or {"valid": True},
    )

    pb8_config.validate_pb8_override_bundle(config_path)

    assert calls == [("validate_overrides", {"config_path": str(config_path.resolve())})]


def test_exchange_metadata_is_runtime_cached_and_keeps_capability_boundaries(monkeypatch) -> None:
    """Live-only connectors must not be advertised as historical-data clients."""
    _reset_cache(monkeypatch)
    calls = []
    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: ("runtime",))
    monkeypatch.setattr(
        pb8_config,
        "_call_helper",
        lambda operation, **_payload: calls.append(operation) or {
            "contract_version": 1,
            "live": ["weex", "bitunix"],
            "backtest": ["weex"],
            "optimize": ["weex"],
            "suite": ["weex"],
        },
    )

    first = pb8_config.get_pb8_exchange_metadata()
    first["live"].append("mutated")
    second = pb8_config.get_pb8_exchange_metadata()

    assert second["live"] == ["bitunix", "weex"]
    assert second["backtest"] == ["weex"]
    assert calls == ["exchange_metadata"]


def test_runtime_fingerprint_change_invalidates_optimize_metadata_cache(monkeypatch) -> None:
    """A PB8 update must invalidate metadata before the 30-second TTL expires."""
    _reset_cache(monkeypatch)
    fingerprint = ["commit-a"]
    calls = []

    def fake_call(operation: str, **_payload) -> dict:
        calls.append(operation)
        return {"template": {"runtime": fingerprint[0]}, "strategies": []}

    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: tuple(fingerprint))
    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    assert pb8_config.get_pb8_optimize_metadata()["template"]["runtime"] == "commit-a"
    assert pb8_config.get_pb8_optimize_metadata()["template"]["runtime"] == "commit-a"
    fingerprint[0] = "commit-b"
    assert pb8_config.get_pb8_optimize_metadata()["template"]["runtime"] == "commit-b"
    assert calls == ["optimize_metadata", "optimize_metadata"]


def test_runtime_fingerprint_change_invalidates_loaded_config_cache(tmp_path, monkeypatch) -> None:
    """Canonical configs cached by file signature must also belong to the current PB8 runtime."""
    _reset_cache(monkeypatch)
    source = tmp_path / "backtest.json"
    source.write_text("{}", encoding="utf-8")
    fingerprint = ["commit-a"]
    calls = []

    def fake_call(operation: str, **_payload) -> dict:
        calls.append(operation)
        return {"config": {"runtime": fingerprint[0]}}

    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: tuple(fingerprint))
    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    assert pb8_config.load_pb8_config(source) == {"runtime": "commit-a"}
    assert pb8_config.load_pb8_config(source) == {"runtime": "commit-a"}
    fingerprint[0] = "commit-b"
    assert pb8_config.load_pb8_config(source) == {"runtime": "commit-b"}
    assert calls == ["load", "load"]


def test_call_helper_uses_pb8_venv_cwd_and_releases_update_lock(monkeypatch) -> None:
    """PB8 helper subprocesses hold and release the master runtime lease."""
    released = []
    captured = {}

    class Lease:
        def release(self) -> None:
            released.append(True)

    class Proc:
        returncode = 0
        stdout = '{"ok":true,"result":{"version":"v8"}}'
        stderr = ""

    monkeypatch.setattr(pb8_config, "acquire_master_runtime_lock", lambda _root: Lease())
    monkeypatch.setattr(
        pb8_config,
        "pb8_runtime_status",
        lambda: {"ready": True, "pb8dir": "/runtime/pb8", "pb8venv": "/runtime/venv/bin/python"},
    )

    def fake_run(command, **kwargs):
        captured.update(command=command, kwargs=kwargs)
        return Proc()

    monkeypatch.setattr(pb8_config.subprocess, "run", fake_run)

    assert pb8_config._call_helper("status") == {"version": "v8"}
    assert captured["command"][0] == "/runtime/venv/bin/python"
    assert captured["kwargs"]["cwd"] == "/runtime/pb8"
    assert released == [True]


def test_call_helper_transforms_update_lock_busy_without_subprocess(monkeypatch) -> None:
    """A PB8 update remains distinguishable and never starts a helper process."""
    busy = MasterUpdateBusyError("update active")
    monkeypatch.setattr(
        pb8_config,
        "acquire_master_runtime_lock",
        lambda _root: (_ for _ in ()).throw(busy),
    )
    monkeypatch.setattr(
        pb8_config.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("subprocess started")),
    )

    with pytest.raises(pb8_config.PB8RuntimeBusyError, match="Retry") as error:
        pb8_config._call_helper("status")

    assert error.value.retryable is True
    assert error.value.status_code == 503
    assert error.value.__cause__ is busy


def test_save_pb8_config_writes_prepared_config_atomically(tmp_path, monkeypatch) -> None:
    """Saving must persist the canonical helper output rather than the request payload."""
    destination = tmp_path / "demo" / "backtest.json"
    prepared = {"config_version": "v8.0.0", "backtest": {"base_dir": "backtests/pbgui/demo"}}
    monkeypatch.setattr(pb8_config, "prepare_pb8_config", lambda *_args, **_kwargs: prepared)

    result = pb8_config.save_pb8_config({"legacy": True}, destination)

    assert result == prepared
    assert json.loads(destination.read_text(encoding="utf-8")) == prepared
    assert not list(destination.parent.glob(f".{destination.name}.*"))


def test_template_and_file_loads_use_bounded_signature_cache(tmp_path, monkeypatch) -> None:
    """Repeated reads avoid helper startup while file changes invalidate cached configs."""
    _reset_cache(monkeypatch)
    source = tmp_path / "backtest.json"
    source.write_text('{"version": 1}', encoding="utf-8")
    calls = []

    def fake_call(operation: str, **payload) -> dict:
        calls.append((operation, payload))
        if operation == "default":
            return {"config": {"template": True}}
        return {"config": json.loads(source.read_text(encoding="utf-8"))}

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    first_template = pb8_config.get_pb8_template_config()
    first_template["mutated"] = True
    assert pb8_config.get_pb8_template_config() == {"template": True}
    first_load = pb8_config.load_pb8_config(source)
    first_load["mutated"] = True
    assert pb8_config.load_pb8_config(source) == {"version": 1}
    source.write_text('{"version": 200}', encoding="utf-8")
    assert pb8_config.load_pb8_config(source) == {"version": 200}
    assert [operation for operation, _payload in calls] == ["default", "load", "load"]


def test_save_prepared_pb8_config_skips_second_helper_and_warms_load_cache(tmp_path, monkeypatch) -> None:
    """Already prepared API payloads should write atomically and load without another helper process."""
    _reset_cache(monkeypatch)
    destination = tmp_path / "demo" / "backtest.json"
    prepared = {"config_version": "v8.0.0", "backtest": {"base_dir": "backtests/pbgui/demo"}}
    monkeypatch.setattr(pb8_config, "_call_helper", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("helper called")))

    result = pb8_config.save_prepared_pb8_config(prepared, destination)

    assert result == prepared
    assert pb8_config.load_pb8_config(destination) == prepared
    assert json.loads(destination.read_text(encoding="utf-8")) == prepared


def test_migrate_pb7_config_passes_distinct_absolute_paths(tmp_path, monkeypatch) -> None:
    """The client should provide explicit source and output paths to PB8's migration helper."""
    source = tmp_path / "v7" / "backtest.json"
    output = tmp_path / "v8" / "backtest.json"
    captured = {}

    def fake_call(operation: str, **payload) -> dict:
        captured.update({"operation": operation, **payload})
        return {"report": {"output_written": True}, "config": {}}

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    result = pb8_config.migrate_pb7_config(source, output)

    assert result["report"]["output_written"] is True
    assert captured == {
        "operation": "migrate_v7",
        "source_path": str(source.resolve()),
        "output_path": str(output.resolve()),
        "allow_manual_review_output": False,
    }


def test_helper_prepare_preserves_pbgui_metadata_outside_pb8_payload() -> None:
    """PBGui-owned metadata must survive while remaining invisible to PB8 canonicalization."""
    received = {}

    def prepare(config, **_kwargs):
        received.update(config)
        return {"config_version": "v8.0.0", "backtest": {}}

    modules = {"prepare_config": prepare, "sanitize": lambda value: value}
    metadata = {"market_cap": 25, "future": {"keep": True}}

    result = pb8_config_helper._prepare(modules, {"backtest": {}, "pbgui": metadata})

    assert "pbgui" not in received
    assert result["pbgui"] == metadata


def test_helper_optimizer_override_validation_exercises_every_side() -> None:
    """PB8's native override application must validate global, long, and short contexts."""
    calls = []

    def apply(overrides, config, pside):
        calls.append((list(overrides), pside))
        return config

    modules = {
        "prepare_config": lambda config, **_kwargs: config,
        "sanitize": lambda value: value,
        "apply_optimizer_overrides": apply,
    }
    config = {
        "bot": {"long": {}, "short": {}},
        "optimize": {"enable_overrides": ["mirror_short_from_long"]},
    }

    pb8_config_helper._validate_optimizer_overrides(modules, config)

    assert calls == [
        (["mirror_short_from_long"], None),
        (["mirror_short_from_long"], "long"),
        (["mirror_short_from_long"], "short"),
    ]


def test_helper_load_restores_nested_pbgui_metadata(tmp_path, monkeypatch) -> None:
    """Loading a stored PB8 config must merge its opaque PBGui envelope back in."""
    source = tmp_path / "backtest.json"
    metadata = {"tags": ["defi"], "future": {"enabled": True}}
    source.write_text(json.dumps({"backtest": {}, "pbgui": metadata}), encoding="utf-8")
    modules = {
        "load_prepared_config": lambda *_args, **_kwargs: {"config_version": "v8.0.0", "backtest": {}},
        "sanitize": lambda value: value,
    }
    monkeypatch.setattr(pb8_config_helper, "_load_pb8_modules", lambda _path: modules)

    result = pb8_config_helper.handle({"operation": "load", "pb8_dir": str(tmp_path), "config_path": str(source)})

    assert result["config"]["pbgui"] == metadata


@pytest.mark.local_runtime
@pytest.mark.parametrize(
    ("legacy_distance", "expected_activation", "expected_distance"),
    [(0.005, 10, 0.005), (0.0, 0, 0.005)],
)
def test_local_pb81_runtime_migrates_v80_without_mutating_source(
    tmp_path, legacy_distance, expected_activation, expected_distance
) -> None:
    """The installed PB8.1 loader must migrate v8.0 in memory and save canonically."""
    from pbgui_purefunc import pb8_runtime_status

    runtime = pb8_runtime_status()
    assert runtime.get("ready") is True
    assert runtime.get("version") == "8.1.0"
    source_config = pb8_config.get_pb8_template_config()
    source_config["config_version"] = "v8.0.0"
    live = source_config["live"]
    for key in (
        "order_replacement_churn_gate_activation_count",
        "order_replacement_churn_gate_market_dist_pct",
        "order_replacement_churn_gate_stability_minutes",
        "order_replacement_churn_gate_window_minutes",
    ):
        live.pop(key, None)
    live["initial_entry_exec_max_market_dist_pct"] = legacy_distance
    source = tmp_path / "legacy-v80.json"
    source_bytes = (json.dumps(source_config, indent=4) + "\n").encode()
    source.write_bytes(source_bytes)

    loaded = pb8_config.load_pb8_config(source)

    assert source.read_bytes() == source_bytes
    assert loaded["config_version"] == "v8.1.0"
    assert "initial_entry_exec_max_market_dist_pct" not in loaded["live"]
    assert loaded["live"]["order_replacement_churn_gate_activation_count"] == expected_activation
    assert loaded["live"]["order_replacement_churn_gate_market_dist_pct"] == pytest.approx(expected_distance)

    saved = tmp_path / "saved-v81.json"
    pb8_config.save_prepared_pb8_config(loaded, saved)
    persisted = json.loads(saved.read_text(encoding="utf-8"))
    assert persisted["config_version"] == "v8.1.0"
    assert persisted["live"]["order_replacement_churn_gate_activation_count"] == expected_activation


def test_helper_market_identifiers_uses_pb8_catalog_and_preserves_exact_ids() -> None:
    """PB8's official catalog must supply collision IDs while imported exact IDs stay unchanged."""

    class AmbiguousMarketIdentifier(RuntimeError):
        pass

    class MarketIdentifierExchangeMismatch(RuntimeError):
        pass

    class UnknownMarketIdentifier(RuntimeError):
        pass

    symbols = {
        ("BTC", "bitget"): "BTC/USDT:USDT",
        ("BTC", "bybit"): "BTC/USDT:USDT",
        ("bitget::ABCUSDT", "bitget"): "ABC/USDT:USDT",
        ("bitget::1000ABCUSDT", "bitget"): "1000ABC/USDT:USDT",
        ("1000ABC/USDT:USDT", "bitget"): "1000ABC/USDT:USDT",
        ("USD1", "bitget"): "1/USDT:USDT",
    }

    def coin_to_symbol(identifier, exchange, **_kwargs):
        if identifier == "ABC":
            raise AmbiguousMarketIdentifier("ABC is ambiguous")
        if "::" in identifier and not identifier.startswith(f"{exchange}::"):
            raise MarketIdentifierExchangeMismatch("wrong exchange")
        try:
            return symbols[(identifier, exchange)]
        except KeyError as exc:
            raise UnknownMarketIdentifier("unknown") from exc

    async def reject_collisions(identifiers, _exchanges, **_kwargs):
        if "ABC" in identifiers:
            raise AmbiguousMarketIdentifier("ABC resolves to multiple contracts")

    modules = {
        "AmbiguousMarketIdentifier": AmbiguousMarketIdentifier,
        "MarketIdentifierExchangeMismatch": MarketIdentifierExchangeMismatch,
        "UnknownMarketIdentifier": UnknownMarketIdentifier,
        "coin_to_symbol": coin_to_symbol,
        "approved_all_market_identifiers": lambda _rows: {
            "BTC", "USD1", "bitget::ABCUSDT", "bitget::1000ABCUSDT"
        },
        "filter_markets": lambda markets, _exchange, **_kwargs: (markets, None),
        "get_quote": lambda _exchange, quote=None: quote or "USDT",
        "load_markets": lambda exchange, **_kwargs: asyncio.sleep(
            0,
            result={
                symbol: {}
                for (identifier, item_exchange), symbol in symbols.items()
                if item_exchange == exchange and identifier != "USD1"
            },
        ),
        "looks_like_exact_market_identifier": lambda value: "::" in value or "/" in value or ":" in value,
        "reject_cross_exchange_market_identifier_collisions": reject_collisions,
        "split_exchange_qualified_market_identifier": lambda value: tuple(value.split("::", 1))
        if "::" in value
        else (None, value),
        "symbol_to_coin": lambda value, **_kwargs: value.split("/", 1)[0],
        "to_standard_exchange_name": lambda value: value.lower(),
    }

    result = asyncio.run(
        pb8_config_helper._market_identifiers(
            modules,
            {
                "exchanges": ["bitget", "bybit"],
                "identifiers": ["BTC", "ABC", "1000ABC/USDT:USDT"],
                "quote": "USDT",
            },
        )
    )

    assert result["symbols"] == ["BTC", "bitget::1000ABCUSDT", "bitget::ABCUSDT"]
    assert {entry["config_id"]: entry["coin"] for entry in result["catalog"]} == {
        "BTC": "BTC",
        "bitget::ABCUSDT": "ABC",
        "bitget::1000ABCUSDT": "1000ABC",
    }
    assert "USD1" not in result["symbols"]
    assert all(entry["display"] for entry in result["catalog"])
    assert result["statuses"]["BTC"]["status"] == "valid"
    assert result["statuses"]["ABC"]["reason"] == "ambiguous"
    assert result["statuses"]["1000ABC/USDT:USDT"]["normalized"] == "1000ABC/USDT:USDT"
    assert result["statuses"]["1000ABC/USDT:USDT"]["display"] == "1000ABC"


@pytest.mark.parametrize(
    "payload",
    [
        {"exchanges": []},
        {"exchanges": ["fake"]},
        {"exchanges": ["bitget"], "identifiers": ["bad\nvalue"]},
        {"exchanges": ["bitget"], "identifiers": ["x" * 257]},
        {"exchanges": ["bitget"], "quote": "USD/T"},
    ],
)
def test_helper_market_identifier_payload_validation_fails_before_loading(payload) -> None:
    """Malformed bounded resolver requests must fail before PB8 market loading."""
    modules = {
        "to_standard_exchange_name": lambda value: value,
        "load_markets": lambda *_args, **_kwargs: pytest.fail("market load started"),
    }

    with pytest.raises((TypeError, ValueError)):
        asyncio.run(pb8_config_helper._market_identifiers(modules, payload))


def test_optimize_metadata_builds_nonempty_bounds_and_bot_defaults_for_every_strategy() -> None:
    """The strategy selector must receive real per-strategy controls, not empty placeholders."""
    strategies = ("trailing_martingale", "ema_anchor", "trailing_grid_v7")
    all_bounds = {
        side: {
            "risk": {"n_positions": [1, 10, 1]},
            "strategy": {kind: {"entry": {"value": [index, index + 1, 0.1]}} for index, kind in enumerate(strategies)},
        }
        for side in ("long", "short")
    }
    template = {
        "bot": {side: {"strategy": {strategies[0]: {"entry": {"value": 0}}}} for side in ("long", "short")},
        "live": {"strategy_kind": strategies[0]},
        "optimize": {"bounds": {side: {"risk": {"n_positions": [1, 10, 1]}, "strategy": {strategies[0]: {"entry": {"value": [0, 1, 0.1]}}}} for side in ("long", "short")}},
    }
    modules = {
        "get_template_config": lambda: template,
        "prepare_config": lambda config, **_kwargs: config,
        "sanitize": lambda value: value,
        "get_supported_strategy_kinds": lambda: strategies,
        "get_strategy_spec": lambda kind: {"kind": kind},
        "get_all_strategy_defaults": lambda: {side: {kind: {"entry": {"value": index}} for index, kind in enumerate(strategies)} for side in ("long", "short")},
        "get_optimize_bounds_defaults": lambda: all_bounds,
        "result_metrics": [],
        "default_objective_goals": {},
        "backends": ["pymoo"],
        "pymoo_algorithms": ["nsga2"],
        "pymoo_ref_dir_methods": ["das_dennis"],
        "objective_goals": ["min", "max"],
        "limit_statistics": ["mean"],
        "limit_basis_field": "reducer",
        "scoring_basis_field": "reducer",
        "optimizer_overrides": [],
        "fixed_runtime_overrides": {},
    }

    metadata = pb8_config_helper._optimize_metadata(modules)

    assert metadata["limits"]["basis_field"] == "reducer"
    assert metadata["scoring"]["basis_field"] == "reducer"
    assert set(metadata["active_bounds"]) == set(strategies)
    for kind in strategies:
        for side in ("long", "short"):
            assert metadata["active_bounds"][kind][side]["strategy"] == {
                kind: all_bounds[side]["strategy"][kind]
            }
            assert metadata["strategy_defaults"][side][kind]


def test_optimize_basis_contract_supports_legacy_and_reducer_pb8() -> None:
    """PBGui must follow each installed PB8 generation's canonical reduction fields."""
    legacy = pb8_config_helper._optimize_basis_contract(
        SimpleNamespace(SUPPORTED_LIMIT_STATS={"mean", "median"}),
        {"metric", "goal", "scenario", "aggregate"},
    )
    current = pb8_config_helper._optimize_basis_contract(
        SimpleNamespace(),
        {"metric", "goal", "scenario", "reducer"},
        SimpleNamespace(SUPPORTED_REDUCERS={"mean", "median"}),
    )

    assert legacy == {
        "statistics": ["mean", "median"],
        "limit_basis_field": "stat",
        "scoring_basis_field": "aggregate",
    }
    assert current == {
        "statistics": ["mean", "median"],
        "limit_basis_field": "reducer",
        "scoring_basis_field": "reducer",
    }
