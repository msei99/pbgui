"""Offline tests for the isolated PB8 Strategy Explorer helper."""

from __future__ import annotations

import copy
import csv
import json
import math
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

import pb8_strategy_explorer_helper as helper


def _modules() -> dict:
    """Return a minimal dynamic PB8 module facade for pure helper tests."""
    spec = {
        "parameters": [
            {
                "name": "entry.initial_qty_pct",
                "label": "Initial quantity",
                "side": "long",
                "config_path": ["strategy", "long", "entry", "initial_qty_pct"],
                "default": 0.1,
                "bounds": [0.01, 0.5, 0.01],
            }
        ],
        "fixed_parameters": [
            {
                "name": "entry.mode",
                "side": "long",
                "config_path": ["strategy", "long", "entry", "mode"],
                "default": "initial",
                "allowed_values": ["initial", "all"],
            }
        ],
    }
    return {
        "prepare_config": lambda config, **_kwargs: copy.deepcopy(config),
        "sanitize": lambda config: copy.deepcopy(config),
        "template_config": lambda: {
            "backtest": {"exchanges": ["binance"], "start_date": "2026-01-01", "end_date": "2026-01-02"},
            "live": {
                "strategy_kind": "future_strategy",
                "approved_coins": {"long": ["BTC"], "short": []},
            },
            "bot": {
                "long": {
                    "risk": {"n_positions": 1, "total_wallet_exposure_limit": 1.0},
                    "forager": {"score_weights": {"volume": 0.5, "volatility": 0.5}},
                },
                "short": {"risk": {"n_positions": 0, "total_wallet_exposure_limit": 0.0}},
            },
        },
        "get_kinds": lambda: ("future_strategy",),
        "get_spec": lambda kind: spec if kind == "future_strategy" else {},
        "shared_groups": ("risk", "forager"),
        "shared_group_map": {
            "risk": {
                "n_positions": "n_positions",
                "total_wallet_exposure_limit": "total_wallet_exposure_limit",
            },
            "forager": {"score_weights": "forager_score_weights"},
        },
    }


def test_capabilities_uses_dynamic_strategy_metadata() -> None:
    """Capabilities must expose Rust-provided kinds and fields without a hardcoded strategy model."""
    modules = _modules()

    result = helper._capabilities({}, modules)

    assert result["ok"] is True
    assert result["engine"] == "pb8_engine"
    assert result["simulation_modes"] == [{"key": "pb8_engine", "label": "PB8 Native Replay"}]
    assert result["strategy"]["supported_kinds"] == ["future_strategy"]
    assert "entry.initial_qty_pct" in result["strategy"]["param_field_meta"]
    assert result["strategy"]["param_field_meta"]["entry.initial_qty_pct"]["path"] == (
        "bot.long.strategy.future_strategy.entry.initial_qty_pct"
    )
    assert result["strategy"]["param_field_meta"]["entry.initial_qty_pct"]["step"] == 0.01
    assert result["strategy"]["param_field_meta"]["entry.mode"]["type"] == "select"
    assert result["strategy"]["param_field_meta"]["entry.mode"]["options"] == ["initial", "all"]
    assert "forager_score_weights.volume" in result["strategy"]["param_field_meta"]
    assert result["canonical_config"]["live"]["strategy_kind"] == "future_strategy"


def test_canonicalize_preserves_pbgui_metadata() -> None:
    """PBGui-only metadata must not enter PB8 validation but must survive the round trip."""
    modules = _modules()
    seen = {}

    def prepare(config, **_kwargs):
        seen.update(copy.deepcopy(config))
        return copy.deepcopy(config)

    modules["prepare_config"] = prepare
    result = helper._canonicalize({"live": {}, "pbgui": {"runtime": "pb8"}}, modules)

    assert "pbgui" not in seen
    assert result["pbgui"] == {"runtime": "pb8"}


def test_normalize_native_fills_returns_pb7_event_shape() -> None:
    """The native 19-column fill matrix must map to the existing GUI event contract."""
    fills = [
        [0, 1_700_000_000_000, "BTC", 2.0, -0.1, 1002.0, 0.0, 1002.0, 30_000.0, 0.01, 100.0, 0.01, 100.0, "entry_grid_long", "maker", 0.001, 0.001, 0.0, 0.001],
        [1, 1_700_000_060_000, "BTC", 1.0, -0.1, 1003.0, 0.0, 1003.0, 30_000.0, 0.01, 101.0, -0.01, 101.0, "entry_grid_short", "maker", 0.001, 0.0, 0.001, -0.001],
    ]

    result = helper._normalize_native_fills(fills, 10)

    assert set(result) == {"long", "short"}
    assert result["long"][0]["order_type"] == "entry_grid_long"
    assert result["short"][0]["pos_size"] == pytest.approx(-0.01)
    assert {
        "timestamp",
        "event",
        "order_type",
        "coin",
        "qty",
        "price",
        "pos_size",
        "pos_price",
        "wallet_balance",
        "wallet_exposure",
        "pnl",
        "fee_paid",
    }.issubset(result["long"][0])


def test_ema_spans_are_inferred_from_nested_runtime_fields() -> None:
    """Version-neutral EMA grouping must react to arbitrary nested PB8 field names."""
    spans = helper._ema_spans(
        {"new_group": {"close_ema_span_1m": 12, "volume_ema_span_1m": 24}},
        {"future": {"entry_volatility_ema_span_1h": 48}},
    )

    assert spans["m1"]["close"] == {12.0}
    assert spans["m1"]["volume"] == {24.0}
    assert spans["h1"]["log_range"] == {48.0}


def test_ema_bundle_adds_geometric_strategy_span_per_side() -> None:
    """Rust must receive each side's exact geometric strategy EMA span."""
    data = {
        "hlcvs": np.asarray(
            [
                [[101.0, 99.0, 100.0, 10.0]],
                [[102.0, 100.0, 101.0, 11.0]],
            ],
            dtype=float,
        )
    }

    result = helper._ema_bundle(
        data,
        {},
        {
            "long": {"ema_span_0": 910.0, "ema_span_1": 1700.0},
            "short": {"ema_span_0": 100.0, "ema_span_1": 100.0},
        },
    )

    spans = {item[0] for item in result["m1"]["close"]}
    assert math.sqrt(910.0 * 1700.0) in spans
    assert 100.0 in spans


def test_orchestrator_bot_params_remove_python_aliases_and_flatten_hsl_tiers() -> None:
    """The strict Rust JSON boundary must not receive duplicate Python runtime aliases."""
    result = helper._orchestrator_bot_params(
        {
            "filter_volatility_ema_span_1m": 10.0,
            "filter_volume_ema_span_1m": 20.0,
            "forager_volatility_ema_span_1m": 10.0,
            "forager_volume_ema_span_1m": 20.0,
            "forager_volume_drop_pct": 0.1,
            "filter_volume_drop_pct": 0.1,
            "hsl_tier_ratios": {"yellow": 0.5, "orange": 0.75},
        }
    )

    assert "forager_volatility_ema_span_1m" not in result
    assert "forager_volume_ema_span_1m" not in result
    assert "filter_volume_drop_pct" not in result
    assert result["hsl_tier_ratio_yellow"] == pytest.approx(0.5)
    assert result["hsl_tier_ratio_orange"] == pytest.approx(0.75)


def test_snapshot_preserves_full_config_and_asymmetric_side_approval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single-market native calculation must not replace the complete Explorer config."""
    canonical = {
        "backtest": {
            "exchanges": ["binance", "bybit"],
            "start_date": "2026-01-01",
            "end_date": "2026-01-03",
            "suite_enabled": True,
            "scenarios": [{"name": "stress"}],
        },
        "live": {
            "strategy_kind": "future_strategy",
            "approved_coins": {"long": ["BTC"], "short": ["ETH"]},
        },
        "bot": {
            "long": {"risk": {"n_positions": 1, "total_wallet_exposure_limit": 1.0}},
            "short": {"risk": {"n_positions": 1, "total_wallet_exposure_limit": 1.0}},
        },
        "coin_overrides": {"BTC": {"long": {}}, "ETH": {"short": {}}},
    }
    restricted = copy.deepcopy(canonical)
    restricted["backtest"].update(
        {"exchanges": ["binance"], "suite_enabled": False, "scenarios": []}
    )
    restricted["live"]["approved_coins"] = {"long": ["BTC"], "short": []}
    restricted["coin_overrides"] = {"BTC": {"long": {}}}
    timestamps = np.asarray([0, 60_000, 120_000], dtype=np.int64)
    data = {
        "hlcvs": np.asarray(
            [[[101.0, 99.0, 100.0, 1.0]], [[102.0, 100.0, 101.0, 2.0]], [[103.0, 101.0, 102.0, 3.0]]]
        ),
        "mss": {"BTC": {"price_step": 0.1, "qty_step": 0.001}, "__meta__": {}},
        "timestamps": timestamps,
        "trade_idx": 0,
        "source_hlcvs": object(),
    }
    monkeypatch.setattr(helper, "_canonicalize", lambda _config, _modules: copy.deepcopy(canonical))
    monkeypatch.setattr(
        helper,
        "_restrict_config",
        lambda *_args: (copy.deepcopy(restricted), "binance", "BTC", 3),
    )
    monkeypatch.setattr(helper, "_native_data", lambda *_args, **_kwargs: data)
    modules = _modules()
    modules.update(
        {
            "backtest": SimpleNamespace(
                prep_backtest_args=lambda *_args: (
                    [{"long": {}, "short": {}}],
                    [{"long": {}, "short": {}}],
                    [{"c_mult": 1.0}],
                    {"strategy_kind": "future_strategy"},
                )
            ),
            "pbr": SimpleNamespace(compute_ideal_orders_json=lambda _payload: json.dumps({"orders": []})),
            "flatten_shared": lambda side: copy.deepcopy(side),
            "grouped_value": lambda side, key, default: (side.get("risk") or {}).get(key, default),
            "release": lambda _value: None,
        }
    )

    result = helper._snapshot(
        {"config": canonical, "options": {"exchange": "binance", "coin": "BTC", "context_days": 0.5}},
        modules,
    )

    assert result["config"] == canonical
    assert result["config"]["backtest"]["suite_enabled"] is True
    assert set(result["config"]["coin_overrides"]) == {"BTC", "ETH"}
    assert result["sides"]["long"]["active"] is True
    assert result["sides"]["short"]["active"] is False
    assert result["market"]["reference_price"] == pytest.approx(102.0)
    assert result["market"]["metadata"]["ohlcv"]["selected_start"] == "1970-01-01T00:00:00Z"
    assert result["market"]["metadata"]["ohlcv"]["grid_time"] == "1970-01-01T00:02:00Z"


def test_movie_bounds_frames_and_uses_real_replay_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Movie generation must cap frames and preserve native candles and fills."""
    candles = [
        {
            "timestamp": f"2026-01-01T00:{index:02d}:00Z",
            "timestamp_ms": index * 60_000,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0 + index,
            "volume": 1.0,
        }
        for index in range(10)
    ]
    event = {"timestamp_ms": 60_000, "wallet_balance": 1001.0, "pos_size": 1.0, "pos_price": 100.0}
    monkeypatch.setattr(
        helper,
        "_replay",
        lambda _request, _modules: {
            "ok": True,
            "candles": candles,
            "events": {"long": [event], "short": []},
            "metadata": {"exchange": "binance", "coin": "BTC"},
        },
    )
    monkeypatch.setattr(
        helper,
        "_movie_frames_with_native_orders",
        lambda _config, _options, _replay, selected, _modules: [
            {
                "index": index + 1,
                "timestamp": candle["timestamp"],
                "candle": candle,
                "long": {"orders": {"entries": [], "closes": []}},
                "short": {"orders": {"entries": [], "closes": []}},
            }
            for index, candle in enumerate(selected)
        ],
    )

    result = helper._movie(
        {"config": {}, "options": {"frames": 50_000, "step_mins": 2}}, {}
    )

    assert result["engine"] == "pb8_engine"
    assert len(result["frames"]) == 5
    assert result["frames"][0]["candle"] == {
        "timestamp": candles[0]["timestamp"],
        "timestamp_ms": candles[0]["timestamp_ms"],
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 2.0,
    }
    assert result["events"]["long"] == [event]


def test_movie_frames_include_native_orders_at_replay_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB8 movie frames must not invent orders from incomplete historical replay state."""
    config = {
        "backtest": {"starting_balance": 1000},
        "live": {"strategy_kind": "test"},
    }
    modules = {"pbr": SimpleNamespace(compute_ideal_orders_json=lambda _payload: pytest.fail("historical orders were recomputed"))}
    replay = {
        "metadata": {"coin": "BTC"},
        "events": {
            "long": [
                {
                    "timestamp_ms": 60_000,
                    "wallet_balance": 1001.0,
                    "pos_size": 1.0,
                    "pos_price": 100.0,
                    "wallet_exposure": 0.1,
                }
            ],
            "short": [],
        },
    }
    selected = [
        {
            "timestamp": "1970-01-01T00:01:00Z",
            "timestamp_ms": 60_000,
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 11.0,
        }
    ]

    frames = helper._movie_frames_with_native_orders(
        config, {"start_time": "00:00"}, replay, selected, modules
    )

    assert frames[0]["long"]["summary"] == {"entry_orders": 0, "close_orders": 0}
    assert frames[0]["long"]["orders"] == {"entries": [], "closes": []}
    assert frames[0]["long"]["debug"]["orders_available"] is False


def test_market_choice_rejects_values_outside_config_boundaries() -> None:
    """Explicit selector values must remain within configured exchanges and coins."""
    config = _modules()["template_config"]()

    assert helper._market_choice(config, {"exchange": "binance", "coin": "BTC"}) == (
        "binance",
        "BTC",
    )
    with pytest.raises(ValueError, match="exchange is not approved"):
        helper._market_choice(config, {"exchange": "bybit", "coin": "BTC"})
    with pytest.raises(ValueError, match="coin is not approved"):
        helper._market_choice(config, {"exchange": "binance", "coin": "ETH"})


def test_replay_maps_requested_start_to_aggregated_payload_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A minute-level selected start must become a candle index after PB8 aggregation."""
    import numpy as np

    timestamps = np.asarray([0, 60_000, 120_000, 180_000, 240_000], dtype=np.int64)
    data = {
        "hlcvs": np.ones((5, 1, 4), dtype=float),
        "mss": {"BTC": {}, "__meta__": {}},
        "btc_prices": np.ones(5, dtype=float),
        "timestamps": timestamps,
        "trade_idx": 3,
        "source_hlcvs": object(),
    }
    payload = SimpleNamespace(
        backtest_params={"trade_start_indices": [1]},
        bundle=SimpleNamespace(timestamps=np.asarray([0, 120_000, 240_000], dtype=np.int64)),
    )
    modules = _modules()
    monkeypatch.setattr(helper, "_restrict_config", lambda *_args: ({}, "binance", "BTC", 5))
    monkeypatch.setattr(helper, "_native_data", lambda *_args, **_kwargs: data)
    backtest = SimpleNamespace(
        build_backtest_payload=lambda *_args: payload,
        execute_backtest=lambda *_args: ([], [], {}),
    )
    modules.update({"backtest": backtest, "release": lambda _value: None})

    result = helper._replay(
        {"config": {}, "options": {}},
        modules,
    )

    assert result["ok"] is True
    assert payload.backtest_params["requested_start_timestamp_ms"] == 180_000
    assert payload.backtest_params["trade_start_indices"] == [2]


def test_stored_events_filter_selected_coin_before_limit(tmp_path: Path) -> None:
    """Stored multi-coin fills must compare only the selected replay coin."""
    columns = list(helper.FILL_COLUMNS)
    rows = [
        [0, 1_700_000_000_000, "ETH", 0, 0, 1000, 0, 1000, 30000, 1, 100, 1, 100, "entry_grid_long", "maker", 0, 0, 0, 0],
        [1, 1_700_000_060_000, "BTC", 0, 0, 1000, 0, 1000, 30000, 1, 101, 1, 101, "entry_grid_long", "maker", 0, 0, 0, 0],
        [2, 1_700_000_120_000, "BTC", 0, 0, 1000, 0, 1000, 30000, -1, 102, -1, 102, "entry_grid_short", "maker", 0, 0, 0, 0],
    ]
    with (tmp_path / "fills.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)

    events = helper._stored_events(tmp_path, 1, "BTC")

    assert len(events["long"]) == 1
    assert events["long"][0]["coin"] == "BTC"
    assert events["short"] == []


def test_stored_events_apply_time_window_before_order_limit(tmp_path: Path) -> None:
    """Old result fills must not consume the bounded compare window's order limit."""
    columns = list(helper.FILL_COLUMNS)
    rows = [
        [0, 1_700_000_000_000, "BTC", 0, 0, 1000, 0, 1000, 30000, 1, 100, 1, 100, "entry_grid_long", "maker", 0, 0, 0, 0],
        [1, 1_700_000_060_000, "BTC", 0, 0, 1000, 0, 1000, 30000, 1, 101, 2, 101, "entry_grid_long", "maker", 0, 0, 0, 0],
    ]
    with (tmp_path / "fills.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)

    events = helper._stored_events(
        tmp_path,
        1,
        "BTC",
        start_timestamp_ms=1_700_000_060_000,
        end_timestamp_ms=1_700_000_120_000,
    )

    assert len(events["long"]) == 1
    assert events["long"][0]["timestamp_ms"] == 1_700_000_060_000


def test_compare_rows_do_not_pair_events_outside_timestamp_tolerance() -> None:
    """Unrelated times are source-only events, not value mismatches paired by index."""
    left = {
        "long": [{"timestamp_ms": 1_000, "timestamp": "left", "order_type": "entry", "qty": 1, "price": 10}],
        "short": [],
    }
    right = {
        "long": [{"timestamp_ms": 10_000, "timestamp": "right", "order_type": "entry", "qty": 1, "price": 10}],
        "short": [],
    }

    summary, rows = helper._compare_rows(
        left,
        right,
        {"timestamp_tolerance_ms": 1_000},
        "pb7",
        "c",
    )

    assert summary["long"]["mismatch"] == 0
    assert summary["long"]["pb7_only"] == 1
    assert summary["long"]["c_only"] == 1
    assert [row["status"] for row in rows["long"]] == ["pb7_only", "c_only"]


def test_compare_rows_match_reordered_events_and_quantize_exchange_steps() -> None:
    """Same-time fills are a multiset and numeric identity follows exchange steps."""
    first = {"timestamp_ms": 1_000, "timestamp": "a", "order_type": "entry", "qty": 1.0, "price": 100_000.00}
    second = {"timestamp_ms": 1_000, "timestamp": "b", "order_type": "close", "qty": -1.0, "price": 100_100.00}
    left = {"long": [first, second], "short": []}
    right = {"long": [copy.deepcopy(second), copy.deepcopy(first)], "short": []}

    summary, rows = helper._compare_rows(
        left,
        right,
        {"price_step": 0.01, "qty_step": 0.001},
        "pb7",
        "c",
    )

    assert summary["long"]["match"] == 2
    assert [row["status"] for row in rows["long"]] == ["match", "match"]

    right["long"][1]["price"] = 100_000.05
    summary, rows = helper._compare_rows(
        {"long": [first], "short": []},
        {"long": [right["long"][1]], "short": []},
        {"price_step": 0.01, "qty_step": 0.001},
        "pb7",
        "c",
    )
    assert summary["long"]["match"] == 0
    assert summary["long"]["pb7_only"] == 1
    assert summary["long"]["c_only"] == 1


def test_compare_rejects_missing_or_runtime_identical_second_source() -> None:
    """A diagnostic replay must never masquerade as a successful comparison."""
    with pytest.raises(ValueError, match="requires validated stored fills"):
        helper._compare({"config": {}, "options": {}}, {})

    config = _modules()["template_config"]()
    baseline = copy.deepcopy(config)
    baseline["pbgui"] = {"note": "metadata-only difference"}
    with pytest.raises(ValueError, match="runtime-distinct"):
        helper._compare(
            {"config": config, "compare_config": baseline, "options": {}},
            _modules(),
        )


def test_compare_uses_first_stored_fill_and_exact_replay_window(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Stored-result comparison must replay and filter the same bounded time window."""
    seed_event = {
        "timestamp_ms": 1_700_000_000_000,
        "timestamp": "2023-11-14T22:13:20Z",
        "order_type": "entry",
        "qty": 1.0,
        "price": 100.0,
    }
    stored_calls = []

    def stored(_path, max_orders, coin, **bounds):
        stored_calls.append((max_orders, coin, bounds))
        return {"long": [seed_event], "short": []}

    def replay(request, _modules):
        assert request["options"]["start_date"] == "2023-11-14"
        assert request["options"]["start_time"] == "22:12"
        return {
            "events": {"long": [seed_event], "short": []},
            "metadata": {
                "coin": "BTC",
                "start_timestamp_ms": 1_700_000_000_000,
                "end_timestamp_ms": 1_700_000_600_000,
            },
        }

    monkeypatch.setattr(helper, "_safe_result_dir", lambda *_args: tmp_path)
    monkeypatch.setattr(helper, "_stored_events", stored)
    monkeypatch.setattr(
        helper,
        "_stored_fill_bounds",
        lambda *_args: (seed_event["timestamp_ms"], seed_event["timestamp_ms"], 1),
    )
    monkeypatch.setattr(helper, "_canonicalize", lambda config, _modules: copy.deepcopy(config))
    monkeypatch.setattr(helper, "_market_choice", lambda *_args: ("binance", "BTC"))
    monkeypatch.setattr(helper, "_replay", replay)

    result = helper._compare(
        {
            "config": {"backtest": {"candle_interval_minutes": 1}},
            "result_path": str(tmp_path),
            "options": {
                "coin": "BTC",
                "use_fills_range": True,
                "compare_max_orders": 100,
            },
        },
        {},
    )

    assert result["summary"]["long"]["match"] == 1
    assert stored_calls[0][:2] == (100, "BTC")
    assert stored_calls[0][2] == {
        "start_timestamp_ms": 1_700_000_000_000,
        "end_timestamp_ms": 1_700_000_600_000,
    }
    assert result["summary"]["coverage"]["partial"] is False


def test_stored_events_bound_scanned_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Non-selected rows in a compressed or plain result may not cause unbounded scans."""
    with (tmp_path / "fills.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(helper.FILL_COLUMNS)
        writer.writerow([0, 1_700_000_000_000, "ETH", 0, 0, 1000, 0, 1000, 30000, 1, 100, 1, 100, "entry_grid_long", "maker", 0, 0, 0, 0])
        writer.writerow([1, 1_700_000_060_000, "ETH", 0, 0, 1000, 0, 1000, 30000, 1, 101, 1, 101, "entry_grid_long", "maker", 0, 0, 0, 0])
    monkeypatch.setattr(helper, "MAX_RESULT_FILL_ROWS", 1)

    with pytest.raises(ValueError, match="row Strategy Explorer limit"):
        helper._stored_events(tmp_path, 10, "BTC")


def test_native_data_rejects_start_after_available_candles() -> None:
    """A future selected start must not silently replay the last historical candle."""
    import numpy as np

    released = []

    async def prepare(_config, _exchange):
        hlcvs = np.ones((2, 1, 4), dtype=float)
        return (
            ["BTC"],
            hlcvs,
            {"BTC": {"first_valid_index": 0, "last_valid_index": 1}, "__meta__": {}},
            None,
            None,
            np.ones(2, dtype=float),
            np.asarray([1_700_000_000_000, 1_700_000_060_000], dtype=np.int64),
        )

    modules = {
        "backtest": SimpleNamespace(prepare_hlcvs_mss=prepare),
        "release": lambda value: released.append(value),
    }
    config = {"backtest": {"start_date": "2026-01-01", "coins": {}}}

    with pytest.raises(ValueError, match="after the available"):
        helper._native_data(config, "binance", "BTC", 10, modules)

    assert len(released) == 1


@pytest.mark.parametrize("bad", ["", ".", "..", "BTC/USDT", "BTC\\USDT", "BTC\x00USDT"])
def test_market_identifier_rejects_unsafe_segments(bad: str) -> None:
    """Filesystem-facing exchange and coin segments must reject traversal forms."""
    with pytest.raises(ValueError):
        helper._identifier(bad, "coin")
