"""Regression tests for shared PB7/PB8 balance calculation."""

from pathlib import Path

from fastapi import HTTPException
import pytest

from api import balance_calc


def _write_instance(root: Path, name: str) -> None:
    """Create one minimal instance config below a temporary run root."""
    instance_dir = root / name
    instance_dir.mkdir(parents=True)
    (instance_dir / "config.json").write_text("{}\n", encoding="utf-8")


def test_instances_lists_pb7_and_pb8_without_filesystem_paths(tmp_path, monkeypatch) -> None:
    """The picker must expose versioned names from both run roots, but no paths."""
    run_v7 = tmp_path / "run_v7"
    run_v8 = tmp_path / "run_v8"
    _write_instance(run_v7, "shared")
    _write_instance(run_v8, "shared")
    _write_instance(run_v8, "v8-only")
    monkeypatch.setattr(balance_calc, "RUN_V7_DIR", run_v7)
    monkeypatch.setattr(balance_calc, "RUN_V8_DIR", run_v8)

    instances = balance_calc.get_instances(session=object())

    assert instances == [
        {"name": "shared", "version": "v7"},
        {"name": "shared", "version": "v8"},
        {"name": "v8-only", "version": "v8"},
    ]
    assert all("config_file" not in instance for instance in instances)


@pytest.mark.parametrize("version", ["v7", "v8"])
def test_load_config_uses_selected_version_and_resolves_exchange(version, tmp_path, monkeypatch) -> None:
    """A version-labelled selection must load from its matching root and return its exchange."""
    run_v7 = tmp_path / "run_v7"
    run_v8 = tmp_path / "run_v8"
    _write_instance(run_v7, "same-name")
    _write_instance(run_v8, "same-name")
    monkeypatch.setattr(balance_calc, "RUN_V7_DIR", run_v7)
    monkeypatch.setattr(balance_calc, "RUN_V8_DIR", run_v8)
    monkeypatch.setattr(
        balance_calc,
        "load_pb7_config",
        lambda path, neutralize_added=False: {"source": "v7", "live": {"user": "alice"}},
    )
    monkeypatch.setattr(
        balance_calc,
        "load_pb8_config",
        lambda path: {"source": "v8", "live": {"user": "alice"}},
    )

    class FakeUsers:
        """Resolve the fixture user without touching real credentials."""

        def find_exchange(self, user):
            """Return the fixture exchange."""
            return "Bybit" if user == "alice" else None

    monkeypatch.setattr(balance_calc, "Users", FakeUsers)

    result = balance_calc.load_config({"version": version, "name": "same-name"}, session=object())

    assert result == {"config": {"source": version, "live": {"user": "alice"}}, "exchange": "bybit"}


@pytest.mark.parametrize(
    ("body", "status_code"),
    [
        ({"version": "v9", "name": "safe"}, 400),
        ({"version": "v7", "name": "../escape"}, 400),
        ({"version": "v8", "name": ".hidden"}, 400),
    ],
)
def test_load_config_rejects_invalid_version_or_name(body, status_code) -> None:
    """Version and instance identifiers must be validated before filesystem access."""
    with pytest.raises(HTTPException) as exc_info:
        balance_calc.load_config(body, session=object())

    assert exc_info.value.status_code == status_code


def test_extract_bot_params_preserves_pb7_side_layout() -> None:
    """PB7 flat side parameters must retain their existing interpretation."""
    config = {
        "bot": {
            "long": {
                "n_positions": 3,
                "total_wallet_exposure_limit": 1.5,
                "entry_initial_qty_pct": 0.02,
            },
            "short": {},
        }
    }

    params = balance_calc._extract_bot_params(config)

    assert params["long"] == {
        "n_positions": 3.0,
        "total_wallet_exposure_limit": 1.5,
        "entry_initial_qty_pct": 0.02,
    }


def test_extract_bot_params_uses_active_pb8_strategy() -> None:
    """PB8 risk and active strategy entry paths must feed the shared formula."""
    config = {
        "config_version": "v8.0.0",
        "live": {"strategy_kind": "trailing_grid_v7"},
        "bot": {
            "long": {
                "risk": {"n_positions": 4, "total_wallet_exposure_limit": 2.0},
                "strategy": {
                    "trailing_grid_v7": {"entry": {"initial_qty_pct": 0.025}},
                    "inactive": {"entry": {"initial_qty_pct": 0.5}},
                },
            },
            "short": {},
        },
    }

    params = balance_calc._extract_bot_params(config)

    assert params["long"] == {
        "n_positions": 4.0,
        "total_wallet_exposure_limit": 2.0,
        "entry_initial_qty_pct": 0.025,
    }


def test_calculate_supports_pb8_config(monkeypatch) -> None:
    """A canonical PB8 config must produce the same balance recommendation contract."""
    config = {
        "config_version": "v8.0.0",
        "live": {
            "strategy_kind": "trailing_martingale",
            "approved_coins": {"long": ["BTC"], "short": []},
        },
        "bot": {
            "long": {
                "risk": {"n_positions": 4, "total_wallet_exposure_limit": 2.0},
                "strategy": {"trailing_martingale": {"entry": {"initial_qty_pct": 0.025}}},
            },
            "short": {},
        },
    }
    monkeypatch.setattr(
        balance_calc,
        "_load_mapping",
        lambda _exchange: [
            {
                "coin": "BTC",
                "quote": "USDT",
                "price_last": 100,
                "contract_size": 1,
                "min_amount": 0.1,
                "min_cost": 0,
                "active": True,
                "swap": True,
                "linear": True,
            }
        ],
    )

    result = balance_calc._calculate(config, "binance")

    assert result["balance_long"] == [{"coin": "BTC", "balance": 800.0}]
    assert result["recommendation"]["recommended_balance"] == 880


def test_calculate_supports_pb8_ema_anchor_base_qty(monkeypatch) -> None:
    """EMA Anchor's root base_qty_pct must produce an inline and standalone recommendation."""
    config = {
        "config_version": "v8.2.0",
        "live": {
            "strategy_kind": "ema_anchor",
            "approved_coins": {"long": ["HYPE"], "short": []},
        },
        "bot": {
            "long": {
                "risk": {"n_positions": 1, "total_wallet_exposure_limit": 6.0},
                "strategy": {"ema_anchor": {"base_qty_pct": 0.0478}},
            },
            "short": {},
        },
    }
    monkeypatch.setattr(
        balance_calc,
        "_load_mapping",
        lambda _exchange: [
            {
                "coin": "HYPE",
                "quote": "USDC",
                "price_last": 76.23,
                "contract_size": 1,
                "min_amount": 0.01,
                "min_cost": 10,
                "active": True,
                "swap": True,
                "linear": True,
            }
        ],
    )

    result = balance_calc._calculate(config, "hyperliquid")

    assert result["bot_params"]["long"]["entry_initial_qty_pct"] == 0.0478
    assert result["balance_long"] == [{"coin": "HYPE", "balance": 34.87}]
    assert result["recommendation"]["recommended_balance"] == 40


def test_pb8_all_expands_only_eligible_mapped_swaps_and_applies_ignored_coins(monkeypatch) -> None:
    """PB8's all sentinel must not become a literal ALL token or include ineligible markets."""
    config = {
        "live": {
            "strategy_kind": "trailing_martingale",
            "approved_coins": "all",
            "ignored_coins": {"long": ["ETH"], "short": []},
        },
        "bot": {
            "long": {
                "risk": {"n_positions": 2, "total_wallet_exposure_limit": 1.0},
                "strategy": {"trailing_martingale": {"entry": {"initial_qty_pct": 0.1}}},
            },
            "short": {},
        },
    }
    monkeypatch.setattr(
        balance_calc,
        "_load_mapping",
        lambda _exchange: [
            {"coin": "BTC", "quote": "USDT", "active": True, "swap": True, "linear": True, "price_last": 100, "min_amount": 0.01},
            {"coin": "ETH", "quote": "USDT", "active": True, "swap": True, "linear": True, "price_last": 10, "min_amount": 0.1},
            {"coin": "ALL", "quote": "USDT", "active": True, "swap": False, "linear": True, "price_last": 1, "min_amount": 1},
            {"coin": "DOGE", "quote": "USDC", "active": True, "swap": True, "linear": True, "price_last": 1, "min_amount": 1},
        ],
    )

    result = balance_calc._calculate(config, "binance")

    assert [item["coin"] for item in result["coin_infos"]] == ["BTC", "ETH"]
    assert [item["coin"] for item in result["balance_long"]] == ["BTC"]


def test_balance_calculator_skips_malformed_mapping_rows(monkeypatch) -> None:
    """One malformed local mapping row must not hide otherwise usable eligible markets."""
    config = {
        "live": {"strategy_kind": "grid", "approved_coins": {"long": ["BTC"], "short": []}},
        "bot": {
            "long": {
                "risk": {"n_positions": 1, "total_wallet_exposure_limit": 1},
                "strategy": {"grid": {"entry": {"initial_qty_pct": 0.1}}},
            },
            "short": {},
        },
    }
    monkeypatch.setattr(
        balance_calc,
        "_load_mapping",
        lambda _exchange: [
            "invalid",
            {"coin": "BROKEN", "quote": "USDT", "active": True, "swap": True, "linear": True, "price_last": "N/A"},
            {"coin": "BTC", "quote": "USDT", "active": True, "swap": True, "linear": True, "price_last": 100, "min_amount": 0.01},
        ],
    )

    result = balance_calc._calculate(config, "binance")

    assert [item["coin"] for item in result["balance_long"]] == ["BTC"]


def test_balance_calculator_derives_coin_from_mapping_symbol(monkeypatch) -> None:
    """Legacy mapping rows without coin retain the established symbol-to-coin fallback."""
    config = {
        "live": {"strategy_kind": "grid", "approved_coins": {"long": ["BTC"], "short": []}},
        "bot": {
            "long": {
                "risk": {"n_positions": 1, "total_wallet_exposure_limit": 1},
                "strategy": {"grid": {"entry": {"initial_qty_pct": 0.1}}},
            },
            "short": {},
        },
    }
    monkeypatch.setattr(
        balance_calc,
        "_load_mapping",
        lambda _exchange: [
            {"symbol": "BTCUSDT", "quote": "USDT", "active": True, "swap": True, "linear": True, "price_last": 100, "min_amount": 0.01}
        ],
    )

    result = balance_calc._calculate(config, "binance")

    assert [item["coin"] for item in result["balance_long"]] == ["BTC"]


def test_balance_calculator_errors_when_all_eligible_rows_lack_usable_numbers(monkeypatch) -> None:
    """An eligible coin universe without usable minimum-order data must not look successful."""
    config = {"live": {"approved_coins": "all"}, "bot": {"long": {}, "short": {}}}
    monkeypatch.setattr(
        balance_calc,
        "_load_mapping",
        lambda _exchange: [
            {"coin": "BTC", "quote": "USDT", "active": True, "swap": True, "linear": True, "price_last": "N/A"}
        ],
    )

    result = balance_calc._calculate(config, "binance")

    assert "No eligible approved coins" in result["error"]
