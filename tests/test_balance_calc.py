"""Regression tests for shared PB7/PB8 balance calculation."""

from api import balance_calc


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
