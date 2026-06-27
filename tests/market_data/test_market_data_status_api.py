"""Tests for market-data status API filtering."""

from pathlib import Path
import importlib
import importlib.util
import sys


repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root))

pbcoindata_path = repo_root / "PBCoinData.py"
pbcoindata_spec = importlib.util.spec_from_file_location("PBCoinData", pbcoindata_path)
pbcoindata_module = importlib.util.module_from_spec(pbcoindata_spec)
sys.modules["PBCoinData"] = pbcoindata_module
pbcoindata_spec.loader.exec_module(pbcoindata_module)

market_data_api = importlib.import_module("api.market_data")


def test_filter_status_coins_to_enabled_prunes_removed_coin(monkeypatch) -> None:
    """Status payloads keep only currently enabled coins."""

    monkeypatch.setattr(market_data_api, "load_market_data_config", lambda: object())
    monkeypatch.setattr(
        market_data_api,
        "get_effective_enabled_coins",
        lambda exchange, cfg=None: (["BTC", "ETH"], [], True),
    )

    status = {
        "coins": {
            "BTC": {"result": "ok"},
            "OM": {"result": "ok"},
        },
        "coins_total": 3,
        "coins_done": 2,
        "current_coin": "OM",
    }

    filtered = market_data_api._filter_status_coins_to_enabled("hyperliquid", status)

    assert filtered["coins"] == {"BTC": {"result": "ok"}}
    assert filtered["coins_total"] == 2
    assert filtered["coins_done"] == 2
    assert filtered["current_coin"] == ""


def test_okx_status_and_best_1m_wiring() -> None:
    """OKX has status flag keys and Best 1m queue metadata."""

    assert market_data_api._get_exchange_status_key("okx") == "okx_latest_1m"
    assert market_data_api._get_exchange_flag_prefix("okx") == "okx_latest_1m"

    meta = market_data_api._best_1m_exchange_meta("okx")
    assert meta is not None
    assert meta["label"] == "OKX"
    assert meta["job_type"] == "okx_best_1m"
    assert meta["queue_exchange"] == "okx"


def test_best_1m_available_coins_do_not_require_enabled_settings(monkeypatch) -> None:
    """Manual Best 1m builds list available coins, not auto-refresh enabled coins."""

    monkeypatch.setattr(market_data_api, "get_market_data_coin_options", lambda exchange: ["BTC", "ETH"])
    monkeypatch.setattr(
        market_data_api,
        "get_effective_enabled_coins",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("enabled coins should not be used")),
    )

    assert market_data_api._get_best_1m_available_coins("okx") == ["BTC", "ETH"]
