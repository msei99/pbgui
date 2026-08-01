"""PB8 Market Data cache inventory regression tests."""

from pathlib import Path

from api import market_data as market_data_api
import inventory_cache


def test_pb8_cache_is_a_separate_read_only_inventory_view() -> None:
    """Every exchange exposes PB8 cache inventory independently from PB7."""
    views = market_data_api._inventory_views_for_exchange("binance")

    assert {item["key"] for item in views} >= {"pb7_cache", "pb8_cache"}
    assert market_data_api.INVENTORY_VIEW_META["pb8_cache"]["read_only"] is True
    assert market_data_api._normalize_inventory_view("PB8 cache") == "pb8_cache"


def test_pb8_cache_inventory_reads_only_pb8_root(tmp_path: Path, monkeypatch) -> None:
    """PB8 inventory discovers timeframe/coin files below the configured PB8 checkout."""
    pb8_root = tmp_path / "pb8"
    coin_dir = pb8_root / "caches" / "ohlcv" / "binance" / "1m" / "BTC"
    coin_dir.mkdir(parents=True)
    (coin_dir / "2026-01-02.npy").write_bytes(b"pb8-cache")
    monkeypatch.setattr(inventory_cache, "_get_pb8_root_dir", lambda: pb8_root)
    monkeypatch.setattr(inventory_cache, "_db_path", lambda: tmp_path / "inventory.db")
    monkeypatch.setattr(market_data_api, "_get_pb8_root_dir", lambda: pb8_root)

    payload = market_data_api._build_inventory_dataset_payload("binance", "pb8_cache")

    assert payload["view"] == "pb8_cache"
    assert payload["read_only"] is True
    assert payload["rows"][0]["dataset"] == "pb8_cache:1m"
    assert payload["rows"][0]["coin"] == "BTC"
    assert payload["rows"][0]["n_files"] == 1
    assert "pb8/caches/ohlcv" in payload["helper_note"]


def test_pb8_cache_mutation_routes_are_rejected() -> None:
    """Authenticated mutation endpoints still enforce PB8 inventory read-only status."""
    result = market_data_api.delete_inventory_selected(
        "binance",
        {"view": "pb8_cache", "coins": ["BTC"]},
        session=None,
    )

    assert result["success"] is False
    assert result["error"] == "PB8 cache is read-only."
