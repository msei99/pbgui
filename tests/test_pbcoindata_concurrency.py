"""Multiprocess locking and durable publication tests for CoinData state."""

from __future__ import annotations

import json
import importlib.util
import multiprocessing
import os
from pathlib import Path
import stat
import threading
import time

import pytest

ROOT_DIR = Path(__file__).parent.parent.resolve()
MODULE_SPEC = importlib.util.spec_from_file_location("PBCoinData_concurrency_real", ROOT_DIR / "PBCoinData.py")
PBCoinData = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(PBCoinData)
CoinData = PBCoinData.CoinData
CoinDataPersistenceError = PBCoinData.CoinDataPersistenceError


def _bare_coindata() -> CoinData:
    """Build the minimal disk-cache state needed without touching runtime config."""
    coindata = CoinData.__new__(CoinData)
    coindata._ccxt_markets = {}
    coindata._exchange_mappings = {}
    coindata._exchange_mapping_ts = {}
    coindata._copy_trading_cache = {}
    coindata._last_build_mapping_stats = {}
    return coindata


def _refresh_trace_worker(root: str, start_event) -> None:
    """Run a mocked refresh whose stage order is recorded by one child process."""
    os.chdir(root)
    PBCoinData._log = lambda *_args, **_kwargs: None
    coindata = _bare_coindata()
    trace_path = Path(root) / "refresh-trace.txt"

    def mark(stage: str) -> None:
        """Publish one stage marker while the refresh lock should be held."""
        with trace_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{os.getpid()}:{stage}\n")
            handle.flush()
            os.fsync(handle.fileno())
        time.sleep(0.03)

    def fetch_markets(_exchange: str) -> bool:
        """Record the market-fetch stage."""
        mark("markets")
        return True

    def load_markets(_exchange: str, *, use_cache: bool = True) -> dict:
        """Record the fresh market-cache read."""
        del use_cache
        mark("load_markets")
        return {"BTC/USDT:USDT": {"id": "BTCUSDT"}}

    def fetch_copy_trading(_exchange: str, _markets: dict) -> list:
        """Record the copy-trading stage."""
        mark("copy")
        return []

    def build_mapping(_exchange: str) -> bool:
        """Record the mapping-build stage."""
        mark("mapping")
        return True

    def update_prices(_exchange: str) -> bool:
        """Record the price-publication stage."""
        mark("prices")
        return True

    def load_mapping(*, exchange: str, use_cache: bool = True) -> list:
        """Record the final fresh mapping read."""
        del exchange, use_cache
        mark("load_mapping")
        return [{"active": True, "price_last": 1.0}]

    coindata.fetch_ccxt_markets = fetch_markets
    coindata.load_ccxt_markets = load_markets
    coindata.fetch_copy_trading_symbols = fetch_copy_trading
    coindata.build_mapping = build_mapping
    coindata.update_prices = update_prices
    coindata.load_mapping = load_mapping

    if not start_event.wait(5):
        raise RuntimeError("refresh start event timed out")
    result = coindata.refresh_exchange_mapping("binance")
    if not result["ok"]:
        raise RuntimeError(f"refresh failed: {result}")


def _price_update_worker(
    root: str,
    target_symbol: str,
    price: float,
    started_event,
    fetch_entered_event,
    release_event,
) -> None:
    """Update one mapping row from a child process using the real RMW method."""
    os.chdir(root)
    PBCoinData._log = lambda *_args, **_kwargs: None

    import Exchange as exchange_module

    class FakeExchange:
        """Return a price for only the row owned by this process."""

        def __init__(self, _exchange_id: str):
            """Retain no external exchange state."""

        def connect(self) -> None:
            """Avoid external exchange connections."""

        def fetch_prices(self, _symbols: list[str], _market_type: str) -> dict:
            """Pause the first writer after its mapping snapshot was read."""
            fetch_entered_event.set()
            if release_event is not None and not release_event.wait(5):
                raise RuntimeError("price release event timed out")
            return {target_symbol: {"last": price, "timestamp": int(price * 1000)}}

        def fetch_price(self, _symbol: str, _market_type: str) -> None:
            """Leave the other row unchanged during fallback fetching."""
            return None

    exchange_module.Exchange = FakeExchange
    coindata = _bare_coindata()
    started_event.set()
    coindata.update_prices("binance")


@pytest.mark.skipif("fork" not in multiprocessing.get_all_start_methods(), reason="requires fork")
def test_refresh_transaction_is_serialized_across_processes(tmp_path: Path) -> None:
    """No second process can interleave any stage of an exchange refresh."""
    context = multiprocessing.get_context("fork")
    start_event = context.Event()
    processes = [
        context.Process(target=_refresh_trace_worker, args=(str(tmp_path), start_event))
        for _ in range(2)
    ]

    for process in processes:
        process.start()
    start_event.set()
    for process in processes:
        process.join(10)

    assert [process.exitcode for process in processes] == [0, 0]
    entries = (tmp_path / "refresh-trace.txt").read_text(encoding="utf-8").splitlines()
    expected_stages = ["markets", "load_markets", "copy", "mapping", "prices", "load_mapping"]
    assert len(entries) == len(expected_stages) * 2
    assert [entry.split(":", 1)[1] for entry in entries[:6]] == expected_stages
    assert [entry.split(":", 1)[1] for entry in entries[6:]] == expected_stages
    assert len({entry.split(":", 1)[0] for entry in entries[:6]}) == 1
    assert len({entry.split(":", 1)[0] for entry in entries[6:]}) == 1
    assert entries[0].split(":", 1)[0] != entries[6].split(":", 1)[0]


@pytest.mark.skipif("fork" not in multiprocessing.get_all_start_methods(), reason="requires fork")
def test_price_rmw_waits_then_preserves_newer_process_update(tmp_path: Path) -> None:
    """A blocked stale reader reloads the mapping and cannot lose the first update."""
    mapping_dir = tmp_path / "data" / "coindata" / "binance"
    mapping_dir.mkdir(parents=True)
    mapping_path = mapping_dir / "mapping.json"
    mapping_path.write_text(
        json.dumps(
            [
                {"symbol": "AAAUSDT", "ccxt_symbol": "AAA/USDT:USDT", "active": True},
                {"symbol": "BBBUSDT", "ccxt_symbol": "BBB/USDT:USDT", "active": True},
            ],
            indent=4,
        ),
        encoding="utf-8",
    )

    context = multiprocessing.get_context("fork")
    first_started = context.Event()
    first_fetch_entered = context.Event()
    first_release = context.Event()
    second_started = context.Event()
    second_fetch_entered = context.Event()
    first = context.Process(
        target=_price_update_worker,
        args=(
            str(tmp_path),
            "AAA/USDT:USDT",
            11.0,
            first_started,
            first_fetch_entered,
            first_release,
        ),
    )
    second = context.Process(
        target=_price_update_worker,
        args=(
            str(tmp_path),
            "BBB/USDT:USDT",
            22.0,
            second_started,
            second_fetch_entered,
            None,
        ),
    )

    first.start()
    assert first_started.wait(5)
    assert first_fetch_entered.wait(5)
    second.start()
    try:
        assert second_started.wait(5)
        assert not second_fetch_entered.wait(0.4)
    finally:
        first_release.set()
        first.join(10)
        second.join(10)

    assert first.exitcode == 0
    assert second.exitcode == 0
    assert second_fetch_entered.is_set()
    rows = {row["symbol"]: row for row in json.loads(mapping_path.read_text(encoding="utf-8"))}
    assert rows["AAAUSDT"]["price_last"] == 11.0
    assert rows["BBBUSDT"]["price_last"] == 22.0


def test_json_publication_uses_unique_fsynced_temporaries(monkeypatch, tmp_path: Path) -> None:
    """Each publication fsyncs and replaces from a distinct temporary path."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(PBCoinData, "_log", lambda *_args, **_kwargs: None)
    replaced_sources = []
    fsync_calls = []
    original_replace = PBCoinData.os.replace
    original_fsync = PBCoinData.os.fsync

    def tracked_replace(source, destination) -> None:
        """Capture unique source paths while preserving replacement behavior."""
        replaced_sources.append(Path(source))
        original_replace(source, destination)

    def tracked_fsync(file_descriptor: int) -> None:
        """Capture durability calls while preserving fsync behavior."""
        mode = os.fstat(file_descriptor).st_mode
        fsync_calls.append("directory" if stat.S_ISDIR(mode) else "file")
        original_fsync(file_descriptor)

    monkeypatch.setattr(PBCoinData.os, "replace", tracked_replace)
    monkeypatch.setattr(PBCoinData.os, "fsync", tracked_fsync)
    coindata = _bare_coindata()
    coindata.save_exchange_mapping("binance", [{"symbol": "AAAUSDT"}])
    coindata.save_exchange_mapping("binance", [{"symbol": "BBBUSDT"}])

    assert fsync_calls == ["file", "directory", "file", "directory"]
    assert len(replaced_sources) == 2
    assert replaced_sources[0] != replaced_sources[1]
    assert all(path.name.startswith(".mapping.json.") and path.suffix == ".tmp" for path in replaced_sources)
    assert not list((tmp_path / "data" / "coindata" / "binance").glob("*.tmp"))


def test_mapping_publication_failure_propagates_and_preserves_file(monkeypatch, tmp_path: Path) -> None:
    """A failed replace leaves the old mapping and cannot be reported as success."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(PBCoinData, "_log", lambda *_args, **_kwargs: None)
    coindata = _bare_coindata()
    coindata.save_exchange_mapping("binance", [{"symbol": "OLDUSDT"}])
    mapping_path = tmp_path / "data" / "coindata" / "binance" / "mapping.json"

    def fail_replace(_source, _destination) -> None:
        """Simulate an operating-system publication failure."""
        raise OSError("replace failed")

    monkeypatch.setattr(PBCoinData.os, "replace", fail_replace)
    with pytest.raises(CoinDataPersistenceError):
        coindata.save_exchange_mapping("binance", [{"symbol": "NEWUSDT"}])

    assert json.loads(mapping_path.read_text(encoding="utf-8")) == [{"symbol": "OLDUSDT"}]
    assert not list(mapping_path.parent.glob("*.tmp"))


@pytest.mark.parametrize(
    ("field", "fetch_name", "load_name", "save_name", "filename", "old_payload", "new_payload"),
    [
        (
            "data",
            "fetch_data",
            "load_data",
            "save_data",
            "coindata.json",
            {"data": [{"id": 1}]},
            {"data": [{"id": 2}]},
        ),
        (
            "metadata",
            "fetch_metadata",
            "load_metadata",
            "save_metadata",
            "metadata.json",
            {"data": {"1": {"slug": "old"}}},
            {"data": {"2": {"slug": "new"}}},
        ),
    ],
)
def test_paused_older_loader_cannot_republish_after_newer_owner(
    monkeypatch,
    tmp_path: Path,
    field: str,
    fetch_name: str,
    load_name: str,
    save_name: str,
    filename: str,
    old_payload: dict,
    new_payload: dict,
) -> None:
    """A loader resuming with stale memory cannot overwrite a newer locked publication."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(PBCoinData, "_log", lambda *_args, **_kwargs: None)
    older = _bare_coindata()
    older.fetch_interval = 1
    older.metadata_interval = 1
    older.data = None
    older.metadata = None
    older._has_cmc_api_key = lambda: True
    older_published = threading.Event()
    resume_older = threading.Event()
    errors = []

    def paused_fetch() -> bool:
        """Model the older owner publishing under its fetch lock, then pausing its caller."""
        setattr(older, field, old_payload)
        getattr(CoinData, save_name)(older)
        older_published.set()
        if not resume_older.wait(5):
            raise RuntimeError("older loader resume timed out")
        return True

    setattr(older, fetch_name, paused_fetch)

    def run_older_loader() -> None:
        """Capture worker errors so thread failures cannot pass silently."""
        try:
            getattr(older, load_name)()
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_older_loader)
    thread.start()
    assert older_published.wait(5)

    newer = _bare_coindata()
    setattr(newer, field, new_payload)
    getattr(newer, save_name)()
    resume_older.set()
    thread.join(5)

    assert not thread.is_alive()
    assert errors == []
    cache_path = tmp_path / "data" / "coindata" / filename
    assert json.loads(cache_path.read_text(encoding="utf-8")) == new_payload
