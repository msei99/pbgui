"""Regression tests for the PBGui optimize runtime status parser.

These tests lock down the live dashboard data extraction added for the
FastAPI optimize page:
- parsing real optimizer progress lines from optimize logs
- combining parsed log data with config, queue, and runtime stats

Background:
The optimize page now shows a live dashboard above the shared log viewer.
That dashboard depends on api.optimize_v7 parsing the optimizer log format
correctly even while runs are still active.
"""

import json
import sys
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def normalize_symbol(symbol, symbol_mappings=None):
    base = str(symbol or "")
    for quote in ("USDT", "USDC", "BUSD", "TUSD", "USD", "EUR", "GBP", "DAI"):
        if base.endswith(quote):
            base = base[:-len(quote)]
            break
    if base.startswith("1000"):
        base = base[4:]
    if base.startswith("k") and len(base) > 1 and base[1:].isupper():
        base = base[1:]
    return base


def compute_coin_name(market_id, quote=""):
    return normalize_symbol(market_id)


def get_symbol_for_coin(coin, *args, **kwargs):
    return normalize_symbol(coin)


class MockCoinData:
    def __init__(self):
        self._symbols = []

    def load_symbols(self, exchange):
        pass

    def load_symbols_all(self):
        pass


PBCoinData = ModuleType("PBCoinData")
PBCoinData.normalize_symbol = normalize_symbol
PBCoinData.compute_coin_name = compute_coin_name
PBCoinData.get_symbol_for_coin = get_symbol_for_coin
PBCoinData.CoinData = MockCoinData
sys.modules["PBCoinData"] = PBCoinData


from api import optimize_v7  # noqa: E402


LOG_SAMPLE = """2026-03-30T12:10:14Z INFO Selected optimizer backend: pymoo
2026-03-30T12:10:14Z INFO Loaded 1 starting configs before quantization (population size=325)
2026-03-30T12:10:14Z INFO Using pymoo nsga3 | n_obj=3 | ref_dirs=325 | n_partitions=24 (auto)
2026-03-30T12:10:15Z INFO Evaluated 1/1 starting configs
2026-03-30T12:10:16Z INFO Starting optimize...
2026-03-30T12:10:40Z INFO Iter: 4601 | Pareto ↑ | +1/-0 | size:26 | adg_usd:(0.00054554,0.005997) | omega_ratio_usd:(34.228,128.3829) | sharpe_ratio_usd:(0.0203019,0.0765356) | constraint:(0.0,0.0)
2026-03-30T12:10:40Z INFO Pareto update | eval=4601 | front=26 | objectives=[adg_usd=0.0015244, omega_ratio_usd=78.3137, sharpe_ratio_usd=0.0738] | constraint=0.0
"""


def test_optimize_log_parser_extracts_live_progress_fields():
    """Parse real optimize log lines into dashboard-ready progress fields."""
    summary = optimize_v7._parse_optimize_log_summary(LOG_SAMPLE)

    assert summary["phase"] == "optimizing"
    assert summary["backend"] == "pymoo"
    assert summary["algorithm"] == "nsga3"
    assert summary["objective_count"] == 3
    assert summary["ref_dirs"] == 325
    assert summary["n_partitions"] == 24
    assert summary["starting_configs_loaded"] == 1
    assert summary["starting_configs_done"] == 1
    assert summary["starting_configs_total"] == 1
    assert summary["population_size"] == 325
    assert summary["iter"] == 4601
    assert summary["eval"] == 4601
    assert summary["front"] == 26
    assert summary["pareto_added"] == 1
    assert summary["pareto_removed"] == 0
    assert summary["constraint"] == 0.0
    assert summary["objectives"] == {
        "adg_usd": 0.0015244,
        "omega_ratio_usd": 78.3137,
        "sharpe_ratio_usd": 0.0738,
    }
    assert summary["objective_ranges"]["adg_usd"] == {"min": 0.00054554, "max": 0.005997}
    assert summary["last_line"].startswith("Pareto update | eval=4601")


def test_build_runtime_status_merges_log_progress_with_runtime_metadata(monkeypatch, tmp_path):
    """Build the optimize runtime payload used by the floating log dashboard."""
    config_path = tmp_path / "job.json"
    config_path.write_text(json.dumps({"optimize": {"backend": "pymoo", "iters": 10000, "n_cpus": 6}}))
    log_path = tmp_path / "job.log"
    log_path.write_text(LOG_SAMPLE)

    monkeypatch.setattr(optimize_v7, "load_pb7_config", lambda path: json.loads(config_path.read_text()))
    monkeypatch.setattr(optimize_v7, "_load_queue_sync", lambda: [
        {"status": "optimizing"},
        {"status": "queued"},
        {"status": "error"},
    ])
    monkeypatch.setattr(optimize_v7, "_collect_optimize_process_stats", lambda pid: {
        "running": True,
        "pid": pid,
        "status": "running",
        "rss_bytes": 2147483648,
        "memory_percent": 6.5,
        "threads": 12,
        "children": 4,
        "started_at": "2026-03-30T12:00:00+00:00",
    })
    monkeypatch.setattr(optimize_v7, "_collect_optimize_system_stats", lambda: {
        "cpu_percent": 73.2,
        "memory_percent": 64.1,
        "memory_used_bytes": 17179869184,
        "memory_total_bytes": 34359738368,
        "load_avg": (2.1, 2.4, 2.8),
    })
    monkeypatch.setattr(optimize_v7._store, "_migrate_old_log", lambda filename, path: None)

    payload = optimize_v7._build_optimize_runtime_status(
        {
            "filename": "job.json",
            "name": "ETH optimize",
            "status": "optimizing",
            "pid": 1234,
            "json": str(config_path),
            "log_path": str(log_path),
            "seed_mode": "result",
            "exchange": "bybit",
        }
    )

    assert payload["phase"] == "optimizing"
    assert payload["progress"]["eval"] == 4601
    assert payload["progress"]["target_iters"] == 10000
    assert round(payload["progress"]["percent"], 2) == 46.01
    assert payload["progress"]["front"] == 26
    assert payload["runtime"]["backend"] == "pymoo"
    assert payload["runtime"]["algorithm"] == "nsga3"
    assert payload["runtime"]["seed_mode"] == "result"
    assert payload["runtime"]["exchange"] == "bybit"
    assert payload["metrics"]["objectives"]["omega_ratio_usd"] == 78.3137
    assert payload["process"]["rss_bytes"] == 2147483648
    assert payload["system"]["cpu_percent"] == 73.2
    assert payload["queue"] == {"queued": 1, "running": 1, "error": 1, "complete": 0}
    assert payload["log"]["size_bytes"] > 0
