"""Offline coverage of benchmark isolation, input boundaries, and process cleanup."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest


def load_module(filename):
    """Load standalone benchmark helpers without starting PB8."""
    path = Path(__file__).resolve().parents[1] / "setup/vast_gpu_benchmark" / filename
    spec = importlib.util.spec_from_file_location(f"vast_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


prepare = load_module("prepare.py")
runner = load_module("run.py")


def test_adg_objective_is_explicit_and_applies_equally_to_all_cases():
    """The approved objective adjustment stays opt-in and preserves user data."""
    source = {"live": {}, "backtest": {}, "optimize": {"gpu": {}, "scoring": [{"goal": "max", "metric": "gain_strategy_eq"}, {"goal": "min", "metric": "drawdown_worst_strategy_eq"}]}}
    before = copy.deepcopy(source)
    default = prepare.benchmark_configs(source, 512, 7)
    adjusted = prepare.benchmark_configs(source, 512, 7, gpu_adg_objective=True)
    assert source == before
    assert all(case["optimize"]["scoring"] == before["optimize"]["scoring"] for case in default.values())
    assert all(case["optimize"]["scoring"] == [{"goal": "max", "metric": "adg_strategy_eq"}, {"goal": "min", "metric": "drawdown_worst_strategy_eq"}] for case in adjusted.values())


def test_binance_export_uses_native_source_directory(tmp_path):
    """PBGui CCXT storage names translate to PB8's standard exchange paths."""
    mapping = tmp_path / "mapping/binance"
    mapping.mkdir(parents=True)
    (mapping / "mapping.json").write_text(json.dumps([{"coin": "BTC", "quote": "USDT", "swap": True, "linear": True, "ccxt_symbol": "BTC/USDT:USDT"}]))
    source = tmp_path / "ohlcv/binanceusdm/1m/BTC_USDT:USDT/2020-01-01.npz"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"fixture")
    config = {"backtest": {"exchanges": ["binance"]}, "live": {"approved_coins": {"long": [], "short": []}}}
    assert prepare.select_shards(config, tmp_path / "ohlcv", tmp_path / "mapping") == [(source, Path("binance/1m/BTC_USDT:USDT/2020-01-01.npz"))]


def test_config_copies_preserve_original_and_workload():
    """Worker counts and budgets change only in independent copies."""
    original = {"live": {"user": "local"}, "backtest": {"scenarios": [{"start_date": "2020-01-01"}]}, "optimize": {"gpu": {}, "iters": 200000}, "pbgui": {"private": "local"}}
    before = copy.deepcopy(original)
    cases = prepare.benchmark_configs(original, 512, 7)
    assert original == before
    assert cases["gpu1"]["optimize"]["gpu"]["exact_workers"] == 1
    assert cases["gpu4"]["optimize"]["gpu"]["exact_workers"] == 4
    assert cases["cpu4"]["optimize"]["backend"] == "pymoo"
    assert all(config["backtest"]["scenarios"] == original["backtest"]["scenarios"] for config in cases.values())
    assert all("pbgui" not in config for config in cases.values())
    cases["gpu1"]["backtest"]["scenarios"].clear()
    assert cases["gpu4"]["backtest"]["scenarios"]


@pytest.mark.parametrize("value", ["..", "../gpu_test", "a/b", "a\\b", "a\x00b", "a\nb"])
def test_reject_unsafe_identifiers(value):
    """Input names must not become paths or shell syntax."""
    with pytest.raises(ValueError):
        prepare.safe_component(value)


def test_mapping_and_shard_selection(tmp_path):
    """Resolve only mapped linear USDT perpetuals and include BTC history."""
    mapping = tmp_path / "mapping/binance"
    mapping.mkdir(parents=True)
    rows = [{"coin": coin, "quote": "USDT", "swap": True, "linear": True, "ccxt_symbol": f"{coin}/USDT:USDT"} for coin in ("ETH", "BTC")]
    (mapping / "mapping.json").write_text(json.dumps(rows))
    for coin in ("ETH", "BTC", "SOL"):
        folder = tmp_path / f"ohlcv/binanceusdm/1m/{coin}_USDT:USDT"
        folder.mkdir(parents=True)
        (folder / "2020-01-01.npz").write_bytes(b"fixture")
        (folder / "unrelated.json").write_text("{}")
    config = {"backtest": {"exchanges": ["binance"]}, "live": {"approved_coins": {"long": ["ETH"], "short": []}}}
    result = prepare.select_shards(config, tmp_path / "ohlcv", tmp_path / "mapping")
    assert len(result) == 2
    assert {relative.parent.name for _, relative in result} == {"ETH_USDT:USDT", "BTC_USDT:USDT"}


def test_source_symlink_escape(tmp_path):
    """A source symlink cannot export files outside the approved root."""
    root = tmp_path / "root"
    root.mkdir()
    external = tmp_path / "external"
    external.write_text("private")
    (root / "escape").symlink_to(external)
    with pytest.raises(ValueError):
        prepare.contained_file(root, root / "escape")


def test_timeout_preserves_partial_profile(tmp_path):
    """A timed-out owned child is reaped and its emitted evidence is retained."""
    command = [sys.executable, "-c", "import time; print('[gpu-profile] {\"event\":\"generation\"}', flush=True); time.sleep(60)"]
    result = runner.run_case(command, tmp_path / "case", 0.2)
    assert result["timed_out"]
    assert result["exit_code"] != 0
    assert result["gpu_profiles"] == [{"event": "generation"}]


def test_failed_child_not_reported_as_success(tmp_path):
    """Native errors remain visible even when profile output is malformed."""
    result = runner.run_case([sys.executable, "-c", "print('[gpu-profile] {'); raise SystemExit(3)"], tmp_path / "case", 10)
    assert result["exit_code"] == 3
    assert result["gpu_profiles"] == []
    assert not result["timed_out"]


def test_bundle_verification_detects_changed_inputs(tmp_path):
    """Transferred inputs must match the locally prepared checksums."""
    (tmp_path / "configs").mkdir()
    checksums = {}
    for case in runner.CASES:
        path = tmp_path / "configs" / f"{case}.json"
        path.write_bytes(b"{}")
        checksums[case] = hashlib.sha256(b"{}").hexdigest()
    (tmp_path / "manifest.json").write_text(json.dumps({"files": [], "config_sha256": checksums}))
    assert runner.verify_bundle(tmp_path)["config_sha256"] == checksums
    (tmp_path / "configs/cpu4.json").write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksum mismatch"):
        runner.verify_bundle(tmp_path)


@pytest.mark.parametrize("path", ["../../outside", "/etc/passwd", "../configs/cpu4.json", "bad\\path"])
def test_bundle_manifest_rejects_escape_paths(tmp_path, path):
    """Remote manifest paths cannot redirect reads outside the data bundle."""
    manifest = {"files": [{"path": path, "sha256": "unused"}], "config_sha256": {case: "unused" for case in runner.CASES}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        runner.verify_bundle(tmp_path)
