"""Export a private, isolated PB8 optimizer benchmark bundle from PBGui."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from secure_files import atomic_write_private_text, ensure_private_directory, secure_private_file
SERVICE = "VastGpuBenchmark"
PB8_REVISION = "ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8"


def safe_component(value: str) -> str:
    """Reject traversal and control characters in exported identifiers."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_:.+-]+", value) or value in (".", ".."):
        raise ValueError("Invalid path component")
    return value


def contained_file(root: Path, candidate: Path) -> Path:
    """Resolve a source file without permitting symlink escapes."""
    resolved = candidate.resolve(strict=True)
    resolved.relative_to(root.resolve(strict=True))
    if not resolved.is_file():
        raise ValueError("Expected a regular source file")
    return resolved


def benchmark_configs(source: dict, iterations: int, seed: int, *, gpu_adg_objective: bool = False) -> dict:
    """Build comparable workload copies without changing the source config."""
    if not 256 <= iterations <= 10000:
        raise ValueError("Iterations must be between 256 and 10000")
    base = copy.deepcopy(source)
    if gpu_adg_objective:
        for objective in base["optimize"]["scoring"]:
            if objective["metric"] == "gain_strategy_eq":
                objective["metric"] = "adg_strategy_eq"
    base.pop("pbgui", None)
    base["live"]["user"] = "benchmark"
    base["backtest"]["ohlcv_source_dir"] = "/work/input/ohlcv"
    base["backtest"]["hlcvs_data_dir"] = None
    base["backtest"]["base_dir"] = "backtests"
    base["optimize"].update(iters=iterations, seed=seed)
    cases = {}
    for name, backend, workers in (("cpu4", "pymoo", 4), ("gpu1", "gpu", 1), ("gpu2", "gpu", 2), ("gpu4", "gpu", 4)):
        config = copy.deepcopy(base)
        config["optimize"].update(backend=backend, n_cpus=workers)
        if backend == "gpu":
            config["optimize"]["gpu"].update(exact_workers=workers, auto_lean_parallelism=False)
        cases[name] = config
    return cases


def select_shards(config: dict, market_root: Path, mapping_root: Path) -> list[tuple[Path, Path]]:
    """Select mapped USDT perpetual data, including BTC reference history.

    The first prototype exports all available daily history for selected symbols
    to retain warmup data. It rejects overrides needing a richer data planner.
    """
    bt = config["backtest"]
    if config.get("coin_overrides") or bt.get("coin_sources") or bt.get("market_settings_sources"):
        raise ValueError("This prototype does not export coin/source overrides")
    for scenario in bt.get("scenarios", []):
        if set(scenario) - {"label", "start_date", "end_date"}:
            raise ValueError("This prototype supports date-only suite scenarios")
    approved = config["live"]["approved_coins"]
    if not isinstance(approved, dict) or any(not isinstance(approved.get(side), list) for side in ("long", "short")):
        raise ValueError("Explicit approved coin lists are required")
    coins = set(approved["long"] + approved["short"]) | {"BTC"}
    selected = []
    for exchange in bt["exchanges"]:
        if exchange not in ("binance", "bybit"):
            raise ValueError("This prototype supports Binance and Bybit only")
        mapping = contained_file(mapping_root, mapping_root / exchange / "mapping.json")
        try:
            rows = json.loads(mapping.read_text())
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid mapping for {exchange}") from exc
        if not isinstance(rows, list):
            raise ValueError("Expected a market mapping list")
        directory = "binanceusdm" if exchange == "binance" else exchange
        for coin in sorted(coins):
            safe_component(coin)
            matches = [row for row in rows if row.get("coin") == coin and row.get("quote") == "USDT" and row.get("swap") is True and row.get("linear") is True]
            if len(matches) != 1:
                raise ValueError(f"Missing or ambiguous USDT perpetual mapping: {exchange}/{coin}")
            symbol = safe_component(matches[0]["ccxt_symbol"].replace("/", "_"))
            relative = Path(directory) / "1m" / symbol
            folder = market_root / relative
            shards = sorted(path for path in folder.glob("*") if re.fullmatch(r"\d{4}-\d{2}-\d{2}\.(npz|npy)", path.name))
            if not shards:
                raise ValueError(f"No local OHLCV files for {exchange}/{coin}")
            # PBGui storage uses the CCXT ID; PB8's source loader uses the standard name.
            destination = Path(exchange) / "1m" / symbol
            selected.extend((contained_file(market_root, path), destination / path.name) for path in shards)
    return selected


def vast_benchmark_main() -> None:
    """Prepare a new bundle locally; never connect to Vast or start an optimizer."""
    from pb8_config import load_pb8_config, save_pb8_config, shutdown_pb8_migration_helper

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="gpu_test")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=512)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--gpu-adg-objective", action="store_true", help="Explicitly replace gain_strategy_eq scoring with adg_strategy_eq in every benchmark copy; changes the optimization objective")
    args = parser.parse_args()
    name = safe_component(args.config)
    source = contained_file(ROOT / "data/opt_v8", ROOT / "data/opt_v8" / name / "optimize.json")
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError("Choose a new output directory; existing bundles are never overwritten")
    try:
        config = load_pb8_config(source)
        cases = benchmark_configs(config, args.iterations, args.seed, gpu_adg_objective=args.gpu_adg_objective)
        shards = select_shards(config, ROOT / "data/ohlcv", ROOT / "data/coindata")
        output.mkdir(mode=0o700, parents=True, exist_ok=False)
        ensure_private_directory(output)
        ensure_private_directory(output / "configs")
        config_hashes = {}
        for case, value in cases.items():
            destination = output / "configs" / f"{case}.json"
            save_pb8_config(value, destination)
            secure_private_file(destination)
            config_hashes[case] = hashlib.sha256(destination.read_bytes()).hexdigest()
        entries = []
        for origin, relative in shards:
            target = output / "ohlcv" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(origin, target)
            entries.append({"path": str(relative), "bytes": target.stat().st_size, "sha256": hashlib.sha256(target.read_bytes()).hexdigest()})
        manifest = {"schema_version": 1, "pb8_revision": PB8_REVISION, "source_config_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "iterations": args.iterations, "seed": args.seed, "cases": list(cases), "config_sha256": config_hashes, "files": entries, "data_bytes": sum(entry["bytes"] for entry in entries), "coverage_validated": False}
        if args.gpu_adg_objective and any(item["metric"] == "gain_strategy_eq" for item in config["optimize"]["scoring"]):
            manifest["benchmark_adjustments"] = [{"surface": "optimize.scoring.metric", "from": "gain_strategy_eq", "to": "adg_strategy_eq", "explicit_cli_option": "--gpu-adg-objective"}]
        atomic_write_private_text(output / "manifest.json", json.dumps(manifest, indent=4) + "\n")
        print(f"Prepared {len(cases)} cases and {len(entries)} shards ({manifest['data_bytes']} bytes) in {output}")
    finally:
        shutdown_pb8_migration_helper()


if __name__ == "__main__":
    vast_benchmark_main()
