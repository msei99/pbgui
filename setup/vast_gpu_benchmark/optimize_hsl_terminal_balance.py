"""Run a small offline GPU optimization using isolated upstream fixture data."""

import asyncio
import json
from pathlib import Path
import sys

SERVICE = "VastHslOptimizerReproduction"


def main():
    """Generate a canonical stress config and execute the public optimizer."""
    source = Path("/opt/passivbot")
    sys.path[:0] = [str(source / "src"), str(source / "tests")]
    import pytest
    from test_hsl_offline_runtime import offline_cli_config
    from config.optimize_bounds import set_flat_optimize_bound
    from config_utils import dump_config
    from optimization.shape import build_optimization_shape
    from optimize import main as optimize_main

    root = Path(sys.argv[1]).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    with pytest.MonkeyPatch.context() as monkeypatch:
        cfg = offline_cli_config(root, monkeypatch, "coin")
        cfg["live"]["strategy_kind"] = "trailing_martingale"
        cfg["backtest"]["taker_fee_override"] = 2.0
        entry = cfg["bot"]["long"]["strategy"]["trailing_martingale"]["entry"]
        entry.update(initial_qty_pct=1.0, initial_ema_dist=0.0,
                     ema_span_0=2.0, ema_span_1=3.0,
                     retracement_base_pct=0.0)
        cfg["optimize"].update(backend="gpu", iters=32, n_cpus=1,
            population_size=4, scoring=[{"metric": "adg_usd", "goal": "max"}],
            limits=[], seed=42, enable_overrides=[],
            compress_results_file=False, write_all_results=True)
        cfg["optimize"]["gpu"].update(population_size=4, batch_size=4,
            exact_workers=1, max_pending_exact=2, validate_per_generation=2,
            drift_probes=1)
        # Synthetic taker fees deterministically exhaust cash on the HSL close.
        # They are fixture values, not a model of the user's exchange fees.
        market_path = root / "caches/binance/markets.json"
        markets = json.loads(market_path.read_text())
        markets["BTC/USDT:USDT"]["taker"] = 2.0
        market_path.write_text(json.dumps(markets, indent=4))
        shape = build_optimization_shape(cfg)
        for key, path in shape.key_paths:
            value = cfg
            for part in path:
                value = value[part]
            set_flat_optimize_bound(cfg["optimize"]["bounds"],
                "trailing_martingale", key, [value, value])
        cfg["optimize"]["bounds"]["long"]["hsl"]["red_threshold"] = [.005, .05]
        config_path = root / "config.json"
        dump_config(cfg, str(config_path))
        monkeypatch.setattr(sys, "argv", ["optimize", str(config_path), "--suite", "n"])
        asyncio.run(optimize_main())


if __name__ == "__main__":
    main()
