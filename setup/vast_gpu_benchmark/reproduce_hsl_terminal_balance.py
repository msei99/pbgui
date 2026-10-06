"""Replay isolated synthetic HSL closing losses on the worker's actual GPU.

Run in a disposable worker checkout with its upstream test helpers available.
No market downloads, accounts, live trading, or production data are used.
"""

import importlib.util
import json
from pathlib import Path
import sys

import numpy as np
import torch

SERVICE = "VastHslReproduction"


def main():
    """Record the failing candidate and controls without masking GPU failures."""
    root = Path("/opt/passivbot")
    sys.path.insert(0, str(root / "src"))
    spec = importlib.util.spec_from_file_location(
        "upstream_gpu_tests", root / "tests/optimization/test_gpu_mps.py"
    )
    helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helpers)
    from optimization.gpu.model import ProxyMarket

    assert torch.cuda.is_available(), "A real CUDA device is required"
    config = json.loads(Path(sys.argv[1]).read_text())
    records = []
    for strategy in config["strategies"]:
        for mode in config["hsl_modes"]:
            for taker_fee in config["taker_fees"]:
                count = config["bars"]
                closes = np.full((count, 2), 100.0)
                closes[12:] = config["crash_price"]
                assert np.isfinite(closes).all() and (closes > 0).all()
                runner, row = helpers._multicoin_exposure_fixture(
                    strategy, "long", count=count, closes=closes,
                    markets=[ProxyMarket(.001, .01, .001, 0., 1., .0002,
                                         taker_fee=taker_fee) for _ in range(2)],
                    hsl_panic_market=True, market_orders_allowed=False,
                )
                keys = (helpers.EMA_ANCHOR_MULTICOIN_PARAM_KEYS if strategy == "ema_anchor"
                        else helpers.TRAILING_MARTINGALE_MULTICOIN_PARAM_KEYS)
                for name, value in {"hsl_enabled": 1., "hsl_red_threshold": .01,
                                    "hsl_ema_span_minutes": 1., "hsl_signal_mode": mode,
                                    "hsl_cooldown_minutes_after_red": 60.}.items():
                    row[keys.index(name)] = value
                record = dict(strategy=strategy, mode=mode, taker_fee=taker_fee,
                              valid_candles=True, parameters=dict(zip(keys, row)))
                try:
                    output = runner.run(np.asarray([row], dtype=np.float64))
                    torch.cuda.synchronize()
                    record.update(status="completed", output={
                        key: value.detach().cpu().tolist() for key, value in output.items()
                        if isinstance(value, torch.Tensor) and value.numel() <= 8
                    })
                except ValueError as exc:
                    record.update(status="failed", error=str(exc))
                records.append(record)
                print(json.dumps({key: record[key] for key in
                    ("strategy", "mode", "taker_fee", "status", "valid_candles", "error")
                    if key in record}), flush=True)
    Path(sys.argv[2]).write_text(json.dumps({"device": torch.cuda.get_device_name(),
        "revision": (Path("/opt/pb8-revision").read_text().strip()),
        "config": config, "records": records}, indent=4))


if __name__ == "__main__":
    main()
