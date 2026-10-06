"""Run the upstream terminal-close boundary probe on CUDA without market data."""

import importlib.util
import json
from pathlib import Path
import sys

import torch

SERVICE = "VastHslBoundaryProbe"


def main():
    """Check depleted, positive, and non-finite balances using real GPU code."""
    root = Path("/opt/passivbot")
    sys.path.insert(0, str(root / "src"))
    from optimization.gpu.runtime import compile_shader, synchronize

    assert torch.cuda.is_available(), "A real CUDA device is required"
    spec = importlib.util.spec_from_file_location(
        "upstream_hsl_probe", root / "tests/optimization/test_gpu_hsl_episode_boundaries.py"
    )
    helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helpers)
    probe = helpers._PROBE.replace(
        "if (finished) {",
        "output[7] = owner.hsl_valid; output[8] = owner.hsl.last_observed; return;\n"
        "    if (finished) {",
    )
    records = []
    for name in helpers._KERNELS:
        library = compile_shader(helpers._source(name) + probe)
        for mode in (0, 1, 2):
            for balance in (1000., 0., -100., float("nan")):
                params = torch.tensor([1, .1, 1, 60, 0, mode, 1],
                                      dtype=torch.float32, device="cuda")
                trees = torch.empty((36, 32), dtype=torch.uint8, device="cuda")
                rows = torch.empty(256, dtype=torch.int32, device="cuda")
                output = torch.zeros(12, dtype=torch.float32, device="cuda")
                library.episode_boundary_probe(params, trees, rows, output,
                    0, 0, 1000. - balance, 0, threads=1)
                synchronize()
                values = output.cpu().tolist()
                record = dict(kernel=name, mode=mode,
                    balance=balance if balance == balance else "nan",
                    finished=values[1], triggers=values[4],
                    hsl_valid=values[7], last_observed=values[8])
                records.append(record)
                print(json.dumps(record), flush=True)
    Path(sys.argv[1]).write_text(json.dumps({"device": torch.cuda.get_device_name(),
        "records": records}, indent=4))


if __name__ == "__main__":
    main()
