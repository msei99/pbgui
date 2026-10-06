"""Verify paired real-GPU replay and terminal-boundary result artifacts."""

import json
import math
from pathlib import Path
import sys

SERVICE = "VastHslVerification"


def equal_outputs(left, right):
    """Compare deterministic controls, allowing matching diagnostic NaNs."""
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(equal_outputs(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(equal_outputs(a, b) for a, b in zip(left, right))
    if isinstance(left, float) and math.isnan(left):
        return isinstance(right, float) and math.isnan(right)
    return left == right


def main():
    """Require reproduction, successful liquidation, and unchanged controls."""
    root = Path(sys.argv[1])
    before, after, old_probe, new_probe = [json.loads((root / name).read_text()) for name in
        ("baseline-replay.json", "fixed-replay.json", "baseline-probe.json", "fixed-probe.json")]
    assert before["config"] == after["config"]
    assert len(before["records"]) == len(after["records"]) == 18
    failures = controls = 0
    for old, new in zip(before["records"], after["records"]):
        for key in ("strategy", "mode", "taker_fee", "parameters", "valid_candles"):
            assert old[key] == new[key], key
        assert new["status"] == "completed", new
        if old["status"] == "failed":
            assert "MPS proxy unavailable held-position valuation" in old["error"]
            assert new["output"]["alive"] == [False], new
            assert new["output"]["balance"][0] <= 0., new
            failures += 1
        else:
            assert equal_outputs(old["output"], new["output"]), (old, new)
            controls += 1
    assert (failures, controls) == (12, 6)
    assert len(old_probe["records"]) == len(new_probe["records"]) == 48
    for old, new in zip(old_probe["records"], new_probe["records"]):
        for key in ("kernel", "mode", "balance"):
            assert old[key] == new[key]
        assert new["finished"] == 1 and new["triggers"] == 0
        if old["balance"] in (0., -100.):
            assert old["hsl_valid"] == 0 and new["hsl_valid"] == 1
            assert new["last_observed"] == 1
        else:
            assert old == new
    print("PASS: 12 reproduced failures fixed, 6 controls unchanged, 48 boundary probes verified")


if __name__ == "__main__":
    main()
