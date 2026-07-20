"""Frontend regression tests for backtest fee input formatting."""

from pathlib import Path
import re
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]


def test_fee_inputs_hide_binary_float_artifacts_without_coarse_rounding() -> None:
    """Fee inputs must display stable decimals while retaining useful precision."""
    source = (ROOT / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    match = re.search(r"function formatFeeInputValue\(value\) \{.*?\n\}", source, re.DOTALL)
    assert match
    assert "mfEnabled ? formatFeeInputValue(bt.maker_fee_override) : 0" in source
    assert "tfEnabled ? formatFeeInputValue(bt.taker_fee_override) : 0" in source
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {match.group(0)}
        assert.equal(formatFeeInputValue(0.00019999999999999047), '0.0002');
        assert.equal(formatFeeInputValue(0.0005500000000000314), '0.00055');
        assert.equal(formatFeeInputValue(0.000123456789), '0.000123456789');
        assert.equal(formatFeeInputValue(0), '0');
        """
    )

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)

    assert completed.returncode == 0, completed.stderr or completed.stdout
