"""Frontend contracts for focused Hyperliquid Market Data actions."""

from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "frontend" / "hl_data_actions.html"


def _extract_function(source: str, name: str) -> str:
    """Extract one named JavaScript function from the embedded page."""
    start = source.index(f"function {name}(")
    brace_start = source.index("{", start)
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(brace_start, len(source)):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"Could not extract JavaScript function {name!r}")


def test_build_coin_filters_combine_tradfi_and_local_data_state() -> None:
    """TradFi and no-local-data toggles should compose with the text filter."""
    source = PAGE.read_text(encoding="utf-8")
    function = _extract_function(source, "getBuildVisibleCoins")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var buildCoins = ['BTC', 'ETH', 'xyz:AAPL', 'XYZ-MSFT'];
        var buildCoinsWithDownloadedHistory = new Set(['BTC', 'xyz:AAPL']);
        var buildFilter = '';
        var buildTradfiOnly = false;
        var buildNoLocalData = false;
        {function}
        assert.deepEqual(getBuildVisibleCoins(), buildCoins);
        buildTradfiOnly = true;
        assert.deepEqual(getBuildVisibleCoins(), ['xyz:AAPL', 'XYZ-MSFT']);
        buildNoLocalData = true;
        assert.deepEqual(getBuildVisibleCoins(), ['XYZ-MSFT']);
        buildTradfiOnly = false;
        assert.deepEqual(getBuildVisibleCoins(), ['ETH', 'XYZ-MSFT']);
        buildFilter = 'msft';
        assert.deepEqual(getBuildVisibleCoins(), ['XYZ-MSFT']);
        """
    )

    subprocess.run(["node", "-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_build_coin_filter_controls_and_payload_contract_are_present() -> None:
    """The focused panel should expose both toggles and consume backend availability metadata."""
    source = PAGE.read_text(encoding="utf-8")

    assert 'data-action="build-tradfi-only"' in source
    assert 'data-action="build-no-local-data"' in source
    assert "buildCoinsWithDownloadedHistory = new Set(bD.coins_with_downloaded_history||[])" in source
