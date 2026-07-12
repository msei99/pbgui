"""
Test: PB7 vs PBGui Mapping Coin Name Comparison.

Refreshes PB7's market caches via load_markets(), then compares
PB7's coin_to_symbol_map with our mapping-based coin names.

This validates that PBCoinData.compute_coin_name() produces the same
canonical coin names that PB7 uses internally.

Usage:
    python -m pytest tests/market_data/test_pb7_coin_comparison.py --run-external-pb7 -v -s
"""
import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# PB7 path setup - add pb7/src to sys.path so we can import load_markets
# ---------------------------------------------------------------------------
# PBGui imports - insert project root at front to bypass tests/PBCoinData.py mock
PBGUI_ROOT = Path(__file__).resolve().parents[2]
PB7_ROOT = Path(os.environ.get("PB7_ROOT", PBGUI_ROOT.parent / "pb7")).expanduser().resolve()
PB7_SRC = PB7_ROOT / "src"
sys.path.insert(0, str(PBGUI_ROOT))

pytestmark = pytest.mark.external_pb7

# Import from real PBCoinData (not the tests/ mock)
# Force fresh import from project root
_saved_modules = {k: v for k, v in sys.modules.items() if "PBCoinData" in k}
for k in _saved_modules:
    del sys.modules[k]
import PBCoinData as _real_pbcoindata
compute_coin_name = _real_pbcoindata.compute_coin_name
remove_powers_of_ten = _real_pbcoindata.remove_powers_of_ten
# Restore any previously loaded mocks
sys.modules.update(_saved_modules)


# ---------------------------------------------------------------------------
# Exchanges to test (PB7 uses these names internally)
# ---------------------------------------------------------------------------
EXCHANGES = ["binance", "bybit", "bitget", "gateio", "okx", "hyperliquid"]

# Known expected differences – coins that intentionally differ between PB7 and
# our mapping.  Key = exchange, value = set of coin names to exclude from diff.
KNOWN_ONLY_IN_PB7 = {
    # PB7 may have coins from non-USDT quote currencies (USDC on Hyperliquid)
    # or delisted markets still in cache
}
KNOWN_ONLY_IN_PBGUI = {
    # PBGui may include stablecoins or very new listings not yet in PB7 cache
}


def _refresh_pb7_caches():
    """Refresh PB7 market caches by calling load_markets for each exchange."""
    if not PB7_SRC.is_dir():
        pytest.skip(f"PB7 source directory not found: {PB7_SRC}")
    # Must chdir to pb7 root because load_markets uses relative cache paths
    original_cwd = os.getcwd()
    os.chdir(str(PB7_ROOT))
    try:
        sys.path.insert(0, str(PB7_SRC))
        from utils import load_markets

        async def _refresh():
            for ex in EXCHANGES:
                print(f"  Refreshing PB7 cache: {ex} ...")
                try:
                    await load_markets(ex, max_age_ms=0)
                except Exception as e:
                    print(f"  WARNING: Failed to refresh {ex}: {e}")

        asyncio.run(_refresh())
    finally:
        os.chdir(original_cwd)
        # Clean up PB7 module imports to avoid contamination
        mods_to_remove = [m for m in sys.modules if m.startswith("utils") or m.startswith("passivbot")]
        for m in mods_to_remove:
            del sys.modules[m]
        if str(PB7_SRC) in sys.path:
            sys.path.remove(str(PB7_SRC))


def _load_pb7_coins(exchange: str) -> set:
    """Load coin names from PB7's per-exchange coin_to_symbol_map.json.

    PB7 stores multiple key variants for the same coin (e.g. both SHIB and
    1000SHIB point to the same CCXT symbol). We normalize by looking at the
    *values* (lists of CCXT symbols) and resolving the canonical coin via
    PB7's symbol_to_coin_map.
    """
    cache_path = PB7_ROOT / "caches" / exchange / "coin_to_symbol_map.json"
    if not cache_path.exists():
        pytest.skip(f"PB7 cache not found: {cache_path}")

    # Load the global symbol_to_coin_map for canonical resolution
    s2c_path = PB7_ROOT / "caches" / "symbol_to_coin_map.json"
    with s2c_path.open() as f:
        s2c = json.load(f)

    with cache_path.open() as f:
        coin_map = json.load(f)

    # Collect canonical coin names by resolving through symbol_to_coin
    coins = set()
    for key, symbols in coin_map.items():
        # Skip namespaced non-crypto markets; this comparison targets crypto coin canonicalization.
        if ":" in key or key.startswith("XYZ-"):
            continue
        # Skip numeric keys (Hyperliquid index numbers)
        if key.isdigit():
            continue
        # Resolve canonical coin name from first symbol
        if isinstance(symbols, list) and symbols:
            canonical = s2c.get(symbols[0], key)
        else:
            canonical = key
        # Skip namespaced non-crypto markets in canonical form too.
        if ":" in canonical:
            continue
        coins.add(canonical.upper())
    return coins


def _load_pbgui_coins(exchange: str) -> set:
    """Load coin names from PBGui's mapping.json, computing coin names."""
    mapping_path = PBGUI_ROOT / "data" / "coindata" / exchange / "mapping.json"
    if not mapping_path.exists():
        pytest.skip(f"PBGui mapping not found: {mapping_path}")

    with mapping_path.open() as f:
        mapping = json.load(f)

    coins = set()
    for record in mapping:
        # Skip HIP-3 stock perps; this comparison targets crypto coin canonicalization.
        if bool(record.get("is_hip3", False)):
            continue

        quote = record.get("quote", "USDT")
        # Skip non-standard quote currencies (e.g. OKX USD-margined contracts)
        if quote not in ("USDT", "USDC"):
            continue
        # Use pre-computed coin field if available, otherwise compute
        coin = record.get("coin")
        if not coin:
            symbol = record.get("symbol", "")
            coin = compute_coin_name(symbol, quote)

        if coin:
            coin_u = coin.upper()
            if coin_u.startswith("XYZ-") or coin_u.startswith("XYZ:"):
                continue
            coins.add(coin_u)
    return coins


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def refresh_caches():
    """Refresh PB7 caches once before all tests run."""
    print("\n--- Refreshing PB7 market caches ---")
    _refresh_pb7_caches()
    print("--- PB7 caches refreshed ---\n")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestPB7CoinComparison:
    """Compare PB7 coin names with PBGui mapping-derived coin names."""

    @pytest.mark.parametrize("exchange", EXCHANGES)
    def test_coin_name_overlap(self, exchange: str):
        """
        Compare the set of coins from PB7's cache with PBGui's mapping.
        Reports differences but does not fail on small discrepancies due to
        timing (new listings, delistings).
        """
        pb7_coins = _load_pb7_coins(exchange)
        pbgui_coins = _load_pbgui_coins(exchange)

        only_pb7 = pb7_coins - pbgui_coins
        only_pbgui = pbgui_coins - pb7_coins
        common = pb7_coins & pbgui_coins

        # Remove known expected differences
        known_pb7 = KNOWN_ONLY_IN_PB7.get(exchange, set())
        known_pbgui = KNOWN_ONLY_IN_PBGUI.get(exchange, set())
        unexpected_pb7 = only_pb7 - known_pb7
        unexpected_pbgui = only_pbgui - known_pbgui

        print(f"\n{'='*60}")
        print(f"  {exchange.upper()}")
        print(f"{'='*60}")
        print(f"  PB7 coins:   {len(pb7_coins)}")
        print(f"  PBGui coins: {len(pbgui_coins)}")
        print(f"  Common:      {len(common)}")
        print(f"  Only PB7:    {len(only_pb7)}")
        print(f"  Only PBGui:  {len(only_pbgui)}")

        if only_pb7:
            print(f"\n  Only in PB7 ({len(only_pb7)}):")
            for c in sorted(only_pb7):
                marker = " [KNOWN]" if c in known_pb7 else ""
                print(f"    - {c}{marker}")

        if only_pbgui:
            print(f"\n  Only in PBGui ({len(only_pbgui)}):")
            for c in sorted(only_pbgui):
                marker = " [KNOWN]" if c in known_pbgui else ""
                print(f"    - {c}{marker}")

        match_pct = len(common) / max(len(pb7_coins), len(pbgui_coins), 1) * 100
        print(f"\n  Match: {match_pct:.1f}%")

        # Soft assertion: at least 95% overlap
        assert match_pct >= 95.0, (
            f"{exchange}: Only {match_pct:.1f}% overlap. "
            f"Unexpected only in PB7: {sorted(unexpected_pb7)[:20]}, "
            f"Unexpected only in PBGui: {sorted(unexpected_pbgui)[:20]}"
        )

    @pytest.mark.parametrize("exchange", EXCHANGES)
    def test_compute_coin_matches_pb7(self, exchange: str):
        """
        For each symbol in PBGui mapping, verify that compute_coin_name
        produces the same result as PB7's symbol_to_coin mapping.
        """
        # Load PB7's global symbol_to_coin_map
        s2c_path = PB7_ROOT / "caches" / "symbol_to_coin_map.json"
        if not s2c_path.exists():
            pytest.skip("PB7 symbol_to_coin_map.json not found")

        with s2c_path.open() as f:
            pb7_s2c = json.load(f)

        # Load PBGui mapping
        mapping_path = PBGUI_ROOT / "data" / "coindata" / exchange / "mapping.json"
        if not mapping_path.exists():
            pytest.skip(f"PBGui mapping not found: {mapping_path}")

        with mapping_path.open() as f:
            mapping = json.load(f)

        mismatches = []
        checked = 0
        for record in mapping:
            symbol = record.get("symbol", "")
            ccxt_symbol = record.get("ccxt_symbol", "")
            quote = record.get("quote", "USDT")

            # Skip HIP-3 stock-perps; PB7 canonical form uses xyz: prefix and is
            # intentionally not comparable 1:1 to PBGui's XYZ-coin field here.
            if bool(record.get("is_hip3", False)):
                continue

            # Skip non-standard quote currencies
            if quote not in ("USDT", "USDC"):
                continue

            our_coin = record.get("coin") or compute_coin_name(symbol, quote)
            if str(our_coin or "").upper().startswith(("XYZ-", "XYZ:")):
                continue

            # Look up PB7's coin name via multiple keys
            pb7_coin = None
            for key in [ccxt_symbol, symbol]:
                if key in pb7_s2c:
                    pb7_coin = pb7_s2c[key]
                    break

            if pb7_coin is None:
                continue  # Symbol not in PB7's map (maybe new listing)

            checked += 1
            if our_coin.upper() != pb7_coin.upper():
                mismatches.append({
                    "symbol": symbol,
                    "ccxt_symbol": ccxt_symbol,
                    "our_coin": our_coin,
                    "pb7_coin": pb7_coin,
                })

        print(f"\n  {exchange}: Checked {checked} symbols, {len(mismatches)} mismatches")
        if mismatches:
            print(f"  Mismatches:")
            for m in mismatches[:20]:
                print(f"    {m['symbol']}: ours={m['our_coin']}, pb7={m['pb7_coin']}")

        # Allow a small number of known mismatches (e.g. DegenReborn vs DEGEN)
        assert len(mismatches) <= 5, (
            f"{exchange}: {len(mismatches)} coin name mismatches (max 5 allowed): "
            f"{mismatches[:10]}"
        )
