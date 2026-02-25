#!/usr/bin/env python3
"""Reprocess existing TradFi stock-perp .npz files to apply split-adjustment.

Usage:
    python reprocess_tradfi_splits.py                # all TradFi coins
    python reprocess_tradfi_splits.py --coin AAPL    # single coin
    python reprocess_tradfi_splits.py --dry-run      # preview without writing

This script:
1. Identifies all TradFi (stock-perp) coins by checking tradfi_symbol_map.json.
2. For each coin, fetches split factors from Tiingo Daily API (cached to disk).
3. Reads every .npz day-file, applies backward split-adjustment, rewrites the file.

Only IEX (equity) coins are processed — FX coins have no splits.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np

# Ensure project root is on sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from hyperliquid_best_1m import (
    _best_day_path,
    _day_tag,
    _list_best_days,
    _load_tradfi_profiles_from_ini,
    _load_tradfi_symbol_map_cached,
    _read_day_npz,
    _tradfi_ticker_from_hyperliquid_coin,
    _write_day_npz,
    compute_cumulative_split_adjustment,
    fetch_tiingo_split_factors,
    resolve_tradfi_symbol,
)
from hyperliquid_api import normalize_hyperliquid_coin
from market_data import normalize_market_data_coin_dir, get_exchange_raw_root_dir


def _get_iex_tradfi_coins() -> list[tuple[str, str]]:
    """Return list of (hl_coin, tiingo_ticker) for IEX stock-perp coins."""
    records = _load_tradfi_symbol_map_cached()
    results: list[tuple[str, str]] = []
    for rec in records:
        xyz_coin = str(rec.get("xyz_coin") or "").strip().upper()
        if not xyz_coin:
            continue
        tiingo_ticker = str(rec.get("tiingo_ticker") or "").strip().upper()
        if not tiingo_ticker:
            continue
        # Only IEX equity coins, not FX
        if rec.get("tiingo_fx_ticker"):
            continue
        results.append((xyz_coin, tiingo_ticker))
    return results


def reprocess_coin(
    coin: str,
    tiingo_ticker: str,
    api_key: str,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict[str, int]:
    """Reprocess all .npz files for a single coin.

    Returns stats dict with keys: days_total, days_adjusted, minutes_adjusted.
    """
    coin_u = normalize_hyperliquid_coin(coin)
    stats = {"days_total": 0, "days_adjusted": 0, "minutes_adjusted": 0}

    # Fetch split factors
    splits = fetch_tiingo_split_factors(tiingo_ticker, api_key, force_refresh=True)
    if not splits:
        if verbose:
            print(f"  {coin_u}: no splits found for {tiingo_ticker}, skipping")
        return stats

    if verbose:
        print(f"  {coin_u}: {len(splits)} split(s) for {tiingo_ticker}:")
        for sd, sf in splits:
            print(f"    {sd}  factor={sf}")

    # List all .npz day files — TradFi coins use the XYZ- prefix for directories
    base = get_exchange_raw_root_dir("hyperliquid") / "1m"
    # Ensure XYZ- prefix so normalize_market_data_coin_dir maps to XYZ-AAPL_USDC:USDC
    coin_for_dir = coin_u if coin_u.upper().startswith("XYZ") else f"XYZ-{coin_u}"
    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin_for_dir)
    if not coin_dir:
        return stats
    npz_dir = base / coin_dir
    if not npz_dir.exists():
        if verbose:
            print(f"  {coin_u}: no data directory {npz_dir}")
        return stats

    day_files = sorted(npz_dir.glob("*.npz"))
    stats["days_total"] = len(day_files)

    for npz_path in day_files:
        try:
            d = datetime.strptime(npz_path.stem, "%Y-%m-%d").date()
        except Exception:
            continue

        factor = compute_cumulative_split_adjustment(splits, d)
        if factor == 1.0:
            continue  # No adjustment needed for this day

        # Read existing data
        day_s = d.strftime("%Y%m%d")
        existing = _read_day_npz(npz_path, day=day_s)
        if not existing:
            continue

        # Apply adjustment
        adjusted_count = 0
        for idx, bar in existing.items():
            bar["o"] = float(bar["o"]) / factor
            bar["h"] = float(bar["h"]) / factor
            bar["l"] = float(bar["l"]) / factor
            bar["c"] = float(bar["c"]) / factor
            bar["v"] = float(bar["v"]) * factor
            adjusted_count += 1

        if adjusted_count > 0:
            stats["days_adjusted"] += 1
            stats["minutes_adjusted"] += adjusted_count
            if dry_run:
                if verbose:
                    sample = next(iter(existing.values()))
                    print(f"    [DRY-RUN] {d}: would adjust {adjusted_count} minutes (factor={factor:.4f}, sample close={sample['c']:.4f})")
            else:
                _write_day_npz(npz_path, existing)
                if verbose:
                    print(f"    {d}: adjusted {adjusted_count} minutes (factor={factor:.4f})")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Reprocess TradFi .npz files for stock split-adjustment")
    parser.add_argument("--coin", type=str, default=None, help="Single HL coin to reprocess (e.g. AAPL)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    # Get Tiingo API key
    profiles = _load_tradfi_profiles_from_ini()
    api_key = str((profiles.get("tiingo") or {}).get("api_key") or "").strip()
    if not api_key:
        print("ERROR: No Tiingo API key found in pbgui.ini [tradfi_profiles]")
        sys.exit(1)

    # Determine which coins to process
    if args.coin:
        coin_u = args.coin.strip().upper()
        xyz_name = _tradfi_ticker_from_hyperliquid_coin(coin_u)
        tiingo_ticker, _, _, _ = resolve_tradfi_symbol(xyz_name)
        if not tiingo_ticker:
            print(f"ERROR: {coin_u} is not a recognized IEX stock-perp in tradfi_symbol_map.json")
            sys.exit(1)
        coins = [(coin_u, tiingo_ticker.upper())]
    else:
        coins = _get_iex_tradfi_coins()

    if not coins:
        print("No TradFi IEX coins found to reprocess.")
        return

    mode = "[DRY-RUN] " if args.dry_run else ""
    print(f"{mode}Reprocessing {len(coins)} TradFi coin(s) for split-adjustment...")

    total_stats = {"days_total": 0, "days_adjusted": 0, "minutes_adjusted": 0}
    for coin, ticker in coins:
        if args.verbose:
            print(f"\nProcessing {coin} (ticker={ticker})...")
        s = reprocess_coin(coin, ticker, api_key, dry_run=args.dry_run, verbose=args.verbose)
        for k in total_stats:
            total_stats[k] += s[k]

    print(f"\n{mode}Done. {total_stats['days_total']} day-files scanned, "
          f"{total_stats['days_adjusted']} adjusted, "
          f"{total_stats['minutes_adjusted']} minute-bars modified.")


if __name__ == "__main__":
    main()
