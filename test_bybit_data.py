"""
test_bybit_data.py — Quick exploration of public.bybit.com/trading/ data format

Tests 3 symbols: BTC, ETH, DOGE
For each symbol:
  1. Lists available files from the public directory index
  2. Downloads the most recent complete day
  3. Aggregates raw trades to 1m OHLCV
  4. Reports candle count, first/last candle, gap analysis, file size

Run:
    /home/mani/software/venv_pbgui/bin/python test_bybit_data.py
"""

from __future__ import annotations

import gzip
import io
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://public.bybit.com/trading/"
SYMBOLS = {
    "BTC":  "BTCUSDT",
    "ETH":  "ETHUSDT",
    "DOGE": "DOGEUSDT",
}
TIMEOUT_S = 60
DAYS_TO_TEST = 3   # how many recent days to check per symbol

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _list_directory(symbol: str) -> list[str]:
    """Scrape the HTML directory listing and return all .csv.gz filenames."""
    url = f"{BASE_URL}{symbol}/"
    print(f"  GET {url}")
    resp = requests.get(url, timeout=TIMEOUT_S)
    resp.raise_for_status()
    # Extract all XXXX-XX-XX.csv.gz entries from the HTML
    import re
    pattern = rf'{re.escape(symbol)}(\d{{4}}-\d{{2}}-\d{{2}})\.csv\.gz'
    dates_found = re.findall(pattern, resp.text)
    return sorted(dates_found)


def _download_day_csv(symbol: str, day: str) -> pd.DataFrame | None:
    """Download and parse one daily trade file → raw DataFrame."""
    url = f"{BASE_URL}{symbol}/{symbol}{day}.csv.gz"
    t0 = time.time()
    resp = requests.get(url, timeout=TIMEOUT_S)
    elapsed = time.time() - t0
    size_kb = len(resp.content) / 1024
    print(f"  Downloaded {symbol}{day}.csv.gz  {size_kb:.0f} KB  in {elapsed:.1f}s")
    if resp.status_code != 200:
        print(f"  ERROR: HTTP {resp.status_code}")
        return None
    with gzip.open(io.BytesIO(resp.content)) as f:
        df = pd.read_csv(f)
    return df


def _trades_to_1m_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate raw trade DataFrame to 1m OHLCV.

    Bybit CSV columns vary by era, but key columns are:
      - 'timestamp'  (Unix seconds, float)
      - 'price'      (float)
      - 'size'       (float  — base amount)
      - 'side'       (Buy/Sell)
    """
    # Normalise column names to lowercase
    df.columns = [c.lower().strip() for c in df.columns]
    print(f"  Columns: {list(df.columns)}")
    print(f"  Rows:    {len(df):,}")

    if "timestamp" not in df.columns:
        print("  ERROR: 'timestamp' column missing!")
        return pd.DataFrame()

    # Convert timestamp to ms
    ts_raw = df["timestamp"].values
    # Bybit timestamps are in seconds (float) → convert to ms int
    if ts_raw.max() < 2e12:  # < year 2033 in ms means it's in seconds
        ts_ms = (ts_raw * 1000).astype("int64")
    else:
        ts_ms = ts_raw.astype("int64")

    # Minute bucket
    bucket_ms = (ts_ms // 60_000) * 60_000

    df2 = df.copy()
    df2["_bucket_ms"] = bucket_ms
    df2["price"] = df2["price"].astype(float)
    df2["size"]  = df2["size"].astype(float)

    grp = df2.groupby("_bucket_ms")
    ohlcv = pd.DataFrame({
        "ts_ms": grp["_bucket_ms"].first(),
        "open":  grp["price"].first(),
        "high":  grp["price"].max(),
        "low":   grp["price"].min(),
        "close": grp["price"].last(),
        "vol":   grp["size"].sum(),
    }).reset_index(drop=True)

    return ohlcv


def _analyse_ohlcv(ohlcv: pd.DataFrame, day: str) -> dict[str, Any]:
    """Return a summary dict for a day's OHLCV."""
    if ohlcv.empty:
        return {"candles": 0}
    day_start_ms = int(
        datetime.strptime(day, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp() * 1000
    )
    day_end_ms = day_start_ms + 1440 * 60_000

    # Filter to this day only (trades spanning midnight would leak)
    ohlcv = ohlcv[(ohlcv["ts_ms"] >= day_start_ms) & (ohlcv["ts_ms"] < day_end_ms)].copy()
    count = len(ohlcv)

    # Gap analysis: which 1m slots are missing?
    all_slots = set(range(1440))
    present_slots = set(int((r - day_start_ms) // 60_000) for r in ohlcv["ts_ms"])
    missing_slots = sorted(all_slots - present_slots)

    first_dt = datetime.utcfromtimestamp(ohlcv["ts_ms"].iloc[0] / 1000).strftime("%H:%M") if count else "—"
    last_dt  = datetime.utcfromtimestamp(ohlcv["ts_ms"].iloc[-1] / 1000).strftime("%H:%M") if count else "—"

    return {
        "candles":      count,
        "expected":     1440,
        "missing":      len(missing_slots),
        "coverage_%":   round(count / 14.40, 1),
        "first_candle": first_dt,
        "last_candle":  last_dt,
        "open":         round(ohlcv["open"].iloc[0], 4) if count else None,
        "close":        round(ohlcv["close"].iloc[-1], 4) if count else None,
        "missing_slots_sample": missing_slots[:10],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def test_symbol(coin: str, symbol: str):
    print("\n" + "=" * 60)
    print(f"  SYMBOL: {symbol}  (coin={coin})")
    print("=" * 60)

    # 1. List available dates
    try:
        dates = _list_directory(symbol)
    except Exception as e:
        print(f"  ERROR listing directory: {e}")
        return

    print(f"  Available dates: {len(dates)}  first={dates[0] if dates else 'n/a'}  last={dates[-1] if dates else 'n/a'}")

    # 2. Test most recent N complete days
    # Use the last DAYS_TO_TEST dates from the listing (skip very last which may be partial)
    test_dates = dates[-DAYS_TO_TEST - 1 : -1] if len(dates) > DAYS_TO_TEST else dates[:DAYS_TO_TEST]

    all_results = []
    for day in test_dates:
        print(f"\n  --- Day: {day} ---")
        try:
            df_raw = _download_day_csv(symbol, day)
            if df_raw is None or df_raw.empty:
                print("  Skipping — empty/error")
                continue
            ohlcv = _trades_to_1m_ohlcv(df_raw)
            summary = _analyse_ohlcv(ohlcv, day)
            all_results.append({"day": day, **summary})
            print(f"  Candles:   {summary['candles']}/1440  ({summary['coverage_%']}% coverage)")
            print(f"  Range:     {summary.get('first_candle','?')} – {summary.get('last_candle','?')} UTC")
            print(f"  Open/Close: {summary.get('open')} / {summary.get('close')}")
            if summary.get("missing", 0) > 0:
                print(f"  Missing:   {summary['missing']} slots  sample={summary['missing_slots_sample']}")
            else:
                print(f"  Missing:   0  ← perfect coverage ✓")
        except Exception as e:
            print(f"  ERROR for day {day}: {e}")
            import traceback; traceback.print_exc()

    print(f"\n  Summary for {symbol}: {len(all_results)} days tested")
    for r in all_results:
        print(f"    {r['day']}  {r['candles']}/1440  {r['coverage_%']}%  missing={r['missing']}")


if __name__ == "__main__":
    print("Bybit public.bybit.com/trading/ — data format test")
    print(f"Date: {date.today()}  Python: {__import__('sys').version.split()[0]}")
    print(f"Testing symbols: {list(SYMBOLS.values())}\n")

    for coin, symbol in SYMBOLS.items():
        test_symbol(coin, symbol)

    print("\n\nDone.")
