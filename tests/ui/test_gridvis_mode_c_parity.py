"""Grid Visualizer V7 Mode C Parity Test.

Tests cover:
- Grid visualization mode C output parity with PB7 backtest fills
- Cross-validation with PB7 standalone backtest results
- OHLCV data consistency checks

Background:
    Mode C recomputes fills by replaying historical OHLCV candles through the
    Rust backtest engine. This test ensures perfect parity with PB7's standalone
    backtest results.

Note:
    This test uses the v7_strategy_explorer module (formerly v7_grid_visualizer)
    from the navi package. The module was renamed during refactoring.
"""

import os
import json
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


PBGUI_ROOT = Path(__file__).resolve().parents[2]
PB7_ROOT = Path(os.environ.get("PB7_ROOT", PBGUI_ROOT.parent / "pb7")).expanduser().resolve()


def _run_pb7_python_pipeline_fills(cfg: dict, exchange: str) -> pd.DataFrame:
    """Run PB7's own Python pipeline (cached hlcvs + build_backtest_payload) and return fills df.

    This is used as a ground-truth cross-check to determine whether mismatches
    come from our Mode C bundle-building or from different OHLCV inputs.
    """

    pb7_root = str(PB7_ROOT)
    pb7_src = os.path.join(pb7_root, "src")
    if not os.path.isdir(pb7_src):
        raise RuntimeError(f"PB7 src not found: {pb7_src}")

    # PB7 backtest code expects to run with CWD=pb7 root so relative cache paths resolve.
    old_cwd = os.getcwd()
    try:
        os.chdir(pb7_root)
        if pb7_src not in sys.path:
            sys.path.insert(0, pb7_src)

        # PB7 backtest.py imports plotting utilities which depend on `prettytable`.
        # For this parity test we don't need plotting; provide a tiny stub if missing.
        try:
            import prettytable  # type: ignore  # noqa: F401
        except Exception:
            import types

            stub = types.ModuleType("prettytable")

            class PrettyTable:  # noqa: D401
                def __init__(self, *args, **kwargs):
                    pass

                def add_row(self, *args, **kwargs):
                    return None

                def get_string(self, *args, **kwargs):
                    return ""

            stub.PrettyTable = PrettyTable
            sys.modules["prettytable"] = stub

        # PB7's downloader imports tqdm; importing backtest.py pulls it in via suite_runner.
        # The parity test never calls downloader paths; stub tqdm if it's not installed.
        try:
            import tqdm  # type: ignore  # noqa: F401
        except Exception:
            import types

            stub = types.ModuleType("tqdm")

            def tqdm(iterable=None, *args, **kwargs):  # type: ignore
                return iterable

            stub.tqdm = tqdm
            sys.modules["tqdm"] = stub

        import backtest as pb7_backtest  # type: ignore

        cached = pb7_backtest.load_coins_hlcvs_from_cache(cfg, exchange)
        if not cached:
            raise RuntimeError(
                "PB7 hlcvs_data cache missing for this config; run PB7 backtest once to populate caches/hlcvs_data"
            )

        _cache_dir, coins, hlcvs, mss, _results_path, btc_usd_prices, timestamps = cached
        payload = pb7_backtest.build_backtest_payload(
            hlcvs,
            mss,
            cfg,
            exchange,
            btc_usd_prices,
            timestamps=timestamps,
            coin_indices=None,
        )
        fills, equities_array, _analysis = pb7_backtest.execute_backtest(payload, cfg)

        fdf, _analysis_py, _bal_eq = pb7_backtest.process_forager_fills(
            fills,
            pb7_backtest.require_config_value(cfg, f"backtest.coins.{exchange}"),
            hlcvs,
            equities_array,
            balance_sample_divider=pb7_backtest.get_optional_config_value(cfg, "backtest.balance_sample_divider", 60),
        )

        # Convert into the same normalized schema we compare against.
        # process_forager_fills uses 'timestamp' as datetime already.
        out = fdf.copy()
        out.rename(
            columns={
                "qty": "fill_qty",
                "price": "fill_price",
                "psize": "position_size",
                "usd_total_balance": "usd_total_balance",
                "type": "order_type",
            },
            inplace=True,
        )
        out["timestamp_ms"] = out["timestamp"].apply(lambda x: int(pd.Timestamp(x).value // 1_000_000)).astype(
            "int64"
        )
        out["fill_qty"] = out["fill_qty"].astype("float64")
        out["fill_price"] = out["fill_price"].astype("float64")
        out["position_size"] = out["position_size"].astype("float64")
        out["usd_total_balance"] = out["usd_total_balance"].astype("float64")
        out["order_type"] = out["order_type"].astype(str)
        out.sort_values(["timestamp_ms"], inplace=True, kind="mergesort")
        out.reset_index(drop=True, inplace=True)
        return out[["timestamp_ms", "order_type", "fill_qty", "fill_price", "position_size", "usd_total_balance"]]
    finally:
        os.chdir(old_cwd)


def _read_backtest_folder() -> str:
    """Pick a known PB7 backtest result folder with fills.csv committed in workspace."""
    default = PB7_ROOT / "backtests" / "pbgui" / "bitget_DOGEUSDT" / "bybit" / "2026-01-14T17_45_14"
    base = os.environ.get("PB7_PARITY_BACKTEST_DIR", str(default))
    if not os.path.isdir(base):
        raise RuntimeError(f"Expected backtest folder not found: {base}")
    if not os.path.exists(os.path.join(base, "config.json")):
        raise RuntimeError(f"Missing config.json in {base}")
    if not os.path.exists(os.path.join(base, "fills.csv")):
        raise RuntimeError(f"Missing fills.csv in {base}")
    return base


def _to_ms(ts: pd.Timestamp) -> int:
    return int(pd.Timestamp(ts).value // 1_000_000)


def _normalize_pb7_fills_csv(df: pd.DataFrame) -> pd.DataFrame:
    # PB7 saved fills.csv uses `timestamp` as ISO string.
    out = df.copy()

    # Handle the extra unnamed index column written by pandas.
    if "Unnamed: 0" in out.columns:
        out.drop(columns=["Unnamed: 0"], inplace=True)
    if out.columns.tolist() and out.columns[0] == "":
        out.rename(columns={out.columns[0]: "_row"}, inplace=True)

    # Unify columns
    rename_map = {
        "qty": "fill_qty",
        "price": "fill_price",
        "psize": "position_size",
        "pprice": "position_price",
        "type": "order_type",
    }
    for k, v in rename_map.items():
        if k in out.columns and v not in out.columns:
            out.rename(columns={k: v}, inplace=True)

    # Build timestamp_ms
    if "timestamp_ms" not in out.columns:
        if "timestamp" not in out.columns:
            raise AssertionError(f"fills.csv missing timestamp column(s): {list(out.columns)}")
        ts = pd.to_datetime(out["timestamp"], errors="coerce")
        if ts.isna().any():
            bad = out[ts.isna()].head(3).to_dict(orient="records")
            raise AssertionError(f"failed parsing some timestamps; examples: {bad}")
        out["timestamp_ms"] = ts.apply(lambda x: int(pd.Timestamp(x).value // 1_000_000)).astype("int64")

    # Ensure required numeric types
    out["timestamp_ms"] = out["timestamp_ms"].astype("int64")
    out["fill_qty"] = out["fill_qty"].astype("float64")
    out["fill_price"] = out["fill_price"].astype("float64")
    out["position_size"] = out["position_size"].astype("float64")
    out["usd_total_balance"] = out["usd_total_balance"].astype("float64")
    out["order_type"] = out["order_type"].astype(str)

    # Use PB7's minute/index columns if present for stable sort; else timestamp only.
    sort_cols = ["timestamp_ms"]
    if "minute" in out.columns:
        sort_cols = ["minute", "timestamp_ms"]
    if "index" in out.columns:
        sort_cols.append("index")
    out.sort_values(sort_cols, inplace=True, kind="mergesort")
    out.reset_index(drop=True, inplace=True)
    return out


def _normalize_mode_c_events(events: list[dict]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["timestamp_ms", "order_type", "fill_qty", "fill_price", "position_size", "usd_total_balance"])

    df = pd.DataFrame(events)
    df["timestamp_ms"] = df["timestamp"].apply(lambda x: _to_ms(pd.to_datetime(x)))
    df["fill_qty"] = df["qty"].astype("float64")
    df["fill_price"] = df["price"].astype("float64")
    df["position_size"] = df["pos_size"].astype("float64")
    df["usd_total_balance"] = df["wallet_balance"].astype("float64")
    df["order_type"] = df["order_type"].astype(str)
    df.sort_values(["timestamp_ms"], inplace=True, kind="mergesort")
    df.reset_index(drop=True, inplace=True)
    return df[["timestamp_ms", "order_type", "fill_qty", "fill_price", "position_size", "usd_total_balance"]]


def _assert_close_series(a: pd.Series, b: pd.Series, *, name: str, atol: float) -> None:
    a = a.to_numpy(dtype=float)
    b = b.to_numpy(dtype=float)
    if a.shape != b.shape:
        raise AssertionError(f"shape mismatch for {name}: {a.shape} vs {b.shape}")
    ok = np.isclose(a, b, rtol=0.0, atol=atol, equal_nan=True)
    if not bool(np.all(ok)):
        bad_idx = int(np.where(~ok)[0][0])
        raise AssertionError(
            f"first mismatch in {name} at row {bad_idx}: got {a[bad_idx]} expected {b[bad_idx]} (atol={atol})"
        )


@pytest.mark.external_pb7
def test_gridvis_mode_c_matches_pb7_fills_csv():
    """Compare GridVis Mode C (PB7 engine) output against an existing PB7 fills.csv."""

    backtest_dir = _read_backtest_folder()
    with open(os.path.join(backtest_dir, "config.json"), "r", encoding="utf-8") as f:
        cfg = json.load(f)

    exchange = cfg["backtest"]["exchanges"][0]
    coin = cfg["live"]["approved_coins"]["long"][0]

    # PB7 interprets these date strings as UTC timestamps at 00:00.
    start_date = dt.datetime.strptime(cfg["backtest"]["start_date"], "%Y-%m-%d").replace(hour=0, minute=0)
    end_date = dt.datetime.strptime(cfg["backtest"]["end_date"], "%Y-%m-%d").replace(hour=0, minute=0)

    # Import Strategy Explorer helpers (keep import local to test)
    from navi import v7_strategy_explorer as gv
    from strategy_explorer_types import ExchangeParams, BotParams

    pbr = gv._get_passivbot_rust(gv._pb7_src_dir())

    hist_df = gv.load_historical_ohlcv_v7(exchange, coin)
    assert not hist_df.empty, f"No OHLCV found for {exchange} {coin}"

    # Determine candle range length in minutes inside available OHLCV
    idx_start = int(hist_df.index.get_indexer([start_date], method="nearest")[0])
    idx_end = int(hist_df.index.get_indexer([end_date], method="nearest")[0])
    if idx_end <= idx_start:
        raise AssertionError(f"bad idx range: {idx_start}..{idx_end}")

    max_candles_forward = int(idx_end - idx_start + 1)

    # Exchange params from market cache (best-effort)
    market_ep = gv._derive_exchange_params_from_market(exchange, coin)
    ep = ExchangeParams(
        qty_step=float(market_ep.get("qty_step") or 0.0),
        price_step=float(market_ep.get("price_step") or 0.0),
        min_qty=float(market_ep.get("min_qty") or 0.0),
        min_cost=float(market_ep.get("min_cost") or 0.0),
        c_mult=float(market_ep.get("c_mult") or 1.0),
    )

    bp_long = BotParams(**cfg["bot"]["long"])
    bp_short = BotParams(**cfg["bot"]["short"])

    events_long, events_short = gv._run_pb7_engine_backtest_for_visualizer(
        pbr=pbr,
        exchange=exchange,
        coin=coin,
        analysis_time=start_date,
        hist_df=hist_df,
        exchange_params=ep,
        bot_params_long=bp_long,
        bot_params_short=bp_short,
        starting_balance=float(cfg["backtest"]["starting_balance"]),
        max_candles_forward=max_candles_forward,
        config=cfg,
    )

    # This backtest has short disabled; compare long only.
    assert events_long, "Mode C returned no long events"
    assert not events_short, "Expected no short events for this config"

    df_mode_c = _normalize_mode_c_events(events_long)

    df_pb7 = pd.read_csv(os.path.join(backtest_dir, "fills.csv"))
    df_pb7 = _normalize_pb7_fills_csv(df_pb7)

    # Keep only this coin (some fills.csv may contain multiple coins)
    if "coin" in df_pb7.columns:
        df_pb7 = df_pb7[df_pb7["coin"].astype(str) == str(coin)].copy()
        df_pb7.reset_index(drop=True, inplace=True)

    # Compare row counts first; if mismatch, show helpful context.
    if len(df_mode_c) != len(df_pb7):
        head_a = df_mode_c.head(5).to_dict(orient="records")
        head_b = df_pb7.head(5)[["timestamp_ms", "order_type", "fill_qty", "fill_price", "position_size", "usd_total_balance"]].to_dict(orient="records")
        raise AssertionError(
            f"row count mismatch: mode_c={len(df_mode_c)} pb7={len(df_pb7)}\n"
            f"mode_c head: {head_a}\n"
            f"pb7 head: {head_b}"
        )

    # Compare key fields.
    a_ts = df_mode_c["timestamp_ms"].to_numpy()
    b_ts = df_pb7["timestamp_ms"].to_numpy()
    if not bool((a_ts == b_ts).all()):
        bad = int(np.where(a_ts != b_ts)[0][0])
        a_row = df_mode_c.iloc[bad].to_dict()
        b_row = df_pb7.iloc[bad][
            ["timestamp_ms", "order_type", "fill_qty", "fill_price", "position_size", "usd_total_balance"]
        ].to_dict()

        # Extra cross-check: does PB7's python pipeline reproduce the saved fills.csv?
        df_pb7_pipeline = _run_pb7_python_pipeline_fills(cfg, exchange)
        p_ts = df_pb7_pipeline["timestamp_ms"].to_numpy()
        pipeline_matches_file = len(df_pb7_pipeline) == len(df_pb7) and bool((p_ts == b_ts).all())

        raise AssertionError(
            "timestamp_ms differs\n"
            f"first mismatch row {bad}\n"
            f"mode_c: {a_row}\n"
            f"pb7_csv: {b_row}\n"
            f"pb7_python_pipeline_matches_saved_csv={pipeline_matches_file}"
        )
    assert (df_mode_c["order_type"].to_numpy() == df_pb7["order_type"].to_numpy()).all(), "order_type differs"

    _assert_close_series(df_mode_c["fill_qty"], df_pb7["fill_qty"], name="fill_qty", atol=1e-12)
    _assert_close_series(df_mode_c["fill_price"], df_pb7["fill_price"], name="fill_price", atol=1e-12)
    _assert_close_series(df_mode_c["position_size"], df_pb7["position_size"], name="position_size", atol=1e-12)
    _assert_close_series(df_mode_c["usd_total_balance"], df_pb7["usd_total_balance"], name="usd_total_balance", atol=1e-10)
