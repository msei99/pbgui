import json
import hjson
import pprint
import configparser
from pathlib import Path
import subprocess
import re


def coin_from_symbol_code(symbol_code: str) -> str:
    """Return a normalized base coin from various symbol code formats.

    Examples:
    - "DOGE_USDT:USDT" -> "DOGE"
    - "BTC/USDT:USDT"  -> "BTC"
    - "BTCUSDC"        -> "BTC"
    - "kPEPEUSDC"      -> "PEPE"
    - "1000SHIBUSDT"   -> "SHIB"

    This is intentionally lightweight (pure string ops + regex), so it can be
    used across PBGui without pulling in heavier modules.
    """
    s = str(symbol_code or "").strip()
    if not s:
        return ""

    # Split on common separators used by ccxt/PB7 cache dirs
    # (take the base part before the quote/settle).
    for sep in ("_", "/", ":", "-"):
        if sep in s:
            s = s.split(sep, 1)[0]
            break

    s = s.strip()
    if not s:
        return ""

    # Remove quote currency suffixes if present.
    for quote in ("USDT", "USDC", "BUSD", "TUSD", "USD", "EUR", "GBP", "DAI"):
        if s.endswith(quote) and len(s) > len(quote):
            s = s[: -len(quote)]
            break

    # Hyperliquid multiplier prefix (kPEPE -> PEPE)
    if s.startswith("k") and len(s) > 1 and s[1].isupper():
        s = s[1:]

    # Numeric multiplier prefixes (1000SHIB -> SHIB)
    m = re.match(r"^(\d+)([A-Z].*)$", s)
    if m:
        multiplier, coin = m.groups()
        if multiplier in {"1000", "10000", "100000", "1000000", "10000000", "1000000000"}:
            s = coin

    return s.strip().upper()

def save_ini(section : str, parameter : str, value : str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if not pb_config.has_section(section):
        pb_config.add_section(section)
    pb_config.set(section, parameter, value)
    with open('pbgui.ini', 'w') as pbgui_configfile:
        pb_config.write(pbgui_configfile)

def load_ini(section : str, parameter : str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if pb_config.has_option(section, parameter):
        return pb_config.get(section, parameter)
    else:
        return ""

def pbdir(): return load_ini("main", "pbdir")

def pbvenv(): return load_ini("main", "pbvenv")

def is_pb_installed():
    if Path(f"{pbdir()}/passivbot.py").exists():
        return True
    return False

def pb7dir(): return load_ini("main", "pb7dir")

def pb7venv(): return load_ini("main", "pb7venv")

def is_pb7_installed():
    if Path(f"{pb7dir()}/src/passivbot.py").exists():
        return True
    return False


def pb7srcdir() -> str:
    d = pb7dir()
    return f"{d}/src" if d else ""


def import_passivbot_rust():
    """Import PB7's compiled Rust extension (passivbot_rust).

    This uses `pb7dir` from `pbgui.ini` and prepends `<pb7dir>/src` to `sys.path`.
    """
    import sys

    src_dir = pb7srcdir()
    if src_dir and src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    import passivbot_rust as pbr  # type: ignore

    return pbr

PBGDIR = Path.cwd()

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except (ValueError,TypeError) as err:
        return False
    return True


def compute_pb7_entry_gating_ohlcv_df(
    cfg: dict,
    exchange: str,
    symbol_code: str,
    start_ms: int,
    end_ms: int,
    *,
    side: str = "long",
    warmup_days: int = 3,
    threshold_override: float | None = None,
    use_prev_minute_entry: bool = True,
):
    """Compute an OHLCV-based entry + price_distance_threshold gating series.

    This is meant for PBGui diagnostics and visualization of 'dip-only' opportunities.
    It intentionally uses 1m OHLCV bounds (open/close + favorable extreme) as a
    proxy for what the live bot might have seen as last_mprice.

    Returns a pandas.DataFrame with UTC timestamps and columns:
      time, open, high, low, close,
      entry_price, gate_price,
      diff_open, diff_close, diff_best,
      gate_open_open, gate_open_close, gate_open_best,
      dip_only
    """

    from datetime import datetime, timedelta, timezone

    import numpy as np
    import pandas as pd

    ex = str(exchange or "").strip().lower()
    side_n = str(side or "").strip().lower()
    if side_n not in {"long", "short"}:
        raise ValueError("side must be 'long' or 'short'")

    coin = coin_from_symbol_code(symbol_code)
    if not coin:
        raise ValueError("symbol_code has no coin")

    # Parse needed parameters from v7 config.json
    bot = (cfg or {}).get("bot", {}) if isinstance(cfg, dict) else {}
    live = (cfg or {}).get("live", {}) if isinstance(cfg, dict) else {}
    side_cfg = bot.get(side_n, {}) if isinstance(bot, dict) else {}

    span0 = float(side_cfg.get("ema_span_0", 0.0) or 0.0)
    span1 = float(side_cfg.get("ema_span_1", 0.0) or 0.0)
    dist = float(side_cfg.get("entry_initial_ema_dist", 0.0) or 0.0)

    if span0 <= 0.0 or span1 <= 0.0:
        raise ValueError("ema_span_0/ema_span_1 missing or invalid")

    threshold = (
        float(threshold_override)
        if threshold_override is not None
        else float(live.get("price_distance_threshold", 0.0) or 0.0)
    )
    if threshold < 0.0:
        threshold = 0.0

    pb7_root = pb7dir()
    if not pb7_root:
        raise FileNotFoundError("pb7dir not configured in pbgui.ini")

    # PB7 stores daily 1m OHLCV as npy under historical_data/ohlcvs_<exchange>/<COIN>/YYYY-MM-DD.npy
    # Some exchanges have variants (binanceusdm vs binance).
    exchange_candidates = [ex]
    if ex == "binance":
        exchange_candidates = ["binanceusdm", "binance"]
    
    # Build date list with warmup
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    start_day = (start_dt.date() - timedelta(days=max(0, int(warmup_days))))
    end_day = end_dt.date()

    def daterange(d0, d1):
        cur = d0
        while cur <= d1:
            yield cur
            cur = cur + timedelta(days=1)

    arrays = []
    for cand_ex in exchange_candidates:
        base = (
            Path(pb7_root)
            / "historical_data"
            / f"ohlcvs_{cand_ex}"
            / coin
        )
        if not base.exists():
            continue
        for d in daterange(start_day, end_day):
            p = base / f"{d.isoformat()}.npy"
            if not p.exists():
                continue
            try:
                a = np.load(p)
                if a is None or getattr(a, "size", 0) == 0:
                    continue
                arrays.append(a)
            except Exception:
                continue
        if arrays:
            break

    if not arrays:
        raise FileNotFoundError(
            f"No OHLCV npy found for {coin} on {exchange_candidates} in pb7 historical_data"
        )

    arr = np.vstack(arrays)
    if arr.shape[1] < 5:
        raise ValueError("Unexpected OHLCV npy shape")

    ts = arr[:, 0].astype("int64")
    o = arr[:, 1].astype(float)
    h = arr[:, 2].astype(float)
    l = arr[:, 3].astype(float)
    c = arr[:, 4].astype(float)

    # Sort by timestamp to be safe.
    idx = np.argsort(ts)
    ts, o, h, l, c = ts[idx], o[idx], h[idx], l[idx], c[idx]

    # Filter to include warmup pre-window.
    lo_ms = int(start_ms - max(0, int(warmup_days)) * 86_400_000)
    hi_ms = int(end_ms)
    m = (ts >= lo_ms) & (ts <= hi_ms)
    ts, o, h, l, c = ts[m], o[m], h[m], l[m], c[m]
    if ts.size < 10:
        raise ValueError("Not enough OHLCV rows")

    def ema(series: np.ndarray, span: float) -> np.ndarray:
        span = float(span)
        alpha = 2.0 / (span + 1.0)
        out = np.empty_like(series, dtype=float)
        out[0] = float(series[0])
        for i in range(1, series.shape[0]):
            out[i] = out[i - 1] + alpha * (float(series[i]) - out[i - 1])
        return out

    s2 = (span0 * span1) ** 0.5
    e0 = ema(c, span0)
    e1 = ema(c, span1)
    e2 = ema(c, s2)
    lower = np.minimum(np.minimum(e0, e1), e2)
    upper = np.maximum(np.maximum(e0, e1), e2)

    if side_n == "long":
        entry_raw = lower * (1.0 - dist)
    else:
        entry_raw = upper * (1.0 + dist)

    entry_price = entry_raw.copy()
    if use_prev_minute_entry and entry_price.size > 1:
        entry_price[1:] = entry_price[:-1]
        entry_price[0] = np.nan

    # Gate price: market price must cross this bound for gating to open.
    if side_n == "long":
        gate_price = entry_price / (1.0 - threshold) if threshold < 1.0 else np.nan
        # diff = 1 - P/M
        diff_open = 1.0 - entry_price / o
        diff_close = 1.0 - entry_price / c
        diff_best = 1.0 - entry_price / l
        gate_open_open = o <= gate_price
        gate_open_close = c <= gate_price
        gate_open_best = l <= gate_price
    else:
        gate_price = entry_price / (1.0 + threshold)
        # diff = P/M - 1
        diff_open = entry_price / o - 1.0
        diff_close = entry_price / c - 1.0
        diff_best = entry_price / h - 1.0
        gate_open_open = o >= gate_price
        gate_open_close = c >= gate_price
        gate_open_best = h >= gate_price

    dip_only = gate_open_best & (~gate_open_open) & (~gate_open_close)

    df = pd.DataFrame(
        {
            "ts_ms": ts,
            "time": pd.to_datetime(ts, unit="ms", utc=True),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "entry_price": entry_price,
            "gate_price": gate_price,
            "diff_open": diff_open,
            "diff_close": diff_close,
            "diff_best": diff_best,
            "gate_open_open": gate_open_open,
            "gate_open_close": gate_open_close,
            "gate_open_best": gate_open_best,
            "dip_only": dip_only,
        }
    )

    # Final filter to requested window
    df = df[(df["ts_ms"] >= int(start_ms)) & (df["ts_ms"] <= int(end_ms))].reset_index(drop=True)
    return df

def validateHJSON(hjsonData):
    try:
        hjson.loads(hjsonData)
    except (ValueError) as err:
        return False
    return True

def config_pretty_str(config: dict):
    try:
        return json.dumps(config, indent=4)
    except TypeError:
        pretty_str = pprint.pformat(config)
        for r in [("'", '"'), ("True", "true"), ("False", "false"), ("None", "null")]:
            pretty_str = pretty_str.replace(*r)
        return pretty_str

def load_symbols_from_ini(exchange: str, market_type: str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if pb_config.has_option("exchanges", f'{exchange}.{market_type}'):
        return eval(pb_config.get("exchanges", f'{exchange}.{market_type}'))
    else:
        return []


def list_remote_git_branches(remote_url: str, timeout_sec: int = 20) -> list[str]:
    """Return branch names from a remote URL (e.g. https://github.com/<user>/passivbot.git).

    Uses `git ls-remote --heads` and parses refs/heads/*.
    """
    remote_url = (remote_url or "").strip()
    if not remote_url:
        return []

    try:
        res = subprocess.run(
            ["git", "ls-remote", "--heads", remote_url],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Timeout fetching remote branches ({timeout_sec}s).") from e
    except Exception as e:
        raise RuntimeError("Failed to run git to fetch remote branches.") from e

    if res.returncode != 0:
        msg = (res.stderr or "").strip() or "git ls-remote failed"
        raise RuntimeError(msg)

    branches: list[str] = []
    for line in (res.stdout or "").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        ref = parts[1]
        if ref.startswith("refs/heads/"):
            branches.append(ref[len("refs/heads/"):])

    return sorted(set(branches))


def list_git_remotes(repo_dir: str, timeout_sec: int = 10) -> list[str]:
    """List git remote names for a local repo directory."""
    repo_dir = (repo_dir or "").strip()
    if not repo_dir:
        return []
    try:
        res = subprocess.run(
            ["git", "-C", repo_dir, "remote"],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except Exception:
        return []
    if res.returncode != 0:
        return []
    remotes = [ln.strip() for ln in (res.stdout or "").splitlines() if ln.strip()]
    return sorted(set(remotes))


def get_git_remote_url(repo_dir: str, remote_name: str, timeout_sec: int = 10) -> str:
    """Get remote URL from a local repo directory. Returns empty string on failure."""
    repo_dir = (repo_dir or "").strip()
    remote_name = (remote_name or "").strip()
    if not repo_dir or not remote_name:
        return ""
    try:
        res = subprocess.run(
            ["git", "-C", repo_dir, "remote", "get-url", remote_name],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except Exception:
        return ""
    if res.returncode != 0:
        return ""
    return (res.stdout or "").strip()


def pb7_suite_preflight_errors(config: dict) -> list[str]:
    """Return preflight errors for PB7 suite configs.

    PB7 behavior note:
    - If `backtest.suite.enabled` is true and `include_base_scenario` is false,
      PB7 builds the master coin universe ONLY from scenario `coins` and
      `coin_sources`. In that mode, `live.approved_coins` is effectively ignored
      for coin selection.

    This helper is intentionally pure (no Streamlit, no IO) and conservative.
    """

    errors: list[str] = []
    if not isinstance(config, dict):
        return ["Invalid config: expected a JSON object."]

    backtest = config.get("backtest")
    if not isinstance(backtest, dict):
        return ["Invalid config: missing or invalid 'backtest' section."]

    suite = backtest.get("suite") or {}
    if not isinstance(suite, dict) or not suite.get("enabled"):
        return []

    scenarios = suite.get("scenarios") or []
    if not isinstance(scenarios, list) or not scenarios:
        errors.append("Suite is enabled but has no scenarios.")
        return errors

    include_base = bool(suite.get("include_base_scenario", False))
    if include_base:
        return []

    base_coin_sources = backtest.get("coin_sources") or {}
    has_base_coin_sources = isinstance(base_coin_sources, dict) and any(
        v is not None and str(v).strip() for v in base_coin_sources.values()
    )

    has_any_scenario_coins = False
    has_any_scenario_coin_sources = False
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            continue
        coins = scenario.get("coins")
        if isinstance(coins, (list, tuple, set)):
            if any(str(x).strip() for x in coins if x is not None):
                has_any_scenario_coins = True
        elif isinstance(coins, str) and coins.strip():
            has_any_scenario_coins = True

        coin_sources = scenario.get("coin_sources")
        if isinstance(coin_sources, dict) and any(
            v is not None and str(v).strip() for v in coin_sources.values()
        ):
            has_any_scenario_coin_sources = True

    if not (has_base_coin_sources or has_any_scenario_coins or has_any_scenario_coin_sources):
        errors.append(
            "Suite is enabled with include_base_scenario=false, but no coins are defined in any scenario "
            "(and no coin_sources are set). PB7 will build an empty master coin list and fail with "
            "'No coin data found on any exchange for the requested date range.'\n"
            "Fix: enable include_base_scenario, OR set scenario coins, OR set coin_sources."
        )

    return errors
