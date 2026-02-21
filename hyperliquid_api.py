from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import numpy as np

from market_data import append_exchange_download_log, get_exchange_raw_root_dir, normalize_market_data_coin_dir
from PBCoinData import get_symbol_for_coin


HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
_HYPERLIQUID_META_CACHE: dict[str, Any] = {"ts": 0.0, "names": set(), "names_upper": {}}
_HYPERLIQUID_META_TTL_S = 300.0


def _utc_day_start_ms(d: date) -> int:
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _ensure_date(v: Any) -> date:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                pass
    raise ValueError(f"Invalid date: {v!r}")


def normalize_hyperliquid_coin(coin: str) -> str:
    """Normalize coin identifier for Hyperliquid /info endpoints.

    - Market Data config usually uses base coin (e.g. ETH).
    - If a symbol-like value is passed (e.g. ETHUSDC, kPEPEUSDC), strip the quote.
    """

    c = str(coin or "").strip()
    if not c:
        raise ValueError("coin is empty")

    if "/" in c:
        c = c.split("/", 1)[0]
    elif "_" in c and ":" in c:
        c = c.split("_", 1)[0]

    c_u = c.upper()
    # Normalize stock-perp aliases to Hyperliquid dex-meta format.
    # Supported inputs: xyz:AAPL, XYZ:AAPL, xyz-aapl, XYZ-AAPL
    if (c_u.startswith("XYZ:") or c_u.startswith("XYZ-")) and len(c_u) > 4:
        return f"xyz:{c_u[4:]}"

    for suffix in ("USDC", "USD", "USDT"):
        if c_u.endswith(suffix) and len(c_u) > len(suffix):
            c_u = c_u[: -len(suffix)]
            break
    return c_u


def _load_hyperliquid_meta_names(*, timeout_s: float = 30.0) -> tuple[set[str], dict[str, str]]:
    now = time.time()
    cached = _HYPERLIQUID_META_CACHE.get("names")
    cached_upper = _HYPERLIQUID_META_CACHE.get("names_upper")
    if cached and cached_upper and (now - float(_HYPERLIQUID_META_CACHE.get("ts", 0.0)) < _HYPERLIQUID_META_TTL_S):
        return set(cached), dict(cached_upper)

    names: set[str] = set()
    names_upper: dict[str, str] = {}
    try:
        for payload in ({"type": "meta"}, {"type": "meta", "dex": "xyz"}):
            res = hyperliquid_info_post(payload, timeout_s=timeout_s)
            if not isinstance(res, dict):
                continue
            universe = res.get("universe")
            if isinstance(universe, list):
                for item in universe:
                    if not isinstance(item, dict):
                        continue
                    name = item.get("name")
                    if isinstance(name, str) and name:
                        names.add(name)
                        names_upper[name.upper()] = name
    except Exception:
        names = set()
        names_upper = {}

    _HYPERLIQUID_META_CACHE["ts"] = now
    _HYPERLIQUID_META_CACHE["names"] = names
    _HYPERLIQUID_META_CACHE["names_upper"] = names_upper
    return set(names), dict(names_upper)


def resolve_hyperliquid_coin_name(*, coin: str, timeout_s: float = 30.0) -> str:
    """Resolve to the canonical coin name from Hyperliquid meta."""

    coin_norm = normalize_hyperliquid_coin(coin)
    names, names_upper = _load_hyperliquid_meta_names(timeout_s=timeout_s)
    if coin_norm in names:
        return coin_norm
    if coin_norm.upper() in names_upper:
        return names_upper[coin_norm.upper()]

    try:
        sym = get_symbol_for_coin(coin_norm, "hyperliquid.swap")
        sym_norm = normalize_hyperliquid_coin(sym)
        if sym_norm in names:
            return sym_norm
        if sym_norm.upper() in names_upper:
            return names_upper[sym_norm.upper()]
    except Exception:
        pass

    if names:
        raise ValueError(f"Hyperliquid meta does not contain coin '{coin_norm}'")
    raise ValueError(f"Unable to resolve Hyperliquid coin '{coin_norm}' (meta unavailable)")


def hyperliquid_info_post(payload: dict[str, Any], *, timeout_s: float = 30.0) -> Any:
    r = requests.post(HYPERLIQUID_INFO_URL, json=payload, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def fetch_candle_snapshot(
    *,
    coin: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    timeout_s: float = 30.0,
) -> list[dict[str, Any]]:
    """Fetch candle snapshot from Hyperliquid.

    Uses /info type=candleSnapshot.

    Returns list of dict candles (as provided by API). Expected keys commonly include:
      t, o, h, l, c, v
    """

    if start_ms >= end_ms:
        return []

    coin_resolved = resolve_hyperliquid_coin_name(coin=coin, timeout_s=timeout_s)
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin_resolved,
            "interval": str(interval).strip(),
            "startTime": int(start_ms),
            "endTime": int(end_ms),
        },
    }

    res = hyperliquid_info_post(payload, timeout_s=timeout_s)
    if res is None:
        return []
    if isinstance(res, list):
        out: list[dict[str, Any]] = []
        for item in res:
            if isinstance(item, dict):
                out.append(item)
        return out
    raise RuntimeError(f"Unexpected candleSnapshot response type: {type(res).__name__}")


def _candle_ts_ms(candle: dict[str, Any]) -> int | None:
    if not isinstance(candle, dict):
        return None
    t = candle.get("t")
    if t is None:
        return None
    try:
        return int(t)
    except Exception:
        return None


def _dataset_dir(interval: str) -> Path:
    interval_norm = str(interval).strip()
    # Keep true API 1m downloads separate from computed/best 1m output.
    # - 1m_api: raw API downloads
    # - candles_1m: computed/best archive (l2Book synthesis + API 1m)
    if interval_norm == "1m":
        return get_exchange_raw_root_dir("hyperliquid") / "1m_api"
    return get_exchange_raw_root_dir("hyperliquid") / f"candles_{interval_norm}"


@dataclass
class DownloadCandlesResult:
    coin: str
    interval: str
    start_date: str
    end_date: str
    n_days: int
    n_files_written: int
    n_files_skipped: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "coin": self.coin,
            "interval": self.interval,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "n_days": int(self.n_days),
            "n_files_written": int(self.n_files_written),
            "n_files_skipped": int(self.n_files_skipped),
        }


def download_hyperliquid_candles_api(
    *,
    coin: str,
    interval: str,
    start_date: date | str,
    end_date: date | str,
    overwrite: bool = False,
    dry_run: bool = False,
    timeout_s: float = 30.0,
    sleep_s: float = 0.05,
) -> DownloadCandlesResult:
    """Download candles via Hyperliquid API and store as shards.

        Writes files:
                - interval=1m  -> data/ohlcv/hyperliquid/1m_api/<coin>/YYYY-MM-DD.npz
            - other        -> data/ohlcv/hyperliquid/candles_<interval>/<coin>/YYYYMMDD-HH.jsonl
    """

    coin_norm = str(coin or "").strip().upper()
    if not coin_norm:
        raise ValueError("coin is empty")

    interval_norm = str(interval or "").strip()
    if not interval_norm:
        raise ValueError("interval is empty")

    d0 = _ensure_date(start_date)
    d1 = _ensure_date(end_date)
    if d1 < d0:
        raise ValueError("end_date must be >= start_date")

    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin_norm)
    out_dir = _dataset_dir(interval_norm) / coin_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    n_files_written = 0
    n_files_skipped = 0

    n_days = (d1 - d0).days + 1
    append_exchange_download_log(
        "hyperliquid",
        f"[hl_api_candles] start coin={coin_norm} interval={interval_norm} {d0.strftime('%Y%m%d')}->{d1.strftime('%Y%m%d')} overwrite={bool(overwrite)} dry_run={bool(dry_run)}",
    )

    cur = d0
    while cur <= d1:
        start_ms = _utc_day_start_ms(cur)
        # Hyperliquid's candleSnapshot appears to treat endTime as inclusive.
        # Use (day_end - 1ms) to avoid pulling the first candle of the next day.
        end_ms = start_ms + 86_400_000 - 1

        candles = []
        if not dry_run:
            candles = fetch_candle_snapshot(
                coin=coin_norm,
                interval=interval_norm,
                start_ms=start_ms,
                end_ms=end_ms,
                timeout_s=timeout_s,
            )

        day_str = cur.strftime("%Y%m%d")
        if interval_norm == "1m":
            day_tag = cur.strftime("%Y-%m-%d")
            out_path = out_dir / f"{day_tag}.npz"
            if out_path.exists() and not overwrite:
                n_files_skipped += 1
            else:
                if dry_run:
                    n_files_written += 1
                else:
                    rows = []
                    for c in candles:
                        if not isinstance(c, dict):
                            continue
                        t = c.get("t")
                        o = c.get("o")
                        h = c.get("h")
                        l = c.get("l")
                        cc = c.get("c")
                        v = c.get("v")
                        if t is None or o is None or h is None or l is None or cc is None or v is None:
                            continue
                        try:
                            rows.append((int(t), float(o), float(h), float(l), float(cc), float(v)))
                        except Exception:
                            continue
                    if rows:
                        rows.sort(key=lambda r: r[0])
                        dtype = np.dtype([
                            ("ts", "i8"),
                            ("o", "f4"),
                            ("h", "f4"),
                            ("l", "f4"),
                            ("c", "f4"),
                            ("bv", "f4"),
                        ])
                        arr = np.array(rows, dtype=dtype)
                        out_path.parent.mkdir(parents=True, exist_ok=True)
                        np.savez_compressed(out_path, candles=arr)
                        n_files_written += 1
        else:
            by_hour: dict[str, list[dict[str, Any]]] = {}
            for c in candles:
                ts_ms = _candle_ts_ms(c)
                if ts_ms is None:
                    continue
                dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                hour_key = dt.strftime("%Y%m%d-%H")
                by_hour.setdefault(hour_key, []).append(c)

            for hour in range(24):
                hour_key = f"{day_str}-{hour:02d}"
                if hour_key not in by_hour:
                    continue
                out_path = out_dir / f"{hour_key}.jsonl"
                if out_path.exists() and not overwrite:
                    n_files_skipped += 1
                    continue

                if dry_run:
                    n_files_written += 1
                    continue

                lines = [json.dumps(x, separators=(",", ":"), ensure_ascii=False) for x in by_hour[hour_key]]
                out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                n_files_written += 1

        append_exchange_download_log(
            "hyperliquid",
            f"[hl_api_candles] day={day_str} candles={len(candles)} files_written={n_files_written} files_skipped={n_files_skipped}",
        )

        if sleep_s:
            time.sleep(float(sleep_s))
        cur = cur + timedelta(days=1)

    res = DownloadCandlesResult(
        coin=coin_norm,
        interval=interval_norm,
        start_date=d0.strftime("%Y%m%d"),
        end_date=d1.strftime("%Y%m%d"),
        n_days=int(n_days),
        n_files_written=int(n_files_written),
        n_files_skipped=int(n_files_skipped),
    )
    append_exchange_download_log("hyperliquid", f"[INFO] [hl_api_candles] done {res.to_dict()}")
    return res
