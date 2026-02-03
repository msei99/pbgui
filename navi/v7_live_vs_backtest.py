import json
import math
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import pbgui_help
from BacktestV7 import BacktestV7Item, BacktestV7Queue
from Database import Database
from pbgui_func import (
    PBGDIR,
    get_navi_paths,
    is_authenticted,
    is_pb7_installed,
    is_session_state_not_initialized,
    pb7dir,
    set_page_config,
)
from pbgui_purefunc import coin_from_symbol_code


def _pb7_markets_cache_path(exchange: str) -> Path:
    ex = str(exchange or "").strip().lower()
    return Path(f"{pb7dir()}/caches/{ex}/markets.json")


def _load_pb7_markets_cache(exchange: str) -> dict | None:
    """Load PB7's cached CCXT markets.json for an exchange.

    This is used for diagnostics only (min_cost/min_qty/qty_step/etc.).
    """

    ex = str(exchange or "").strip().lower()
    if not ex:
        return None

    cache_key = f"v7_live_vs_backtest_markets_cache__{ex}"
    if cache_key in st.session_state:
        return st.session_state.get(cache_key)

    p = _pb7_markets_cache_path(ex)
    if not p.exists():
        st.session_state[cache_key] = None
        return None

    try:
        markets = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(markets, dict):
            markets = None
    except Exception:
        markets = None

    st.session_state[cache_key] = markets
    return markets


def _pb7_markets_cache_candidates(exchange: str, *, prefer_futures: bool = False) -> list[str]:
    """Return PB7 cache exchange ids to try for a given logical exchange.

    PBGui often uses CCXT ids like "binance" at the UI level, while PB7 may cache
    futures markets under "binanceusdm". For diagnostics we can safely try both.
    """

    ex = str(exchange or "").strip().lower()
    if not ex:
        return []

    # Prefer futures markets cache when we know we're dealing with perp/swap symbols.
    if ex == "binance" and prefer_futures:
        return ["binanceusdm", "binance"]

    return [ex]


def _find_market_for_coin(markets: dict, coin: str, quote: str = "USDT") -> tuple[str, dict] | tuple[None, None]:
    """Find a CCXT market entry for a coin/quote pair.

    Returns (symbol, market_dict) or (None, None).
    """

    if not markets or not coin:
        return None, None

    c = str(coin).strip().upper()
    q = str(quote).strip().upper()
    best = None

    for sym, m in markets.items():
        if not isinstance(m, dict):
            continue
        try:
            if str(m.get("base", "")).upper() != c:
                continue
            if str(m.get("quote", "")).upper() != q:
                continue
        except Exception:
            continue

        # Prefer swap/perp markets when available
        try:
            is_swap = bool(m.get("swap"))
        except Exception:
            is_swap = False

        if best is None:
            best = (sym, m, is_swap)
        else:
            # If current is swap and previous isn't, prefer current
            if is_swap and not best[2]:
                best = (sym, m, is_swap)

    if best is None:
        return None, None
    return best[0], best[1]


def _market_constraints_from_ccxt_market(m: dict) -> dict:
    """Mirror PB7's HLCVManager.get_market_specific_settings() extraction."""
    limits = m.get("limits", {}) if isinstance(m, dict) else {}
    precision = m.get("precision", {}) if isinstance(m, dict) else {}

    min_cost = 0.01
    try:
        mc = (limits.get("cost", {}) or {}).get("min")
        if mc is not None:
            min_cost = float(mc)
    except Exception:
        pass

    price_step = None
    qty_step = None
    try:
        price_step = precision.get("price")
    except Exception:
        price_step = None
    try:
        qty_step = precision.get("amount")
    except Exception:
        qty_step = None

    min_qty = 0.0
    try:
        lm = (limits.get("amount", {}) or {}).get("min")
        if lm is not None:
            min_qty = max(min_qty, float(lm))
    except Exception:
        pass
    try:
        if qty_step is not None:
            min_qty = max(min_qty, float(qty_step))
    except Exception:
        pass

    out = {
        "min_cost": min_cost,
        "min_qty": min_qty,
        "qty_step": qty_step,
        "price_step": price_step,
        "maker_fee": m.get("maker"),
        "taker_fee": m.get("taker"),
        "c_mult": m.get("contractSize"),
    }
    return out


def _effective_min_order_at_price(
    *,
    price: float | None,
    min_cost: float | None,
    min_qty: float | None,
    qty_step: float | None,
    c_mult: float | None,
) -> tuple[float | None, float | None, float | None]:
    """Return (min_qty_eff_units, min_coin_eff, min_notional_eff) at a given price.

    Notes:
    - qty units are whatever the exchange uses (coin for spot, contracts for perps).
    - coin amount is qty_units * c_mult (contractSize). For spot, c_mult is effectively 1.
    """

    try:
        p = float(price) if price is not None else None
        if p is None or not math.isfinite(p) or p <= 0:
            return None, None, None

        mc = float(min_cost) if min_cost is not None else 0.0
        mq = float(min_qty) if min_qty is not None else 0.0
        qs = float(qty_step) if qty_step is not None else 0.0
        cm = float(c_mult) if c_mult is not None else 1.0
        if not math.isfinite(cm) or cm <= 0:
            cm = 1.0

        qty_by_cost = (mc / (p * cm)) if mc > 0 else 0.0

        if qs and math.isfinite(qs) and qs > 0:
            qty_by_cost = math.ceil(qty_by_cost / qs) * qs
            qty_eff = max(mq, qty_by_cost)
            # align to step
            qty_eff = math.ceil(qty_eff / qs) * qs
        else:
            qty_eff = max(mq, qty_by_cost)

        coin_eff = qty_eff * cm
        notional_eff = coin_eff * p
        return qty_eff, coin_eff, notional_eff
    except Exception:
        return None, None, None


def _infer_coins_from_config_dict(cfg: dict) -> list[str]:
    """Infer traded coins from a PB7 config dict (prefer live.approved_coins)."""

    if not isinstance(cfg, dict):
        return []

    coins: set[str] = set()
    live = cfg.get("live", {}) if isinstance(cfg.get("live", {}), dict) else {}
    approved = live.get("approved_coins", {}) if isinstance(live.get("approved_coins", {}), dict) else {}

    for side_key in ("long", "short"):
        vals = approved.get(side_key, [])
        if not isinstance(vals, list):
            continue
        for sym in vals:
            c = coin_from_symbol_code(_coerce_usdc_to_usdt(sym))
            if c:
                coins.add(str(c).upper())

    return sorted(coins)


def _live_exchange_from_config_dict(cfg: dict) -> str | None:
    if not isinstance(cfg, dict):
        return None
    live = cfg.get("live", {}) if isinstance(cfg.get("live", {}), dict) else {}
    ex = live.get("exchange")
    ex = str(ex or "").strip().lower()
    return ex or None


def _coerce_usdc_to_usdt(sym: str) -> str:
    s = str(sym or "").strip()
    if not s:
        return ""
    if s.endswith("USDC") and len(s) > 4:
        return s[:-4] + "USDT"
    return s


def _extract_quote_from_symbol(sym: str) -> str | None:
    """Extract quote currency from common symbol formats.

    Examples:
    - 'ETH/USDC:USDC' -> 'USDC'
    - 'ETH/USDT' -> 'USDT'
    - 'ETHUSDT' -> 'USDT' (best-effort)
    """

    s = str(sym or "").strip().upper()
    if not s:
        return None

    if "/" in s:
        try:
            right = s.split("/", 1)[1]
            return right.split(":", 1)[0].strip() or None
        except Exception:
            return None

    for q in ("USDT", "USDC", "USD"):
        if s.endswith(q):
            return q
    return None


def _infer_coins_for_coin_sources(bt_config, *, fallback_symbols: list[str] | None = None) -> list[str]:
    coins: set[str] = set()

    # Prefer explicit approved coin lists from the config (these are what the bot intends to trade).
    try:
        for sym in (bt_config.live.approved_coins.long or []):
            c = coin_from_symbol_code(_coerce_usdc_to_usdt(sym))
            if c:
                coins.add(str(c))
        for sym in (bt_config.live.approved_coins.short or []):
            c = coin_from_symbol_code(_coerce_usdc_to_usdt(sym))
            if c:
                coins.add(str(c))
    except Exception:
        pass

    # If the config doesn't have explicit lists (or they are empty), fall back to DB-derived symbols.
    if not coins and fallback_symbols:
        for sym in fallback_symbols:
            c = coin_from_symbol_code(_coerce_usdc_to_usdt(sym))
            if c:
                coins.add(str(c))

    return sorted(coins)


def _classify_uniqueid(uid: str) -> str:
    s = str(uid or '').strip().lower()
    if not s:
        return 'unknown'
    if s.isdigit():
        return 'trade_id'
    for key in ('fund', 'funding'):
        if key in s:
            return 'funding'
    for key in ('fee', 'commission'):
        if key in s:
            return 'fee'
    for key in ('transfer', 'deposit', 'withdraw'):
        if key in s:
            return 'transfer'
    for key in ('interest', 'borrow'):
        if key in s:
            return 'interest'
    return 'other'


def _coin_from_exec_symbol(sym: str | None) -> str | None:
    """Extract base coin from a CCXT-style symbol like 'VET/USDT:USDT'."""
    if not sym:
        return None
    s = str(sym).strip()
    if not s:
        return None
    try:
        base = s.split('/', 1)[0]
        base = base.split(':', 1)[0]
        base = base.replace('_', '').replace('-', '').strip().upper()
        return base or None
    except Exception:
        return None


def _match_rows_by_time(
    bt_rows: pd.DataFrame,
    ex_rows: pd.DataFrame,
    tolerance_s: int = 120,
    require_side_match: bool = True,
    require_coin_match: bool = True,
    include_unmatched_exec_rows: bool = False,
    suppress_exec_rows_seen_as_candidates: bool = True,
    candidates_from_unmatched_only: bool = True,
) -> pd.DataFrame:
    """Greedy match two time-sorted tables by nearest timestamp.

    bt_rows expects a 'time' column (datetime64[ns, UTC])
    ex_rows expects a 'time' column (datetime64[ns, UTC])
    """
    if bt_rows is None or ex_rows is None or bt_rows.empty or ex_rows.empty:
        return pd.DataFrame(
            columns=[
                'bt_time', 'bt_coin', 'bt_type', 'bt_qty', 'bt_price', 'bt_net', 'bt_expected_side',
                'bt_qty_contracts', 'bt_qty_coin', 'bt_contract_size',
                'bt_psize_delta', 'bt_psize_delta_coin',
                'candidate_time', 'candidate_symbol', 'candidate_side', 'candidate_qty', 'candidate_price', 'candidate_net',
                'candidate_trade_count', 'candidate_trade_ids',
                'matched_time', 'matched_symbol', 'matched_side', 'matched_qty', 'matched_price', 'matched_net',
                'matched_trade_count', 'matched_trade_ids',
                'dt_s', 'match', 'reason',
            ]
        )

    tol = max(0, int(tolerance_s or 0))
    bt = bt_rows.sort_values('time').reset_index(drop=True)
    ex = ex_rows.sort_values('time').reset_index(drop=True)

    # Normalize exec side strings
    if 'side' in ex.columns:
        ex['side'] = ex['side'].astype(str).str.lower().replace({'long': 'buy', 'short': 'sell'})

    # Ensure exec coin exists if we have symbols (aggregation may drop derived columns).
    if 'coin' not in ex.columns and 'symbol' in ex.columns:
        try:
            ex['coin'] = ex['symbol'].apply(_coin_from_exec_symbol)
        except Exception:
            pass

    # If we have coin/symbol info, prevent cross-coin matching (important in Total scope).
    has_bt_coin = 'coin' in bt.columns and bt['coin'].notna().any()
    has_ex_coin = 'coin' in ex.columns and ex['coin'].notna().any()
    do_coin_match = bool(require_coin_match) and bool(has_bt_coin) and bool(has_ex_coin)

    ex_used = set()
    ex_seen_as_candidate = set()
    out = []
    ex_times = ex['time'].tolist()

    # Store per-BT-row info so we can recompute candidates after matching.
    bt_expected_sides: list[str | None] = []
    bt_times: list[pd.Timestamp] = []
    matched_exec_idx: list[int | None] = []

    # Simple pointer-based greedy: for each bt row, search around current pointer
    j = 0
    for i in range(len(bt)):
        bt_t = bt.at[i, 'time']
        bt_coin = None
        if 'coin' in bt.columns:
            try:
                bt_coin = str(bt.at[i, 'coin']).strip().upper() or None
            except Exception:
                bt_coin = None
        if do_coin_match and not bt_coin:
            # If we require coin matching but this row has no coin, disable coin match for it.
            bt_coin = None
        bt_qty = None
        try:
            bt_qty = float(bt.at[i, 'qty']) if 'qty' in bt.columns else None
        except Exception:
            bt_qty = None
        bt_expected_side = None
        if bt_qty is not None:
            if bt_qty > 0:
                bt_expected_side = 'buy'
            elif bt_qty < 0:
                bt_expected_side = 'sell'

        bt_times.append(bt_t)
        bt_expected_sides.append(bt_expected_side)
        # advance pointer so ex[j] is not too far behind
        while j < len(ex_times) and ex_times[j] < bt_t - pd.Timedelta(seconds=tol):
            j += 1

        best_k = None
        best_dt = None
        # check a small window ahead; typical per-day counts are low
        bt_qty_abs = None
        try:
            if bt_qty is not None and math.isfinite(bt_qty):
                bt_qty_abs = abs(float(bt_qty))
        except Exception:
            bt_qty_abs = None

        for k in range(max(0, j - 3), min(len(ex_times), j + 10)):
            if k in ex_used:
                continue
            if do_coin_match and bt_coin:
                try:
                    if str(ex.at[k, 'coin']).strip().upper() != bt_coin:
                        continue
                except Exception:
                    continue
            if require_side_match and bt_expected_side and 'side' in ex.columns:
                try:
                    if str(ex.at[k, 'side']).lower() != bt_expected_side:
                        continue
                except Exception:
                    pass
            dt = abs((ex_times[k] - bt_t).total_seconds())
            if best_dt is None or dt < best_dt:
                best_dt = dt
                best_k = k
            elif best_dt is not None and dt == best_dt and bt_qty_abs is not None and 'qty' in ex.columns:
                # Tie-breaker: prefer qty-closest candidate when timestamps are identical.
                try:
                    cand_qty = float(ex.at[k, 'qty'])
                    best_qty = float(ex.at[best_k, 'qty']) if best_k is not None else None
                    if best_qty is None or not math.isfinite(best_qty):
                        best_k = k
                    else:
                        if math.isfinite(cand_qty):
                            if abs(cand_qty - bt_qty_abs) < abs(best_qty - bt_qty_abs):
                                best_k = k
                except Exception:
                    pass

        # Always show nearest candidate (if any), but only consume it if matched.
        cand_t = None
        cand_symbol = None
        cand_side = None
        cand_qty = None
        cand_price = None
        cand_net = None
        cand_trade_count = None
        cand_trade_ids = None
        if best_k is not None:
            ex_seen_as_candidate.add(best_k)
            cand_t = ex.at[best_k, 'time']
            cand_symbol = ex.at[best_k, 'symbol'] if 'symbol' in ex.columns else None
            cand_side = ex.at[best_k, 'side'] if 'side' in ex.columns else None
            cand_qty = ex.at[best_k, 'qty'] if 'qty' in ex.columns else None
            cand_price = ex.at[best_k, 'price'] if 'price' in ex.columns else None
            cand_net = ex.at[best_k, 'net'] if 'net' in ex.columns else None
            cand_trade_count = ex.at[best_k, 'trade_count'] if 'trade_count' in ex.columns else None
            cand_trade_ids = ex.at[best_k, 'trade_ids_preview'] if 'trade_ids_preview' in ex.columns else None

        matched = (best_k is not None and best_dt is not None and best_dt <= tol)
        reason = None
        if best_k is None or best_dt is None:
            matched = False
            reason = 'no_candidate'
        elif best_dt > tol:
            matched = False
            reason = 'dt>tol'
        else:
            # within tolerance; side constraint already applied in candidate filter
            matched = True
            reason = 'ok'
            ex_used.add(best_k)

        matched_exec_idx.append(best_k if matched else None)

        # Matched execution columns: only populate if actually matched
        m_t = cand_t if matched else None
        m_symbol = cand_symbol if matched else None
        m_side = cand_side if matched else None
        m_qty = cand_qty if matched else None
        m_price = cand_price if matched else None
        m_net = cand_net if matched else None
        m_trade_count = cand_trade_count if matched else None
        m_trade_ids = cand_trade_ids if matched else None

        out.append({
            'bt_time': bt_t,
            'bt_coin': bt_coin,
            'bt_type': bt.at[i, 'type'] if 'type' in bt.columns else None,
            'bt_qty': bt.at[i, 'qty'] if 'qty' in bt.columns else None,
            'bt_price': bt.at[i, 'price'] if 'price' in bt.columns else None,
            'bt_net': bt.at[i, 'net'] if 'net' in bt.columns else None,
            'bt_expected_side': bt_expected_side,
            'bt_qty_contracts': bt.at[i, 'qty_contracts'] if 'qty_contracts' in bt.columns else None,
            'bt_qty_coin': bt.at[i, 'qty_coin'] if 'qty_coin' in bt.columns else None,
            'bt_contract_size': bt.at[i, 'contract_size'] if 'contract_size' in bt.columns else None,
            'bt_psize_delta': bt.at[i, 'psize_delta'] if 'psize_delta' in bt.columns else None,
            'bt_psize_delta_coin': bt.at[i, 'psize_delta_coin'] if 'psize_delta_coin' in bt.columns else None,
            'candidate_time': cand_t,
            'candidate_symbol': cand_symbol,
            'candidate_side': cand_side,
            'candidate_qty': cand_qty,
            'candidate_price': cand_price,
            'candidate_net': cand_net,
            'candidate_trade_count': cand_trade_count,
            'candidate_trade_ids': cand_trade_ids,
            'matched_time': m_t,
            'matched_symbol': m_symbol,
            'matched_side': m_side,
            'matched_qty': m_qty,
            'matched_price': m_price,
            'matched_net': m_net,
            'matched_trade_count': m_trade_count,
            'matched_trade_ids': m_trade_ids,
            'dt_s': float(best_dt) if best_dt is not None else None,
            'match': bool(matched),
            'reason': reason,
        })

    # If enabled: recompute candidates for unmatched BT rows from *unmatched* executions only.
    # This avoids showing the same execution as candidate for earlier dt>tol rows when it is
    # actually matched to a later BT row.
    if candidates_from_unmatched_only:
        try:
            unmatched_pool = [k for k in range(len(ex)) if k not in ex_used]
        except Exception:
            unmatched_pool = []

        if unmatched_pool:
            # Precompute for speed
            pool_times = {k: ex_times[k] for k in unmatched_pool}
        else:
            pool_times = {}

        for i in range(len(out)):
            if out[i].get('match'):
                continue
            bt_t = bt_times[i]
            expected = bt_expected_sides[i]
            bt_coin = out[i].get('bt_coin')

            best_k = None
            best_dt = None
            for k, t in pool_times.items():
                if do_coin_match and bt_coin:
                    try:
                        if str(ex.at[k, 'coin']).strip().upper() != bt_coin:
                            continue
                    except Exception:
                        continue
                if require_side_match and expected and 'side' in ex.columns:
                    try:
                        if str(ex.at[k, 'side']).lower() != expected:
                            continue
                    except Exception:
                        pass
                dt = abs((t - bt_t).total_seconds())
                if best_dt is None or dt < best_dt:
                    best_dt = dt
                    best_k = k

            if best_k is None or best_dt is None:
                # No unmatched candidate at all
                out[i]['candidate_time'] = None
                out[i]['candidate_side'] = None
                out[i]['candidate_qty'] = None
                out[i]['candidate_price'] = None
                out[i]['candidate_net'] = None
                out[i]['candidate_trade_count'] = None
                out[i]['candidate_trade_ids'] = None
                out[i]['dt_s'] = None
                out[i]['reason'] = 'no_unmatched_candidate'
                continue

            # Show the best unmatched candidate (even if dt>tol)
            out[i]['candidate_time'] = ex.at[best_k, 'time']
            out[i]['candidate_symbol'] = ex.at[best_k, 'symbol'] if 'symbol' in ex.columns else None
            out[i]['candidate_side'] = ex.at[best_k, 'side'] if 'side' in ex.columns else None
            out[i]['candidate_qty'] = ex.at[best_k, 'qty'] if 'qty' in ex.columns else None
            out[i]['candidate_price'] = ex.at[best_k, 'price'] if 'price' in ex.columns else None
            out[i]['candidate_net'] = ex.at[best_k, 'net'] if 'net' in ex.columns else None
            out[i]['candidate_trade_count'] = ex.at[best_k, 'trade_count'] if 'trade_count' in ex.columns else None
            out[i]['candidate_trade_ids'] = ex.at[best_k, 'trade_ids_preview'] if 'trade_ids_preview' in ex.columns else None
            out[i]['dt_s'] = float(best_dt)
            out[i]['reason'] = 'dt>tol' if best_dt > tol else out[i].get('reason')
        # Rebuild seen-as-candidate set so unmatched exec row suppression still makes sense
        try:
            ex_seen_as_candidate = {
                int(k)
                for k in range(len(ex))
                if any((row.get('candidate_time') == ex.at[k, 'time']) for row in out)
            }
        except Exception:
            pass

    # Optionally add unmatched executions as separate rows (bt_* empty).
    # To avoid confusing duplicates, we can suppress exec rows already shown as candidates.
    if include_unmatched_exec_rows:
        for k in range(len(ex)):
            if k in ex_used:
                continue
            if suppress_exec_rows_seen_as_candidates and k in ex_seen_as_candidate:
                continue
            out.append({
                'bt_time': None,
                'bt_coin': None,
                'bt_type': None,
                'bt_qty': None,
                'bt_price': None,
                'bt_net': None,
                'bt_expected_side': None,
                'bt_psize_delta': None,
                # Populate matched_* so exec-only rows are readable without showing candidate columns
                'candidate_time': None,
                'candidate_symbol': None,
                'candidate_side': None,
                'candidate_qty': None,
                'candidate_price': None,
                'candidate_net': None,
                'candidate_trade_count': None,
                'candidate_trade_ids': None,
                'matched_time': ex.at[k, 'time'],
                'matched_symbol': ex.at[k, 'symbol'] if 'symbol' in ex.columns else None,
                'matched_side': ex.at[k, 'side'] if 'side' in ex.columns else None,
                'matched_qty': ex.at[k, 'qty'] if 'qty' in ex.columns else None,
                'matched_price': ex.at[k, 'price'] if 'price' in ex.columns else None,
                'matched_net': ex.at[k, 'net'] if 'net' in ex.columns else None,
                'matched_trade_count': ex.at[k, 'trade_count'] if 'trade_count' in ex.columns else None,
                'matched_trade_ids': ex.at[k, 'trade_ids_preview'] if 'trade_ids_preview' in ex.columns else None,
                'dt_s': None,
                'match': False,
                'reason': 'unmatched_exec',
            })

    mdf = pd.DataFrame(out)
    # Stable ordering: bt_time first, then ex_time
    try:
        sort_series = None
        for c in ('bt_time', 'matched_time', 'candidate_time'):
            if c not in mdf.columns:
                continue
            if sort_series is None:
                sort_series = mdf[c]
            else:
                sort_series = sort_series.fillna(mdf[c])
        if sort_series is not None:
            mdf['_sort'] = sort_series
            mdf = mdf.sort_values('_sort').drop(columns=['_sort'])
    except Exception:
        pass
    return mdf


def _aggregate_partial_executions(ex_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate partial fills so one order/tranche doesn't appear as multiple exec rows.

    Preference:
    - If order_id is present: group by (order_id)
    - Else: group by (time floored to second, side, rounded price)

    Produces columns: time, side, qty, price, net, order_id, trade_id (preview), trade_count
    """
    if ex_df is None or ex_df.empty:
        return pd.DataFrame()

    df = ex_df.copy()
    if 'time' not in df.columns:
        return df

    # Normalize
    if 'side' in df.columns:
        df['side'] = df['side'].astype(str).str.lower().replace({'long': 'buy', 'short': 'sell'})

    # Ensure numeric
    for col in ('qty', 'price', 'net'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    has_order_id = 'order_id' in df.columns and df['order_id'].notna().any()
    if has_order_id:
        # Preserve side: otherwise "Require side match" will eliminate all candidates.
        # Some venues may reuse order_id across symbols; keep symbol if present.
        gcols = ['order_id']
        if 'side' in df.columns:
            gcols.append('side')
        if 'symbol' in df.columns:
            gcols.append('symbol')
    else:
        df['time_s'] = df['time'].dt.floor('s')
        # price rounding helps merge tiny float deltas
        if 'price' in df.columns:
            df['price_r'] = df['price'].round(2)
        else:
            df['price_r'] = pd.NA
        gcols = ['time_s', 'side', 'price_r']
        if 'symbol' in df.columns:
            gcols.append('symbol')

    def _preview_trade_ids(x: pd.Series) -> str:
        vals = [str(v) for v in x.dropna().astype(str).tolist() if str(v).strip()]
        if not vals:
            return ''
        if len(vals) <= 4:
            return ','.join(vals)
        return ','.join(vals[:4]) + f" …(+{len(vals)-4})"

    def _agg_group(g: pd.DataFrame) -> pd.Series:
        qty_sum = float(g['qty'].sum()) if 'qty' in g.columns else 0.0
        net_sum = float(g['net'].sum()) if 'net' in g.columns else 0.0
        # Weighted average by abs(qty) to avoid tiny partials skewing price
        price_val = None
        try:
            if 'price' in g.columns and 'qty' in g.columns:
                w = g['qty'].abs()
                denom = float(w.sum())
                if denom > 0:
                    price_val = float((g['price'] * w).sum() / denom)
                else:
                    price_val = float(g['price'].mean())
            elif 'price' in g.columns:
                price_val = float(g['price'].mean())
        except Exception:
            price_val = None

        time_min = None
        try:
            time_min = g['time'].min() if 'time' in g.columns else None
        except Exception:
            time_min = None

        trade_preview = ''
        trade_count = None
        if 'trade_id' in g.columns:
            trade_preview = _preview_trade_ids(g['trade_id'])
            try:
                trade_count = int(g['trade_id'].count())
            except Exception:
                trade_count = None

        return pd.Series({
            'time': time_min,
            'qty': qty_sum,
            'net': net_sum,
            'price': price_val,
            'trade_ids_preview': trade_preview,
            'trade_count': trade_count,
        })

    grouped = df.groupby(gcols, dropna=False).apply(_agg_group).reset_index()

    # Put back to expected schema
    if not has_order_id:
        # from synthetic keys
        grouped = grouped.rename(columns={'time_s': 'time'})
        grouped['order_id'] = pd.NA
        if 'price_r' in grouped.columns and 'price' in grouped.columns:
            # use rounded price as display price if mean is NaN
            try:
                grouped['price'] = grouped['price'].fillna(grouped['price_r'])
            except Exception:
                pass

    # Ensure columns exist
    for col in ['side', 'qty', 'price', 'net', 'order_id', 'trade_ids_preview', 'trade_count']:
        if col not in grouped.columns:
            grouped[col] = pd.NA

    # Sort
    try:
        grouped = grouped.sort_values('time')
    except Exception:
        pass
    return grouped


def _docs_index(lang: str) -> list[tuple[str, str]]:
    ln = str(lang or "EN").strip().upper()
    folder = "help_de" if ln == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for p in sorted(docs_dir.glob("*.md")):
        label = p.name
        try:
            with open(p, "r", encoding="utf-8") as f:
                first = f.readline().strip()
            if first.startswith("#"):
                label = first.lstrip("#").strip() or p.name
        except Exception:
            label = p.name
        out.append((label, str(p)))
    return out


def _read_markdown(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Failed to read docs: {e}"


@st.dialog("Help & Tutorials", width="large")
def _help_modal(default_topic: str = "Live vs Backtest"):
    # Mirror the API-Keys editor guide behavior
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="v7_lvb_help_lang")
    docs = _docs_index(str(lang))
    if not docs:
        st.info("No help docs found.")
        return

    labels = [d[0] for d in docs]
    default_index = 0
    try:
        target = str(default_topic or "").strip().lower()
        if target:
            for i, lbl in enumerate(labels):
                if target in str(lbl).lower():
                    default_index = i
                    break
    except Exception:
        default_index = 0

    sel = st.selectbox(
        "Select Topic",
        options=list(range(len(labels))),
        format_func=lambda i: labels[int(i)],
        index=int(default_index),
        key="v7_lvb_help_sel",
    )
    path = docs[int(sel)][1]
    md = _read_markdown(path)
    st.markdown(md, unsafe_allow_html=True)
    try:
        base = str(st.get_option("server.baseUrlPath") or "").strip("/")
        prefix = f"/{base}" if base else ""
        st.markdown(
            f"<a href='{prefix}/help' target='_blank'>Open full Help page in new tab</a>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass


def _get_db() -> Database:
    if "v7_live_vs_backtest_db" not in st.session_state:
        st.session_state.v7_live_vs_backtest_db = Database()
    return st.session_state.v7_live_vs_backtest_db


def _extract_users_from_dashboard_config(cfg: dict) -> list[str]:
    users: list[str] = []
    if not isinstance(cfg, dict):
        return users
    for k, v in cfg.items():
        if not isinstance(k, str) or "_users_" not in k:
            continue
        if isinstance(v, str):
            if v:
                users.append(v)
        elif isinstance(v, (list, tuple)):
            for item in v:
                if isinstance(item, str) and item:
                    users.append(item)
    return users


def _infer_user_from_current_dashboard() -> str | None:
    """Infer a likely user from the currently loaded dashboard config."""
    dash_obj = st.session_state.get("dashboard")
    cfg = getattr(dash_obj, "dashboard_config", None)
    extracted = _extract_users_from_dashboard_config(cfg)
    extracted = [u for u in extracted if isinstance(u, str) and u and u.upper() != "ALL"]
    if not extracted:
        return None
    counts: dict[str, int] = {}
    for u in extracted:
        counts[u] = counts.get(u, 0) + 1
    # Prefer most frequent user; stable tie-breaker by name
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def live_vs_backtest_page():
    users = st.session_state.users
    db = _get_db()

    all_user_options = users.list()
    if not all_user_options:
        st.warning("No users found. Add users in API-Keys.")
        return

    show_all_key = "v7_live_vs_backtest_show_all_users"
    if show_all_key not in st.session_state:
        st.session_state[show_all_key] = False

    # Build filtered user list (default: only users with live income rows)
    user_with_income_cache_key = "v7_live_vs_backtest_users_with_income_cache"
    users_with_income = st.session_state.get(user_with_income_cache_key)
    if users_with_income is None:
        try:
            users_with_income = db.list_income_users()
        except Exception:
            users_with_income = []
        st.session_state[user_with_income_cache_key] = users_with_income

    users_with_income_set = set(users_with_income or [])
    live_user_options = [u for u in all_user_options if u in users_with_income_set]

    # Toggle to show all API-key users (even without live income)
    col_user, col_toggle = st.columns([3, 1], vertical_alignment="bottom")
    with col_toggle:
        st.toggle("All users", key=show_all_key, help="Show all API-Keys users (including those without live income data)")

    user_options = all_user_options if st.session_state.get(show_all_key, False) else live_user_options
    if not user_options:
        st.warning("No users with live income data found; showing all users.")
        user_options = all_user_options

    prefill_user = st.session_state.get("v7_live_vs_backtest_prefill_user")

    # If user navigated here via menu from Dashboards, prefill user from that dashboard.
    try:
        last_dash_ts = float(st.session_state.get("dashboards_last_active_ts") or 0.0)
    except Exception:
        last_dash_ts = 0.0
    came_from_dashboards = (time.time() - last_dash_ts) <= 30.0
    if came_from_dashboards and not prefill_user:
        inferred = _infer_user_from_current_dashboard()
        if inferred:
            prefill_user = inferred

            # If the inferred user is not in the filtered list, show all users so it can be selected.
            if inferred in all_user_options and inferred not in user_options:
                st.session_state[show_all_key] = True
                user_options = all_user_options
    user_key = "v7_live_vs_backtest_user"
    if came_from_dashboards and prefill_user in user_options:
        # Override persisted selection when user came from Dashboards via menu.
        st.session_state[user_key] = prefill_user
    elif user_key not in st.session_state:
        st.session_state[user_key] = prefill_user if prefill_user in user_options else (users.default() if users.default() in user_options else user_options[0])
    else:
        # If current selection is not in the filtered options, reset to a valid value
        if st.session_state.get(user_key) not in user_options:
            st.session_state[user_key] = prefill_user if prefill_user in user_options else user_options[0]

    with col_user:
        single_user = st.selectbox("User", user_options, key=user_key)

    # Defaults for date range
    today = date.today()
    default_start_date = today - timedelta(days=365)
    default_end_date = today
    try:
        live_min_ts = db.min_income_timestamp(single_user)
        if live_min_ts is not None:
            default_start_date = datetime.fromtimestamp(int(live_min_ts) / 1000, tz=timezone.utc).date()
    except Exception:
        pass

    try:
        live_max_ts = db.max_income_timestamp(single_user)
        if live_max_ts is not None:
            default_end_date = datetime.fromtimestamp(int(live_max_ts) / 1000, tz=timezone.utc).date()
    except Exception:
        pass

    # Determine exchange from the User object (api-keys), not from the username
    user_exchange = None
    try:
        user_obj = users.find_user(single_user)
        if user_obj is not None:
            user_exchange = user_obj.exchange
    except Exception:
        user_exchange = None

    compare_exchange_key = "v7_live_vs_backtest_exchange"
    default_compare_exchange = user_exchange or "binance"
    available_compare_exchanges = ["binance", "bybit", "gateio", "bitget", "hyperliquid", "combined"]
    if compare_exchange_key not in st.session_state:
        st.session_state[compare_exchange_key] = default_compare_exchange if default_compare_exchange in available_compare_exchanges else "binance"
    if st.session_state.get(compare_exchange_key) not in available_compare_exchanges:
        st.session_state[compare_exchange_key] = "binance"

    compare_start_key = "v7_live_vs_backtest_start"
    compare_end_key = "v7_live_vs_backtest_end"
    compare_pending_start_key = "v7_live_vs_backtest_pending_start"
    compare_pending_end_key = "v7_live_vs_backtest_pending_end"
    compare_symbols_key = "v7_live_vs_backtest_symbols"
    compare_result_key = "v7_live_vs_backtest_result"

    min_picker_date = date(1970, 1, 1)
    max_picker_date = today

    # If user changes, reset date range defaults (and dependent selections) to match that user's live data.
    last_user_key = "v7_live_vs_backtest_last_user"
    prev_user = st.session_state.get(last_user_key)
    if prev_user != single_user:
        st.session_state[last_user_key] = single_user
        # Clear pending sync from previous user
        st.session_state.pop(compare_pending_start_key, None)
        st.session_state.pop(compare_pending_end_key, None)

        ds = max(min_picker_date, min(default_start_date, max_picker_date))
        de = max(min_picker_date, min(default_end_date, max_picker_date))
        st.session_state[compare_start_key] = ds
        st.session_state[compare_end_key] = de

        # Reset selections tied to the previous user
        st.session_state.pop(compare_symbols_key, None)
        st.session_state.pop(compare_result_key, None)

        # Reset exchange default for the newly selected user
        st.session_state[compare_exchange_key] = (
            default_compare_exchange if default_compare_exchange in available_compare_exchanges else "binance"
        )

    # Apply any pending sync values BEFORE instantiating the date_input widgets
    try:
        if compare_pending_start_key in st.session_state and compare_pending_end_key in st.session_state:
            ps = st.session_state.get(compare_pending_start_key)
            pe = st.session_state.get(compare_pending_end_key)
            if isinstance(ps, datetime):
                ps = ps.date()
            if isinstance(pe, datetime):
                pe = pe.date()
            if isinstance(ps, date) and isinstance(pe, date):
                ps = max(min_picker_date, min(ps, max_picker_date))
                pe = max(min_picker_date, min(pe, max_picker_date))
                st.session_state[compare_start_key] = ps
                st.session_state[compare_end_key] = pe
            st.session_state.pop(compare_pending_start_key, None)
            st.session_state.pop(compare_pending_end_key, None)
    except Exception:
        st.session_state.pop(compare_pending_start_key, None)
        st.session_state.pop(compare_pending_end_key, None)

    if compare_start_key in st.session_state:
        try:
            v = st.session_state[compare_start_key]
            if isinstance(v, datetime):
                v = v.date()
            if v < min_picker_date or v > max_picker_date:
                st.session_state[compare_start_key] = max(min_picker_date, min(default_start_date, max_picker_date))
        except Exception:
            st.session_state[compare_start_key] = max(min_picker_date, min(default_start_date, max_picker_date))

    if compare_end_key in st.session_state:
        try:
            v = st.session_state[compare_end_key]
            if isinstance(v, datetime):
                v = v.date()
            if v < min_picker_date or v > max_picker_date:
                st.session_state[compare_end_key] = max(min_picker_date, min(default_end_date, max_picker_date))
        except Exception:
            st.session_state[compare_end_key] = max(min_picker_date, min(default_end_date, max_picker_date))

    # Pre-calculate starting balance for the chosen start date and allow override
    sb_override_key = "v7_live_vs_backtest_starting_balance"
    sb_sig_key = "v7_live_vs_backtest_starting_balance_sig"
    sb_calc_info_key = "v7_live_vs_backtest_starting_balance_calc_info"
    sb_manual_key = "v7_live_vs_backtest_starting_balance_manual"
    sb_last_calc_key = "v7_live_vs_backtest_starting_balance_last_calc"
    sb_calc_val_key = "v7_live_vs_backtest_starting_balance_calc_value"

    def _mark_starting_balance_manual():
        st.session_state[sb_manual_key] = True

    def _reset_starting_balance_to_calc():
        try:
            calc_val = st.session_state.get(sb_calc_val_key)
            if calc_val is None:
                return
            calc_val_f = float(calc_val)
            st.session_state[sb_manual_key] = False
            st.session_state[sb_override_key] = calc_val_f
            st.session_state[sb_last_calc_key] = calc_val_f
        except Exception:
            return

    sb_start_date = st.session_state.get(compare_start_key, default_start_date)
    if isinstance(sb_start_date, datetime):
        sb_start_date = sb_start_date.date()
    if not isinstance(sb_start_date, date):
        sb_start_date = max(min_picker_date, min(default_start_date, max_picker_date))

    sb_start_ms = int(datetime(sb_start_date.year, sb_start_date.month, sb_start_date.day, tzinfo=timezone.utc).timestamp() * 1000)
    sb_signature = f"{single_user}|{sb_start_ms}"

    calc_sb = None
    calc_info = None
    try:
        balances = db.fetch_balances([single_user])
        if balances:
            ref_ts = int(balances[0][1])
            ref_balance = float(balances[0][2])
            income_sum = db.sum_income(single_user, sb_start_ms, ref_ts) if ref_ts > sb_start_ms else 0.0
            calc_sb = float(ref_balance - income_sum)
            calc_info = (ref_balance, income_sum, calc_sb)
    except Exception:
        calc_sb = None
        calc_info = None

    # Expose the latest calculated value to callbacks (button on_click runs early).
    try:
        st.session_state[sb_calc_val_key] = float(calc_sb) if calc_sb is not None else None
    except Exception:
        st.session_state[sb_calc_val_key] = None

    # Keep the input synced when user/start changes or when DB-derived balance changes,
    # unless the user explicitly overrides the value.
    prev_sig = st.session_state.get(sb_sig_key)
    if prev_sig != sb_signature:
        st.session_state[sb_sig_key] = sb_signature
        st.session_state[sb_manual_key] = False
        if calc_sb is not None:
            st.session_state[sb_override_key] = float(calc_sb)
        elif sb_override_key not in st.session_state:
            st.session_state[sb_override_key] = 1000.0
        st.session_state[sb_last_calc_key] = float(calc_sb) if calc_sb is not None else None
    else:
        # Same user/start: if we have a new calculation (e.g. balance table updated)
        # and the user did not manually override, auto-refresh the input.
        try:
            manual = bool(st.session_state.get(sb_manual_key, False))
        except Exception:
            manual = False
        if not manual and calc_sb is not None:
            prev_calc = st.session_state.get(sb_last_calc_key)
            try:
                prev_calc_f = float(prev_calc) if prev_calc is not None else None
            except Exception:
                prev_calc_f = None
            if prev_calc_f is None or abs(prev_calc_f - float(calc_sb)) > 1e-9:
                # Only overwrite if the current value still matches the previous auto value
                # or is unset/zero-ish.
                try:
                    cur_val = st.session_state.get(sb_override_key)
                    cur_val_f = float(cur_val) if cur_val is not None else None
                except Exception:
                    cur_val_f = None
                if cur_val_f is None or abs(cur_val_f) < 1e-12 or (prev_calc_f is not None and abs(cur_val_f - prev_calc_f) < 1e-9):
                    st.session_state[sb_override_key] = float(calc_sb)
                st.session_state[sb_last_calc_key] = float(calc_sb)
    st.session_state[sb_calc_info_key] = calc_info

    with st.container(border=True):
        c1, c2, c3, c4, c5, c6, c7 = st.columns([1, 1, 1.1, 0.9, 0.6, 0.35, 0.35], vertical_alignment="bottom")
        with c1:
            compare_start_date = st.date_input(
                "Start",
                value=max(min_picker_date, min(default_start_date, max_picker_date)),
                key=compare_start_key,
                min_value=min_picker_date,
                max_value=max_picker_date,
            )
        with c2:
            compare_end_date = st.date_input(
                "End",
                value=max(min_picker_date, min(default_end_date, max_picker_date)),
                key=compare_end_key,
                min_value=min_picker_date,
                max_value=max_picker_date,
            )
        with c3:
            compare_exchange = st.selectbox(
                "Exchange",
                available_compare_exchanges,
                key=compare_exchange_key,
                help=pbgui_help.compare_backtest_exchange_help,
            )
        with c4:
            sb_help = ""
            try:
                if calc_sb is not None:
                    sb_help = "Calculated from latest balance in DB and income since Start; you can override it for the run."
                else:
                    sb_help = "No balance in DB; set starting balance manually."
            except Exception:
                sb_help = ""

            sb_c1, sb_c2 = st.columns([6, 2], vertical_alignment="bottom")
            with sb_c1:
                st.number_input(
                    "Starting Balance",
                    key=sb_override_key,
                    step=0.01,
                    format="%.2f",
                    label_visibility="collapsed",
                    help=sb_help,
                    on_change=_mark_starting_balance_manual,
                )
            with sb_c2:
                st.button(
                    ":material/replay:",
                    key="v7_live_vs_backtest_reset_starting_balance",
                    help="Reset to calculated starting balance",
                    disabled=(calc_sb is None),
                    use_container_width=True,
                    on_click=_reset_starting_balance_to_calc,
                )
        with c5:
            run_bt = st.button(
                ":material/play_arrow:",
                key="v7_live_vs_backtest_run_bt",
                help=(
                    "Disabled when Exchange is 'combined'. "
                    f"Enqueues a backtest using your data/run_v7/{single_user}/config.json"
                ),
                disabled=(compare_exchange == "combined"),
                use_container_width=True,
            )
        with c6:
            if st.button(
                ":material/refresh:",
                key="v7_live_vs_backtest_refresh",
                help="Refresh this page",
                use_container_width=True,
            ):
                # Clear cached DB-derived user list
                st.session_state.pop(user_with_income_cache_key, None)
                st.rerun()
        with c7:
            if st.button(
                ":material/home:",
                key="v7_live_vs_backtest_back_to_dashboards",
                help="Go to Dashboards",
                use_container_width=True,
            ):
                # Ask Dashboards page to open the matching user dashboard if it exists.
                st.session_state["dashboards_open_dashboard"] = single_user
                st.switch_page(get_navi_paths()["INFO_DASHBOARDS"])

        if compare_start_date > compare_end_date:
            st.error("Start must be <= End")
            return

        start_ms = int(datetime(compare_start_date.year, compare_start_date.month, compare_start_date.day, tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int((datetime(compare_end_date.year, compare_end_date.month, compare_end_date.day, tzinfo=timezone.utc) + timedelta(days=1)).timestamp() * 1000) - 1

        try:
            symbols_in_range = db.list_income_symbols(single_user, start_ms, end_ms)
        except Exception:
            symbols_in_range = []

        if not symbols_in_range:
            st.session_state.pop(compare_symbols_key, None)

        latest_balance_info = None
        if run_bt:
            if compare_exchange == "combined":
                st.warning("Exchange 'combined' is for comparing existing results only.")
                return
            run_cfg = Path(f"{PBGDIR}/data/run_v7/{single_user}/config.json")
            if not run_cfg.exists():
                st.error(f"No run_v7 config found at {run_cfg}")
            else:
                bt = BacktestV7Item(str(run_cfg))
                bt.config.backtest.exchanges = [compare_exchange]
                # Default behavior for most exchanges: run per-exchange backtests (avoid combined dataset runs).
                # Special case: Hyperliquid often lacks long-term candles, so we keep execution exchange
                # as Hyperliquid but force candle sourcing from Binance via coin_sources.
                try:
                    if str(compare_exchange).lower() == "hyperliquid":
                        bt.config.backtest.combine_ohlcvs = True
                        coins = _infer_coins_for_coin_sources(
                            bt.config,
                            fallback_symbols=symbols_in_range,
                        )
                        bt.config.backtest.coin_sources = {c: "binance" for c in coins}
                    else:
                        bt.config.backtest.combine_ohlcvs = False
                except Exception:
                    pass

                # Translate USDC symbols to USDT symbols so BacktestV7 UI can display them
                try:
                    bt.config.live.approved_coins.long = [
                        _coerce_usdc_to_usdt(x) for x in (bt.config.live.approved_coins.long or []) if str(x or "").strip()
                    ]
                    bt.config.live.approved_coins.short = [
                        _coerce_usdc_to_usdt(x) for x in (bt.config.live.approved_coins.short or []) if str(x or "").strip()
                    ]
                    bt.config.live.ignored_coins.long = [
                        _coerce_usdc_to_usdt(x) for x in (bt.config.live.ignored_coins.long or []) if str(x or "").strip()
                    ]
                    bt.config.live.ignored_coins.short = [
                        _coerce_usdc_to_usdt(x) for x in (bt.config.live.ignored_coins.short or []) if str(x or "").strip()
                    ]
                except Exception:
                    pass

                bt.config.backtest.start_date = compare_start_date.strftime("%Y-%m-%d")
                bt.config.backtest.end_date = compare_end_date.strftime("%Y-%m-%d")

                # Use the (possibly overridden) starting balance input
                try:
                    bt.config.backtest.starting_balance = float(st.session_state.get(sb_override_key))
                except Exception:
                    pass

                # Optional info for debugging/calibration
                latest_balance_info = st.session_state.get(sb_calc_info_key)

                bt.save()
                bt.save_queue()
                if "bt_v7_queue" not in st.session_state:
                    st.session_state.bt_v7_queue = BacktestV7Queue()
                st.session_state.bt_v7_queue.run()
                st.success("Backtest enqueued.")

    # Discover available PB7 results for this user
    results_root = Path(f"{pb7dir()}/backtests/pbgui/{single_user}")
    analysis_files = []
    if results_root.exists():
        analysis_files = [p for p in results_root.glob("**/analysis.json") if p.is_file()]
    result_dirs = [p.parent for p in analysis_files]

    has_any_combined = any(d.parent.name == "combined" for d in result_dirs)

    compare_exchange = st.session_state.get(compare_exchange_key)
    results_exchange = "combined" if str(compare_exchange).lower() == "hyperliquid" else compare_exchange
    if results_exchange:
        result_dirs = [d for d in result_dirs if d.parent.name == results_exchange]
    result_dirs = sorted(result_dirs, key=lambda p: p.stat().st_mtime, reverse=True)

    labels = []
    label_to_dir = {}
    for d in result_dirs[:200]:
        try:
            timestamp_name = d.name
            exchange_name = d.parent.name
            if str(compare_exchange).lower() == "hyperliquid" and exchange_name == "combined":
                exchange_name = "hyperliquid"
            label = f"{exchange_name}/{timestamp_name}"
        except Exception:
            label = str(d)
        if label in label_to_dir:
            label = f"{label} ({int(d.stat().st_mtime)})"
        labels.append(label)
        label_to_dir[label] = d

    has_backtest_results = bool(labels)
    if not has_backtest_results:
        extra = ""
        if str(compare_exchange).lower() not in ("combined", "hyperliquid") and has_any_combined:
            extra = " (You have combined results; set Exchange to 'combined' to compare them.)"
        shown_root = results_exchange if results_exchange else compare_exchange
        st.caption(f"No backtest results found under {results_root}/{shown_root}. Showing Live only.{extra}")

        if symbols_in_range:
            selected_symbols = st.multiselect(
                "Symbols/Coins (optional)",
                symbols_in_range,
                key=compare_symbols_key,
                help="If empty: compare total. If selected: compare per symbol/coin.",
            )
            if selected_symbols and len(selected_symbols) > 12:
                st.warning("Selecting many symbols may be slow.")

    selected_result_dir = None
    bt_cfg = {}
    bt_full_cfg = {}

    if has_backtest_results:
        res_col1, res_col_symbols, res_col_balance, res_col2 = st.columns([12, 10, 5, 2], vertical_alignment="bottom")
        with res_col1:
            selected_label = st.selectbox("Result", labels, key=compare_result_key, index=0)

        with res_col_symbols:
            if symbols_in_range:
                selected_symbols = st.multiselect(
                    "Symbols/Coins (optional)",
                    symbols_in_range,
                    key=compare_symbols_key,
                    help="If empty: compare total. If selected: compare per symbol/coin.",
                )
                if selected_symbols and len(selected_symbols) > 12:
                    st.warning("Selecting many symbols may be slow.")
            else:
                st.caption("")
        selected_result_dir = label_to_dir.get(selected_label)

        try:
            if selected_result_dir is not None:
                cfg_path = Path(selected_result_dir) / "config.json"
                if cfg_path.exists():
                    with open(cfg_path, "r", encoding="utf-8") as f:
                        cfg = json.load(f)
                    bt_full_cfg = cfg if isinstance(cfg, dict) else {}
                    bt_cfg = bt_full_cfg.get("backtest", {}) if isinstance(bt_full_cfg, dict) else {}
        except Exception:
            bt_cfg = {}
            bt_full_cfg = {}

        with res_col_balance:
            bt_starting_balance = bt_cfg.get("starting_balance", None)
            sb_widget_key = "v7_live_vs_backtest_bt_starting_balance"
            if bt_starting_balance is not None:
                try:
                    st.session_state[sb_widget_key] = float(bt_starting_balance)
                    st.number_input(
                        "Starting Balance",
                        step=0.01,
                        format="%.2f",
                        key=sb_widget_key,
                        disabled=True,
                        label_visibility="collapsed",
                    )
                except Exception:
                    st.caption(f"{bt_starting_balance}")
            else:
                st.caption("")

        with res_col2:
            if st.button(
                ":material/sync:",
                key="v7_live_vs_backtest_sync",
                help="Sync Start/End to selected backtest range",
                use_container_width=True,
            ):
                try:
                    sd = bt_cfg.get("start_date", None)
                    ed = bt_cfg.get("end_date", None)
                    if sd and ed:
                        st.session_state[compare_pending_start_key] = datetime.strptime(sd, "%Y-%m-%d").date()
                        st.session_state[compare_pending_end_key] = datetime.strptime(ed, "%Y-%m-%d").date()
                        st.rerun()
                    else:
                        st.warning("Selected backtest has no start_date/end_date in config.json")
                except Exception as e:
                    st.warning(f"Failed to sync date range: {e}")

        # Diagnostics: show market constraints (min_cost/min_qty/qty_step/etc.) sourced from PB7 caches.
        # Useful to explain rounding such as 0.008 ETH due to min_cost and qty_step.
        try:
            with st.expander("Market constraints (diagnostics)", expanded=False):
                bt_coin_sources = bt_cfg.get("coin_sources", {}) if isinstance(bt_cfg, dict) else {}

                # Use selected page start as "live start" reference.
                live_start_date = st.session_state.get(compare_start_key)
                if isinstance(live_start_date, datetime):
                    live_start_date = live_start_date.date()
                live_end_date = st.session_state.get(compare_end_key)
                if isinstance(live_end_date, datetime):
                    live_end_date = live_end_date.date()

                live_start_ms = None
                live_end_ms = None
                if isinstance(live_start_date, date):
                    live_start_dt = datetime(
                        live_start_date.year, live_start_date.month, live_start_date.day, tzinfo=timezone.utc
                    )
                    live_start_ms = int(live_start_dt.timestamp() * 1000)
                    if isinstance(live_end_date, date):
                        live_end_dt = datetime(
                            live_end_date.year, live_end_date.month, live_end_date.day, tzinfo=timezone.utc
                        ) + timedelta(days=1)
                        live_end_ms = int(live_end_dt.timestamp() * 1000) - 1
                    else:
                        # small window is enough to find a representative price
                        live_end_ms = live_start_ms + int(timedelta(days=7).total_seconds() * 1000)

                # Use selected backtest's start_date as "backtest start" reference.
                bt_start_date = None
                try:
                    sd = bt_cfg.get("start_date", None)
                    if sd:
                        bt_start_date = datetime.strptime(str(sd), "%Y-%m-%d").date()
                except Exception:
                    bt_start_date = None
                if bt_start_date is None and isinstance(live_start_date, date):
                    bt_start_date = live_start_date

                # Load fills.csv once (if available) to derive a start price.
                fills_df = None
                if selected_result_dir is not None:
                    try:
                        fills_path = Path(selected_result_dir) / "fills.csv"
                        if fills_path.exists():
                            fills_df = pd.read_csv(
                                fills_path,
                                usecols=["timestamp", "coin", "price"],
                                low_memory=False,
                            )
                            fills_df["timestamp"] = pd.to_datetime(fills_df["timestamp"], errors="coerce")
                    except Exception:
                        fills_df = None

                # Prefer user's currently selected symbols; else fall back to coin_sources keys.
                selected_symbols = st.session_state.get(compare_symbols_key) or []
                coins: list[str] = []
                if selected_symbols:
                    for sym in selected_symbols:
                        c = coin_from_symbol_code(_coerce_usdc_to_usdt(sym))
                        if c:
                            coins.append(str(c))
                elif isinstance(bt_coin_sources, dict) and bt_coin_sources:
                    coins = [str(k) for k in bt_coin_sources.keys()]

                # Last fallback: infer from the backtest config.json (live.approved_coins)
                if not coins and isinstance(bt_full_cfg, dict) and bt_full_cfg:
                    coins = _infer_coins_from_config_dict(bt_full_cfg)

                coins = sorted({c.strip().upper() for c in coins if str(c).strip()})
                if not coins:
                    st.info("Select a Symbol/Coin to show constraints.")
                else:
                    rows = []
                    for coin in coins:
                        src_ex = None
                        if isinstance(bt_coin_sources, dict):
                            src_ex = bt_coin_sources.get(coin) or bt_coin_sources.get(coin.upper())
                        if not src_ex:
                            src_ex = "binance" if str(compare_exchange).lower() == "hyperliquid" else str(compare_exchange)

                        src_ex_used = None
                        src_markets = None
                        src_sym, src_m = None, None
                        # For V7, Binance is typically futures; try binanceusdm first for constraints.
                        src_candidates = _pb7_markets_cache_candidates(str(src_ex), prefer_futures=str(src_ex).strip().lower() == "binance")
                        for ex_try in (src_candidates or [str(src_ex).strip().lower()]):
                            mk = _load_pb7_markets_cache(str(ex_try))
                            sym, m = _find_market_for_coin(mk, coin, quote="USDT") if mk else (None, None)
                            if m is not None:
                                src_ex_used = str(ex_try)
                                src_markets = mk
                                src_sym, src_m = sym, m
                                break
                        if src_ex_used is None:
                            src_ex_used = str(src_ex)
                        src_cst = _market_constraints_from_ccxt_market(src_m) if src_m is not None else {}

                        # Determine a representative start price.
                        bt_price = None
                        bt_price_ts = None
                        if fills_df is not None and not fills_df.empty:
                            try:
                                sdf = fills_df
                                sdf = sdf[sdf["coin"].astype(str).str.upper() == str(coin).upper()]
                                if bt_start_date is not None:
                                    bt_start_dt_naive = datetime(
                                        bt_start_date.year, bt_start_date.month, bt_start_date.day
                                    )
                                    sdf = sdf[sdf["timestamp"] >= bt_start_dt_naive]
                                sdf = sdf.dropna(subset=["timestamp", "price"]).sort_values("timestamp")
                                if not sdf.empty:
                                    r0 = sdf.iloc[0]
                                    bt_price = float(r0["price"])
                                    bt_price_ts = r0["timestamp"]
                            except Exception:
                                bt_price = None
                                bt_price_ts = None

                        live_price = None
                        live_price_ts = None
                        live_price_exchange = None
                        live_exec_symbol = None
                        try:
                            if live_start_ms is not None and live_end_ms is not None:
                                ex_rows = db.select_executions_rows(
                                    single_user,
                                    str(compare_exchange),
                                    int(live_start_ms),
                                    int(live_end_ms),
                                    coin=str(coin),
                                    limit=1,
                                    newest_first=False,
                                )
                                if ex_rows:
                                    # (timestamp, symbol, side, price, qty, fee, realized_pnl, order_id, trade_id)
                                    ts0, sym0, _, px0, *_ = ex_rows[0]
                                    live_price = float(px0) if px0 is not None else None
                                    live_price_exchange = str(compare_exchange)
                                    live_exec_symbol = sym0
                                    if ts0 is not None:
                                        live_price_ts = datetime.fromtimestamp(int(ts0) / 1000, tz=timezone.utc)
                                else:
                                    ex_rows2 = db.select_executions_rows_any_exchange(
                                        single_user,
                                        int(live_start_ms),
                                        int(live_end_ms),
                                        coin=str(coin),
                                        limit=1,
                                        newest_first=False,
                                    )
                                    if ex_rows2:
                                        # (timestamp, exchange, symbol, side, price, qty, fee, realized_pnl, order_id, trade_id)
                                        ts0, ex0, sym0, _, px0, *_ = ex_rows2[0]
                                        live_price = float(px0) if px0 is not None else None
                                        live_price_exchange = str(ex0)
                                        live_exec_symbol = sym0
                                        if ts0 is not None:
                                            live_price_ts = datetime.fromtimestamp(int(ts0) / 1000, tz=timezone.utc)
                        except Exception:
                            live_price = None
                            live_price_ts = None
                            live_price_exchange = None
                            live_exec_symbol = None

                        live_cst = {}
                        live_sym = None
                        live_markets = None
                        live_quote = _extract_quote_from_symbol(live_exec_symbol) if live_exec_symbol else None

                        live_exchange_for_constraints = None
                        live_exchange_inferred_from = None
                        if live_price_exchange:
                            live_exchange_for_constraints = str(live_price_exchange)
                            live_exchange_inferred_from = "executions"
                        else:
                            cfg_ex = _live_exchange_from_config_dict(bt_full_cfg)
                            if cfg_ex:
                                live_exchange_for_constraints = cfg_ex
                                live_exchange_inferred_from = "config"
                            else:
                                live_exchange_for_constraints = str(compare_exchange)
                                live_exchange_inferred_from = "dropdown"

                        live_exchange_used = None
                        live_any_cache_found = False
                        if live_exchange_for_constraints:
                            # If we saw a perp-like symbol (':USDT'), prefer futures cache.
                            prefer_futures = False
                            try:
                                prefer_futures = bool(live_exec_symbol and ":" in str(live_exec_symbol))
                            except Exception:
                                prefer_futures = False

                            live_candidates = _pb7_markets_cache_candidates(str(live_exchange_for_constraints), prefer_futures=prefer_futures)
                            quote_candidates = [q for q in [live_quote, "USDC", "USDT"] if q]

                            for ex_try in (live_candidates or [str(live_exchange_for_constraints)]):
                                mk = _load_pb7_markets_cache(str(ex_try))
                                if mk is None:
                                    continue
                                live_any_cache_found = True
                                for q in quote_candidates:
                                    live_sym, live_m = _find_market_for_coin(mk, coin, quote=q) if mk else (None, None)
                                    if live_m is not None:
                                        live_markets = mk
                                        live_exchange_used = str(ex_try)
                                        live_cst = _market_constraints_from_ccxt_market(live_m)
                                        break
                                if live_cst:
                                    break

                        bt_qty_eff, bt_coin_eff, bt_notional_eff = _effective_min_order_at_price(
                            price=bt_price,
                            min_cost=src_cst.get("min_cost"),
                            min_qty=src_cst.get("min_qty"),
                            qty_step=src_cst.get("qty_step"),
                            c_mult=src_cst.get("c_mult"),
                        )
                        live_qty_eff = live_coin_eff = live_notional_eff = None
                        if live_cst and live_price is not None:
                            live_qty_eff, live_coin_eff, live_notional_eff = _effective_min_order_at_price(
                                price=live_price,
                                min_cost=live_cst.get("min_cost"),
                                min_qty=live_cst.get("min_qty"),
                                qty_step=live_cst.get("qty_step"),
                                c_mult=live_cst.get("c_mult"),
                            )

                        note_parts = []
                        if src_m is None:
                            note_parts.append(
                                f"bt markets cache missing ({_pb7_markets_cache_path(str(src_ex))})" if src_markets is None else "bt symbol not found"
                            )
                        if live_exchange_for_constraints and not live_any_cache_found:
                            tried = ",".join(_pb7_markets_cache_candidates(str(live_exchange_for_constraints), prefer_futures=True) or [str(live_exchange_for_constraints)])
                            note_parts.append(f"live markets cache missing (tried {tried})")
                        if live_exchange_for_constraints and live_markets is not None and not live_cst:
                            tried = ",".join([q for q in [live_quote, "USDC", "USDT"] if q])
                            note_parts.append(f"live symbol not found (tried {tried})")
                        if live_cst and live_price is None:
                            note_parts.append("live_start_price missing (no executions in range)")
                        note = "; ".join([p for p in note_parts if p])

                        rows.append({
                            "Coin": coin,
                            "bt_source": str(src_ex_used),
                            "bt_symbol": src_sym,
                            "bt_min_cost": src_cst.get("min_cost"),
                            "bt_min_qty": src_cst.get("min_qty"),
                            "bt_qty_step": src_cst.get("qty_step"),
                            "bt_price_step": src_cst.get("price_step"),
                            "bt_maker_fee": src_cst.get("maker_fee"),
                            "bt_taker_fee": src_cst.get("taker_fee"),
                            "bt_c_mult": src_cst.get("c_mult"),
                            "bt_start_price": bt_price,
                            "bt_min_qty_eff": bt_qty_eff,
                            "bt_min_coin_eff": bt_coin_eff,
                            "bt_min_notional_eff": bt_notional_eff,
                            "live_start_price": live_price,
                            "live_exchange": live_exchange_used or live_exchange_for_constraints,
                            "live_exchange_src": live_exchange_inferred_from,
                            "live_symbol": live_sym,
                            "live_exec_symbol": live_exec_symbol,
                            "live_min_cost": live_cst.get("min_cost"),
                            "live_min_qty": live_cst.get("min_qty"),
                            "live_qty_step": live_cst.get("qty_step"),
                            "live_c_mult": live_cst.get("c_mult"),
                            "live_min_qty_eff": live_qty_eff,
                            "live_min_coin_eff": live_coin_eff,
                            "live_min_notional_eff": live_notional_eff,
                            "note": note,
                        })

                    df_c = pd.DataFrame(rows)

                    # Split into smaller tables to avoid horizontal scrolling.
                    tab_bt, tab_live, tab_eff, tab_notes = st.tabs(
                        [
                            "Backtest constraints",
                            "Live constraints",
                            "Effective mins",
                            "Notes",
                        ]
                    )

                    with tab_bt:
                        st.markdown(
                            "<div style='font-size:0.95rem; color: rgba(255,255,255,0.7);'>"
                            "Backtest constraints (<code>bt_*</code>) come from PB7 markets cache for the candle-source exchange (see <code>bt_source</code>)."
                            "</div>",
                            unsafe_allow_html=True,
                        )
                        cols = [
                            "Coin",
                            "bt_source",
                            "bt_symbol",
                            "bt_min_cost",
                            "bt_min_qty",
                            "bt_qty_step",
                            "bt_price_step",
                            "bt_c_mult",
                            "bt_maker_fee",
                            "bt_taker_fee",
                        ]
                        df_bt = df_c[[c for c in cols if c in df_c.columns]].copy()
                        st.dataframe(df_bt, use_container_width=True, hide_index=True)

                    with tab_live:
                        st.markdown(
                            "<div style='font-size:0.95rem; color: rgba(255,255,255,0.7);'>"
                            "Live constraints (<code>live_*</code>) come from PB7 markets cache for the live exchange (inferred from executions/config/dropdown)."
                            "</div>",
                            unsafe_allow_html=True,
                        )
                        cols = [
                            "Coin",
                            "live_exchange",
                            "live_exchange_src",
                            "live_symbol",
                            "live_exec_symbol",
                            "live_min_cost",
                            "live_min_qty",
                            "live_qty_step",
                            "live_c_mult",
                        ]
                        df_live = df_c[[c for c in cols if c in df_c.columns]].copy()
                        st.dataframe(df_live, use_container_width=True, hide_index=True)

                    with tab_eff:
                        st.markdown(
                            "<div style='font-size:0.95rem; color: rgba(255,255,255,0.7);'>"
                            "Effective mins: combines constraints with a start-price. <code>bt_*</code> uses backtest start price from <code>fills.csv</code>; <code>live_*</code> uses first execution price after Start (if any)."
                            "</div>",
                            unsafe_allow_html=True,
                        )
                        cols = [
                            "Coin",
                            "bt_start_price",
                            "bt_min_qty_eff",
                            "bt_min_coin_eff",
                            "bt_min_notional_eff",
                            "live_start_price",
                            "live_min_qty_eff",
                            "live_min_coin_eff",
                            "live_min_notional_eff",
                        ]
                        df_eff = df_c[[c for c in cols if c in df_c.columns]].copy()
                        st.dataframe(df_eff, use_container_width=True, hide_index=True)
                        st.markdown(
                            "<div style='font-size:0.95rem; color: rgba(255,255,255,0.7);'>"
                            "Effective minimum order at start-price is computed as: "
                            "<code>min_qty_eff_units ≈ max(min_qty, ceil(min_cost / (price * c_mult) / qty_step) * qty_step)</code>. "
                            "Then <code>min_coin_eff = min_qty_eff_units * c_mult</code> and <code>min_notional_eff = min_coin_eff * price</code>."
                            "</div>",
                            unsafe_allow_html=True,
                        )

                    with tab_notes:
                        st.markdown(
                            "<div style='font-size:0.95rem; color: rgba(255,255,255,0.7);'>"
                            "Notes show missing caches/symbols or why live effective mins are empty (e.g., no executions in selected range)."
                            "</div>",
                            unsafe_allow_html=True,
                        )
                        cols = ["Coin", "bt_source", "bt_symbol", "live_exchange", "live_symbol", "note"]
                        df_n = df_c[[c for c in cols if c in df_c.columns]].copy()
                        st.dataframe(df_n, use_container_width=True, hide_index=True)
        except Exception:
            pass

    # Build curves
    compare_start_date = st.session_state.get(compare_start_key)
    compare_end_date = st.session_state.get(compare_end_key)
    if isinstance(compare_start_date, datetime):
        compare_start_date = compare_start_date.date()
    if isinstance(compare_end_date, datetime):
        compare_end_date = compare_end_date.date()
    if not isinstance(compare_start_date, date) or not isinstance(compare_end_date, date):
        return
    if compare_start_date > compare_end_date:
        st.error("Start must be <= End")
        return

    start_dt = datetime(compare_start_date.year, compare_start_date.month, compare_start_date.day, tzinfo=timezone.utc)
    end_dt = datetime(compare_end_date.year, compare_end_date.month, compare_end_date.day, tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int((end_dt + timedelta(days=1)).timestamp() * 1000) - 1
    all_days = pd.date_range(compare_start_date, compare_end_date, freq="D")

    live_daily_map: dict[str, pd.Series] = {}
    bt_daily_map: dict[str, pd.Series] = {}

    selected_symbols = st.session_state.get(compare_symbols_key, []) or []
    if selected_symbols:
        live_cum_map = {}
        for sym in selected_symbols:
            rows = db.select_pnl_symbol(single_user, sym, start_ms, end_ms)
            sdf = pd.DataFrame(rows, columns=["Date", "Income"])
            if not sdf.empty:
                sdf["Date"] = pd.to_datetime(sdf["Date"], format="%Y-%m-%d")
                sdf = sdf.set_index("Date").sort_index()
                daily = sdf["Income"].reindex(all_days, fill_value=0.0)
            else:
                daily = pd.Series(0.0, index=all_days)
            live_daily_map[sym] = daily
            live_cum_map[sym] = daily.cumsum()
    else:
        pnl_rows = db.select_pnl([single_user], start_ms, end_ms)
        live_df = pd.DataFrame(pnl_rows, columns=["Date", "Income"])
        if not live_df.empty:
            live_df["Date"] = pd.to_datetime(live_df["Date"], format="%Y-%m-%d")
            live_df = live_df.set_index("Date").sort_index()
        live_daily = live_df["Income"].reindex(all_days, fill_value=0.0) if not live_df.empty else pd.Series(0.0, index=all_days)
        live_daily_map = {"Total": live_daily}
        live_cum_map = {"Total": live_daily.cumsum()}

    bt_cum_map = {}
    fills = None
    if has_backtest_results and selected_result_dir is not None:
        fills_path = Path(selected_result_dir) / "fills.csv"
        if not fills_path.exists():
            fills_path = Path(selected_result_dir) / "fills.csv.gz"
        config_path = Path(selected_result_dir) / "config.json"

        bt_cum_map = {k: pd.Series(0.0, index=all_days) for k in live_cum_map.keys()}
        bt_daily_map = {k: pd.Series(0.0, index=all_days) for k in live_cum_map.keys()}

    if has_backtest_results and selected_result_dir is not None and fills_path.exists() and config_path.exists():
        try:
            fills = pd.read_csv(fills_path)
            if "minute" in fills.columns:
                cfg = {}
                try:
                    with open(config_path, "r", encoding="utf-8") as f:
                        cfg = json.load(f)
                except Exception:
                    cfg = {}

                end_date_str = None
                try:
                    end_date_str = cfg.get("backtest", {}).get("end_date", None)
                except Exception:
                    end_date_str = None
                if not end_date_str:
                    end_date_str = compare_end_date.strftime("%Y-%m-%d")

                # Prefer explicit per-fill timestamps when available; it's more accurate
                # than reconstructing from minute/end_date (which can be offset if
                # end_date differs from the run's actual backtest window).
                if "timestamp" in fills.columns:
                    try:
                        t = pd.to_datetime(fills["timestamp"], utc=True, errors="coerce")
                        if t.notna().any():
                            fills["time"] = t
                        else:
                            raise ValueError("timestamp parse produced all-NaT")
                    except Exception:
                        end_ts = int(datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
                        last_minute = float(fills["minute"].max())
                        start_ts = end_ts - int(last_minute * 60)
                        fills["time"] = pd.to_datetime(start_ts + (fills["minute"].astype(float) * 60), unit="s", utc=True)
                else:
                    end_ts = int(datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
                    last_minute = float(fills["minute"].max())
                    start_ts = end_ts - int(last_minute * 60)
                    fills["time"] = pd.to_datetime(start_ts + (fills["minute"].astype(float) * 60), unit="s", utc=True)

                pnl_col = fills["pnl"] if "pnl" in fills.columns else 0.0
                fee_col = fills["fee_paid"] if "fee_paid" in fills.columns else 0.0
                fills["net"] = pnl_col + fee_col

                # If qty is a direction marker (+/-1) in some PB7 outputs, we can derive
                # effective trade size from position size deltas.
                try:
                    if "psize" in fills.columns:
                        if "coin" in fills.columns:
                            g = fills["coin"].astype(str).str.upper()
                            fills["psize_delta"] = fills.groupby(g)["psize"].diff()
                        else:
                            fills["psize_delta"] = fills["psize"].diff()
                except Exception:
                    pass

                # Estimate contract size (base-coin per 1.0 psize unit) when possible.
                # For many derivatives venues, qty/psize are in "contracts" where 1 contract
                # equals e.g. 0.01 ETH. We can infer this from wallet_exposure:
                # wallet_exposure ~= abs(psize * contract_size * pprice) / usd_total_balance
                try:
                    fills["contract_size"] = pd.NA
                    has_needed = all(c in fills.columns for c in ["wallet_exposure", "usd_total_balance", "psize", "pprice"])
                    if has_needed:
                        we = pd.to_numeric(fills["wallet_exposure"], errors="coerce").abs()
                        bal = pd.to_numeric(fills["usd_total_balance"], errors="coerce").abs()
                        psize = pd.to_numeric(fills["psize"], errors="coerce").abs()
                        pprice = pd.to_numeric(fills["pprice"], errors="coerce").abs()
                        denom = (psize * pprice)
                        est = (we * bal) / denom
                        est = est.where((we > 0) & (bal > 0) & (psize > 0) & (pprice > 0))
                        est = est.replace([float("inf"), float("-inf")], pd.NA)

                        if "coin" in fills.columns:
                            coin_key = fills["coin"].astype(str).str.upper()
                            tmp = pd.DataFrame({"coin": coin_key, "est": est})
                            contract_map = tmp.groupby("coin")["est"].median(numeric_only=True)
                            fills["contract_size"] = coin_key.map(contract_map)
                        else:
                            try:
                                fills["contract_size"] = float(pd.to_numeric(est, errors="coerce").median())
                            except Exception:
                                fills["contract_size"] = pd.NA

                        cs = pd.to_numeric(fills["contract_size"], errors="coerce")
                        if "qty" in fills.columns:
                            fills["qty_coin"] = pd.to_numeric(fills["qty"], errors="coerce") * cs
                        if "psize" in fills.columns:
                            fills["psize_coin"] = pd.to_numeric(fills["psize"], errors="coerce") * cs
                        if "psize_delta" in fills.columns:
                            fills["psize_delta_coin"] = pd.to_numeric(fills["psize_delta"], errors="coerce") * cs
                except Exception:
                    pass

                try:
                    fills["date"] = fills["time"].dt.tz_convert("UTC").dt.floor("D").dt.tz_localize(None)
                except Exception:
                    fills["date"] = pd.NaT

                if selected_symbols:
                    wanted = {sym: coin_from_symbol_code(sym) for sym in selected_symbols}
                    for sym, coin in wanted.items():
                        if "coin" in fills.columns:
                            sub = fills[fills["coin"].astype(str).str.upper() == coin]
                        else:
                            sub = fills.iloc[0:0]
                        if sub.empty:
                            bt_daily_map[sym] = pd.Series(0.0, index=all_days)
                            bt_cum_map[sym] = pd.Series(0.0, index=all_days)
                            continue
                        bt_daily = sub.groupby(sub["time"].dt.tz_convert("UTC").dt.floor("D"))["net"].sum()
                        bt_daily.index = bt_daily.index.tz_localize(None)
                        bt_daily = bt_daily.reindex(all_days, fill_value=0.0)
                        bt_daily_map[sym] = bt_daily
                        bt_cum_map[sym] = bt_daily.cumsum()
                else:
                    bt_daily = fills.groupby(fills["time"].dt.tz_convert("UTC").dt.floor("D"))["net"].sum()
                    bt_daily.index = bt_daily.index.tz_localize(None)
                    bt_daily = bt_daily.reindex(all_days, fill_value=0.0)
                    bt_daily_map["Total"] = bt_daily
                    bt_cum_map["Total"] = bt_daily.cumsum()
            else:
                st.warning("Backtest fills.csv has no 'minute' column; cannot compute income curve.")
        except Exception as e:
            st.warning(f"Failed to load backtest fills: {e}")
    elif has_backtest_results and selected_result_dir is not None:
        st.caption("Selected result has no fills/config yet (still running?).")

    # Keep the page clean: starting balance is shown as input above.

    plot_height_key = "v7_live_vs_backtest_plot_height"
    if plot_height_key not in st.session_state:
        st.session_state[plot_height_key] = 800

    interaction_key = "v7_live_vs_backtest_plot_interaction"  # legacy string state: "Select range"/"Zoom"
    select_toggle_key = "v7_live_vs_backtest_select_range_enabled"  # visible tool-like control
    if interaction_key not in st.session_state:
        st.session_state[interaction_key] = "Select range"
    if select_toggle_key not in st.session_state:
        st.session_state[select_toggle_key] = (st.session_state.get(interaction_key) == "Select range")

    hcol1, hcol2 = st.columns([3, 1], vertical_alignment="center")
    with hcol1:
        st.slider(
            "Chart height",
            min_value=350,
            max_value=1400,
            step=50,
            key=plot_height_key,
            help="Adjust chart height (Streamlit charts can't be drag-resized like tables).",
        )
    with hcol2:
        st.toggle(
            "Select range",
            key=select_toggle_key,
            help="When enabled: drag a box in the chart to sync Start/End to that time window.",
        )

    # Keep string-based interaction_key in sync for downstream logic
    st.session_state[interaction_key] = "Select range" if st.session_state.get(select_toggle_key) else "Zoom"

    rows = []
    for sym, s in live_cum_map.items():
        for d, v in zip(all_days, s.values):
            rows.append({"Date": d, "Symbol": sym, "Source": "Live", "Income": float(v)})
    if bt_cum_map:
        for sym, s in bt_cum_map.items():
            for d, v in zip(all_days, s.values):
                rows.append({"Date": d, "Symbol": sym, "Source": "Backtest", "Income": float(v)})

    plot_long = pd.DataFrame(rows)
    fig = px.line(plot_long, x="Date", y="Income", color="Symbol", line_dash="Source", hover_data={"Income": ":.2f"})
    fig.update_layout(height=int(st.session_state.get(plot_height_key) or 800))

    interaction_mode = str(st.session_state.get(interaction_key) or "Select range")

    plotly_config = {
        "displayModeBar": True,
        "displaylogo": False,
        # Try to ensure selection tools are available in the modebar
        "modeBarButtonsToAdd": ["select2d", "lasso2d"],
        "modeBarButtonsToRemove": [],
        # Optional: allow mousewheel zoom
        "scrollZoom": True,
    }

    # Newer plotly versions support layout-level modebar add/remove.
    # If supported, this is more reliable than config-only.
    try:
        fig.update_layout(modebar_add=["select2d", "lasso2d"])
    except Exception:
        pass

    # Streamlit doesn't expose Plotly zoom/pan (relayout) events, but it can expose
    # box/lasso selection in newer Streamlit versions. Use that as a practical
    # way to sync Start/End from an interactively selected range.
    selection_state = None
    if interaction_mode == "Select range":
        try:
            fig.update_layout(dragmode="select")
            selection_state = st.plotly_chart(
                fig,
                use_container_width=True,
                on_select="rerun",
                selection_mode=["box"],
                key="v7_live_vs_backtest_plot",
                config=plotly_config,
            )
        except TypeError:
            # Older Streamlit: fall back to regular Plotly chart with explicit modebar config.
            try:
                st.plotly_chart(fig, use_container_width=True, config=plotly_config)
            except Exception:
                st.plotly_chart(fig, use_container_width=True)
        except Exception:
            try:
                st.plotly_chart(fig, use_container_width=True, config=plotly_config)
            except Exception:
                st.plotly_chart(fig, use_container_width=True)

        # Apply selection -> date range sync
        try:
            sel = getattr(selection_state, "selection", None)
            if sel is None and isinstance(selection_state, dict):
                sel = selection_state.get("selection")
            points = sel.get("points", []) if isinstance(sel, dict) else []

            # Prefer box selection range (more reliable than points on line charts)
            x_min = None
            x_max = None
            if isinstance(sel, dict):
                box = sel.get("box")
                if isinstance(box, list) and box:
                    b0 = box[0] if isinstance(box[0], dict) else None
                    if isinstance(b0, dict):
                        xr = b0.get("x")
                        if isinstance(xr, (list, tuple)) and len(xr) == 2:
                            x_min = pd.to_datetime(xr[0]).date()
                            x_max = pd.to_datetime(xr[1]).date()
                        else:
                            # Some variants expose x0/x1
                            if b0.get("x0") is not None and b0.get("x1") is not None:
                                x_min = pd.to_datetime(b0.get("x0")).date()
                                x_max = pd.to_datetime(b0.get("x1")).date()

            if x_min is None or x_max is None:
                xs = []
                for p in points:
                    x = p.get("x") if isinstance(p, dict) else None
                    if x is not None:
                        xs.append(x)
                if xs:
                    x_min = pd.to_datetime(min(xs)).date()
                    x_max = pd.to_datetime(max(xs)).date()

            # Store last signature to avoid repeated reruns on same selection
            sel_sig_key = "v7_live_vs_backtest_plot_selection_sig"

            if x_min is not None and x_max is not None:
                if x_min > x_max:
                    x_min, x_max = x_max, x_min
                # Clamp to picker bounds
                x_min = max(min_picker_date, min(x_min, max_picker_date))
                x_max = max(min_picker_date, min(x_max, max_picker_date))
                sig = f"{x_min.isoformat()}|{x_max.isoformat()}"
                if st.session_state.get(sel_sig_key) != sig:
                    st.session_state[sel_sig_key] = sig
                    # Set pending sync (applied at top before date widgets)
                    st.session_state[compare_pending_start_key] = x_min
                    st.session_state[compare_pending_end_key] = x_max
                    st.rerun()
        except Exception:
            pass
    else:
        # Normal zoom/pan usage. (You can also switch back to Select range above.)
        try:
            fig.update_layout(dragmode="zoom")
        except Exception:
            pass
        try:
            st.plotly_chart(fig, use_container_width=True, config=plotly_config)
        except Exception:
            st.plotly_chart(fig, use_container_width=True)

    # If Select range is turned off, reset the date window back to live data range.
    try:
        prev_key = "v7_live_vs_backtest_select_range_prev"
        prev = st.session_state.get(prev_key)
        cur = bool(st.session_state.get(select_toggle_key))
        if prev is None:
            st.session_state[prev_key] = cur
        else:
            if prev and not cur:
                # Clear last selection signature so future selections trigger
                st.session_state.pop("v7_live_vs_backtest_plot_selection_sig", None)

                ds = max(min_picker_date, min(default_start_date, max_picker_date))
                de = max(min_picker_date, min(default_end_date, max_picker_date))
                st.session_state[compare_pending_start_key] = ds
                st.session_state[compare_pending_end_key] = de
                st.session_state[prev_key] = cur
                st.rerun()
            else:
                st.session_state[prev_key] = cur
    except Exception:
        pass

    # --- Diagnostics / Detail Compare (keep main plot unchanged) ---
    with st.expander("Details / Diagnostics", expanded=False):
        st.caption("Find the first deviations and inspect possible causes (income rows vs backtest fills vs live executions).")

        scope_options = list(live_daily_map.keys())
        if len(scope_options) == 1:
            scope = scope_options[0]
        else:
            scope = st.selectbox(
                "Scope",
                options=scope_options,
                index=0,
                key="v7_live_vs_backtest_diag_scope",
            )

        live_daily = live_daily_map.get(scope, pd.Series(0.0, index=all_days))
        bt_daily = bt_daily_map.get(scope, pd.Series(0.0, index=all_days)) if bt_daily_map else pd.Series(0.0, index=all_days)
        diff_daily = (live_daily - bt_daily).astype(float)

        # Keep UI minimal: use a small fixed threshold to flag deviations.
        threshold = 0.01

        comp_df = pd.DataFrame({
            "Date": all_days,
            "Live": live_daily.values,
            "Backtest": bt_daily.values,
            "Diff": diff_daily.values,
        })
        comp_df["AbsDiff"] = comp_df["Diff"].abs()

        # Add counts from history (income) for context
        try:
            hist_symbol = None if scope == 'Total' else scope
            daily_rows = db.select_history_daily(single_user, start_ms, end_ms, symbol=hist_symbol)
            daily_map = {r[0]: {"IncomeRows": int(r[2] or 0)} for r in (daily_rows or []) if r and r[0]}
            comp_df["IncomeRows"] = [daily_map.get(d.strftime('%Y-%m-%d'), {}).get("IncomeRows", 0) for d in comp_df["Date"]]
        except Exception:
            comp_df["IncomeRows"] = 0

        # Add backtest fill counts for the selected scope (if fills are available)
        bt_fill_counts = {}
        if isinstance(fills, pd.DataFrame) and not fills.empty and "date" in fills.columns:
            try:
                if scope == 'Total':
                    fsub = fills
                else:
                    coin = coin_from_symbol_code(scope)
                    if "coin" in fills.columns:
                        fsub = fills[fills["coin"].astype(str).str.upper() == str(coin).upper()]
                    else:
                        fsub = fills.iloc[0:0]
                if not fsub.empty:
                    bt_fill_counts = fsub.groupby("date").size().to_dict()
            except Exception:
                bt_fill_counts = {}
        comp_df["BTFills"] = [int(bt_fill_counts.get(d.to_pydatetime(), 0) or 0) for d in comp_df["Date"]]

        # Add live executions aggregates if we have a matching user exchange
        exec_daily_map = {}
        try:
            uobj = users.find_user(single_user)
            live_exchange = getattr(uobj, 'exchange', None)
            if live_exchange:
                exec_coin = None if scope == 'Total' else coin_from_symbol_code(scope)
                erows = db.select_executions_daily(single_user, str(live_exchange), start_ms, end_ms, coin=exec_coin)
                for r in (erows or []):
                    if not r or not r[0]:
                        continue
                    exec_daily_map[str(r[0])] = {
                        "ExecTrades": int(r[1] or 0),
                        "ExecFee": float(r[2] or 0.0),
                        "ExecRealizedPnL": float(r[3] or 0.0),
                    }
        except Exception:
            exec_daily_map = {}

        comp_df["ExecTrades"] = [exec_daily_map.get(d.strftime('%Y-%m-%d'), {}).get("ExecTrades", 0) for d in comp_df["Date"]]
        comp_df["ExecFee"] = [exec_daily_map.get(d.strftime('%Y-%m-%d'), {}).get("ExecFee", 0.0) for d in comp_df["Date"]]
        comp_df["ExecRealizedPnL"] = [exec_daily_map.get(d.strftime('%Y-%m-%d'), {}).get("ExecRealizedPnL", 0.0) for d in comp_df["Date"]]
        # Note: in trades DB, fee is stored as a positive cost.
        comp_df["ExecNet"] = comp_df["ExecRealizedPnL"] - comp_df["ExecFee"]

        # Quick summary: where does the gap come from?
        # Most commonly: Backtest has fills on days where Live has none (due to fill-model differences,
        # downtime, post-only orders not filling, etc.).
        try:
            bt_has = pd.to_numeric(comp_df.get("BTFills", 0), errors="coerce").fillna(0).astype(float) > 0
            live_has = (
                pd.to_numeric(comp_df.get("IncomeRows", 0), errors="coerce").fillna(0).astype(float) > 0
            ) | (
                pd.to_numeric(comp_df.get("ExecTrades", 0), errors="coerce").fillna(0).astype(float) > 0
            )

            comp_df["BT_minus_Live"] = (pd.to_numeric(comp_df.get("Backtest", 0.0), errors="coerce").fillna(0.0) -
                                       pd.to_numeric(comp_df.get("Live", 0.0), errors="coerce").fillna(0.0))

            cat = pd.Series("No trades", index=comp_df.index)
            cat[bt_has & ~live_has] = "Backtest-only (fills; no live trades)"
            cat[~bt_has & live_has] = "Live-only (trades; no backtest fills)"
            cat[bt_has & live_has] = "Both traded"
            comp_df["Category"] = cat

            sum_df = comp_df.groupby("Category", dropna=False).agg(
                Days=("Date", "count"),
                LiveSum=("Live", "sum"),
                BacktestSum=("Backtest", "sum"),
                GapSum=("BT_minus_Live", "sum"),
                IncomeRows=("IncomeRows", "sum"),
                ExecTrades=("ExecTrades", "sum"),
                BTFills=("BTFills", "sum"),
            ).reset_index()

            # Show as a small overview table (keeps the main plot unchanged)
            st.dataframe(
                sum_df.sort_values(["GapSum", "Category"], ascending=[False, True]),
                use_container_width=True,
                hide_index=True,
            )

            # Helpful nudge: show the strongest BT-only day (if any)
            bt_only = comp_df[bt_has & ~live_has].copy()
            if not bt_only.empty:
                top = bt_only.sort_values("BT_minus_Live", ascending=False).iloc[0]
                try:
                    top_d = top["Date"].date()
                except Exception:
                    top_d = None
                if top_d is not None:
                    st.caption(
                        f"Largest Backtest-only gap day: {top_d.isoformat()} (Backtest-Live = {float(top['BT_minus_Live']):.4f})"
                    )
        except Exception:
            pass

        show_top = 30

        deviating = comp_df[comp_df["AbsDiff"] >= float(threshold)].copy()
        if deviating.empty:
            st.info("No deviations within the current threshold in this range.")
            st.dataframe(comp_df.sort_values("Date"), use_container_width=True, hide_index=True)
            return

        st.dataframe(
            deviating.sort_values(["AbsDiff", "Date"], ascending=[False, True]).head(int(show_top)),
            use_container_width=True,
            hide_index=True,
        )

        # Pick first deviation chronologically by default
        first_dev_date = deviating.sort_values("Date").iloc[0]["Date"]
        dev_dates = list(deviating.sort_values("Date")["Date"].dt.date)

        # Allow fast +/-1 day navigation (faster than searching the dropdown).
        # We expose the full date range in the selector and mark deviation days.
        day_key = "v7_live_vs_backtest_diag_day"
        # Filter out days with neither backtest fills nor live activity.
        # Prefer count columns when available (more reliable than net=0).
        try:
            cdf_sorted = comp_df.sort_values("Date").copy()
            bt_fills_s = pd.to_numeric(cdf_sorted.get("BTFills", 0), errors="coerce").fillna(0)
            exec_trades_s = pd.to_numeric(cdf_sorted.get("ExecTrades", 0), errors="coerce").fillna(0)
            income_rows_s = pd.to_numeric(cdf_sorted.get("IncomeRows", 0), errors="coerce").fillna(0)
            active_mask = (bt_fills_s > 0) | (exec_trades_s > 0) | (income_rows_s > 0)
            date_series = cdf_sorted.loc[active_mask, "Date"]
            all_dates = list(date_series.dt.date)
        except Exception:
            all_dates = list(comp_df.sort_values("Date")["Date"].dt.date)

        # Ensure uniqueness while preserving chronological order
        try:
            seen = set()
            all_dates = [d for d in all_dates if not (d in seen or seen.add(d))]
        except Exception:
            pass
        dev_set = set(dev_dates)
        default_day = first_dev_date.date() if hasattr(first_dev_date, "date") else (all_dates[0] if all_dates else date.today())
        if not all_dates:
            st.info("No days available in the selected range.")
            return

        if day_key not in st.session_state or st.session_state.get(day_key) not in all_dates:
            st.session_state[day_key] = default_day if default_day in all_dates else all_dates[0]

        cur_idx = 0
        try:
            cur_idx = all_dates.index(st.session_state.get(day_key))
        except Exception:
            cur_idx = 0
            st.session_state[day_key] = all_dates[0]

        nav_prev, nav_sel, nav_next = st.columns([0.15, 0.7, 0.15], vertical_alignment="bottom")

        # IMPORTANT: handle button clicks *before* instantiating the selectbox with the same key.
        # Otherwise Streamlit will raise:
        # "st.session_state.<key> cannot be modified after the widget with key <key> is instantiated."
        with nav_prev:
            clicked_prev = st.button(
                "◀︎ -1d",
                key="v7_live_vs_backtest_diag_day_prev",
                disabled=cur_idx <= 0,
                use_container_width=True,
                help="Go to previous day",
            )

        with nav_next:
            clicked_next = st.button(
                "+1d ▶︎",
                key="v7_live_vs_backtest_diag_day_next",
                disabled=cur_idx >= len(all_dates) - 1,
                use_container_width=True,
                help="Go to next day",
            )

        if clicked_prev:
            st.session_state[day_key] = all_dates[max(0, cur_idx - 1)]
            st.rerun()
        if clicked_next:
            st.session_state[day_key] = all_dates[min(len(all_dates) - 1, cur_idx + 1)]
            st.rerun()

        with nav_sel:
            st.selectbox(
                "Inspect day",
                options=all_dates,
                index=cur_idx,
                key=day_key,
                format_func=lambda d: f"{d.isoformat()} (deviation)" if d in dev_set else d.isoformat(),
            )

        selected_day = st.session_state.get(day_key)
        if selected_day not in all_dates:
            selected_day = all_dates[0]
            st.session_state[day_key] = selected_day

        day_start_dt = datetime(selected_day.year, selected_day.month, selected_day.day, tzinfo=timezone.utc)
        day_start_ms = int(day_start_dt.timestamp() * 1000)
        day_end_ms = int((day_start_dt + timedelta(days=1)).timestamp() * 1000) - 1

        # Quick summary for the selected day
        try:
            row = comp_df[comp_df["Date"].dt.date == selected_day].iloc[0]
            st.markdown(
                f"**{selected_day.isoformat()}** — Live: {row['Live']:.4f} | Backtest: {row['Backtest']:.4f} | Diff: {row['Diff']:.4f}"
            )
        except Exception:
            pass

        tab_income, tab_bt, tab_exec, tab_match = st.tabs(["Live income rows", "Backtest fills", "Live executions", "BT vs Live (matched)"])

        with tab_income:
            hist_symbol = None if scope == 'Total' else scope
            rows = db.select_history_rows(single_user, day_start_ms, day_end_ms, symbol=hist_symbol, limit=5000, newest_first=False)
            hdf = pd.DataFrame(rows, columns=["timestamp", "symbol", "income", "uniqueid"]) if rows else pd.DataFrame(columns=["timestamp", "symbol", "income", "uniqueid"])
            if not hdf.empty:
                hdf["time"] = pd.to_datetime(hdf["timestamp"].astype('int64'), unit='ms', utc=True)
                hdf["kind"] = hdf["uniqueid"].apply(_classify_uniqueid)
                ksum = hdf.groupby("kind").agg(rows=("uniqueid", "count"), income_sum=("income", "sum")).reset_index()
                st.dataframe(ksum.sort_values("income_sum", ascending=False), use_container_width=True, hide_index=True)
                st.dataframe(hdf[["time", "symbol", "income", "kind", "uniqueid"]], use_container_width=True, hide_index=True)
            else:
                st.info("No live income rows for this day (in this scope).")

        with tab_bt:
            if isinstance(fills, pd.DataFrame) and not fills.empty and "date" in fills.columns:
                try:
                    if scope == 'Total':
                        fsub = fills
                    else:
                        coin = coin_from_symbol_code(scope)
                        if "coin" in fills.columns:
                            fsub = fills[fills["coin"].astype(str).str.upper() == str(coin).upper()]
                        else:
                            fsub = fills.iloc[0:0]
                    fday = fsub[fsub["date"].dt.date == selected_day] if not fsub.empty else fsub
                except Exception:
                    fday = fills.iloc[0:0]

                if not fday.empty:
                    cols = [c for c in ["time", "coin", "type", "qty", "qty_coin", "contract_size", "price", "pnl", "fee_paid", "net"] if c in fday.columns]
                    st.dataframe(fday[cols].sort_values("time"), use_container_width=True, hide_index=True)
                else:
                    st.info("No backtest fills for this day (in this scope).")
            else:
                st.info("No backtest fills loaded/available (selected result missing fills.csv?).")

        with tab_exec:
            try:
                uobj = users.find_user(single_user)
                live_exchange = getattr(uobj, 'exchange', None)
                if not live_exchange:
                    st.info("No live exchange found for this user.")
                else:
                    exec_coin = None if scope == 'Total' else coin_from_symbol_code(scope)
                    erows = db.select_executions_rows(single_user, str(live_exchange), day_start_ms, day_end_ms, coin=exec_coin, limit=5000, newest_first=False)
                    edf = pd.DataFrame(erows, columns=["timestamp", "symbol", "side", "price", "qty", "fee", "realized_pnl", "order_id", "trade_id"]) if erows else pd.DataFrame(
                        columns=["timestamp", "symbol", "side", "price", "qty", "fee", "realized_pnl", "order_id", "trade_id"]
                    )
                    if not edf.empty:
                        edf["time"] = pd.to_datetime(edf["timestamp"].astype('int64'), unit='ms', utc=True)
                        # fee is stored as a positive cost in executions
                        edf["net"] = (edf["realized_pnl"].fillna(0.0) - edf["fee"].fillna(0.0)).astype(float)
                        st.dataframe(edf[["time", "symbol", "side", "qty", "price", "fee", "realized_pnl", "net", "trade_id", "order_id"]], use_container_width=True, hide_index=True)
                    else:
                        # Minimal, user-facing hint: show earliest/latest execution we have.
                        all_min = all_max = None
                        try:
                            with db._connect_trades() as conn:
                                cur = conn.cursor()
                                cur.execute(
                                    "SELECT MIN(timestamp), MAX(timestamp) FROM executions WHERE user=? AND exchange=?",
                                    (single_user, str(live_exchange)),
                                )
                                all_min, all_max = cur.fetchone() or (None, None)
                        except Exception:
                            all_min, all_max = None, None

                        st.info("No live executions for this day.")
                        if all_min is not None and all_max is not None:
                            min_dt = datetime.fromtimestamp(int(all_min) / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
                            max_dt = datetime.fromtimestamp(int(all_max) / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
                            sel_dt = datetime.fromtimestamp(int(day_start_ms) / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
                            reason = ""
                            if str(live_exchange).strip().lower() == "binance":
                                reason = " (Binance: initial backfill capped to last 180 days due to API limits)"
                            if str(live_exchange).strip().lower() == "bitget":
                                reason = " (Bitget: initial backfill capped to last 90 days due to API limits)"
                            if int(day_start_ms) < int(all_min):
                                st.caption(f"Executions available from {min_dt} to {max_dt} (UTC). Selected day {sel_dt} is earlier than the first stored execution.{reason}")
                            else:
                                st.caption(f"Executions available from {min_dt} to {max_dt} (UTC).{reason}")
            except Exception as e:
                st.info(f"Executions view unavailable: {e}")

        with tab_match:
            tol_s = st.number_input(
                "Match tolerance (seconds)",
                min_value=0,
                value=120,
                step=30,
                key="v7_live_vs_backtest_diag_match_tol_s",
                help="Greedy nearest-time matching between backtest fills and live executions.",
            )

            # Keep UI minimal: enforce sensible defaults.
            require_side = True
            agg_partials = True
            candidates_unmatched_only = True

            # One row: 4 toggles next to each other
            tcol1, tcol2, tcol3, tcol4 = st.columns(4)
            with tcol1:
                show_unmatched_exec = st.toggle(
                    "Show unmatched execution rows",
                    value=False,
                    key="v7_live_vs_backtest_show_unmatched_exec_rows",
                    help="Adds extra rows for executions that were not matched to any backtest fill (bt_* empty).",
                )
            with tcol2:
                suppress_exec_rows_seen_as_candidates = st.toggle(
                    "Hide executions already shown as candidates",
                    value=True,
                    key="v7_live_vs_backtest_hide_candidate_exec_rows",
                    help=(
                        "Only applies when 'Show unmatched execution rows' is enabled. "
                        "When enabled, executions referenced in candidate columns are not added again as extra rows."
                    ),
                    disabled=not bool(show_unmatched_exec),
                )
            with tcol3:
                show_candidate_cols = st.toggle(
                    "Show candidate columns",
                    value=False,
                    key="v7_live_vs_backtest_show_candidate_cols",
                    help="Shows the nearest candidate execution even if it wasn't matched (useful for debugging).",
                )
            with tcol4:
                show_debug_cols = st.toggle(
                    "Show debug columns",
                    value=False,
                    key="v7_live_vs_backtest_show_debug_cols",
                    help="Shows extra backtest sizing fields (psize_delta/contract_size/qty breakdown).",
                )

            # Build the same per-day subsets as the other tabs
            bt_day_df = pd.DataFrame()
            if isinstance(fills, pd.DataFrame) and not fills.empty and "date" in fills.columns:
                try:
                    if scope == 'Total':
                        fsub = fills
                    else:
                        coin = coin_from_symbol_code(scope)
                        if "coin" in fills.columns:
                            fsub = fills[fills["coin"].astype(str).str.upper() == str(coin).upper()]
                        else:
                            fsub = fills.iloc[0:0]
                    fday = fsub[fsub["date"].dt.date == selected_day] if not fsub.empty else fsub
                    if not fday.empty:
                        bt_day_df = fday.copy()
                        # Derive effective qty from psize_delta when qty is only +/-1 marker.
                        try:
                            if "qty" in bt_day_df.columns and "psize_delta" in bt_day_df.columns:
                                q = pd.to_numeric(bt_day_df["qty"], errors='coerce')
                                d = pd.to_numeric(bt_day_df["psize_delta"], errors='coerce')
                                use_delta = q.abs() <= 1.1
                                qty_eff = q.where(~use_delta, d)
                                bt_day_df["qty_eff"] = qty_eff
                        except Exception:
                            pass

                        # Keep both representations for readability.
                        try:
                            bt_day_df["qty_contracts"] = pd.to_numeric(bt_day_df.get("qty"), errors="coerce")
                        except Exception:
                            pass

                        # If contract_size was inferred at load time, compute base-coin sizes.
                        try:
                            if "contract_size" in bt_day_df.columns:
                                cs = pd.to_numeric(bt_day_df["contract_size"], errors="coerce")
                                if "qty_eff" in bt_day_df.columns:
                                    bt_day_df["qty_coin"] = pd.to_numeric(bt_day_df["qty_eff"], errors="coerce") * cs
                                elif "qty" in bt_day_df.columns:
                                    bt_day_df["qty_coin"] = pd.to_numeric(bt_day_df["qty"], errors="coerce") * cs
                                if "psize_delta" in bt_day_df.columns:
                                    bt_day_df["psize_delta_coin"] = pd.to_numeric(bt_day_df["psize_delta"], errors="coerce") * cs
                        except Exception:
                            pass
                except Exception:
                    bt_day_df = pd.DataFrame()

            ex_day_df = pd.DataFrame()
            raw_exec_count = 0
            try:
                uobj = users.find_user(single_user)
                live_exchange = getattr(uobj, 'exchange', None)
                if live_exchange:
                    exec_coin = None if scope == 'Total' else coin_from_symbol_code(scope)
                    erows = db.select_executions_rows(single_user, str(live_exchange), day_start_ms, day_end_ms, coin=exec_coin, limit=5000, newest_first=False)
                    if erows:
                        raw_exec_count = len(erows)
                        ex_day_df = pd.DataFrame(erows, columns=["timestamp", "symbol", "side", "price", "qty", "fee", "realized_pnl", "order_id", "trade_id"])
                        ex_day_df["time"] = pd.to_datetime(ex_day_df["timestamp"].astype('int64'), unit='ms', utc=True)
                        # fee is stored as a positive cost in executions
                        ex_day_df["net"] = (ex_day_df["realized_pnl"].fillna(0.0) - ex_day_df["fee"].fillna(0.0)).astype(float)
                        # Add derived coin so Total-scope matching won't cross-match symbols.
                        try:
                            ex_day_df["coin"] = ex_day_df["symbol"].apply(_coin_from_exec_symbol)
                        except Exception:
                            pass
            except Exception:
                ex_day_df = pd.DataFrame()

            # Optional aggregation to avoid partial fill fragmentation
            if not ex_day_df.empty and bool(agg_partials):
                try:
                    ex_day_df = _aggregate_partial_executions(ex_day_df)
                except Exception:
                    pass

            # Aggregation may drop derived columns; ensure coin exists for matching.
            if ex_day_df is not None and not ex_day_df.empty:
                if 'coin' not in ex_day_df.columns and 'symbol' in ex_day_df.columns:
                    try:
                        ex_day_df['coin'] = ex_day_df['symbol'].apply(_coin_from_exec_symbol)
                    except Exception:
                        pass

            agg_exec_count = 0
            try:
                agg_exec_count = int(len(ex_day_df)) if ex_day_df is not None else 0
            except Exception:
                agg_exec_count = 0

            if bt_day_df.empty and ex_day_df.empty:
                st.info("No fills and no executions for this day.")
                return
            if bt_day_df.empty:
                st.info("No backtest fills for this day (in this scope).")
            if ex_day_df.empty:
                st.info("No live executions for this day (in this scope).")

            # Ensure time columns exist
            if not bt_day_df.empty:
                if 'time' not in bt_day_df.columns and 'minute' in bt_day_df.columns:
                    st.warning("Backtest fills have no 'time' column; cannot match.")
                else:
                    # keep only matching-relevant columns
                    keep_cols = [c for c in ['time', 'coin', 'type', 'qty', 'qty_contracts', 'qty_coin', 'contract_size', 'qty_eff', 'psize_delta', 'psize_delta_coin', 'price', 'net'] if c in bt_day_df.columns]
                    bt_day_df = bt_day_df[keep_cols].sort_values('time')
                    # Prefer qty_eff (derived from psize_delta) when present
                    try:
                        if 'qty_coin' in bt_day_df.columns:
                            bt_day_df['qty'] = bt_day_df['qty_coin']
                        elif 'qty_eff' in bt_day_df.columns:
                            bt_day_df['qty'] = bt_day_df['qty_eff']
                    except Exception:
                        pass

            if not ex_day_df.empty:
                keep_cols = [c for c in ['time', 'coin', 'side', 'qty', 'price', 'net', 'trade_id', 'trade_ids_preview', 'trade_count', 'order_id', 'symbol'] if c in ex_day_df.columns]
                ex_day_df = ex_day_df[keep_cols].sort_values('time')

            mdf = _match_rows_by_time(
                bt_day_df,
                ex_day_df,
                tolerance_s=int(tol_s),
                require_side_match=bool(require_side),
                require_coin_match=True,
                include_unmatched_exec_rows=bool(show_unmatched_exec),
                suppress_exec_rows_seen_as_candidates=bool(suppress_exec_rows_seen_as_candidates),
                candidates_from_unmatched_only=bool(candidates_unmatched_only),
            )
            if mdf.empty:
                st.info("Nothing to match.")
                return

            # Quick stats
            try:
                bt_n = int((mdf['bt_time'].notna()).sum())
                cand_n = int((mdf['candidate_time'].notna()).sum())
                matched_n = int((mdf['match'] == True).sum())
                if raw_exec_count:
                    st.caption(
                        f"BT rows: {bt_n} | Exec rows (raw→agg): {raw_exec_count}→{agg_exec_count} | Candidate exec refs: {cand_n} | Matched: {matched_n}"
                    )
                else:
                    st.caption(f"BT rows: {bt_n} | Candidate exec refs: {cand_n} | Matched: {matched_n}")
            except Exception:
                pass

            st.caption(f"Δt (s) = |BT time − nearest execution time| (match requires Δt ≤ {int(tol_s)}s)")

            cols = [
                # Time + match metadata
                'bt_time', 'matched_time', 'dt_s', 'match', 'reason',
                # Side/type
                'bt_type', 'bt_expected_side', 'matched_side',
                # Coin/Symbol context
                'bt_coin', 'matched_symbol',
                # Quantity + price + net (BT next to matched)
                'bt_qty', 'matched_qty',
                'bt_price', 'matched_price',
                'bt_net', 'matched_net',
                # Matched trade context
                'matched_trade_count', 'matched_trade_ids',
            ]
            if bool(show_candidate_cols):
                cols += [
                    'candidate_time', 'candidate_symbol', 'candidate_side', 'candidate_qty', 'candidate_price', 'candidate_net',
                    'candidate_trade_count', 'candidate_trade_ids',
                ]
            if bool(show_debug_cols):
                cols += [
                    'bt_qty_contracts', 'bt_qty_coin', 'bt_contract_size',
                    'bt_psize_delta', 'bt_psize_delta_coin',
                ]

            view = mdf[[c for c in cols if c in mdf.columns]].copy()

            # User-facing naming/help for time delta column
            dt_col = 'dt_s'
            dt_label = 'Δt (s)'

            # Light formatting for readability
            try:
                for c in [
                    'bt_qty', 'matched_qty', 'bt_price', 'matched_price', 'bt_net', 'matched_net',
                    'candidate_qty', 'candidate_price', 'candidate_net', 'dt_s',
                ]:
                    if c in view.columns:
                        view[c] = pd.to_numeric(view[c], errors='coerce')
            except Exception:
                pass

            # Rename dt column for display (keep internal key in mdf)
            try:
                if dt_col in view.columns:
                    view = view.rename(columns={dt_col: dt_label})
            except Exception:
                pass

            # Color legend + coloring
            try:
                with st.expander("Color legend", expanded=False):
                    st.markdown(
                        "- **Green (two shades)**: matched rows; **BT columns** lighter green, **Live matched** columns darker green\n"
                        "- **Red (two shades)**: no candidate found; **BT** lighter red, **Live matched** darker red\n"
                        "- **Amber (two shades)**: nearest exec exists but is outside tolerance (`dt>tol`)\n",
                    )
                    st.markdown(
                        f"- **{dt_label}**: absolute time difference between **BT fill time** and **nearest execution time** (seconds). Match requires **{dt_label} ≤ tolerance** (currently {int(tol_s)}s)."
                    )

                bt_cols = [c for c in view.columns if str(c).startswith('bt_')]
                matched_cols = [c for c in view.columns if str(c).startswith('matched_')]

                # Two-tone palette: BT (lighter) vs matched (darker)
                colors = {
                    'match_bt': 'background-color: rgba(0, 128, 0, 0.12)',
                    'match_m': 'background-color: rgba(0, 128, 0, 0.22)',
                    'fail_bt': 'background-color: rgba(220, 20, 60, 0.12)',
                    'fail_m': 'background-color: rgba(220, 20, 60, 0.22)',
                    'warn_bt': 'background-color: rgba(255, 165, 0, 0.12)',
                    'warn_m': 'background-color: rgba(255, 165, 0, 0.22)',
                }

                def _cell_style(df: pd.DataFrame) -> pd.DataFrame:
                    styles = pd.DataFrame('', index=df.index, columns=df.columns)
                    for idx, row in df.iterrows():
                        try:
                            is_match = bool(row.get('match'))
                        except Exception:
                            is_match = False
                        reason = str(row.get('reason') or '')

                        if is_match:
                            bt_style, m_style = colors['match_bt'], colors['match_m']
                        elif reason in ('no_candidate', 'no_unmatched_candidate'):
                            bt_style, m_style = colors['fail_bt'], colors['fail_m']
                        elif reason == 'dt>tol':
                            bt_style, m_style = colors['warn_bt'], colors['warn_m']
                        else:
                            bt_style = m_style = ''

                        for c in bt_cols:
                            styles.at[idx, c] = bt_style
                        for c in matched_cols:
                            styles.at[idx, c] = m_style
                    return styles

                styled = view.style.apply(_cell_style, axis=None)
                st.dataframe(styled, use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(view, use_container_width=True, hide_index=True)


# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv7 Live vs Backtest")

# Header row: Title + quick link to the new guide
col_title, col_guide = st.columns([0.9, 0.1], vertical_alignment="center")
with col_title:
    st.header("PBv7 Live vs Backtest", divider="red")
with col_guide:
    if st.button("📖 Guide", key="v7_live_vs_backtest_guide_btn", help="Open help and tutorials"):
        _help_modal(default_topic="Live vs Backtest")

# Check if PB7 is installed
if not is_pb7_installed():
    st.warning("Passivbot Version 7.x is not installed", icon="⚠️")
    st.stop()

# Check if CoinData is configured
if st.session_state.pbcoindata.api_error:
    st.warning("Coin Data API is not configured / Go to Coin Data and configure your API-Key", icon="⚠️")
    st.stop()

live_vs_backtest_page()
