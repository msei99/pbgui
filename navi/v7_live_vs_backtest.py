import json
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


def _coerce_usdc_to_usdt(sym: str) -> str:
    s = str(sym or "").strip()
    if not s:
        return ""
    if s.endswith("USDC") and len(s) > 4:
        return s[:-4] + "USDT"
    return s


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
    default_compare_exchange = "binance" if user_exchange == "hyperliquid" else (user_exchange or "binance")
    available_compare_exchanges = ["binance", "bybit", "gateio", "bitget", "combined"]
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
                # Force per-exchange backtest execution (avoid 'combined' dataset runs)
                try:
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
    if compare_exchange:
        result_dirs = [d for d in result_dirs if d.parent.name == compare_exchange]
    result_dirs = sorted(result_dirs, key=lambda p: p.stat().st_mtime, reverse=True)

    labels = []
    label_to_dir = {}
    for d in result_dirs[:200]:
        try:
            timestamp_name = d.name
            exchange_name = d.parent.name
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
        if compare_exchange != "combined" and has_any_combined:
            extra = " (You have combined results; set Exchange to 'combined' to compare them.)"
        st.caption(f"No backtest results found under {results_root}/{compare_exchange}. Showing Live only.{extra}")

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
                    bt_cfg = cfg.get("backtest", {}) if isinstance(cfg, dict) else {}
        except Exception:
            bt_cfg = {}

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
            live_cum_map[sym] = daily.cumsum()
    else:
        pnl_rows = db.select_pnl([single_user], start_ms, end_ms)
        live_df = pd.DataFrame(pnl_rows, columns=["Date", "Income"])
        if not live_df.empty:
            live_df["Date"] = pd.to_datetime(live_df["Date"], format="%Y-%m-%d")
            live_df = live_df.set_index("Date").sort_index()
        live_daily = live_df["Income"].reindex(all_days, fill_value=0.0) if not live_df.empty else pd.Series(0.0, index=all_days)
        live_cum_map = {"Total": live_daily.cumsum()}

    bt_cum_map = {}
    if has_backtest_results and selected_result_dir is not None:
        fills_path = Path(selected_result_dir) / "fills.csv"
        if not fills_path.exists():
            fills_path = Path(selected_result_dir) / "fills.csv.gz"
        config_path = Path(selected_result_dir) / "config.json"

        bt_cum_map = {k: pd.Series(0.0, index=all_days) for k in live_cum_map.keys()}

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

                end_ts = int(datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
                last_minute = float(fills["minute"].max())
                start_ts = end_ts - int(last_minute * 60)
                fills["time"] = pd.to_datetime(start_ts + (fills["minute"].astype(float) * 60), unit="s", utc=True)

                pnl_col = fills["pnl"] if "pnl" in fills.columns else 0.0
                fee_col = fills["fee_paid"] if "fee_paid" in fills.columns else 0.0
                fills["net"] = pnl_col + fee_col

                if selected_symbols:
                    wanted = {sym: coin_from_symbol_code(sym) for sym in selected_symbols}
                    for sym, coin in wanted.items():
                        if "coin" in fills.columns:
                            sub = fills[fills["coin"].astype(str).str.upper() == coin]
                        else:
                            sub = fills.iloc[0:0]
                        if sub.empty:
                            bt_cum_map[sym] = pd.Series(0.0, index=all_days)
                            continue
                        bt_daily = sub.groupby(sub["time"].dt.tz_convert("UTC").dt.floor("D"))["net"].sum()
                        bt_daily.index = bt_daily.index.tz_localize(None)
                        bt_daily = bt_daily.reindex(all_days, fill_value=0.0)
                        bt_cum_map[sym] = bt_daily.cumsum()
                else:
                    bt_daily = fills.groupby(fills["time"].dt.tz_convert("UTC").dt.floor("D"))["net"].sum()
                    bt_daily.index = bt_daily.index.tz_localize(None)
                    bt_daily = bt_daily.reindex(all_days, fill_value=0.0)
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
