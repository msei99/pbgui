import streamlit as st
import plotly.graph_objects as go
import calendar

from datetime import date as _date, datetime as _datetime, timedelta as _timedelta
from pathlib import Path
import subprocess
import sys
import os
import signal
import time
import inspect
import json

from Exchange import Exchanges
from PBCoinData import CoinData, get_symbol_for_coin, compute_coin_name
from pbgui_purefunc import load_ini, save_ini

from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    info_popup,
    error_popup,
    is_authenticted,
    get_navi_paths,
    load_symbols_from_ini,
    render_header_with_guide,
)
from logging_view import view_log_filtered

from market_data import (
    load_market_data_config,
    set_enabled_coins,
    summarize_raw_inventory,
    summarize_pb7_cache_inventory,
    get_daily_hour_coverage_for_dataset,
    get_minute_presence_for_dataset,
    get_exchange_download_log_path,
    append_exchange_download_log,
    get_exchange_raw_root_dir,
    load_aws_profile_credentials,
    save_aws_profile_credentials,
    load_aws_profile_region,
    save_aws_profile_region,
)
from market_data_sources import get_daily_source_counts_for_range, remove_days_from_index

from hyperliquid_aws import (
    HYPERLIQUID_AWS_REGION,
    download_hyperliquid_l2book_aws,
    check_hyperliquid_l2book_coin_exists_aws,
    get_hyperliquid_archive_day_range_aws,
    list_hyperliquid_archive_hours_aws,
)

from hyperliquid_best_1m import (
    update_latest_hyperliquid_1m_api_for_coin,
)

from task_queue import (
    enqueue_job,
    list_jobs,
    read_worker_pid,
    is_pid_running,
    request_cancel_job,
    force_fail_job,
    clear_worker_pid,
)


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


def _format_unix_ts(ts: object) -> str:
    try:
        ts_i = int(float(ts))
    except Exception:
        return str(ts or "")
    if ts_i > 10_000_000_000:
        ts_i = ts_i // 1000
    try:
        return _datetime.fromtimestamp(ts_i).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts or "")


def _load_market_data_status() -> dict:
    path = Path(__file__).resolve().parents[1] / "data" / "logs" / "market_data_status.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_markdown(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Failed to read docs: {e}"


def _supports_fragment_run_every() -> bool:
    try:
        return "run_every" in inspect.signature(st.fragment).parameters
    except Exception:
        return False


def _fmt_bytes(n: int | float | None) -> str:
    if n is None:
        return "0 B"
    try:
        val = float(n)
    except Exception:
        return "0 B"
    if val < 1:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while val >= 1024.0 and idx < len(units) - 1:
        val /= 1024.0
        idx += 1
    return f"{val:.2f} {units[idx]}"


def _filter_jobs_by_type(jobs: list[dict], job_types: list[str] | None) -> list[dict]:
    if not job_types:
        return list(jobs)
    allowed = {str(t) for t in job_types}
    return [j for j in jobs if str(j.get("type") or "") in allowed]


def _has_active_jobs(job_types: list[str] | None) -> bool:
    try:
        jobs = list_jobs(states=["pending", "running"], limit=20)
        return bool(_filter_jobs_by_type(jobs, job_types))
    except Exception:
        return False


def _render_jobs_panel(
    *,
    job_types: list[str] | None,
    details_key: str,
    panel_key: str,
    show_worker_controls: bool = False,
    auto_refresh_key: str | None = None,
) -> None:
    if show_worker_controls:
        pass

    try:
        jobs = list_jobs(states=["pending", "running"], limit=20)
        jobs = _filter_jobs_by_type(jobs, job_types)
        st.caption("Queued/running jobs")
        if not jobs:
            st.write("No active jobs.")
            if auto_refresh_key:
                st.session_state[auto_refresh_key] = False
                st.rerun()
            return

        h1, h2, h3, h4, h5, h6, h7, h8, h9 = st.columns([0.16, 0.13, 0.09, 0.06, 0.17, 0.16, 0.08, 0.07, 0.08])
        h1.write("id")
        h2.write("type")
        h3.write("status")
        h4.write("coin")
        h5.write("chunk")
        h6.write("progress")
        h7.write("updated_ts")
        h8.write(":material/expand_more:")
        h9.write("stop")

        def _render_job_details(match: dict) -> None:
            pr = match.get("progress") if isinstance(match.get("progress"), dict) else {}
            step_i = (pr or {}).get("step")
            total_i = (pr or {}).get("total")
            step_txt = f"{step_i}/{total_i}" if total_i else ""

            dl_t = (pr or {}).get("downloaded_total")
            sk_t = (pr or {}).get("skipped_existing_total")
            fl_t = (pr or {}).get("failed_total")
            totals_txt = ""
            if dl_t is not None or sk_t is not None or fl_t is not None:
                totals_txt = f"d={dl_t or 0} s={sk_t or 0} f={fl_t or 0}"

            dl_b = (pr or {}).get("downloaded_bytes_total")
            sk_b = (pr or {}).get("skipped_existing_bytes_total")
            fl_b = (pr or {}).get("failed_bytes_total")
            bytes_txt = ""
            if dl_b is not None or sk_b is not None or fl_b is not None:
                bytes_txt = f"bytes: d={_fmt_bytes(dl_b)} s={_fmt_bytes(sk_b)} f={_fmt_bytes(fl_b)}"

            line = (step_txt + ("  " if step_txt and totals_txt else "") + totals_txt).strip()
            if bytes_txt:
                line = (line + (" | " if line else "") + bytes_txt).strip()
            stage = str((pr or {}).get("stage") or "")
            mode = str((pr or {}).get("mode") or "")
            chunk_done = (pr or {}).get("chunk_done")
            chunk_total = (pr or {}).get("chunk_total")
            extra_parts: list[str] = []
            if stage:
                extra_parts.append(f"stage={stage}")
            if mode:
                extra_parts.append(f"mode={mode}")
            if chunk_total:
                extra_parts.append(f"chunk={chunk_done or 0}/{chunk_total}")
            merged_parts = []
            if line:
                merged_parts.append(line)
            if extra_parts:
                merged_parts.append(" ".join(extra_parts))
            if merged_parts:
                st.caption(" | ".join(merged_parts))
            sub_day = (pr or {}).get("day")
            sub_hour = (pr or {}).get("hour")
            last_binance_day = (pr or {}).get("last_binance_fill_day")
            if stage == "binance_fill" and sub_day:
                st.caption(f"substatus: binance_fill day={sub_day}")
            elif last_binance_day:
                st.caption(f"substatus: binance_fill day={last_binance_day}")
            if (sub_day or sub_hour is not None) and stage != "binance_fill" and not sub_day:
                try:
                    hour_txt = f"{int(sub_hour):02d}" if sub_hour is not None else ""
                except Exception:
                    hour_txt = str(sub_hour)
                if sub_day and hour_txt:
                    st.caption(f"substatus: l2book {sub_day} {hour_txt}:00")
                elif sub_day:
                    st.caption(f"substatus: l2book {sub_day}")
                elif hour_txt:
                    st.caption(f"substatus: l2book hour {hour_txt}")
            corrupt = (pr or {}).get("corrupt_files")
            if isinstance(corrupt, list) and corrupt:
                st.caption(f"corrupt_files: {len(corrupt)}")
            recent_failed = (pr or {}).get("recent_failed")
            if isinstance(recent_failed, list) and recent_failed:
                st.caption("recent_failed:")
                st.code("\n".join(str(x) for x in recent_failed[:12]))
            recent_keys = (pr or {}).get("recent_keys")
            if isinstance(recent_keys, list) and recent_keys:
                st.caption("recent_keys:")
                st.code("\n".join(str(x) for x in recent_keys[:12]))

        for j in jobs:
            jid = str(j.get("id") or "").strip()
            pr = j.get("progress") if isinstance(j.get("progress"), dict) else {}
            coin = str((pr or {}).get("coin") or "")
            chunk = f"{(pr or {}).get('chunk_start')}→{(pr or {}).get('chunk_end')}" if (pr or {}).get("chunk_start") else ""

            step_i = (pr or {}).get("step")
            total_i = (pr or {}).get("total")
            chunk_done = (pr or {}).get("chunk_done")
            chunk_total = (pr or {}).get("chunk_total")
            pct = 0.0
            try:
                if total_i:
                    if chunk_total and step_i:
                        frac = float(chunk_done or 0) / float(chunk_total)
                        pct = float(max(0, int(step_i) - 1) + frac) / float(total_i)
                    else:
                        pct = float(step_i or 0) / float(total_i)
            except Exception:
                pct = 0.0
            pct = max(0.0, min(1.0, pct))
            step_txt = f"{step_i}/{total_i}" if total_i else ""

            dl_t = (pr or {}).get("downloaded_total")
            sk_t = (pr or {}).get("skipped_existing_total")
            fl_t = (pr or {}).get("failed_total")
            totals_txt = ""
            if dl_t is not None or sk_t is not None or fl_t is not None:
                totals_txt = f"d={dl_t or 0} s={sk_t or 0} f={fl_t or 0}"

            dl_b = (pr or {}).get("downloaded_bytes_total")
            sk_b = (pr or {}).get("skipped_existing_bytes_total")
            fl_b = (pr or {}).get("failed_bytes_total")
            bytes_txt = ""
            if dl_b is not None or sk_b is not None or fl_b is not None:
                bytes_txt = f"bytes: d={_fmt_bytes(dl_b)} s={_fmt_bytes(sk_b)} f={_fmt_bytes(fl_b)}"

            c1, c2, c3, c4, c5, c6, c7, c8, c9 = st.columns([0.16, 0.13, 0.09, 0.06, 0.17, 0.16, 0.08, 0.07, 0.08])
            c1.write(jid)
            c2.write(str(j.get("type") or ""))
            c3.write(str(j.get("status") or ""))
            c4.write(coin)
            c5.write(chunk)
            c6.progress(pct)
            sub_day = (pr or {}).get("day")
            sub_hour = (pr or {}).get("hour")
            last_binance_day = (pr or {}).get("last_binance_fill_day")
            if sub_day or sub_hour is not None:
                try:
                    hour_txt = f"{int(sub_hour):02d}" if sub_hour is not None else ""
                except Exception:
                    hour_txt = str(sub_hour)
                if sub_day and hour_txt:
                    c6.caption(f"l2book {sub_day} {hour_txt}:00")
                elif sub_day:
                    c6.caption(f"l2book {sub_day}")
                elif hour_txt:
                    c6.caption(f"l2book hour {hour_txt}")
            if last_binance_day:
                c6.caption(f"binance_fill day={last_binance_day}")
            c7.write(str(j.get("updated_ts") or ""))
            with c8:
                is_open = str(st.session_state.get(details_key) or "").strip() == jid
                icon = ":material/expand_less:" if is_open else ":material/expand_more:"
                help_txt = "Hide details" if is_open else "Show details"
                if st.button(icon, key=f"{panel_key}_details_{jid}", help=help_txt):
                    st.session_state[details_key] = "" if is_open else jid
            with c9:
                if st.button("Stop", key=f"{panel_key}_stop_{jid}"):
                    try:
                        request_cancel_job(jid, reason="user stop")

                        pid2 = read_worker_pid()
                        if pid2 and is_pid_running(int(pid2)):
                            os.kill(int(pid2), signal.SIGKILL)
                            clear_worker_pid()

                        ok2 = force_fail_job(jid, error="cancelled")
                        if not ok2:
                            st.warning("Job not found.")
                    except Exception as e:
                        st.error(str(e))

    except Exception as e:
        st.error(str(e))


@st.dialog("Help & Tutorials", width="large")
def _help_modal(default_topic: str = "Market Data"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="market_data_help_lang")
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
        key="market_data_help_sel",
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


def _normalize_archive_range(v: object) -> dict[str, str]:
    if isinstance(v, dict):
        oldest = str(v.get("oldest_day") or "").strip()
        newest = str(v.get("newest_day") or "").strip()
        return {"oldest_day": oldest, "newest_day": newest}
    if isinstance(v, (tuple, list)) and len(v) == 2:
        oldest = str(v[0] or "").strip()
        newest = str(v[1] or "").strip()
        return {"oldest_day": oldest, "newest_day": newest}
    return {"oldest_day": "", "newest_day": ""}


def _coin_options_for_exchange(exchange: str) -> list[str]:
    def _canonical_market_coin(exchange_name: str, coin_value: str) -> str:
        ex_name = str(exchange_name or "").strip().lower()
        value = str(coin_value or "").strip()
        if not value:
            return ""
        if ex_name == "hyperliquid":
            lower = value.lower()
            if lower.startswith("xyz:") or lower.startswith("xyz-"):
                tail = value[4:].strip().upper()
                return f"xyz:{tail}" if tail else ""
        return value.upper()

    try:
        coindata = CoinData()
        approved_coins, _ = coindata.filter_mapping(
            exchange=str(exchange).lower(),
            market_cap_min_m=0,
            vol_mcap_max=float("inf"),
            only_cpt=False,
            notices_ignore=False,
            tags=[],
            quote_filter=None,
            use_cache=True,
            active_only=True,
        )
        approved = {
            _canonical_market_coin(exchange, c)
            for c in approved_coins
            if _canonical_market_coin(exchange, c)
        }
        if approved:
            return sorted(approved)

        mapping_path = Path(__file__).resolve().parents[1] / "data" / "coindata" / str(exchange).lower() / "mapping.json"
        if mapping_path.exists():
            mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
            mapped_coins = set()
            for row in mapping if isinstance(mapping, list) else []:
                if not bool(row.get("swap", False)) or not bool(row.get("active", True)) or not bool(row.get("linear", True)):
                    continue
                coin = str(row.get("coin") or "").strip()
                if not coin:
                    symbol = str(row.get("ccxt_symbol") or row.get("symbol") or "").strip()
                    quote = str(row.get("quote") or "").strip().upper()
                    if not symbol:
                        continue
                    coin = compute_coin_name(symbol, quote)
                coin = _canonical_market_coin(exchange, coin)
                if coin:
                    mapped_coins.add(coin)
            return sorted(mapped_coins)

        symbols = load_symbols_from_ini(exchange, "swap")
        fallback_coins = {
            _canonical_market_coin(exchange, s)
            for s in symbols
            if _canonical_market_coin(exchange, s)
        }
        if fallback_coins:
            return sorted(fallback_coins)
        return []
    except Exception:
        return []


def view_market_data():
    def _canonical_market_coin(exchange_name: str, coin_value: str) -> str:
        ex_name = str(exchange_name or "").strip().lower()
        value = str(coin_value or "").strip()
        if not value:
            return ""
        if ex_name == "hyperliquid":
            lower = value.lower()
            if lower.startswith("xyz:") or lower.startswith("xyz-"):
                tail = value[4:].strip().upper()
                return f"xyz:{tail}" if tail else ""
        return value.upper()

    exchanges = list(Exchanges.list())
    if "hyperliquid" in exchanges:
        default_exchange = "hyperliquid"
    else:
        default_exchange = exchanges[0] if exchanges else "hyperliquid"

    exchange = st.selectbox(
        "Exchange",
        options=exchanges or [default_exchange],
        index=(exchanges.index(default_exchange) if default_exchange in exchanges else 0),
        key="market_data_exchange",
    )

    cfg = load_market_data_config()
    enabled_default_raw = [
        _canonical_market_coin(exchange, c)
        for c in (cfg.enabled_coins.get(str(exchange).lower(), []) or [])
        if _canonical_market_coin(exchange, c)
    ]

    coin_options = _coin_options_for_exchange(str(exchange))
    option_set = set(coin_options)
    enabled_default = [c for c in enabled_default_raw if c in option_set]
    dropped_defaults = sorted(set(enabled_default_raw) - option_set)
    enabled_key = f"market_data_enabled_{str(exchange).lower()}"

    enabled_preview = enabled_default
    try:
        if enabled_key in st.session_state and isinstance(st.session_state.get(enabled_key), list):
            enabled_preview = [
                _canonical_market_coin(exchange, c)
                for c in (st.session_state.get(enabled_key) or [])
                if _canonical_market_coin(exchange, c) in option_set
            ]
    except Exception:
        enabled_preview = enabled_default

    if str(exchange).lower() == "hyperliquid":
        with st.expander("Settings (Latest 1m Auto-Refresh)", expanded=False):
            st.caption("Configure automatic 1m candle refresh settings. Changes are saved to pbgui.ini and applied automatically in the next cycle (no restart needed).")

            enabled_in_settings = st.multiselect(
                "Enabled coins",
                options=coin_options,
                default=enabled_default,
                key=enabled_key,
            )
            if dropped_defaults:
                st.warning(
                    "Ignored missing saved coins (not in current options): " + ", ".join(dropped_defaults),
                    icon="⚠️",
                )
            st.caption(f"Enabled: {len(enabled_in_settings)}")

            def _read_int_ini(section: str, key: str, default: int) -> int:
                try:
                    v = load_ini(section, key)
                    s = str(v).strip() if v is not None else ''
                    if s == '':
                        return default
                    return int(float(s))
                except Exception:
                    return default

            def _read_float_ini(section: str, key: str, default: float) -> float:
                try:
                    v = load_ini(section, key)
                    s = str(v).strip() if v is not None else ''
                    if s == '':
                        return default
                    return float(s)
                except Exception:
                    return default

            interval_val = _read_int_ini('pbdata', 'latest_1m_interval_seconds', 120)
            coin_pause_val = _read_float_ini('pbdata', 'latest_1m_coin_pause_seconds', 0.5)
            timeout_val = _read_float_ini('pbdata', 'latest_1m_api_timeout_seconds', 30.0)
            min_lb_val = _read_int_ini('pbdata', 'latest_1m_min_lookback_days', 2)
            max_lb_val = _read_int_ini('pbdata', 'latest_1m_max_lookback_days', 4)

            c1, c2, c3, c4, c5 = st.columns(5)
            with c1:
                st.number_input(
                    'Cycle interval (s)',
                    min_value=60,
                    max_value=3600,
                    value=int(interval_val),
                    step=30,
                    key='md_setting_interval',
                    help='How often to refresh all enabled coins (default: 120s). Increase for many symbols (e.g. 300-600s for all Hyperliquid coins).'
                )
            with c2:
                st.number_input(
                    'Pause between coins (s)',
                    min_value=0.0,
                    max_value=10.0,
                    value=float(coin_pause_val),
                    step=0.1,
                    key='md_setting_coin_pause',
                    help='Pause after each coin to avoid rate limits (default: 0.5s). Increase to 1-2s if seeing 429 errors.'
                )
            with c3:
                st.number_input(
                    'API timeout per coin (s)',
                    min_value=10.0,
                    max_value=120.0,
                    value=float(timeout_val),
                    step=5.0,
                    key='md_setting_timeout',
                    help='Timeout for API request per coin (default: 30s). Increase for slow connections or larger lookback windows.'
                )
            with c4:
                st.number_input(
                    'Min lookback days',
                    min_value=1,
                    max_value=10,
                    value=int(min_lb_val),
                    step=1,
                    key='md_setting_min_lb',
                    help='Minimum lookback window for API fetch (default: 2 days).'
                )
            with c5:
                st.number_input(
                    'Max lookback days',
                    min_value=1,
                    max_value=10,
                    value=int(max_lb_val),
                    step=1,
                    key='md_setting_max_lb',
                    help='Maximum lookback window for API fetch (default: 4 days).'
                )

            st.markdown("**AWS Settings (l2Book)**")
            profile_for_settings = str(st.session_state.get("market_data_hl_aws_profile") or "pbgui-hyperliquid")

            creds_settings = {}
            try:
                creds_settings = load_aws_profile_credentials(profile_for_settings)
            except Exception:
                creds_settings = {}

            region_default_settings = load_aws_profile_region(profile_for_settings) or HYPERLIQUID_AWS_REGION

            c_p, c_ak, c_sk, c_rg, c_to, c_wk = st.columns([1.3, 1.2, 1.2, 1.0, 0.9, 0.9], vertical_alignment="bottom")
            with c_p:
                st.text_input(
                    "AWS profile name",
                    value=profile_for_settings,
                    key="market_data_hl_aws_profile",
                    help="Named local AWS profile used to store/read credentials for this page.",
                )
            with c_ak:
                st.text_input(
                    "aws_access_key_id",
                    value=str(creds_settings.get("aws_access_key_id") or ""),
                    key="market_data_hl_aws_access_key_id",
                    type="password",
                    help="AWS Access Key ID for Requester-Pays S3 access.",
                )
            with c_sk:
                st.text_input(
                    "aws_secret_access_key",
                    value=str(creds_settings.get("aws_secret_access_key") or ""),
                    key="market_data_hl_aws_secret_access_key",
                    type="password",
                    help="AWS Secret Access Key for the selected profile.",
                )
            with c_rg:
                st.text_input(
                    "AWS region",
                    value=str(st.session_state.get("market_data_hl_aws_region") or region_default_settings),
                    key="market_data_hl_aws_region",
                    help="AWS region for the Hyperliquid archive bucket (default: us-east-2).",
                )
            with c_to:
                st.number_input(
                    "Scan timeout (s)",
                    min_value=0.1,
                    max_value=60.0,
                    value=float(st.session_state.get("market_data_hl_l2book_scan_timeout_s") or _read_float_ini("market_data", "hl_l2book_scan_timeout_s", 5.0)),
                    step=0.5,
                    key="market_data_hl_l2book_scan_timeout_s",
                    help="Timeout per S3 list operation while scanning archive availability.",
                )
            with c_wk:
                st.number_input(
                    "Workers",
                    min_value=1,
                    max_value=64,
                    value=int(st.session_state.get("market_data_hl_l2book_scan_workers") or _read_int_ini("market_data", "hl_l2book_scan_workers", 8)),
                    step=1,
                    key="market_data_hl_l2book_scan_workers",
                    help="Parallel workers used for archive scan checks.",
                )

            if st.button('Save', key='md_save_settings_btn'):
                try:
                    set_enabled_coins(exchange, enabled_in_settings)
                    save_ini('pbdata', 'latest_1m_interval_seconds', str(int(st.session_state.get('md_setting_interval', 120))))
                    save_ini('pbdata', 'latest_1m_coin_pause_seconds', str(float(st.session_state.get('md_setting_coin_pause', 0.5))))
                    save_ini('pbdata', 'latest_1m_api_timeout_seconds', str(float(st.session_state.get('md_setting_timeout', 30.0))))
                    save_ini('pbdata', 'latest_1m_min_lookback_days', str(int(st.session_state.get('md_setting_min_lb', 2))))
                    save_ini('pbdata', 'latest_1m_max_lookback_days', str(int(st.session_state.get('md_setting_max_lb', 4))))
                    ak = str(st.session_state.get("market_data_hl_aws_access_key_id") or "").strip()
                    sk = str(st.session_state.get("market_data_hl_aws_secret_access_key") or "").strip()
                    profile = str(st.session_state.get("market_data_hl_aws_profile") or "pbgui-hyperliquid").strip() or "pbgui-hyperliquid"
                    region = str(st.session_state.get("market_data_hl_aws_region") or "").strip()
                    save_aws_profile_credentials(profile=profile, aws_access_key_id=ak, aws_secret_access_key=sk)
                    save_aws_profile_region(profile=profile, region=region)
                    timeout_s = float(st.session_state.get("market_data_hl_l2book_scan_timeout_s", 5.0))
                    workers = int(st.session_state.get("market_data_hl_l2book_scan_workers", 8))
                    save_ini("market_data", "hl_l2book_scan_timeout_s", str(timeout_s))
                    save_ini("market_data", "hl_l2book_scan_workers", str(workers))
                    st.success('✅ Settings saved. Enabled coins and auto-refresh settings are applied automatically in the next refresh cycle.')
                except Exception as e:
                    st.error(f'Failed to save settings: {e}')

    tab_actions, tab_have, tab_log = st.tabs(["Actions", "Already have", "Activity log"])

    with tab_actions:
        if str(exchange).lower() != "hyperliquid":
            st.info("Market Data actions are currently implemented for Hyperliquid.")
        else:
            coin_list = [str(c).strip().upper() for c in enabled_preview if str(c).strip()]

            with st.expander("Market Data status", expanded=False):
                status = _load_market_data_status()
                if not status:
                    st.info("No status yet. Start PBData to populate market data status.")
                else:
                    latest = status.get("latest_1m") if isinstance(status, dict) else {}
                    latest_coins = latest.get("coins") if isinstance(latest, dict) else {}
                    interval_s = int(latest.get("interval_seconds") or 0) if isinstance(latest, dict) else 0
                    
                    if isinstance(latest_coins, dict) and latest_coins:
                        rows = []
                        now = _datetime.now()
                        ex_key = f"{str(exchange).lower()}.swap" if "." not in str(exchange) else str(exchange).lower()
                        for coin, cst in sorted(latest_coins.items()):
                            last_fetch = str(cst.get("last_fetch") or "") if isinstance(cst, dict) else ""
                            next_run = ""
                            if interval_s and last_fetch:
                                try:
                                    last_dt = _datetime.fromisoformat(last_fetch)
                                    next_run = max(0, int(interval_s - (now - last_dt).total_seconds()))
                                except Exception:
                                    next_run = ""
                            coin_display = coin
                            try:
                                if str(exchange).lower() == "hyperliquid":
                                    coin_display = get_symbol_for_coin(str(coin), ex_key)
                            except Exception:
                                coin_display = coin
                            rows.append(
                                {
                                    "coin": coin_display,
                                    "last_fetch": last_fetch,
                                    "result": (cst.get("result") if isinstance(cst, dict) else ""),
                                    "lookback_days": (cst.get("lookback_days") if isinstance(cst, dict) else ""),
                                    "newest_day": (cst.get("newest_day") if isinstance(cst, dict) else ""),
                                    "next_run_in_s": next_run,
                                }
                            )
                        st.dataframe(rows, use_container_width=True)
                    else:
                        st.info("No latest 1m status available yet.")

            if False:
                st.caption(
                    "Automatic refresh for newest data. Merges missing minutes in a small lookback window per coin. "
                    "(Hyperliquid provides only the most recent ~5000 1m candles.)"
                )

                if not coin_list:
                    st.warning("No enabled coins selected.")
                else:
                    st.caption(f"Coins: {', '.join(coin_list[:12])}{' …' if len(coin_list) > 12 else ''}")

                latest_coin_options = ["All"] + coin_list if coin_list else []
                latest_coin_sel = st.multiselect(
                    "Coins for latest update",
                    options=latest_coin_options,
                    default=["All"] if coin_list else [],
                    key="market_data_hl_latest_1m_coins",
                )
                if "All" in latest_coin_sel or not latest_coin_sel:
                    latest_coins = list(coin_list)
                else:
                    latest_coins = [c for c in latest_coin_sel if c in coin_list]

                if st.button("Update latest 1m", key="market_data_hl_latest_1m_run"):
                    try:
                        if not latest_coins:
                            raise ValueError("No enabled coins selected")

                        lookback_days = 7
                        p = st.progress(0)
                        total = max(1, len(latest_coins))

                        append_exchange_download_log(
                            "hyperliquid",
                            f"[hl_latest_1m] bulk start coins={len(latest_coins)} lookback_days={lookback_days}",
                        )

                        for i, coin in enumerate(latest_coins):
                            with st.spinner(f"{coin}: updating latest 1m"):
                                r = update_latest_hyperliquid_1m_api_for_coin(
                                    coin=coin,
                                    lookback_days=int(lookback_days),
                                    overwrite=False,
                                    dry_run=False,
                                )
                            append_exchange_download_log("hyperliquid", f"[INFO] [hl_latest_1m] result {r}")
                            p.progress(min(1.0, (i + 1) / float(total)))

                        st.success("Latest 1m update finished.")
                    except Exception as e:
                        append_exchange_download_log("hyperliquid", f"[hl_latest_1m] ERROR {e}")
                        st.error(str(e))

            with st.expander("Build OHLCV", expanded=False):
                if not coin_list:
                    st.warning("No enabled coins selected.")

                build_coin_options = ["All"] + coin_list if coin_list else []
                build_coin_sel = st.multiselect(
                    "Coins for build",
                    options=build_coin_options,
                    default=["All"] if coin_list else [],
                    key="market_data_hl_best_1m_coins",
                )
                if "All" in build_coin_sel or not build_coin_sel:
                    build_coins = list(coin_list)
                else:
                    build_coins = [c for c in build_coin_sel if c in coin_list]

                run_improve = st.button("Build best 1m", key="market_data_hl_best_1m_run_improve")

                if run_improve:
                    try:
                        if not build_coins:
                            raise ValueError("No enabled coins selected")

                        job = enqueue_job(
                            job_type="hl_best_1m",
                            payload={
                                "coins": list(build_coins),
                                "end_day": _date.today().strftime("%Y%m%d"),
                            },
                        )
                        st.session_state["market_data_hl_last_job_id"] = job.job_id
                        append_exchange_download_log(
                            "hyperliquid",
                            f"[hl_best_1m] queued job_id={job.job_id}",
                        )

                        pid = read_worker_pid()
                        if not (pid and is_pid_running(int(pid))):
                            subprocess.Popen(
                                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                close_fds=True,
                            )

                        st.success(f"Queued background build job: {job.job_id}")
                        st.rerun()
                    except Exception as e:
                        append_exchange_download_log("hyperliquid", f"[hl_best_1m] ERROR {e}")
                        st.error(str(e))

                jobs_active_best = _has_active_jobs(["hl_best_1m"])
                if jobs_active_best and _supports_fragment_run_every():
                    @st.fragment(run_every=2)
                    def _best_jobs_fragment():
                        _render_jobs_panel(
                            job_types=["hl_best_1m"],
                            details_key="market_data_hl_best_job_details",
                            panel_key="market_data_hl_best_jobs",
                            show_worker_controls=True,
                            auto_refresh_key="market_data_best_auto_refresh",
                        )
                else:
                    @st.fragment
                    def _best_jobs_fragment():
                        _render_jobs_panel(
                            job_types=["hl_best_1m"],
                            details_key="market_data_hl_best_job_details",
                            panel_key="market_data_hl_best_jobs",
                            show_worker_controls=True,
                        )

                _best_jobs_fragment()

                # Last build job summary (auto-refresh while jobs are active)
                try:
                    def _render_last_build_job() -> None:
                        jobs_any = list_jobs(states=["running", "done", "failed"], limit=50)
                        jobs_any = [j for j in jobs_any if str(j.get("type") or "") == "hl_best_1m"]
                        try:
                            jobs_any = sorted(
                                jobs_any,
                                key=lambda j: int(float(j.get("updated_ts") or 0)),
                                reverse=True,
                            )
                        except Exception:
                            pass

                        if jobs_any:
                            last = jobs_any[0]
                            status = str(last.get("status") or "")
                            upd_ts_raw = last.get("updated_ts")
                            upd_ts = _format_unix_ts(upd_ts_raw)
                            err = str(last.get("error") or "")
                            payload = last.get("payload") if isinstance(last.get("payload"), dict) else {}
                            coins = payload.get("coins") if isinstance(payload, dict) else None
                            coins = coins if isinstance(coins, list) else []
                            coins_preview = ", ".join(str(c) for c in coins[:12])
                            if coins_preview and len(coins) > 12:
                                coins_preview += " …"
                            pr = last.get("progress") if isinstance(last.get("progress"), dict) else {}
                            lr = (pr or {}).get("last_result") if isinstance(pr, dict) else {}
                            st.write(
                                f"status={status}"
                                + (f" updated={upd_ts}" if upd_ts else "")
                                + (f" error={err}" if err else "")
                            )
                            if coins_preview:
                                st.caption(f"coins: {coins_preview}")
                            if isinstance(lr, dict) and lr:
                                dur_s = None
                                try:
                                    if lr.get("duration_s") is not None:
                                        dur_s = int(float(lr.get("duration_s") or 0))
                                except Exception:
                                    dur_s = None
                                dur_txt = ""
                                if dur_s is not None:
                                    h = dur_s // 3600
                                    m = (dur_s % 3600) // 60
                                    s = dur_s % 60
                                    if h > 0:
                                        dur_txt = f" duration={h}h {m:02d}m {s:02d}s"
                                    elif m > 0:
                                        dur_txt = f" duration={m}m {s:02d}s"
                                    else:
                                        dur_txt = f" duration={s}s"
                                if all(k in lr for k in ("days_checked", "l2book_minutes_added", "binance_minutes_filled")):
                                    st.caption(
                                        "improve: "
                                        f"days={lr.get('days_checked')} "
                                        f"l2book_added={lr.get('l2book_minutes_added')} "
                                        f"binance_filled={lr.get('binance_minutes_filled')} "
                                        f"bybit_filled={lr.get('bybit_minutes_filled', 0)}"
                                        f"{dur_txt}"
                                    )
                                else:
                                    st.caption(f"result: {lr}")
                        else:
                            st.write("No build jobs yet.")

                    with st.expander("Last build job", expanded=False):
                        jobs_active_last = _has_active_jobs(["hl_best_1m"])
                        if jobs_active_last and _supports_fragment_run_every():
                            @st.fragment(run_every=2)
                            def _last_build_fragment():
                                # Stop auto-refresh when no jobs remain
                                try:
                                    if not bool(list_jobs(states=["pending", "running"], limit=1)):
                                        st.rerun()
                                except Exception:
                                    pass
                                _render_last_build_job()
                        else:
                            @st.fragment
                            def _last_build_fragment():
                                _render_last_build_job()
                        _last_build_fragment()
                except Exception:
                    pass

            with st.expander("Download l2books from AWS", expanded=False):
                profile = str(st.session_state.get("market_data_hl_aws_profile") or "pbgui-hyperliquid").strip() or "pbgui-hyperliquid"
                region_default = load_aws_profile_region(profile) or HYPERLIQUID_AWS_REGION
                region = str(st.session_state.get("market_data_hl_aws_region") or region_default).strip()

                jobs_active = _has_active_jobs(["hl_aws_l2book_auto"])
                if jobs_active and _supports_fragment_run_every():
                    @st.fragment(run_every=2)
                    def _jobs_fragment():
                        _render_jobs_panel(
                            job_types=["hl_aws_l2book_auto"],
                            details_key="market_data_hl_job_details",
                            panel_key="market_data_hl_jobs",
                            show_worker_controls=True,
                            auto_refresh_key="market_data_aws_auto_refresh",
                        )
                else:
                    @st.fragment
                    def _jobs_fragment():
                        _render_jobs_panel(
                            job_types=["hl_aws_l2book_auto"],
                            details_key="market_data_hl_job_details",
                            panel_key="market_data_hl_jobs",
                            show_worker_controls=True,
                        )

                _jobs_fragment()

                if coin_list:
                    aws_coin_options = ["All"] + coin_list if coin_list else []
                    st.multiselect(
                        "Coins",
                        options=aws_coin_options,
                        default=[],
                        key="market_data_hl_aws_coins",
                    )
                else:
                    st.warning("No enabled coins selected.")

                rr = st.session_state.get("market_data_hl_archive_range") or {}
                default_oldest = str(rr.get("oldest_day") or "20230415")
                default_newest = str(rr.get("newest_day") or "20251202")

                # Parse defaults to date objects
                try:
                    from datetime import datetime as _dt
                    default_start_date = _dt.strptime(default_oldest, "%Y%m%d").date()
                except Exception:
                    default_start_date = _dt(2023, 4, 15).date()

                try:
                    default_end_date = _dt.strptime(default_newest, "%Y%m%d").date()
                except Exception:
                    default_end_date = _dt.now().date()

                col1, col2 = st.columns(2)
                with col1:
                    dl_start_date = st.date_input(
                        "Start date",
                        value=default_start_date,
                        key="market_data_hl_dl_start_date",
                        help="First day to download (archive oldest: " + default_oldest + ")",
                    )
                with col2:
                    dl_end_date = st.date_input(
                        "End date",
                        value=default_end_date,
                        key="market_data_hl_dl_end_date",
                        help="Last day to download (archive newest: " + default_newest + ")",
                    )

                c_dl_btn, c_dl_opt = st.columns([0.28, 0.72], vertical_alignment="center")
                with c_dl_opt:
                    st.checkbox(
                        "Only missing 1m_src hours",
                        value=bool(st.session_state.get("market_data_hl_dl_only_missing_1m_src_hours", True)),
                        key="market_data_hl_dl_only_missing_1m_src_hours",
                        help="If enabled, downloads only l2Book hours that have no minute coverage in 1m_src yet. "
                        "Also skips days older than your local oldest l2Book day for that coin. "
                        "Useful to keep disk usage low after you delete processed l2Book files.",
                    )
                with c_dl_btn:
                    do_download = st.button("Download", key="market_data_hl_dl_auto")

                if do_download:
                    try:
                        # Resolve selected coins for auto-download. If 'All' is mixed with
                        # specific coins, prefer the explicit selection.
                        if not coin_list:
                            raise ValueError("No enabled coins selected")
                        _aws_sel = st.session_state.get("market_data_hl_aws_coins") or []
                        _aws_sel = [str(x).strip().upper() for x in _aws_sel if str(x).strip()]
                        if "ALL" in _aws_sel and len(_aws_sel) > 1:
                            _aws_sel = [c for c in _aws_sel if c != "ALL"]
                        if not _aws_sel or "ALL" in _aws_sel:
                            _payload_coins = list(coin_list)
                        else:
                            _payload_coins = [c for c in _aws_sel if c in coin_list]
                        if not _payload_coins:
                            raise ValueError("No enabled coins selected")

                        # Convert normalized coin names to full symbols (e.g., PEPE -> PEPE_USDC:USDC)
                        _payload_symbols = []
                        for coin in _payload_coins:
                            symbol = get_symbol_for_coin(coin=coin, exchange=f"{str(exchange).lower()}.swap")
                            if symbol:
                                _payload_symbols.append(symbol)
                            else:
                                _payload_symbols.append(coin)  # Fallback to coin name
                        _payload_coins = _payload_symbols

                        ak = str(st.session_state.get("market_data_hl_aws_access_key_id") or "").strip()
                        sk = str(st.session_state.get("market_data_hl_aws_secret_access_key") or "").strip()
                        if not ak or not sk:
                            raise ValueError("Missing AWS credentials")

                        # Persist creds+region so the background worker can use them
                        try:
                            save_aws_profile_credentials(profile=profile, aws_access_key_id=ak, aws_secret_access_key=sk)
                        except Exception:
                            pass
                        try:
                            save_aws_profile_region(profile=profile, region=str(region).strip())
                        except Exception:
                            pass

                        # Preflight: verify coin exists in archive for a probe day (fast).
                        rr = _normalize_archive_range(st.session_state.get("market_data_hl_archive_range") or {})
                        probe_day = str(rr.get("newest_day") or rr.get("oldest_day") or "").strip()
                        if not probe_day:
                            with st.spinner("Detecting available archive day range..."):
                                rr = _normalize_archive_range(
                                    get_hyperliquid_archive_day_range_aws(
                                        aws_access_key_id=ak,
                                        aws_secret_access_key=sk,
                                        region_name=str(region).strip(),
                                    )
                                )
                            st.session_state["market_data_hl_archive_range"] = rr
                            probe_day = str(rr.get("newest_day") or rr.get("oldest_day") or "").strip()
                        if not probe_day:
                            raise RuntimeError("Failed to detect archive range for preflight")

                        probe_hours = list_hyperliquid_archive_hours_aws(
                            day=probe_day,
                            aws_access_key_id=ak,
                            aws_secret_access_key=sk,
                            region_name=str(region).strip(),
                        )
                        if not probe_hours:
                            raise RuntimeError(f"No archive hours found for {probe_day}")

                        missing_coins = []
                        for coin in list(_payload_coins):
                            ok = check_hyperliquid_l2book_coin_exists_aws(
                                coin=coin,
                                day=probe_day,
                                aws_access_key_id=ak,
                                aws_secret_access_key=sk,
                                region_name=str(region).strip(),
                                hours=probe_hours,
                            )
                            if not ok:
                                missing_coins.append(coin)

                        if missing_coins:
                            st.warning(
                                f"No l2Book objects found for: {', '.join(missing_coins)} (probe day {probe_day}). Skipping."
                            )
                            _payload_coins = [c for c in _payload_coins if c not in missing_coins]
                        if not _payload_coins:
                            raise ValueError("No selected coins exist in the archive")

                        # Convert selected dates to YYYYMMDD format
                        start_day_str = dl_start_date.strftime("%Y%m%d") if dl_start_date else ""
                        end_day_str = dl_end_date.strftime("%Y%m%d") if dl_end_date else ""

                        if not start_day_str or not end_day_str:
                            raise ValueError("Start and end dates are required")

                        # Enqueue background job
                        job = enqueue_job(
                            job_type="hl_aws_l2book_auto",
                            payload={
                                "profile": str(profile).strip() or "pbgui-hyperliquid",
                                "region": str(region).strip(),
                                "coins": list(_payload_coins),
                                "chunk_days": 7,
                                "start_day": start_day_str,
                                "end_day": end_day_str,
                                "only_missing_1m_src_hours": bool(
                                    st.session_state.get("market_data_hl_dl_only_missing_1m_src_hours", True)
                                ),
                            },
                        )
                        # Log which coins are enqueued for easier debugging
                        append_exchange_download_log("hyperliquid", f"[hl_aws_l2book_auto] queued job_id={job.job_id} coins={_payload_coins} range={start_day_str}-{end_day_str}")
                        st.success(f"Queued background download job: {job.job_id} (coins={len(_payload_coins)})")
                        st.session_state["market_data_hl_last_job_id"] = job.job_id

                        # Start worker if not running
                        pid = read_worker_pid()
                        if not (pid and is_pid_running(int(pid))):
                            subprocess.Popen(
                                [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                close_fds=True,
                            )

                        st.rerun()
                    except Exception as e:
                        append_exchange_download_log("hyperliquid", f"[hl_aws_l2book_auto] ERROR {e}")
                        st.error(str(e))


    with tab_have:
        _cache_key = f"market_data_have_table_cached_{str(exchange).lower()}"
        if _cache_key not in st.session_state:
            # Load inventory with intelligent coverage calculation
            # - 1m: uses sources.idx (fast!)
            # - l2book: hour from filename
            # - 1m_api: minimal estimation
            st.session_state[_cache_key] = summarize_raw_inventory(str(exchange).lower(), skip_coverage=False)
        rows = st.session_state.get(_cache_key) or []
        if not rows:
            st.info("No raw files found yet.")
        else:
            import pandas as pd

            # Split inventory by dataset
            rows_1m = [r for r in rows if str(r.get("dataset") or "").lower() in ("1m", "candles_1m")]
            rows_1m_api = [r for r in rows if str(r.get("dataset") or "").lower() in ("1m_api", "candles_1m_api")]
            rows_l2book = [r for r in rows if str(r.get("dataset") or "").lower() == "l2book"]

            tab_1m, tab_1m_api, tab_l2book, tab_pb7_cache = st.tabs(["1m", "1m_api", "l2Book", "PB7 cache"])

            try:
                _latest_interval_s = int(str(load_ini("pbdata", "latest_1m_interval_seconds") or "120").strip())
            except Exception:
                _latest_interval_s = 120
            _missing_lag_minutes = max(0, int((_latest_interval_s + 59) // 60))

            _coin_cutoff_cache: dict[str, int | None] = {}

            def _latest_1m_api_cutoff_ts_ms(coin_dir: str) -> int | None:
                key = str(coin_dir or "").strip().upper()
                if not key:
                    return None
                if key in _coin_cutoff_cache:
                    return _coin_cutoff_cache.get(key)

                try:
                    import numpy as np
                except Exception:
                    _coin_cutoff_cache[key] = None
                    return None

                try:
                    ddir = get_exchange_raw_root_dir("hyperliquid") / "1m_api" / key
                    files = sorted(ddir.glob("*.npz"))
                    if not files:
                        _coin_cutoff_cache[key] = None
                        return None
                    latest = files[-1]
                    with np.load(latest) as data:
                        arr = data["candles"] if "candles" in data else None
                    if arr is None or len(arr) == 0:
                        _coin_cutoff_cache[key] = None
                        return None
                    ts = int(arr["ts"][-1])
                    _coin_cutoff_cache[key] = ts
                    return ts
                except Exception:
                    _coin_cutoff_cache[key] = None
                    return None

            # Helper function to render table for a specific dataset
            def _render_dataset_table(dataset_rows: list, dataset_label: str, tab_key: str):
                if not dataset_rows:
                    st.info(f"No {dataset_label} data found yet.")
                    return None

                # ----------------------------------------------------------
                # Cache computed table_rows so df input is 100% stable
                # between reruns.  Only recomputed on sidebar refresh
                # (which clears "market_data_trows_*" keys).
                # ----------------------------------------------------------
                _trows_key = f"market_data_trows_{tab_key}"
                if _trows_key not in st.session_state:
                    _rows: list[dict] = []
                    for r in dataset_rows:
                        ex = str(r.get("exchange", "")).lower()
                        ds = str(r.get("dataset", "")).lower()
                        cn = str(r.get("coin", ""))
                        hl_minutes = ""
                        other_minutes = ""
                        missing_minutes = ""
                        if ex == "hyperliquid" and ds in ("1m", "candles_1m") and cn:
                            try:
                                start_day = str(r.get("oldest_day") or "").strip()
                                end_day = _date.today().strftime("%Y%m%d")
                                counts = get_daily_source_counts_for_range(
                                    exchange=ex,
                                    coin=cn,
                                    start_day=start_day,
                                    end_day=end_day,
                                    lag_minutes=_missing_lag_minutes,
                                    cutoff_ts_ms=_latest_1m_api_cutoff_ts_ms(cn),
                                )
                                if isinstance(counts, dict) and counts:
                                    hl = 0
                                    oth = 0
                                    miss = 0
                                    for c in counts.values():
                                        if not isinstance(c, dict):
                                            continue
                                        hl += int(c.get("api") or 0) + int(c.get("l2Book_mid") or 0)
                                        oth += int(c.get("other_exchange") or 0)
                                        miss += int(c.get("missing") or 0)
                                    hl_minutes = hl
                                    other_minutes = oth
                                    missing_minutes = miss
                            except Exception:
                                pass

                        total_bytes = r.get("total_bytes", 0) or 0
                        size_mb = float(total_bytes) / (1024.0 * 1024.0)
                        _rows.append(
                            {
                                "exchange": r.get("exchange", ""),
                                "dataset": r.get("dataset", ""),
                                "coin": r.get("coin", ""),
                                "n_files": r.get("n_files", 0),
                                "size": float(size_mb),
                                "oldest_day": r.get("oldest_day", ""),
                                "newest_day": r.get("newest_day", ""),
                                "n_days": r.get("n_days", 0),
                                "expected_hours": r.get("expected_hours", 0),
                                "coverage_pct": r.get("coverage_pct", 0),
                                "missing_days_count": r.get("missing_days_count", 0),
                                "missing_days_sample": r.get("missing_days_sample", ""),
                                "hl_minutes": hl_minutes,
                                "other_minutes": other_minutes,
                                "missing_minutes": missing_minutes,
                            }
                        )
                    st.session_state[_trows_key] = _rows

                table_rows = st.session_state[_trows_key]

                df_cached = pd.DataFrame(table_rows)
                df_view = df_cached
                if str(tab_key).lower() in ("1m_api", "l2book"):
                    drop_cols = [c for c in ("hl_minutes", "other_minutes", "missing_minutes") if c in df_cached.columns]
                    if drop_cols:
                        df_view = df_cached.drop(columns=drop_cols)

                # ---- stable st.dataframe with on_select ----
                column_config = {
                    "size": st.column_config.NumberColumn(
                        "size",
                        format="%.2f MB",
                    )
                }
                event = st.dataframe(
                    df_view,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key=f"market_data_have_table_{tab_key}",
                    column_config=column_config,
                )

                # Track selection per tab so we can distinguish
                # "empty because rerun" from "user explicitly deselected".
                _prev_sel_key = f"market_data_prev_sel_{tab_key}"
                sel_indices = event.selection.rows if event and event.selection else []
                prev_sel = st.session_state.get(_prev_sel_key, [])
                if sel_indices:
                    # User selected a row
                    idx = sel_indices[0]
                    if 0 <= idx < len(dataset_rows):
                        clicked = (
                            str(dataset_rows[idx].get("dataset") or ""),
                            str(dataset_rows[idx].get("coin") or ""),
                        )
                        st.session_state["market_data_heatmap_sel"] = clicked
                        st.session_state["market_data_heatmap_tab"] = tab_key
                elif prev_sel:
                    # Had a selection before, now empty → user deselected
                    st.session_state.pop("market_data_heatmap_sel", None)
                    st.session_state.pop("market_data_heatmap_tab", None)
                st.session_state[_prev_sel_key] = list(sel_indices)

                # Only show selection if it belongs to this tab
                sel_row = None
                hm = st.session_state.get("market_data_heatmap_sel")
                hm_tab = st.session_state.get("market_data_heatmap_tab")
                if isinstance(hm, (tuple, list)) and len(hm) == 2 and hm_tab == tab_key:
                    for r in dataset_rows:
                        if (str(r.get("dataset") or ""), str(r.get("coin") or "")) == tuple(hm):
                            sel_row = r
                            break

                if sel_row:
                    st.caption(f"Heatmap: {sel_row.get('dataset')} / {sel_row.get('coin')}")
                else:
                    st.info("Click a row to display the heatmap. Use the sidebar refresh button to reload inventory.")

                return sel_row

            # Helper for deletion operations
            def _render_deletion_tools(dataset_rows: list, dataset_key: str, dataset_label: str, sel_row: dict | None = None):
                """Render deletion tools for a specific dataset."""
                import shutil

                if not dataset_rows:
                    return

                # Get all coins in this dataset
                available_coins = sorted({str(r.get("coin", "")).strip().upper() for r in dataset_rows if str(r.get("coin", "")).strip()})
                
                if not available_coins:
                    return

                with st.expander("🗑️ Deletion Tools", expanded=False):
                    # Get selected coin from table if available
                    selected_coin_from_table = None
                    if sel_row and isinstance(sel_row, dict):
                        selected_coin_from_table = str(sel_row.get("coin", "")).strip().upper()

                    st.info(f"**{len(available_coins)} coins** in {dataset_label}")

                    # 0. Quick delete selected row if available
                    if selected_coin_from_table and selected_coin_from_table in available_coins:
                        st.subheader("Quick delete: Selected row", divider="red")
                        st.caption(f"Delete currently selected coin: **{selected_coin_from_table}**")
                        if st.button(f"🗑️ Delete {selected_coin_from_table}", key=f"market_data_delete_selected_row_{dataset_key}", type="secondary"):
                            try:
                                # Use dataset name and coin name EXACTLY as they appear in the row
                                actual_dataset = str(sel_row.get("dataset", "")).strip()
                                actual_dataset_lower = actual_dataset.lower()
                                actual_coin = str(sel_row.get("coin", "")).strip()
                                
                                coin_dir = get_exchange_raw_root_dir(str(exchange).lower()) / actual_dataset / actual_coin
                                if coin_dir.exists():
                                    shutil.rmtree(coin_dir)
                                    st.success(f"✅ Deleted {actual_coin}")
                                else:
                                    st.error(f"❌ Directory not found: {coin_dir}")
                                
                                # Also delete 1m_src index if deleting 1m dataset
                                if actual_dataset_lower in ("1m", "candles_1m"):
                                    src_dir = get_exchange_raw_root_dir(str(exchange).lower()) / "1m_src" / actual_coin
                                    if src_dir.exists():
                                        shutil.rmtree(src_dir)
                                
                                # Clear cache
                                _cache_key = f"market_data_have_table_cached_{str(exchange).lower()}"
                                st.session_state.pop(_cache_key, None)
                                _trows_key = f"market_data_trows_{dataset_key}"
                                st.session_state.pop(_trows_key, None)
                                st.session_state.pop("market_data_heatmap_sel", None)
                                
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error deleting: {e}")
                        st.divider()

                    # 1. Delete selected coins
                    st.subheader("1️⃣ Delete selected coins", divider="red")
                    sel_coins_key = f"market_data_delete_sel_coins_{dataset_key}"
                    
                    # Create options with "ALL" at the top
                    coin_options = ["🔴 DELETE ALL COINS"] + available_coins
                    selected_for_delete = st.multiselect(
                        f"Select coins to delete:",
                        options=coin_options,
                        key=sel_coins_key,
                        help="Multi-select coins to delete all their data from this dataset. Use '🔴 DELETE ALL COINS' to delete entire dataset."
                    )
                    
                    # Handle "DELETE ALL" option
                    if "🔴 DELETE ALL COINS" in selected_for_delete:
                        # If "DELETE ALL" is selected, that's the only thing we delete
                        selected_for_delete = available_coins
                    else:
                        # Filter out the ALL option if it was there
                        selected_for_delete = [c for c in selected_for_delete if c != "🔴 DELETE ALL COINS"]

                    if selected_for_delete:
                        # Calculate size preview
                        total_size = 0
                        total_files = 0
                        for r in dataset_rows:
                            if str(r.get("coin", "")).strip().upper() in selected_for_delete:
                                total_size += int(r.get("total_bytes", 0) or 0)
                                total_files += int(r.get("n_files", 0) or 0)

                        size_str = _fmt_bytes(total_size) if total_size else "0 B"
                        st.caption(f"📊 Preview: {len(selected_for_delete)} coins, {total_files} files, {size_str}")

                        if st.button("🗑️ Delete selected coins", key=f"market_data_delete_selected_btn_{dataset_key}", type="secondary"):
                            try:
                                deleted_count = 0
                                # Use actual dataset name from first row
                                actual_dataset = str(dataset_rows[0].get("dataset", "")).strip() if dataset_rows else dataset_key
                                actual_dataset_lower = actual_dataset.lower()
                                
                                for r in dataset_rows:
                                    coin = str(r.get("coin", "")).strip().upper()
                                    if coin not in selected_for_delete:
                                        continue
                                    
                                    # Use actual coin name (original case) from row
                                    actual_coin = str(r.get("coin", "")).strip()
                                    coin_dir = get_exchange_raw_root_dir(str(exchange).lower()) / actual_dataset / actual_coin
                                    if coin_dir.exists():
                                        shutil.rmtree(coin_dir)
                                        deleted_count += 1
                                    
                                    # Also delete 1m_src index if deleting 1m dataset
                                    if actual_dataset_lower in ("1m", "candles_1m"):
                                        src_dir = get_exchange_raw_root_dir(str(exchange).lower()) / "1m_src" / actual_coin
                                        if src_dir.exists():
                                            shutil.rmtree(src_dir)

                                # Clear cache to refresh inventory
                                _cache_key = f"market_data_have_table_cached_{str(exchange).lower()}"
                                st.session_state.pop(_cache_key, None)
                                _trows_key = f"market_data_trows_{dataset_key}"
                                st.session_state.pop(_trows_key, None)

                                st.success(f"✅ Deleted {deleted_count} coin directories ({size_str})")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error deleting: {e}")

                    st.divider()

                    # 2. Delete older than date
                    st.subheader("2️⃣ Delete data older than date", divider="orange")
                    st.caption("⚠️ Deletes individual files (days) older than the cutoff date, keeps coin directories")
                    
                    cutoff_date = st.date_input(
                        "Select cutoff date:",
                        value=None,
                        key=f"market_data_delete_older_date_{dataset_key}",
                        help=f"Deletes files dated before this date (e.g., 20241205.npz < 2025-01-01)"
                    )

                    if cutoff_date:
                        cutoff_str = cutoff_date.strftime("%Y%m%d")
                        
                        # Determine which coins to check based on selections (combine from both sources!)
                        coins_to_check = set()
                        
                        if selected_coin_from_table:
                            coins_to_check.add(selected_coin_from_table)
                        
                        # Also include selected coins from section 1
                        if selected_for_delete and "🔴 DELETE ALL COINS" not in selected_for_delete:
                            coins_to_check.update(selected_for_delete)
                        
                        # Initialize variables OUTSIDE the if block
                        from pathlib import Path
                        would_delete_files = 0
                        would_delete_size = 0
                        affected_coins_info = []
                        debug_info = []
                        scope_label = "no coins selected"
                        
                        if coins_to_check:
                            scope_label = f"{len(coins_to_check)} selected coins" if len(coins_to_check) > 1 else f"{list(coins_to_check)[0]}"
                            
                            for r in dataset_rows:
                                coin = str(r.get("coin", "")).strip().upper()
                                if coin not in coins_to_check:
                                    continue
                                
                                actual_coin = str(r.get("coin", "")).strip()
                                actual_dataset = str(r.get("dataset", "")).strip()
                                coin_dir = get_exchange_raw_root_dir(str(exchange).lower()) / actual_dataset / actual_coin
                                
                                debug_info.append(f"Checking: {coin_dir} (exists={coin_dir.exists()})")
                                
                                if not coin_dir.exists():
                                    continue
                                
                                coin_old_files = 0
                                coin_old_size = 0
                                all_files = []
                                
                                # Scan ALL files in coin directory
                                try:
                                    for file_path in coin_dir.iterdir():
                                        if file_path.is_file():
                                            all_files.append(file_path.name)
                                            # Extract date from filename (try multiple patterns)
                                            fname = file_path.stem  # Without extension
                                            
                                            file_date = None
                                            
                                            # Pattern 1: 20241205.npz (8 digits)
                                            if len(fname) == 8 and fname.isdigit():
                                                file_date = fname
                                            # Pattern 2: 20241205-16.lz4 (8 digits + hyphen + hour)
                                            elif len(fname) >= 8 and fname[:8].isdigit():
                                                file_date = fname[:8]
                                            # Pattern 3: 2026-02-05.npz (ISO format YYYY-MM-DD)
                                            elif len(fname) == 10 and fname[4] == '-' and fname[7] == '-':
                                                # Convert YYYY-MM-DD to YYYYMMDD
                                                file_date = fname.replace('-', '')
                                            
                                            if file_date and file_date < cutoff_str:
                                                coin_old_files += 1
                                                coin_old_size += file_path.stat().st_size
                                except Exception as e:
                                    debug_info.append(f"Error scanning {coin_dir}: {e}")
                                
                                debug_info.append(f"  Files: {len(all_files)}, Old: {coin_old_files}, Sample: {all_files[:3]}")
                                
                                if coin_old_files > 0:
                                    would_delete_files += coin_old_files
                                    would_delete_size += coin_old_size
                                    affected_coins_info.append((coin, coin_old_files, coin_old_size))
                        
                        # Always show preview
                        size_str_old = _fmt_bytes(would_delete_size) if would_delete_size else "0 B"
                        
                        preview_container = st.container()
                        with preview_container:
                            st.info(f"📊 Cutoff: **{cutoff_date.strftime('%Y-%m-%d')}** | Scope: {scope_label}")
                            st.metric(
                                label="Files to delete",
                                value=f"{would_delete_files} files",
                                delta=f"{size_str_old}"
                            )
                        
                        if would_delete_files > 0:
                            # Show affected coins directly (no expander)
                            st.subheader(f"📋 Affected coins ({len(affected_coins_info)})", divider="gray")
                            for coin, nfiles, size in sorted(affected_coins_info, key=lambda x: -x[2]):
                                st.caption(f"• {coin}: {nfiles} files, {_fmt_bytes(size)}")

                            if st.button("🗑️ Delete old files", key=f"market_data_delete_older_btn_{dataset_key}", type="secondary"):
                                try:
                                    deleted_count = 0
                                    deleted_size = 0
                                    coins_deleted_days: dict[str, set[str]] = {}  # Track deleted days per coin
                                    
                                    for r in dataset_rows:
                                        coin = str(r.get("coin", "")).strip().upper()
                                        if coin not in coins_to_check:
                                            continue
                                        
                                        actual_coin = str(r.get("coin", "")).strip()
                                        actual_dataset = str(r.get("dataset", "")).strip()
                                        actual_dataset_lower = actual_dataset.lower()
                                        coin_dir = get_exchange_raw_root_dir(str(exchange).lower()) / actual_dataset / actual_coin
                                        
                                        if not coin_dir.exists():
                                            continue
                                        
                                        # Delete old files (all file types)
                                        for file_path in coin_dir.iterdir():
                                            if not file_path.is_file():
                                                continue
                                            fname = file_path.stem
                                            
                                            file_date = None
                                            
                                            # Pattern 1: 20241205 (8 digits)
                                            if len(fname) == 8 and fname.isdigit():
                                                file_date = fname
                                            # Pattern 2: 20241205-16 (8 digits + hyphen + hour)
                                            elif len(fname) >= 8 and fname[:8].isdigit():
                                                file_date = fname[:8]
                                            # Pattern 3: 2026-02-05 (ISO format YYYY-MM-DD)
                                            elif len(fname) == 10 and fname[4] == '-' and fname[7] == '-':
                                                file_date = fname.replace('-', '')
                                            
                                            if file_date and file_date < cutoff_str:
                                                file_size = file_path.stat().st_size
                                                file_path.unlink()
                                                deleted_count += 1
                                                deleted_size += file_size
                                                
                                                # Track deleted day for index update
                                                if actual_dataset_lower in ("1m", "candles_1m"):
                                                    if actual_coin not in coins_deleted_days:
                                                        coins_deleted_days[actual_coin] = set()
                                                    coins_deleted_days[actual_coin].add(file_date)
                                    
                                    # Remove deleted days from 1m_src indexes
                                    updated_count = 0
                                    if coins_deleted_days:
                                        for coin, deleted_days in coins_deleted_days.items():
                                            removed = remove_days_from_index(
                                                exchange=str(exchange).lower(),
                                                coin=coin,
                                                days_to_remove=deleted_days
                                            )
                                            if removed > 0:
                                                updated_count += 1

                                    # Clear cache
                                    _cache_key = f"market_data_have_table_cached_{str(exchange).lower()}"
                                    st.session_state.pop(_cache_key, None)
                                    _trows_key = f"market_data_trows_{dataset_key}"
                                    st.session_state.pop(_trows_key, None)

                                    index_msg = f" (updated {updated_count} source indexes)" if updated_count > 0 else ""
                                    st.success(f"✅ Deleted {deleted_count} files ({_fmt_bytes(deleted_size)}){index_msg}")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Error deleting: {e}")
                                    import traceback
                                    st.code(traceback.format_exc())
                        else:
                            st.info(f"✅ No files older than {cutoff_str} in {scope_label}")

                    else:
                        st.warning("⚠️ Select a coin or use section 1️⃣ to select coins before using date-based deletion")

                    st.divider()

                    # 3. Clear entire dataset (for this specific tab)
                    st.subheader("3️⃣ Clear entire dataset", divider="red")
                    st.warning(f"⚠️ This will delete ALL {dataset_label} data. This action cannot be undone!")

                    if st.button(f"🗑️ Clear all {dataset_label}", key=f"market_data_clear_dataset_{dataset_key}", type="secondary"):
                        try:
                            dataset_dir = get_exchange_raw_root_dir(str(exchange).lower()) / dataset_key
                            
                            # Also clear 1m_src indexes for each coin if clearing 1m dataset
                            dataset_key_lower = dataset_key.lower()
                            cleaned_indexes = 0
                            if dataset_key_lower in ("1m", "candles_1m") and dataset_dir.exists():
                                # Get list of coins before deleting
                                coins_in_dataset = [d.name for d in dataset_dir.iterdir() if d.is_dir()]
                                for coin in coins_in_dataset:
                                    src_dir = get_exchange_raw_root_dir(str(exchange).lower()) / "1m_src" / coin
                                    if src_dir.exists():
                                        shutil.rmtree(src_dir)
                                        cleaned_indexes += 1
                            
                            # Now delete the dataset
                            if dataset_dir.exists():
                                shutil.rmtree(dataset_dir)
                            
                            st.session_state.pop(f"market_data_have_table_cached_{str(exchange).lower()}", None)
                            _trows_key = f"market_data_trows_{dataset_key}"
                            st.session_state.pop(_trows_key, None)
                            
                            index_msg = f" (cleaned {cleaned_indexes} source indexes)" if cleaned_indexes > 0 else ""
                            st.success(f"✅ {dataset_label} dataset cleared{index_msg}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")

            # Render each dataset tab
            with tab_1m:
                sel_row_1m = _render_dataset_table(rows_1m, "1m", "1m")
                _render_deletion_tools(rows_1m, "1m", "1m candles", sel_row_1m)

            with tab_1m_api:
                sel_row_1m_api = _render_dataset_table(rows_1m_api, "1m_api", "1m_api")
                _render_deletion_tools(rows_1m_api, "1m_api", "1m API", sel_row_1m_api)

            with tab_l2book:
                sel_row_l2book = _render_dataset_table(rows_l2book, "l2Book", "l2book")
                _render_deletion_tools(rows_l2book, "l2book", "l2Book", sel_row_l2book)

            with tab_pb7_cache:
                pb7_rows = summarize_pb7_cache_inventory(str(exchange).lower(), limit=2000)
                if not pb7_rows:
                    st.info("No PB7 cache files found for this exchange (expected path: pb7/caches/ohlcv/<exchange>/...).")
                else:
                    import pandas as pd
                    df_pb7 = pd.DataFrame(pb7_rows)
                    if not df_pb7.empty:
                        total_files = int(df_pb7["n_files"].sum()) if "n_files" in df_pb7.columns else 0
                        total_bytes = int(df_pb7["total_bytes"].sum()) if "total_bytes" in df_pb7.columns else 0
                        n_coins = int(df_pb7["coin"].nunique()) if "coin" in df_pb7.columns else 0
                        n_tf = int(df_pb7["timeframe"].nunique()) if "timeframe" in df_pb7.columns else 0

                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("timeframes", n_tf)
                        c2.metric("coins", n_coins)
                        c3.metric("files", total_files)
                        c4.metric("size", _fmt_bytes(total_bytes))

                    if "total_bytes" in df_pb7.columns:
                        df_pb7["size_mb"] = (df_pb7["total_bytes"].astype(float) / (1024.0 * 1024.0)).round(2)
                        df_pb7 = df_pb7.drop(columns=["total_bytes"])

                    col_cfg = {
                        "size_mb": st.column_config.NumberColumn("size", format="%.2f MB")
                    }
                    st.dataframe(
                        df_pb7,
                        use_container_width=True,
                        hide_index=True,
                        column_config=col_cfg,
                        key="market_data_pb7_cache_table",
                    )
                    st.caption("Read-only view of PB7 cache inventory from pb7/caches/ohlcv.")

            # Get the selected row from any of the tabs
            sel_row = sel_row_1m or sel_row_1m_api or sel_row_l2book

            def _render_gap_heatmap() -> None:
                if sel_row:
                    r = sel_row
                    ex = str(exchange).lower()
                    ds = str(r.get("dataset") or "")
                    cn = str(r.get("coin") or "")
                    ds_l = ds.strip().lower()

                    # For candles datasets: show only the gap from l2Book->today.
                    start_day = None
                    end_day = _date.today().strftime("%Y%m%d")
                    if ds_l not in ("l2book", "1m", "candles_1m", "1m_api", "candles_1m_api"):
                        l2 = get_daily_hour_coverage_for_dataset(ex, "l2Book", cn)
                        l2_newest = str(l2.get("newest_day") or "") if isinstance(l2, dict) else ""
                        if l2_newest:
                            start_day = l2_newest

                    if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
                        # Candles view:
                        # - 1m: 1 row per day, 24 hour cells

                        # Choose presence resolution
                        if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
                            day_counts = {}
                            if ds_l in ("1m", "candles_1m") and ex == "hyperliquid":
                                day_counts = get_daily_source_counts_for_range(
                                    exchange=ex,
                                    coin=cn,
                                    start_day=start_day,
                                    end_day=end_day,
                                    lag_minutes=_missing_lag_minutes,
                                    cutoff_ts_ms=_latest_1m_api_cutoff_ts_ms(cn),
                                )

                            if isinstance(day_counts, dict) and day_counts:
                                try:
                                    oldest_s = min(day_counts.keys())
                                    newest_s = max(day_counts.keys())
                                    dt0 = _datetime.strptime(oldest_s, "%Y%m%d").date()
                                    dt1 = _date.today()
                                    if dt1 < _datetime.strptime(newest_s, "%Y%m%d").date():
                                        dt1 = _datetime.strptime(newest_s, "%Y%m%d").date()
                                except Exception:
                                    dt0 = None
                                    dt1 = None

                                if dt0 and dt1:
                                    years: list[int] = []
                                    cur = dt0
                                    while cur <= dt1:
                                        y = int(cur.strftime("%Y"))
                                        if y not in years and y >= 2023:
                                            years.append(y)
                                        cur = cur + _timedelta(days=1)
                                    years = sorted(years)
                                    max_days = 366
                                    z = []
                                    text = []
                                    for y in years:
                                        days_in_year = 366 if calendar.isleap(int(y)) else 365
                                        row: list[float | None] = [None] * max_days
                                        row_text = [""] * max_days
                                        cur_day = dt0
                                        while cur_day <= dt1:
                                            day_s = cur_day.strftime("%Y%m%d")
                                            if not day_s.startswith(str(y)):
                                                cur_day = cur_day + _timedelta(days=1)
                                                continue
                                            try:
                                                doy = cur_day.timetuple().tm_yday
                                            except Exception:
                                                cur_day = cur_day + _timedelta(days=1)
                                                continue
                                            idx = doy - 1
                                            if 0 <= idx < days_in_year:
                                                counts = day_counts.get(day_s) or {}
                                                api = int(counts.get("api") or 0)
                                                l2b = int(counts.get("l2Book_mid") or 0)
                                                oth = int(counts.get("other_exchange") or 0)
                                                miss = int(counts.get("missing") or 0)
                                                if not counts:
                                                    miss = 1440
                                                if miss > 0:
                                                    row[idx] = 0.0
                                                elif oth > 0:
                                                    row[idx] = 0.75 if oth < 5 else 0.5
                                                else:
                                                    row[idx] = 1.0
                                                row_text[idx] = (
                                                    f"{day_s} | api={api} l2Book={l2b} other={oth} missing={miss}"
                                                )
                                            cur_day = cur_day + _timedelta(days=1)
                                        z.append(row)
                                        text.append(row_text)

                                    fig = go.Figure(
                                        data=go.Heatmap(
                                            z=z,
                                            x=list(range(1, max_days + 1)),
                                            y=[str(y) for y in years],
                                            text=text,
                                            hovertemplate="%{text}<extra></extra>",
                                            colorscale=[
                                                [0.0, "#b23b3b"],
                                                [0.49, "#b23b3b"],
                                                [0.5, "#ef6c00"],
                                                [0.74, "#ef6c00"],
                                                [0.75, "#7cb342"],
                                                [0.99, "#7cb342"],
                                                [1.0, "#2e7d32"],
                                            ],
                                            zmin=0,
                                            zmax=1,
                                            showscale=False,
                                            xgap=1,
                                            ygap=1,
                                        )
                                    )
                                    fig.update_layout(
                                        height=120 + (len(years) * 26),
                                        margin=dict(l=10, r=10, t=20, b=20),
                                        xaxis=dict(tickangle=-45, automargin=True, showgrid=False),
                                        yaxis=dict(
                                            autorange="reversed",
                                            showgrid=False,
                                            type="category",
                                            categoryorder="array",
                                            categoryarray=[str(y) for y in years],
                                            tickmode="array",
                                            tickvals=[str(y) for y in years],
                                            ticktext=[str(y) for y in years],
                                        ),
                                    )
                                    st.markdown(
                                        "<span style='display:inline-block;padding:6px;border-radius:4px;background:#2e7d32;color:#fff;margin-right:8px;'>HL only</span>"
                                        "<span style='display:inline-block;padding:6px;border-radius:4px;background:#7cb342;color:#fff;margin-right:8px;'>other_exchange &lt; 5 min</span>"
                                        "<span style='display:inline-block;padding:6px;border-radius:4px;background:#ef6c00;color:#fff;margin-right:8px;'>other_exchange ≥ 5 min</span>"
                                        "<span style='display:inline-block;padding:6px;border-radius:4px;background:#b23b3b;color:#fff;margin-right:8px;'>missing minutes</span>",
                                        unsafe_allow_html=True,
                                    )
                                    st.caption("Overview (days). Select a month below to inspect minutes.")
                                    st.plotly_chart(fig, use_container_width=True)

                                    # Build month list from date range
                                    month_list: list[str] = []
                                    if dt0 and dt1:
                                        cur_m = dt0.replace(day=1)
                                        end_m = dt1.replace(day=1)
                                        while cur_m <= end_m:
                                            month_list.append(cur_m.strftime("%Y-%m"))
                                            # advance to next month
                                            if cur_m.month == 12:
                                                cur_m = cur_m.replace(year=cur_m.year + 1, month=1)
                                            else:
                                                cur_m = cur_m.replace(month=cur_m.month + 1)
                                    if not month_list:
                                        month_list = [dt0.strftime("%Y-%m")] if dt0 else []
                                    sel_key = f"market_data_1m_month_{cn}"
                                    if month_list and sel_key not in st.session_state:
                                        st.session_state[sel_key] = month_list[-1]
                                    cur_month = st.session_state.get(sel_key) or (month_list[-1] if month_list else "")
                                    cur_idx = month_list.index(cur_month) if cur_month in month_list else len(month_list) - 1

                                    def _go_prev(_key=sel_key, _ml=month_list, _ci=cur_idx):
                                        if _ci > 0:
                                            st.session_state[_key] = _ml[_ci - 1]

                                    def _go_next(_key=sel_key, _ml=month_list, _ci=cur_idx):
                                        if _ci < len(_ml) - 1:
                                            st.session_state[_key] = _ml[_ci + 1]

                                    # Small chevron buttons like Strategy Explorer
                                    c_prev, c_sel, c_next = st.columns([0.04, 0.92, 0.04], vertical_alignment="bottom")
                                    with c_prev:
                                        st.button(":material/chevron_left:", key=f"{sel_key}_prev", disabled=cur_idx <= 0, on_click=_go_prev)
                                    with c_sel:
                                        sel_month = st.selectbox(
                                            "Select month for minute view",
                                            options=month_list,
                                            index=cur_idx,
                                            key=sel_key,
                                        )
                                    with c_next:
                                        st.button(":material/chevron_right:", key=f"{sel_key}_next", disabled=cur_idx >= len(month_list) - 1, on_click=_go_next)
                                    if sel_month:
                                        import calendar as _cal
                                        _sm_year = int(sel_month[:4])
                                        _sm_mon = int(sel_month[5:7])
                                        _last_day = _cal.monthrange(_sm_year, _sm_mon)[1]
                                        start_day = f"{_sm_year:04d}{_sm_mon:02d}01"
                                        end_day = f"{_sm_year:04d}{_sm_mon:02d}{_last_day:02d}"

                            hp = get_minute_presence_for_dataset(
                                ex,
                                ds,
                                cn,
                                start_day=start_day,
                                end_day=end_day,
                            )
                            present = hp.get("days") if isinstance(hp, dict) else {}
                            if not isinstance(present, dict) or not present:
                                st.info("No minute candles found for this selection.")
                                return

                        oldest_s = str(hp.get("oldest_day") or "")
                        newest_s = str(hp.get("newest_day") or "")
                        try:
                            dt0 = _datetime.strptime(oldest_s, "%Y%m%d").date()
                            dt1 = _datetime.strptime(newest_s, "%Y%m%d").date()
                        except Exception:
                            st.info("No date range found for this selection.")
                            return

                        colorscale = [
                            [0.0, "#b23b3b"],
                            [1.0, "#2e7d32"],
                        ]

                        if ds_l in ("1m", "candles_1m", "1m_api", "candles_1m_api"):
                            # Days split into two 12-hour rows (00-11, 12-23)
                            days_list: list[_date] = []
                            cur = dt0
                            while cur <= dt1:
                                days_list.append(cur)
                                cur = cur + _timedelta(days=1)

                            z = []
                            text = []
                            y_labels = []

                            # src -> code mapping (discrete)
                            src_code = {
                                None: 0,
                                "missing": 0,
                                "filled_gap": 1,
                                "api": 2,
                                "best": 3,
                                "other_exchange": 4,
                                "binance_perp_usdt": 4,
                                "l2Book_mid": 5,
                            }

                            # colorscale: 0 missing (red), 1 filled (purple), 2 api (green), 3 best (teal), 4 other_exchange (orange), 5 l2book (blue)
                            colorscale = [
                                [0.0, "#b23b3b"],
                                [0.2, "#6a1b9a"],
                                [0.4, "#2e7d32"],
                                [0.6, "#00897b"],
                                [0.8, "#ef6c00"],
                                [1.0, "#1e88e5"],
                            ]

                            for d in days_list:
                                day_s = d.strftime("%Y%m%d")
                                # present[day_s] is {HH: {MM: src}}
                                hours_map = present.get(day_s) if isinstance(present.get(day_s), dict) else {}
                                hours_map = hours_map if isinstance(hours_map, dict) else {}

                                for block_start in (0, 12):
                                    row = []
                                    row_text = []
                                    for h in range(block_start, block_start + 12):
                                        hh = f"{h:02d}"
                                        mins_map = hours_map.get(hh) or {}
                                        mins_map = mins_map if isinstance(mins_map, dict) else {}
                                        for minute in range(60):
                                            src = mins_map.get(minute)
                                            code = int(src_code.get(str(src), src_code.get(src, 0)))
                                            row.append(code)
                                            # hover text per minute
                                            hhmm = f"{h:02d}:{minute:02d}"
                                            src_label = str(src) if src is not None else "missing"
                                            row_text.append(f"{day_s} {hhmm} ({src_label})")
                                    z.append(row)
                                    text.append(row_text)
                                    y_labels.append(f"{day_s} {block_start:02d}-{block_start+11:02d}")

                            if not z:
                                st.info("No minute presence found for this selection.")
                                return

                            fig = go.Figure(
                                data=go.Heatmap(
                                    z=z,
                                    x=list(range(720)),
                                    y=[str(y) for y in y_labels],
                                    text=text,
                                    hovertemplate="%{text}<extra></extra>",
                                    colorscale=colorscale,
                                    zmin=0,
                                    zmax=5,
                                    showscale=False,
                                    xgap=1,
                                    ygap=1,
                                )
                            )
                            fig.update_layout(
                                height=max(300, 80 + (len(y_labels) * 18)),
                                margin=dict(l=10, r=10, t=20, b=20),
                                xaxis=dict(tickmode="array", tickvals=list(range(0, 720, 60)), ticktext=[f"{x:02d}h" for x in range(0, 12)]),
                                yaxis=dict(autorange="reversed", showgrid=False),
                            )
                            st.plotly_chart(fig, use_container_width=True)


                    # Default view (incl. l2Book): render year rows with day-of-year columns.
                    cov = get_daily_hour_coverage_for_dataset(
                        ex,
                        ds,
                        cn,
                        start_day=start_day,
                        end_day=end_day,
                    )
                    days = cov.get("days") if isinstance(cov, dict) else []
                    if isinstance(days, list) and days:
                        years: list[int] = []
                        for d in days:
                            try:
                                y = int(str(d.get("day") or "")[0:4])
                                if y not in years:
                                    years.append(y)
                            except Exception:
                                continue
                        years = sorted(years)
                        max_days = 366
                        z = []
                        text = []
                        for y in years:
                            row = [None] * max_days
                            row_text = [""] * max_days
                            for d in days:
                                day_s = str(d.get("day") or "")
                                if not day_s.startswith(str(y)):
                                    continue
                                try:
                                    dt = _datetime.strptime(day_s, "%Y%m%d").date()
                                    doy = dt.timetuple().tm_yday
                                except Exception:
                                    continue
                                status = int(d.get("status") or 0)
                                hrs = int(d.get("hours") or 0)
                                idx = doy - 1
                                if 0 <= idx < max_days:
                                    row[idx] = status
                                    row_text[idx] = f"{day_s} | hours={hrs}/24"
                            z.append(row)
                            text.append(row_text)

                        fig = go.Figure(
                            data=go.Heatmap(
                                z=z,
                                x=list(range(1, max_days + 1)),
                                y=[str(y) for y in years],
                                text=text,
                                hovertemplate="%{text}<extra></extra>",
                                colorscale=[
                                    [0.0, "#b23b3b"],
                                    [0.5, "#c9a227"],
                                    [1.0, "#2e7d32"],
                                ],
                                zmin=0,
                                zmax=2,
                                showscale=False,
                                xgap=1,
                                ygap=1,
                            )
                        )
                        fig.update_layout(
                            height=120 + (len(years) * 26),
                            margin=dict(l=10, r=10, t=20, b=20),
                            xaxis=dict(tickangle=-45, automargin=True, showgrid=False),
                            yaxis=dict(autorange="reversed", showgrid=False),
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No day range found for this selection.")

            if sel_row:
                gaps_active = False
                try:
                    gaps_active = bool(list_jobs(states=["pending", "running"], limit=1))
                except Exception:
                    gaps_active = False

                if gaps_active and _supports_fragment_run_every():
                    @st.fragment(run_every=5)
                    def _gap_fragment():
                        # Stop auto-refresh when no jobs remain
                        try:
                            still_active = bool(list_jobs(states=["pending", "running"], limit=1))
                        except Exception:
                            still_active = False
                        if not still_active:
                            # Clear table caches so next full render picks up final state
                            for _k in list(st.session_state.keys()):
                                if str(_k).startswith("market_data_have_table_cached_") or str(_k).startswith("market_data_trows_"):
                                    st.session_state.pop(_k, None)
                            st.rerun()
                        _render_gap_heatmap()
                else:
                    @st.fragment
                    def _gap_fragment():
                        _render_gap_heatmap()

                _gap_fragment()

    with tab_log:
        view_log_filtered("MarketData")


# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

set_page_config("Market Data")
render_header_with_guide(
    "Market Data",
    guide_callback=lambda: _help_modal(default_topic="Market Data"),
    guide_key="market_data_guide_btn",
)

with st.sidebar:
    if st.button(":material/refresh:", help="Reload page"):
        for _k in list(st.session_state.keys()):
            if str(_k).startswith("market_data_have_table_cached_") or str(_k).startswith("market_data_trows_"):
                st.session_state.pop(_k, None)
        st.rerun()

    _pid = read_worker_pid()
    _running = bool(_pid and is_pid_running(int(_pid)))
    _worker_icon = ":material/stop_circle:" if _running else ":material/play_disabled:"
    _worker_help = "Worker running — click to stop" if _running else "Worker stopped"
    if st.button(_worker_icon, key="market_data_sidebar_worker_toggle", help=_worker_help, disabled=not _running):
        try:
            if _pid and is_pid_running(int(_pid)):
                os.kill(int(_pid), signal.SIGTERM)
            clear_worker_pid()
            st.rerun()
        except Exception as e:
            error_popup(str(e))

view_market_data()
