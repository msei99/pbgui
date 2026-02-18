import streamlit as st
import pbgui_help
from pbgui_func import set_page_config, is_session_state_not_initialized, info_popup, error_popup, is_authenticted, get_navi_paths, render_header_with_guide
from PBCoinData import CoinData, compute_coin_name
from Exchange import V7
from datetime import datetime
from pathlib import Path


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
def _help_modal(default_topic: str = "Coin Data"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="coin_data_help_lang")
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
        key="coin_data_help_sel",
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


def _refresh_single_exchange(coindata: CoinData, exchange_id: str):
    coindata.fetch_ccxt_markets(exchange_id)
    markets = coindata.load_ccxt_markets(exchange_id)
    coindata.fetch_copy_trading_symbols(exchange_id, markets)
    coindata.build_mapping(exchange_id)
    coindata.update_prices(exchange_id)


def _selected_row_idx(event) -> int | None:
    if not event:
        return None
    selection = event.get("selection") if isinstance(event, dict) else getattr(event, "selection", None)
    if not selection:
        return None
    rows = selection.get("rows", []) if isinstance(selection, dict) else getattr(selection, "rows", [])
    if rows:
        try:
            return int(rows[0])
        except Exception:
            return None
    return None


def _format_age(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h ago"
    return f"{int(seconds // 86400)}d ago"


def _format_ts(ts: float | None) -> str:
    if ts is None:
        return "n/a"
    now_ts = datetime.now().timestamp()
    dt = datetime.fromtimestamp(ts)
    age = max(0.0, now_ts - ts)
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} ({_format_age(age)})"


def _file_mtime(path: Path) -> float | None:
    if path.exists():
        return path.stat().st_mtime
    return None


def _max_price_ts(mapping_rows: list[dict]) -> float | None:
    max_ts = None
    for row in mapping_rows:
        ts = row.get("price_ts")
        if ts is None:
            continue
        try:
            ts_f = float(ts)
        except Exception:
            continue
        # Normalize ms timestamps to seconds if needed
        if ts_f > 1_000_000_000_000:
            ts_f = ts_f / 1000.0
        if max_ts is None or ts_f > max_ts:
            max_ts = ts_f
    return max_ts


def view_coindata():
    # Navigation
    with st.sidebar:
        if st.button(":material/settings:"):
            st.session_state.setup_coindata = True
            st.rerun()
        if st.button(":material/sync:", help="Refresh selected exchange"):
            with st.spinner(f"Refreshing {coindata.exchange}..."):
                try:
                    _refresh_single_exchange(coindata, coindata.exchange)
                    st.success(f"Refreshed {coindata.exchange}")
                except Exception as e:
                    st.error(f"Failed to refresh {coindata.exchange}: {e}")
            st.rerun()
        if st.button(":material/sync_alt:", help="Refresh all exchanges"):
            exchanges = V7.list()
            total = len(exchanges)
            errors = []
            status_placeholder = st.empty()
            progress_bar = st.progress(0, text="Starting refresh...")

            for idx, exchange in enumerate(exchanges, start=1):
                status_placeholder.info(f"Refreshing {exchange} ({idx}/{total})...")
                try:
                    _refresh_single_exchange(coindata, exchange)
                except Exception as e:
                    errors.append(f"{exchange}: {e}")

                progress_pct = int((idx / total) * 100)
                progress_bar.progress(progress_pct, text=f"Refreshing exchanges... {idx}/{total}")

            progress_bar.empty()
            status_placeholder.empty()

            if errors:
                st.error("Some exchanges failed to refresh:\n" + "\n".join(errors))
            else:
                st.success("All exchanges refreshed")
            st.rerun()
        if st.button(":material/cloud_sync:", help="Refresh CoinMarketCap data"):
            with st.spinner("Refreshing CoinMarketCap data..."):
                try:
                    coindata.fetch_data()
                    coindata.save_data()
                    coindata.load_data()
                    coindata.fetch_metadata()
                    coindata.save_metadata()
                    coindata.load_metadata()
                    _refresh_single_exchange(coindata, coindata.exchange)
                    st.success("CoinMarketCap data refreshed")
                except Exception as e:
                    st.error(f"Failed to refresh CoinMarketCap data: {e}")
            st.rerun()
    # Init session states for keys
    if "view_coindata_exchange" in st.session_state:
        if st.session_state.view_coindata_exchange != coindata.exchange:
            coindata.exchange = st.session_state.view_coindata_exchange
    else:
        st.session_state.view_coindata_exchange = coindata.exchange
    
    if "view_coindata_market_cap" in st.session_state:
        if st.session_state.view_coindata_market_cap != coindata.market_cap:
            coindata.market_cap = st.session_state.view_coindata_market_cap
    else:
        st.session_state.view_coindata_market_cap = float(coindata.market_cap)
    
    if "view_coindata_vol_mcap" in st.session_state:
        if st.session_state.view_coindata_vol_mcap != coindata.vol_mcap:
            coindata.vol_mcap = st.session_state.view_coindata_vol_mcap
    else:
        st.session_state.view_coindata_vol_mcap = float(coindata.vol_mcap)

    if "edit_coindata_tags" in st.session_state:
        if st.session_state.edit_coindata_tags != coindata.tags:
            coindata.tags = st.session_state.edit_coindata_tags
    else:
        st.session_state.edit_coindata_tags = coindata.tags
    # Load mapping for selected exchange first (needed for quote/tag controls)
    mapping_rows = coindata.load_exchange_mapping(coindata.exchange)
    if not mapping_rows:
        with st.spinner(f"Building mapping for {coindata.exchange}..."):
            try:
                coindata.build_mapping(coindata.exchange)
                coindata.update_prices(coindata.exchange)
            except Exception:
                pass
        mapping_rows = coindata.load_exchange_mapping(coindata.exchange)
        if not mapping_rows:
            st.warning(
                f"No mapping data available for {coindata.exchange}. "
                "Please refresh market data and try again."
            )
            return

    # Hyperliquid self-heal in UI: if mapping exists but contains no HIP-3 rows,
    # rebuild once so newly available stock-perp markets become visible.
    if coindata.exchange == "hyperliquid" and not any(r.get("is_hip3", False) for r in mapping_rows):
        with st.spinner("Hyperliquid mapping has no HIP-3 symbols, rebuilding once..."):
            try:
                _refresh_single_exchange(coindata, "hyperliquid")
                mapping_rows = coindata.load_exchange_mapping("hyperliquid")
            except Exception:
                pass

    pbgdir = Path.cwd()
    coindata_dir = pbgdir / "data" / "coindata"
    exchange_dir = coindata_dir / coindata.exchange

    cmc_data_ts = _file_mtime(coindata_dir / "coindata.json")
    cmc_metadata_ts = _file_mtime(coindata_dir / "metadata.json")
    ccxt_markets_ts = _file_mtime(exchange_dir / "ccxt_markets.json")
    mapping_ts = _file_mtime(exchange_dir / "mapping.json")
    cpt_cache_ts = _file_mtime(exchange_dir / "copy_trading.json")
    prices_ts = _max_price_ts(mapping_rows)

    st.markdown(
        f"**CMC** · Listings: **{_format_ts(cmc_data_ts)}** · Metadata: **{_format_ts(cmc_metadata_ts)}**"
    )
    st.markdown(
        f"**{coindata.exchange}** · Markets: **{_format_ts(ccxt_markets_ts)}** · "
        f"Mapping: **{_format_ts(mapping_ts)}** · Prices: **{_format_ts(prices_ts)}** · "
        f"CPT cache: **{_format_ts(cpt_cache_ts)}**"
    )

    available_quotes = sorted({
        (row.get("quote") or "").upper()
        for row in mapping_rows
        if row.get("quote")
    })

    preferred_quotes = ["USDT"]
    if coindata.exchange == "hyperliquid":
        preferred_quotes = ["USDC", "USDT0"]

    quote_filter = [q for q in preferred_quotes if q in available_quotes]
    if not quote_filter:
        quote_filter = list(available_quotes)

    mapping_tags = coindata.get_mapping_tags(coindata.exchange, quote_filter=quote_filter)

    # Display controls (compact header row)
    col_1, col_2, col_3, col_4 = st.columns([20, 15, 15, 50])
    with col_1:
        st.selectbox('Exchange', options=coindata.exchanges, index=coindata.exchange_index, key="view_coindata_exchange")
    with col_2:
        st.number_input("market_cap", min_value=0, step=50, format="%.d", key="view_coindata_market_cap", help=pbgui_help.market_cap)
    with col_3:
        st.number_input("vol/mcap", min_value=0.0, step=0.05, format="%.2f", key="view_coindata_vol_mcap", help=pbgui_help.vol_mcap)
    with col_4:
        st.multiselect("Tags", options=mapping_tags, default=[], key="edit_coindata_tags", help=pbgui_help.coindata_tags)

    unmatched_all = [
        row for row in mapping_rows
        if row.get("cmc_id") is None and not row.get("is_hip3", False)
    ]
    unmatched_usdt = [
        row for row in unmatched_all
        if (row.get("quote") or "").upper() in quote_filter
    ]

    unmatched_display = []
    for row in unmatched_usdt:
        quote = (row.get("quote") or "").upper()
        symbol = row.get("symbol") or ""
        coin = compute_coin_name(symbol, quote)
        unmatched_display.append({
            "coin": coin,
            "symbol": symbol,
            "base": row.get("base"),
            "quote": row.get("quote"),
            "ccxt_symbol": row.get("ccxt_symbol"),
        })

    # Deduplicate rows by symbol while preserving deterministic ordering
    unmatched_display = sorted(
        {entry["symbol"]: entry for entry in unmatched_display}.values(),
        key=lambda x: (x.get("coin") or "", x.get("symbol") or "")
    )

    quotes_label = ", ".join(quote_filter) if quote_filter else "all"
    with st.expander(
        f"CMC unmatched ({coindata.exchange}) — {quotes_label}: {len(unmatched_display)}, all quotes: {len(unmatched_all)}",
        expanded=False,
    ):
        if unmatched_display:
            st.dataframe(unmatched_display, use_container_width=True, hide_index=True)
        else:
            st.success("No unmatched CMC symbols for selected exchange.")

    filtered_rows_all = coindata.filter_mapping_rows(
        exchange=coindata.exchange,
        market_cap_min_m=coindata.market_cap,
        vol_mcap_max=coindata.vol_mcap,
        only_cpt=coindata.only_cpt,
        notices_ignore=coindata.notices_ignore,
        tags=coindata.tags,
        quote_filter=quote_filter,
    )

    # HIP-3 rows are displayed without CMC-dependent filters (market cap/tags/notices/etc.),
    # but still respect market availability flags.
    hip3_rows = [
        r for r in mapping_rows
        if r.get("is_hip3", False)
        and bool(r.get("active", True))
        and bool(r.get("linear", True))
    ]
    filtered_rows = [r for r in filtered_rows_all if not r.get("is_hip3", False)]

    if coindata.exchange == "hyperliquid":
        hip3_rows.sort(key=lambda x: ((x.get("coin") or ""), (x.get("symbol") or "")))

    column_config = {
        "price": st.column_config.NumberColumn("price", format="$%.8g"),
        "market_cap": st.column_config.NumberColumn("market_cap (USD)", format="compact"),
        "volume_24h": st.column_config.NumberColumn("volume_24h (USD)", format="compact"),
        "vol/mcap": st.column_config.NumberColumn("vol/mcap", format="%.4f×"),
    }
    column_order = [
        "coin",
        "ccxt_symbol",
        "base",
        "quote",
        "copy_trading",
        "cmc_id",
        "cmc_rank",
        "price",
        "market_cap",
        "volume_24h",
        "vol/mcap",
        "tags",
        "notice",
        "contract_size",
        "min_amount",
        "min_cost",
        "precision_amount",
        "max_leverage",
        "min_order_price",
    ]

    hip3_column_config = {
        "price": st.column_config.NumberColumn("price", format="$%.8g"),
        "volume_24h": st.column_config.NumberColumn("volume_24h (USD)", format="compact"),
    }
    hip3_column_order = [
        "dex",
        "coin",
        "ccxt_symbol",
        "quote",
        "price",
        "volume_24h",
        "copy_trading",
        "contract_size",
        "min_amount",
        "min_cost",
        "precision_amount",
        "max_leverage",
        "min_order_price",
    ]

    if coindata.exchange == "hyperliquid":
        with st.expander(f"HIP-3 symbols ({len(hip3_rows)})", expanded=False):
            if hip3_rows:
                hip3_event = st.dataframe(
                    hip3_rows,
                    column_config=hip3_column_config,
                    column_order=hip3_column_order,
                    hide_index=True,
                    key="coindata_hip3_table",
                    on_select="rerun",
                    selection_mode="single-row",
                )
                idx = _selected_row_idx(hip3_event)
                if idx is not None and 0 <= idx < len(hip3_rows):
                    notice_text = hip3_rows[idx].get("notice")
                    if notice_text:
                        st.info(f"Notice: {notice_text}")
            else:
                st.info("No HIP-3 symbols match current filters.")

    if filtered_rows:
        main_event = st.dataframe(
            filtered_rows,
            height=36 + (len(filtered_rows)) * 35,
            column_config=column_config,
            column_order=column_order,
            key="coindata_main_table",
            on_select="rerun",
            selection_mode="single-row",
        )
        idx = _selected_row_idx(main_event)
        if idx is not None and 0 <= idx < len(filtered_rows):
            notice_text = filtered_rows[idx].get("notice")
            if notice_text:
                st.info(f"Notice: {notice_text}")

def setup_coindata():
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.session_state.coindata = CoinData()
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.setup_coindata
            st.rerun()
        if st.button(":material/save:"):
            coindata.save_config()
            info_popup("Config saved")
    # Init session states for keys
    if "edit_coindata_api_key" in st.session_state:
        if st.session_state.edit_coindata_api_key != coindata.api_key:
            coindata.api_key = st.session_state.edit_coindata_api_key
    if "edit_coindata_fetch_limit" in st.session_state:
        if st.session_state.edit_coindata_fetch_limit != coindata.fetch_limit:
            coindata.fetch_limit = st.session_state.edit_coindata_fetch_limit
    if "edit_coindata_fetch_interval" in st.session_state:
        if st.session_state.edit_coindata_fetch_interval != coindata.fetch_interval:
            coindata.fetch_interval = st.session_state.edit_coindata_fetch_interval
    if "edit_coindata_metadata_interval" in st.session_state:
        if st.session_state.edit_coindata_metadata_interval != coindata.metadata_interval:
            coindata.metadata_interval = st.session_state.edit_coindata_metadata_interval
    if "edit_coindata_mapping_interval" in st.session_state:
        if st.session_state.edit_coindata_mapping_interval != coindata.mapping_interval:
            coindata.mapping_interval = st.session_state.edit_coindata_mapping_interval
    # Edit
    st.text_input("CoinMarketCap API_Key", value=coindata.api_key, type="password", key="edit_coindata_api_key", help=pbgui_help.coindata_api_key)
    st.number_input("Fetch Limit", min_value=200, max_value=5000, value=coindata.fetch_limit, step=200, format="%.d", key="edit_coindata_fetch_limit", help=pbgui_help.coindata_fetch_limit)
    st.number_input("Fetch Interval", min_value=1, max_value=24, value=coindata.fetch_interval, step=1, format="%.d", key="edit_coindata_fetch_interval", help=pbgui_help.coindata_fetch_interval)
    st.number_input("Metadata Interval", min_value=1, max_value=7, value=coindata.metadata_interval, step=1, format="%.d", help=pbgui_help.coindata_metadata_interval)
    st.number_input("Mapping Interval", min_value=1, max_value=168, value=coindata.mapping_interval, step=1, format="%.d", key="edit_coindata_mapping_interval", help=pbgui_help.coindata_mapping_interval)
    if coindata.api_key:
        if coindata.fetch_api_status():
            st.success("API Key is valid", icon="✅")
            st.write(f"API limit monthly: {coindata.credit_limit_monthly}")
            st.write(f"Next API credits reset in: {coindata.credit_limit_monthly_reset} at: {coindata.credit_limit_monthly_reset_timestamp}")
            st.write(f"API credits used today: {coindata.credits_used_day}")
            st.write(f"API credits used monthly: {coindata.credits_used_month}")
            st.write(f"API credits left: {coindata.credits_left}")
        else:
            st.error(coindata.api_error, icon="🚨")

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("Coin Data")
render_header_with_guide(
    "Coin Data",
    guide_callback=lambda: _help_modal(default_topic="Coin Data"),
    guide_key="coin_data_guide_btn",
)

# Check if CoinData is configured
if not "pbcoindata" in st.session_state:
    st.session_state.pbcoindata = CoinData()
coindata  = st.session_state.pbcoindata
if coindata.api_error:
    st.session_state.setup_coindata = True

if 'setup_coindata' in st.session_state:
    setup_coindata()
else:
    view_coindata()
