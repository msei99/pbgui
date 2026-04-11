import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_pb7_installed, is_authenticted, get_navi_paths, render_header_with_guide, redirect_to_fastapi_v7_backtest
from pbgui_func import PBGDIR, pb7dir
from BacktestV7 import BacktestV7Item, BacktestsV7, BacktestV7Queue, BacktestV7Results, ConfigV7Archives
from RunV7 import V7Instance
from Config import BalanceCalculator
from pathlib import Path
import multiprocessing


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
def _bt_v7_help_modal(default_topic: str = "Backtest"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="bt_v7_help_lang")
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
        key="bt_v7_help_sel",
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

def bt_v7():
    # Init bt_v7
    bt_v7 = st.session_state.bt_v7
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            del st.session_state.bt_v7
            st.rerun()
        if st.button(":material/save:"):
            if bt_v7.name:
                with st.spinner("Saving..."):
                    bt_v7.save()
                    if "bt_v7_list" in st.session_state:
                        del st.session_state.bt_v7_list
            else:
                info_popup("Name is empty")
        if st.button("Import"):
            bt_v7.import_instance()
        if st.button("Results"):
            st.session_state.bt_v7_results = bt_v7.results
            st.session_state["_bt_v7_main_view_next"] = "Results"
            del st.session_state.bt_v7
            st.rerun()
        if st.button("Caclulate Balance"):
            st.session_state.bc_context_exchanges = list(bt_v7.config.backtest.exchanges or [])
            st.session_state.balance_calc = BalanceCalculator(bt_v7.config.config_file)
            st.switch_page(get_navi_paths()["V7_BALANCE_CALC"])
        if st.button("Add to Backtest Queue"):
            if bt_v7.name:
                with st.spinner("Saving and adding to queue"):
                    bt_v7.save()
                    if "bt_v7_list" in st.session_state:
                        del st.session_state.bt_v7_list
                    bt_v7.save_queue()
                st.session_state.bt_v7_queue = BacktestV7Queue()
                del st.session_state.bt_v7
                st.session_state["_bt_v7_main_view_next"] = "Queue"
                st.rerun()
            else:
                if not bt_v7.name:
                    info_popup("Name is empty")
    st.subheader(f"Create/Edit: {bt_v7.name}")
    bt_v7.edit()

def bt_v7_list():
    # Init bt_v7_list
    if "bt_v7_list" not in st.session_state:
        st.session_state.bt_v7_list = BacktestsV7()
    bt_v7_list = st.session_state.bt_v7_list
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.session_state.bt_v7_list = BacktestsV7()
            st.rerun()
        if st.button(":material/add:", help="Add new Backtest"):
            st.session_state.bt_v7 = BacktestV7Item()
            st.rerun()
        if st.button(":material/chart_data:", help="View selected Backtest Results"):
            bt_v7_list.view_selected()
        if st.button(":material/edit:", help="Edit selected Backtest"):
            bt_v7_list.edit_selected()
        col1, col2 = st.columns([0.5,1])
        with col1:
            if st.button(":material/delete:", help="Delete selected Backtests. If none selected all will be deleted"):
                bt_v7_list.remove_selected()
        with col2:
            st.checkbox("Results", key="bt_v7_remove_results", value=False, help="Also remove results of selected Backtests")
    bt_v7_list.view_backtests()

def config_v7_archives():
    # Init bt_v7_list
    config_v7_archives = st.session_state.config_v7_archives
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            config_v7_archives.load()
            st.rerun()
        if st.button(":material/settings:"):
            st.session_state.setup_config_archive = True
            st.rerun()
        if st.button("Sync Github"):
            config_v7_archives.git_pull()
        if st.button("Push own Archive"):
            config_v7_archives.git_push()
    config_v7_archives.add()
    config_v7_archives.list()

def setup_config_archive():
    # Init bt_v7_list
    config_v7_archives = st.session_state.config_v7_archives
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            del st.session_state.setup_config_archive
            st.rerun()
        if st.button(":material/save:"):
            config_v7_archives.save_config()
            info_popup("Config saved")
    config_v7_archives.setup()

def config_v7_config_archive():
    # Init bt_v7_results
    config_v7_config_archive = st.session_state.config_v7_config_archive
    if not config_v7_config_archive.results:
        with st.spinner("Loading Results"):
            st.session_state.config_v7_config_archive.load()
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            config_v7_config_archive.results = []
            config_v7_config_archive.results_d = []
            st.rerun()
        if st.button(":material/arrow_upward_alt:", help="Back to Archive"):
            del st.session_state.config_v7_config_archive
            st.rerun()
        if st.button(":material/home:", help="Back to Configs"):
            del st.session_state.config_v7_config_archive
            del st.session_state.config_v7_archives
            st.session_state.bt_v7_main_view = "Configs"
            st.rerun()
        if st.button("Queue"):
            if "bt_v7_queue" not in st.session_state:
                st.session_state.bt_v7_queue = BacktestV7Queue()
            del st.session_state.config_v7_config_archive
            del st.session_state.config_v7_archives
            st.session_state.bt_v7_main_view = "Queue"
            st.rerun()
        if st.button("BT selected"):
            config_v7_config_archive.backtest_selected_results()
        if st.button("Caclulate Balance"):
            config_v7_config_archive.calculate_balance()
        if st.button("Add to Compare"):
            config_v7_config_archive.add_to_compare()
        if st.button(":material/delete: selected"):
            config_v7_config_archive.remove_selected_results()
            config_v7_config_archive.results = []
            config_v7_config_archive.results_d = []
            st.rerun()
    st.subheader(f"Config Archive: {config_v7_config_archive.name}")
    config_v7_config_archive.view()

def bt_v7_results():
    # Init bt_v7_results
    bt_v7_results = st.session_state.bt_v7_results
    if not bt_v7_results.results:
        with st.spinner("Loading Results"):
            st.session_state.bt_v7_results.load()
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            bt_v7_results.results = []
            bt_v7_results.results_d = []
            st.rerun()
        if st.button("All Results"):
            bt_v7_results.results = []
            bt_v7_results.results_d = []
            bt_v7_results.results_path = f'{pb7dir()}/backtests/pbgui'
            bt_v7_results.name = "All Results"
            st.rerun()
        if st.button("BT selected"):
            bt_v7_results.backtest_selected_results()
        if st.button("Strategy Explorer"):
            bt_v7_results.strategy_explorer()
        if st.button("Caclulate Balance"):
            bt_v7_results.calculate_balance()
        if st.button("Add to Compare"):
            bt_v7_results.add_to_compare()
        if st.button("Add to Run"):
            bt_v7_results.add_to_run()
        if st.button("Optimize from Result"):
            bt_v7_results.optimize_from_result()
        if st.button("Add to Config Archive"):
            bt_v7_results.add_to_config_archive()
        if st.button("Go to Config Archives"):
            if "config_v7_archives" not in st.session_state:
                st.session_state.config_v7_archives = ConfigV7Archives()
            st.session_state["_bt_v7_main_view_next"] = "Archive"
            st.rerun()
        if st.button(":material/delete: selected"):
            bt_v7_results.remove_selected_results()
            bt_v7_results.results = []
            bt_v7_results.results_d = []
            st.rerun()
        if st.button(":material/delete: all"):
            bt_v7_results.remove_all_results()
            bt_v7_results.results = []
            bt_v7_results.results_d = []
            st.rerun()
    st.subheader(f"Results: {bt_v7_results.name}")
    bt_v7_results.view()

def bt_v7_queue(show_log=False):
    # Init bt_v7_queue
    bt_v7_queue = st.session_state.bt_v7_queue
    # Init session state for keys
    if "backtest_v7_cpu" in st.session_state:
        if st.session_state.backtest_v7_cpu != bt_v7_queue.cpu:
            bt_v7_queue.cpu = st.session_state.backtest_v7_cpu
    if "backtest_v7_autostart" in st.session_state:
        if st.session_state.backtest_v7_autostart != bt_v7_queue.autostart:
            bt_v7_queue.autostart = st.session_state.backtest_v7_autostart
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            bt_v7_queue.items = []
            bt_v7_queue.d = []
            st.rerun()
        st.number_input(f'Max CPU(1 - {multiprocessing.cpu_count()})', min_value=1, max_value=multiprocessing.cpu_count(), value=bt_v7_queue.cpu, step=1, key = "backtest_v7_cpu")
        st.toggle("Autostart", value=bt_v7_queue.autostart, key="backtest_v7_autostart", help=None)
        if st.button(":material/delete: selected"):
            bt_v7_queue.remove_selected()
            st.rerun()
        if st.button(":material/delete: finished"):
            bt_v7_queue.remove_finish()
            st.rerun()
        if st.button(":material/delete: all"):
            bt_v7_queue.remove_finish(all=True)
            st.rerun()
    if show_log:
        bt_v7_queue.view_log()
    else:
        bt_v7_queue.view()

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Redirect immediately to the FastAPI backtest page
redirect_to_fastapi_v7_backtest()
st.stop()
render_header_with_guide(
    "PBv7 Backtest",
    guide_callback=lambda: _bt_v7_help_modal(),
    guide_key="bt_v7_header_help_btn",
)

# Check if PB7 is installed
if not is_pb7_installed():
    st.warning('Passivbot Version 7.x is not installed', icon="⚠️")
    st.stop()

# Check if CoinData is configured
if st.session_state.pbcoindata.api_error:
    st.warning('Coin Data API is not configured / Go to Coin Data and configure your API-Key', icon="⚠️")
    st.stop()

# ── Relay: open edit with instance config ────────────────
if "_relay_draft_id" in st.session_state:
    _draft_id = st.session_state.pop("_relay_draft_id")
    _draft_name = st.session_state.pop("_relay_draft_name", "draft")
    try:
        import requests as _req
        from pbgui_func import _start_fastapi_server_if_needed
        _api_host, _api_port, _api_ok = _start_fastapi_server_if_needed()
        if _api_ok and st.session_state.get("api_token"):
            _resp = _req.get(
                f"http://{_api_host}:{_api_port}/api/v7/draft/{_draft_id}",
                headers={"Authorization": f"Bearer {st.session_state['api_token']}"},
                timeout=5,
            )
            if _resp.status_code == 200:
                _cfg = _resp.json().get("config", {})
                if _cfg:
                    _bt = BacktestV7Item()
                    _bt.config.config = _cfg
                    _bt.name = _draft_name
                    st.session_state.bt_v7 = _bt
                    st.session_state.bt_v7_main_view = "Configs"
    except Exception:
        pass

if "_relay_config_file" in st.session_state:
    _relay_instance = st.session_state.pop("_relay_config_file")
    _instance_cfg = Path(f'{PBGDIR}/data/run_v7/{_relay_instance}/config.json')
    if _instance_cfg.is_file():
        _bt = BacktestV7Item()
        _bt.config.config_file = str(_instance_cfg)
        _bt.config.load_config()
        _bt.name = _relay_instance
        st.session_state.bt_v7 = _bt
        st.session_state.bt_v7_main_view = "Configs"

# ── Main tab navigation ──────────────────────────────────
_MAIN_TABS = ["Configs", "Queue", "Log", "Results", "Archive"]
# Apply pending tab switch (must happen before widget is instantiated)
if "_bt_v7_main_view_next" in st.session_state:
    st.session_state.bt_v7_main_view = st.session_state["_bt_v7_main_view_next"]
    del st.session_state["_bt_v7_main_view_next"]
elif "bt_v7_main_view" not in st.session_state:
    if "bt_v7_queue_log_preselect" in st.session_state:
        st.session_state.bt_v7_main_view = "Log"
    elif "bt_v7_queue" in st.session_state:
        st.session_state.bt_v7_main_view = "Queue"
    elif "config_v7_archives" in st.session_state:
        st.session_state.bt_v7_main_view = "Archive"
    elif "bt_v7_results" in st.session_state:
        st.session_state.bt_v7_main_view = "Results"
    else:
        st.session_state.bt_v7_main_view = "Configs"
# bt_v7 edit always lives under Configs tab
if "bt_v7" in st.session_state:
    st.session_state.bt_v7_main_view = "Configs"
_active = st.segmented_control(
    "Navigation", options=_MAIN_TABS, default="Configs", key="bt_v7_main_view", label_visibility="collapsed"
)
if _active == "Queue":
    if "bt_v7_queue" not in st.session_state:
        st.session_state.bt_v7_queue = BacktestV7Queue()
    bt_v7_queue()
elif _active == "Log":
    if "bt_v7_queue" not in st.session_state:
        st.session_state.bt_v7_queue = BacktestV7Queue()
    bt_v7_queue(show_log=True)
elif _active == "Results":
    if "bt_v7_results" not in st.session_state:
        results = BacktestV7Results()
        results.results_path = f'{pb7dir()}/backtests/pbgui'
        results.name = "All Results"
        st.session_state.bt_v7_results = results
    bt_v7_results()
elif _active == "Archive":
    if "config_v7_archives" not in st.session_state:
        st.session_state.config_v7_archives = ConfigV7Archives()
    if "setup_config_archive" in st.session_state:
        setup_config_archive()
    elif "config_v7_config_archive" in st.session_state:
        config_v7_config_archive()
    else:
        config_v7_archives()
else:  # Configs
    if "bt_v7" in st.session_state:
        bt_v7()
    else:
        bt_v7_list()
