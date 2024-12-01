import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_pb_installed, is_authenticted, get_navi_paths
from BacktestMulti import BacktestMultiItem, BacktestsMulti, BacktestMultiQueue
import datetime
from Instance import Instance
from User import Users
import multiprocessing

def bt_multi():
    # Init bt_multi
    bt_multi = st.session_state.bt_multi
    # Navigation
    with st.sidebar:
        if st.button(":top:"):
            del st.session_state.bt_multi
            st.rerun()
        if st.button(":floppy_disk:"):
            if bt_multi.name:
                bt_multi.save()
            else:
                info_popup("Name is empty")
        if st.button("Results"):
            st.session_state.bt_multi_results = bt_multi
            del st.session_state.bt_multi
            st.rerun()
        if st.button("Queue"):
            del st.session_state.bt_multi
            st.session_state.bt_multi_queue = BacktestMultiQueue()
            st.rerun()
        if st.button("Add to Backtest Queue"):
            if bt_multi.name and bt_multi.hjson and bt_multi.symbols:
                bt_multi.save()
                bt_multi.save_queue()
            else:
                if not bt_multi.name:
                    info_popup("Name is empty")
                elif not bt_multi.hjson:
                    info_popup("Backtest not saved")
                elif not bt_multi.symbols:
                    info_popup("No Symbols")
    st.subheader(f"Create/Edit: {bt_multi.name}")
    bt_multi.edit()
    with st.expander("Optimize loss_allowance_pct, stuck_threshold and unstuck_close_pct", expanded=False):
        bt_multi.optimize()

def bt_multi_edit_symbol():
    # Init bt_multi
    bt_multi = st.session_state.bt_multi
    symbol = st.session_state.bt_multi_edit_symbol
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.bt_multi_edit_symbol
            st.rerun()
        if st.button(":floppy_disk:"):
            bt_multi.symbols[symbol].save_config()
    st.title(f"Backtest Multi: {bt_multi.name} - Symbol: {symbol}")
    bt_multi.symbols[symbol].edit_config()
    if bt_multi.symbols[symbol].preview_grid:
        if "preview_grid_instance" not in st.session_state:
            st.session_state.preview_grid_instance = Instance()
        instance = st.session_state.preview_grid_instance
        instance.config = bt_multi.symbols[symbol].config
        user = Users().find_exchange_user(bt_multi.exchange)
        if user:
            instance.user = user
            instance.symbol = symbol
            instance.market_type = "futures"
            instance.view_grid(bt_multi.sb)
        else:
            error_popup(f"Can't preview grid. No User for Exchange {bt_multi.exchange} found")
            if "config_preview_grid" in st.session_state:
                del st.session_state.config_preview_grid
                st.session_state.config_preview_grid = False

def bt_multi_list():
    # Init bt_multi_list
    if "bt_multi_list" not in st.session_state:
        st.session_state.bt_multi_list = BacktestsMulti()
    bt_multi_list = st.session_state.bt_multi_list
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.session_state.bt_multi_list = BacktestsMulti()
            st.rerun()
        if st.button("Queue"):
            st.session_state.bt_multi_queue = BacktestMultiQueue()
            st.rerun()
        if st.button("Add Backtest"):
            st.session_state.bt_multi = BacktestMultiItem()
            st.rerun()
    st.subheader("Available Configs")
    bt_multi_list.view_backtests()

def bt_multi_results():
    # Init bt_multi_results
    bt_multi_results = st.session_state.bt_multi_results
    if not bt_multi_results.backtest_results:
        with st.spinner("Loading Results"):
            st.session_state.bt_multi_results.load_results()
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            bt_multi_results.backtest_results = []
            st.rerun()
        if st.button(":top:"):
            del st.session_state.bt_multi_results
            st.rerun()
        if st.button("Edit"):
            st.session_state.bt_multi = bt_multi_results
            del st.session_state.bt_multi_results
            st.rerun()
        if st.button("Queue"):
            st.session_state.bt_multi_queue = BacktestMultiQueue()
            del st.session_state.bt_multi_results
            st.rerun()
        if st.button(":wastebasket: selected"):
            bt_multi_results.remove_selected_results()
            st.rerun()
        if st.button(":wastebasket: all"):
            bt_multi_results.remove_all_results()
            st.rerun()
    st.subheader(f"Results: {bt_multi_results.name}")
    bt_multi_results.view_results()

def bt_multi_queue():
    # Init bt_multi_queue
    bt_multi_queue = st.session_state.bt_multi_queue
    # Init session state for keys
    if "backtest_multi_cpu" in st.session_state:
        if st.session_state.backtest_multi_cpu != bt_multi_queue.cpu:
            bt_multi_queue.cpu = st.session_state.backtest_multi_cpu
    if "backtest_multi_autostart" in st.session_state:
        if st.session_state.backtest_multi_autostart != bt_multi_queue.autostart:
            bt_multi_queue.autostart = st.session_state.backtest_multi_autostart
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            bt_multi_queue.items = []
            st.rerun()
        if st.button(":top:"):
            del st.session_state.bt_multi_queue
            st.rerun()
        st.number_input(f'Max CPU(1 - {multiprocessing.cpu_count()})', min_value=1, max_value=multiprocessing.cpu_count(), value=bt_multi_queue.cpu, step=1, key = "backtest_multi_cpu")
        st.toggle("Autostart", value=bt_multi_queue.autostart, key="backtest_multi_autostart", help=None)
        if st.button(":wastebasket: selected"):
            bt_multi_queue.remove_selected()
            st.rerun()
        if st.button(":wastebasket: finished"):
            bt_multi_queue.remove_finish()
            st.rerun()
        if st.button(":wastebasket: all"):
            bt_multi_queue.remove_finish(all=True)
            st.rerun()
    st.subheader("Queue")
    bt_multi_queue.view()

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv6 Multi Backtest")
st.header("PBv6 Multi Backtest", divider="red")

# Check if PB6 is installed
if not is_pb_installed():
    st.warning('Passivbot Version 6.x is not installed', icon="⚠️")
    st.stop()

if "bt_multi_results" in st.session_state:
    bt_multi_results()
elif "bt_multi_edit_symbol" in st.session_state:
    bt_multi_edit_symbol()
elif "bt_multi" in st.session_state:
    bt_multi()
elif "bt_multi_queue" in st.session_state:
    bt_multi_queue()
else:
    bt_multi_list()
