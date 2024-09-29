import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized, error_popup, info_popup, is_pb7_installed
from BacktestV7 import BacktestV7Item, BacktestsV7, BacktestV7Queue
import datetime
from Instance import Instance
from User import Users
import multiprocessing

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
            st.session_state.bt_v7_results = bt_v7
            del st.session_state.bt_v7
            st.rerun()
        if st.button("Queue"):
            del st.session_state.bt_v7
            st.session_state.bt_v7_queue = BacktestV7Queue()
            st.rerun()
        if st.button("Add to Backtest Queue"):
            if bt_v7.name:
                with st.spinner("Saving and adding to queue"):
                    bt_v7.save()
                    if "bt_v7_list" in st.session_state:
                        del st.session_state.bt_v7_list
                    bt_v7.save_queue()
                    st.session_state.bt_v7_queue = BacktestV7Queue()
                    del st.session_state.bt_v7
                    st.rerun()
            else:
                if not bt_v7.name:
                    info_popup("Name is empty")
    st.title(f"Backtest v7: {bt_v7.name}")
    bt_v7.edit()
    # with st.expander("Optimize loss_allowance_pct, stuck_threshold and unstuck_close_pct", expanded=False):
    #     bt_v7.optimize()

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
        if st.button("Queue"):
            st.session_state.bt_v7_queue = BacktestV7Queue()
            st.rerun()
        if st.button("Add Backtest"):
            st.session_state.bt_v7 = BacktestV7Item()
            st.rerun()
    bt_v7_list.view_backtests()

def bt_v7_results():
    # Init bt_v7_results
    bt_v7_results = st.session_state.bt_v7_results
    if not bt_v7_results.backtest_results:
        with st.spinner("Loading Results"):
            st.session_state.bt_v7_results.load_results()
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            bt_v7_results.backtest_results = []
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.bt_v7_results
            st.rerun()
        if st.button("Edit"):
            st.session_state.bt_v7 = bt_v7_results
            del st.session_state.bt_v7_results
            st.rerun()
        if st.button("Queue"):
            st.session_state.bt_v7_queue = BacktestV7Queue()
            del st.session_state.bt_v7_results
            st.rerun()
        if st.button(":material/delete: selected"):
            bt_v7_results.remove_selected_results()
            st.rerun()
        if st.button(":material/delete: all"):
            bt_v7_results.remove_all_results()
            st.rerun()
    st.title(f"Backtest v7 Results: {bt_v7_results.name}")
    bt_v7_results.view_results()

def bt_v7_queue():
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
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.bt_v7_queue
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
    st.title("Backtest v7 Queue")
    bt_v7_queue.view()

set_page_config("Backtest V7")

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

# Check if PB7 is installed
if not is_pb7_installed():
    st.warning('Passivbot Version 7.x is not installed', icon="⚠️")
    st.stop()

if "bt_v7_results" in st.session_state:
    bt_v7_results()
elif "bt_v7" in st.session_state:
    bt_v7()
elif "bt_v7_queue" in st.session_state:
    bt_v7_queue()
else:
    bt_v7_list()
