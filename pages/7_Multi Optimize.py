import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized, error_popup, info_popup
from OptimizeMulti import OptimizeMultiItem, OptimizesMulti, OptimizeMultiQueue, OptimizeMultiResults
from Instance import Instance

def opt_multi():
    # Init bt_multi
    opt_multi = st.session_state.opt_multi
    # Navigation
    with st.sidebar:
        if st.button(":top:"):
            del st.session_state.opt_multi
            st.rerun()
        if st.button(":floppy_disk:"):
            if opt_multi.name:
                opt_multi.save()
            else:
                info_popup("Name is empty")
        if st.button("Results"):
            st.session_state.opt_multi_results = OptimizeMultiResults()
            del st.session_state.opt_multi
            st.rerun()
        if st.button("Queue"):
            del st.session_state.opt_multi
            st.session_state.opt_multi_queue = OptimizeMultiQueue()
            st.rerun()
        if st.button("Add to Optimizer Queue"):
            if opt_multi.name and opt_multi.hjson and opt_multi.symbols:
                opt_multi.save()
                opt_multi.save_queue()
            else:
                if not opt_multi.name:
                    info_popup("Name is empty")
                elif not opt_multi.hjson:
                    info_popup("Backtest not saved")
                elif not opt_multi.symbols:
                    info_popup("No Symbols")
    st.title(f"Optimize Multi: {opt_multi.name}")
    opt_multi.edit()

def opt_multi_list():
    # Init opt_multi_list
    if "opt_multi_list" not in st.session_state:
        st.session_state.opt_multi_list = OptimizesMulti()
    opt_multi_list = st.session_state.opt_multi_list
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.session_state.opt_multi_list = OptimizesMulti()
            st.rerun()
        if st.button("Results"):
            st.session_state.opt_multi_results = OptimizeMultiResults()
            st.rerun()
        if st.button("Queue"):
            st.session_state.opt_multi_queue = OptimizeMultiQueue()
            st.rerun()
        if st.button("Add Optimize"):
            st.session_state.opt_multi = OptimizeMultiItem()
            st.rerun()
    opt_multi_list.view_optimizes()

def opt_multi_results():
    # Init bt_multi_results
    opt_multi_results = st.session_state.opt_multi_results
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            opt_multi_results.results = []
            opt_multi_results.find_results()
            st.rerun()
        if st.button(":top:"):
            del st.session_state.opt_multi_results
            st.rerun()
        if st.button("Queue"):
            del st.session_state.opt_multi_results
            st.session_state.opt_multi_queue = OptimizeMultiQueue()
            st.rerun()
        if st.button(":wastebasket: selected"):
            opt_multi_results.remove_selected_results()
            st.rerun()
        if st.button(":wastebasket: all"):
            opt_multi_results.remove_all_results()
            st.rerun()
    st.title(f"Optimize Multi Results")
    opt_multi_results.view_results()

def opt_multi_queue():
    # Init opt_multi_queue
    opt_multi_queue = st.session_state.opt_multi_queue
    # Init session state for keys
    if "optimize_multi_autostart" in st.session_state:
        if st.session_state.optimize_multi_autostart != opt_multi_queue.autostart:
            opt_multi_queue.autostart = st.session_state.optimize_multi_autostart
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            opt_multi_queue.items = []
            if "ed_key" in st.session_state:
                st.session_state.ed_key += 1
            st.rerun()
        if st.button(":top:"):
            del st.session_state.opt_multi_queue
            st.rerun()
        if st.button("Results"):
            del st.session_state.opt_multi_queue
            st.session_state.opt_multi_results = OptimizeMultiResults()
            st.rerun()
        st.toggle("Autostart", value=opt_multi_queue.autostart, key="optimize_multi_autostart", help=None)
        if st.button(":wastebasket: selected"):
            opt_multi_queue.remove_selected()
            st.rerun()
        if st.button(":wastebasket: finished"):
            opt_multi_queue.remove_finish()
            st.rerun()
        if st.button(":wastebasket: all"):
            opt_multi_queue.remove_finish(all=True)
            st.rerun()
    st.title("Optimize Multi Queue")
    opt_multi_queue.view()

set_page_config("Multi Optimize")

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

if "opt_multi_results" in st.session_state:
    opt_multi_results()
elif "opt_multi" in st.session_state:
    opt_multi()
elif "opt_multi_queue" in st.session_state:
    opt_multi_queue()
else:
    opt_multi_list()
