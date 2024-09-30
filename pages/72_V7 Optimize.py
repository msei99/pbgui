import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized, error_popup, info_popup, is_pb7_installed
from OptimizeV7 import OptimizeV7Item, OptimizesV7, OptimizeV7Queue, OptimizeV7Results

def opt_v7():
    # Init opt_v7
    opt_v7 = st.session_state.opt_v7
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            del st.session_state.opt_v7
            if "opt_v7_list" in st.session_state:
                del st.session_state.opt_v7_list
            st.rerun()
        if st.button(":material/save:"):
            if opt_v7.name:
                with st.spinner("Saving..."):
                    opt_v7.save()
                    if "opt_v7_list" in st.session_state:
                        del st.session_state.opt_v7_list
            else:
                info_popup("Name is empty")
        if st.button("Results"):
            st.session_state.opt_v7_results = OptimizeV7Results()
            del st.session_state.opt_v7
            st.rerun()
        if st.button("Queue"):
            del st.session_state.opt_v7
            st.session_state.opt_v7_queue = OptimizeV7Queue()
            st.rerun()
        if st.button("Add to Optimizer Queue"):
            if opt_v7.name and opt_v7.config.config_file:
                with st.spinner("Saving and adding to queue"):
                    opt_v7.save()
                    if "opt_v7_list" in st.session_state:
                        del st.session_state.opt_v7_list
                    opt_v7.save_queue()
                    st.session_state.opt_v7_queue = OptimizeV7Queue()
                    del st.session_state.opt_v7
                    st.rerun()
            else:
                if not opt_v7.name:
                    info_popup("Name is empty")
                elif not opt_v7.config.config_file:
                    info_popup("Optimize not saved")
    st.title(f"Optimize V7")
    opt_v7.edit()

def opt_v7_list():
    # Init opt_v7_list
    if "opt_v7_list" not in st.session_state:
        st.session_state.opt_v7_list = OptimizesV7()
    opt_v7_list = st.session_state.opt_v7_list
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.session_state.opt_v7_list = OptimizesV7()
            st.rerun()
        if st.button("Results"):
            st.session_state.opt_v7_results = OptimizeV7Results()
            st.rerun()
        if st.button("Queue"):
            st.session_state.opt_v7_queue = OptimizeV7Queue()
            st.rerun()
        if st.button("Add Optimize"):
            st.session_state.opt_v7 = OptimizeV7Item()
            st.rerun()
    opt_v7_list.view_optimizes()

def opt_v7_results():
    # Init bt_v7_results
    opt_v7_results = st.session_state.opt_v7_results
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            opt_v7_results.results = []
            opt_v7_results.find_results()
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.opt_v7_results
            st.rerun()
        if st.button("Queue"):
            del st.session_state.opt_v7_results
            st.session_state.opt_v7_queue = OptimizeV7Queue()
            st.rerun()
        if st.button(":material/delete: selected"):
            opt_v7_results.remove_selected_results()
            st.rerun()
        if st.button(":material/delete: all"):
            opt_v7_results.remove_all_results()
            st.rerun()
    st.title(f"Optimize V7 Results")
    opt_v7_results.view_results()

def opt_v7_queue():
    # Init opt_v7_queue
    opt_v7_queue = st.session_state.opt_v7_queue
    # Init session state for keys
    if "optimize_v7_autostart" in st.session_state:
        if st.session_state.optimize_v7_autostart != opt_v7_queue.autostart:
            opt_v7_queue.autostart = st.session_state.optimize_v7_autostart
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            opt_v7_queue.items = []
            if "ed_key" in st.session_state:
                st.session_state.ed_key += 1
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.opt_v7_queue
            st.rerun()
        if st.button("Results"):
            del st.session_state.opt_v7_queue
            st.session_state.opt_v7_results = OptimizeV7Results()
            st.rerun()
        st.toggle("Autostart", value=opt_v7_queue.autostart, key="optimize_v7_autostart", help=None)
        if st.button(":material/delete: selected"):
            opt_v7_queue.remove_selected()
            st.rerun()
        if st.button(":material/delete: finished"):
            opt_v7_queue.remove_finish()
            st.rerun()
        if st.button(":material/delete: all"):
            opt_v7_queue.remove_finish(all=True)
            st.rerun()
    st.title("Optimize V7 Queue")
    opt_v7_queue.view()

set_page_config("Optimize V7")

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

# Check if PB7 is installed
if not is_pb7_installed():
    st.warning('Passivbot Version 7.x is not installed', icon="⚠️")
    st.stop()

if "opt_v7_results" in st.session_state:
    opt_v7_results()
elif "opt_v7" in st.session_state:
    opt_v7()
elif "opt_v7_queue" in st.session_state:
    opt_v7_queue()
else:
    opt_v7_list()
