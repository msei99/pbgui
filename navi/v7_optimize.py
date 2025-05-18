import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_pb7_installed, is_authenticted, get_navi_paths
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
                    info_popup(f"Added {opt_v7.name} to Queue")
            else:
                if not opt_v7.name:
                    info_popup("Name is empty")
                elif not opt_v7.config.config_file:
                    info_popup("Optimize not saved")
        
        st.markdown("---")
        st.selectbox('Preset...',opt_v7.find_presets(), key="opt_v7_preset_select")
        col1, col2, col3 = st.columns(3)
        # Load, Save, Delete Buttons 
        if col1.button(":material/folder: Load", key="opt_v7_preset_load"):
            opt_v7.preset_load(st.session_state.opt_v7_preset_select)
            st.rerun()
        if col2.button(":material/save: Save", key="opt_v7_preset_save"):
            if opt_v7.preset_save():
                st.rerun()
        if col3.button(":material/delete: Del", key="opt_v7_preset_delete"):
            opt_v7.preset_remove(st.session_state.opt_v7_preset_select)
            st.rerun()

    st.subheader(f"Create/Edit")
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
    st.subheader("Available Configs")
    opt_v7_list.view_optimizes()

def opt_v7_results():
    # Init bt_v7_results
    opt_v7_results = st.session_state.opt_v7_results
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            opt_v7_results.results = []
            opt_v7_results.results_new = []
            opt_v7_results.find_results()
            if "opt_v7_results_d" in st.session_state:
                del st.session_state.opt_v7_results_d
            if "opt_v7_results_d_new" in st.session_state:
                del st.session_state.opt_v7_results_d_new
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
            if "opt_v7_results_d" in st.session_state:
                del st.session_state.opt_v7_results_d
            if "opt_v7_results_d_new" in st.session_state:
                del st.session_state.opt_v7_results_d_new
            st.rerun()
        if st.button(":material/delete: all"):
            opt_v7_results.remove_all_results()
            if "opt_v7_results_d" in st.session_state:
                del st.session_state.opt_v7_results_d
            if "opt_v7_results_d_new" in st.session_state:
                del st.session_state.opt_v7_results_d_new
            st.rerun()
    st.subheader("All Results")
    opt_v7_results.view_results()

def opt_v7_pareto():
    # Init bt_v7_pareto
    opt_v7_results = st.session_state.opt_v7_results
    opt_v7_pareto = st.session_state.opt_v7_pareto
    opt_v7_pareto_name = st.session_state.opt_v7_pareto_name
    opt_v7_pareto_directory = st.session_state.opt_v7_pareto_directory
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            opt_v7_results.paretos = []
            if "d_paretos" in st.session_state:
                del st.session_state.d_paretos
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.opt_v7_results
            del st.session_state.opt_v7_pareto
            if "d_paretos" in st.session_state:
                del st.session_state.d_paretos
            opt_v7_results.paretos = []
            st.rerun()
        if st.button("Queue"):
            del st.session_state.opt_v7_results
            del st.session_state.opt_v7_pareto
            if "d_paretos" in st.session_state:
                del st.session_state.d_paretos
            opt_v7_results.paretos = []
            st.session_state.opt_v7_queue = OptimizeV7Queue()
            st.rerun()
        if st.button(":material/arrow_upward_alt:"):
            del st.session_state.opt_v7_pareto
            if "d_paretos" in st.session_state:
                del st.session_state.d_paretos
            opt_v7_results.paretos = []
            st.rerun()
        if st.button("BT selected"):
            opt_v7_results.backtest_selected()
        if st.button("BT all"):
            opt_v7_results.backtest_all()
    st.subheader(f"Name: :blue[{opt_v7_pareto_name}] Directory: :blue[{opt_v7_pareto_directory}]")
    opt_v7_results.view_pareto(opt_v7_pareto)

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
    st.subheader("Queue")
    opt_v7_queue.view()

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv7 Optimize")
st.header("PBv7 Optimize", divider="red")

# Check if PB7 is installed
if not is_pb7_installed():
    st.warning('Passivbot Version 7.x is not installed', icon="⚠️")
    st.stop()

# Check if CoinData is configured
if st.session_state.pbcoindata.api_error:
    st.warning('Coin Data API is not configured / Go to Coin Data and configure your API-Key', icon="⚠️")
    st.stop()

if "opt_v7_pareto" in st.session_state:
    opt_v7_pareto()
elif "opt_v7_results" in st.session_state:
    opt_v7_results()
elif "opt_v7" in st.session_state:
    opt_v7()
elif "opt_v7_queue" in st.session_state:
    opt_v7_queue()
else:
    opt_v7_list()
