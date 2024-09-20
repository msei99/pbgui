import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized, is_pb_installed
from Optimize import OptimizeItem, OptimizeQueue, OptimizeResults
from OptimizeConfig import OptimizeConfigs

def opt_edit_config():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.opt_edit_config
            del st.session_state.my_opt_config
            st.rerun()
        my_opt.oc.name = st.text_input('Filename:', value=my_opt.oc.name, max_chars=32, key="opt_config_file_name_input")
        if st.button(":floppy_disk:"):
            my_opt.oc.save()
        if st.button(":wastebasket:"):
            my_opt.oc.remove()
            my_opt.oc.name = OptimizeConfigs().default()
            del st.session_state.opt_edit_config
            del st.session_state.my_opt_config
            st.rerun()
    # Edit Config
    my_opt.oc.edit()

def opt_edit():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Navigation
    with st.sidebar:
        if st.button("Results"):
            st.session_state.opt_results = True
            st.rerun()
        if st.button("Queue"):
            st.session_state.opt_queue = True
            st.rerun()
        if my_opt_config.list():
            config = st.selectbox('Optimize Config',my_opt_config.list(), index = my_opt_config.list().index(my_opt.oc.name))    
            if config != my_opt.oc.name:
                my_opt.oc = my_opt_config.find_config(config)
                my_opt.oc.load()
                st.rerun()
        if st.button(f"Edit {my_opt.oc.name}"):
            st.session_state.opt_edit_config = True
            st.rerun()
        if my_opt.file and my_opt.position >= 0:
           if st.button(":floppy_disk:"):
               my_opt.save(my_opt.position)
    # Create Optimizer GUI
    my_opt.edit_base()
    my_opt.edit_item()
    if st.button("Add to Optimizer Queue"):
        my_opt_queue.add_item(my_opt)
        st.session_state.opt_queue = True
        st.rerun()

def opt_queue():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.rerun()
        if st.button(":back:"):
            del st.session_state.opt_queue
            my_opt.file = None
            st.rerun()
    my_opt_queue.options()
    my_opt_queue.view_queue()

def opt_results():
    # Init OptimizeResults
    if 'my_opt_results' in st.session_state:
        my_opt_results = st.session_state.my_opt_results
    else:
        my_opt_results = OptimizeResults()
        st.session_state.my_opt_results = my_opt_results
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            if my_opt_results.results_d:
                my_opt_results.results_d = []
            st.rerun()
        if my_opt_results.layer > 1:
            if st.button(":top:"):
                del st.session_state.opt_results
                del st.session_state.my_opt_results
                st.rerun()    
        if st.button(":back:"):
            if my_opt_results.layer == 1:
                del st.session_state.opt_results
                del st.session_state.my_opt_results
            elif my_opt_results.layer == 2:
                my_opt_results.layer = 1
            elif my_opt_results.layer == 3:
                if my_opt_results.almo > 5:
                    my_opt_results.layer = 1
                    my_opt_results.results = []
                    my_opt_results.results_d = []
                else:
                    my_opt_results.layer = 2
                    my_opt_results.results_d = []
            st.rerun()
    if my_opt_results.layer == 1:
        my_opt_results.view_results_l1()
    elif my_opt_results.layer == 2:
        my_opt_results.view_results_l2()
    elif my_opt_results.layer == 3:
        my_opt_results.view_results_l3()

set_page_config()

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

# Check if PB6 is installed
if not is_pb_installed():
    st.warning('Passivbot Version 6.x is not installed', icon="⚠️")
    st.stop()

# Init OptimizeConfigs
if 'my_opt_config' in st.session_state:
    my_opt_config = st.session_state.my_opt_config
else:
    my_opt_config = OptimizeConfigs()
    st.session_state.my_opt_config = my_opt_config

# Init Optimizer
if 'my_opt' in st.session_state:
    my_opt = st.session_state.my_opt
else:
    my_opt = OptimizeItem()
    st.session_state.my_opt = my_opt

# Init Optimizer Queue
if 'my_opt_queue' in st.session_state:
    my_opt_queue = st.session_state.my_opt_queue
else:
    my_opt_queue = OptimizeQueue()
    st.session_state.my_opt_queue = my_opt_queue

if "opt_queue" in st.session_state:
    opt_queue()
elif "opt_results" in st.session_state:
    opt_results()
elif "opt_edit_config" in st.session_state:
    opt_edit_config()
else:
    opt_edit()
