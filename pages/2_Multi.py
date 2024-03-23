import streamlit as st
from streamlit_autorefresh import st_autorefresh
from pbgui_func import set_page_config, upload_pbconfigdb
from Instance import Instances, Instance
from Multi import MultiInstance, MultiInstances
from Backtest import BacktestItem
from PBRun import PBRun
from PBStat import PBStat
from PBRemote import PBRemote
from datetime import datetime
import pbgui_help
import pandas as pd
import platform
from time import sleep

def edit_multi_instance():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Init instance
    multi_instance = st.session_state.edit_multi_instance
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.multi_instances
            del st.session_state.edit_multi_instance
            st.rerun()
        if st.button(":floppy_disk:"):
            multi_instance.save()
        if st.button("Activate"):
            PBRun().activate(str(multi_instance.instance_path).split("/")[-1], True)
        if st.button("Refresh from Disk"):
            del st.session_state.pbgui_instances
            st.rerun()
    multi_instance.edit()

def select_instance():
    # Init MultiInstances
    multi_instances = st.session_state.multi_instances
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    if "confirm" in st.session_state:
        st.session_state.confirm = st.checkbox(st.session_state.confirm_text)
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            del st.session_state.multi_instances
            st.rerun()
        if st.button("Add"):
            st.session_state.edit_multi_instance = MultiInstance()
            st.rerun()
    if "editor_select_multi_instance" in st.session_state:
        ed = st.session_state["editor_select_multi_instance"]
        for row in ed["edited_rows"]:
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_multi_instance = multi_instances.instances[row]
                st.rerun()
            if "Delete" in ed["edited_rows"][row]:
                if not "confirm" in st.session_state:
                    st.session_state.confirm_text = f':red[Delete selected instance ({multi_instances.instances[row].user})?]'
                    st.session_state.confirm = False
                    st.rerun()
                elif "confirm" in st.session_state:
                    if st.session_state.confirm:
                        multi_instances.remove(multi_instances.instances[row])
                        PBRun().restart_pbrun()
                        del st.session_state.confirm
                        del st.session_state.confirm_text
                        st.rerun()
    d = []
    for id, instance in enumerate(multi_instances):
        d.append({
            'id': id,
            'Edit': False,
            # 'Running': instance.is_running(),
            'User': instance.user,
            # 'Wallet Exposure': instance.we,
            'Delete': False,
        })
    column_config = {
        "id": None}
    st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key="editor_select_multi_instance", hide_index=None, column_order=None, column_config=column_config, disabled=['id','User'])
    

set_page_config()

# Init session state
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    st.switch_page("pbgui.py")
if 'pbgui_instances' not in st.session_state:
    st.session_state.pbgui_instances = Instances()
if 'multi_instances' not in st.session_state:
    st.session_state.multi_instances = MultiInstances()

if 'edit_multi_instance' in st.session_state:
    edit_multi_instance()
else:
    select_instance()
