import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized, load_ini, save_ini
import pbgui_help
from datetime import datetime
import pandas as pd


def pbrun_overview():
    pbrun = st.session_state.pbrun
    pbrun_status = pbrun.is_running()
    if "service_pbrun" in st.session_state:
        if st.session_state.service_pbrun != pbrun_status:
            pbrun_status = st.session_state.service_pbrun
    st.toggle("PBRun", value=pbrun_status, key="service_pbrun", help=pbgui_help.pbrun)
    if pbrun_status:
        pbrun.run()
        pbrun_icon = '✅'
    else:
        pbrun.stop()
        pbrun_icon = '❌'
    st.metric(label="PBRun", value=pbrun_icon)

def pbremote_overview():
    pbremote = st.session_state.pbremote
    pbremote_status = pbremote.is_running()
    if "service_pbremote" in st.session_state:
        if st.session_state.service_pbremote != pbremote_status:
            pbremote_status = st.session_state.service_pbremote
    st.toggle("PBRemote", value=pbremote_status, key="service_pbremote", help=pbgui_help.pbremote)
    if pbremote_status:
        pbremote.run()
        pbremote_icon = '✅'
    else:
        pbremote.stop()
        pbremote_icon = '❌'
    st.metric(label="PBRemote", value=pbremote_icon)

def pbstat_overview():
    pbstat = st.session_state.pbstat
    pbstat_status = pbstat.is_running()
    if "service_pbstat" in st.session_state:
        if st.session_state.service_pbstat != pbstat_status:
            pbstat_status = st.session_state.service_pbstat
    st.toggle("PBStat", value=pbstat_status, key="service_pbstat", help=pbgui_help.pbstat)
    if pbstat_status:
        pbstat.run()
        pbstat_icon = '✅'
    else:
        pbstat.stop()
        pbstat_icon = '❌'
    st.metric(label="PBStat", value=pbstat_icon)

def pbdata_overview():
    pbdata = st.session_state.pbdata
    pbdata_status = pbdata.is_running()
    if "service_pbdata" in st.session_state:
        if st.session_state.service_pbdata != pbdata_status:
            pbdata_status = st.session_state.service_pbdata
    st.toggle("PBData", value=pbdata_status, key="service_pbdata", help=pbgui_help.pbdata)
    if pbdata_status:
        pbdata.run()
        pbdata_icon = '✅'
    else:
        pbdata.stop()
        pbdata_icon = '❌'
    st.metric(label="PBData", value=pbdata_icon)

def overview():
    st.header("Service Status")
    col_1, col_2, col_3, col_4 = st.columns([1,1,1,1])
    with col_1:
        pbrun_overview()
        if st.button("Show Details", key="button_pbrun_details"):
            st.session_state.pbrun_details = True
            st.rerun()
    with col_2:
        pbremote_overview()
        if st.button("Show Details", key="button_pbremote_details"):
            st.session_state.pbremote_details = True
            st.rerun()
    with col_3:
        pbstat_overview()
        if st.button("Show Details", key="button_pbstat_details"):
            st.session_state.pbstat_details = True
            st.rerun()
    with col_4:
        pbdata_overview()
        if st.button("Show Details", key="button_pbdata_details"):
            st.session_state.pbdata_details = True
            st.rerun()

def pbrun_details():
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbrun_back"):
            del st.session_state.pbrun_details
            st.rerun()
    st.header("PBRun Details")
    pbrun_overview()
    if st.checkbox("Show logfile", key="pbrun_log"):
        st.session_state.pbgui_instances.view_log("PBRun")

def load_monitor_config():
    st.session_state.mem_warning_v7 = load_ini("monitor", "mem_warning_v7")
    if st.session_state.mem_warning_v7 == "":
        st.session_state.mem_warning_v7 = 100
    st.session_state.mem_error_v7 = load_ini("monitor", "mem_error_v7")
    if st.session_state.mem_error_v7 == "":
        st.session_state.mem_error_v7 = 250
    st.session_state.cpu_warning_v7 = load_ini("monitor", "cpu_warning_v7")
    if st.session_state.cpu_warning_v7 == "":
        st.session_state.cpu_warning_v7 = 10
    st.session_state.cpu_error_v7 = load_ini("monitor", "cpu_error_v7")
    if st.session_state.cpu_error_v7 == "":
        st.session_state.cpu_error_v7 = 15
    st.session_state.error_warning_v7 = load_ini("monitor", "error_warning_v7")
    if st.session_state.error_warning_v7 == "":
        st.session_state.error_warning_v7 = 25
    st.session_state.error_error_v7 = load_ini("monitor", "error_error_v7")
    if st.session_state.error_error_v7 == "":
        st.session_state.error_error_v7 = 50
    st.session_state.traceback_warning_v7 = load_ini("monitor", "traceback_warning_v7")
    if st.session_state.traceback_warning_v7 == "":
        st.session_state.traceback_warning_v7 = 25
    st.session_state.traceback_error_v7 = load_ini("monitor", "traceback_error_v7")
    if st.session_state.traceback_error_v7 == "":
        st.session_state.traceback_error_v7 = 50
    # Multi
    st.session_state.mem_warning_multi = load_ini("monitor", "mem_warning_multi")
    if st.session_state.mem_warning_multi == "":
        st.session_state.mem_warning_multi = 50
    st.session_state.mem_error_multi = load_ini("monitor", "mem_error_multi")
    if st.session_state.mem_error_multi == "":
        st.session_state.mem_error_multi = 100
    st.session_state.cpu_warning_multi = load_ini("monitor", "cpu_warning_multi")
    if st.session_state.cpu_warning_multi == "":
        st.session_state.cpu_warning_multi = 5
    st.session_state.cpu_error_multi = load_ini("monitor", "cpu_error_multi")
    if st.session_state.cpu_error_multi == "":
        st.session_state.cpu_error_multi = 10
    st.session_state.error_warning_multi = load_ini("monitor", "error_warning_multi")
    if st.session_state.error_warning_multi == "":
        st.session_state.error_warning_multi = 25
    st.session_state.error_error_multi = load_ini("monitor", "error_error_multi")
    if st.session_state.error_error_multi == "":
        st.session_state.error_error_multi = 50
    st.session_state.traceback_warning_multi = load_ini("monitor", "traceback_warning_multi")
    if st.session_state.traceback_warning_multi == "":
        st.session_state.traceback_warning_multi = 25
    st.session_state.traceback_error_multi = load_ini("monitor", "traceback_error_multi")
    if st.session_state.traceback_error_multi == "":
        st.session_state.traceback_error_multi = 50
    # Single
    st.session_state.mem_warning_single = load_ini("monitor", "mem_warning_single")
    if st.session_state.mem_warning_single == "":
        st.session_state.mem_warning_single = 50
    st.session_state.mem_error_single = load_ini("monitor", "mem_error_single")
    if st.session_state.mem_error_single == "":
        st.session_state.mem_error_single = 100
    st.session_state.cpu_warning_single = load_ini("monitor", "cpu_warning_single")
    if st.session_state.cpu_warning_single == "":
        st.session_state.cpu_warning_single = 5
    st.session_state.cpu_error_single = load_ini("monitor", "cpu_error_single")
    if st.session_state.cpu_error_single == "":
        st.session_state.cpu_error_single = 10
    st.session_state.error_warning_single = load_ini("monitor", "error_warning_single")
    if st.session_state.error_warning_single == "":
        st.session_state.error_warning_single = 25
    st.session_state.error_error_single = load_ini("monitor", "error_error_single")
    if st.session_state.error_error_single == "":
        st.session_state.error_error_single = 50
    st.session_state.traceback_warning_single = load_ini("monitor", "traceback_warning_single")
    if st.session_state.traceback_warning_single == "":
        st.session_state.traceback_warning_single = 25
    st.session_state.traceback_error_single = load_ini("monitor", "traceback_error_single")
    if st.session_state.traceback_error_single == "":
        st.session_state.traceback_error_single = 50


def pbremote_edit():
    pbremote = st.session_state.pbremote
    # Load config
    if "edit_mem_warning_v7" in st.session_state:
        if st.session_state.mem_warning_v7 != st.session_state.edit_mem_warning_v7:
            st.session_state.mem_warning_v7 = st.session_state.edit_mem_warning_v7
    if "edit_mem_error_v7" in st.session_state:
        if st.session_state.mem_error_v7 != st.session_state.edit_mem_error_v7:
            st.session_state.mem_error_v7 = st.session_state.edit_mem_error_v7
    if "edit_cpu_warning_v7" in st.session_state:
        if st.session_state.cpu_warning_v7 != st.session_state.edit_cpu_warning_v7:
            st.session_state.cpu_warning_v7 = st.session_state.edit_cpu_warning_v7
    if "edit_cpu_error_v7" in st.session_state:
        if st.session_state.cpu_error_v7 != st.session_state.edit_cpu_error_v7:
            st.session_state.cpu_error_v7 = st.session_state.edit_cpu_error_v7
    if "edit_error_warning_v7" in st.session_state:
        if st.session_state.error_warning_v7 != st.session_state.edit_error_warning_v7:
            st.session_state.error_warning_v7 = st.session_state.edit_error_warning_v7
    if "edit_error_error_v7" in st.session_state:
        if st.session_state.error_error_v7 != st.session_state.edit_error_error_v7:
            st.session_state.error_error_v7 = st.session_state.edit_error_error_v7
    if "edit_traceback_warning_v7" in st.session_state:
        if st.session_state.traceback_warning_v7 != st.session_state.edit_traceback_warning_v7:
            st.session_state.traceback_warning_v7 = st.session_state.edit_traceback_warning_v7
    if "edit_traceback_error_v7" in st.session_state:
        if st.session_state.traceback_error_v7 != st.session_state.edit_traceback_error_v7:
            st.session_state.traceback_error_v7 = st.session_state.edit_traceback_error_v7
    # Multi
    if "edit_mem_warning_multi" in st.session_state:
        if st.session_state.mem_warning_multi != st.session_state.edit_mem_warning_multi:
            st.session_state.mem_warning_multi = st.session_state.edit_mem_warning_multi
    if "edit_mem_error_multi" in st.session_state:
        if st.session_state.mem_error_multi != st.session_state.edit_mem_error_multi:
            st.session_state.mem_error_multi = st.session_state.edit_mem_error_multi
    if "edit_cpu_warning_multi" in st.session_state:
        if st.session_state.cpu_warning_multi != st.session_state.edit_cpu_warning_multi:
            st.session_state.cpu_warning_multi = st.session_state.edit_cpu_warning_multi
    if "edit_cpu_error_multi" in st.session_state:
        if st.session_state.cpu_error_multi != st.session_state.edit_cpu_error_multi:
            st.session_state.cpu_error_multi = st.session_state.edit_cpu_error_multi
    if "edit_error_warning_multi" in st.session_state:
        if st.session_state.error_warning_multi != st.session_state.edit_error_warning_multi:
            st.session_state.error_warning_multi = st.session_state.edit_error_warning_multi
    if "edit_error_error_multi" in st.session_state:
        if st.session_state.error_error_multi != st.session_state.edit_error_error_multi:
            st.session_state.error_error_multi = st.session_state.edit_error_error_multi
    if "edit_traceback_warning_multi" in st.session_state:
        if st.session_state.traceback_warning_multi != st.session_state.edit_traceback_warning_multi:
            st.session_state.traceback_warning_multi = st.session_state.edit_traceback_warning_multi
    if "edit_traceback_error_multi" in st.session_state:
        if st.session_state.traceback_error_multi != st.session_state.edit_traceback_error_multi:
            st.session_state.traceback_error_multi = st.session_state.edit_traceback_error_multi
    # Single
    if "edit_mem_warning_single" in st.session_state:
        if st.session_state.mem_warning_single != st.session_state.edit_mem_warning_single:
            st.session_state.mem_warning_single = st.session_state.edit_mem_warning_single
    if "edit_mem_error_single" in st.session_state:
        if st.session_state.mem_error_single != st.session_state.edit_mem_error_single:
            st.session_state.mem_error_single = st.session_state.edit_mem_error_single
    if "edit_cpu_warning_single" in st.session_state:
        if st.session_state.cpu_warning_single != st.session_state.edit_cpu_warning_single:
            st.session_state.cpu_warning_single = st.session_state.edit_cpu_warning_single
    if "edit_cpu_error_single" in st.session_state:
        if st.session_state.cpu_error_single != st.session_state.edit_cpu_error_single:
            st.session_state.cpu_error_single = st.session_state.edit_cpu_error_single
    if "edit_error_warning_single" in st.session_state:
        if st.session_state.error_warning_single != st.session_state.edit_error_warning_single:
            st.session_state.error_warning_single = st.session_state.edit_error_warning_single
    if "edit_error_error_single" in st.session_state:
        if st.session_state.error_error_single != st.session_state.edit_error_error_single:
            st.session_state.error_error_single = st.session_state.edit_error_error_single
    if "edit_traceback_warning_single" in st.session_state:
        if st.session_state.traceback_warning_single != st.session_state.edit_traceback_warning_single:
            st.session_state.traceback_warning_single = st.session_state.edit_traceback_warning_single
    if "edit_traceback_error_single" in st.session_state:
        if st.session_state.traceback_error_single != st.session_state.edit_traceback_error_single:
            st.session_state.traceback_error_single = st.session_state.edit_traceback_error_single
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            del st.session_state.pbremote_edit
            del st.session_state.mem_warning_v7
            del st.session_state.mem_error_v7
            del st.session_state.cpu_warning_v7
            del st.session_state.cpu_error_v7
            del st.session_state.error_warning_v7
            del st.session_state.error_error_v7
            del st.session_state.traceback_warning_v7
            del st.session_state.traceback_error_v7
            del st.session_state.mem_warning_multi
            del st.session_state.mem_error_multi
            del st.session_state.cpu_warning_multi
            del st.session_state.cpu_error_multi
            del st.session_state.error_warning_multi
            del st.session_state.error_error_multi
            del st.session_state.traceback_warning_multi
            del st.session_state.traceback_error_multi
            del st.session_state.mem_warning_single
            del st.session_state.mem_error_single
            del st.session_state.cpu_warning_single
            del st.session_state.cpu_error_single
            del st.session_state.error_warning_single
            del st.session_state.error_error_single
            del st.session_state.traceback_warning_single
            del st.session_state.traceback_error_single
            st.session_state.pbremote_details = True
            st.rerun()
        if st.button(":material/save:"):
            save_ini("monitor", "mem_warning_v7", str(st.session_state.mem_warning_v7))
            save_ini("monitor", "mem_error_v7", str(st.session_state.mem_error_v7))
            save_ini("monitor", "cpu_warning_v7", str(st.session_state.cpu_warning_v7))
            save_ini("monitor", "cpu_error_v7", str(st.session_state.cpu_error_v7))
            save_ini("monitor", "error_warning_v7", str(st.session_state.error_warning_v7))
            save_ini("monitor", "error_error_v7", str(st.session_state.error_error_v7))
            save_ini("monitor", "traceback_warning_v7", str(st.session_state.traceback_warning_v7))
            save_ini("monitor", "traceback_error_v7", str(st.session_state.traceback_error_v7))
            save_ini("monitor", "mem_warning_multi", str(st.session_state.mem_warning_multi))
            save_ini("monitor", "mem_error_multi", str(st.session_state.mem_error_multi))
            save_ini("monitor", "cpu_warning_multi", str(st.session_state.cpu_warning_multi))
            save_ini("monitor", "cpu_error_multi", str(st.session_state.cpu_error_multi))
            save_ini("monitor", "error_warning_multi", str(st.session_state.error_warning_multi))
            save_ini("monitor", "error_error_multi", str(st.session_state.error_error_multi))
            save_ini("monitor", "traceback_warning_multi", str(st.session_state.traceback_warning_multi))
            save_ini("monitor", "traceback_error_multi", str(st.session_state.traceback_error_multi))
            save_ini("monitor", "mem_warning_single", str(st.session_state.mem_warning_single))
            save_ini("monitor", "mem_error_single", str(st.session_state.mem_error_single))
            save_ini("monitor", "cpu_warning_single", str(st.session_state.cpu_warning_single))
            save_ini("monitor", "cpu_error_single", str(st.session_state.cpu_error_single))
            save_ini("monitor", "error_warning_single", str(st.session_state.error_warning_single))
            save_ini("monitor", "error_error_single", str(st.session_state.error_error_single))
            save_ini("monitor", "traceback_warning_single", str(st.session_state.traceback_warning_single))
            save_ini("monitor", "traceback_error_single", str(st.session_state.traceback_error_single))
            st.session_state.pbremote_details = True
            del st.session_state.pbremote_edit
            st.rerun()
    st.header("PBRemote Edit")
    st.subheader("V7 Monitor Settings")
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.number_input('Memory Warning',value=int(st.session_state.mem_warning_v7),step=10, key="edit_mem_warning_v7")
        st.number_input('Error Warning',value=int(st.session_state.error_warning_v7),step=1, key="edit_error_warning_v7")
    with col2:
        st.number_input('Memory Error',value=int(st.session_state.mem_error_v7),step=10, key="edit_mem_error_v7")
        st.number_input('Error Error',value=int(st.session_state.error_error_v7),step=1, key="edit_error_error_v7")
    with col3:
        st.number_input('CPU Warning',value=int(st.session_state.cpu_warning_v7),step=1, key="edit_cpu_warning_v7")
        st.number_input('Traceback Warning',value=int(st.session_state.traceback_warning_v7),step=1, key="edit_traceback_warning_v7")
    with col4:
        st.number_input('CPU Error',value=int(st.session_state.cpu_error_v7),step=1, key="edit_cpu_error_v7")
        st.number_input('Traceback Error',value=int(st.session_state.traceback_error_v7),step=1, key="edit_traceback_error_v7")
    st.subheader("Multi Monitor Settings")
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.number_input('Memory Warning',value=int(st.session_state.mem_warning_multi),step=10, key="edit_mem_warning_multi")
        st.number_input('Error Warning',value=int(st.session_state.error_warning_multi),step=1, key="edit_error_warning_multi")
    with col2:
        st.number_input('Memory Error',value=int(st.session_state.mem_error_multi),step=10, key="edit_mem_error_multi")
        st.number_input('Error Error',value=int(st.session_state.error_error_multi),step=1, key="edit_error_error_multi")
    with col3:
        st.number_input('CPU Warning',value=int(st.session_state.cpu_warning_multi),step=1, key="edit_cpu_warning_multi")
        st.number_input('Traceback Warning',value=int(st.session_state.traceback_warning_multi),step=1, key="edit_traceback_warning_multi")
    with col4:
        st.number_input('CPU Error',value=int(st.session_state.cpu_error_multi),step=1, key="edit_cpu_error_multi")
        st.number_input('Traceback Error',value=int(st.session_state.traceback_error_multi),step=1, key="edit_traceback_error_multi")
    st.subheader("Single Monitor Settings")
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.number_input('Memory Warning',value=int(st.session_state.mem_warning_single),step=10, key="edit_mem_warning_single")
        st.number_input('Error Warning',value=int(st.session_state.error_warning_single),step=1, key="edit_error_warning_single")
    with col2:
        st.number_input('Memory Error',value=int(st.session_state.mem_error_single),step=10, key="edit_mem_error_single")
        st.number_input('Error Error',value=int(st.session_state.error_error_single),step=1, key="edit_error_error_single")
    with col3:
        st.number_input('CPU Warning',value=int(st.session_state.cpu_warning_single),step=1, key="edit_cpu_warning_single")
        st.number_input('Traceback Warning',value=int(st.session_state.traceback_warning_single),step=1, key="edit_traceback_warning_single")
    with col4:
        st.number_input('CPU Error',value=int(st.session_state.cpu_error_single),step=1, key="edit_cpu_error_single")
        st.number_input('Traceback Error',value=int(st.session_state.traceback_error_single),step=1, key="edit_traceback_error_single")

def pbremote_details():
    # Init PBRemote
    pbremote = st.session_state.pbremote
    # Init Monitor defaults
    if "mem_warning_v7" not in st.session_state:
        load_monitor_config()
    # Init from session_state keys
    if "pbremote_bucket" in st.session_state:
        if st.session_state.pbremote_bucket != pbremote.bucket:
            pbremote.bucket = st.session_state.pbremote_bucket
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.pbremote_details
            st.rerun()
        if st.button(":material/save:"):
            pbremote.save_config()
        if st.button(":material/edit:"):
            st.session_state.pbremote_edit = True
            del st.session_state.pbremote_details
            st.rerun()
        st.markdown("""---""")
        st.markdown("Remote Servers")
        api_sync = []
        for rserver in pbremote.remote_servers:
            if rserver.is_online():
                color = "green"
                if not rserver.is_api_md5_same(pbremote.api_md5):
                    api_sync.append(rserver)
            else: color = "red"
            if st.button(f':{color}[{rserver.name}]'):
                st.session_state.server = rserver
    st.header("PBRemote Details")
    pbremote_overview()
    if pbremote.bucket:
        buckets_index = pbremote.buckets.index(pbremote.bucket)
    else: buckets_index = 0
    if pbremote.buckets:
        st.selectbox('Select bucket',pbremote.buckets, index = buckets_index, key="pbremote_bucket", help=pbgui_help.pbremote_bucket)
    else:
        if pbremote.rclone_installed:
            st.write("No bucket found. Please configure rclone.")
        else:
            st.write("rclone not installed. Please install rclone.")
    if st.checkbox("Show logfile", key="pbremote_log"):
        st.session_state.pbgui_instances.view_log("PBRemote")
    if len(api_sync) > 0:
        api_sync_list = []
        for api in api_sync:
            api_sync_list.append(api.name)
        st.header("API not in sync with remote servers:")
        st.write(f"{api_sync_list}")
        if st.button(f'Sync API to all',key="sync_api"):
            pbremote.sync_api_up()
    if "server" in st.session_state:
        server = st.session_state.server
        if server.is_online():
            color = "green"
        else: color = "red"
        st.markdown(f'### Remote Server: :{color}[{server.name}] ({server.rtd}s)')
        col_1, col_2 = st.columns([1,1])
        with col_1:
            mem_total = int(server.mem[0] / 1024 / 1024)
            mem_free = int(server.mem[1] / 1024 / 1024)
            mem_used = int(server.mem[3] / 1024 / 1024)
            mem_usage = int(server.mem[2])
            st.progress(mem_usage, text=f'### Memory Free: :green[{mem_free}] MB  |  Used: :red[{mem_used}] MB  |  Total: :blue[{mem_total}] MB')
            disk_total = int(server.disk[0] / 1024 / 1024)
            disk_used = int(server.disk[1] / 1024 / 1024)
            disk_free = int(server.disk[2] / 1024 / 1024)
            disk_usage = int(server.disk[3])
            st.progress(disk_usage, text=f'### Disk Free: :green[{disk_free}] MB  |  Used: :red[{disk_used}] MB  |  Total: :blue[{disk_total}] MB')
        with col_2:
            swap_total = int(server.swap[0] / 1024 / 1024)
            swap_used = int(server.swap[1] / 1024 / 1024)
            swap_free = int(server.swap[2] / 1024 / 1024)
            swap_usage = min(int(server.swap[3]),100)
            st.progress(swap_usage, text=f'### Swap Free: :green[{swap_free}] MB  |  Used: :red[{swap_used}] MB  |  Total: :blue[{swap_total}] MB')
            boot = datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S")
            if server.cpu > 90:
                cpu_color = "red"
            elif server.cpu < 50:
                cpu_color = "green"
            else:
                cpu_color = "yellow"
            st.markdown(f"##### CPU utilization: :{cpu_color}[{server.cpu}] %  |  System boot: :blue[{boot}]")
        v7_selected = None
        if f"pbremote_v7_select" in st.session_state:
            v7_selected = st.session_state.pbremote_v7_select
        multi_selected = None
        if f"pbremote_multi_select" in st.session_state:
            multi_selected = st.session_state.pbremote_multi_select
        single_selected = None
        if f"pbremote_single_select" in st.session_state:
            single_selected = st.session_state.pbremote_single_select
        d_v7 = []
        d_multi = []
        d_single = []
        if server.monitor:
            for monitor in server.monitor:
                info = ({
                    # u = user
                    # p = pb_version
                    # v = version
                    # st = start_time
                    # m = memory
                    # c = cpu
                    # i = info
                    # it = infos_today
                    # iy = infos_yesterday
                    # e = error
                    # et = errors_today
                    # ey = errors_yesterday
                    # t = traceback
                    # tt = tracebacks_today
                    # ty = tracebacks_yesterday
                    # pt = pnl_today
                    # py = pnl_yesterday
                    # ct = pnl_counter_today
                    # cy = pnl_counter_yesterday
                    'Name': monitor["u"],
                    'PB Version': monitor["p"],
                    'Version': monitor["v"],
                    'Start Time': datetime.fromtimestamp(monitor["st"]),
                    'Memory': monitor["m"][0]/1024/1024,
                    'CPU': monitor["c"],
                    'PNLs Today': monitor["ct"],
                    'PNL Today': monitor["pt"],
                    'PNLs Yesterday': monitor["cy"],
                    'PNL Yesterday': monitor["py"],
                    'Last Info': monitor["i"],
                    'Infos Today': monitor["it"],
                    'Infos Yesterday': monitor["iy"],
                    'Last Error': monitor["e"],
                    'Errors Today': monitor["et"],
                    'Errors Yesterday': monitor["ey"],
                    'Last Traceback': monitor["t"],
                    'Tracebacks Today': monitor["tt"],
                    'Tracebacks Yesterday': monitor["ty"]
                })
                if info["PB Version"] == "7":
                    d_v7.append(info)
                elif info["PB Version"] == "6":
                    d_multi.append(info)
                elif info["PB Version"] == "s":
                    d_single.append(info)
        column_config = {
            "PB Version": None,
            "Last Info": None,
            "Last Error": None,
            "Last Traceback": None,
            "Memory": st.column_config.NumberColumn(format="%.2f MB"),
            "CPU": st.column_config.NumberColumn(format="%.2f %%"),
        }
        st.header(f"Running V7 Instances ({len(d_v7)})")
        if d_v7:
            df = pd.DataFrame(d_v7)
            sdf = df.style.map(lambda x: 'color: green' if x < float(st.session_state.cpu_warning_v7) else 'color: orange' if x < float(st.session_state.cpu_error_v7) else 'color: red', subset=['CPU'])
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.mem_warning_v7) else 'color: orange' if x < float(st.session_state.mem_error_v7) else 'color: red', subset=['Memory'])
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors green if 0, orange if <10 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.error_warning_v7) else 'color: orange' if x < float(st.session_state.error_error_v7) else 'color: red', subset=['Errors Today', 'Errors Yesterday'])
            #Tracebacks green if 0, orange if <5 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.traceback_warning_v7) else 'color: orange' if x < float(st.session_state.traceback_error_v7) else 'color: red', subset=['Tracebacks Today', 'Tracebacks Yesterday'])
            #PNLs green if > 0, orange if 0
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange', subset=['PNLs Today', 'PNLs Yesterday'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['PNL Today', 'PNL Yesterday'])
            st.dataframe(data=sdf, use_container_width=True, height=36+(len(d_v7))*35, key="pbremote_v7_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if v7_selected:
                if v7_selected["selection"]["rows"]:
                    row = v7_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{d_v7[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{d_v7[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{d_v7[row]['Last Traceback']}]")
        else:
            st.write("None")
        st.header(f"Running Multi Instances ({len(d_multi)})")
        if d_multi:
            df = pd.DataFrame(d_multi)
            sdf = df.style.map(lambda x: 'color: green' if x < float(st.session_state.cpu_warning_v7) else 'color: orange' if x < float(st.session_state.cpu_error_v7) else 'color: red', subset=['CPU'])
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.mem_warning_v7) else 'color: orange' if x < float(st.session_state.mem_error_v7) else 'color: red', subset=['Memory'])
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors green if 0, orange if <10 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.error_warning_v7) else 'color: orange' if x < float(st.session_state.error_error_v7) else 'color: red', subset=['Errors Today', 'Errors Yesterday'])
            #Tracebacks green if 0, orange if <5 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.traceback_warning_v7) else 'color: orange' if x < float(st.session_state.traceback_error_v7) else 'color: red', subset=['Tracebacks Today', 'Tracebacks Yesterday'])
            #PNLs green if > 0, orange if 0
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange', subset=['PNLs Today', 'PNLs Yesterday'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['PNL Today', 'PNL Yesterday'])
            st.dataframe(data=sdf, use_container_width=True, height=36+(len(d_multi))*35, key="pbremote_multi_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if multi_selected:
                if multi_selected["selection"]["rows"]:
                    row = multi_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{d_multi[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{d_multi[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{d_multi[row]['Last Traceback']}]")
        else:
            st.write("None")
        st.header(f"Running Single Instances ({len(d_single)})")
        if d_single:
            df = pd.DataFrame(d_single)
            sdf = df.style.map(lambda x: 'color: green' if x < float(st.session_state.cpu_warning_v7) else 'color: orange' if x < float(st.session_state.cpu_error_v7) else 'color: red', subset=['CPU'])
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.mem_warning_v7) else 'color: orange' if x < float(st.session_state.mem_error_v7) else 'color: red', subset=['Memory'])
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors green if 0, orange if <10 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.error_warning_v7) else 'color: orange' if x < float(st.session_state.error_error_v7) else 'color: red', subset=['Errors Today', 'Errors Yesterday'])
            #Tracebacks green if 0, orange if <5 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.traceback_warning_v7) else 'color: orange' if x < float(st.session_state.traceback_error_v7) else 'color: red', subset=['Tracebacks Today', 'Tracebacks Yesterday'])
            #PNLs green if > 0, orange if 0
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange', subset=['PNLs Today', 'PNLs Yesterday'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['PNL Today', 'PNL Yesterday'])
            st.dataframe(data=sdf, use_container_width=True, height=36+(len(d_single))*35, key="pbremote_single_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if single_selected:
                if single_selected["selection"]["rows"]:
                    row = single_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{d_single[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{d_single[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{d_single[row]['Last Traceback']}]")
        else:
            st.write("None")

def pbstat_details():
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbstat_back"):
            del st.session_state.pbstat_details
            st.rerun()
    st.header("PBStat Details")
    pbstat_overview()
    if st.checkbox("Show logfile", key="pbstat_log"):
        st.session_state.pbgui_instances.view_log("PBStat")

def pbdata_details():
    pbdata = st.session_state.pbdata
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbdata_back"):
            del st.session_state.pbdata_details
            st.rerun()
    st.header("PBData Details")
    pbdata_overview()
    users = st.session_state.users

    if "pbdata_users" in st.session_state:
        if st.session_state.pbdata_users != pbdata.fetch_users:
            pbdata.fetch_users = st.session_state.pbdata_users
    st.multiselect('Users', users.list(), default=pbdata.fetch_users ,key="pbdata_users")

    if st.checkbox("Show logfile", key="pbdata_log"):
        st.session_state.pbgui_instances.view_log("PBData")

set_page_config()

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

if 'pbrun_details' in st.session_state:
    pbrun_details()
elif 'pbremote_details' in st.session_state:
    pbremote_details()
elif 'pbstat_details' in st.session_state:
    pbstat_details()
elif 'pbdata_details' in st.session_state:
    pbdata_details()
elif 'pbremote_edit' in st.session_state:
    pbremote_edit()
else:
    overview()
