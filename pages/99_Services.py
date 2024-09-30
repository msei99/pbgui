import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized
import pbgui_help
from datetime import datetime


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

def pbremote_details():
    # Init PBRemote
    pbremote = st.session_state.pbremote
    # Init from session_state keys
    if "pbremote_bucket" in st.session_state:
        if st.session_state.pbremote_bucket != pbremote.bucket:
            pbremote.bucket = st.session_state.pbremote_bucket
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.rerun()
        if st.button(":back:", key="button_pbremote_back"):
            del st.session_state.pbremote_details
            st.rerun()
        if st.button(":floppy_disk:"):
            pbremote.save_config()
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
        d_single = []
        server.instances_status_single.instances = []
        server.instances_status_single.load()
        for single in server.instances_status_single:
            if single.running:
                d_single.append({
                    'Name': single.name,
                    'Version': single.version
                })
        d_multi = []
        server.instances_status.instances = []
        server.instances_status.load()
        for multi in server.instances_status:
            if multi.running:
                d_multi.append({
                    'Name': multi.name,
                    'Version': multi.version
                })
        d_v7 = []
        server.instances_status.instances_v7 = []
        server.instances_status_v7.load()
        for v7 in server.instances_status_v7:
            if v7.running:
                d_v7.append({
                    'Name': v7.name,
                    'Version': v7.version
                })
        # d_old = []
        # for old in server.run:
        #     d_old.append({
        #         'User': old["user"],
        #         'Symbol': old["symbol"],
        #     })
        st.header(f"Running V7 Instances ({len(d_v7)})")
        if d_v7:
            st.dataframe(data=d_v7, width=640, height=36+(len(d_v7))*35)
        else:
            st.write("None")
        st.header(f"Running Single Instances ({len(d_single)})")
        if d_single:
            st.dataframe(data=d_single, width=640, height=36+(len(d_single))*35)
        else:
            st.write("None")
        st.header(f"Running Multi Instances ({len(d_multi)})")
        if d_multi:
            st.dataframe(data=d_multi, width=640, height=36+(len(d_multi))*35)
        else:
            st.write("None")
        # if d_old:
        #     st.header(f"Running old PBRun/PBRemote Single Instances ({len(d_old)})")
        #     st.dataframe(data=d_old, width=640, height=36+(len(d_old))*35)


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
else:
    overview()
