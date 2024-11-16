import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, is_authenticted
import pbgui_help
from Monitor import Monitor


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

def pbcoindata_overview():
    pbcoindata = st.session_state.pbcoindata
    pbcoindata_status = pbcoindata.is_running()
    if "service_pbcoindata" in st.session_state:
        if st.session_state.service_pbcoindata != pbcoindata_status:
            pbcoindata_status = st.session_state.service_pbcoindata
    st.toggle("PBCoinData", value=pbcoindata_status, key="service_pbcoindata", help=pbgui_help.pbcoindata)
    if pbcoindata_status:
        pbcoindata.run()
        pbcoindata_icon = '✅'
    else:
        pbcoindata.stop()
        pbcoindata_icon = '❌'
    st.metric(label="PBCoinData", value=pbcoindata_icon)
    
def overview():
    st.header("Service Status")
    col_1, col_2, col_3, col_4, col_5 = st.columns([1,1,1,1,1])
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
    with col_5:
        pbcoindata_overview()
        if st.button("Show Details", key="button_pbcoindata_details"):
            st.session_state.pbcoindata_details = True
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
    # Init Monitor
    if "monitor" not in st.session_state:
        st.session_state.monitor = Monitor()
    monitor = st.session_state.monitor
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
            st.session_state.monitor_edit = True
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
        if st.button(f'Sync API-Keys to all',key="sync_api"):
            pbremote.sync_api_up()
    if "server" in st.session_state:
        monitor.server = st.session_state.server
        monitor.view_server()

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

def pbcoindata_details():
    pbcoindata = st.session_state.pbcoindata
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbcoindata_back"):
            del st.session_state.pbcoindata_details
            st.rerun()
    st.header("PBCoinData Details")
    pbcoindata_overview()
    if st.checkbox("Show logfile", key="pbcoindata_log"):
        st.session_state.pbgui_instances.view_log("PBCoinData")

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page("pages/00_login.py")
    st.stop()

# Page Setup
set_page_config("PBGUI Services")
st.header("PBGUI Services", divider="red")

if 'monitor_edit' in st.session_state:
    st.session_state.monitor.edit_monitor_config()
elif 'pbrun_details' in st.session_state:
    pbrun_details()
elif 'pbremote_details' in st.session_state:
    pbremote_details()
elif 'pbstat_details' in st.session_state:
    pbstat_details()
elif 'pbdata_details' in st.session_state:
    pbdata_details()
elif 'pbcoindata_details' in st.session_state:
    pbcoindata_details()
else:
    overview()
