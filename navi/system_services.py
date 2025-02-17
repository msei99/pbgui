import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, is_authenticted, error_popup, info_popup, get_navi_paths, sync_api
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

def pbmon_overview():
    pbmon = st.session_state.pbmon
    pbmon_status = pbmon.is_running()
    if "service_pbmon" in st.session_state:
        if st.session_state.service_pbmon != pbmon_status:
            pbmon_status = st.session_state.service_pbmon
    st.toggle("PBMon", value=pbmon_status, key="service_pbmon", help=pbgui_help.pbmon)
    if pbmon_status:
        pbmon.run()
        pbmon_icon = '✅'
    else:
        pbmon.stop()
        pbmon_icon = '❌'
    st.metric(label="PBMon", value=pbmon_icon)

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
    col_1, col_2, col_3, col_4, col_5, col_6 = st.columns([1,1,1,1,1,1])
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
        pbmon_overview()
        if st.button("Show Details", key="button_pbmon_details"):
            st.session_state.pbmon_details = True
            st.rerun()
    with col_4:
        pbstat_overview()
        if st.button("Show Details", key="button_pbstat_details"):
            st.session_state.pbstat_details = True
            st.rerun()
    with col_5:
        pbdata_overview()
        if st.button("Show Details", key="button_pbdata_details"):
            st.session_state.pbdata_details = True
            st.rerun()
    with col_6:
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
    st.subheader("PBRun Details")
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
        col1, col2, col3, col4 = st.columns([1, 1, 1 ,1])
        with col1:
            if st.button(":material/refresh:"):
                pbremote.update_remote_servers()
                monitor.d_v7 = []
                monitor.d_multi = []
                monitor.d_single = []
                st.rerun()
        with col2:
            if st.button(":material/home:"):
                del st.session_state.pbremote_details
                st.rerun()
        with col3:
            if st.button(":material/save:"):
                pbremote.save_config()
        with col4:
            if st.button(":material/edit:"):
                st.session_state.monitor_edit = True
                st.rerun()
        st.markdown("""---""")
        st.markdown("Remote Servers")
        api_sync = []
        if st.button(f'View All Instances'):
            if "server" in st.session_state:
                del st.session_state.server
            monitor.d_v7 = []
            monitor.d_multi = []
            monitor.d_single = []
            monitor.servers = st.session_state.pbremote.remote_servers
            st.rerun()
        for rserver in sorted(st.session_state.pbremote.remote_servers, key=lambda s: s.name):
            if rserver.is_online():
                color = "green"
                if not rserver.is_api_md5_same(pbremote.api_md5):
                    api_sync.append(rserver)
            else: color = "red"
            col1, col2 = st.columns([3, 1])
            with col1:
                if st.button(f':{color}[{rserver.name}]'):
                    monitor.d_v7 = []
                    monitor.d_multi = []
                    monitor.d_single = []
                    st.session_state.server = rserver
            with col2:
                if color == "red":
                    if st.button(":material/delete:", key=f"delete_{rserver.name}"):
                        rserver.delete_server()
                        pbremote.update_remote_servers()
                        st.rerun()
        sync_api()
                
    st.subheader("PBRemote Details")
    pbremote_overview()
    if pbremote.bucket:
        if pbremote.bucket in pbremote.buckets:
            buckets_index = pbremote.buckets.index(pbremote.bucket)
        else: buckets_index = 0
        if "bucket_config" not in st.session_state:
            st.session_state.bucket_config = pbremote.fetch_bucket_config()
    else: buckets_index = 0
    if st.button("Add bucket", key="pbremote_bucket_add"):
        pbremote.bucket = None
        pbremote.bucket_region = None
        pbremote.bucket_endpoint = None
        pbremote.bucket_access_key_id = None
        pbremote.bucket_secret_access_key = None
        st.session_state.edit_bucket = True
        st.rerun()
    if pbremote.buckets:
        col1, col2 = st.columns([1, 1], vertical_alignment='bottom')
        with col1:
            st.selectbox('Select bucket',pbremote.buckets, index = buckets_index, key="pbremote_bucket", help=pbgui_help.pbremote_bucket)
        with col2:
            if st.button("Edit", key="pbremote_bucket_edit"):
                bucket_config = pbremote.fetch_bucket_config()
                if bucket_config:
                    st.session_state.edit_bucket = True
                    st.rerun()
    else:
        if pbremote.rclone_installed:
            st.write("No bucket found. Please configure rclone by using the 'Add bucket' button.")
        else:
            st.write("rclone not installed. Please install rclone.")
            st.info("Go to VPS Manager, select your local system and install rclone.")
    if st.checkbox("Show logfile", key="pbremote_log"):
        st.session_state.pbgui_instances.view_log("PBRemote")
    if len(api_sync) > 0:
        api_sync_list = []
        for api in api_sync:
            api_sync_list.append(api.name)
        st.subheader("API not in sync with remote servers:")
        st.write(f"{api_sync_list}")
    if "server" in st.session_state:
        monitor.server = st.session_state.server
        monitor.view_server()
        monitor.servers = []
        monitor.servers.append(monitor.server)
        monitor.view_server_instances()
    elif monitor.servers:
        monitor.view_server_instances()
    else:
        st.info("Please select a remote server from the sidebar to view details.")

def edit_bucket():
    # Init PBRemote
    pbremote = st.session_state.pbremote
    # Init keys from session_state
    if "pbremote_bucket_name" in st.session_state:
        if st.session_state.pbremote_bucket_name + ":" != pbremote.bucket:
            pbremote.bucket = st.session_state.pbremote_bucket_name + ":"
    if "pbremote_bucket_region" in st.session_state:
        if st.session_state.pbremote_bucket_region != pbremote.bucket_region:
            pbremote.bucket_region = st.session_state.pbremote_bucket_region
    if "pbremote_bucket_endpoint" in st.session_state:
        if st.session_state.pbremote_bucket_endpoint != pbremote.bucket_endpoint:
            pbremote.bucket_endpoint = st.session_state.pbremote_bucket_endpoint
    if "pbremote_bucket_access_key" in st.session_state:
        if st.session_state.pbremote_bucket_access_key != pbremote.bucket_access_key_id:
            pbremote.bucket_access_key_id = st.session_state.pbremote_bucket_access_key
    if "pbremote_bucket_secret_key" in st.session_state:
        if st.session_state.pbremote_bucket_secret_key != pbremote.bucket_secret_access_key:
            pbremote.bucket_secret_access_key = st.session_state.pbremote_bucket_secret_key
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_edit_bucket_back"):
            del st.session_state.edit_bucket
            st.rerun()
        if st.button(":material/save:"):
            ok, result = pbremote.save_bucket_config()
            if ok:
                result_popup("Bucket saved", result)
                pbremote.fetch_buckets()
            else:
                error_popup(result)
        if st.button(":material/delete:"):
            ok, result = pbremote.delete_bucket()
            if ok:
                result_popup("Bucket deleted", result)
                pbremote.fetch_buckets()
                del st.session_state.edit_bucket
            else:
                error_popup(result)

    # Instructions and link to Synology        
    st.write(
        "1. Get your free 15GB account at [Synology C2](https://c2.synology.com/en-uk/object-storage/overview).\n"
        "2. Create a bucket in your C2 Object Storage.\n"
        "3. Fill in the details below.\n"
        "4. Save the config.\n"
        "5. Test the connection.\n"
        "6. Go back and save the settings.\n"
    )
   
    # Display
    if pbremote.bucket:
        bucket_name = pbremote.bucket[0:-1]
    else:
        bucket_name = ""
    st.text_input("Bucket name", value=bucket_name, key="pbremote_bucket_name")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        st.text_input("region", value=pbremote.bucket_region, key="pbremote_bucket_region")
    with col2:
        st.text_input("endpoint", value=pbremote.bucket_endpoint, key="pbremote_bucket_endpoint")
    with col3:
        st.text_input("access_key_id", value=pbremote.bucket_access_key_id, key="pbremote_bucket_access_key")
    with col4:
        st.text_input("secret_access_key", value=pbremote.bucket_secret_access_key, type="password", key="pbremote_bucket_secret_key")
    if st.button("Test Connection"):
        ok, result = pbremote.test_bucket()
        if ok:
            result_popup("Connection successful", result)
        else:
            error_popup(result)
    st.info("Save your config before testing the connection.")

@st.dialog("Info", width="large")
def result_popup(message, result):
    st.info(f'{message}', icon="✅")
    with st.container(height=1200):
        st.text(result)
    if st.button(":green[OK]"):
        st.rerun()

def pbmon_details():
    pbmon = st.session_state.pbmon
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbmon_back"):
            del st.session_state.pbmon_details
            st.rerun()
    st.subheader("PBMon Details")
    pbmon_overview()

    if "pbmon_telegram_token" in st.session_state:
        if st.session_state.pbmon_telegram_token != pbmon.telegram_token:
            pbmon.telegram_token = st.session_state.pbmon_telegram_token
    else:
        st.session_state.pbmon_telegram_token = pbmon.telegram_token

    if "pbmon_telegram_chat_id" in st.session_state:
        if st.session_state.pbmon_telegram_chat_id != pbmon.telegram_chat_id:
            pbmon.telegram_chat_id = st.session_state.pbmon_telegram_chat_id
    else:
        st.session_state.pbmon_telegram_chat_id = pbmon.telegram_chat_id

    st.text_input("Telegram Bot Token", type="password", key="pbmon_telegram_token", help=pbgui_help.pbmon_telegram_token)
    st.text_input("Telegram Chat ID", key="pbmon_telegram_chat_id", help=pbgui_help.pbmon_telegram_chat_id)

    if st.checkbox("Show logfile", key="pbmon_log"):
        st.session_state.pbgui_instances.view_log("PBMon")

def pbstat_details():
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbstat_back"):
            del st.session_state.pbstat_details
            st.rerun()
    st.subheader("PBStat Details")
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
    st.subheader("PBData Details")
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
    st.subheader("PBCoinData Details")
    pbcoindata_overview()
    if st.checkbox("Show logfile", key="pbcoindata_log"):
        st.session_state.pbgui_instances.view_log("PBCoinData")

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBGUI Services")
st.header("PBGUI Services", divider="red")

if 'monitor_edit' in st.session_state:
    st.session_state.monitor.edit_monitor_config()
elif 'pbrun_details' in st.session_state:
    pbrun_details()
elif 'edit_bucket' in st.session_state:
    edit_bucket()
elif 'pbremote_details' in st.session_state:
    pbremote_details()
elif 'pbmon_details' in st.session_state:
    pbmon_details()
elif 'pbstat_details' in st.session_state:
    pbstat_details()
elif 'pbdata_details' in st.session_state:
    pbdata_details()
elif 'pbcoindata_details' in st.session_state:
    pbcoindata_details()
else:
    overview()
