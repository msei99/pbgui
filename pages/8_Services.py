import streamlit as st
from pbgui_func import set_page_config
import pbgui_help
from PBRun import PBRun
from PBStat import PBStat
from PBRemote import PBRemote
from PBShare import PBShare
from Instance import Instances
from pathlib import Path

def init_status():
    if "pbrun" not in st.session_state:
        st.session_state.pbrun = PBRun()
    if "pbremote" not in st.session_state:
        st.session_state.pbremote = PBRemote()
    if "pbstat" not in st.session_state:
        st.session_state.pbstat = PBStat()
    if "pbshare" not in st.session_state:
        st.session_state.pbshare = PBShare()

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

def pbshare_overview():
    pbshare = st.session_state.pbshare
    pbshare_status = pbshare.is_running()
    if "service_pbshare" in st.session_state:
        if st.session_state.service_pbshare != pbshare_status:
            pbshare_status = st.session_state.service_pbshare
    st.toggle("PBShare", value=pbshare_status, key="service_pbshare", help=pbgui_help.pbshare)
    if pbshare_status:
        pbshare.run()
        pbshare_icon = '✅'
    else:
        pbshare.stop()
        pbshare_icon = '❌'
    st.metric(label="PBShare", value=pbshare_icon)

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
        pbshare_overview()
        if st.button("Show Details", key="button_pbshare_details"):
            st.session_state.pbshare_details = True
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
        if st.button(":back:", key="button_pbremote_back"):
            del st.session_state.pbremote_details
            st.rerun()
        if st.button(":floppy_disk:"):
            pbremote.save_config()
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

def pbshare_details():
    # Init PBShare
    pbshare = st.session_state.pbshare
    # Init from session_state keys
    if "pbshare_bucket" in st.session_state:
        if st.session_state.pbshare_bucket != pbshare.bucket:
            pbshare.bucket = st.session_state.pbshare_bucket
    if "pbshare_interval" in st.session_state:
        if st.session_state.pbshare_interval != pbshare.interval:
            pbshare.interval = st.session_state.pbshare_interval
    if "pbshare_upload_images" in st.session_state:
        if st.session_state.pbshare_upload_images != pbshare.upload_images:
            pbshare.upload_images = st.session_state.pbshare_upload_images
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbshare_back"):
            del st.session_state.pbshare_details
            st.rerun()
        if st.button(":floppy_disk:"):
            pbshare.save_config()
    st.header("PBShare Details")
    pbshare_overview()
    if pbshare.bucket:
        buckets_index = pbshare.buckets.index(pbshare.bucket)
    else: buckets_index = 0
    if pbshare.buckets:
        col1, col2 = st.columns([1,1])
        with col1:
            st.selectbox('Upload to remote',pbshare.buckets, index = buckets_index, key="pbshare_bucket", help=pbgui_help.pbshare_bucket)
        with col2:
            st.number_input("Interval", min_value=0, value=pbshare.interval, step=300, format="%.d", key="pbshare_interval", help=pbgui_help.pbshare_interval)
        st.checkbox("Upload grid images", value=pbshare.upload_images, key="pbshare_upload_images", help=pbgui_help.pbshare_upload_images)
    else:
        if pbshare.rclone_installed:
            st.write("No remotes found. Please configure rclone.")
        else:
            st.write("rclone not installed. Please install rclone.")
    index = Path(f'{pbshare.griddir}/index.html')
    if index.exists():
        st.download_button("Download index.html", index.read_text(), "index.html", key="pbshare_download_index", help=pbgui_help.pbshare_download_index)
    if st.checkbox("Show logfile", key="pbshare_log"):
        st.session_state.pbgui_instances.view_log("PBShare")

set_page_config()

# Init Session State
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    st.switch_page("pbgui.py")
if 'pbgui_instances' not in st.session_state:
    st.session_state.pbgui_instances = Instances()
init_status()

if 'pbrun_details' in st.session_state:
    pbrun_details()
elif 'pbremote_details' in st.session_state:
    pbremote_details()
elif 'pbstat_details' in st.session_state:
    pbstat_details()
elif 'pbshare_details' in st.session_state:
    pbshare_details()
else:
    overview()
