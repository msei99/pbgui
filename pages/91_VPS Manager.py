import streamlit as st
import pbgui_help
from pbgui_func import set_page_config, is_session_state_initialized, info_popup, error_popup
from pathlib import Path, PurePath
from VPSManager import VPSManager, VPS
import re


def list_vps():
    vpsmanager = st.session_state.vpsmanager
    # Navigation
    with st.sidebar:
        if st.button(":material/add_box:"):
            st.session_state.init_vps = vpsmanager.add_vps()
            st.rerun()
        for vps in vpsmanager.vpss:
            if vps.hostname:
                if st.button(vps.hostname):
                    st.session_state.manage_vps = vps
                    st.rerun()
    st.header("VPS Manager")


def manage_vps():
    vpsmanager = st.session_state.vpsmanager
    vps = st.session_state.manage_vps
    # Init PBRemote
    pbremote = st.session_state.pbremote
    vps.bucket = pbremote.bucket
    # Init coindata
    coindata = st.session_state.pbcoindata
    vps.coinmarketcap_api_key = coindata.api_key
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.manage_vps
            st.rerun()
        if st.button("Initialize"):
            st.session_state.init_vps = vps
            del st.session_state.manage_vps
            st.rerun()
    st.header("Manage VPS " + vps.hostname)
    st.subheader("VPS Status")
    if vps.is_vps_in_hosts():
        hosts_ok = f' ✅'
    else:
        hosts_ok = f' ❌'
    if vps.is_vps_ssh_open():
        ssh_ok = f' ✅'
    else:
        ssh_ok = f' ❌'
    if vps.init_status == "successful":
        init_ok = f' ✅'
    else:
        init_ok = f' ❌'
    if vps.setup_status == "successful":
        setup_ok = f' ✅'
    else:
        setup_ok = f' ❌'
    if "vps_user_pw" in st.session_state:
        if st.session_state.vps_user_pw != vps.user_pw:
            vps.user_pw = st.session_state.vps_user_pw
    if "vps_swap" in st.session_state:
        if st.session_state.vps_swap != vps.swap:
            vps.swap = st.session_state.vps_swap
    # Init Status
    if pbremote.bucket:
        rclone_ok = f' ✅'
    else:
        rclone_ok = f' ❌'
    if coindata.fetch_api_status():
        coindata_ok = f' ✅'
    else:
        coindata_ok = f' ❌'
    
    st.write(
        "- IP and hostname in your local /etc/hosts" + hosts_ok + "\n"
        "- SSH:" + ssh_ok + "\n"
        "- Initialized" + init_ok + " Last Init: " + str(vps.last_init) + "\n"
        "- PBRemote is configured and running" + rclone_ok + "\n"
        "- PBCoinData is configured and running" + coindata_ok + "\n"
        "- Setup finished" + setup_ok + " Last Setup: " + str(vps.last_setup) + "\n"
    )
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.text_input("VPS user password", value=vps.user_pw, type="password", key="vps_user_pw", help=pbgui_help.vps_user_pw)        
    with col2:
        swap_index = ["0", "1G", "1.5G", "2G", "2.5G", "3G", "4G", "5G", "6G", "8G"].index(vps.swap)
        st.selectbox("Swap size", options=["0", "1G", "1.5G", "2G", "2.5G", "3G", "4G", "5G", "6G", "8G"], key="vps_swap", index=swap_index, help=pbgui_help.vps_swap)
    with col3:
        if pbremote.bucket:
            st.text_input('PBRemote bucket', value=vps.bucket, key="vps_bucket", disabled=True, help=pbgui_help.pbremote_bucket)
        else:
            if pbremote.rclone_installed:
                st.write(":red[No bucket found. Please configure rclone.]")
            else:
                st.write(":red[rclone not installed. Please install rclone.]")
    with col4:
        if coindata.api_key:
            if coindata.fetch_api_status():
                st.text_input("CoinMarketCap API_Key", value=vps.coinmarketcap_api_key, type="password", key="vps_coindata_api_key", disabled=True, help=pbgui_help.coindata_api_key)
            else:
                st.write(":red[Invalid CoinMarketCap API_Key]")
        else:
            st.write(":red[Please configure PBCoinData]")
    st.checkbox("Debug", key="setup_debug")
    if st.button("Setup VPS", disabled=not vps.has_setup_parameters()):
         vpsmanager.setup_vps(vps, debug = st.session_state.setup_debug)
         st.session_state.view_setup = vps
         del st.session_state.manage_vps
         st.rerun()

def init_vps():
    # Init vpsmanager
    vpsmanager = st.session_state.vpsmanager
    # Init new VPS
    vps = st.session_state.init_vps
    # Init from session_state keys
    if "vps_ip" in st.session_state:
        if st.session_state.vps_ip != vps.ip:
            vps.ip = st.session_state.vps_ip
    if "vps_hostname" in st.session_state:
        if st.session_state.vps_hostname != vps.hostname:
            vps.hostname = st.session_state.vps_hostname
    if "vps_initial_root_pw" in st.session_state:
        if st.session_state.vps_initial_root_pw != vps.initial_root_pw:
            vps.initial_root_pw = st.session_state.vps_initial_root_pw
    if "vps_root_pw" in st.session_state:
        if st.session_state.vps_root_pw != vps.root_pw:
            vps.root_pw = st.session_state.vps_root_pw
    if "vps_user" in st.session_state:
        if st.session_state.vps_user != vps.user:
            vps.user = st.session_state.vps_user
    if "vps_user_pw" in st.session_state:
        if st.session_state.vps_user_pw != vps.user_pw:
            vps.user_pw = st.session_state.vps_user_pw
    if vps.is_vps_in_hosts():
        hosts_ok = f' ✅'
    else:
        hosts_ok = f' ❌'
    if vps.is_vps_ssh_open():
        ssh_ok = f' ✅'
    else:
        ssh_ok = f' ❌'
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.init_vps
            st.rerun()
        if st.button(":material/save:"):
            vps.save()
            info_popup("VPS saved")
        
    st.header("Add VPS")
    st.subheader("Step 1: Get your VPS")
    st.write(
        "- I can recommend you the following VPS from IONOS\n"
        "- VPS Linux XS, 1 vCore, 1 GB RAM, 10 GB SSD, 1 €/Monat\n"
        "- VPS Linux S, 2 vCores, 2 GB RAM, 80 GB SSD, 3 €/Monat\n"
        "- VPS Linux M, 2 vCores, 4 GB RAM, 120 GB SSD, 6 €/Monat\n"
        "- Please use my [referral link](https://aklam.io/esMFvG)\n"
    )
    st.subheader("Step 2: Install your VPS")
    st.write(
        "- Select the image Ubuntu 24.04\n"
        "- Configure your firewall policy\n"
        "- Permit only ssh port 22. Allow only your IP if it is static"
        )
    st.subheader("Step 3: Add IP and hostname to your local /etc/hosts")
    st.write(
        "- Add IP and hostname to your local /etc/hosts" + hosts_ok + "\n"
        "- You can ssh to your VPS" + ssh_ok + "\n"
    )
    st.subheader("Step 4: Initial Setup of your VPS (run only one time after installation)")
    st.write(
        "1. Set the hostname\n"
        "2. Create a new user with sudo rights\n"
        "3. Set a new root password\n"
        "4. Disable ssh root login\n"
        "5. Add ssh key to new user\n"
    )
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.text_input("VPS IPv4", value=vps.ip, key="vps_ip", help=pbgui_help.vps_ip)
    with col2:
        st.text_input("VPS hostname", value=vps.hostname, key="vps_hostname", help=pbgui_help.vps_hostname)
    with col3:
        st.text_input("VPS root password", value=vps.initial_root_pw, type="password", key="vps_initial_root_pw", help=pbgui_help.vps_initial_root_pw)
    with col4:
        st.text_input("VPS new root password", value=vps.root_pw, type="password", key="vps_root_pw", help=pbgui_help.vps_root_pw)
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.text_input("VPS user name", value=vps.user, key="vps_user", help=pbgui_help.vps_user)
    with col2:
        st.text_input("VPS user password", value=vps.user_pw, type="password", key="vps_user_pw", help=pbgui_help.vps_user_pw)
    st.checkbox("Debug", key="init_debug")
    if st.button("Init VPS", disabled=not vps.has_init_parameters()):
         vpsmanager.init_vps(vps, debug = st.session_state.init_debug)
         st.session_state.view_init = vps
         del st.session_state.init_vps
         st.rerun()

def view_init():
    vps = st.session_state.view_init
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.view_init
            st.rerun()
        if st.button("Manage VPS"):
            st.session_state.manage_vps = vps
            del st.session_state.view_init
            st.rerun()
    st.header("Initialize VPS " + vps.hostname)
    st.write(
        "- Please wait until the initialization is finished.\n"
        "- This can take some minutes.\n"
        "- After initialization is successful you can go to Manage VPS.\n"
        "- If the status is not successful please check the log.\n"
        )
    vps.view_init_status()
    vps.view_init_log()

def view_setup():
    vps = st.session_state.view_setup
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.view_setup
            st.rerun()
        if st.button("Manage VPS"):
            st.session_state.manage_vps = vps
            del st.session_state.view_setup
            st.rerun()
    st.header("Setup VPS " + vps.hostname)
    st.write(
        "- Please wait until the setup is finished.\n"
        "- This can take some minutes.\n"
        "- After setup is successful you can start using your VPS.\n"
        "- If the status is not successful please check the log.\n"
        )
    vps.view_setup_status()
    vps.view_setup_log()

set_page_config("VPS Manager")

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

if not "vpsmanager" in st.session_state:
    st.session_state.vpsmanager = VPSManager()

if 'init_vps' in st.session_state:
    init_vps()
elif 'view_init' in st.session_state:
    view_init()
elif 'view_setup' in st.session_state:
    view_setup()
elif 'manage_vps' in st.session_state:
    manage_vps()
else:
    list_vps()
