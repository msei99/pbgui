import streamlit as st
import pbgui_help
from pbgui_func import set_page_config, is_session_state_not_initialized, info_popup, error_popup, is_authenticted, get_navi_paths, sync_api, select_file
from VPSManager import VPSManager, VPS
import re
from Monitor import Monitor
from datetime import datetime
from PBCoinData import CoinData
import psutil
import subprocess
import shlex
import getpass
import concurrent.futures


def list_vps():
    vpsmanager = st.session_state.vpsmanager
    pbremote = st.session_state.pbremote
    timestamp = round(datetime.now().timestamp())
    if timestamp - pbremote.systemts > 3600:
        with st.spinner("Loading git origins..."):
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    future = ex.submit(pbremote.local_run.load_git_origin)
                    future.result(timeout=5)
            except concurrent.futures.TimeoutError:
                error_popup("Timeout: 'Loading git origins...' exceeded 5s")
            except Exception as e:
                error_popup(f"Error loading git origins: {e}")
        with st.spinner("Loading versions origins..."):
            pbremote.local_run.load_versions_origin()
        with st.spinner("Loading local Versions..."):
            pbremote.local_run.load_versions()
        with st.spinner("Loading local git commits..."):
            pbremote.local_run.load_git_commits()
        with st.spinner("Loading git branches history..."):
            if hasattr(pbremote.local_run, 'load_git_branches_history'):
                pbremote.local_run.load_git_branches_history()
        with st.spinner("Loading local Sever available updates..."):
            pbremote.local_run.has_upgrades()
        with st.spinner("Loading local Server need reboot"):
            pbremote.local_run.has_reboot()
        pbremote.systemts = timestamp

    # Determine if there are VPS to import (slaves missing from local list)
    existing_hostnames = [v.hostname for v in vpsmanager.vpss if v.hostname]
    vps_to_import = [s for s in pbremote.remote_servers if s.role == "slave" and s.name not in existing_hostnames]

    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            vpsmanager.vpss = []
            vpsmanager.find_vps()
            pbremote.systemts = 0
            pbremote.update_remote_servers()
            st.rerun()
        if st.button(":material/add_box:"):
            st.session_state.init_vps = vpsmanager.add_vps()
            st.rerun()

        # Only show sudo password if there are VPS to import
        if vps_to_import:
            local_user = getpass.getuser()
            st.text_input(f"sudo password for :blue[{local_user}]", type="password", key="sudo_pw", help=pbgui_help.sudo_pw)

        if pbremote.is_running() and pbremote.local_run.is_running():
            color = "green"
        else:
            color = "red"
        if st.button(f':{color}[{pbremote.name} (local)]'):
            st.session_state.manage_master = True
            if "monitor" in st.session_state:
                st.session_state.monitor.d_v7 = []
                st.session_state.monitor.d_multi = []
                st.session_state.monitor.d_single = []
            st.rerun()

        for vps in vpsmanager.vpss:
            if vps.hostname:
                server = pbremote.find_server(vps.hostname)
                color = "red"
                rtd = 9999
                if server:
                    if server.is_online():
                        color = "green"
                    if server.rtd:
                        rtd = server.rtd
                        if rtd > 9999:
                            rtd = 9999
                if st.button(f':{color}[{vps.hostname} ({rtd}s)]', key=f'vps_{vps.hostname}'):
                    if "monitor" in st.session_state:
                        st.session_state.monitor.d_v7 = []
                        st.session_state.monitor.d_multi = []
                        st.session_state.monitor.d_single = []
                    st.session_state.manage_vps = vps
                    st.rerun()

    st.subheader("Overview")
    if not "ed_key" in st.session_state:
        st.session_state.ed_key = 0
    d = []
    # Add Master
    if pbremote.is_running():
        online = "✅"
    else:
        online = "❌"
    if pbremote.local_run.reboot:
        reboot = "❌"
    else:
        reboot = "✅"
    
    # Branch-aware comparison for Master PBGui
    master_branch = pbremote.local_run.pbgui_branch
    
    # Get origin version/commit for the master's branch
    if master_branch != "unknown" and hasattr(pbremote.local_run, 'pbgui_branches_data'):
        # Try to get the latest commit for this branch from branches_data
        if master_branch in pbremote.local_run.pbgui_branches_data:
            branch_commits = pbremote.local_run.pbgui_branches_data[master_branch]
            if branch_commits:
                origin_commit_for_branch = branch_commits[0]['full']  # First commit is HEAD
                if pbremote.local_run.pbgui_commit == origin_commit_for_branch:
                    pbgui = "✅"
                else:
                    pbgui = f"❌ {pbremote.local_run.pbgui_version} (..{origin_commit_for_branch[-5:]})"
            else:
                # No commits found for branch
                pbgui = f"⚠️ {pbremote.local_run.pbgui_version}"
        else:
            # Branch not in branches_data
            pbgui = f"⚠️ {pbremote.local_run.pbgui_version}"
    elif master_branch == "main":
        # For main branch, use the traditional origin comparison
        if pbremote.local_run.pbgui_version == pbremote.local_run.pbgui_version_origin and pbremote.local_run.pbgui_commit == pbremote.local_run.pbgui_commit_origin:
            pbgui = "✅"
        else:
            pbgui = f"❌ {pbremote.local_run.pbgui_version_origin} (..{pbremote.local_run.pbgui_commit_origin[-5:]})"
    else:
        # Unknown branch
        pbgui = f"⚠️ {pbremote.local_run.pbgui_version}"
    
    if pbremote.local_run.pb6_version == pbremote.local_run.pb6_version_origin and pbremote.local_run.pb6_commit == pbremote.local_run.pb6_commit_origin:
        pb6 = "✅"
    else:
        pb6 = f"❌ {pbremote.local_run.pb6_version_origin} (..{pbremote.local_run.pb6_commit_origin[-5:]})"
    if pbremote.local_run.pb7_version == pbremote.local_run.pb7_version_origin and pbremote.local_run.pb7_commit == pbremote.local_run.pb7_commit_origin:
        pb7 = "✅"
    else:
        pb7 = f"❌ {pbremote.local_run.pb7_version_origin} (..{pbremote.local_run.pb7_commit_origin[-5:]})"
    d.append({
        "Name": pbremote.name + " (local)",
        "Online": online,
        "Role": "🧠",
        "Start": datetime.fromtimestamp(psutil.boot_time()).strftime("%Y-%m-%d %H:%M:%S"),
        "Reboot": reboot,
        "Updates": pbremote.local_run.upgrades,
        "PBGui": f'{pbremote.pbgui_version}',
        "PBGui Branch": f'{master_branch} ({pbremote.local_run.pbgui_commit[:7]})',
        "PBGui github": pbgui,
        "PB6": f'{pbremote.pb6_version}',
        "PB6 github": pb6,
        "PB7": f'{pbremote.pb7_version}',
        "PB7 github": pb7,
        "API Sync": "✅"
    })
    # Add VPS
    all_api_sync = True
    for server in sorted(st.session_state.pbremote.remote_servers, key=lambda s: s.name):
        if server.name not in [vps.hostname for vps in vpsmanager.vpss]:
            if server.role == "slave":
                if f'add_vps_{server.name}' not in st.session_state:
                    st.session_state[f'add_vps_{server.name}'] = vpsmanager.add_vps()
                vps = st.session_state[f'add_vps_{server.name}']
                vps.hostname = server.name
                st.write(f"Detected missing VPS :green[{server.name}]")
                vps.ip = vps.fetch_vps_ip_from_hosts()
                if not vps.ip:
                    st.write("VPS IP not found in /etc/hosts.")
                    if "vps_ip" in st.session_state:
                        if st.session_state.vps_ip != vps.ip:
                            # Check if self.ip is a valid IPv4 address
                            if re.match(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$", st.session_state.vps_ip):
                                vps.ip = st.session_state.vps_ip
                            else:
                                st.session_state.vps_ip = vps.ip
                                error_popup("Error: IP address is not valid")
                    else:
                        st.session_state.vps_ip = vps.ip
                    st.text_input("VPS IPv4", key="vps_ip", help=pbgui_help.vps_ip)
                    # removed per-host password field; use global sudo_pw instead

                    if st.button(
                        "Add IP to /etc/hosts", 
                        disabled=not st.session_state.get("sudo_pw") or not st.session_state.get("vps_ip")
                    ):
                        entry = f"{vps.ip} {vps.hostname}"
                        try:
                            pw_escaped = shlex.quote(st.session_state.sudo_pw)
                            entry_escaped = shlex.quote(entry)
                            cmd = f'echo {pw_escaped} | sudo -S bash -c "echo {entry_escaped} >> /etc/hosts"'
                            proc = subprocess.run(cmd, shell=True, text=True, capture_output=True)
                            if proc.returncode == 0:
                                info_popup(f"Added {entry} to /etc/hosts (via sudo)")
                                st.stop()
                            else:
                                err = proc.stderr.strip() or proc.stdout.strip()
                                error_popup(f"Failed to write to /etc/hosts: {err}")
                                st.stop()
                        except Exception as e:
                            error_popup(f"Error while trying sudo write to /etc/hosts: {e}")
                            st.stop()
                    else:
                        st.info(f"Please provide the IP for :green[{vps.hostname}] and your local sudo password to add the entry to /etc/hosts.")
                        st.stop()
                else:
                    st.text_input("VPS user name", value=vps.user, key="vps_user", help=pbgui_help.vps_user)
                    if "vps_user_pw" in st.session_state:
                        if st.session_state.vps_user_pw != vps.user_pw:
                            vps.user_pw = st.session_state.vps_user_pw
                    else:
                        st.session_state.vps_user_pw = vps.user_pw
                    st.text_input("VPS user password", type="password", key="vps_user_pw", help=pbgui_help.vps_user_pw)
                    if st.button("Read VPS settings", disabled=not st.session_state.vps_user_pw):
                        vps.user = st.session_state.vps_user
                        vps.user_pw = st.session_state.vps_user_pw
                        st.write("Trying to login via SSH...")
                        if not vps.can_login_ssh():
                            error_popup("Error: Cannot login via SSH. Please check username and password.")
                            st.stop()
                            break
                        vps.bucket = pbremote.bucket
                        info = vps.fetch_vps_info()
                        vps.install_pb6 = info["pb6"]
                        vps.coinmarketcap_api_key = info["coinmarketcap"]
                        vps.swap = info["swap"]
                        vps.firewall, vps.firewall_ssh_ips = vps.fetch_ufw_settings()
                        st.rerun()
                    else:
                        if vps.bucket:
                            # Print settings
                            st.write("Fetched VPS settings:")
                            st.write(f"- IP: :blue[{vps.ip}]")
                            st.write(f"- Bucket: :blue[{vps.bucket}]")
                            st.write(f"- CoinMarketCap API Key: :blue[{vps.coinmarketcap_api_key}]")
                            st.write(f"- PB6 installed: :blue[{vps.install_pb6}]")
                            st.write(f"- Swap enabled: :blue[{vps.swap}]")
                            st.write(f"- Firewall enabled: :blue[{vps.firewall}]")
                            st.write(f"- Firewall SSH IPs: :blue[{vps.firewall_ssh_ips}]")
                            if st.button("Add VPS with this settings"):
                                # Add the VPS with the fetched settings
                                st.write(vps.hostname + " added successfully.")
                                vps.save()
                                if "vps_user_pw" in st.session_state:
                                    del st.session_state.vps_user_pw
                                vpsmanager.vpss = []
                                vpsmanager.find_vps()
                                st.rerun()
                        else:
                            st.info(f"Please provide the password for :green[{vps.user}] on :green[{vps.hostname}] to fetch settings via SSH.")
                        st.stop()
                        break

        boot = datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S")
        if server.is_online():
            online = "✅"
        else:
            online = "❌"
        if server.role == "master":
            role = "🧠"
        elif server.role == "slave":
            role = "💻"
        else:
            role = "❓"
        
        # Branch-aware comparison for PBGui
        server_branch = getattr(server, "pbgui_branch", "unknown")
        server_commit_short = server.pbgui_commit[:7] if server.pbgui_commit else "unknown"
        
        # Get origin version/commit for the server's branch
        if server_branch != "unknown" and hasattr(pbremote.local_run, 'pbgui_branches_data'):
            # Try to get the latest commit for this branch from branches_data
            if server_branch in pbremote.local_run.pbgui_branches_data:
                branch_commits = pbremote.local_run.pbgui_branches_data[server_branch]
                if branch_commits:
                    origin_commit_for_branch = branch_commits[0]['full']  # First commit is HEAD
                    if server.pbgui_commit == origin_commit_for_branch:
                        pbgui = "✅"
                    else:
                        pbgui = f"❌ {server.pbgui_version} (..{origin_commit_for_branch[-5:]})"
                else:
                    # No commits found for branch
                    pbgui = f"⚠️ {server.pbgui_version}"
            else:
                # Branch not in branches_data
                pbgui = f"⚠️ {server.pbgui_version}"
        elif server_branch == "main":
            # For main branch, use the traditional origin comparison
            if server.pbgui_version == pbremote.local_run.pbgui_version_origin and server.pbgui_commit == pbremote.local_run.pbgui_commit_origin:
                pbgui = "✅"
            else:
                pbgui = f"❌ {pbremote.local_run.pbgui_version_origin} (..{pbremote.local_run.pbgui_commit_origin[-5:]})"
        else:
            # Unknown branch
            pbgui = f"⚠️ {server.pbgui_version}"
        
        if server.pb6_version == pbremote.local_run.pb6_version_origin and server.pb6_commit == pbremote.local_run.pb6_commit_origin:
            pb6 = "✅"
        else:
            pb6 = f"❌ {pbremote.local_run.pb6_version_origin} (..{pbremote.local_run.pb6_commit_origin[-5:]})"
        if server.pb7_version == pbremote.local_run.pb7_version_origin and server.pb7_commit == pbremote.local_run.pb7_commit_origin:
            pb7 = "✅"
        else:
            pb7 = f"❌ {pbremote.local_run.pb7_version_origin} (..{pbremote.local_run.pb7_commit_origin[-5:]})"
        if server.reboot:
            reboot = "❌"
        else:
            reboot = "✅"
        if server.is_api_md5_same(pbremote.api_md5):
            api_sync = "✅"
        else:
            api_sync = "❌"
            all_api_sync = False
        d.append({
            "Name": server.name,
            "Online": online,
            "Role": role,
            "Start": boot,
            "Reboot": reboot,
            "Updates": server.upgrades,
            "PBGui": f'{server.pbgui_version}',
            "PBGui Branch": f'{server_branch} ({server_commit_short})',
            "PBGui github": pbgui,
            "PB6": f'{server.pb6_version}',
            "PB6 github": pb6,
            "PB7": f'{server.pb7_version}',
            "PB7 github": pb7,
            "API Sync": api_sync
        })
    st.data_editor(data=d, height=36+(len(d))*35, key=f"vps_overview_{st.session_state.ed_key}")
    st.info("Select your VPS in the sidebar to get a detailed VPS report.")
    with st.sidebar:
        sync_api()

def manage_master():
    vpsmanager = st.session_state.vpsmanager
    # Init PBRemote
    pbremote = st.session_state.pbremote
    # Init coindata
    coindata = st.session_state.pbcoindata
    # Init Monitor
    if "monitor" not in st.session_state:
        st.session_state.monitor = Monitor()
    monitor = st.session_state.monitor
    # Navigation
    with st.sidebar:
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button(":material/refresh:"):
                pbremote.update_remote_servers()
                monitor.d_v7 = []
                monitor.d_multi = []
                monitor.d_single = []
                st.rerun()
        with col2:
            if st.button(":material/home:"):
                del st.session_state.manage_master
                st.rerun()
        st.checkbox("Debug", key="setup_debug")
        if st.button("Update PBGui, PB6 and PB7"):
            vpsmanager.command = "master-update-pb"
            vpsmanager.command_text = "Update PBGui, PB6 and PB7"
            vpsmanager.update_master(debug = st.session_state.setup_debug)
            del st.session_state.manage_master
            st.session_state.view_update_master = True
            st.rerun()
        # Update only PBGui
        if st.button("Update PBGui"):
            vpsmanager.command = "master-update-pbgui"
            vpsmanager.command_text = "Update PBGui"
            vpsmanager.update_master(debug = st.session_state.setup_debug)
            del st.session_state.manage_master
            st.session_state.view_update_master = True
            st.rerun()
        if st.button("Update pb6 and pb7"):
            vpsmanager.command = "master-update-pbonly"
            vpsmanager.command_text = "Update pb6 and pb7"
            vpsmanager.update_master(debug = st.session_state.setup_debug)
            del st.session_state.manage_master
            st.session_state.view_update_master = True
            st.rerun()
        st.text_input("sudo password", type="password", key="sudo_pw", help=pbgui_help.sudo_pw)
        enable_install = False
        if "sudo_pw" in st.session_state:
            if st.session_state.sudo_pw != "":
                enable_install = True
        if st.button("Install rustup", disabled=not enable_install):
            vpsmanager.command = "master-install-rustup"
            vpsmanager.command_text = "Install rustup"
            vpsmanager.update_master(debug = st.session_state.setup_debug, sudo_pw = st.session_state.sudo_pw)
            del st.session_state.manage_master
            st.session_state.view_update_master = True
            st.rerun()
        if st.button("Install rclone", disabled=not enable_install):
            vpsmanager.command = "master-install-rclone"
            vpsmanager.command_text = "Install rclone"
            vpsmanager.update_master(debug = st.session_state.setup_debug, sudo_pw = st.session_state.sudo_pw)
            del st.session_state.manage_master
            st.session_state.view_update_master = True
            st.rerun()

    # Init Status
    if pbremote.bucket:
        rclone_ok = f' ✅'
    else:
        rclone_ok = f' ❌'
    if coindata.fetch_api_status():
        coindata_ok = f' ✅'
    else:
        coindata_ok = f' ❌'
    if vpsmanager.update_status == "successful":
        update_ok = f' ✅' 
    else:
        update_ok = f' ❌'


    st.subheader(f"Local Status {pbremote.name}")
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.empty()
    with col2:
        st.write(
            "- PBRemote is configured and running" + rclone_ok + "\n"
            "- PBCoinData is configured and running" + coindata_ok + "\n"
        )
    with col3:
        st.write(
            "- Last command: " + vpsmanager.command_text + " " + update_ok + " " + str(vpsmanager.last_update) + "\n"
        )
    d = []
    boot = datetime.fromtimestamp(pbremote.boot).strftime("%Y-%m-%d %H:%M:%S")
    if pbremote.is_online():
        online = "✅"
    else:
        online = "❌"
    
    # Branch-aware comparison for PBGui
    master_branch = pbremote.local_run.pbgui_branch
    pbgui_branches_data = pbremote.local_run.pbgui_branches_data
    if master_branch in pbgui_branches_data and pbgui_branches_data[master_branch]:
        # Compare with HEAD of the actual branch
        origin_commit = pbgui_branches_data[master_branch][0]['full']
        if pbremote.pbgui_commit == origin_commit:
            pbgui = "✅"
        else:
            pbgui = f"❌ {pbremote.pbgui_version} (..{origin_commit[-5:]})"
    elif master_branch == "main":
        # Fallback to origin/main comparison for main branch
        if pbremote.pbgui_version == pbremote.local_run.pbgui_version_origin and pbremote.pbgui_commit == pbremote.local_run.pbgui_commit_origin:
            pbgui = "✅"
        else:
            pbgui = f"❌ {pbremote.local_run.pbgui_version_origin} (..{pbremote.local_run.pbgui_commit_origin[-5:]})"
    else:
        # Unknown branch or no branch data
        pbgui = "⚠️ version"
    if pbremote.pb6_version == pbremote.local_run.pb6_version_origin and pbremote.pb6_commit == pbremote.local_run.pb6_commit_origin:
        pb6 = "✅"
    else:
        pb6 = f"❌ {pbremote.local_run.pb6_version_origin} (..{pbremote.local_run.pb6_commit_origin[-5:]})"
    if pbremote.pb7_version == pbremote.local_run.pb7_version_origin and pbremote.pb7_commit == pbremote.local_run.pb7_commit_origin:
        pb7 = "✅"
    else:
        pb7 = f"❌ {pbremote.local_run.pb7_version_origin} (..{pbremote.local_run.pb7_commit_origin[-5:]})"
    if pbremote.local_run.reboot:
        reboot = "❌"
    else:
        reboot = "✅"
    d.append({
        "Name": pbremote.name,
        "Online": online,
        "Start": datetime.fromtimestamp(pbremote.boot).strftime("%Y-%m-%d %H:%M:%S"),
        "Reboot": reboot,
        "Updates": pbremote.local_run.upgrades,
        "PBGui": f'{pbremote.pbgui_version}',
        "PBGui Branch": f'{pbremote.local_run.pbgui_branch} ({pbremote.local_run.pbgui_commit[:7]})',
        "PBGui github": pbgui,
        "PB6": f'{pbremote.pb6_version}',
        "PB6 github": pb6,
        "PB7": f'{pbremote.pb7_version}',
        "PB7 github": pb7
    })
    
    # Branch Management Section - directly above table
    with st.expander("🔀 **Local Branch Management**", expanded=False):
        # Get branch list - with backward compatibility check
        available_branches = []
        current_branch = getattr(pbremote.local_run, 'pbgui_branch', 'unknown')
        current_commit_full = getattr(pbremote.local_run, 'pbgui_commit', '')
        
        if hasattr(pbremote.local_run, 'pbgui_branches_data') and pbremote.local_run.pbgui_branches_data:
            available_branches = list(pbremote.local_run.pbgui_branches_data.keys())
        
        if available_branches:
            # Current state display
            st.info(f"📍 **Current:** {current_branch} @ {current_commit_full[:7]}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Branch selector
                try:
                    current_index = available_branches.index(current_branch)
                except ValueError:
                    current_index = 0
                
                selected_branch = st.selectbox(
                    "Target Branch",
                    available_branches,
                    index=current_index,
                    key="pbgui_branch_selector"
                )
                
                # Reload and Load More buttons below branch selector
                col_btn1, col_btn2, col_btn3 = st.columns(3)
                with col_btn1:
                    if st.button("🔄 Reload", key="reload_master_branches", use_container_width=True):
                        with st.spinner("Reloading..."):
                            if hasattr(pbremote.local_run, 'load_git_branches_history'):
                                pbremote.local_run.load_git_branches_history()
                            # Reset commit counters for all branches
                            if 'master_commits_loaded' in st.session_state:
                                del st.session_state.master_commits_loaded
                        st.rerun()
                with col_btn2:
                    if st.button("🔽 +50", key="load_more_top_master", use_container_width=True):
                        if 'master_commits_loaded' not in st.session_state:
                            st.session_state.master_commits_loaded = {}
                        current_count = st.session_state.master_commits_loaded.get(selected_branch, 50)
                        new_count = current_count + 50
                        st.session_state.master_commits_loaded[selected_branch] = new_count
                        pbremote.local_run.load_more_commits(selected_branch, new_count)
                        st.rerun()
                with col_btn3:
                    if st.button("🔽 All", key="load_all_top_master", use_container_width=True):
                        if 'master_commits_loaded' not in st.session_state:
                            st.session_state.master_commits_loaded = {}
                        st.session_state.master_commits_loaded[selected_branch] = 999999
                        pbremote.local_run.load_more_commits(selected_branch, 999999)
                        st.rerun()
            
            with col2:
                # Commit selector for the selected branch
                if selected_branch in pbremote.local_run.pbgui_branches_data:
                    commits = pbremote.local_run.pbgui_branches_data[selected_branch]
                    
                    # Create commit labels (shortened for selectbox)
                    commit_options = []
                    for c in commits:
                        is_current = (c['full'] == current_commit_full and selected_branch == current_branch)
                        prefix = "🔹 CURRENT: " if is_current else ""
                        # Use first line only for selectbox display, shorten to 50 chars
                        # Replace newlines with space to prevent selectbox breaking
                        first_line = c['message'].split('\n')[0].replace('\n', ' ').replace('\r', ' ')
                        short_msg = first_line[:50] + "..." if len(first_line) > 50 else first_line
                        label = f"{prefix}{c['short']} | {short_msg} | {c['date']} | {c['author']}"
                        commit_options.append(label)
                    
                    # Find current commit index for default selection
                    if selected_branch == current_branch:
                        try:
                            current_commit_index = next(i for i, c in enumerate(commits) if c['full'] == current_commit_full)
                            # Add 1 to account for HEAD option
                            current_commit_index = current_commit_index + 1
                        except StopIteration:
                            current_commit_index = 0
                    else:
                        # Different branch selected - default to HEAD
                        current_commit_index = 0
                    
                    selected_commit_label = st.selectbox(
                        f"Target Commit ({len(commits)} loaded - optional, leave at HEAD for latest)",
                        options=["HEAD (latest)"] + commit_options,
                        index=current_commit_index,
                        key="pbgui_commit_selector"
                    )
                    
                    # Extract selected commit details
                    if selected_commit_label != "HEAD (latest)":
                        selected_commit_idx = commit_options.index(selected_commit_label)
                        selected_commit_data = commits[selected_commit_idx]
                        selected_commit_hash = selected_commit_data['full']
                    
                        # Show commit details with message in tooltip
                        st.markdown(f"**Commit:** `{selected_commit_data['short']}` | **Author:** {selected_commit_data['author']} | **Date:** {selected_commit_data['date']}")
                        st.markdown(f"**Full Hash:** `{selected_commit_data['full']}`")
                        # Show first line of message, full message in tooltip
                        short_message = selected_commit_data['message'].split('\n')[0]
                        st.markdown(f"**Message:** {short_message}", help=selected_commit_data['message'])
                    else:
                        # HEAD selected
                        selected_commit_hash = current_commit_full
                    
                    # Switch button
                    branch_changed = selected_branch != current_branch
                    commit_changed = (selected_commit_label != "HEAD (latest)" and selected_commit_hash != current_commit_full)
                    
                    # Check if already on target
                    is_on_target = (selected_branch == current_branch and not commit_changed)
                    
                    # Status text and button in one row
                    status_col, btn_col = st.columns([3, 1])
                    
                    with status_col:
                        if is_on_target:
                            st.success(f"✅ Already on branch `{selected_branch}` at the target commit")
                        else:
                            if branch_changed:
                                st.warning(f"⚠️ This will switch from `{current_branch}` to `{selected_branch}`")
                            elif commit_changed:
                                st.warning(f"⚠️ This will switch to commit `{selected_commit_hash[:7]}`")
                    
                    with btn_col:
                        if st.button("🔀 Switch Branch", disabled=is_on_target, type="primary"):
                            vpsmanager.command = "master-switch-pbgui-branch"
                            vpsmanager.command_text = f"Switch to {selected_branch}"
                            if selected_commit_label != "HEAD (latest)":
                                vpsmanager.command_text += f" @ {selected_commit_hash[:7]}"
                            # Pass branch and commit to Ansible playbook
                            extra_vars = {'branch': selected_branch}
                            if selected_commit_label != "HEAD (latest)":
                                extra_vars['commit'] = selected_commit_hash
                            vpsmanager.update_master(
                                debug=st.session_state.setup_debug,
                                extra_vars=extra_vars
                            )
                            del st.session_state.manage_master
                            st.session_state.view_update_master = True
                            st.rerun()
                else:
                    st.error(f"No commits found for branch: {selected_branch}")
        else:
            st.warning("⚠️ No branch history loaded. Click 🔄 Refresh in sidebar to load branch data.")
    
    st.data_editor(data=d, height=36+(len(d))*35, key=f"vps_overview_{st.session_state.ed_key}")
    monitor.server = pbremote
    monitor.servers = []
    monitor.servers.append(monitor.server)
    monitor.view_server()
    monitor.view_server_instances()

def manage_vps():
    vpsmanager = st.session_state.vpsmanager
    if "manage_vps_select_vps" in st.session_state:
        if st.session_state.manage_vps_select_vps != st.session_state.manage_vps.hostname:
            st.session_state.manage_vps = vpsmanager.find_vps_by_hostname(st.session_state.manage_vps_select_vps)
            del st.session_state.vps_user_pw
            del st.session_state.vps_coindata_api_key
            del st.session_state.vps_install_pb6
            del st.session_state.vps_swap
            del st.session_state.vps_firewall
            del st.session_state.vps_firewall_ssh_port
            del st.session_state.vps_firewall_ssh_ips
            if "monitor" in st.session_state:
                st.session_state.monitor.d_v7 = []
                st.session_state.monitor.d_multi = []
                st.session_state.monitor.d_single = []
    vps = st.session_state.manage_vps
    # Init PBRemote
    pbremote = st.session_state.pbremote
    vps.bucket = pbremote.bucket
    # Init Monitor
    if "monitor" not in st.session_state:
        st.session_state.monitor = Monitor()
    monitor = st.session_state.monitor
    # Init VPS coindata
    if "vps_coindata" not in st.session_state:
        st.session_state.vps_coindata = CoinData()
    vps_coindata = st.session_state.vps_coindata
    if not vps.coinmarketcap_api_key:
        vps.coinmarketcap_api_key = vps_coindata.api_key
    else:
        vps_coindata.api_key = vps.coinmarketcap_api_key
    # Init keys from session_state
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
    if vps.update_status == "successful":
        update_ok = f' ✅' 
    else:
        update_ok = f' ❌'
    if "vps_user_pw" in st.session_state:
        if st.session_state.vps_user_pw != vps.user_pw:
            vps.user_pw = st.session_state.vps_user_pw
    else:
        st.session_state.vps_user_pw = vps.user_pw
    if "vps_swap" in st.session_state:
        if st.session_state.vps_swap != vps.swap:
            vps.swap = st.session_state.vps_swap
    else:
        st.session_state.vps_swap = vps.swap
    if "vps_coindata_api_key" in st.session_state:
        if st.session_state.vps_coindata_api_key != vps.coinmarketcap_api_key:
            vps.coinmarketcap_api_key = st.session_state.vps_coindata_api_key
            vps_coindata.api_key = st.session_state.vps_coindata_api_key
    else:
        st.session_state.vps_coindata_api_key = vps.coinmarketcap_api_key
    if "vps_install_pb6" in st.session_state:
        if st.session_state.vps_install_pb6 != vps.install_pb6:
            vps.install_pb6 = st.session_state.vps_install_pb6
    else:
        st.session_state.vps_install_pb6 = vps.install_pb6
    if "vps_firewall" in st.session_state:
        if st.session_state.vps_firewall != vps.firewall:
            vps.firewall = st.session_state.vps_firewall
    else:
        st.session_state.vps_firewall = vps.firewall
    if "vps_firewall_ssh_port" in st.session_state:
        if st.session_state.vps_firewall_ssh_port != vps.firewall_ssh_port:
            vps.firewall_ssh_port = st.session_state.vps_firewall_ssh_port
    else:
        st.session_state.vps_firewall_ssh_port = vps.firewall_ssh_port
    if "vps_firewall_ssh_ips" in st.session_state:
        if st.session_state.vps_firewall_ssh_ips != vps.firewall_ssh_ips:
            # regex for ip check: "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
            if all([re.match(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$", ip) for ip in st.session_state.vps_firewall_ssh_ips.split(",")]):
                vps.firewall_ssh_ips = st.session_state.vps_firewall_ssh_ips
            elif st.session_state.vps_firewall_ssh_ips == "":
                vps.firewall_ssh_ips = st.session_state.vps_firewall_ssh_ips
            else:
                st.session_state.vps_firewall_ssh_ips = vps.firewall_ssh_ips
    else:
        st.session_state.vps_firewall_ssh_ips = vps.firewall_ssh_ips
    # Init Status
    if "rclone_test" not in st.session_state:
        st.session_state.rclone_test, detail_result = pbremote.test_bucket()
    if st.session_state.rclone_test:
        rclone_ok = f' ✅'
    else:
        rclone_ok = f' ❌'
    if vps_coindata.fetch_api_status():
        coindata_ok = f' ✅'
    else:
        coindata_ok = f' ❌'
    # Navigation
    with st.sidebar:
        st.selectbox("VPS", vpsmanager.list(), index=vpsmanager.list().index(vps.hostname), key='manage_vps_select_vps')
        st.checkbox("Debug", key="setup_debug")
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button(":material/refresh:"):
                monitor.d_v7 = []
                monitor.d_multi = []
                monitor.d_single = []
                if "rclone_test" in st.session_state:
                    del st.session_state.rclone_test
                st.rerun()
        with col2:
            if st.button(":material/home:"):
                del st.session_state.manage_vps
                st.rerun()
        with col3:
            if st.button(":material/delete:"):
                vps.delete()
                del st.session_state.vpsmanager
                del st.session_state.manage_vps
                st.rerun()

        # New: Read settings from VPS
        if st.button("Read settings from VPS", disabled=not vps.has_user_pw()):
            # ensure creds from UI are used
            if "vps_user_pw" in st.session_state and st.session_state.vps_user_pw:
                vps.user_pw = st.session_state.vps_user_pw
            st.write("Trying to login via SSH...")
            if not vps.can_login_ssh():
                error_popup("Error: Cannot login via SSH. Please check username and password.")
                st.stop()
            vps.bucket = pbremote.bucket
            info = vps.fetch_vps_info()
            vps.install_pb6 = info["pb6"]
            vps.coinmarketcap_api_key = info["coinmarketcap"]
            # make sure swap value is valid
            vps.swap = info.get("swap", "0") if info.get("swap") in ["0", "1G", "1.5G", "2G", "2.5G", "3G", "4G", "5G", "6G", "8G"] else "0"
            vps.firewall, vps.firewall_ssh_ips = vps.fetch_ufw_settings()
            # save
            vps.save()
            # sync back to controls
            st.session_state.vps_swap = vps.swap
            st.session_state.vps_coindata_api_key = vps.coinmarketcap_api_key
            st.session_state.vps_install_pb6 = vps.install_pb6
            st.session_state.vps_firewall = vps.firewall
            st.session_state.vps_firewall_ssh_ips = vps.firewall_ssh_ips
            info_popup("VPS settings refreshed.")

        if st.button("Initialize"):
            st.session_state.init_vps = vps
            del st.session_state.manage_vps
            st.rerun()
        if st.button("Update PBGui"):
            vps.command = "vps-update-pbgui"
            vps.command_text = "Update PBGui"
            vpsmanager.update_vps(vps, debug = st.session_state.setup_debug)
            st.session_state.view_update = vps
            del st.session_state.manage_vps
            st.rerun()
        if st.button("Update PBGui, PB6 and PB7"):
            vps.command = "vps-update-pb"
            vps.command_text = "Update PBGui, PB6 and PB7"
            vpsmanager.update_vps(vps, debug = st.session_state.setup_debug)
            st.session_state.view_update = vps
            del st.session_state.manage_vps
            st.rerun()
        col1, col2 = st.columns([1,0.8])
        with col1:
            if st.button("Update Linux", disabled=not vps.has_user_pw()):
                vps.command = "vps-update"
                vps.command_text = "Update Linux"
                vps.reboot = st.session_state.update_reboot
                vpsmanager.update_vps(vps, debug = st.session_state.setup_debug)
                st.session_state.view_update = vps
                del st.session_state.manage_vps
                st.rerun()
        with col2:
            st.checkbox("Reboot", key="update_reboot")
        if st.button("Reboot VPS", disabled=not vps.has_user_pw()):
            vps.command = "vps-reboot"
            vps.command_text = "Reboot VPS"
            vpsmanager.update_vps(vps, debug = st.session_state.setup_debug)
            st.session_state.view_update = vps
            del st.session_state.manage_vps
            st.rerun()
        if st.button("Cleanup VPS", disabled=not vps.has_user_pw()):
            vps.command = "vps-cleanup"
            vps.command_text = "Cleanup VPS"
            vpsmanager.update_vps(vps, debug = st.session_state.setup_debug)
            st.session_state.view_update = vps
            del st.session_state.manage_vps
            st.rerun()
        if st.button("Resize Swap", disabled=not vps.has_user_pw()):
            vps.command = "vps-resize-swap"
            vps.command_text = "Resize Swap"
            vpsmanager.update_vps(vps, debug = st.session_state.setup_debug)
            st.session_state.view_update = vps
            del st.session_state.manage_vps
            st.rerun()
        if st.button("Update Firewall Settings", disabled=not vps.has_user_pw()):
            vps.command = "vps-update-firewall"
            vps.command_text = "Update Firewall Settings"
            vpsmanager.update_vps(vps, debug = st.session_state.setup_debug)
            st.session_state.view_update = vps
            del st.session_state.manage_vps
            st.rerun()
        if st.button("Update CoinData API"):
            vps.command = "vps-update-coindata"
            vps.command_text = "Update CoinData API"
            vpsmanager.update_vps(vps, debug = st.session_state.setup_debug)
            st.session_state.view_update = vps
            del st.session_state.manage_vps
            st.rerun()

    st.subheader(f"VPS Status: {vps.hostname}")
    with st.expander("VPS Setup Settings", expanded = vps.setup_status != "successful"):
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.write(
                "- IP and hostname in your local /etc/hosts" + hosts_ok + "\n"
                "- SSH:" + ssh_ok + "\n"
                "- Initialized" + init_ok + " Last Init: " + str(vps.last_init) + "\n"
            )
        with col2:
            st.write(
                "- PBRemote is configured and running" + rclone_ok + "\n"
                "- PBCoinData is configured and running" + coindata_ok + "\n"
                "- Setup finished" + setup_ok + " " + str(vps.last_setup) + "\n"
            )
        with col3:
            st.write(
                "- Last command: " + vps.command_text + " " + update_ok + " " + str(vps.last_update) + "\n"
            )

        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.text_input("VPS user password", value=vps.user_pw, type="password", key="vps_user_pw", help=pbgui_help.vps_user_pw)
        with col2:
            swap_index = ["0", "1G", "1.5G", "2G", "2.5G", "3G", "4G", "5G", "6G", "8G"].index(vps.swap or "0")
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
            if vps_coindata.api_key:
                if vps_coindata.fetch_api_status():
                    st.text_input("CoinMarketCap API_Key", value=vps.coinmarketcap_api_key, type="password", key="vps_coindata_api_key", disabled=False, help=pbgui_help.coindata_api_key)
                else:
                    st.text_input(":red[CoinMarketCap API_Key (Invalid)]", value=vps.coinmarketcap_api_key, type="password", key="vps_coindata_api_key", disabled=False, help=pbgui_help.coindata_api_key)
            else:
                st.write(":red[Please configure PBCoinData]")
        col1, col2, col3 = st.columns([1,1,2], vertical_alignment='bottom')
        with col1:
            st.checkbox("Install pb6", value=vps.install_pb6, key="vps_install_pb6", help=pbgui_help.vps_install_pb6)
            st.checkbox("Enable Linux Firewall (ufw)", value=vps.firewall, key="vps_firewall", help=pbgui_help.vps_firewall)
        with col2:
            st.number_input("SSH port", value=vps.firewall_ssh_port, format="%d", key="vps_firewall_ssh_port", help=pbgui_help.vps_firewall_ssh_port)
        with col3:
            st.text_input("IP-Addresses to allow", value=vps.firewall_ssh_ips, key="vps_firewall_ssh_ips", help=pbgui_help.vps_firewall_ssh_ips)
        if st.button("Setup VPS", disabled=not vps.has_setup_parameters()):
            vpsmanager.setup_vps(vps, debug = st.session_state.setup_debug)
            st.session_state.view_setup = vps
            del st.session_state.manage_vps
            st.rerun()
    
    # Branch Management for VPS
    server = pbremote.find_server(vps.hostname)
    if server and hasattr(pbremote.local_run, 'pbgui_branches_data') and pbremote.local_run.pbgui_branches_data:
        with st.expander("🔀 **VPS Branch Management**"):
            current_vps_branch = getattr(server, 'pbgui_branch', 'unknown')
            current_vps_commit = getattr(server, 'pbgui_commit', '')
            current_master_branch = getattr(pbremote.local_run, 'pbgui_branch', 'unknown')
            
            st.info(f"📍 **Current VPS:** {current_vps_branch} @ {current_vps_commit[:7] if current_vps_commit else 'unknown'}")
            
            available_branches = list(pbremote.local_run.pbgui_branches_data.keys())
            current_index = available_branches.index(current_vps_branch) if current_vps_branch in available_branches else 0
            
            col1, col2 = st.columns(2)
            with col1:
                selected_branch = st.selectbox(
                    "Target Branch",
                    options=available_branches,
                    index=current_index,
                    key="vps_pbgui_branch_selector"
                )
                
                # Reload and Load More buttons below branch selector
                col_btn1, col_btn2, col_btn3 = st.columns(3)
                with col_btn1:
                    if st.button("🔄 Reload", key="reload_vps_branches", use_container_width=True):
                        with st.spinner("Reloading..."):
                            if hasattr(pbremote.local_run, 'load_git_branches_history'):
                                pbremote.local_run.load_git_branches_history()
                            # Reset commit counters for all branches
                            if 'vps_commits_loaded' in st.session_state:
                                del st.session_state.vps_commits_loaded
                        st.rerun()
                with col_btn2:
                    if st.button("🔽 +50", key="load_more_top_vps", use_container_width=True):
                        if 'vps_commits_loaded' not in st.session_state:
                            st.session_state.vps_commits_loaded = {}
                        current_count = st.session_state.vps_commits_loaded.get(selected_branch, 50)
                        new_count = current_count + 50
                        st.session_state.vps_commits_loaded[selected_branch] = new_count
                        pbremote.local_run.load_more_commits(selected_branch, new_count)
                        st.rerun()
                with col_btn3:
                    if st.button("🔽 All", key="load_all_top_vps", use_container_width=True):
                        if 'vps_commits_loaded' not in st.session_state:
                            st.session_state.vps_commits_loaded = {}
                        st.session_state.vps_commits_loaded[selected_branch] = 999999
                        pbremote.local_run.load_more_commits(selected_branch, 999999)
                        st.rerun()
            
            with col2:
                if selected_branch in pbremote.local_run.pbgui_branches_data:
                    commits = pbremote.local_run.pbgui_branches_data[selected_branch]
                    
                    commit_labels = []
                    for commit in commits:
                        is_current = (commit['full'] == getattr(server, 'pbgui_commit', ''))
                        prefix = "🔹 CURRENT: " if is_current else ""
                        # Use first line only for selectbox display, shorten to 50 chars
                        # Replace newlines with space to prevent selectbox breaking
                        first_line = commit['message'].split('\n')[0].replace('\n', ' ').replace('\r', ' ')
                        short_msg = first_line[:50] + "..." if len(first_line) > 50 else first_line
                        label = f"{prefix}{commit['short']} | {short_msg} | {commit['date']} | {commit['author']}"
                        commit_labels.append(label)
                    
                    # Find current commit index for default selection
                    if selected_branch == current_vps_branch:
                        try:
                            current_commit_index = next(i for i, c in enumerate(commits) if c['full'] == current_vps_commit)
                            # Add 1 to account for HEAD option
                            current_commit_index = current_commit_index + 1
                        except StopIteration:
                            current_commit_index = 0
                    else:
                        # Different branch selected - default to HEAD
                        current_commit_index = 0
                    
                    selected_commit_label = st.selectbox(
                        f"Target Commit ({len(commits)} loaded - optional, leave at HEAD for latest)",
                        options=["HEAD (latest)"] + commit_labels,
                        index=current_commit_index,
                        key="vps_pbgui_commit_selector"
                    )
                    
                    if selected_commit_label != "HEAD (latest)":
                        commit_index = commit_labels.index(selected_commit_label)
                        selected_commit_data = commits[commit_index]
                        selected_commit = selected_commit_data['full']
                    else:
                        selected_commit = ""
                        # Show current commit details when HEAD is selected
                        if current_vps_commit and selected_branch == current_vps_branch:
                            try:
                                selected_commit_data = next(c for c in commits if c['full'] == current_vps_commit)
                            except StopIteration:
                                selected_commit_data = None
                        else:
                            selected_commit_data = None
                    
                    # Show commit details with message in tooltip
                    if selected_commit_data:
                        st.markdown(f"**Commit:** `{selected_commit_data['short']}` | **Author:** {selected_commit_data['author']} | **Date:** {selected_commit_data['date']}")
                        st.markdown(f"**Full Hash:** `{selected_commit_data['full']}`")
                        # Show first line of message, full message in tooltip
                        short_message = selected_commit_data['message'].split('\n')[0]
                        st.markdown(f"**Message:** {short_message}", help=selected_commit_data['message'])
                    
                    # Check if already on target
                    is_on_target = (current_vps_branch == selected_branch and 
                                  current_vps_branch != 'unknown' and
                                  (selected_commit == "" or selected_commit == current_vps_commit))
                    
                    # Status text and button in one row
                    status_col, btn_col = st.columns([3, 1])
                    
                    with status_col:
                        if is_on_target:
                            st.success(f"✅ Already on branch `{selected_branch}` at the target commit")
                        else:
                            if current_vps_branch != selected_branch:
                                st.warning(f"⚠️ This will switch from `{current_vps_branch}` to `{selected_branch}`")
                            elif selected_commit:
                                st.warning(f"⚠️ This will switch to commit `{selected_commit[:7]}`")
                    
                    with btn_col:
                        if st.button("🔀 Switch Branch", disabled=is_on_target, type="primary"):
                            extra_vars = {'branch': selected_branch}
                            if selected_commit:
                                extra_vars['commit'] = selected_commit
                            
                            # Trigger vps-switch-pbgui-branch.yml
                            vps.command = "vps-switch-pbgui-branch"
                            vps.command_text = f"Switch to branch {selected_branch}"
                            vpsmanager.update_vps(vps, debug=st.session_state.setup_debug, extra_vars=extra_vars)
                            st.session_state.view_update = vps
                            del st.session_state.manage_vps
                            st.rerun()
    
    if server:
        d = []
        boot = datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S")
        if server.is_online():
            online = "✅"
        else:
            online = "❌"
        
        # Branch-aware comparison for PBGui (same logic as overview)
        server_branch = getattr(server, "pbgui_branch", "unknown")
        
        if server_branch != "unknown" and hasattr(pbremote.local_run, 'pbgui_branches_data'):
            if server_branch in pbremote.local_run.pbgui_branches_data:
                branch_commits = pbremote.local_run.pbgui_branches_data[server_branch]
                if branch_commits:
                    origin_commit_for_branch = branch_commits[0]['full']
                    if server.pbgui_commit == origin_commit_for_branch:
                        pbgui = "✅"
                    else:
                        pbgui = f"❌ {server.pbgui_version} (..{origin_commit_for_branch[-5:]})"
                else:
                    pbgui = f"⚠️ {server.pbgui_version}"
            else:
                pbgui = f"⚠️ {server.pbgui_version}"
        elif server_branch == "main":
            if server.pbgui_version == pbremote.local_run.pbgui_version_origin and server.pbgui_commit == pbremote.local_run.pbgui_commit_origin:
                pbgui = "✅"
            else:
                pbgui = f"❌ {pbremote.local_run.pbgui_version_origin} (..{pbremote.local_run.pbgui_commit_origin[-5:]})"
        else:
            pbgui = f"⚠️ {server.pbgui_version}"
        
        if server.pb6_version == pbremote.local_run.pb6_version_origin and server.pb6_commit == pbremote.local_run.pb6_commit_origin:
            pb6 = "✅"
        else:
            pb6 = f"❌ {pbremote.local_run.pb6_version_origin} (..{pbremote.local_run.pb6_commit_origin[-5:]})"
        if server.pb7_version == pbremote.local_run.pb7_version_origin and server.pb7_commit == pbremote.local_run.pb7_commit_origin:
            pb7 = "✅"
        else:
            pb7 = f"❌ {pbremote.local_run.pb7_version_origin} (..{pbremote.local_run.pb7_commit_origin[-5:]})"
        if server.reboot:
            reboot = "❌"
        else:
            reboot = "✅"
        d.append({
            "Name": server.name,
            "Online": online,
            "Start": boot,
            "Reboot": reboot,
            "Updates": server.upgrades,
            "CMC Credits": server.cmc_credits,
            "PBGui": f'{server.pbgui_version}',
            "PBGui Branch": f'{getattr(server, "pbgui_branch", "unknown")}',
            "PBGui github": pbgui,
            "PB6": f'{server.pb6_version}',
            "PB6 github": pb6,
            "PB7": f'{server.pb7_version}',
            "PB7 github": pb7
        })
        st.data_editor(data=d, height=36+(len(d))*35, key=f"vps_overview_{st.session_state.ed_key}")
        monitor.server = server
        monitor.servers = []
        monitor.servers.append(monitor.server)
        monitor.view_server()
        monitor.view_server_instances()
        logs = ["logs/PBCoinData.log", "logs/PBRun.log", "logs/PBRemote.log", "logs/sync.log"] + monitor.logfiles
        view_log(vps, logs)

@st.fragment
def view_log(vps : VPS, logs : list):
    vpsmanager = st.session_state.vpsmanager
    # Init keys from session_state
    if "select_log_vps" in st.session_state:
        if st.session_state.select_log_vps != vps.logfilename:
            vps.logfilename = st.session_state.select_log_vps
    if 'size_log_vps' in st.session_state:
        if st.session_state.size_log_vps != vps.logsize:
            vps.logsize = st.session_state.size_log_vps
            vps.load_log()
    col1, col2, col3, col4 = st.columns([4,1,1,4], vertical_alignment="bottom")
    with col1:
        st.selectbox("Logfile", logs, key=f'select_log_vps')
    with col2:
        st.checkbox("Reverse", value=True, key=f'select_reverse_log_vps')
    with col3:
        st.selectbox("view last kB", [50, 100, 250, 500, 1000, 2000, 5000, 10000, 100000], key=f'size_log_vps')
    with col4:
        if st.button(":material/refresh:", key=f'fetch_log_vps'):
            vps.command = "vps-fetch-logfile"
            vps.command_text = f"Fetch logfile {vps.logfilename}"
            with st.spinner(f"Fetching logfile {vps.logfilename}"):
                vpsmanager.fetch_log(vps, debug = st.session_state.setup_debug)
                st.rerun(scope="fragment")
    logfile = vps.logfile
    if logfile:
        if st.session_state[f'select_reverse_log_vps']:
            logfile = '\n'.join(logfile.split('\n')[::-1])
        with st.container(height=1200):
            if vps.logsize <= 250:
                st.code(logfile)
            else:
                st.text(logfile)

def init_vps():
    # Init vpsmanager
    vpsmanager = st.session_state.vpsmanager
    # Init new VPS
    vps = st.session_state.init_vps
    # Init from session_state keys
    if "vps_ip" in st.session_state:
        if st.session_state.vps_ip != vps.ip:
            # Check if self.ip is a valid IPv4 address
            if re.match(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$", st.session_state.vps_ip):
                vps.ip = st.session_state.vps_ip
            else:
                st.session_state.vps_ip = vps.ip
                error_popup("Error: IP address is not valid")
    if "vps_hostname" in st.session_state:
        if st.session_state.vps_hostname != vps.hostname:
            if st.session_state.vps_hostname == st.session_state.pbname:
                st.session_state.vps_hostname = vps.hostname
                error_popup("Error: hostname is equal to master, use another hostname")
            elif st.session_state.vps_hostname in vpsmanager.list():
                st.session_state.vps_hostname = vps.hostname
                error_popup("Error: hostname already exists")
            else:
                vps.hostname = st.session_state.vps_hostname
    if "vps_init_methode" in st.session_state:
        if st.session_state.vps_init_methode != vps.init_methode:
            vps.init_methode = st.session_state.vps_init_methode
    if "vps_remove_user" in st.session_state:
        if st.session_state.vps_remove_user != vps.remove_user:
            vps.remove_user = st.session_state.vps_remove_user
    if "vps_initial_root_pw" in st.session_state:
        if st.session_state.vps_initial_root_pw != vps.initial_root_pw:
            vps.initial_root_pw = st.session_state.vps_initial_root_pw
    if "vps_root_pw" in st.session_state:
        if st.session_state.vps_root_pw != vps.root_pw:
            #error when root_pw has {{ or }} in it
            if "{{" in st.session_state.vps_root_pw or "}}" in st.session_state.vps_root_pw:
                st.session_state.vps_root_pw = vps.root_pw
                error_popup("Error: root_pw contains '{{' or '}}'")
            else:
                vps.root_pw = st.session_state.vps_root_pw
    if "vps_user_sudo" in st.session_state:
        if st.session_state.vps_user_sudo != vps.user_sudo:
            vps.user_sudo = st.session_state.vps_user_sudo
    if "vps_user_sudo_pw" in st.session_state:
        if st.session_state.vps_user_sudo_pw != vps.user_sudo_pw:
            if st.session_state.vps_user_sudo_pw != "":
                #error when user_sudo_pw has {{ or }} in it
                if "{{" in st.session_state.vps_user_sudo_pw or "}}" in st.session_state.vps_user_sudo_pw:
                    st.session_state.vps_user_sudo_pw = vps.user_sudo_pw
                    error_popup("Error: user_sudo_pw contains '{{' or '}}'")
                else:
                    vps.user_sudo_pw = st.session_state.vps_user_sudo_pw
    if "vps_private_key_user" in st.session_state:
        if st.session_state.vps_private_key_user != vps.private_key_user:
            vps.private_key_user = st.session_state.vps_private_key_user
    if "vps_private_key_file" in st.session_state:
        if st.session_state.vps_private_key_file != vps.private_key_file:
            vps.private_key_file = st.session_state.vps_private_key_file
    if "vps_user" in st.session_state:
        if st.session_state.vps_user != vps.user:
            vps.user = st.session_state.vps_user
    if "vps_user_pw" in st.session_state:
        if st.session_state.vps_user_pw != vps.user_pw:
            if st.session_state.vps_user_pw != "":
                #error when user_pw has {{ or }} in it
                if "{{" in st.session_state.vps_user_pw or "}}" in st.session_state.vps_user_pw:
                    st.session_state.vps_user_pw = vps.user_pw
                    error_popup("Error: user_pw contains '{{' or '}}'")
                else:
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
        "- VPS Linux XS, 1 vCore, 1 GB RAM, 10 GB SSD, 1 €/Monat (max 6 running v7 passivbots)\n"
        "- VPS Linux S, 2 vCores, 2 GB RAM, 80 GB SSD, 3 €/Monat\n"
        "- VPS Linux M, 2 vCores, 4 GB RAM, 120 GB SSD, 6 €/Monat\n"
        "- Please use my [referral link](https://aklam.io/esMFvG)\n"
        "- RackNerd has also nice small VPS for 11$ year\n"
        "- Please use my [referral link](https://my.racknerd.com/aff.php?aff=15714)\n"
        "- A good alternative is a VPS from Contabo\n"
        "- VPS 1, 4 vCores, 6 GB RAM, 100 GB SSD, 4,50 €/Monat\n"
        "- Please use my [referral link](https://www.tkqlhce.com/click-101296145-12454592)\n"
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
    col1, col2, col3, col4 = st.columns([1,1,1,1], vertical_alignment='bottom')
    with col1:
        st.selectbox("Init methode", ["root", "password", "private_key"], index=0, key ="vps_init_methode", help=pbgui_help.vps_init_methode)
    with col2:
        if st.session_state.vps_init_methode in ["password", "private_key"]:
            st.checkbox("Remove user from vps after init", value=vps.remove_user, key="vps_remove_user", help=pbgui_help.vps_remove_user)
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.text_input("VPS IPv4", value=vps.ip, key="vps_ip", help=pbgui_help.vps_ip)
    with col2:
        st.text_input("VPS hostname", value=vps.hostname, key="vps_hostname", help=pbgui_help.vps_hostname)
    with col3:
        if st.session_state.vps_init_methode == "private_key":
            st.text_input("VPS user that have private_key", value=vps.private_key_user, key="vps_private_key_user", help=pbgui_help.vps_private_key_user)
        elif st.session_state.vps_init_methode == "password":
            st.text_input("VPS user with sudo rights", value=vps.user_sudo, key="vps_user_sudo", help=pbgui_help.vps_user_sudo)
        else:
            st.text_input("VPS root password", value=vps.initial_root_pw, type="password", key="vps_initial_root_pw", help=pbgui_help.vps_initial_root_pw)
    with col4:
        if st.session_state.vps_init_methode == "private_key":
            st.text_input("private_key /path/filename.pem", value=vps.private_key_file, key="vps_private_key_file", help=pbgui_help.vps_private_key_file)
        elif st.session_state.vps_init_methode == "password":
            st.text_input("VPS sudo user password", value=vps.user_sudo_pw, type="password", key="vps_user_sudo_pw", help=pbgui_help.vps_user_sudo_pw)
        else:
            st.text_input("VPS new root password", value=vps.root_pw, type="password", key="vps_root_pw", help=pbgui_help.vps_root_pw)
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        st.text_input("VPS user name", value=vps.user, key="vps_user", help=pbgui_help.vps_user)
    with col2:
        st.text_input("VPS user password", value=vps.user_pw, type="password", key="vps_user_pw", help=pbgui_help.vps_user_pw)
    with col4:
        if st.session_state.vps_init_methode == "private_key":
            if st.button("Browse", key="button_browse_private_key"):
                select_file("vps_private_key_file")
    st.checkbox("Debug", key="init_debug")
    if st.button("Init VPS", disabled=not vps.has_init_parameters()):
         vpsmanager.init_vps(vps, debug = st.session_state.init_debug)
         st.session_state.view_init = vps
         del st.session_state.init_vps
         st.rerun()

def view_update_master():
    vpsmanager = st.session_state.vpsmanager
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.view_update_master
            st.rerun()
    st.header(vpsmanager.command_text + " " + st.session_state.pbname)
    st.write(
        "- Please wait until the update is finished.\n"
        "- This can take some minutes.\n"
        "- After update is successful you can go back to Overview.\n")
    vpsmanager.view_update_status()
    vpsmanager.view_update_log()

def view_update():
    vps = st.session_state.view_update
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.view_update
            st.rerun()
        if st.button("Manage VPS"):
            st.session_state.manage_vps = vps
            del st.session_state.view_update
            st.rerun()
    st.header(vps.command_text + " " + vps.hostname)
    st.write(
        "- Please wait until the update is finished.\n"
        "- This can take some minutes.\n"
        "- After update is successful you can go back to Manage VPS.\n")
    vps.view_update_status()
    vps.view_update_log()

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
            if "vpsmanager" in st.session_state:
                del st.session_state.vpsmanager
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

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("VPS Manager")
st.header("VPS Manager", divider="red")

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
elif 'manage_master' in st.session_state:
    manage_master()
elif 'view_update' in st.session_state:
    view_update()
elif 'view_update_master' in st.session_state:
    view_update_master()
else:
    list_vps()
