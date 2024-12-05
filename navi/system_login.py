import streamlit as st
import platform
from pbgui_func import check_password, set_page_config, change_ini, is_pb7_installed, is_pb_installed, is_authenticted, get_navi_paths
from pbgui_purefunc import load_ini, save_ini
import pbgui_help
from Services import Services
from Instance import Instances
from RunV7 import V7Instances
from Multi import MultiInstances
from User import Users
import toml
import os
from pathlib import Path, PurePath

def change_password():
    with st.expander("Change Password"):
        with st.form("change_password_form"):
            current_password = st.text_input("Current Password", type="password", placeholder="Enter current password", help=pbgui_help.change_password)
            new_password = st.text_input("New Password", type="password", placeholder="Enter new password", help=pbgui_help.change_password)
            confirm_password = st.text_input("Confirm New Password", type="password", placeholder="Re-enter new password", help=pbgui_help.change_password)
            submit_button = st.form_submit_button("Update Password", help=pbgui_help.change_password)

        if submit_button:
            # Retrieve the current password from secrets, default to empty string if not set
            stored_password = st.secrets.get("password", "")

            # Verify current password
            if current_password != stored_password:
                st.error("Current password is incorrect.")
                return

            # Check if new passwords match
            if new_password != confirm_password:
                st.error("New passwords do not match.")
                return

            # Update the secrets.toml file
            try:
                secrets_path = Path(".streamlit/secrets.toml")
                if not secrets_path.exists():
                    st.error("secrets.toml file does not exist.")
                    # Create empty file
                    with open(secrets_path, "w") as f:
                        f.write("")

                # Load existing secrets
                with open(secrets_path, "r") as f:
                    try:
                        secrets = toml.load(f)
                    except toml.TomlDecodeError:
                        st.error("secrets.toml is not a valid TOML file.")
                        return

                # Update the password
                secrets["password"] = new_password

                # Write back to the file
                with open(secrets_path, "w") as f:
                    toml.dump(secrets, f)

                st.success("Password updated successfully. Please log in again.")

                # Clear session state to force re-authentication
                st.session_state.clear()
                st.rerun()

            except Exception as e:
                st.error(f"An error occurred while updating the password: {e}")

            
def do_init():
    # Missing Password
    if "password_missing" in st.session_state:
        st.warning('You are using PBGUI without a password! Please edit pbgui/.streamlit/secrets.toml to add a password.', icon="⚠️")
    
    # Load pb6 path from pbgui.ini
    if "input_pbdir" in st.session_state:
        if st.session_state.input_pbdir != st.session_state.pbdir:
            st.session_state.pbdir = st.session_state.input_pbdir
            save_ini("main", "pbdir", st.session_state.pbdir)
            if "users" in st.session_state:
                del st.session_state.users
    st.session_state.pbdir = load_ini("main", "pbdir")
    if ".." in st.session_state.pbdir:
        st.session_state.pbdir = os.path.abspath(st.session_state.pbdir)
        save_ini("main", "pbdir", st.session_state.pbdir)
    if Path(f"{st.session_state.pbdir}/passivbot.py").exists():
        pbdir_ok = "✅"
    else:
        pbdir_ok = "❌"

    # Load pb6 venv from pbgui.ini
    if "input_pbvenv" in st.session_state:
        if st.session_state.input_pbvenv != st.session_state.pbvenv:
            st.session_state.pbvenv = st.session_state.input_pbvenv
            save_ini("main", "pbvenv", st.session_state.pbvenv)
    st.session_state.pbvenv = load_ini("main", "pbvenv")
    if ".." in st.session_state.pbvenv:
        st.session_state.pbvenv = os.path.abspath(st.session_state.pbvenv)
        save_ini("main", "pbvenv", st.session_state.pbvenv)
    if Path(st.session_state.pbvenv).is_file() and PurePath(st.session_state.pbvenv).name.startswith("python"):
        pbvenv_ok = "✅"
    else:
        pbvenv_ok = "❌"

    # Load pb7 path from pbgui.ini
    if "input_pb7dir" in st.session_state:
        if st.session_state.input_pb7dir != st.session_state.pb7dir:
            st.session_state.pb7dir = st.session_state.input_pb7dir
            save_ini("main", "pb7dir", st.session_state.pb7dir)
            if "users" in st.session_state:
                del st.session_state.users
    st.session_state.pb7dir = load_ini("main", "pb7dir")
    if ".." in st.session_state.pb7dir:
        st.session_state.pb7dir = os.path.abspath(st.session_state.pb7dir)
        save_ini("main", "pb7dir", st.session_state.pb7dir)
    if Path(f"{st.session_state.pb7dir}/src/passivbot.py").exists():
        pb7dir_ok = "✅"
    else:
        pb7dir_ok = "❌"

    # Load pb7 venv from pbgui.ini
    if "input_pb7venv" in st.session_state:
        if st.session_state.input_pb7venv != st.session_state.pb7venv:
            st.session_state.pb7venv = st.session_state.input_pb7venv
            save_ini("main", "pb7venv", st.session_state.pb7venv)
    st.session_state.pb7venv = load_ini("main", "pb7venv")
    if ".." in st.session_state.pb7venv:
        st.session_state.pb7venv = os.path.abspath(st.session_state.pb7venv)
        save_ini("main", "pb7venv", st.session_state.pb7venv)
    if Path(st.session_state.pb7venv).is_file() and PurePath(st.session_state.pb7venv).name.startswith("python"):
        pb7venv_ok = "✅"
    else:
        pb7venv_ok = "❌"

    # Load pbname from pbgui.ini
    st.session_state.pbname = load_ini("main", "pbname")
    if not st.session_state.pbname:
        st.session_state.pbname = platform.node()
        save_ini("main", "pbname", st.session_state.pbname)

    # Load role from pbgui.ini
    if "role" not in st.session_state:
        st.session_state.role = load_ini("main", "role")
        if st.session_state.role == "master":
            st.session_state.master = True
        else:
            st.session_state.master = False

    col1, col2 = st.columns([5,1], vertical_alignment="bottom")
    with col1:
        st.text_input("Passivbot V6 path " + pbdir_ok, value=st.session_state.pbdir, key='input_pbdir')
    with col2:
        if st.button("Browse", key='button_change_pbdir'):
            del st.session_state.input_pbdir
            change_ini("main", "pbdir")
            if "users" in st.session_state:
                del st.session_state.users

    col1, col2 = st.columns([5,1], vertical_alignment="bottom")
    with col1:
        st.text_input("Passivbot V6 python interpreter (venv/bin/python) " + pbvenv_ok, value=st.session_state.pbvenv, key='input_pbvenv')
    with col2:
        if st.button("Browse", key='button_change_pbvenv'):
            del st.session_state.input_pbvenv
            change_ini("main", "pbvenv")

    col1, col2 = st.columns([5,1], vertical_alignment="bottom")
    with col1:
        st.text_input("Passivbot V7 path " + pb7dir_ok, value=st.session_state.pb7dir, key='input_pb7dir')
    with col2:
        if st.button("Browse", key='button_change_pb7dir'):
            del st.session_state.input_pb7dir
            change_ini("main", "pb7dir")
            if "users" in st.session_state:
                del st.session_state.users

    col1, col2 = st.columns([5,1], vertical_alignment="bottom")
    with col1:
        if "input_pb7venv" in st.session_state:
            if st.session_state.input_pb7venv != st.session_state.pb7venv:
                st.session_state.pb7venv = st.session_state.input_pb7venv
                save_ini("main", "pb7venv", st.session_state.pb7venv)
        st.text_input("Passivbot V7 python interpreter (venv/bin/python) " + pb7venv_ok, value=st.session_state.pb7venv, key='input_pb7venv')
    with col2:
        if st.button("Browse", key='button_change_pb7venv'):
            del st.session_state.input_pb7venv
            change_ini("main", "pb7venv")

    col1, col2 = st.columns([5,1], vertical_alignment="bottom")
    with col1:
        if "input_pbname" in st.session_state:
            if st.session_state.input_pbname != st.session_state.pbname:
                st.session_state.pbname = st.session_state.input_pbname
                save_ini("main", "pbname", st.session_state.pbname)
        st.text_input("Bot Name", value=st.session_state.pbname, key="input_pbname", max_chars=32)
    with col2:
        if "input_master" in st.session_state:
            if st.session_state.input_master != st.session_state.master:
                st.session_state.master = st.session_state.input_master
                if st.session_state.master:
                    save_ini("main", "role", "master")
                    st.session_state.role = "master"
                else:
                    save_ini("main", "role", "slave")
                    st.session_state.role = "slave"
        st.checkbox("Master", value=st.session_state.master, key="input_master", help=pbgui_help.role)

    # Check if any passivbot is installed
    if not any([is_pb7_installed(), is_pb_installed()]):
        st.warning('No Passivbot installed', icon="⚠️")
        st.stop()
    # Check if any pb6 venv is configured
    if is_pb_installed() and not st.session_state.pbvenv:
        st.warning('Passivbot V6 venv is not configured', icon="⚠️")
        st.stop()
    # Check if any pb7 venv is configured
    if is_pb7_installed() and not st.session_state.pb7venv:
        st.warning('Passivbot V7 venv is not configured', icon="⚠️")
        st.stop()
    # Init Users
    if 'users' not in st.session_state:
        with st.spinner('Initializing Users...'):
            st.session_state.users = Users()
    # Init Instances
    if 'pbgui_instances' not in st.session_state:
        with st.spinner('Initializing Instances...'):
            st.session_state.pbgui_instances = Instances()
    # Init Multi Instances
    if 'multi_instances' not in st.session_state:
        with st.spinner('Initializing Multi Instances...'):
            st.session_state.multi_instances = MultiInstances()
    # Init V7 Instances
    if 'v7_instances' not in st.session_state:
        with st.spinner('Initializing v7 Instances...'):
            st.session_state.v7_instances = V7Instances()
    # Init Services
    if 'services' not in st.session_state:
        with st.spinner('Initializing Services...'):
            st.session_state.services = Services()
    # Check if any users are configured
    if not st.session_state.users.list():
        st.warning('No users configured / Go to Setup API-Keys and configure your first user', icon="⚠️")
        st.stop()
    
    # Add a horizontal divider
    st.markdown("---")
    
    # Add the Change Password section
    change_password()

# Page Setup
set_page_config("Welcome")
st.header("Welcome to Passivbot GUI", divider="red")
    
# Show Login-Dialog on demand
check_password()

# Once we're logged in, we can initialize the session and do checks
if is_authenticted():
    do_init()