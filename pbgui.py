import streamlit as st
import configparser
import os
import inspect
import platform
from pbgui_func import check_password, set_page_config, st_file_selector, load_pb7venv, save_pb7venv
from Services import Services
from Instance import Instances
from Multi import MultiInstances
from User import Users

@st.dialog("Select file")
def change_pb7venv():
    # st.info(f"File: {filename}", icon="ℹ️")
    # reason = st.text_input("Because...")
    filename = st_file_selector(st, path=st.session_state.pb7venv, key = 'file_change_pb7venv', label = 'select venv')
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(":green[Yes]"):
            st.session_state.pb7venv = filename
            save_pb7venv()
            st.rerun()
    with col2:
        if st.button(":red[No]"):
            st.rerun()


set_page_config()

# Password Check
if not check_password():
    st.stop()

st.header("Passivbot GUI")
st.session_state.pbgdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(st.session_state.pbgdir)

# Load ini and initialize session state
pb_config = configparser.ConfigParser()
pb_config.read('pbgui.ini')

# Load pb dir and venv
if pb_config.has_option("main", "pbdir"):
    st.session_state.pbdir = pb_config.get("main", "pbdir")
else:
    st.session_state.pbdir = '.'
if pb_config.has_option("main", "pbvenv"):
    st.session_state.pbvenv = pb_config.get("main", "pbvenv")
else:
    st.session_state.pbvenv = '.'

# Load pb7 dir and venv
if pb_config.has_option("main", "pb7dir"):
    st.session_state.pb7dir = pb_config.get("main", "pb7dir")
else:
    st.session_state.pb7dir = '.'
# if pb_config.has_option("main", "pb7venv"):
#     st.session_state.pb7venv = pb_config.get("main", "pb7venv")
# else:
#     st.session_state.pb7venv = '.'
load_pb7venv()

# Load bot name
if pb_config.has_option("main", "pbname"):
    st.session_state.pbname = pb_config.get("main", "pbname")
else:
    st.session_state.pbname = platform.node()

st.session_state.pbdir = os.path.abspath(st_file_selector(st, path=st.session_state.pbdir, key = 'pbdir_selected', label = 'Choose passivbot v6 directory'))
st.session_state.pbvenv = st_file_selector(st, path=st.session_state.pbvenv, key = 'pbvenv_selected', label = 'Choose passivbot v6 venv python interpreter')
st.session_state.pb7dir = os.path.abspath(st_file_selector(st, path=st.session_state.pb7dir, key = 'pb7dir_selected', label = 'Choose passivbot v7 directory'))
# st.session_state.pb7venv = st_file_selector(st, path=st.session_state.pb7venv, key = 'pb7venv_selected', label = 'Choose passivbot v7 venv python interpreter')
# if "input_pb7venv" in st.session_state:
#     if st.session_state.input_pb7venv != st.session_state.pb7venv:
#         st.session_state.pb7venv = st.session_state.input_pb7venv
#         save_pb7venv()
st.text_input("passivbot v7 python interpreter (venv)", value=st.session_state.pb7venv, key='input_pb7venv')
if st.button("Change", key='button_change_pb7venv'):
    change_pb7venv()
st.session_state.pbname = st.text_input("Bot Name", value=st.session_state.pbname, max_chars=32)

if not pb_config.has_section("main"):
    pb_config.add_section("main")
pb_config.set("main", "pbdir", os.path.abspath(st.session_state.pbdir))
pb_config.set("main", "pbvenv", st.session_state.pbvenv)
pb_config.set("main", "pb7dir", os.path.abspath(st.session_state.pb7dir))
pb_config.set("main", "pb7venv", st.session_state.pb7venv)
pb_config.set("main", "pbname", st.session_state.pbname)
with open('pbgui.ini', 'w') as pbgui_configfile:
    pb_config.write(pbgui_configfile)

# Init Services
if 'services' not in st.session_state:
    with st.spinner('Initializing Services...'):
        st.session_state.services = Services()
# Init Instances
if 'pbgui_instances' not in st.session_state:
    with st.spinner('Initializing Instances...'):
        st.session_state.pbgui_instances = Instances()
# Init Multi Instances
if 'multi_instances' not in st.session_state:
    with st.spinner('Initializing Multi Instances...'):
        st.session_state.multi_instances = MultiInstances()
# Init Users
if 'users' not in st.session_state:
    with st.spinner('Initializing Users...'):
        st.session_state.users = Users()
