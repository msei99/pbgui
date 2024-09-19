import streamlit as st
import configparser
import os
import inspect
import platform
from pbgui_func import check_password, set_page_config, st_file_selector, load_pb7venv, change_ini, load_ini, save_ini
from Services import Services
from Instance import Instances
from Multi import MultiInstances
from User import Users

# @st.dialog("Select file")
# def change_pb7venv(name):
#     # st.info(f"File: {filename}", icon="ℹ️")
#     # reason = st.text_input("Because...")
#     filename = st_file_selector(st, path=st.session_state[name], key = f'file_change_{name}', label = f'select {name}')
#     col1, col2 = st.columns([1,1])
#     with col1:
#         if st.button(":green[Yes]"):
#             st.session_state[name] = filename
#             save_pb7venv()
#             st.rerun()
#     with col2:
#         if st.button(":red[No]"):
#             st.rerun()


set_page_config()

# Password Check
if not check_password():
    st.stop()

st.header("Passivbot GUI")
st.session_state.pbgdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
os.chdir(st.session_state.pbgdir)

# Load ini and initialize session state
# pb_config = configparser.ConfigParser()
# pb_config.read('pbgui.ini')

# # Load bot name
# if pb_config.has_option("main", "pbname"):
#     st.session_state.pbname = pb_config.get("main", "pbname")
# else:
#     st.session_state.pbname = platform.node()

load_ini("main", "pbdir")
load_ini("main", "pbvenv")
load_ini("main", "pb7dir")
load_ini("main", "pb7venv")
if load_ini("main", "pbname") == "":
    st.session_state.pbname = platform.node()

col1, col2 = st.columns([5,1], vertical_alignment="bottom")
with col1:
    st.text_input("Passivbot V6 path", value=st.session_state.pbdir, key='input_pbdir')
with col2:
    if st.button("Browse", key='button_change_pbdir'):
        change_ini("main", "pbdir")
col1, col2 = st.columns([5,1], vertical_alignment="bottom")
with col1:
    st.text_input("Passivbot V6 python interpreter (venv)", value=st.session_state.pbvenv, key='input_pbvenv')
with col2:
    if st.button("Browse", key='button_change_pbvenv'):
        change_ini("main", "pbvenv")
col1, col2 = st.columns([5,1], vertical_alignment="bottom")
with col1:
    st.text_input("Passivbot V7 path", value=st.session_state.pb7dir, key='input_pb7dir')
with col2:
    if st.button("Browse", key='button_change_pb7dir'):
        change_ini("main", "pb7dir")
col1, col2 = st.columns([5,1], vertical_alignment="bottom")
with col1:
    st.text_input("Passivbot V7 python interpreter (venv)", value=st.session_state.pb7venv, key='input_pb7venv')
with col2:
    if st.button("Browse", key='button_change_pb7venv'):
        change_ini("main", "pb7venv")

if "input_pbname" in st.session_state:
    if st.session_state.input_pbname != st.session_state.pbname:
        st.session_state.pbname = st.session_state.input_pbname
        save_ini("main", "pbname")
st.text_input("Bot Name", value=st.session_state.pbname, key="input_pbname", max_chars=32)

# if not pb_config.has_section("main"):
#     pb_config.add_section("main")
# pb_config.set("main", "pbdir", os.path.abspath(st.session_state.pbdir))
# pb_config.set("main", "pbvenv", st.session_state.pbvenv)
# pb_config.set("main", "pb7dir", os.path.abspath(st.session_state.pb7dir))
# # pb_config.set("main", "pb7venv", st.session_state.pb7venv)
# pb_config.set("main", "pbname", st.session_state.pbname)
# with open('pbgui.ini', 'w') as pbgui_configfile:
#     pb_config.write(pbgui_configfile)

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
