import streamlit as st
import platform
from pbgui_func import check_password, set_page_config, change_ini, load_ini, save_ini
from Services import Services
from Instance import Instances
from RunV7 import V7Instances
from Multi import MultiInstances
from User import Users
from pathlib import Path

set_page_config()

# Password Check
if not check_password():
    st.stop()

st.header("Passivbot GUI")
# st.session_state.pbgdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
# os.chdir(st.session_state.pbgdir)

# Load pbgui.ini settings
load_ini("main", "pbdir")
load_ini("main", "pbvenv")
load_ini("main", "pb7dir")
load_ini("main", "pb7venv")
if not load_ini("main", "pbname"):
    st.session_state.pbname = platform.node()
    save_ini("main", "pbname")

col1, col2 = st.columns([5,1], vertical_alignment="bottom")
with col1:
    if "input_pbdir" in st.session_state:
        if st.session_state.input_pbdir != st.session_state.pbdir:
            st.session_state.pbdir = st.session_state.input_pbdir
            save_ini("main", "pbdir")
    st.text_input("Passivbot V6 path", value=st.session_state.pbdir, key='input_pbdir')
with col2:
    if st.button("Browse", key='button_change_pbdir'):
        del st.session_state.input_pbdir
        change_ini("main", "pbdir")
col1, col2 = st.columns([5,1], vertical_alignment="bottom")
with col1:
    if "input_pbvenv" in st.session_state:
        if st.session_state.input_pbvenv != st.session_state.pbvenv:
            st.session_state.pbvenv = st.session_state.input_pbvenv
            save_ini("main", "pbvenv")
    st.text_input("Passivbot V6 python interpreter (venv)", value=st.session_state.pbvenv, key='input_pbvenv')
with col2:
    if st.button("Browse", key='button_change_pbvenv'):
        del st.session_state.input_pbvenv
        change_ini("main", "pbvenv")
col1, col2 = st.columns([5,1], vertical_alignment="bottom")
with col1:
    if "input_pb7dir" in st.session_state:
        if st.session_state.input_pb7dir != st.session_state.pb7dir:
            st.session_state.pb7dir = st.session_state.input_pb7dir
            save_ini("main", "pb7dir")
    st.text_input("Passivbot V7 path", value=st.session_state.pb7dir, key='input_pb7dir')
with col2:
    if st.button("Browse", key='button_change_pb7dir'):
        del st.session_state.input_pb7dir
        change_ini("main", "pb7dir")
col1, col2 = st.columns([5,1], vertical_alignment="bottom")
with col1:
    if "input_pb7venv" in st.session_state:
        if st.session_state.input_pb7venv != st.session_state.pb7venv:
            st.session_state.pb7venv = st.session_state.input_pb7venv
            save_ini("main", "pb7venv")
    st.text_input("Passivbot V7 python interpreter (venv)", value=st.session_state.pb7venv, key='input_pb7venv')
with col2:
    if st.button("Browse", key='button_change_pb7venv'):
        del st.session_state.input_pb7venv
        change_ini("main", "pb7venv")

if "input_pbname" in st.session_state:
    if st.session_state.input_pbname != st.session_state.pbname:
        st.session_state.pbname = st.session_state.input_pbname
        save_ini("main", "pbname")
st.text_input("Bot Name", value=st.session_state.pbname, key="input_pbname", max_chars=32)

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
# Init V7 Instances
if 'v7_instances' not in st.session_state:
    with st.spinner('Initializing v7 Instances...'):
        st.session_state.v7_instances = V7Instances()
# Init Users
if 'users' not in st.session_state:
    with st.spinner('Initializing Users...'):
        st.session_state.users = Users()
