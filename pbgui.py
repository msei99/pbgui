import streamlit as st
import configparser
import os
import inspect
import platform
from pbgui_func import check_password, set_page_config, st_file_selector
from Services import Services
from Instance import Instances
from Multi import MultiInstances
from User import Users

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

if pb_config.has_option("main", "pbdir"):
    st.session_state.pbdir = pb_config.get("main", "pbdir")
else:
    st.session_state.pbdir = '.'
if pb_config.has_option("main", "pbname"):
    st.session_state.pbname = pb_config.get("main", "pbname")
else:
    st.session_state.pbname = platform.node()

st.session_state.pbdir = os.path.abspath(st_file_selector(st, path=st.session_state.pbdir, key = 'pbdir_selected', label = 'Choose passivbot directory'))
st.session_state.pbname = st.text_input("Bot Name", value=st.session_state.pbname, max_chars=32)
if not pb_config.has_section("main"):
    pb_config.add_section("main")
pb_config.set("main", "pbdir", os.path.abspath(st.session_state.pbdir))
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
