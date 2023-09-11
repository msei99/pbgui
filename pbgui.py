import streamlit as st
import configparser
import os
import inspect
from pbgui_func import check_password

def update_dir(key):
    choice = st.session_state[key]
    if os.path.isdir(os.path.join(st.session_state[key+'curr_dir'], choice)):
        st.session_state[key+'curr_dir'] = os.path.normpath(os.path.join(st.session_state[key+'curr_dir'], choice))
        files = sorted(os.listdir(st.session_state[key+'curr_dir']))
        files.insert(0, '..')
        files.insert(0, '.')
        st.session_state[key+'files'] = files

def st_file_selector(st_placeholder, path='.', label='Select a file/folder', key = 'selected'):
    if key+'curr_dir' not in st.session_state:
        base_path = '.' if path is None or path == '' else path
        base_path = base_path if os.path.isdir(base_path) else os.path.dirname(base_path)
        base_path = '.' if base_path is None or base_path == '' else base_path

        files = sorted(os.listdir(base_path))
        files.insert(0, '..')
        files.insert(0, '.')
        st.session_state[key+'files'] = files
        st.session_state[key+'curr_dir'] = base_path
    else:
        base_path = st.session_state[key+'curr_dir']
    selected_file = st_placeholder.selectbox(label=label, 
                                        options=st.session_state[key+'files'], 
                                        key=key, 
                                        on_change = lambda: update_dir(key))
    selected_path = os.path.normpath(os.path.join(base_path, selected_file))
    st_placeholder.write(os.path.abspath(selected_path))
    return selected_path

st.set_page_config(
    page_title="Passivbot GUI - Start",
    page_icon=":screwdriver:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
#        'Get Help': 'https://www.extremelycoolapp.com/help',
        'About': "Passivbot GUI"
    }
)

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

st.session_state.pbdir = os.path.abspath(st_file_selector(st, path=st.session_state.pbdir, key = 'pbdir_selected', label = 'Choose passivbot directory'))
if not pb_config.has_section("main"):
    pb_config.add_section("main")
pb_config.set("main", "pbdir", os.path.abspath(st.session_state.pbdir))
with open('pbgui.ini', 'w') as pbgui_configfile:
    pb_config.write(pbgui_configfile)
