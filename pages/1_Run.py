import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from datetime import datetime
import subprocess
import sys
import shlex
import shutil
import re
import yaml
import os
import pandas as pd
import jinja2

#  backup old config.yaml and save new
def save_yaml():
    with open('config.json.jinja') as f:
        tmpl = jinja2.Template(f.read())
        yaml = tmpl.render(instances = st.session_state.pb_instances, version=st.session_state.pb_manager.config_parser.config['version'], defaults=st.session_state.pb_manager.config_parser.config['defaults'])
        now = datetime.now()
        date = now.strftime("%Y-%m-%d_%H:%M:%S")
        path = f'data/run/manager'
        if not os.path.exists(path):
            os.makedirs(path)
        shutil.copy(f'{st.session_state.pbdir}/manager/config.yaml', f'{path}/{date}_config.yaml')
        with open(f'{st.session_state.pbdir}/manager/config.yaml', 'w', encoding='utf-8') as f:
            f.write(yaml)

# Display all bots
def display_bots():
    col1, col2, col3, col4, col5 = st.columns([1,1,0.5,0.3,3])
    with col1:
        st.write("#### **User**")
    with col2:
        st.write("#### **Symbol**")
    with col3:
        st.write("#### **Status**")
    with col4:
        st.write("#### **Edit**")
    with col5:
        st.write("#### **Configuration file**")

    for instance in st.session_state.pb_instances:
        col1, col2, col3, col4, col5 = st.columns([1,1,0.5,0.3,3])
        with col1:
            st.write(instance.user)
        with col2:
            st.write(instance.symbol)
        with col3:
            if instance.is_running():
                ss_button = ":green[Stop]"
            else:
                ss_button = ":red[Start]"
            st.button(ss_button, key=instance, on_click=start_stop_instance, args=[instance])
        with col4:
            st.button("Edit", key=f'edit {instance}', on_click=button_handler, args=[instance])
        with col5:
            st.write(instance.config)

# Start or Stop bot
def start_stop_instance(instance):
    if instance.is_running():
        instance.stop()
    else:
        os.chdir(st.session_state.pbdir)
        instance.start()

# backup old config and save new
def save_instance_config(instance, config):
    now = datetime.now()
    date = now.strftime("%Y-%m-%d_%H:%M:%S")
    path = f'data/run/{instance.user}'
    if not os.path.exists(path):
        os.makedirs(path)
    shutil.copy(instance.config, f'{path}/{instance.symbol}_{date}.json')
    with open(instance.config, 'w', encoding='utf-8') as f:
        f.write(config)

# handler for button clicks
def button_handler(instance, button=None):
    if button == "back":
        del st.session_state.edit_instance
        del st.session_state.instance_config
        if 'save_cfg' in st.session_state:
            del st.session_state.save_cfg
        if 'save_yaml' in st.session_state:
            del st.session_state.save_yaml
    elif button == "save":
        if 'save_cfg' in st.session_state:
            del st.session_state.save_cfg
            save_instance_config(instance, st.session_state.instance_config)
        if 'save_yaml' in st.session_state:
            del st.session_state.save_yaml
            save_yaml()
    else:
        st.session_state.edit_instance = instance

# handler for text input
def input_handler(input=None):
    if input == "cfg":
        st.session_state.save_cfg = True
    elif input == "symbol":
        st.session_state.save_yaml = True

# edit bot instance
def edit_instance(instance):
    api = pd.read_json(st.session_state.pbdir+'/api-keys.json', typ='frame', orient='index')
    st.selectbox('User',api.index, api.index.get_loc(instance.user))
    instance.symbol = st.text_input('SYMBOL',value=instance.symbol, on_change=input_handler, args=["symbol"])
    if 'instance_config' not in st.session_state:
        with open(instance.config, 'r', encoding='utf-8') as f:
            st.session_state.instance_config = f.read()
        st.session_state.instance_config_high = len(st.session_state.instance_config.splitlines()) * 24
    st.session_state.instance_config = st.text_area(instance.config, st.session_state.instance_config, on_change=input_handler, args=["cfg"], height=st.session_state.instance_config_high)
    col11, col12, col13 = st.columns([1,1,1])
    with col11:
        st.button("Back", key="back", on_click=button_handler, args=[instance, "back"])
    with col12:
        if 'save_cfg' in st.session_state or 'save_yaml' in st.session_state:
            st.button("Save", key="save", on_click=button_handler, args=[instance, "save"])

st.set_page_config(
    page_title="Passivbot GUI - Run",
    page_icon=":screwdriver:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
#        'Get Help': 'https://www.extremelycoolapp.com/help',
        'About': "Passivbot GUI"
    }
)

# Init Session State
if 'pbdir' not in st.session_state:
    switch_page("pbgui")
else:
    sys.path.insert(0,st.session_state.pbdir)
    sys.path.insert(0,f'{st.session_state.pbdir}/manager')
    manager = __import__("manager")
    Manager = getattr(manager,"Manager")

if 'pb_manager' not in st.session_state:
    st.session_state.pb_manager = Manager()
    st.session_state.pb_instances = st.session_state.pb_manager.get_instances()
if 'edit_instance' in st.session_state:
    edit_instance(st.session_state.edit_instance)
else:
    display_bots()
