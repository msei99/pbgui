import streamlit as st
from streamlit_extras.switch_page_button import switch_page
import subprocess
import sys
import shlex
import re
import yaml
from typing import Dict
from os import path
import logging

def save_config(file):
    with open('/tmp/config.settings.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(file,f)

def ss_inst(instance):
    print(instance.get_symbol())
    if instance.is_running():
        instance.stop()
    else:
        instance.start()

def edit_inst(instance):
    print(instance.get_config())

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

st.header("Run")

pb_manager = Manager()
#pb_manager.get_instances()
#pb_inst = pb_manager.get_instances()
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

for inst in pb_manager.get_instances():
    col1, col2, col3, col4, col5 = st.columns([1,1,0.5,0.3,3])
    with col1:
        st.write(inst.get_user())
    with col2:
        st.write(inst.get_symbol())
    with col3:
        if inst.is_running():
            ss_button = ":green[Stop]"
        else:
            ss_button = ":red[Start]"
        st.button(ss_button, key=inst, on_click=ss_inst, args=[inst])
    with col4:
        st.button("Edit", key=f'edit {inst}', on_click=edit_inst, args=[inst])
    with col5:
        st.write(f'{inst.get_config()}')

#save_config(pb_manager.config_parser.config)

#print(pb_manager.instances)
#print(pb_manager.config_parser.config)

#st.table(pb_manager.config_parser.config)

#pm_list = run_list()
#for line in pm_list.stdout.splitlines():
#    line = re.sub('\x1b[^m]*m', '', line) 
#    print(line)
#    if line and line[0] == "-":
#        line_list = line.split(" ")
#        print(line)
#        print(line_list[1])
#        print(line_list[2].split("/")[1])
