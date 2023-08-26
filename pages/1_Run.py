import streamlit as st
from streamlit_extras.switch_page_button import switch_page
import subprocess
import sys
import shlex
import re
import jsonpointer
import yaml
from typing import Dict
from os import path
import logging

def save_config(file):
    with open('/tmp/config.settings.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(file,f)

def ss_inst(instance):
    print(instance.get_symbol())

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
    sys.path.insert(0,{st.session_state.pbdir})
    manager = __import__("manager")
    Manager = getattr(manager,"Manager")

st.header("Run")

pb_manager = Manager()
#pb_manager.get_instances()
#pb_inst = pb_manager.get_instances()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.write("User")
with col2:
    st.write("Symbol")
with col3:
    st.write("Status")

for inst in pb_manager.get_instances():
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.write(inst.get_user())
    with col2:
        st.write(inst.get_symbol())
    with col3:
        st.button(inst.get_status(), key=inst, on_click=ss_inst, args=[inst])

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
