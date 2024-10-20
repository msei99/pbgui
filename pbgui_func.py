import streamlit as st
import json
import hjson
import pprint
import uuid
import requests
import configparser
import os
from pathlib import Path, PurePath

@st.dialog("Select file")
def change_ini(section, parameter):
    filename = st_file_selector(st, path=st.session_state[parameter], key = f'file_change_{parameter}', label = f'select {parameter}')
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(":green[Yes]"):
            filename = str(Path(filename).absolute())
            st.session_state[parameter] = filename
            save_ini(section, parameter, filename)
            st.rerun()
    with col2:
        if st.button(":red[No]"):
            st.rerun()

def save_ini(section : str, parameter : str, value : str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if not pb_config.has_section(section):
        pb_config.add_section(section)
    pb_config.set(section, parameter, value)
    with open('pbgui.ini', 'w') as pbgui_configfile:
        pb_config.write(pbgui_configfile)

def load_ini(section : str, parameter : str):
    if parameter not in st.session_state:
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option(section, parameter):
            st.session_state[parameter] = pb_config.get(section, parameter)
        else:
            st.session_state[parameter] = ""
    return st.session_state[parameter]

def pbdir(): return load_ini("main", "pbdir")

def pbvenv(): return load_ini("main", "pbvenv")

def is_pb_installed():
    if Path(f"{pbdir()}/passivbot.py").exists():
        return True
    return False

def pb7dir(): return load_ini("main", "pb7dir")

def pb7venv(): return load_ini("main", "pb7venv")

def is_pb7_installed():
    if Path(f"{pb7dir()}/src/passivbot.py").exists():
        return True
    return False

PBGDIR = Path.cwd()

def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

def set_page_config(page : str = "Start"):
    st.session_state.page = page
    st.set_page_config(
        page_title=f"PBGUI - {page}",
        page_icon=":screwdriver:",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'Get help': 'https://github.com/msei99/pbgui/#readme',
            'About': "Passivbot GUI v1.19"
        }
    )

def is_session_state_initialized():
    # Init Services
    if (
        'pbdir' not in st.session_state or
        'services' not in st.session_state or
        'pbgui_instances' not in st.session_state or
        'multi_instances' not in st.session_state or
        'users' not in st.session_state
    ):
        return True
    return False

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except (ValueError,TypeError) as err:
        return False
    return True

def validateHJSON(hjsonData):
    try:
        hjson.loads(hjsonData)
    except (ValueError) as err:
        return False
    return True

def config_pretty_str(config: dict):
    pretty_str = pprint.pformat(config)
    for r in [("'", '"'), ("True", "true"), ("False", "false")]:
        pretty_str = pretty_str.replace(*r)
    return pretty_str

def upload_pbconfigdb(config: str, symbol: str, source_name : str):
    if validateJSON(config):
        uniq = str(uuid.uuid4().hex)
        url = 'https://pbconfigdb.scud.dedyn.io/uploads/b1ea37f7cfa0ebf9b67c2f6b30b95b8b1a92e249/'
        headers = {
            'Content-Type': 'application/json',
            'data': json.dumps(json.loads(config)),
            'filename': f'{symbol}-{source_name}-{uniq}.json' # {symbol}-{username}-{unique}.json
        }
        response = requests.put(url, headers=headers)
        st.info(response.text, icon="ℹ️")
    else:
        st.error("Invalid config", icon="🚨")

def load_symbols_from_ini(exchange: str, market_type: str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if pb_config.has_option("exchanges", f'{exchange}.{market_type}'):
        return eval(pb_config.get("exchanges", f'{exchange}.{market_type}'))
    else:
        return []

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

@st.dialog("Error")
def error_popup(message):
    st.error(f'{message}', icon="⚠️")
    if st.button(":green[OK]"):
        st.rerun()

@st.dialog("Info")
def info_popup(message):
    st.info(f'{message}', icon="✅")
    if st.button(":green[OK]"):
        st.rerun()
