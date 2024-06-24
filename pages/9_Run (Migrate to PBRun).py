import streamlit as st
from pbgui_func import set_page_config
from Backtest import BacktestItem, BacktestResults
from Instance import Instances, Instance
import streamlit_scrollable_textbox as stx
import pbgui_help
from datetime import datetime
from pathlib import Path, PurePath
import sys
from shutil import copy
import json
import os
import platform
import pandas as pd
import jinja2


#  backup old config.yaml and save new
def save_yaml(instance):
    os.chdir(st.session_state.pbgdir)
    with open('config.json.jinja') as f:
        my_instances = []
        if st.session_state.new_instance:
            for inst in st.session_state.pb_instances:
                my_instances.append(inst)
            my_instances.append(instance)
            st.session_state.new_instance = False
        elif "del_instance" in st.session_state:
            for inst in st.session_state.pb_instances:
                if inst != instance:
                    my_instances.append(inst)
            del st.session_state.del_instance
            if "editpb_instance"in st.session_state:
                del st.session_state.editpb_instance
                del st.session_state.instance_config
            if "error"in st.session_state:
                del st.session_state.error
        else:
            my_instances = st.session_state.pb_instances
        tmpl = jinja2.Template(f.read())
        yaml = tmpl.render(
            instances = my_instances,
            version=st.session_state.pb_manager.config_parser.config['version'],
            defaults=st.session_state.pb_manager.config_parser.config['defaults'])
        now = datetime.now()
        date = now.strftime("%Y-%m-%d_%H-%M-%S")
        path = PurePath(f'data/run/manager')
        if not os.path.exists(path):
            os.makedirs(path)
        copy(PurePath(f'{st.session_state.pbdir}/manager/config.yaml'), PurePath(f'{path}/{date}_config.yaml'))
        with open(PurePath(f'{st.session_state.pbdir}/manager/config.yaml'), 'w', encoding='utf-8') as f:
            f.write(yaml)

# Display all bots
def display_bots():
    if not "pb_instances" in st.session_state:
        return
    col1, col2, col3, col4, col5 = st.columns([1,1,0.5,0.5,3])
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
        col1, col2, col3, col4, col5 = st.columns([1,1,0.5,0.5,3])
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
        instance.reset_state()
    else:
        os.chdir(st.session_state.pbdir)
        instance.start()
        os.chdir(st.session_state.pbgdir)

# backup old config and save new
def save_instance_config(instance, config):
    os.chdir(st.session_state.pbgdir)
    now = datetime.now()
    date = now.strftime("%Y-%m-%d_%H-%M-%S")
    path = f'data/run/{instance.user}'
    if not os.path.exists(path):
        os.makedirs(path)
    if os.path.isfile(instance.config):
        copy(instance.config, f'{path}/{instance.symbol}_{date}.json')
    with open(instance.config, 'w', encoding='utf-8') as f:
        f.write(config)

# Cleanup session_state
def cleanup():
    if "editpb_instance" in st.session_state:
        del st.session_state.editpb_instance
    if "instance_config" in st.session_state:
        del st.session_state.instance_config
    if "config_filename" in st.session_state:
        del st.session_state.config_filename
    if "new_config_filename" in st.session_state:
        del st.session_state.new_config_filename
    if "error" in st.session_state:
        del st.session_state.error
    if "bt_results_run" in st.session_state:
        del st.session_state.bt_results_run

# handler for button clicks
def button_handler(instance, button=None):
    if button == "back":
        cleanup()
    elif button == "restart":
        os.chdir(st.session_state.pbdir)
        instance.restart()
        os.chdir(st.session_state.pbgdir)
    elif button == "del":
        st.session_state.del_instance = True
        save_yaml(instance)
        st.session_state.pb_manager = Manager()
        try:
            st.session_state.pb_instances = st.session_state.pb_manager.get_instances()
        except Exception as e:
            print(f'No Instances: {e}')
    else:
        st.session_state.editpb_instance = instance
        st.session_state.new_instance = False

# edit bot instance
def editpb_instance(instance):
    # load instance_config
    if 'instance_config' not in st.session_state:
        try:
            with open(instance.config, 'r', encoding='utf-8') as f:
                st.session_state.instance_config = f.read()
        except FileNotFoundError:
            st.session_state.instance_config = ""
            st.session_state.instance_config_high = 500
        if st.session_state.instance_config != "":
            st.session_state.instance_config_high = len(st.session_state.instance_config.splitlines()) * 24
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Navigation
    with st.sidebar:
        st.text(f'Instance is: {instance.get_status()}')
        if instance.is_running():
            st.button(":green[Stop]", key="instance", on_click=start_stop_instance, args=[instance])
            st.button(":green[Restart]", key="restart", on_click=button_handler, args=[instance, "restart"])
        else:
            st.button(":red[Start]", key="instance", on_click=start_stop_instance, args=[instance])
            st.button(":wastebasket:", key='del', on_click=button_handler, args=[instance, "del"])
        st.button(":back:", key="back", on_click=button_handler, args=[instance, "back"])
    with st.form("myInstance config"):
        # Init user index
        api = pd.read_json(st.session_state.pbdir+'/api-keys.json', typ='frame', orient='index')
        if instance.user in api.index:
            user_index = api.index.get_loc(instance.user)
        else:
            user_index = 0
        # assigned_balance
        if '-ab' in instance.flags:
            assigned_balance = instance.flags['-ab']
        else:
            assigned_balance = 0
        # market
        if '-m' in instance.flags and instance.flags['-m'] == 'spot':
            market_index = 1
        else:
            market_index = 0
        # price_distance_threshold
        if '-pt' in instance.flags:
            price_distance_threshold = instance.flags['-pt']
        else:
            price_distance_threshold = 0.5
        # leverage
        if '-lev' in instance.flags:
            lev = instance.flags['-lev']
        else:
            lev = 7
        # ohlcv
        if ('-oh' in instance.flags and instance.flags['-oh'] in ["y", "yes", "t", "true", True]) or '-oh' not in instance.flags:
            ohlcv = True
        else:
            ohlcv = False
        # price_precision
        if '-pp' in instance.flags:
            price_precision = instance.flags['-pp']
        else:
            price_precision = 0.0
        # price_step (not supported from manager)
        if '-ps' in instance.flags:
            price_step = instance.flags['-ps']
        else:
            price_step = 0.0
        # long_mode
        if '-lm' in instance.flags:
            long_mode = instance.flags['-lm']
            if long_mode == "n" or long_mode == "normal":
                long_index = 0
            elif long_mode == "m" or long_mode == "manual":
                long_index = 1
            elif long_mode == "gs" or long_mode == "graceful_stop":
                long_index = 2
            elif long_mode == "p" or long_mode == "panic":
                long_index = 3
            elif long_mode == "t" or long_mode == "tp_only":
                long_index = 4
        else:
            try: 
                if float(json.loads(st.session_state.instance_config)["long"]["enabled"]):
                    long_mode = 'n'
                    long_index = 0
                else:
                    long_mode = 'm'
                    long_index = 1
            except:
                long_mode = 'm'
                long_index = 1
        # short_mode
        if '-sm' in instance.flags:
            short_mode = instance.flags['-sm']
            if short_mode == "n" or short_mode == "normal":
                short_index = 0
            elif short_mode == "m" or short_mode == "manual":
                short_index = 1
            elif short_mode == "gs" or short_mode == "graceful_stop":
                short_index = 2
            elif short_mode == "p" or short_mode == "panic":
                short_index = 3
            elif short_mode == "t" or short_mode == "tp_only":
                short_index = 4
        else:
            try: 
                if float(json.loads(st.session_state.instance_config)["short"]["enabled"]):
                    short_mode = 'n'
                    short_index = 0
                else:
                    short_mode = 'm'
                    short_index = 1
            except:
                short_mode = 'm'
                short_index = 1
        # long_exposure
        if '-lw' in instance.flags:
            long_exposure = instance.flags['-lw']
        else:
            try: 
                long_exposure = float(json.loads(st.session_state.instance_config)["long"]["wallet_exposure_limit"])
            except:
                long_exposure = 1.0
        # short_exposure
        if '-sw' in instance.flags:
            short_exposure = instance.flags['-sw']
        else:
            try:
                short_exposure = float(json.loads(st.session_state.instance_config)["short"]["wallet_exposure_limit"])
            except:
                short_exposure = 1.0
        # long_min_markup
        if '-lmm' in instance.flags:
            long_min_markup = instance.flags['-lmm']
        else:
#            try:
#                long_min_markup = float(json.loads(st.session_state.instance_config)["long"]["min_markup"])
#            except:
            long_min_markup = 0.0
        # short_min_markup
        if '-smm' in instance.flags:
            short_min_markup = instance.flags['-smm']
        else:
#            try:
#                short_min_markup = float(json.loads(st.session_state.instance_config)["short"]["min_markup"])
#            except:
            short_min_markup = 0.0
        # long_markup_range
        if '-lmr' in instance.flags:
            long_markup_range = instance.flags['-lmr']
        else:
#            try:
#                long_markup_range = float(json.loads(st.session_state.instance_config)["long"]["markup_range"])
#            except:
            long_markup_range = 0.0
        # short_markup_range
        if '-smr' in instance.flags:
            short_markup_range = instance.flags['-smr']
        else:
#            try:
#                short_markup_range = float(json.loads(st.session_state.instance_config)["short"]["markup_range"])
#            except:
            short_markup_range = 0.0
        col11, col12, col13 = st.columns([1,1,1])
        with col11:
            user = st.selectbox('User',api.index, index=user_index)
            long_mode = st.radio("LONG_MODE",('normal', 'manual', 'graceful_stop', 'panic', 'tp_only'), key="long_mode", index=long_index, help=pbgui_help.mode)
            long_exposure = round(st.slider("LONG_WALLET_EXPOSURE_LIMIT", key="long_exposure", min_value=0.0, max_value=3.0, step=0.05, value=long_exposure, help=pbgui_help.exposure),2)
        with col12:
            symbol = st.text_input('SYMBOL',value=instance.symbol)
            short_mode = st.radio("SHORT_MODE",('normal', 'manual', 'graceful_stop', 'panic', 'tp_only'), index=short_index, help=pbgui_help.mode)
            short_exposure = round(st.slider("SHORT_WALLET_EXPOSURE_LIMIT", key="short_exposure", min_value=0.0, max_value=3.0, step=0.05, value=short_exposure, help=pbgui_help.exposure),2)
        with col13:
            market = st.radio("MARKET_TYPE",('futures', 'spot'), index=market_index)
            ohlcv = st.checkbox("OHLCV", value=ohlcv, key="ohlcv", help=pbgui_help.ohlcv)
            lev = st.slider("LEVERAGE", key="lev", min_value=2, max_value=20, value=lev, help=pbgui_help.lev)
        with st.expander("Advanced configurations", expanded=False):
            col21, col22, col23 = st.columns([1,1,1])
            with col21:
                long_min_markup = round(st.number_input("LONG_MIN_MARKUP", key="long_min_markup", format="%.4f", min_value=0.0, max_value=0.2, step=0.0001, value=long_min_markup, help=pbgui_help.min_markup),4)
                long_markup_range = round(st.number_input("LONG_MARKUP_RANGE", key="long_markup_range", format="%.4f", min_value=0.0, max_value=0.2, step=0.0001, value=long_markup_range, help=pbgui_help.markup_range),4)
            with col22:
                short_min_markup = round(st.number_input("SHORT_MIN_MARKUP", key="short_min_markup", format="%.4f", min_value=0.0, max_value=0.2, step=0.0001, value=short_min_markup, help=pbgui_help.min_markup),4)
                short_markup_range = round(st.number_input("SHORT_MARKUP_RANGE", key="short_markup_range", format="%.4f", min_value=0.0, max_value=0.2, step=0.0001, value=short_markup_range, help=pbgui_help.markup_range),4)
            with col23:
                assigned_balance = st.number_input("ASSIGNED_BALANCE", key="assigned_balance", min_value=0, step=500, value=assigned_balance, help=pbgui_help.assigned_balance)
                price_distance_threshold = round(st.number_input("PRICE_DISTANCE_THRESHOLD", key="price_distance_threshold", min_value=0.00, step=0.05, value=price_distance_threshold, help=pbgui_help.price_distance_threshold),2)
                price_precision = round(st.number_input("PRICE_PRECISION_MULTIPLIER", key="price_precision", format="%.4f", min_value=0.0000, step=0.0001, value=price_precision, help=pbgui_help.price_precision),4)
                price_step = round(st.number_input("PRICE_STEP_CUSTOM", key="price_step", format="%.3f", min_value=0.000, step=0.001, value=price_step, help=pbgui_help.price_step),3)
        if not "new_config_filename" in st.session_state or not "config_filename" in st.session_state:
            config_filename = f'{os.path.split(instance.config)[-1]}'
        else:
            config_filename = st.session_state.config_filename
        if config_filename in ['live_config.json', 'new_config.json', '']:
            st.session_state.new_config_filename = st.text_input(":red[config filename ***(Please Change filename)***]", value=config_filename, max_chars=None, key="config_filename")
        else:
            st.session_state.new_config_filename = st.text_input("config filename", value=config_filename, max_chars=None, key="config_filename")
        new_instance_config = st.text_area("Instance config", st.session_state.instance_config, key="input_instance_config", height=st.session_state.instance_config_high, placeholder="paste config.json or load config from disk")
        if "overwrite" in st.session_state:
            st.session_state.overwrite = st.checkbox(st.session_state.error)
        if st.form_submit_button("Save"):
            st.write("Changes are no longer supported, please migrate to PBRun.")
    # Display Logfile
    logfile = Path(f'{st.session_state.pbdir}/logs/{instance.user}/{instance.symbol}.log')
    logr = ""
    if logfile.exists():
        with open(logfile, 'r', encoding='utf-8') as f:
            log = f.readlines()
            for line in reversed(log):
                logr = logr+line
    st.button(':recycle: **Refresh Logfile**',)
    stx.scrollableTextbox(logr,height="300")

set_page_config()

# Not supported on windows
if platform.system() == "Windows":
    st.write("Run Module is not supported on Windows")
    exit()

# Init Session State
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    st.switch_page("pbgui.py")
else:
    if not os.path.isfile(f'{st.session_state.pbdir}/manager/config.yaml'):
        copy(f'{st.session_state.pbdir}/manager/config.example.yaml', f'{st.session_state.pbdir}/manager/config.yaml')
    sys.path.insert(0,st.session_state.pbdir)
    sys.path.insert(0,f'{st.session_state.pbdir}/manager')
    manager = __import__("manager")
    Manager = getattr(manager,"Manager")

if 'pb_manager' not in st.session_state:
    try:
        st.session_state.pb_manager = Manager()
        st.session_state.pb_instances = st.session_state.pb_manager.get_instances()
    except Exception as e:
        st.write("### No Instances configured in passivbot instance manager.")
if 'go_backtest' in st.session_state:
    if st.session_state.go_backtest:
        st.session_state.go_backtest = False
        st.switch_page("pages/3_Backtest.py")
if 'editpb_instance' in st.session_state:
    editpb_instance(st.session_state.editpb_instance)
else:
    display_bots()
