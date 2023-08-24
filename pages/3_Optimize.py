import streamlit as st
import streamlit_scrollable_textbox as stx
from streamlit_extras.switch_page_button import switch_page
from streamlit_autorefresh import st_autorefresh
from getpass import getuser
import hjson, json
import datetime
import subprocess
import sys
import shlex
import shutil
import os
import glob
import pandas as pd
import multiprocessing

# Load Optimizer config to cache
@st.cache_data
def load_opt_conf():
    with open(st.session_state.pbdir+'/configs/optimize/default.hjson', 'r', encoding='utf-8') as f:
        opt_conf = hjson.load(f)
    return opt_conf

# Load Backtester config to cache
@st.cache_data
def load_bt_conf():
    with open(st.session_state.pbdir+'/configs/backtest/default.hjson', 'r', encoding='utf-8') as f:
        bt_conf = hjson.load(f)
    return bt_conf

# Load optimizer logfile from tmp
def load_opt_log():
    try:
        with open('/tmp/opt.log', 'r', encoding='utf-8') as f:
            logdata = f.read()
    except FileNotFoundError:
        print("Optimizer Logfile (/tmp/opt.log), not found. Maybe you have a running optimizer not started from pbgui.")
        logdata = "Optimizer Logfile (/tmp/opt.log), not found. Maybe you have a running optimizer not started from pbgui."
    return logdata

# Change dir to selected
def select_optdir(dir):
    if dir == "back":
        st.session_state.expand_files = ""
    else:
        st.session_state.expand_files = glob.glob(f'{dir}/*best_config_*.json')

# store selected config to session state and switch to backtester
def select_bt_conf_file(file):
    st.session_state.expand_files = ""
    st.session_state[file] = False
    st.session_state.bt_conf_filename = file
    st.session_state.go_backtest = True

# Delete old files
def delete_files_and_subdirectories(directory_path):
   try:
     with os.scandir(directory_path) as entries:
       for entry in entries:
         if entry.is_file():
            os.unlink(entry.path)
         else:
            shutil.rmtree(entry.path)
     print("All files and subdirectories deleted successfully.")
   except OSError:
     print("Error occurred while deleting files and subdirectories.")

# Check optimizer is running
def optimizer():
    try:
        cmd = ["pgrep", "-U", getuser(), "-f", "optimize.py"]
        pids = subprocess.check_output(cmd).decode("utf-8").strip()
    except subprocess.CalledProcessError:
        return False
    return True

# Run Optimizer in background
def run_optimizer(user, symbol, sd, ed, sb, iters, algo, market, mode, cpu):
#    directory_path = f'{st.session_state.pbdir}/results_{algo}_{mode}'
#    delete_files_and_subdirectories(directory_path)
    try:
        cmd = ["pgrep", "-U", getuser(), "-f", "optimize.py"]
        pids = subprocess.check_output(cmd).decode("utf-8").strip()
    except subprocess.CalledProcessError:
        symbol = ','.join(symbol)
        sd = sd.strftime("%Y-%m-%d")
        ed = ed.strftime("%Y-%m-%d")
        cmd = f'{sys.executable} -u {st.session_state.pbdir}/optimize.py -u {user} -i {iters} -pm {mode} -a {algo} -s {symbol} -sd {sd} -ed {ed} -sb {sb} -m {market} -c {cpu}'
        opt_log = open("/tmp/opt.log","w")
        bt_proc = subprocess.Popen(shlex.split(cmd), stdout=opt_log, stderr=opt_log, cwd=st.session_state.pbdir, text=True)
        return
    for pid in pids.splitlines():
        os.kill(int(pid),9)

st.set_page_config(
    page_title="Passivbot GUI - Optimize",
    page_icon=":screwdriver:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "Passivbot GUI"
    }
)

# Init Session State
if 'pbdir' not in st.session_state:
    switch_page("pbgui")
if 'go_backtest' in st.session_state:
    if st.session_state.go_backtest:
        st.session_state.go_backtest = False
        switch_page("Backtest")
if 'expand_files' not in st.session_state:
    st.session_state.expand_files = ""

st.header("Optimize")

# Load defaul optimize, backtest and api-keys
opt_conf = load_opt_conf()
bt_conf = load_bt_conf()
api = pd.read_json(st.session_state.pbdir+'/api-keys.json', typ='frame', orient='index')

# Create Optimizer GUI
col1, col2 = st.columns(2)
with col1:
    user = st.selectbox('User',api.index, api.index.get_loc(bt_conf['user']))
    if 'symbol' not in st.session_state:
        if bt_conf['symbol'] not in opt_conf['symbols']:
            st.session_state.symbol = 'BTCUSDT'
        else:
            st.session_state.symbol = bt_conf['symbol']
    symbol = st.multiselect('SYMBOL', opt_conf['symbols'], st.session_state.symbol)
    mode = st.radio('PASSIVBOT_MODE',('recursive_grid', 'neat_grid', 'clock'))
    algo = st.radio("ALGORITHM",('harmony_search', 'particle_swarm_optimization'))
    market = st.radio("MARKET_TYPE",('futures', 'spot'))
with col2:
    sb = st.number_input('STARTING_BALANCE',value=1000,step=500)
    iters = st.number_input('ITERS',value=10000,step=1000)
    today = datetime.datetime.now()
    sd = st.date_input("START_DATE", datetime.date.today() - datetime.timedelta(days=365*4),format="YYYY-MM-DD")
    ed = st.date_input("END_DATE", datetime.date.today(),format="YYYY-MM-DD")
    cpu = st.slider("N_CPUS",min_value=1, max_value=multiprocessing.cpu_count(), value=multiprocessing.cpu_count()-1)
    if optimizer():
        st.header('Optimizer is running....')
    else:
        st.header('Optimizer is stopped')
# Start optimizer
with col1:
    st.button("Start/Stop Optimizer", on_click=run_optimizer, args=[user,symbol, sd, ed, sb, iters, algo, market, mode, cpu])
if optimizer():
    st.button(':recycle: **Optimizer Logfile**',)
    logfile = load_opt_log()
    stx.scrollableTextbox(logfile,height="300")
    opt_count = st_autorefresh(interval=15000, limit=None, key="opt_counter")

# Display optimizer results
if st.session_state.expand_files != "":
    if st.session_state.symbol != os.path.dirname(st.session_state.expand_files[0]).split('_')[-1]:
        st.session_state.symbol = os.path.dirname(st.session_state.expand_files[0]).split('_')[-1]
        st.experimental_rerun()
    st.subheader('Optimizing results: (Select a file for run backtest)')
    st.checkbox("..", False, key=("back"), on_change=select_optdir, args=["back"])
    for file in st.session_state.expand_files:
        st.checkbox(file, False, key=(file), on_change=select_bt_conf_file, args=[file])
else:
    dirs = glob.glob(f'{st.session_state.pbdir}/results_{algo}_{mode}/*')
    if dirs:
        for dir in dirs:
            st.checkbox(dir, False, key=(dir), on_change=select_optdir, args=[dir])
