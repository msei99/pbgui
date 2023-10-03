import streamlit as st
from pbgui_func import set_page_config
import streamlit_scrollable_textbox as stx
from streamlit_extras.switch_page_button import switch_page
from streamlit_autorefresh import st_autorefresh
from getpass import getuser
import datetime
import subprocess
import sys
import shlex
import shutil
import os
import glob
import multiprocessing
from User import Users
from Backtest import BacktestItem


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
        del st.session_state.expand_files
    else:
        st.session_state.expand_files = glob.glob(f'{dir}/*best_config_*.json')

# store selected config and switch to backtester
def select_bt_conf_file(file):
    del st.session_state.expand_files
    st.session_state[file] = False
    opt.config_file = file

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
    try:
        cmd = ["pgrep", "-U", getuser(), "-f", "optimize.py"]
        pids = subprocess.check_output(cmd).decode("utf-8").strip()
    except subprocess.CalledProcessError:
        cmd = f'{sys.executable} -u {st.session_state.pbdir}/optimize.py -u {user} -i {iters} -pm {mode} -a {algo} -s {symbol} -sd {sd} -ed {ed} -sb {sb} -m {market} -c {cpu}'
        opt_log = open("/tmp/opt.log","w")
        bt_proc = subprocess.Popen(shlex.split(cmd), stdout=opt_log, stderr=opt_log, cwd=st.session_state.pbdir, text=True)
        return
    for pid in pids.splitlines():
        os.kill(int(pid),9)

set_page_config()

# Init Session State
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    switch_page("pbgui")

# Init users
users = Users(f'{st.session_state.pbdir}/api-keys.json')

# Init Optimizer
if 'opt' in st.session_state:
    opt = st.session_state.opt
else:
    opt = BacktestItem()
    st.session_state.opt = opt

if opt.config_file:
    st.session_state.my_bt = opt
    del st.session_state.opt
    if "bt_view" in st.session_state:
        del st.session_state.bt_view
    if "bt_compare" in st.session_state:
        del st.session_state.bt_compare
    if "bt_queue" in st.session_state:
        del st.session_state.bt_queue
    switch_page("Backtest")

# Create Optimizer GUI
col1, col2 = st.columns(2)
with col1:
    opt.user = st.selectbox('User',users.list(), index = users.list().index(opt.user))
    if opt.market_type == "spot":
        opt.symbol = st.selectbox('SYMBOL', opt.spot, index = opt.spot.index(opt.symbol))
    else:
        opt.symbol = st.selectbox('SYMBOL', opt.swap, index = opt.swap.index(opt.symbol))
    opt.market_type = st.radio("MARKET_TYPE",('futures', 'spot'), index = 0 if opt.market_type == "futures" else 1)
    mode = st.radio('PASSIVBOT_MODE',('recursive_grid', 'neat_grid', 'clock'))
    algo = st.radio("ALGORITHM",('harmony_search', 'particle_swarm_optimization'))
with col2:
    opt.sb = st.number_input('STARTING_BALANCE',value=opt.sb,step=500)
    iters = st.number_input('ITERS',value=10000,step=1000)
    opt.sd = st.date_input("START_DATE", datetime.datetime.strptime(opt.sd, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
    opt.ed = st.date_input("END_DATE", datetime.datetime.strptime(opt.ed, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
    cpu = st.slider("N_CPUS",min_value=1, max_value=multiprocessing.cpu_count(), value=multiprocessing.cpu_count()-1)
    if optimizer():
        st.header('Optimizer is running....')
    else:
        st.header('Optimizer is stopped')
# Start optimizer
with col1:
    st.button("Start/Stop Optimizer", on_click=run_optimizer, args=[opt.user,opt.symbol, opt.sd, opt.ed, opt.sb, iters, algo, opt.market_type, mode, cpu])
if optimizer():
    st.button(':recycle: **Optimizer Logfile**',)
    logfile = load_opt_log()
    stx.scrollableTextbox(logfile,height="300")
    opt_count = st_autorefresh(interval=15000, limit=None, key="opt_counter")

# Display optimizer results
if 'expand_files' in st.session_state:
    if st.session_state.expand_files:
        if opt.symbol != os.path.dirname(st.session_state.expand_files[0]).split('_')[-1]:
            opt.symbol = os.path.dirname(st.session_state.expand_files[0]).split('_')[-1]
            st.experimental_rerun()
        st.subheader('Optimizing results: (Select a file for run backtest)')
        st.checkbox("..", False, key=("back"), on_change=select_optdir, args=["back"])
        st.session_state.expand_files.sort(reverse=True)
        for file in st.session_state.expand_files:
            st.checkbox(file, False, key=(file), on_change=select_bt_conf_file, args=[file])
    else:
        st.subheader('Empty Directory')
        st.checkbox("..", False, key=("back"), on_change=select_optdir, args=["back"])
else:
    dirs = glob.glob(f'{st.session_state.pbdir}/results_{algo}_{mode}/*')
    if dirs:
        dirs.sort(reverse=True)
        for dir in dirs:
            st.checkbox(dir, False, key=(dir), on_change=select_optdir, args=[dir])
