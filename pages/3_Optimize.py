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
import os
import glob
import psutil
import multiprocessing
from shutil import rmtree
from User import Users
from Backtest import BacktestItem
from pathlib import Path, PurePath

# Load optimizer logfile from tmp
def load_opt_log():
    pbgdir = Path.cwd()
    logfile = Path(f'{pbgdir}/data/logs/opt.log')
    logr = ""
    try:
        with open(logfile, 'r', encoding='utf-8') as f:
            log = f.readlines()
            for line in reversed(log):
                logr = logr+line
    except FileNotFoundError:
        print(f'Optimizer Logfile {log}, not found. Maybe you have a running optimizer not started from pbgui.')
        logdata = f'Optimizer Logfile {log}, not found. Maybe you have a running optimizer not started from pbgui.'
    return logr

# Change dir to selected
def select_optdir(dir):
    if dir == "back":
        del st.session_state.expand_files
    else:
        st.session_state.expand_files = glob.glob(f'{dir}/*best_config_*.json')
        if not st.session_state.expand_files:
            st.session_state.expand_files_empty = dir

# store selected config and switch to backtester
def select_bt_conf_file(file):
    del st.session_state.expand_files
    st.session_state[file] = False
    opt._config.config_file = file
    opt._config.load_config()

# Check optimizer is running
def optimizer():
    try:
        for process in psutil.process_iter(['pid', 'name', 'cmdline', 'username']):
            if str(process.info["username"]).endswith(getuser()) and any("optimize.py" in sub for sub in process.info["cmdline"]):
                return True
    except psutil.NoSuchProcess:
        pass
    return False

# Run Optimizer in background
def run_optimizer(user, symbol, sd, ed, sb, iters, algo, market, mode, cpu):
    pids = []
    try:
        for process in psutil.process_iter(['pid', 'name', 'cmdline', 'username']):
            if str(process.info["username"]).endswith(getuser()) and any("optimize.py" in sub for sub in process.info["cmdline"]):
                pids.append(process.info["pid"])
    except psutil.NoSuchProcess:
        pass
    if pids:
        for pid in pids:
            os.kill(int(pid),9)
    else:
        cmd_end = f'-u {user} -i {iters} -pm {mode} -a {algo} -s {symbol} -sd {sd} -ed {ed} -sb {sb} -m {market} -c {cpu}'
        cmd = [sys.executable, '-u', PurePath(f'{st.session_state.pbdir}/optimize.py')]
        cmd.extend(shlex.split(cmd_end))
        pbgdir = Path.cwd()
        dest = Path(f'{pbgdir}/data/logs')
        if not dest.exists():
            dest.mkdir(parents=True)
        opt_log = open(Path(f'{dest}/opt.log'),"w")
        bt_proc = subprocess.Popen(cmd, stdout=opt_log, stderr=opt_log, cwd=PurePath(st.session_state.pbdir), text=True)

set_page_config()

# Init Session State
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    switch_page("pbgui")

# Init users
users = Users()

# Init Optimizer
if 'opt' in st.session_state:
    opt = st.session_state.opt
else:
    opt = BacktestItem()
    st.session_state.opt = opt

if opt._config.config_file:
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
opt.edit_base()
col_1, col_2, col_3 = st.columns([1,1,1])
with col_1:
    opt.sb = st.number_input('STARTING_BALANCE',value=opt.sb,step=500)
with col_2:
    opt.sd = st.date_input("START_DATE", datetime.datetime.strptime(opt.sd, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
with col_3:
    opt.ed = st.date_input("END_DATE", datetime.datetime.strptime(opt.ed, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")

col1, col2, col3 = st.columns([1,1,1])
with col1:
    mode = st.radio('PASSIVBOT_MODE',('recursive_grid', 'neat_grid', 'clock'))
with col2:
    algo = st.radio("ALGORITHM",('harmony_search', 'particle_swarm_optimization'))
with col3:
    iters = st.number_input('ITERS',value=10000,step=1000)
    cpu = st.slider("N_CPUS",min_value=1, max_value=multiprocessing.cpu_count(), value=multiprocessing.cpu_count()-1)
# Start optimizer
with col1:
    if not optimizer():
        if st.button("Start Optimizer"):
            run_optimizer(opt.user,opt.symbol, opt.sd, opt.ed, opt.sb, iters, algo, opt.market_type, mode, cpu)
            st.experimental_rerun()
    else:
        if st.button("Stop Optimizer"):
            run_optimizer(opt.user,opt.symbol, opt.sd, opt.ed, opt.sb, iters, algo, opt.market_type, mode, cpu)
            st.experimental_rerun()
if optimizer():
    st.button(':recycle: **Optimizer Logfile**',)
    logr = load_opt_log()
    stx.scrollableTextbox(logr,height="300")
    opt_count = st_autorefresh(interval=15000, limit=None, key="opt_counter")

# Display optimizer results
if 'expand_files' in st.session_state:
    if st.session_state.expand_files:
        if opt.symbol != os.path.dirname(st.session_state.expand_files[0]).split('_')[-1]:
            opt.symbol = os.path.dirname(st.session_state.expand_files[0]).split('_')[-1]
            st.experimental_rerun()
        st.subheader('Optimizing results: (Select a file for run backtest)')
        if st.checkbox("Delete"):
            rmtree(PurePath(st.session_state.expand_files[0]).parents[0], ignore_errors=True)
            del st.session_state.expand_files
            st.experimental_rerun()
        st.checkbox("..", False, key=("back"), on_change=select_optdir, args=["back"])
        st.session_state.expand_files.sort(reverse=True)
        for file in st.session_state.expand_files:
            st.checkbox(file, False, key=(file), on_change=select_bt_conf_file, args=[file])
    else:
        st.subheader('Empty Directory')
        if st.checkbox("Delete"):
            rmtree(st.session_state.expand_files_empty, ignore_errors=True)
            del st.session_state.expand_files_empty
            del st.session_state.expand_files
            st.experimental_rerun()
        st.checkbox("..", False, key=("back"), on_change=select_optdir, args=["back"])
else:
    dirs = glob.glob(f'{st.session_state.pbdir}/results_{algo}_{mode}/*')
    if dirs:
        dirs.sort(reverse=True)
        for dir in dirs:
            st.checkbox(dir, False, key=(dir), on_change=select_optdir, args=[dir])
