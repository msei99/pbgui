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

# Save passivbot config (.json) to temporary folder for backtesting
def save_bt_conf_file(file):
    with open('/tmp/bt_conffile.json', 'w', encoding='utf-8') as f:
        f.write(file)

# Load backtest logfile from tmp
def load_bt_log():
    with open('/tmp/bt.log', 'r', encoding='utf-8') as f:
        logdata = f.read()
    return logdata

# Load passivbot config (.json) from optimizer
def load_bt_conffile(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        bt_conf_file = f.read()
    return bt_conf_file

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

# Check backtest is running
def backtest():
    try:
        cmd = ["pgrep", "-U", getuser(), "-f", "backtest.py"]
        pids = subprocess.check_output(cmd).decode("utf-8").strip()
    except subprocess.CalledProcessError:
        return False
    return True

# Run Backtest in background
def run_backtest(user, symbol, sd, ed, sb, market, file):
#    directory_path = f'{st.session_state.pbdir}/backtests/pbgui'
#    delete_files_and_subdirectories(directory_path)
    save_bt_conf_file(file)
    sd = sd.strftime("%Y-%m-%d")
    ed = ed.strftime("%Y-%m-%d")
    cmd = f'{sys.executable} -u {st.session_state.pbdir}/backtest.py -u {user} -s {symbol} -sd {sd} -ed {ed} -sb {sb} -m {market} -bd ./backtests/pbgui /tmp/bt_conffile.json'
    bt_log = open("/tmp/bt.log","w")
    subprocess.Popen(shlex.split(cmd), stdout=bt_log, stderr=bt_log, cwd=st.session_state.pbdir, text=True)

st.set_page_config(
    page_title="Passivbot GUI - Backtest",
    page_icon=":screwdriver:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get help': 'https://github.com/msei99/pbgui/#readme',
        'About': "Passivbot GUI"
    }
)

# Init session state
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    switch_page("pbgui")
if 'bt_conf_filename' in st.session_state:
    st.session_state.bt_conf_file = load_bt_conffile(st.session_state.bt_conf_filename)
else:
    st.session_state.bt_conf_file = ""

# Load defaul optimize, backtest and api-keys
opt_conf = load_opt_conf()
bt_conf = load_bt_conf()
api = pd.read_json(st.session_state.pbdir+'/api-keys.json', typ='frame', orient='index')

# Create Backtest GUI
col1, col2 = st.columns(2)
with col1:
    user = st.selectbox('User',api.index, api.index.get_loc(bt_conf['user']))
    if 'symbol' not in st.session_state:
        if bt_conf['symbol'] not in opt_conf['symbols']:
            st.session_state.symbol = 'BTCUSDT'
        else:
            st.session_state.symbol = bt_conf['symbol']
    symbol = st.selectbox('SYMBOL', opt_conf['symbols'], opt_conf['symbols'].index(st.session_state.symbol))
    market = st.radio("MARKET_TYPE",('futures', 'spot'))
with col2:
    sb = st.number_input('STARTING_BALANCE',value=1000,step=500)
    today = datetime.datetime.now()
    sd = st.date_input("START_DATE", datetime.date.today() - datetime.timedelta(days=365*4),format="YYYY-MM-DD")
    ed = st.date_input("END_DATE", datetime.date.today(),format="YYYY-MM-DD")

# Load selected passivbot config (.json) from optimizer
bt_filename = ""
if 'bt_conf_filename' in st.session_state:
    bt_filename = os.path.split(st.session_state.bt_conf_filename)[1]

#stx.scrollableTextbox(st.session_state.bt_conf_file, height="500", fontFamily="Courier")
st.session_state.bt_conf_file = st.text_area("Passivbot Config: "+symbol+" "+bt_filename,st.session_state.bt_conf_file, height=500, placeholder="Paste config and select correct SYMBOL")

# Start backtest
if st.session_state.bt_conf_file != "":
    st.button('Start Backtest', on_click=run_backtest, args=[user, symbol, sd, ed, sb, market, st.session_state.bt_conf_file])
if backtest():
    st.header('Backtest is running....')
    st.button(':recycle: **Backtest Logfile**',)
    logfile = load_bt_log()
    stx.scrollableTextbox(logfile,height="300")
    bt_count = st_autorefresh(interval=15000, limit=None, key="bt_counter")

# Display backtest results
if st.session_state.bt_conf_file != "" and not backtest():
    bt_l = json.loads(st.session_state.bt_conf_file)["long"]
    bt_s = json.loads(st.session_state.bt_conf_file)["short"]
    files = glob.glob(f'{st.session_state.pbdir}/backtests/pbgui/*/*/plots/*/live_config.json', recursive = True)
    if files:
        for file in files:
            with open(file, 'r', encoding='utf-8') as f:
                bt_json = json.load(f)
                if bt_json["long"] == bt_l and bt_json["short"] == bt_s:
                    bt_path = os.path.split(file)[0]
                    st.header('Backtest results:')
                    files = glob.glob(f'{bt_path}/balance_and_equity_sampled_*.png', 
                            recursive = True)
                    for file in files:
                        st.image(file)
                    st.image(f'{bt_path}/wallet_exposures_plot.png')
                    with open(f'{bt_path}/backtest_result.txt', 'r', encoding='utf-8') as f:
                        st.code(f.read())
