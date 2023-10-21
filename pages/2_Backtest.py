import streamlit as st
from streamlit_super_slider import st_slider
from pbgui_func import set_page_config, validateJSON
from Backtest import BacktestItem, BacktestQueue, BacktestResults
from User import Users
from streamlit_extras.switch_page_button import switch_page
import datetime
import multiprocessing

# Cleanup session_state
def cleanup():
    if "bt_queue" in st.session_state:
        del st.session_state.bt_queue
    if "bt_view" in st.session_state:
        del st.session_state.bt_view
    if "log" in st.session_state:
        del st.session_state.log
    if "bt_results" in st.session_state:
        del st.session_state.bt_results

# handler for button clicks
def button_handler(button=None, item=None):
    if button == "back":
        cleanup()
    if button == "back_compare":
        del st.session_state.bt_compare
    if button == "back_view":
        st.session_state.bt_queue = True
        del st.session_state.bt_view
    if button == "add_queue":
        if not my_bt.config or not validateJSON(my_bt.config):
           st.session_state.error = 'config is empty or invalid'
        else:
             if "error" in st.session_state:
                del st.session_state.error
             my_bt.save()
             my_bt.file = None
             st.session_state.bt_queue = True
    if button == "queue":
        st.session_state.bt_queue = True
    if button == "remove":
        item.remove()
    if button == "run":
        item.run()
    if button == "stop":
        item.stop()
    if button == "view":
        cleanup()
        st.session_state.bt_view = item
    if button == "compare":
        cleanup()
        st.session_state.bt_compare = True
    if button == "log":
        st.session_state.bt_log = item

def bt_add():
    # Init users
    users = Users()
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Navigation
    with st.sidebar:
        st.button("Backtest Queue", key="queue", on_click=button_handler, args=["queue"])
        st.button("Compare Backtests", key="compare", on_click=button_handler, args=["compare"])
    # Create Backtest GUI
    col1, col2 = st.columns(2)
    with col1:
        my_bt.user = st.selectbox('User',users.list(), index = users.list().index(my_bt.user))
        if my_bt.market_type == "spot":
            my_bt.symbol = st.selectbox('SYMBOL', my_bt.spot, index = my_bt.spot.index(my_bt.symbol))
        else:
            my_bt.symbol = st.selectbox('SYMBOL', my_bt.swap, index = my_bt.swap.index(my_bt.symbol))
        my_bt.market_type = st.radio("MARKET_TYPE",('futures', 'spot'), index = 0 if my_bt.market_type == "futures" else 1)
    with col2:
        my_bt.sb = st.number_input('STARTING_BALANCE',value=my_bt.sb,step=500)
        my_bt.sd = st.date_input("START_DATE", datetime.datetime.strptime(my_bt.sd, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
        my_bt.ed = st.date_input("END_DATE", datetime.datetime.strptime(my_bt.ed, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
    my_bt.config = st.text_area("Passivbot Config: ",my_bt.config, height=500)
    st.button("Add to Backtest Queue", key=f'add_queue', on_click=button_handler, args=["add_queue"])

def bt_queue():
    if "my_btq" in st.session_state:
        my_btq = st.session_state.my_btq
    else:
        st.session_state.my_btq = BacktestQueue() 
        my_btq = st.session_state.my_btq
    my_btq.load()
    col_run, col_cpu = st.columns([1,1]) 
    with col_cpu:
        st.markdown("###### <center>Max running Backtests</center>", unsafe_allow_html=True)
        my_btq.cpu = st_slider(min_value=1, max_value=multiprocessing.cpu_count(), default_value=my_btq.cpu)
#        my_btq.cpu = st.slider("Max running Backtests",min_value=1, max_value=multiprocessing.cpu_count(), value=my_btq.cpu)
    with col_run:
        my_btq.autostart = st.toggle("Autostart", value=my_btq.autostart, key="autostart", help=None)
        st.button(':recycle:',)
    col_del, col_run, col1, col2, col_exchange, col3, col4, col5, col_log = st.columns([1,1,1,1,1,1,1,1,1]) 
    with col_del:
        st.write("##### **Remove**")
    with col_run:
        st.write("##### **Run**")
    with col1:
        st.write("##### **Status**")
    with col2:
        st.write("##### **Symbol**")
    with col_exchange:
        st.write("##### **Exchange**")
    with col3:
        st.write("##### **Start**")
    with col4:
        st.write("##### **End**")
    with col5:
        st.write("##### **Balance**")
    with col_log:
        st.write("##### **Log**")
    for i in my_btq.items:
        col_del, col_run, col1, col2, col_exchange, col3, col4, col5, col_log = st.columns([1,1,1,1,1,1,1,1,1])
        with col_del:
            help_config = ""
            for line in i.config.split(sep=','):
                help_config = help_config + line + ",  "
            help_config = help_config[:-3]
            st.button(":wastebasket:", key=f'remove {i}', on_click=button_handler, args=["remove",i], help=help_config)
        with col_run:
            if i.is_running():
                st.button("Stop", key=f'stop {i}', on_click=button_handler, args=["stop",i])
            elif i.is_finish():
                st.button("View", key=f'stop {i}', on_click=button_handler, args=["view",i])
            else:
                st.button("Run", key=f'run {i}', on_click=button_handler, args=["run",i])
        with col1:
            st.write(i.status())
        with col2:
                st.write(i.symbol)
        with col_exchange:
                st.write(i.exchange.id)
        with col3:
                st.write(i.sd)
        with col4:
                st.write(i.ed)
        with col5:
                st.write(i.sb)
        with col_log:
            st.button("Log", key=f'log {i}', on_click=button_handler, args=["log",i])
    # Navigation
    with st.sidebar:
        st.button(":back:", key="back", on_click=button_handler, args=["back"])
    if "bt_log" in st.session_state:
        if st.session_state.bt_log:
            st.button(':recycle: **Backtest Logfile**',)
            st.code(st.session_state.bt_log.load_log())

def bt_view():
    # Navigation
    with st.sidebar:
        st.button(":back:", key="back_view", on_click=button_handler, args=["back_view"])
    if "bt_view" in st.session_state:
        if "bt_results" in st.session_state:
            bt_results = st.session_state.bt_results
        else:     
            st.session_state.bt_results = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
            bt_results = st.session_state.bt_results
            bt_results.match_item(st.session_state.bt_view)
    bt_results.view()

def bt_compare():
    # Navigation
    with st.sidebar:
        st.button(":back:", key="back_compare", on_click=button_handler, args=["back_compare"])
    st.markdown('### Filter and select backtests for view')
    if "bt_results" in st.session_state:
        bt_results = st.session_state.bt_results
    else:     
        st.session_state.bt_results = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
        bt_results = st.session_state.bt_results
        bt_results.find_all()
    col_symbol, col_exchange = st.columns([1,1])
    with col_symbol:
        symbols = st.multiselect("Symbols", bt_results.symbols, default=None, key=None, on_change=None, args=None)
    with col_exchange:
        exchanges = st.multiselect("Exchanges", bt_results.exchanges, default=None, key=None, on_change=None, args=None)
    bt_results.view(symbols = symbols, exchanges = exchanges)


set_page_config()

# Init session state
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    switch_page("pbgui")
if 'my_bt' in st.session_state:
    my_bt = st.session_state.my_bt
else:
    my_bt = BacktestItem()
    st.session_state.my_bt = my_bt

if "bt_queue" in st.session_state:
    bt_queue()
elif "bt_view" in st.session_state:
    bt_view()
elif "bt_compare" in st.session_state:
    bt_compare()
else:
    bt_add()
