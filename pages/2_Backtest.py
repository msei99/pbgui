import streamlit as st
from pbgui_func import set_page_config
from Backtest import BacktestItem, BacktestQueue, BacktestResults
from streamlit_extras.switch_page_button import switch_page
import datetime
import multiprocessing


def bt_add():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Navigation
    with st.sidebar:
        if st.button("Queue"):
            st.session_state.bt_queue = True
            st.experimental_rerun()
        if st.button("Compare"):
            st.session_state.bt_compare = True
            st.experimental_rerun()
        if st.button("Import"):
            st.session_state.bt_import = True
            st.experimental_rerun()
    # Create Backtest GUI
    my_bt.edit_base()
    col_1, col_2, col_3 = st.columns([1,1,1])
    with col_1:
        my_bt.sb = st.number_input('STARTING_BALANCE',value=my_bt.sb,step=500)
    with col_2:
        my_bt.sd = st.date_input("START_DATE", datetime.datetime.strptime(my_bt.sd, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
    with col_3:
        my_bt.ed = st.date_input("END_DATE", datetime.datetime.strptime(my_bt.ed, '%Y-%m-%d'), format="YYYY-MM-DD").strftime("%Y-%m-%d")
    my_bt.edit_config()
    if st.button("Add to Backtest Queue"):
        if not my_bt.config:
            st.session_state.error = 'Config is empty'
        elif not "error" in st.session_state:
            my_bt.save()
            my_bt.file = None
            st.session_state.bt_queue = True
        st.experimental_rerun()
 

def bt_queue():
    # Init backtest queue
    if "my_btq" in st.session_state:
        my_btq = st.session_state.my_btq
    else:
        st.session_state.my_btq = BacktestQueue() 
        my_btq = st.session_state.my_btq
    my_btq.load()
    # Navigation
    with st.sidebar:
        st.button(":recycle:")
        if st.button("Compare"):
            st.session_state.bt_compare = True
            del st.session_state.bt_queue
            st.experimental_rerun()
        if st.button(":back:"):
            del st.session_state.bt_queue
            st.experimental_rerun()
    # Options
    col_run, col_cpu, col_empty = st.columns([1,2,7]) 
    with col_run:
        my_btq.autostart = st.toggle("Autostart", value=my_btq.autostart, key="autostart", help=None)
        if st.button(":wastebasket:"):
            my_btq.remove_finish()
    with col_cpu:
        my_btq.cpu = st.number_input(f'Max running Backtests CPU(1 - {multiprocessing.cpu_count()})', min_value=1, max_value=multiprocessing.cpu_count(), value=my_btq.cpu, step=1)
    # Backtest Queue
    d = []
    if not "ed_bt_key" in st.session_state:
        st.session_state.ed_bt_key = 0
    column_config = {
        "Run": st.column_config.CheckboxColumn('Start/Stop/View', default=False),
        "Log": st.column_config.CheckboxColumn('Log', default=False),
        "Config": st.column_config.CheckboxColumn('Config', default=False)}
    for id, bt in enumerate(my_btq.items):
        d.append({
            'id': id,
            'Run': bt.is_running(),
            'Status': bt.status(),
            'Log': False,
            'Config': False,
            'User': bt.user,
            'Symbol': bt.symbol,
            'Market': bt.market_type,
            'Exchange': bt.exchange.id,
            'Start': bt.sd,
            'End': bt.ed,
            'Balance': bt.sb,
            'Delete': False,
        })
    selected = st.data_editor(data=d, width=None, height=(len(my_btq.items)+1)*36, use_container_width=True, key=f'editor_{st.session_state.ed_bt_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['User','Symbol','Exchange','Start','End','Balance'])
    for line in selected:
        if line["Run"]:
            if not my_btq.items[line["id"]].is_running() and not my_btq.items[line["id"]].is_finish():
                my_btq.items[line["id"]].run()
                st.experimental_rerun()
            elif my_btq.items[line["id"]].is_finish():
                del st.session_state.bt_queue
                if "bt_results" in st.session_state:
                    del st.session_state.bt_results
                st.session_state.bt_view = my_btq.items[line["id"]]
                st.experimental_rerun()
        elif my_btq.items[line["id"]].is_running():
            my_btq.items[line["id"]].stop()
            st.experimental_rerun()
        if line["Log"]:
            st.session_state.bt_log = ({
                'id': line["id"],
                'log': my_btq.items[line["id"]].load_log()})
            st.session_state.ed_bt_key += 1
            st.experimental_rerun()
        if line["Config"]:
            st.session_state.bt_config = ({
                'id': line["id"],
                'config': my_btq.items[line["id"]].config})
            st.session_state.ed_bt_key += 1
            st.experimental_rerun()
        if line["Delete"]:
            my_btq.items[line["id"]].remove()
            st.experimental_rerun()
    if "bt_config" in st.session_state:
        if st.button(f':negative_squared_cross_mark: {st.session_state.bt_config["id"]}', key="view_bt_config"):
            del st.session_state.bt_config
            st.experimental_rerun()
        st.code(st.session_state.bt_config["config"])
    if "bt_log" in st.session_state:
        if st.button(f':negative_squared_cross_mark: {st.session_state.bt_log["id"]}', key="view_bt_log"):
            del st.session_state.bt_log
            st.experimental_rerun()
        st.code(st.session_state.bt_log["log"])

def bt_view():
    # Navigation
    with st.sidebar:
        if st.button("Queue"):
            if "bt_results" in st.session_state:
                del st.session_state.bt_results
            st.session_state.bt_queue = True
            del st.session_state.bt_view
            st.experimental_rerun()
        if st.button("Compare"):
            st.session_state.bt_compare = True
            del st.session_state.bt_view
            if "bt_results" in st.session_state:
                del st.session_state.bt_results
            st.experimental_rerun()
        if st.button(":back:"):
            del st.session_state.bt_view
            if "bt_results" in st.session_state:
                del st.session_state.bt_results
            st.experimental_rerun()
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
        if st.button(":recycle:"):
            del st.session_state.bt_results
            st.experimental_rerun()
        if st.button("Queue"):
            st.session_state.bt_queue = True
            del st.session_state.bt_compare
            if "bt_results" in st.session_state:
                del st.session_state.bt_results
            st.experimental_rerun()
        if st.button(":back:"):
            if "bt_results" in st.session_state:
                del st.session_state.bt_results
            del st.session_state.bt_compare
            st.experimental_rerun()
    st.markdown('### Filter and select backtests for view')
    if "bt_results" in st.session_state:
        bt_results = st.session_state.bt_results
    else:     
        st.session_state.bt_results = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
        bt_results = st.session_state.bt_results
        bt_results.find_all()
    col1, col2, col3, col4 = st.columns([1,1,1,1])
    with col1:
        symbols = st.multiselect("Symbols", bt_results.symbols, default=None, key=None, on_change=None, args=None)
    with col2:
        exchanges = st.multiselect("Exchanges", bt_results.exchanges, default=None, key=None, on_change=None, args=None)
    bt_results.view(symbols = symbols, exchanges = exchanges)

def bt_import():
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.bt_import
            st.experimental_rerun()
    my_bt.import_pbconfigdb()
#    st.data_editor(data=df, width=None, height=None, use_container_width=True, hide_index=None, column_order=None)


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
elif "bt_import" in st.session_state:
    bt_import()
else:
    bt_add()
