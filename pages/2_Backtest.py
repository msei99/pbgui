import streamlit as st
from pbgui_func import set_page_config
from Backtest import BacktestItem, BacktestQueue, BacktestResults
import datetime
import multiprocessing


def bt_add():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Navigation
    with st.sidebar:
        if st.button("Results"):
            st.session_state.bt_compare = True
            st.rerun()
        if st.button("Queue"):
            st.session_state.bt_queue = True
            st.rerun()
        if st.button("Import"):
            st.session_state.bt_import = True
            st.rerun()
    # Create Backtest GUI
    my_bt.edit_base()
    col_1, col_2, col_3 = st.columns([1,1,1])
    with col_1:
        if "config_bt_sb" in st.session_state:
            my_bt.sb = st.session_state.config_bt_sb
        st.number_input('STARTING_BALANCE',value=my_bt.sb,step=500, key="config_bt_sb")
    with col_2:
        if "config_bt_sd" in st.session_state:
            my_bt.sd = st.session_state.config_bt_sd.strftime("%Y-%m-%d")
        st.date_input("START_DATE", datetime.datetime.strptime(my_bt.sd, '%Y-%m-%d'), format="YYYY-MM-DD", key="config_bt_sd")
    with col_3:
        if "config_bt_ed" in st.session_state:
            my_bt.ed = st.session_state.config_bt_ed.strftime("%Y-%m-%d")
        st.date_input("END_DATE", datetime.datetime.strptime(my_bt.ed, '%Y-%m-%d'), format="YYYY-MM-DD", key="config_bt_ed")
    my_bt.edit_config()
    if st.button("Add to Backtest Queue"):
        if not my_bt.config:
            st.session_state.error = 'Config is empty'
        elif not "error" in st.session_state:
            my_bt.save()
            my_bt.file = None
            st.session_state.bt_queue = True
        st.rerun()
 

def bt_queue():
    # Init backtest queue
    if "my_btq" in st.session_state:
        my_btq = st.session_state.my_btq
    else:
        st.session_state.my_btq = BacktestQueue() 
        my_btq = st.session_state.my_btq
    # Init session state for keys
    if "backtest_cpu" in st.session_state:
        if st.session_state.backtest_cpu != my_btq.cpu:
            my_btq.cpu = st.session_state.backtest_cpu
    if "backtest_autostart" in st.session_state:
        if st.session_state.backtest_autostart != my_btq.autostart:
            my_btq.autostart = st.session_state.backtest_autostart
    # Load Queue
    my_btq.load()
    # Navigation
    with st.sidebar:
        st.button(":recycle:")
        if st.button(":back:"):
            del st.session_state.bt_queue
            st.rerun()
        if st.button("Results"):
            st.session_state.bt_compare = True
            del st.session_state.bt_queue
            st.rerun()
        st.number_input(f'Max CPU(1 - {multiprocessing.cpu_count()})', min_value=1, max_value=multiprocessing.cpu_count(), value=my_btq.cpu, step=1, key = "backtest_cpu")
        st.toggle("Autostart", value=my_btq.autostart, key="backtest_autostart", help=None)
        if st.button(":wastebasket: finished"):
            my_btq.remove_finish()
            st.rerun()
        if st.button(":wastebasket: all"):
            my_btq.remove_finish(all=True)
            st.rerun()
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
            'Run': False,
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
            elif my_btq.items[line["id"]].is_running():
                my_btq.items[line["id"]].stop()
            elif my_btq.items[line["id"]].is_finish():
                st.session_state.bt_view = ({
                    'id': line["id"],
                    'view': my_btq.items[line["id"]]})
                st.session_state.ed_bt_key += 1
            st.rerun()
        if line["Log"]:
            st.session_state.bt_log = ({
                'id': line["id"],
                'log': my_btq.items[line["id"]].load_log()})
            st.session_state.ed_bt_key += 1
            st.rerun()
        if line["Config"]:
            st.session_state.bt_config = ({
                'id': line["id"],
                'config': my_btq.items[line["id"]].config})
            st.session_state.ed_bt_key += 1
            st.rerun()
        if line["Delete"]:
            my_btq.items[line["id"]].remove()
            st.rerun()
    if "bt_view" in st.session_state:
        if st.button(f':negative_squared_cross_mark: {st.session_state.bt_view["id"]}', key="view_bt_view"):
            del st.session_state.bt_view
            st.rerun()
        view = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
        view.match_item(st.session_state.bt_view["view"])
        if len(view.backtests) > 0:
            view.backtests[0].selected = True
        view.view(only=True)
    if "bt_config" in st.session_state:
        if st.button(f':negative_squared_cross_mark: {st.session_state.bt_config["id"]}', key="view_bt_config"):
            del st.session_state.bt_config
            st.rerun()
        st.code(st.session_state.bt_config["config"])
    if "bt_log" in st.session_state:
        if st.button(f':negative_squared_cross_mark: {st.session_state.bt_log["id"]}', key="view_bt_log"):
            del st.session_state.bt_log
            st.rerun()
        st.code(st.session_state.bt_log["log"])

def bt_compare():
    # Init bt_results
    if "bt_results" in st.session_state:
        bt_results = st.session_state.bt_results
    else:     
        st.session_state.bt_results = BacktestResults(f'{st.session_state.pbdir}/backtests')
        bt_results = st.session_state.bt_results
        bt_results.find_all()
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            if f"setup_table_bt_{bt_results}" in st.session_state:
                del st.session_state[f'setup_table_bt_{bt_results}']
                del st.session_state.backtest_view_keys
                st.rerun()
            if "bt_results" in st.session_state:
                del st.session_state.bt_results
            del st.session_state.bt_compare
            st.rerun()
        if st.button("Queue"):
            st.session_state.bt_queue = True
            del st.session_state.bt_compare
            if "bt_results" in st.session_state:
                del st.session_state.bt_results
            st.rerun()
    bt_results.view()

def bt_import():
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.bt_import
            st.rerun()
    my_bt.import_pbconfigdb()

set_page_config("Backtest")

# Init session state
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    st.switch_page("pbgui.py")
if 'my_bt' in st.session_state:
    my_bt = st.session_state.my_bt
else:
    my_bt = BacktestItem()
    st.session_state.my_bt = my_bt

if "bt_queue" in st.session_state:
    bt_queue()
elif "bt_compare" in st.session_state:
    bt_compare()
elif "bt_import" in st.session_state:
    bt_import()
else:
    bt_add()
