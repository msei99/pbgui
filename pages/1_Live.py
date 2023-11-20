import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from streamlit_autorefresh import st_autorefresh
from pbgui_func import set_page_config, upload_pbconfigdb
from Instance import Instances, Instance
from Backtest import BacktestItem
from PBRun import PBRun
from PBStat import PBStat
import pbgui_help
import pandas as pd
import platform


def bgcolor_positive_or_negative(value):
    bgcolor = "lightcoral" if value < 0 else "lightgreen"
    return f"background-color: {bgcolor};"

def edited():
    st.session_state.edited = True

def select_instance():
    instances = st.session_state.pbgui_instances
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    if "confirm" in st.session_state:
        st.session_state.confirm = st.checkbox(st.session_state.confirm_text)
    # Navigation
    with st.sidebar:
        if st.toggle("PBRun", value=PBRun().is_running(), key="pbrun", help=pbgui_help.pbrun):
            if not PBRun().is_running():
                PBRun().run()
                st.experimental_rerun()
        else:
            if PBRun().is_running():
                PBRun().stop()
                st.experimental_rerun()
        instances.pbrun_log = st.checkbox("PBRun Logfile", value=instances.pbrun_log, key="view_pbrun_log")
        if st.toggle("PBStat", value=PBStat().is_running(), key="pbstat", help=pbgui_help.pbstat):
            if not PBStat().is_running():
                PBStat().run()
                st.experimental_rerun()
        else:
            if PBStat().is_running():
                PBStat().stop()
                st.experimental_rerun()
        instances.pbstat_log = st.checkbox("PBStat Logfile", value=instances.pbstat_log, key="view_pbstat_log")
        if st.button("Add"):
            st.session_state.edit_instance = Instance()
            st.experimental_rerun()
        if not platform.system() == "Windows":
            if st.button("Import"):
                st.session_state.import_instance = True
                st.experimental_rerun()
    if "editor_select_instance" in st.session_state:
        ed = st.session_state["editor_select_instance"]
        for row in ed["edited_rows"]:
            if "View" in ed["edited_rows"][row]:
                st.session_state.view_instance = instances.instances[row]
                if "confirm" in st.session_state:
                    del st.session_state.confirm
                    del st.session_state.confirm_text
                st.experimental_rerun()
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_instance = instances.instances[row]
                if "confirm" in st.session_state:
                    del st.session_state.confirm
                    del st.session_state.confirm_text
                st.experimental_rerun()
            if "Delete" in ed["edited_rows"][row]:
                if not "confirm" in st.session_state:
                    st.session_state.confirm_text = f':red[Delete selected instance ?]'
                    st.session_state.confirm = False
                    st.experimental_rerun()
                elif "confirm" in st.session_state:
                    if st.session_state.confirm:
                        instances.remove(instances.instances[row])
                        del st.session_state.confirm
                        del st.session_state.confirm_text
                        st.experimental_rerun()
    d = []
    wb = 0
    we = 0
    total_upnl = 0
    total_we = 0
    for id, instance in enumerate(instances):
        if any(dic.get('User') == instance.user for dic in d):
            balance = 0
        else:
            balance = instance.balance
        if instance.we > we:
            we = instance.we
        d.append({
            'id': id,
            'View': False,
            'Edit': False,
            'Running': instance.is_running(),
            'User': instance.user,
            'Symbol': instance.symbol,
            'Market_type': instance.market_type,
            'Balance': f'${instance.balance:.2f}',
            'uPnl': instance.upnl,
            'Position': f'{instance.psize}',
            'Price': f'{instance.price}',
            'Entry': f'{instance.entry}',
            'DCA': f'{instance.dca}',
            'Next DCA': f'{instance.next_dca}',
            'Next TP': f'{instance.next_tp}',
            'Wallet Exposure': instance.we,
            'Delete': False,
        })
        if type(balance) == float:
            wb += balance
        total_upnl += instance.upnl
        total_we += instance.we
    if len(instances.instances) > 0:
        total_we = total_we / len(instances.instances)
        if we == 0:
            we = 100
        column_config = {
            "Balance": st.column_config.TextColumn(f'Balance: ${wb:.2f}'),
            "uPnl": st.column_config.TextColumn(f'uPnl: ${total_upnl:.2f}'),
            "Wallet Exposure": st.column_config.ProgressColumn(f'Wallet Exposure: {total_we:.2f} %', format="%.2f %%", max_value=we),
            "id": None}
        df = pd.DataFrame(d)
        sdf = df.style.applymap(bgcolor_positive_or_negative, subset=['uPnl'])
        st.data_editor(data=sdf, width=None, height=(len(instances.instances)+1)*36, use_container_width=True, key="editor_select_instance", hide_index=None, column_order=None, column_config=column_config, on_change = edited, disabled=['id','Running','User','Symbol','Market_type','Balance','uPnl','Position','Price','Entry','DCA','Next DCA','Next TP','Wallet Exposure'])
    if instances.pbrun_log:
        instances.view_log("PBRun")
    if instances.pbstat_log:
        instances.view_log("PBStat")

def view_instance():
    # Init instance
    instance = st.session_state.view_instance
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.view_instance
            st.experimental_rerun()
        if st.button("History"):
            st.session_state.view_history = True
            st.experimental_rerun()
    col_tf, col_auto, col_rec, col_empty = st.columns([3,3,2,10])
    with col_rec:
        st.write("## ")
        st.button(':recycle:',)
    with col_auto:
        refresh = st.selectbox('Interval',['off','5','10','15','30','60'])
        if refresh != "off":
            st_autorefresh(interval=int(refresh)*1000, limit=None, key="refresh_counter")
    with col_tf:
        instance.tf = st.selectbox('Timeframe',instance.exchange.tf,index=instance.exchange.tf.index(instance.tf))
    instance.view_ohlcv()

def view_history():
    # Init instance
    instance = st.session_state.view_instance
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.view_history
            st.experimental_rerun()
    instance.compare_history()

def edit_instance():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Init instance
    instance = st.session_state.edit_instance
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            st.session_state.edit_instance.refresh()
            del st.session_state.edit_instance
            st.experimental_rerun()
        if st.button(":floppy_disk:", help=pbgui_help.instance_save):
            st.session_state.edit_instance.save()
            if st.session_state.edit_instance not in st.session_state.pbgui_instances.instances:
                st.session_state.pbgui_instances.instances.append(st.session_state.edit_instance)
#            del st.session_state.edit_instance
            st.experimental_rerun()
        if st.button("Backtest"):
            st.session_state.my_bt = BacktestItem(instance._config.config)
            st.session_state.my_bt.user = instance.user
            st.session_state.my_bt.symbol = instance.symbol
            st.session_state.my_bt.market_type = instance.market_type
            switch_page("Backtest")
        instance.enabled = st.toggle("enable", value=instance.enabled, key="live_enabled", help=pbgui_help.instance_enable)
        if instance.enabled:
            if st.button("restart", help=pbgui_help.instance_restart):
                st.session_state.edit_instance.save()
                PBRun().restart(instance.user, instance.symbol)
        source_name = st.text_input('pbconfigdb by [Scud](%s)' % "https://pbconfigdb.scud.dedyn.io/", value="PBGUI", max_chars=16, key="name_input", help=pbgui_help.upload_pbguidb)
        if not "error" in st.session_state:
            if st.button("Upload"):
                upload_pbconfigdb(instance._config.config, instance.symbol, source_name)
    instance.edit_base()
    instance.edit_mode()
    col_1, col_2, col_3 = st.columns([1,1,1])
    with col_1:
        with st.session_state.placeholder.expander("Advanced configurations", expanded=False):
            instance.co = st.number_input("COUNTDOWN_OFFSET", min_value=-1, max_value=59, value=instance.co, step=1, format="%d", key="live_co", help=pbgui_help.co)
            instance.leverage = st.number_input("LEVERAGE", min_value=2, max_value=20, value=instance.leverage, step=1, format="%d", key="live_lev", help=pbgui_help.lev)
            instance.assigned_balance = st.number_input("ASSIGNED_BALANCE", key="live_assigned_balance", min_value=0, step=500, value=instance.assigned_balance, help=pbgui_help.assigned_balance)
            instance.price_distance_threshold = round(st.number_input("PRICE_DISTANCE_THRESHOLD", key="live_price_distance_threshold", min_value=0.00, step=0.05, value=instance.price_distance_threshold, help=pbgui_help.price_distance_threshold),2)
            instance.price_precision = round(st.number_input("PRICE_PRECISION_MULTIPLIER", key="live_price_precision", format="%.4f", min_value=0.0000, step=0.0001, value=instance.price_precision, help=pbgui_help.price_precision),4)
            instance.price_step = round(st.number_input("PRICE_STEP_CUSTOM", key="live_price_step", format="%.3f", min_value=0.000, step=0.001, value=instance.price_step, help=pbgui_help.price_step),3)
    instance.edit_config()
    instance.view_log()

def import_instance():
    # Init instance
    instances = st.session_state.pbgui_instances
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.import_instance
            del st.session_state.pbgui_instances
            st.experimental_rerun()
    instances.import_manager()

set_page_config()

# Init session state
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    switch_page("pbgui")

if 'pbgui_instances' not in st.session_state:
    st.session_state.pbgui_instances = Instances()
#instances = st.session_state.pbgui_instances

if 'view_history' in st.session_state:
    view_history()
elif 'view_instance' in st.session_state:
    view_instance()
elif 'edit_instance' in st.session_state:
    edit_instance()
elif 'import_instance' in st.session_state:
    import_instance()
else:
    select_instance()
