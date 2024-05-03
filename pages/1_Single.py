import streamlit as st
from streamlit_autorefresh import st_autorefresh
from pbgui_func import set_page_config, upload_pbconfigdb
from Instance import Instances, Instance
from Backtest import BacktestItem
from PBRun import PBRun
from PBStat import PBStat
from PBRemote import PBRemote
from datetime import datetime
import pbgui_help
import pandas as pd
import platform
from time import sleep


def bgcolor_positive_or_negative(value):
    bgcolor = "lightcoral" if value < 0 else "lightgreen"
    return f"background-color: {bgcolor};"

def select_instance():
    # Init Instances
    if "pbgui_instances" not in st.session_state:
        return
    instances = st.session_state.pbgui_instances
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    if "confirm" in st.session_state:
        st.session_state.confirm = st.checkbox(st.session_state.confirm_text)
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.rerun()
        if st.button("Add"):
            st.session_state.edit_instance = Instance()
            st.rerun()
        if st.button("Refresh from Disk"):
            del st.session_state.pbgui_instances
            st.rerun()
        if not platform.system() == "Windows":
            if st.button("Import"):
                st.session_state.import_instance = True
                st.rerun()
    if "editor_select_instance" in st.session_state:
        ed = st.session_state["editor_select_instance"]
        for row in ed["edited_rows"]:
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_instance = instances.instances[row]
                if "confirm" in st.session_state:
                    del st.session_state.confirm
                    del st.session_state.confirm_text
                st.rerun()
            if "Delete" in ed["edited_rows"][row]:
                if not "confirm" in st.session_state:
                    st.session_state.confirm_text = f':red[Delete selected instance ({instances.instances[row].user} {instances.instances[row].symbol} {instances.instances[row].market_type})?]'
                    st.session_state.confirm = False
                    st.rerun()
                elif "confirm" in st.session_state:
                    if st.session_state.confirm:
                        start_pbstat = False
                        start_pbrun = False
                        start_pbremote = False
                        if PBStat().is_running():
                            PBStat().stop()
                            start_pbstat = True
                        if PBRun().is_running():
                            PBRun().stop()
                            start_pbrun = True
                        if PBRemote().is_running():
                            PBRemote().stop()
                            start_pbremote = True
                        instances.remove(instances.instances[row])
                        if start_pbstat:
                            PBStat().run()
                        if start_pbrun:
                            PBRun().run()
                        if start_pbremote:
                            PBRemote().run()
                        del st.session_state.confirm
                        del st.session_state.confirm_text
                        st.rerun()
    d = []
    for id, instance in enumerate(instances):
        d.append({
            'id': id,
            'Edit': False,
            'User': instance.user,
            'Symbol': instance.symbol,
            'Market_type': instance.market_type,
            'Enabled On': instance.enabled_on,
            'Delete': False,
        })
    column_config = {
        "id": None}
    st.data_editor(data=d, width=None, height=36+(len(d))*35, use_container_width=True, key="editor_select_instance", hide_index=None, column_order=None, column_config=column_config, disabled=['id','Running','User','Symbol','Market_type','Enabled On'])

def edit_instance():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Init instance
    instance = st.session_state.edit_instance
    # Init PBremote
    if 'remote' not in st.session_state:
        st.session_state.remote = PBRemote()
    remote = st.session_state.remote
    # Init session_state for keys
    if "live_enable" in st.session_state:
        if st.session_state.live_enable != instance.enabled:
            instance.enabled = st.session_state.live_enable
    if "live_co" in st.session_state:
        if st.session_state.live_co != instance.co:
            instance.co = st.session_state.live_co
    if "live_leverage" in st.session_state:
        if st.session_state.live_leverage != instance.leverage:
            instance.leverage = st.session_state.live_leverage
    if "live_assigned_balance" in st.session_state:
        if st.session_state.live_assigned_balance != instance.assigned_balance:
            instance.assigned_balance = st.session_state.live_assigned_balance
    if "live_price_distance_threshold" in st.session_state:
        if round(st.session_state.live_price_distance_threshold,2) != instance.price_distance_threshold:
            instance.price_distance_threshold = round(st.session_state.live_price_distance_threshold,2)
    if "live_price_precision" in st.session_state:
        if round(st.session_state.live_price_precision,4) != instance.price_precision:
            instance.price_precision = round(st.session_state.live_price_precision,4)
    if "live_price_step" in st.session_state:
        if round(st.session_state.live_price_step,3) != instance.price_step:
            instance.price_step = round(st.session_state.live_price_step,3)
    if "edit_instance_pbshare_grid" in st.session_state:
        if st.session_state.edit_instance_pbshare_grid != instance.pbshare_grid:
            instance.pbshare_grid = st.session_state.edit_instance_pbshare_grid
    if "edit_instance_enabled_on" in st.session_state:
        if st.session_state.edit_instance_enabled_on != instance.enabled_on:
            instance.enabled_on = st.session_state.edit_instance_enabled_on
    if "edit_instance_version" in st.session_state:
        if st.session_state.edit_instance_version != instance.version:
            instance.version = st.session_state.edit_instance_version
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            st.session_state.edit_instance.refresh()
            del st.session_state.edit_instance
            st.rerun()
        if st.button(":floppy_disk:", help=pbgui_help.instance_save):
            st.session_state.edit_instance.save()
            if st.session_state.edit_instance not in st.session_state.pbgui_instances.instances:
                st.session_state.pbgui_instances.instances.append(st.session_state.edit_instance)
                PBStat().restart()
                PBRun().restart_pbrun()
                PBRemote().restart()
#            st.rerun()
        if st.button("Activate"):
            remote.local_run.activate(f'{instance.user}_{instance.symbol}_{instance.market_type}', False)
        if st.button("Backtest"):
            st.session_state.my_bt = BacktestItem(instance._config.config)
            st.session_state.my_bt.user = instance.user
            st.session_state.my_bt.symbol = instance.symbol
            st.session_state.my_bt.market_type = instance.market_type
            st.switch_page("pages/3_Backtest.py")
        # st.toggle("enable", value=instance.enabled, key="live_enable", help=pbgui_help.instance_enable)
        # if instance.enabled:
        #     if st.button("restart", key="live_restart", help=pbgui_help.instance_restart):
        #         st.session_state.edit_instance.save()
        #         PBRun().restart(instance.user, instance.symbol)
        source_name = st.text_input('pbconfigdb by [Scud](%s)' % "https://pbconfigdb.scud.dedyn.io/", value="PBGUI", max_chars=16, key="name_input", help=pbgui_help.upload_pbguidb)
        if not "error" in st.session_state:
            if st.button("Upload"):
                upload_pbconfigdb(instance._config.config, instance.symbol, source_name)
    instance.edit_base()
    instance.edit_mode()
    col_1, col_2, col_3 = st.columns([1,1,1])
    with col_1:
        with st.session_state.placeholder.expander("Advanced configurations", expanded=False):
            st.number_input("COUNTDOWN_OFFSET", min_value=-1, max_value=59, value=instance.co, step=1, format="%d", key="live_co", help=pbgui_help.co)
            st.number_input("LEVERAGE", min_value=2, max_value=20, value=instance.leverage, step=1, format="%d", key="live_leverage", help=pbgui_help.lev)
            st.number_input("ASSIGNED_BALANCE", key="live_assigned_balance", min_value=0, step=500, value=instance.assigned_balance, help=pbgui_help.assigned_balance)
            st.number_input("PRICE_DISTANCE_THRESHOLD", key="live_price_distance_threshold", min_value=0.00, step=0.05, value=instance.price_distance_threshold, help=pbgui_help.price_distance_threshold)
            st.number_input("PRICE_PRECISION_MULTIPLIER", key="live_price_precision", format="%.4f", min_value=0.0000, step=0.0001, value=instance.price_precision, help=pbgui_help.price_precision)
            st.number_input("PRICE_STEP_CUSTOM", key="live_price_step", format="%.3f", min_value=0.000, step=0.001, value=instance.price_step, help=pbgui_help.price_step)
    instance.edit_config()
    if instance.preview_grid:
        instance.view_grid()
    col_1, col_2, col_3 = st.columns([1,1,1])
    with col_1:
        if instance.multi:
            enabled_on = [instance.enabled_on]
            st.selectbox('Enabled on multi',enabled_on, key="edit_instance_enabled_on", disabled=True)
        else:
            enabled_on = ["disabled",remote.name] + remote.list()
            enabled_on_index = enabled_on.index(instance.enabled_on)
            st.selectbox('Enabled on',enabled_on, index = enabled_on_index, key="edit_instance_enabled_on")
    with col_2:
        st.number_input("config version", min_value=instance.version, value=instance.version, step=1, format="%.d", key="edit_instance_version", help=pbgui_help.config_version)
    with col_3:
        st.toggle("PBShare Grid", value=instance.pbshare_grid, help=pbgui_help.pbshare_grid, key="edit_instance_pbshare_grid")
    instance.view_log()

def import_instance():
    # Init instance
    instances = st.session_state.pbgui_instances
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.import_instance
            del st.session_state.pbgui_instances
            st.rerun()
    instances.import_manager()

set_page_config()

# Init session state
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    st.switch_page("pbgui.py")
if 'pbgui_instances' not in st.session_state:
    st.session_state.pbgui_instances = Instances()

elif 'edit_instance' in st.session_state:
    edit_instance()
elif 'import_instance' in st.session_state:
    import_instance()
else:
    select_instance()
