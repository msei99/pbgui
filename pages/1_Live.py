import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from streamlit_autorefresh import st_autorefresh
from pbgui_func import set_page_config, upload_pbconfigdb
from Instance import Instances, Instance
from Backtest import BacktestItem
from PBRun import PBRun
from PBStat import PBStat
from PBRemote import PBRemote
import pbgui_help
import pandas as pd
import platform
from time import sleep


def bgcolor_positive_or_negative(value):
    bgcolor = "lightcoral" if value < 0 else "lightgreen"
    return f"background-color: {bgcolor};"

def list_remote():
    # Init
    instances = st.session_state.pbgui_instances
    # Init PBremote
    if 'remote' not in st.session_state:
        st.session_state.remote = PBRemote()
    remote = st.session_state.remote
    # Init PBRemote Toggle
    if "key_pbremote" in st.session_state:
        pbremote_status = remote.is_running()
        if st.session_state.key_pbremote != pbremote_status:
            if st.session_state.key_pbremote:
                remote.run()
            else:
                remote.stop()
    pbremote_status = remote.is_running()
    if not remote.bucket:
        with st.sidebar:
            if st.button(":back:"):
                del st.session_state.list_remote
                del st.session_state.remote
                st.experimental_rerun()
        st.error(remote.error)
        return
    if not "ed_key" in st.session_state:
        st.session_state.ed_key = 0
    ed_key = st.session_state.ed_key
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.experimental_rerun()
        if st.button(":back:"):
            del st.session_state.list_remote
            st.experimental_rerun()
        st.toggle("PBRemote", value=pbremote_status, key="key_pbremote", help=pbgui_help.pbremote)
        instances.pbremote_log = st.checkbox("PBRemote Logfile", value=instances.pbremote_log, key="view_pbremote_log")
        api_sync = []
        for rserver in remote.remote_servers:
            if rserver.is_online():
                color = "green"
                if not rserver.is_api_md5_same(remote.api_md5):
                    api_sync.append(rserver)
            else: color = "red"
            if st.button(f':{color}[{rserver.name}]'):
                if "run_rserver" in st.session_state:
                    del st.session_state.run_rserver
                st.session_state.server = rserver
        if len(api_sync) > 0:
            if st.button(f'Sync API to all'):
                for s in api_sync:
                    s.send_to("sync_api")
        if st.button(f'Start/Stop Instances'):
            if "server" in st.session_state:
                del st.session_state.server
            st.session_state.run_rserver = True
    column_config = {
        "id": None}
    # Sync Servers
    if "server" in st.session_state:
        server = st.session_state.server
        if f'select_instance_{ed_key}' in st.session_state:
            ed = st.session_state[f'select_instance_{ed_key}']
            for row in ed["edited_rows"]:
                if "Sync to local" in ed["edited_rows"][row]:
                    status = instances.is_same(server.instances.find_instance(instances.instances[row].user,instances.instances[row].symbol,instances.instances[row].market_type))
                    if (status == False):
                        server.send_to("copy", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
                        st.session_state.ed_key += 1
                        st.experimental_rerun()
                if "Sync to remote" in ed["edited_rows"][row]:
                    status = server.instances.is_same(instances.instances[row])
                    if (status == False):
                        server.send_to("sync", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
                        st.session_state.ed_key += 1
                        st.experimental_rerun()
                if "Remove" in ed["edited_rows"][row]:
                    status = server.is_running(instances.instances[row].user, instances.instances[row].symbol) or not server.has_instance(instances.instances[row].user, instances.instances[row].symbol)
                    if not status:
                        server.send_to("remove", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
                        st.session_state.ed_key += 1
                        st.experimental_rerun()
        server.instances = Instances(server.name)
        instances.refresh()
        color = "red"
        if server.is_online():
            color = "green"
        st.markdown(f'### Remote Server: :{color}[{server.name}] ({server.rtd}s)')
        sid = []
        if not server.is_api_md5_same(remote.api_md5):
            if st.checkbox(f'Sync API-Keys to {server.name} (Local md5: {remote.api_md5} remote md5: {server.api_md5})',value=False, key=f'sync_api_{ed_key}'):
                server.send_to("sync_api")
                st.session_state.ed_key += 1
                st.experimental_rerun()
        for id, instance in enumerate(instances):
            if server.is_running(instance.user, instance.symbol) or not server.has_instance(instance.user, instance.symbol):
                remove = None
            else:
                remove = False
            sid.append({
                'id': id,
                'User': instance.user,
                'Symbol': instance.symbol,
                'Running': server.is_running(instance.user, instance.symbol),
                'Sync to local': instances.is_same(server.instances.find_instance(instance.user,instance.symbol,instance.market_type)),
                'Sync to remote': server.instances.is_same(instance),
                'Remove': remove,
            })
        st.data_editor(data=sid, width=None, height=36+(len(sid))*35, use_container_width=True, key=f'select_instance_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','User','Symbol','Running'])
    # Start / Stop Instances
    if f'select_run_{ed_key}' in st.session_state:
        ed = st.session_state[f'select_run_{ed_key}']
        for row in ed["edited_rows"]:
            if "Local Start/Stop" in ed["edited_rows"][row]:
                if instances.instances[row].is_running():
                    PBRun().stop_instance(f'{instances.instances[row].user}_{instances.instances[row].symbol}_{instances.instances[row].market_type}')
                    while instances.instances[row].is_running():
                        sleep(1)
                else:
                    lrun = True
                    for rserver in remote.remote_servers:
                        if rserver.is_running(instances.instances[row].user, instances.instances[row].symbol):
                            lrun = None
                    if lrun:
                        PBRun().start_instance(f'{instances.instances[row].user}_{instances.instances[row].symbol}_{instances.instances[row].market_type}')
                        while not instances.instances[row].is_running():
                            sleep(1)
                st.session_state.ed_key += 1
                st.experimental_rerun()
            for rserver in remote.remote_servers:
                if f'{rserver.name} Start/Stop' in ed["edited_rows"][row]:
                    if rserver.is_running(instances.instances[row].user, instances.instances[row].symbol):
                        rserver.send_to("stop", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
                    else:
                        rrun = True
                        if instances.instances[row].is_running():
                            rrun = None
                        for rrserver in remote.remote_servers:
                            if rrserver.is_running(instances.instances[row].user, instances.instances[row].symbol):
                                rrun = None
                        if not rserver.has_instance(instances.instances[row].user, instances.instances[row].symbol):
                            rrun = None
                        if rrun:
                            rserver.send_to("start", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
                    st.session_state.ed_key += 1
                    st.experimental_rerun()
    if "run_rserver" in st.session_state:
        rlist = []
        instances.refresh()
        for id, instance in enumerate(instances):
            lrun = False
            for rserver in remote.remote_servers:
                if rserver.is_running(instance.user, instance.symbol):
                    lrun = None
            if instance.is_running():
                lrun = True
            rid = {
                'id': id,
                'User': instance.user,
                'Symbol': instance.symbol,
                'Local Start/Stop': lrun,
               }
            rrun = False
            for rserver in remote.remote_servers:
                rrun = False
                for rrserver in remote.remote_servers:
                    if rserver.name != rrserver.name and rrserver.is_running(instance.user, instance.symbol):
                        rrun = None
                if not rserver.has_instance(instance.user, instance.symbol):
                    rrun = None
                if lrun:
                    rrun = None
                if rserver.is_running(instance.user, instance.symbol):
                    rrun = True
                rid.update({
                    f'{rserver.name} Start/Stop': rrun,
                })
            rlist.append(rid)
        st.data_editor(data=rlist, width=None, height=36+(len(rlist))*35, use_container_width=True, key=f'select_run_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','Server','Online','RTD','User','Symbol'])
    if instances.pbremote_log:
        instances.view_log("PBRemote")

def select_instance():
    # Init Instances
    if "pbgui_instances" not in st.session_state:
        return
    instances = st.session_state.pbgui_instances
    # Init PBRun Toggle
    if "pbrun" in st.session_state:
        pbrun_status = PBRun().is_running()
        if st.session_state.pbrun != pbrun_status:
            if st.session_state.pbrun:
                PBRun().run()
            else:
                PBRun().stop()
    pbrun_status = PBRun().is_running()
    # Init PBStat Toggle
    if "pbstat" in st.session_state:
        pbstat_status = PBStat().is_running()
        if st.session_state.pbstat != pbstat_status:
            if st.session_state.pbstat:
                PBStat().run()
            else:
                PBStat().stop()
    pbstat_status = PBStat().is_running()
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    if "confirm" in st.session_state:
        st.session_state.confirm = st.checkbox(st.session_state.confirm_text)
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.experimental_rerun()
        st.toggle("PBRun", value=pbrun_status, key="pbrun", help=pbgui_help.pbrun)
        instances.pbrun_log = st.checkbox("PBRun Logfile", value=instances.pbrun_log, key="view_pbrun_log")
        st.toggle("PBStat", value=pbstat_status, key="pbstat", help=pbgui_help.pbstat)
        instances.pbstat_log = st.checkbox("PBStat Logfile", value=instances.pbstat_log, key="view_pbstat_log")
        if st.button("Add"):
            st.session_state.edit_instance = Instance()
            st.experimental_rerun()
        if st.button("Refresh from Disk"):
            del st.session_state.pbgui_instances
            st.experimental_rerun()
        if st.button("Remote"):
            st.session_state.list_remote = True
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
                    st.session_state.confirm_text = f':red[Delete selected instance ({instances.instances[row].user} {instances.instances[row].symbol} {instances.instances[row].market_type})?]'
                    st.session_state.confirm = False
                    st.experimental_rerun()
                elif "confirm" in st.session_state:
                    if st.session_state.confirm:
                        instances.remove(instances.instances[row])
                        PBStat().restart()
                        PBRun().restart_pbrun()
                        PBRemote().restart()
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
        st.data_editor(data=sdf, width=None, height=36+(len(d))*35, use_container_width=True, key="editor_select_instance", hide_index=None, column_order=None, column_config=column_config, disabled=['id','Running','User','Symbol','Market_type','Balance','uPnl','Position','Price','Entry','DCA','Next DCA','Next TP','Wallet Exposure'])
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
        if st.button("Edit"):
            st.session_state.edit_instance = st.session_state.view_instance
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
                PBStat().restart()
                PBRun().restart_pbrun()
                PBRemote().restart()
#            st.experimental_rerun()
        if st.button("Backtest"):
            st.session_state.my_bt = BacktestItem(instance._config.config)
            st.session_state.my_bt.user = instance.user
            st.session_state.my_bt.symbol = instance.symbol
            st.session_state.my_bt.market_type = instance.market_type
            switch_page("Backtest")
        st.toggle("enable", value=instance.enabled, key="live_enable", help=pbgui_help.instance_enable)
        if instance.enabled:
            if st.button("restart", key="live_restart", help=pbgui_help.instance_restart):
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
            st.number_input("COUNTDOWN_OFFSET", min_value=-1, max_value=59, value=instance.co, step=1, format="%d", key="live_co", help=pbgui_help.co)
            st.number_input("LEVERAGE", min_value=2, max_value=20, value=instance.leverage, step=1, format="%d", key="live_leverage", help=pbgui_help.lev)
            st.number_input("ASSIGNED_BALANCE", key="live_assigned_balance", min_value=0, step=500, value=instance.assigned_balance, help=pbgui_help.assigned_balance)
            st.number_input("PRICE_DISTANCE_THRESHOLD", key="live_price_distance_threshold", min_value=0.00, step=0.05, value=instance.price_distance_threshold, help=pbgui_help.price_distance_threshold)
            st.number_input("PRICE_PRECISION_MULTIPLIER", key="live_price_precision", format="%.4f", min_value=0.0000, step=0.0001, value=instance.price_precision, help=pbgui_help.price_precision)
            st.number_input("PRICE_STEP_CUSTOM", key="live_price_step", format="%.3f", min_value=0.000, step=0.001, value=instance.price_step, help=pbgui_help.price_step)
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

if 'view_history' in st.session_state:
    view_history()
elif 'view_instance' in st.session_state:
    view_instance()
elif 'edit_instance' in st.session_state:
    edit_instance()
elif 'import_instance' in st.session_state:
    import_instance()
elif 'list_remote' in st.session_state:
    list_remote()
else:
    select_instance()
