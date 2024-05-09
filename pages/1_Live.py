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

# def list_remote():
#     # Init
#     instances = st.session_state.pbgui_instances
#     # # Init PBremote
#     # if 'remote' not in st.session_state:
#     #     st.session_state.remote = PBRemote()
#     remote = st.session_state.pbremote
#     if not remote.bucket:
#         with st.sidebar:
#             if st.button(":back:"):
#                 del st.session_state.list_remote
#                 del st.session_state.remote
#                 st.rerun()
#         st.error(remote.error)
#         return
#     if not "ed_key" in st.session_state:
#         st.session_state.ed_key = 0
#     ed_key = st.session_state.ed_key
#     # Navigation
#     with st.sidebar:
#         if st.button(":recycle:"):
#             st.rerun()
#         if st.button(":back:"):
#             del st.session_state.list_remote
#             st.rerun()
#         # api_sync = []
#         # for rserver in remote.remote_servers:
#         #     if rserver.is_online():
#         #         color = "green"
#         #         if not rserver.is_api_md5_same(remote.api_md5):
#         #             api_sync.append(rserver)
#         #     else: color = "red"
#         #     if st.button(f':{color}[{rserver.name}]'):
#         #         if "run_rserver" in st.session_state:
#         #             del st.session_state.run_rserver
#         #         st.session_state.server = rserver
#         # if len(api_sync) > 0:
#         #     if st.button(f'Sync API to all'):
#         #         for s in api_sync:
#         #             s.send_to("sync_api")
#         if st.button(f'Start/Stop Instances'):
#             # if "server" in st.session_state:
#             #     del st.session_state.server
#             st.session_state.run_rserver = True
#     column_config = {
#         "id": None}
#     # # Sync Servers
#     # if "server" in st.session_state:
#     #     server = st.session_state.server
#     #     if f'select_instance_{ed_key}' in st.session_state:
#     #         ed = st.session_state[f'select_instance_{ed_key}']
#     #         for row in ed["edited_rows"]:
#     #             if row > len(instances.instances)-1:
#     #                 rpos = st.session_state.instances_not_local[row - len(instances.instances)]
#     #                 user = server.instances.instances[rpos].user
#     #                 symbol = server.instances.instances[rpos].symbol
#     #                 market_type = server.instances.instances[rpos].market_type
#     #                 if "Sync to local" in ed["edited_rows"][row]:
#     #                     server.send_to("copy", user, symbol, market_type)
#     #                     instances.add_wait(f'{user}_{symbol}_{market_type}')
#     #                 elif "Remove" in ed["edited_rows"][row]:        
#     #                     if not server.is_running(user, symbol):
#     #                         server.send_to("remove", user, symbol, market_type)
#     #                 st.session_state.ed_key += 1
#     #                 st.rerun()
#     #             if "Sync to local" in ed["edited_rows"][row]:
#     #                 status = instances.is_same(server.instances.find_instance(instances.instances[row].user,instances.instances[row].symbol,instances.instances[row].market_type))
#     #                 if (status == False):
#     #                     server.send_to("copy", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
#     #                     st.session_state.ed_key += 1
#     #                     st.rerun()
#     #             if "Sync to remote" in ed["edited_rows"][row]:
#     #                 status = server.instances.is_same(instances.instances[row])
#     #                 if (status == False):
#     #                     server.send_to("sync", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
#     #                     st.session_state.ed_key += 1
#     #                     st.rerun()
#     #             if "Remove" in ed["edited_rows"][row]:
#     #                 status = server.is_running(instances.instances[row].user, instances.instances[row].symbol) or not server.has_instance(instances.instances[row].user, instances.instances[row].symbol)
#     #                 if not status:
#     #                     server.send_to("remove", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
#     #                     st.session_state.ed_key += 1
#     #                     st.rerun()
#     #     server.instances = Instances(server.name)
#     #     instances.refresh()
#     #     color = "red"
#     #     if server.is_online():
#     #         color = "green"
#     #     st.markdown(f'### Remote Server: :{color}[{server.name}] ({server.rtd}s)')
#     #     col_1, col_2 = st.columns([1,1])
#     #     with col_1:
#     #         mem_total = int(server.mem[0] / 1024 / 1024)
#     #         mem_free = int(server.mem[1] / 1024 / 1024)
#     #         mem_used = int(server.mem[3] / 1024 / 1024)
#     #         mem_usage = int(server.mem[2])
#     #         st.progress(mem_usage, text=f'### Memory Free: :green[{mem_free}] MB  |  Used: :red[{mem_used}] MB  |  Total: :blue[{mem_total}] MB')
#     #         disk_total = int(server.disk[0] / 1024 / 1024)
#     #         disk_used = int(server.disk[1] / 1024 / 1024)
#     #         disk_free = int(server.disk[2] / 1024 / 1024)
#     #         disk_usage = int(server.disk[3])
#     #         st.progress(disk_usage, text=f'### Disk Free: :green[{disk_free}] MB  |  Used: :red[{disk_used}] MB  |  Total: :blue[{disk_total}] MB')
#     #     with col_2:
#     #         swap_total = int(server.swap[0] / 1024 / 1024)
#     #         swap_used = int(server.swap[1] / 1024 / 1024)
#     #         swap_free = int(server.swap[2] / 1024 / 1024)
#     #         swap_usage = min(int(server.swap[3]),100)
#     #         st.progress(swap_usage, text=f'### Swap Free: :green[{swap_free}] MB  |  Used: :red[{swap_used}] MB  |  Total: :blue[{swap_total}] MB')
#     #         boot = datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S")
#     #         if server.cpu > 90:
#     #             cpu_color = "red"
#     #         elif server.cpu < 50:
#     #             cpu_color = "green"
#     #         else:
#     #             cpu_color = "yellow"
#     #         st.markdown(f"##### CPU utilization: :{cpu_color}[{server.cpu}] %  |  System boot: :blue[{boot}]")
#     #     sid = []
#     #     if not server.is_api_md5_same(remote.api_md5):
#     #         if st.checkbox(f'Sync API-Keys to {server.name} (Local md5: {remote.api_md5} remote md5: {server.api_md5})',value=False, key=f'sync_api_{ed_key}'):
#     #             server.send_to("sync_api")
#     #             st.session_state.ed_key += 1
#     #             st.rerun()
#     #     for id, instance in enumerate(instances):
#     #         if server.is_running(instance.user, instance.symbol) or not server.has_instance(instance.user, instance.symbol):
#     #             remove = None
#     #         else:
#     #             remove = False
#     #         sid.append({
#     #             'id': id,
#     #             'where': "local",
#     #             'User': instance.user,
#     #             'Symbol': instance.symbol,
#     #             'Running': server.is_running(instance.user, instance.symbol),
#     #             'Sync to local': instances.is_same(server.instances.find_instance(instance.user,instance.symbol,instance.market_type)),
#     #             'Sync to remote': server.instances.is_same(instance),
#     #             'Remove': remove,
#     #         })
#     #     st.session_state.instances_not_local = []
#     #     for id, rinstance in enumerate(server.instances):
#     #         finstance = instances.find_instance(rinstance.user, rinstance.symbol, rinstance.market_type)
#     #         if not finstance:
#     #             st.session_state.instances_not_local.append(id)
#     #             if server.is_running(rinstance.user, rinstance.symbol):
#     #                 remove = None
#     #             else:
#     #                 remove = False
#     #             sid.append({
#     #                 'id': id,
#     #                 'where': server.name,
#     #                 'User': rinstance.user,
#     #                 'Symbol': rinstance.symbol,
#     #                 'Running': server.is_running(rinstance.user, rinstance.symbol),
#     #                 'Sync to local': False,
#     #                 'Sync to remote': None,
#     #                 'Remove': remove,
#     #             })
#     #     st.data_editor(data=sid, width=None, height=36+(len(sid))*35, use_container_width=True, key=f'select_instance_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','User','Symbol','Running'])
#     # Start / Stop Instances
#     if f'select_run_{ed_key}' in st.session_state:
#         ed = st.session_state[f'select_run_{ed_key}']
#         for row in ed["edited_rows"]:
#             if "Local Start/Stop" in ed["edited_rows"][row]:
#                 if instances.instances[row].is_running():
#                     PBRun().stop_instance(f'{instances.instances[row].user}_{instances.instances[row].symbol}_{instances.instances[row].market_type}')
#                     while instances.instances[row].is_running():
#                         sleep(1)
#                 else:
#                     lrun = True
#                     for rserver in remote.remote_servers:
#                         if rserver.is_running(instances.instances[row].user, instances.instances[row].symbol):
#                             lrun = None
#                     if lrun:
#                         PBRun().start_instance(f'{instances.instances[row].user}_{instances.instances[row].symbol}_{instances.instances[row].market_type}')
#                         while not instances.instances[row].is_running():
#                             sleep(1)
#                 st.session_state.ed_key += 1
#                 st.rerun()
#             for rserver in remote.remote_servers:
#                 if f'{rserver.name} Start/Stop' in ed["edited_rows"][row]:
#                     if rserver.is_running(instances.instances[row].user, instances.instances[row].symbol):
#                         rserver.send_to("stop", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
#                     else:
#                         rrun = True
#                         if instances.instances[row].is_running():
#                             rrun = None
#                         for rrserver in remote.remote_servers:
#                             if rrserver.is_running(instances.instances[row].user, instances.instances[row].symbol):
#                                 rrun = None
#                         if not rserver.has_instance(instances.instances[row].user, instances.instances[row].symbol):
#                             rrun = None
#                         if rrun:
#                             rserver.send_to("start", instances.instances[row].user, instances.instances[row].symbol, instances.instances[row].market_type)
#                     st.session_state.ed_key += 1
#                     st.rerun()
#     if "run_rserver" in st.session_state:
#         rlist = []
#         instances.refresh()
#         for id, instance in enumerate(instances):
#             lrun = False
#             for rserver in remote.remote_servers:
#                 if rserver.is_running(instance.user, instance.symbol):
#                     lrun = None
# #            if instance.is_running():
#             if instance.enabled:
#                 lrun = True
#             rid = {
#                 'id': id,
#                 'User': instance.user,
#                 'Symbol': instance.symbol,
#                 'Local Start/Stop': lrun,
#                }
#             rrun = False
#             for rserver in remote.remote_servers:
#                 rrun = False
#                 for rrserver in remote.remote_servers:
#                     if rserver.name != rrserver.name and rrserver.is_running(instance.user, instance.symbol):
#                         rrun = None
#                 if not rserver.has_instance(instance.user, instance.symbol):
#                     rrun = None
#                 if lrun:
#                     rrun = None
#                 if rserver.is_running(instance.user, instance.symbol):
#                     rrun = True
#                 rid.update({
#                     f'{rserver.name} Start/Stop': rrun,
#                 })
#             rlist.append(rid)
#         for rserver in remote.remote_servers:
#             column_config.update({
#                 f'{rserver.name} Start/Stop': st.column_config.CheckboxColumn(f'{rserver.name} Start/Stop', default=None),
#             })
#         st.data_editor(data=rlist, width=None, height=36+(len(rlist))*35, use_container_width=True, key=f'select_run_{ed_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','Server','Online','RTD','User','Symbol'])
#     # if instances.pbremote_log:
#     #     instances.view_log("PBRemote")

#@st.cache_data(experimental_allow_widgets=True)
def select_instance():
    # Init Instances
    instances = st.session_state.pbgui_instances
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.rerun()
        if st.button("Refresh from Disk"):
            del st.session_state.pbgui_instances
            st.rerun()
        if st.button("Remote"):
            st.session_state.list_remote = True
            st.rerun()
    if "editor_select_instance" in st.session_state:
        ed = st.session_state["editor_select_instance"]
        for row in ed["edited_rows"]:
            if "View" in ed["edited_rows"][row]:
                st.session_state.view_instance = instances.instances[row]
                if "confirm" in st.session_state:
                    del st.session_state.confirm
                    del st.session_state.confirm_text
                st.rerun()
            if "History" in ed["edited_rows"][row]:
                st.session_state.view_instance = instances.instances[row]
                st.session_state.view_history = True
                if "confirm" in st.session_state:
                    del st.session_state.confirm
                    del st.session_state.confirm_text
                st.rerun()
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
            'History': False,
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

def view_instance():
    # Init instance
    instance = st.session_state.view_instance
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.view_instance
            st.rerun()
        if st.button("Edit"):
            st.session_state.edit_instance = st.session_state.view_instance
            del st.session_state.view_instance
            st.rerun()
        if st.button("History"):
            st.session_state.view_history = True
            st.rerun()
    col_tf, col_auto, col_rec, col_empty = st.columns([3,3,2,10])
    with col_rec:
        st.write("## ")
        st.button(':recycle:',)
    with col_auto:
        refresh = st.selectbox('Interval',['off','5','10','15','30','60'])
        if refresh != "off":
            st_autorefresh(interval=int(refresh)*1000, limit=None, key="refresh_counter")
    with col_tf:
        if "key_live_tf" in st.session_state:
            instance.tf = st.session_state.key_live_tf
        st.selectbox('Timeframe',instance.exchange.tf,index=instance.exchange.tf.index(instance.tf), key="key_live_tf")
    instance.view_ohlcv()

def view_history():
    # Init instance
    instance = st.session_state.view_instance
    # Navigation
    with st.sidebar:
        if st.button(":top:"):
            del st.session_state.view_history
            del st.session_state.view_instance
            st.rerun()
        if st.button("View"):
            del st.session_state.view_history
            st.rerun()
    instance.compare_history()

set_page_config()

# Init session state
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    st.switch_page("pbgui.py")
# Init Services and Instances
if 'services' not in st.session_state:
    st.switch_page("pbgui.py")

if 'view_history' in st.session_state:
    view_history()
elif 'view_instance' in st.session_state:
    view_instance()
# elif 'list_remote' in st.session_state:
#     list_remote()
else:
    select_instance()
