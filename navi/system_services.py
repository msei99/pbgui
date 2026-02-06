import streamlit as st
from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    is_authenticted,
    error_popup,
    info_popup,
    get_navi_paths,
    sync_api,
    PBGDIR,
    render_header_with_guide,
)
from pbgui_purefunc import load_ini, save_ini
import json
import time
from datetime import datetime
import html as _html
from pathlib import Path
import pbgui_help
from Monitor import Monitor
from logging_view import view_log_filtered
from Exchange import MAX_PRIVATE_WS_GLOBAL, Exchanges


def _docs_index(lang: str) -> list[tuple[str, str]]:
    ln = str(lang or "EN").strip().upper()
    folder = "help_de" if ln == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for p in sorted(docs_dir.glob("*.md")):
        label = p.name
        try:
            with open(p, "r", encoding="utf-8") as f:
                first = f.readline().strip()
            if first.startswith("#"):
                label = first.lstrip("#").strip() or p.name
        except Exception:
            label = p.name
        out.append((label, str(p)))
    return out


def _read_markdown(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Failed to read docs: {e}"


@st.dialog("Help & Tutorials", width="large")
def _help_modal(default_topic: str = "PBData"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="pbdata_help_lang")
    docs = _docs_index(str(lang))
    if not docs:
        st.info("No help docs found.")
        return

    labels = [d[0] for d in docs]
    default_index = 0
    try:
        target = str(default_topic or "").strip().lower()
        if target:
            for i, lbl in enumerate(labels):
                if target in str(lbl).lower():
                    default_index = i
                    break
    except Exception:
        default_index = 0

    sel = st.selectbox(
        "Select Topic",
        options=list(range(len(labels))),
        format_func=lambda i: labels[int(i)],
        index=int(default_index),
        key="pbdata_help_sel",
    )
    path = docs[int(sel)][1]
    md = _read_markdown(path)
    st.markdown(md, unsafe_allow_html=True)
    try:
        base = str(st.get_option("server.baseUrlPath") or "").strip("/")
        prefix = f"/{base}" if base else ""
        st.markdown(
            f"<a href='{prefix}/help' target='_blank'>Open full Help page in new tab</a>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass


def pbrun_overview():
    pbrun = st.session_state.pbrun
    pbrun_status = pbrun.is_running()
    if "service_pbrun" in st.session_state:
        if st.session_state.service_pbrun != pbrun_status:
            pbrun_status = st.session_state.service_pbrun
    st.toggle("PBRun", value=pbrun_status, key="service_pbrun", help=pbgui_help.pbrun)
    if pbrun_status:
        pbrun.run()
        pbrun_icon = '✅'
    else:
        pbrun.stop()
        pbrun_icon = '❌'
    st.metric(label="PBRun", value=pbrun_icon)

def pbremote_overview():
    pbremote = st.session_state.pbremote
    pbremote_status = pbremote.is_running()
    if "service_pbremote" in st.session_state:
        if st.session_state.service_pbremote != pbremote_status:
            pbremote_status = st.session_state.service_pbremote
    st.toggle("PBRemote", value=pbremote_status, key="service_pbremote", help=pbgui_help.pbremote)
    if pbremote_status:
        pbremote.run()
        pbremote_icon = '✅'
    else:
        pbremote.stop()
        pbremote_icon = '❌'
    st.metric(label="PBRemote", value=pbremote_icon)

def pbmon_overview():
    pbmon = st.session_state.pbmon
    pbmon_status = pbmon.is_running()
    if "service_pbmon" in st.session_state:
        if st.session_state.service_pbmon != pbmon_status:
            pbmon_status = st.session_state.service_pbmon
    st.toggle("PBMon", value=pbmon_status, key="service_pbmon", help=pbgui_help.pbmon)
    if pbmon_status:
        pbmon.run()
        pbmon_icon = '✅'
    else:
        pbmon.stop()
        pbmon_icon = '❌'
    st.metric(label="PBMon", value=pbmon_icon)

def pbstat_overview():
    pbstat = st.session_state.pbstat
    pbstat_status = pbstat.is_running()
    if "service_pbstat" in st.session_state:
        if st.session_state.service_pbstat != pbstat_status:
            pbstat_status = st.session_state.service_pbstat
    st.toggle("PBStat", value=pbstat_status, key="service_pbstat", help=pbgui_help.pbstat)
    if pbstat_status:
        pbstat.run()
        pbstat_icon = '✅'
    else:
        pbstat.stop()
        pbstat_icon = '❌'
    st.metric(label="PBStat", value=pbstat_icon)

def pbdata_overview():
    pbdata = st.session_state.pbdata
    pbdata_status = pbdata.is_running()
    if "service_pbdata" in st.session_state:
        if st.session_state.service_pbdata != pbdata_status:
            pbdata_status = st.session_state.service_pbdata
    st.toggle("PBData", value=pbdata_status, key="service_pbdata", help=pbgui_help.pbdata)
    if pbdata_status:
        pbdata.run()
        pbdata_icon = '✅'
    else:
        pbdata.stop()
        pbdata_icon = '❌'
    st.metric(label="PBData", value=pbdata_icon)

def pbcoindata_overview():
    pbcoindata = st.session_state.pbcoindata
    pbcoindata_status = pbcoindata.is_running()
    if "service_pbcoindata" in st.session_state:
        if st.session_state.service_pbcoindata != pbcoindata_status:
            pbcoindata_status = st.session_state.service_pbcoindata
    st.toggle("PBCoinData", value=pbcoindata_status, key="service_pbcoindata", help=pbgui_help.pbcoindata)
    if pbcoindata_status:
        pbcoindata.run()
        pbcoindata_icon = '✅'
    else:
        pbcoindata.stop()
        pbcoindata_icon = '❌'
    st.metric(label="PBCoinData", value=pbcoindata_icon)
    
def overview():
    col_1, col_2, col_3, col_4, col_5, col_6 = st.columns([1,1,1,1,1,1])
    with col_1:
        pbrun_overview()
        if st.button("Show Details", key="button_pbrun_details"):
            st.session_state.pbrun_details = True
            st.rerun()
    with col_2:
        pbremote_overview()
        if st.button("Show Details", key="button_pbremote_details"):
            st.session_state.pbremote_details = True
            st.rerun()
    with col_3:
        pbmon_overview()
        if st.button("Show Details", key="button_pbmon_details"):
            st.session_state.pbmon_details = True
            st.rerun()
    with col_4:
        pbstat_overview()
        if st.button("Show Details", key="button_pbstat_details"):
            st.session_state.pbstat_details = True
            st.rerun()
    with col_5:
        pbdata_overview()
        if st.button("Show Details", key="button_pbdata_details"):
            st.session_state.pbdata_details = True
            st.rerun()
    with col_6:
        pbcoindata_overview()
        if st.button("Show Details", key="button_pbcoindata_details"):
            st.session_state.pbcoindata_details = True
            st.rerun()

def pbrun_details():
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbrun_back"):
            del st.session_state.pbrun_details
            st.rerun()
    st.subheader("PBRun Details")
    pbrun_overview()
    if st.checkbox("Show logfile", key="pbrun_log"):
        st.session_state.pbgui_instances.view_log("PBRun")

def pbremote_details():
    # Init PBRemote
    pbremote = st.session_state.pbremote
    # Init Monitor
    if "monitor" not in st.session_state:
        st.session_state.monitor = Monitor()
    monitor = st.session_state.monitor
    # Init from session_state keys
    if "pbremote_bucket" in st.session_state:
        if st.session_state.pbremote_bucket != pbremote.bucket:
            pbremote.bucket = st.session_state.pbremote_bucket
    # Navigation
    with st.sidebar:
        col1, col2, col3, col4 = st.columns([1, 1, 1 ,1])
        with col1:
            if st.button(":material/refresh:"):
                pbremote.update_remote_servers()
                monitor.d_v7 = []
                monitor.d_multi = []
                monitor.d_single = []
                st.rerun()
        with col2:
            if st.button(":material/home:"):
                del st.session_state.pbremote_details
                st.rerun()
        with col3:
            if st.button(":material/save:"):
                pbremote.save_config()
        with col4:
            if st.button(":material/edit:"):
                st.session_state.monitor_edit = True
                st.rerun()
        st.markdown("""---""")
        st.markdown("Remote Servers")
        api_sync = []
        if st.button(f'View All Instances'):
            if "server" in st.session_state:
                del st.session_state.server
            monitor.d_v7 = []
            monitor.d_multi = []
            monitor.d_single = []
            monitor.servers = st.session_state.pbremote.remote_servers
            st.rerun()
        for rserver in sorted(st.session_state.pbremote.remote_servers, key=lambda s: s.name):
            if rserver.is_online():
                color = "green"
                if not rserver.is_api_md5_same(pbremote.api_md5):
                    api_sync.append(rserver)
            else: color = "red"
            col1, col2 = st.columns([3, 1])
            with col1:
                if st.button(f':{color}[{rserver.name}]'):
                    monitor.d_v7 = []
                    monitor.d_multi = []
                    monitor.d_single = []
                    st.session_state.server = rserver
            with col2:
                if color == "red":
                    if st.button(":material/delete:", key=f"delete_{rserver.name}"):
                        rserver.delete_server()
                        pbremote.update_remote_servers()
                        st.rerun()
        sync_api()
                
    st.subheader("PBRemote Details")
    pbremote_overview()
    if pbremote.bucket:
        if pbremote.bucket in pbremote.buckets:
            buckets_index = pbremote.buckets.index(pbremote.bucket)
        else: buckets_index = 0
        if "bucket_config" not in st.session_state:
            st.session_state.bucket_config = pbremote.fetch_bucket_config()
    else: buckets_index = 0
    if st.button("Add bucket", key="pbremote_bucket_add"):
        pbremote.bucket = None
        pbremote.bucket_region = None
        pbremote.bucket_endpoint = None
        pbremote.bucket_access_key_id = None
        pbremote.bucket_secret_access_key = None
        st.session_state.edit_bucket = True
        st.rerun()
    if pbremote.buckets:
        col1, col2 = st.columns([1, 1], vertical_alignment='bottom')
        with col1:
            st.selectbox('Select bucket',pbremote.buckets, index = buckets_index, key="pbremote_bucket", help=pbgui_help.pbremote_bucket)
        with col2:
            if st.button("Edit", key="pbremote_bucket_edit"):
                bucket_config = pbremote.fetch_bucket_config()
                if bucket_config:
                    st.session_state.edit_bucket = True
                    st.rerun()
    else:
        if pbremote.rclone_installed:
            st.write("No bucket found. Please configure rclone by using the 'Add bucket' button.")
        else:
            st.write("rclone not installed. Please install rclone.")
            st.info("Go to VPS Manager, select your local system and install rclone.")
    if st.checkbox("Show logfile", key="pbremote_log"):
        st.session_state.pbgui_instances.view_log("PBRemote")
    if len(api_sync) > 0:
        api_sync_list = []
        for api in api_sync:
            api_sync_list.append(api.name)
        st.subheader("API not in sync with remote servers:")
        st.write(f"{api_sync_list}")
    if "server" in st.session_state:
        monitor.server = st.session_state.server
        monitor.view_server()
        monitor.servers = []
        monitor.servers.append(monitor.server)
        monitor.view_server_instances()
    elif monitor.servers:
        monitor.view_server_instances()
    else:
        st.info("Please select a remote server from the sidebar to view details.")

def edit_bucket():
    # Init PBRemote
    pbremote = st.session_state.pbremote
    # Init keys from session_state
    if "pbremote_bucket_name" in st.session_state:
        if st.session_state.pbremote_bucket_name + ":" != pbremote.bucket:
            pbremote.bucket = st.session_state.pbremote_bucket_name + ":"
    if "pbremote_bucket_region" in st.session_state:
        if st.session_state.pbremote_bucket_region != pbremote.bucket_region:
            pbremote.bucket_region = st.session_state.pbremote_bucket_region
    if "pbremote_bucket_endpoint" in st.session_state:
        if st.session_state.pbremote_bucket_endpoint != pbremote.bucket_endpoint:
            pbremote.bucket_endpoint = st.session_state.pbremote_bucket_endpoint
    if "pbremote_bucket_access_key" in st.session_state:
        if st.session_state.pbremote_bucket_access_key != pbremote.bucket_access_key_id:
            pbremote.bucket_access_key_id = st.session_state.pbremote_bucket_access_key
    if "pbremote_bucket_secret_key" in st.session_state:
        if st.session_state.pbremote_bucket_secret_key != pbremote.bucket_secret_access_key:
            pbremote.bucket_secret_access_key = st.session_state.pbremote_bucket_secret_key
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_edit_bucket_back"):
            del st.session_state.edit_bucket
            st.rerun()
        if st.button(":material/save:"):
            ok, result = pbremote.save_bucket_config()
            if ok:
                result_popup("Bucket saved", result)
                pbremote.fetch_buckets()
            else:
                error_popup(result)
        if st.button(":material/delete:"):
            ok, result = pbremote.delete_bucket()
            if ok:
                result_popup("Bucket deleted", result)
                pbremote.fetch_buckets()
                del st.session_state.edit_bucket
            else:
                error_popup(result)

    # Instructions and link to Synology        
    st.write(
        "1. Get your free 15GB account at [Synology C2](https://c2.synology.com/en-uk/object-storage/overview).\n"
        "2. Create a bucket in your C2 Object Storage.\n"
        "3. Fill in the details below.\n"
        "4. Save the config.\n"
        "5. Test the connection.\n"
        "6. Go back and save the settings.\n"
    )
   
    # Display
    if pbremote.bucket:
        bucket_name = pbremote.bucket[0:-1]
    else:
        bucket_name = ""
    st.text_input("Bucket name", value=bucket_name, key="pbremote_bucket_name")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        st.text_input("region", value=pbremote.bucket_region, key="pbremote_bucket_region")
    with col2:
        st.text_input("endpoint", value=pbremote.bucket_endpoint, key="pbremote_bucket_endpoint")
    with col3:
        st.text_input("access_key_id", value=pbremote.bucket_access_key_id, key="pbremote_bucket_access_key")
    with col4:
        st.text_input("secret_access_key", value=pbremote.bucket_secret_access_key, type="password", key="pbremote_bucket_secret_key")
    if st.button("Test Connection"):
        ok, result = pbremote.test_bucket()
        if ok:
            result_popup("Connection successful", result)
        else:
            error_popup(result)
    st.info("Save your config before testing the connection.")

@st.dialog("Info", width="large")
def result_popup(message, result):
    st.info(f'{message}', icon="✅")
    with st.container(height=1200):
        st.text(result)
    if st.button(":green[OK]"):
        st.rerun()

def pbmon_details():
    pbmon = st.session_state.pbmon
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbmon_back"):
            del st.session_state.pbmon_details
            st.rerun()
    st.subheader("PBMon Details")
    pbmon_overview()

    if "pbmon_telegram_token" in st.session_state:
        if st.session_state.pbmon_telegram_token != pbmon.telegram_token:
            pbmon.telegram_token = st.session_state.pbmon_telegram_token
    else:
        st.session_state.pbmon_telegram_token = pbmon.telegram_token

    if "pbmon_telegram_chat_id" in st.session_state:
        if st.session_state.pbmon_telegram_chat_id != pbmon.telegram_chat_id:
            pbmon.telegram_chat_id = st.session_state.pbmon_telegram_chat_id
    else:
        st.session_state.pbmon_telegram_chat_id = pbmon.telegram_chat_id

    st.text_input("Telegram Bot Token", type="password", key="pbmon_telegram_token", help=pbgui_help.pbmon_telegram_token)
    st.text_input("Telegram Chat ID", key="pbmon_telegram_chat_id", help=pbgui_help.pbmon_telegram_chat_id)

    if st.checkbox("Show logfile", key="pbmon_log"):
        st.session_state.pbgui_instances.view_log("PBMon")

def pbstat_details():
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbstat_back"):
            del st.session_state.pbstat_details
            st.rerun()
    st.subheader("PBStat Details")
    pbstat_overview()
    if st.checkbox("Show logfile", key="pbstat_log"):
        st.session_state.pbgui_instances.view_log("PBStat")

def pbdata_details():
    pbdata = st.session_state.pbdata
    # Callback for refresh button to avoid manual save/restore of session flags
    def _pbdata_refresh_callback():
        try:
            st.session_state.users.load()
        except Exception:
            pass
    # Navigation
    with st.sidebar:
        col1, col2 = st.columns([1,1])
        with col1:
            st.button(":material/refresh:", key="button_pbdata_refresh", on_click=_pbdata_refresh_callback)
        with col2:
            if st.button(":back:", key="button_pbdata_back"):
                del st.session_state.pbdata_details
                st.rerun()

    st.subheader("PBData Details")

    pbdata_overview()
    users = st.session_state.users

    if "pbdata_users" in st.session_state:
        if st.session_state.pbdata_users != pbdata.fetch_users:
            pbdata.fetch_users = st.session_state.pbdata_users
    st.multiselect(
        'Users',
        users.list(),
        default=pbdata.fetch_users,
        key="pbdata_users",
        help='Users PBData will actively fetch/update (WS + shared REST pollers).',
    )

    # Separate selection for executions/trades downloading (stored in trades DB)
    # Streamlit keeps widget values in session_state across reruns. If trades_users
    # is not configured yet, ensure the widget starts empty (opt-in behavior).
    try:
        _raw_trades_cfg = load_ini('pbdata', 'trades_users')
        _trades_cfg_set = bool(str(_raw_trades_cfg).strip()) if _raw_trades_cfg is not None else False
    except Exception:
        _trades_cfg_set = False

    # One-time reset of stale widget values when INI has no trades_users.
    # Do NOT delete on every rerun, otherwise user selections can never persist.
    if "_pbdata_trades_users_reset_done" not in st.session_state:
        st.session_state["_pbdata_trades_users_reset_done"] = False
    if not _trades_cfg_set and not st.session_state.get("_pbdata_trades_users_reset_done", False):
        st.session_state["pbdata_trades_users"] = []
        st.session_state["_pbdata_trades_users_reset_done"] = True

    try:
        trades_default = pbdata.trades_users
    except Exception:
        trades_default = []
    st.multiselect(
        'Executions download',
        users.list(),
        default=trades_default,
        key="pbdata_trades_users",
        help='Opt-in list: only these users will download/store executions (my trades) via PBData. Default is none.',
    )

    # Persist selection to pbgui.ini via PBData setter
    try:
        selected_trades_users = st.session_state.get("pbdata_trades_users", [])
        if selected_trades_users != getattr(pbdata, 'trades_users', []):
            pbdata.trades_users = selected_trades_users
    except Exception:
        pass

    def _pbdata_log_level_widget():
        # PBData log level persisted under [pbdata] log_level
        try:
            current_ll = load_ini('pbdata', 'log_level')
            if isinstance(current_ll, str) and current_ll.strip() != '':
                cur_ll_val = current_ll.strip().upper()
            else:
                cur_ll_val = None
        except Exception:
            cur_ll_val = None

        ll_options = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NONE']
        default_ll = cur_ll_val if cur_ll_val is not None else 'INFO'
        st.selectbox(
            'PBData Log level',
            ll_options,
            index=ll_options.index(default_ll) if default_ll in ll_options else 1,
            key='pbdata_log_level_select',
            help='Minimum log level for PBData (messages below this level are suppressed).',
        )
        if 'pbdata_log_level_select' in st.session_state and st.session_state['pbdata_log_level_select'] != cur_ll_val:
            try:
                v = st.session_state['pbdata_log_level_select']
                if v == 'NONE':
                    save_ini('pbdata', 'log_level', '')
                else:
                    save_ini('pbdata', 'log_level', v)
                st.success('Saved log_level setting to pbgui.ini')
            except Exception as e:
                st.error(f'Failed to save log_level: {e}')
            st.rerun()

    # Show PBData logfile (filtered viewer) with log level selector next to Logfiles
    view_log_filtered("PBData", header_right_fn=_pbdata_log_level_widget)

    # -----------------------------
    # PBData timers
    # -----------------------------


    def _read_int_ini(section: str, key: str):
        try:
            v = load_ini(section, key)
            s = str(v).strip() if v is not None else ''
            if s == '':
                return None
            return int(float(s))
        except Exception:
            return None

    def _read_float_ini(section: str, key: str):
        try:
            v = load_ini(section, key)
            s = str(v).strip() if v is not None else ''
            if s == '':
                return None
            return float(s)
        except Exception:
            return None

    # Defaults from running PBData instance
    try:
        _def_pollers_delay = int(getattr(pbdata, '_pollers_delay_seconds', 60))
    except Exception:
        _def_pollers_delay = 60
    try:
        _def_combined = int(getattr(pbdata, '_shared_combined_interval_seconds', 90))
    except Exception:
        _def_combined = 90
    try:
        _def_history = int(getattr(pbdata, '_shared_history_interval_seconds', 90))
    except Exception:
        _def_history = 90
    try:
        _def_exec = int(getattr(pbdata, '_shared_executions_interval_seconds', 1800))
    except Exception:
        _def_exec = 1800
    try:
        _def_rest_pause = float(getattr(pbdata, '_shared_rest_user_pause', 0.75))
    except Exception:
        _def_rest_pause = 0.75

    cur_ws_val = _read_int_ini('pbdata', 'ws_max')
    default_ws = cur_ws_val if cur_ws_val is not None else MAX_PRIVATE_WS_GLOBAL

    cur_pollers_delay = _read_int_ini('pbdata', 'pollers_delay_seconds')
    cur_combined = _read_int_ini('pbdata', 'poll_interval_combined_seconds')
    cur_history = _read_int_ini('pbdata', 'poll_interval_history_seconds')
    cur_exec = _read_int_ini('pbdata', 'poll_interval_executions_seconds')
    cur_rest_pause = _read_float_ini('pbdata', 'shared_rest_user_pause_seconds')

    with st.expander('PBData timers', expanded=False):
        r1 = st.columns(4)
        with r1[0]:
            ws_max = st.number_input(
                'Max private WS global',
                min_value=0,
                value=default_ws,
                step=1,
                key='pbdata_ws_max_input',
                help=pbgui_help.pbdata_ws_max,
            )
        with r1[1]:
            pollers_delay_val = st.number_input(
                'Startup delay (s)',
                min_value=0,
                value=cur_pollers_delay if cur_pollers_delay is not None else _def_pollers_delay,
                step=5,
                key='pbdata_pollers_delay_seconds_input',
                help='Grace period before starting shared REST pollers to avoid startup bursts.',
            )
        with r1[2]:
            combined_val = st.number_input(
                'Combined interval (s)',
                min_value=10,
                value=cur_combined if cur_combined is not None else _def_combined,
                step=10,
                key='pbdata_poll_interval_combined_input',
                help='Interval for shared combined REST poller (balances/positions/orders).',
            )
        with r1[3]:
            history_val = st.number_input(
                'History interval (s)',
                min_value=10,
                value=cur_history if cur_history is not None else _def_history,
                step=10,
                key='pbdata_poll_interval_history_input',
                help='Interval for shared history REST poller.',
            )

        r2 = st.columns(4)
        with r2[0]:
            exec_val = st.number_input(
                'Executions interval (s)',
                min_value=60,
                value=cur_exec if cur_exec is not None else _def_exec,
                step=60,
                key='pbdata_poll_interval_executions_input',
                help='Interval for shared executions (my trades) REST poller.',
            )
        with r2[1]:
            rest_pause_val = st.number_input(
                'REST pause/user (s)',
                min_value=0.0,
                value=cur_rest_pause if cur_rest_pause is not None else _def_rest_pause,
                step=0.05,
                key='pbdata_shared_rest_user_pause_input',
                help='Small pause between users in shared REST pollers to reduce rate limits.',
            )
        with r2[2]:
            st.write('')
        with r2[3]:
            st.write('')

        # Per-exchange overrides (stored in INI as JSON, but edited as number inputs)
        try:
            raw_json = load_ini('pbdata', 'shared_rest_pause_by_exchange_json')
            sval = str(raw_json).strip() if raw_json is not None else ''
            ini_overrides = json.loads(sval) if sval != '' else {}
            if not isinstance(ini_overrides, dict):
                ini_overrides = {}
        except Exception:
            ini_overrides = {}

        with st.expander('Shared REST pause per exchange', expanded=False):
            st.caption('Per-exchange pause between users. Defaults come from PBData (e.g. hyperliquid/bybit) unless you set an override; only differences vs the global pause are saved as overrides.')
            exchange_ids = []
            try:
                exchange_ids = Exchanges.list()
            except Exception:
                exchange_ids = []
            try:
                exchange_ids = sorted([str(x) for x in exchange_ids if str(x).strip() != ''])
            except Exception:
                exchange_ids = [str(x) for x in exchange_ids]

            # 4 per row, to keep UI compact
            cols_per_row = 4
            for i in range(0, len(exchange_ids), cols_per_row):
                row = exchange_ids[i:i + cols_per_row]
                cols = st.columns(cols_per_row)
                for col, exid_str in zip(cols, row):
                    value_key = f'pbdata_rest_pause_ex_{exid_str}'
                    try:
                        if exid_str in ini_overrides:
                            default_val = float(ini_overrides.get(exid_str))
                        else:
                            # Fall back to PBData's built-in defaults (e.g. hyperliquid/bybit)
                            try:
                                per_ex_defaults = getattr(pbdata, '_shared_rest_pause_by_exchange', {}) or {}
                                if isinstance(per_ex_defaults, dict) and exid_str in per_ex_defaults:
                                    default_val = float(per_ex_defaults.get(exid_str))
                                else:
                                    default_val = float(rest_pause_val)
                            except Exception:
                                default_val = float(rest_pause_val)
                    except Exception:
                        default_val = float(rest_pause_val)

                    # Streamlit keeps number_input values in session_state.
                    # If the widget was previously initialized to the global pause
                    # (e.g. 0.75) and there's still no INI override, bump it to the
                    # per-exchange built-in default so hyperliquid/bybit show 3.0.
                    try:
                        if exid_str not in ini_overrides and value_key in st.session_state:
                            cur_v = float(st.session_state.get(value_key))
                            if abs(cur_v - float(rest_pause_val)) < 1e-9 and abs(float(default_val) - float(rest_pause_val)) > 1e-9:
                                st.session_state[value_key] = float(default_val)
                    except Exception:
                        pass
                    with col:
                        st.number_input(
                            f'{exid_str} (s)',
                            min_value=0.0,
                            value=float(default_val),
                            step=0.05,
                            key=value_key,
                            help='Per-exchange pause (seconds) between users for shared REST pollers. Only values differing from the global pause are saved as overrides.',
                        )

        if st.button('Save PBData timers', key='pbdata_save_timers_btn'):
            try:
                save_ini('pbdata', 'ws_max', str(int(ws_max)))
                save_ini('pbdata', 'pollers_delay_seconds', str(int(pollers_delay_val)))
                save_ini('pbdata', 'poll_interval_combined_seconds', str(int(combined_val)))
                save_ini('pbdata', 'poll_interval_history_seconds', str(int(history_val)))
                save_ini('pbdata', 'poll_interval_executions_seconds', str(int(exec_val)))
                save_ini('pbdata', 'shared_rest_user_pause_seconds', str(float(rest_pause_val)))
            except Exception as e:
                st.error(f'Failed saving PBData timers: {e}')
                st.stop()

            # Build per-exchange overrides from number inputs.
            # Only persist values which differ from the global pause.
            try:
                cleaned = {}
                for exid in Exchanges.list():
                    exid_str = str(exid)
                    value_key = f'pbdata_rest_pause_ex_{exid_str}'
                    try:
                        v = float(st.session_state.get(value_key, rest_pause_val))
                    except Exception:
                        v = float(rest_pause_val)
                    try:
                        if abs(v - float(rest_pause_val)) > 1e-9:
                            cleaned[exid_str] = v
                    except Exception:
                        pass
                if cleaned:
                    save_ini('pbdata', 'shared_rest_pause_by_exchange_json', json.dumps(cleaned))
                else:
                    # Clear overrides when none are enabled
                    save_ini('pbdata', 'shared_rest_pause_by_exchange_json', '')
            except Exception as e:
                st.error(f'Failed saving per-exchange overrides: {e}')
                st.stop()
            st.success('Saved PBData timers to pbgui.ini')
            st.rerun()

    # Show fetch summary JSON (generated by PBData)
    try:
        # Note: fragment reruns are triggered automatically by widget
        # interactions inside the fragment. No explicit invalidate helper
        # is required — clicking the button below will rerun only the
        # fragment in Streamlit >=1.51.0.

        summary_path = Path(f"{PBGDIR}/data/logs/fetch_summary.json")
        if summary_path.exists():
            @st.fragment
            def render_fetch_summary():
                # Reload the file on each fragment rerun so the refresh button picks up changes
                try:
                    with open(summary_path, 'r') as sf:
                        try:
                            summary_obj = json.load(sf)
                        except Exception:
                            summary_obj = None
                except Exception:
                    summary_obj = None

                if not summary_obj:
                    st.info('No fetch summary available yet. Start PBData or wait for the next summary cycle.')
                    return

                # Place a small refresh button to invalidate only this fragment
                col_l, col_r = st.columns([1, 0.12])
                # Compute seconds since the summary timestamp (if present)
                secs_str = ''
                try:
                    ts_str = summary_obj.get('timestamp') if isinstance(summary_obj, dict) else None
                    if ts_str:
                        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                        delta = int((datetime.now() - dt).total_seconds())
                        secs_str = f" ({delta}s)"
                except Exception:
                    secs_str = ''
                with col_l:
                    st.subheader(f"Fetch Summary{secs_str}")
                with col_r:
                    st.button(":material/refresh:", key="button_fetch_summary_refresh")

                # Prepare table data: one row per user with columns for each category
                bal = summary_obj.get('balances', {})
                pos = summary_obj.get('positions', {})
                ords = summary_obj.get('orders', {})
                hist = summary_obj.get('history', [])
                execs = summary_obj.get('executions', [])

                # Show counts of ws vs rest for each category
                try:
                    bal_ws = len(bal.get('ws') or [])
                    bal_rest = len(bal.get('rest') or [])
                    pos_ws = len(pos.get('ws') or [])
                    pos_rest = len(pos.get('rest') or [])
                    ord_ws = len(ords.get('ws') or [])
                    ord_rest = len(ords.get('rest') or [])
                    hist_count = len(hist or [])
                    exec_count = len(execs or [])
                    c_bal, c_pos, c_ord, c_hist, c_exec = st.columns(5)
                    with c_bal:
                        st.markdown("**Balances**")
                        st.markdown(f"**WS**: 🟢 {bal_ws}  \n**REST**: 🟠 {bal_rest}")
                    with c_pos:
                        st.markdown("**Positions**")
                        st.markdown(f"**WS**: 🟢 {pos_ws}  \n**REST**: 🟠 {pos_rest}")
                    with c_ord:
                        st.markdown("**Orders**")
                        st.markdown(f"**WS**: 🟢 {ord_ws}  \n**REST**: 🟠 {ord_rest}")
                    with c_hist:
                        st.markdown("**History**")
                        st.markdown(f"**REST**: 🟠 {hist_count}")
                    with c_exec:
                        st.markdown("**Executions**")
                        st.markdown(f"**REST**: 🟠 {exec_count}")
                except Exception:
                    pass

                # Build set of all users
                users_set = set()
                users_set.update(bal.get('ws', []) or [])
                users_set.update(bal.get('rest', []) or [])
                users_set.update(pos.get('ws', []) or [])
                users_set.update(pos.get('rest', []) or [])
                users_set.update(ords.get('ws', []) or [])
                users_set.update(ords.get('rest', []) or [])
                users_set.update(hist or [])
                users_set.update(execs or [])
                users = sorted(users_set)

                rows = []
                last_fetch_map = summary_obj.get('last_fetch_ts', {}) or {}

                def fmt_minutes(ts):
                    try:
                        if not ts:
                            return 'never'
                        mins = int((time.time() - float(ts)) // 60)
                        return f'{mins}m' if mins > 0 else '0m'
                    except Exception:
                        return 'err'

                for u in users:
                    lf = last_fetch_map.get(u, {})
                    bal_status = 'ws' if u in (bal.get('ws') or []) else 'rest'
                    pos_status = 'ws' if u in (pos.get('ws') or []) else 'rest'
                    ord_status = 'ws' if u in (ords.get('ws') or []) else 'rest'
                    hist_status = 'rest' if u in (hist or []) else ''
                    exec_status = 'rest' if u in (execs or []) else ''
                    rows.append({
                        'user': u,
                        'balances': f"{bal_status} ({fmt_minutes(lf.get('balances'))})",
                        'positions': f"{pos_status} ({fmt_minutes(lf.get('positions'))})",
                        'orders': f"{ord_status} ({fmt_minutes(lf.get('orders'))})",
                        'history': f"{hist_status} ({fmt_minutes(lf.get('history'))})" if hist_status else '',
                        'executions': f"{exec_status} ({fmt_minutes(lf.get('executions'))})" if exec_status else '',
                    })

                # Filters and details: collapse into an expander for a cleaner view
                # Do not force `expanded=False` here — letting Streamlit manage
                # the expander state preserves the user's open/closed choice
                # across fragment/page reruns (e.g. when pressing refresh).
                with st.expander("Details"):
                    st.markdown('**Filters:**')
                    fcol1, fcol2, fcol3 = st.columns([1,1,1])
                    with fcol1:
                        show_bal_ws = st.checkbox('Balances WS only', value=False, help='Show only users currently updated via websocket for balances.')
                    with fcol2:
                        show_pos_ws = st.checkbox('Positions WS only', value=False, help='Show only users currently updated via websocket for positions.')
                    with fcol3:
                        show_ord_ws = st.checkbox('Orders WS only', value=False, help='Show only users currently updated via websocket for orders.')

                    # Apply filter
                    def matches(row):
                        if show_bal_ws and not str(row.get('balances','')).startswith('ws'):
                            return False
                        if show_pos_ws and not str(row.get('positions','')).startswith('ws'):
                            return False
                        if show_ord_ws and not str(row.get('orders','')).startswith('ws'):
                            return False
                        return True

                    filtered = [r for r in rows if matches(r)]

                    # Render table (always include history + executions columns)
                    if filtered:
                        def color_for_minutes(mins_text):
                            try:
                                if not mins_text or mins_text in ('never', 'err'):
                                    if mins_text == 'never':
                                        return '#ff4d4d'
                                    if mins_text == 'err':
                                        return '#999999'
                                    return '#ff4d4d'
                                if mins_text.endswith('m'):
                                    n = int(mins_text[:-1])
                                else:
                                    n = int(mins_text)
                                if n <= 5:
                                    return '#2ecc71'
                                if n <= 60:
                                    return '#9be7a3'
                                if n <= 240:
                                    return '#ffae42'
                                return '#ff6b6b'
                            except Exception:
                                return '#999999'

                        def render_cell(display_str):
                            try:
                                if '(' in display_str and ')' in display_str:
                                    prefix, rest = display_str.split('(', 1)
                                    mins = rest.rstrip(')')
                                    color = color_for_minutes(mins)
                                    esc_prefix = _html.escape(prefix.strip())
                                    esc_mins = _html.escape(mins)
                                    return f"{esc_prefix} (<span style=\"color:{color};font-weight:600\">{esc_mins}</span>)"
                                else:
                                    return _html.escape(display_str)
                            except Exception:
                                return _html.escape(display_str)

                        header_cols = ['user', 'balances', 'positions', 'orders', 'history', 'executions']
                        html_rows = []
                        style_block = (
                            "<style>"
                            ".pbgui-fetch-table { border-collapse:collapse;width:100%;font-family:Arial, Helvetica, sans-serif;font-size:14px }"
                            ".pbgui-fetch-table th { text-align:left;padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.16) }"
                            ".pbgui-fetch-table td { padding:6px 8px;border-top:none }"
                            ".pbgui-fetch-table tr + tr td { border-top:1px solid rgba(255,255,255,0.10) }"
                            ".pbgui-fetch-table tr:nth-child(odd) td { background: rgba(255,255,255,0.02) }"
                            ".pbgui-fetch-table tr:hover td { background: rgba(255,255,255,0.04) }"
                            "</style>"
                        )
                        header_html = '<tr>' + ''.join([f'<th>{_html.escape(h)}</th>' for h in header_cols]) + '</tr>'
                        for r in filtered:
                            user_html = f"<td>{_html.escape(r.get('user',''))}</td>"
                            bal_html = f"<td>{render_cell(r.get('balances',''))}</td>"
                            pos_html = f"<td>{render_cell(r.get('positions',''))}</td>"
                            ord_html = f"<td>{render_cell(r.get('orders',''))}</td>"
                            hist_html = f"<td>{render_cell(r.get('history',''))}</td>"
                            exec_html = f"<td>{render_cell(r.get('executions',''))}</td>"
                            html_rows.append(f"<tr>{user_html}{bal_html}{pos_html}{ord_html}{hist_html}{exec_html}</tr>")

                        table_html = style_block + f"<table class='pbgui-fetch-table'>{header_html}{''.join(html_rows)}</table>"
                        st.markdown(table_html, unsafe_allow_html=True)
                    else:
                        st.info('No users match the current filters.')

            try:
                render_fetch_summary()
            except Exception as e:
                st.error(f'Failed to render fetch summary fragment: {e}')
        else:
            st.info('No fetch summary available yet. Start PBData or wait for the next summary cycle.')
    except Exception as e:
        st.error(f'Failed to load fetch summary: {e}')

def pbcoindata_details():
    pbcoindata = st.session_state.pbcoindata
    # Navigation
    with st.sidebar:
        if st.button(":back:", key="button_pbcoindata_back"):
            del st.session_state.pbcoindata_details
            st.rerun()
    st.subheader("PBCoinData Details")
    pbcoindata_overview()
    if st.checkbox("Show logfile", key="pbcoindata_log"):
        st.session_state.pbgui_instances.view_log("PBCoinData")

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBGUI Services")

# Header: show PBData Guide in the top header row (above a full-width divider)
if 'pbdata_details' in st.session_state:
    render_header_with_guide(
        "PBGUI Services",
        guide_callback=lambda: _help_modal('PBData'),
        guide_key='pbdata_guide_btn',
        guide_help='Open PBData help & tutorials',
    )
else:
    render_header_with_guide("PBGUI Services")

if 'monitor_edit' in st.session_state:
    st.session_state.monitor.edit_monitor_config()
elif 'pbrun_details' in st.session_state:
    pbrun_details()
elif 'edit_bucket' in st.session_state:
    edit_bucket()
elif 'pbremote_details' in st.session_state:
    pbremote_details()
elif 'pbmon_details' in st.session_state:
    pbmon_details()
elif 'pbstat_details' in st.session_state:
    pbstat_details()
elif 'pbdata_details' in st.session_state:
    pbdata_details()
elif 'pbcoindata_details' in st.session_state:
    pbcoindata_details()
else:
    overview()
