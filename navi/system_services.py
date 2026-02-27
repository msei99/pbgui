import streamlit as st
from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    is_authenticted,
    get_navi_paths,
    sync_api,
    PBGDIR,
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


def pbrun_overview(key_suffix=""):
    pbrun = st.session_state.pbrun
    _key = f"service_pbrun{key_suffix}"
    is_running = pbrun.is_running()
    desired = st.session_state.get(_key, is_running)
    st.toggle("PBRun", value=is_running, key=_key, help=pbgui_help.pbrun)
    if desired and not is_running:
        pbrun.run()
    elif not desired and is_running:
        pbrun.stop()
    st.metric(label="PBRun", value='✅' if desired else '❌')

def pbremote_overview(key_suffix=""):
    pbremote = st.session_state.pbremote
    _key = f"service_pbremote{key_suffix}"
    is_running = pbremote.is_running()
    desired = st.session_state.get(_key, is_running)
    st.toggle("PBRemote", value=is_running, key=_key, help=pbgui_help.pbremote)
    if desired and not is_running:
        pbremote.run()
    elif not desired and is_running:
        pbremote.stop()
    st.metric(label="PBRemote", value='✅' if desired else '❌')

def pbmon_overview(key_suffix=""):
    pbmon = st.session_state.pbmon
    _key = f"service_pbmon{key_suffix}"
    is_running = pbmon.is_running()
    desired = st.session_state.get(_key, is_running)
    st.toggle("PBMon", value=is_running, key=_key, help=pbgui_help.pbmon)
    if desired and not is_running:
        pbmon.run()
    elif not desired and is_running:
        pbmon.stop()
    st.metric(label="PBMon", value='✅' if desired else '❌')

def pbstat_overview(key_suffix=""):
    pbstat = st.session_state.pbstat
    _key = f"service_pbstat{key_suffix}"
    is_running = pbstat.is_running()
    desired = st.session_state.get(_key, is_running)
    st.toggle("PBStat", value=is_running, key=_key, help=pbgui_help.pbstat)
    if desired and not is_running:
        pbstat.run()
    elif not desired and is_running:
        pbstat.stop()
    st.metric(label="PBStat", value='✅' if desired else '❌')

def pbdata_overview(key_suffix=""):
    pbdata = st.session_state.pbdata
    _key = f"service_pbdata{key_suffix}"
    is_running = pbdata.is_running()
    desired = st.session_state.get(_key, is_running)
    st.toggle("PBData", value=is_running, key=_key, help=pbgui_help.pbdata)
    if desired and not is_running:
        pbdata.run()
    elif not desired and is_running:
        pbdata.stop()
    st.metric(label="PBData", value='✅' if desired else '❌')

def pbcoindata_overview(key_suffix=""):
    pbcoindata = st.session_state.pbcoindata
    _key = f"service_pbcoindata{key_suffix}"
    is_running = pbcoindata.is_running()
    desired = st.session_state.get(_key, is_running)
    st.toggle("PBCoinData", value=is_running, key=_key, help=pbgui_help.pbcoindata)
    if desired and not is_running:
        pbcoindata.run()
    elif not desired and is_running:
        pbcoindata.stop()
    st.metric(label="PBCoinData", value='✅' if desired else '❌')

def pbmaster_overview(key_suffix=""):
    pbmaster = st.session_state.pbmaster
    _key = f"service_pbmaster{key_suffix}"
    is_running = pbmaster.is_running()
    desired = st.session_state.get(_key, is_running)
    st.toggle("PBMaster", value=is_running, key=_key, help=pbgui_help.pbmaster)
    if desired and not is_running:
        pbmaster.run()
    elif not desired and is_running:
        pbmaster.stop()
    st.metric(label="PBMaster", value='✅' if desired else '❌')

def overview():
    col_1, col_2, col_3 = st.columns(3)
    with col_1:
        pbrun_overview(key_suffix="_ov")
    with col_2:
        pbremote_overview(key_suffix="_ov")
    with col_3:
        pbmon_overview(key_suffix="_ov")
    col_4, col_5, col_6 = st.columns(3)
    with col_4:
        pbstat_overview(key_suffix="_ov")
    with col_5:
        pbdata_overview(key_suffix="_ov")
    with col_6:
        pbcoindata_overview(key_suffix="_ov")
    col_7, col_8, col_9 = st.columns(3)
    with col_7:
        pbmaster_overview(key_suffix="_ov")

def pbrun_details():
    view_log_filtered("PBRun")

def _pbremote_sidebar():
    """Sidebar content for PBRemote: overview toggle + action buttons + server list."""
    pbremote = st.session_state.pbremote
    if "monitor" not in st.session_state:
        st.session_state.monitor = Monitor()
    monitor = st.session_state.monitor

    pbremote_overview(key_suffix="_det")
    if st.button(":material/refresh:", key="pbremote_sidebar_refresh"):
        pbremote.update_remote_servers()
        monitor.d_v7 = []
        monitor.d_multi = []
        monitor.d_single = []
        st.rerun()
    st.markdown("**Remote Servers**")
    api_sync = []
    if st.button("View All Instances", key="pbremote_sidebar_all"):
        if "server" in st.session_state:
            del st.session_state.server
        monitor.d_v7 = []
        monitor.d_multi = []
        monitor.d_single = []
        monitor.servers = pbremote.remote_servers
        st.rerun()
    for rserver in sorted(pbremote.remote_servers, key=lambda s: s.name):
        if rserver.is_online():
            color = "green"
            if not rserver.is_api_md5_same(pbremote.api_md5):
                api_sync.append(rserver)
        else:
            color = "red"
        _sc1, _sc2 = st.columns([4, 1])
        with _sc1:
            if st.button(f":{color}[{rserver.name}]", key=f"sidebar_srv_{rserver.name}"):
                monitor.d_v7 = []
                monitor.d_multi = []
                monitor.d_single = []
                st.session_state.server = rserver
        with _sc2:
            if color == "red":
                if st.button(":material/delete:", key=f"sidebar_del_{rserver.name}"):
                    rserver.delete_server()
                    pbremote.update_remote_servers()
                    st.rerun()
    sync_api()
    return api_sync

def pbremote_details():
    pbremote = st.session_state.pbremote
    if "monitor" not in st.session_state:
        st.session_state.monitor = Monitor()
    monitor = st.session_state.monitor

    # ── Settings expander ────────────────────────────────────────────────────
    with st.expander(":material/settings: Settings​", expanded=not pbremote.buckets):

        # ── Bucket section ───────────────────────────────────────────────────
        st.markdown("**Bucket**")

        # Sync bucket selectbox → model
        if "pbremote_bucket" in st.session_state:
            if st.session_state.pbremote_bucket != pbremote.bucket:
                pbremote.bucket = st.session_state.pbremote_bucket
                pbremote.fetch_bucket_config()

        # Determine bucket index
        if pbremote.bucket and pbremote.bucket in pbremote.buckets:
            buckets_index = pbremote.buckets.index(pbremote.bucket)
        else:
            buckets_index = 0

        _bc1, _bc2 = st.columns([3, 1])
        with _bc1:
            if pbremote.buckets:
                st.selectbox("Select bucket", pbremote.buckets, index=buckets_index,
                             key="pbremote_bucket", help=pbgui_help.pbremote_bucket)
        with _bc2:
            if st.button(":material/add: Add bucket", key="pbremote_bucket_add"):
                pbremote.bucket = None
                pbremote.bucket_region = None
                pbremote.bucket_endpoint = None
                pbremote.bucket_access_key_id = None
                pbremote.bucket_secret_access_key = None
                for k in ["pbremote_edit_bucket_name", "pbremote_edit_region",
                          "pbremote_edit_endpoint", "pbremote_edit_access_key",
                          "pbremote_edit_secret_key"]:
                    st.session_state.pop(k, None)
                st.session_state["pbremote_show_bucket_edit"] = True
                st.rerun()
            if pbremote.buckets:
                if st.button(":material/edit: Edit", key="pbremote_bucket_edit_btn"):
                    pbremote.fetch_bucket_config()
                    st.session_state["pbremote_show_bucket_edit"] = True
                    st.rerun()

        if not pbremote.buckets:
            if pbremote.rclone_installed:
                st.info("No bucket found. Click **Add bucket** to configure rclone.")
            else:
                st.warning("rclone not installed. Go to VPS Manager → install rclone on your local system.")

        # Inline bucket edit form (conditional)
        if st.session_state.get("pbremote_show_bucket_edit"):
            with st.container(border=True):
                st.caption(
                    "1. Get your free 15 GB account at [Synology C2](https://c2.synology.com/en-uk/object-storage/overview).  \n"
                    "2. Create a bucket · fill in details · Save · Test."
                )
                # Snapshot of saved values — set once when form opens, reset after save
                if "_bucket_saved_snapshot" not in st.session_state:
                    st.session_state["_bucket_saved_snapshot"] = {
                        "name":       pbremote.bucket[:-1] if pbremote.bucket else "",
                        "region":     pbremote.bucket_region or "",
                        "endpoint":   pbremote.bucket_endpoint or "",
                        "access_key": pbremote.bucket_access_key_id or "",
                        "secret_key": pbremote.bucket_secret_access_key or "",
                    }

                # Sync session state → model
                for _wk, _attr in [
                    ("pbremote_edit_bucket_name", None),
                    ("pbremote_edit_region",      "bucket_region"),
                    ("pbremote_edit_endpoint",    "bucket_endpoint"),
                    ("pbremote_edit_access_key",  "bucket_access_key_id"),
                    ("pbremote_edit_secret_key",  "bucket_secret_access_key"),
                ]:
                    if _attr and _wk in st.session_state:
                        if getattr(pbremote, _attr) != st.session_state[_wk]:
                            setattr(pbremote, _attr, st.session_state[_wk])

                _bucket_name_val = pbremote.bucket[:-1] if pbremote.bucket else ""
                st.text_input("Bucket name", value=_bucket_name_val, key="pbremote_edit_bucket_name")
                _ef1, _ef2, _ef3, _ef4 = st.columns(4)
                with _ef1:
                    st.text_input("Region", value=pbremote.bucket_region or "", key="pbremote_edit_region")
                with _ef2:
                    st.text_input("Endpoint", value=pbremote.bucket_endpoint or "", key="pbremote_edit_endpoint")
                with _ef3:
                    st.text_input("Access Key ID", value=pbremote.bucket_access_key_id or "", key="pbremote_edit_access_key")
                with _ef4:
                    st.text_input("Secret Access Key", value=pbremote.bucket_secret_access_key or "",
                                  type="password", key="pbremote_edit_secret_key")

                # Dirty detection: compare widget values against snapshot
                _bsnap = st.session_state["_bucket_saved_snapshot"]
                _bucket_dirty = (
                    st.session_state.get("pbremote_edit_bucket_name", _bsnap["name"]) != _bsnap["name"] or
                    st.session_state.get("pbremote_edit_region",      _bsnap["region"]) != _bsnap["region"] or
                    st.session_state.get("pbremote_edit_endpoint",    _bsnap["endpoint"]) != _bsnap["endpoint"] or
                    st.session_state.get("pbremote_edit_access_key",  _bsnap["access_key"]) != _bsnap["access_key"] or
                    st.session_state.get("pbremote_edit_secret_key",  _bsnap["secret_key"]) != _bsnap["secret_key"]
                )

                # Apply bucket name from widget before save/test
                def _apply_bucket_fields():
                    name = st.session_state.get("pbremote_edit_bucket_name", "").strip()
                    pbremote.bucket = (name + ":") if name else None
                    pbremote.bucket_region = st.session_state.get("pbremote_edit_region", "")
                    pbremote.bucket_endpoint = st.session_state.get("pbremote_edit_endpoint", "")
                    pbremote.bucket_access_key_id = st.session_state.get("pbremote_edit_access_key", "")
                    pbremote.bucket_secret_access_key = st.session_state.get("pbremote_edit_secret_key", "")

                _ba1, _ba2, _ba3, _ba4 = st.columns(4)
                with _ba1:
                    if st.button(":material/save:", key="pbremote_bucket_save",
                                 type="primary" if _bucket_dirty else "secondary"):
                        _apply_bucket_fields()
                        ok, result = pbremote.save_bucket_config()
                        if ok:
                            pbremote.fetch_buckets()
                            st.session_state.pop("pbremote_show_bucket_edit", None)
                            st.session_state.pop("_bucket_saved_snapshot", None)
                            result_popup("Bucket saved", result)
                        else:
                            st.error(result)
                with _ba2:
                    if st.button(":material/lan: Test Connection", key="pbremote_bucket_test"):
                        _apply_bucket_fields()
                        ok, result = pbremote.test_bucket()
                        if ok:
                            result_popup("Connection successful", result)
                        else:
                            st.error(result)
                with _ba3:
                    if pbremote.buckets and st.button(":material/delete: Delete", key="pbremote_bucket_delete"):
                        ok, result = pbremote.delete_bucket()
                        if ok:
                            pbremote.fetch_buckets()
                            st.session_state.pop("pbremote_show_bucket_edit", None)
                            st.session_state.pop("_bucket_saved_snapshot", None)
                            result_popup("Bucket deleted", result)
                        else:
                            st.error(result)
                with _ba4:
                    if st.button(":material/close: Cancel", key="pbremote_bucket_cancel"):
                        st.session_state.pop("pbremote_show_bucket_edit", None)
                        st.session_state.pop("_bucket_saved_snapshot", None)
                        st.rerun()

        st.markdown("---")

        # ── Monitor Settings section ─────────────────────────────────────────
        st.markdown("**Monitor Settings**")

        _MC_FIELDS = [
            'mem_warning_server', 'mem_error_server', 'swap_warning_server', 'swap_error_server',
            'disk_warning_server', 'disk_error_server', 'cpu_warning_server', 'cpu_error_server',
            'mem_warning_v7', 'mem_error_v7', 'swap_warning_v7', 'swap_error_v7',
            'cpu_warning_v7', 'cpu_error_v7', 'error_warning_v7', 'error_error_v7',
            'traceback_warning_v7', 'traceback_error_v7',
            'mem_warning_multi', 'mem_error_multi', 'swap_warning_multi', 'swap_error_multi',
            'cpu_warning_multi', 'cpu_error_multi', 'error_warning_multi', 'error_error_multi',
            'traceback_warning_multi', 'traceback_error_multi',
            'mem_warning_single', 'mem_error_single', 'swap_warning_single', 'swap_error_single',
            'cpu_warning_single', 'cpu_error_single', 'error_warning_single', 'error_error_single',
            'traceback_warning_single', 'traceback_error_single',
        ]
        # Snapshot before edit_monitor_config syncs widgets → model (only init once, reset after save)
        if "_monitor_saved_snapshot" not in st.session_state:
            _mc = monitor.monitor_config
            st.session_state["_monitor_saved_snapshot"] = {f: getattr(_mc, f) for f in _MC_FIELDS}

        monitor.edit_monitor_config(show_navigation=False)

        # After sync: model == widget values; compare against saved snapshot
        _snap = st.session_state["_monitor_saved_snapshot"]
        _mc_dirty = any(
            abs(getattr(monitor.monitor_config, f) - _snap[f]) > 1e-9
            for f in _MC_FIELDS
        )
        if st.button(":material/save:", key="pbremote_save_monitor",
                     type="primary" if _mc_dirty else "secondary"):
            monitor.monitor_config.save_monitor_config()
            st.session_state["_monitor_saved_snapshot"] = {
                f: getattr(monitor.monitor_config, f) for f in _MC_FIELDS
            }
            st.rerun()

    # ── Server panel ────────────────────────────────────────────────────────
    if "server" in st.session_state:
        monitor.server = st.session_state.server
        monitor.view_server()
        monitor.servers = [monitor.server]
        monitor.view_server_instances()
    elif monitor.servers:
        monitor.view_server_instances()
    else:
        st.info("Please select a remote server from the sidebar to view details.")

    st.markdown("---")
    view_log_filtered("PBRemote")

@st.dialog("Info", width="large")
def result_popup(message, result):
    st.info(f'{message}', icon="✅")
    with st.container(height=1200):
        st.text(result)
    if st.button(":green[OK]"):
        st.rerun()

def pbmon_details():
    pbmon = st.session_state.pbmon

    with st.expander(":material/settings: Settings​​", expanded=not (pbmon.telegram_token and pbmon.telegram_chat_id)):
        # Read current values into session state only once (avoids overwriting user edits)
        if "pbmon_telegram_token" not in st.session_state:
            st.session_state.pbmon_telegram_token = pbmon.telegram_token
        if "pbmon_telegram_chat_id" not in st.session_state:
            st.session_state.pbmon_telegram_chat_id = pbmon.telegram_chat_id

        st.text_input("Telegram Bot Token", type="password", key="pbmon_telegram_token", help=pbgui_help.pbmon_telegram_token)
        st.text_input("Telegram Chat ID", key="pbmon_telegram_chat_id", help=pbgui_help.pbmon_telegram_chat_id)

        _pbmon_dirty = (
            st.session_state.get("pbmon_telegram_token", pbmon.telegram_token) != (pbmon.telegram_token or "") or
            st.session_state.get("pbmon_telegram_chat_id", pbmon.telegram_chat_id) != (pbmon.telegram_chat_id or "")
        )
        if st.button(":material/save:", key="pbmon_save_config", type="primary" if _pbmon_dirty else "secondary"):
            pbmon.telegram_token = st.session_state.pbmon_telegram_token
            pbmon.telegram_chat_id = st.session_state.pbmon_telegram_chat_id
            st.rerun()

    view_log_filtered("PBMon")

def pbstat_details():
    view_log_filtered("PBStat")

def pbdata_details():
    pbdata = st.session_state.pbdata
    # Callback for refresh button to avoid manual save/restore of session flags
    def _pbdata_refresh_callback():
        try:
            st.session_state.users.load()
        except Exception:
            pass

    users = st.session_state.users

    # ── Helper functions for reading INI values ──────────────────────────────
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

    # ── Settings expander ────────────────────────────────────────────────────
    with st.expander(":material/settings: Settings​​​", expanded=False):

        # Refresh users button inside expander
        st.button(":material/refresh: Refresh Users", key="button_pbdata_refresh", on_click=_pbdata_refresh_callback)

        # Users multiselect
        if "pbdata_users" not in st.session_state:
            st.session_state.pbdata_users = list(pbdata.fetch_users or [])
        st.multiselect(
            'Users',
            users.list(),
            key="pbdata_users",
            help='Users PBData will actively fetch/update (WS + shared REST pollers).',
        )

        # Executions/trades users
        try:
            _raw_trades_cfg = load_ini('pbdata', 'trades_users')
            _trades_cfg_set = bool(str(_raw_trades_cfg).strip()) if _raw_trades_cfg is not None else False
        except Exception:
            _trades_cfg_set = False

        if "_pbdata_trades_users_reset_done" not in st.session_state:
            st.session_state["_pbdata_trades_users_reset_done"] = False
        if not _trades_cfg_set and not st.session_state.get("_pbdata_trades_users_reset_done", False):
            st.session_state["pbdata_trades_users"] = []
            st.session_state["_pbdata_trades_users_reset_done"] = True

        try:
            trades_default = pbdata.trades_users if "pbdata_trades_users" not in st.session_state else st.session_state["pbdata_trades_users"]
        except Exception:
            trades_default = []
        st.multiselect(
            'Executions download',
            users.list(),
            default=trades_default,
            key="pbdata_trades_users",
            help='Opt-in list: only these users will download/store executions (my trades) via PBData. Default is none.',
        )

        # Log level (no auto-save — saved with the Save button below)
        try:
            current_ll = load_ini('pbdata', 'log_level')
            cur_ll_val = current_ll.strip().upper() if isinstance(current_ll, str) and current_ll.strip() else 'INFO'
        except Exception:
            cur_ll_val = 'INFO'
        ll_options = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NONE']
        if "pbdata_log_level_select" not in st.session_state:
            st.session_state["pbdata_log_level_select"] = cur_ll_val if cur_ll_val in ll_options else 'INFO'
        st.selectbox(
            'Log level',
            ll_options,
            key='pbdata_log_level_select',
            help='Minimum log level for PBData (messages below this level are suppressed).',
        )

        st.markdown("---")

        # ── Timers ───────────────────────────────────────────────────────────
        st.markdown("**Timers**")
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

        r1 = st.columns(4)
        with r1[0]:
            ws_max = st.number_input('Max private WS global', min_value=0, value=default_ws, step=1,
                                     key='pbdata_ws_max_input', help=pbgui_help.pbdata_ws_max)
        with r1[1]:
            pollers_delay_val = st.number_input('Startup delay (s)', min_value=0,
                                                value=cur_pollers_delay if cur_pollers_delay is not None else _def_pollers_delay,
                                                step=5, key='pbdata_pollers_delay_seconds_input',
                                                help='Grace period before starting shared REST pollers to avoid startup bursts.')
        with r1[2]:
            combined_val = st.number_input('Combined interval (s)', min_value=10,
                                           value=cur_combined if cur_combined is not None else _def_combined,
                                           step=10, key='pbdata_poll_interval_combined_input',
                                           help='Interval for shared combined REST poller (balances/positions/orders).')
        with r1[3]:
            history_val = st.number_input('History interval (s)', min_value=10,
                                          value=cur_history if cur_history is not None else _def_history,
                                          step=10, key='pbdata_poll_interval_history_input',
                                          help='Interval for shared history REST poller.')

        r2 = st.columns(2)
        with r2[0]:
            exec_val = st.number_input('Executions interval (s)', min_value=60,
                                       value=cur_exec if cur_exec is not None else _def_exec,
                                       step=60, key='pbdata_poll_interval_executions_input',
                                       help='Interval for shared executions (my trades) REST poller.')
        with r2[1]:
            rest_pause_val = st.number_input('REST pause/user (s)', min_value=0.0,
                                             value=cur_rest_pause if cur_rest_pause is not None else _def_rest_pause,
                                             step=0.05, key='pbdata_shared_rest_user_pause_input',
                                             help='Small pause between users in shared REST pollers to reduce rate limits.')

        # Per-exchange overrides
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

                    try:
                        if exid_str not in ini_overrides and value_key in st.session_state:
                            cur_v = float(st.session_state.get(value_key))
                            if abs(cur_v - float(rest_pause_val)) < 1e-9 and abs(float(default_val) - float(rest_pause_val)) > 1e-9:
                                st.session_state[value_key] = float(default_val)
                    except Exception:
                        pass
                    with col:
                        st.number_input(f'{exid_str} (s)', min_value=0.0, value=float(default_val), step=0.05,
                                        key=value_key,
                                        help='Per-exchange pause (seconds) between users for shared REST pollers.')

        # ── Single Save button ───────────────────────────────────────────────
        _pbdata_dirty = (
            sorted(st.session_state.get("pbdata_users", pbdata.fetch_users or [])) != sorted(pbdata.fetch_users or []) or
            sorted(st.session_state.get("pbdata_trades_users", pbdata.trades_users or [])) != sorted(pbdata.trades_users or []) or
            st.session_state.get("pbdata_log_level_select", cur_ll_val or "INFO") != (cur_ll_val or "INFO") or
            st.session_state.get("pbdata_ws_max_input", default_ws) != default_ws or
            st.session_state.get("pbdata_pollers_delay_seconds_input", cur_pollers_delay if cur_pollers_delay is not None else _def_pollers_delay) != (cur_pollers_delay if cur_pollers_delay is not None else _def_pollers_delay) or
            st.session_state.get("pbdata_poll_interval_combined_input", cur_combined if cur_combined is not None else _def_combined) != (cur_combined if cur_combined is not None else _def_combined) or
            st.session_state.get("pbdata_poll_interval_history_input", cur_history if cur_history is not None else _def_history) != (cur_history if cur_history is not None else _def_history) or
            st.session_state.get("pbdata_poll_interval_executions_input", cur_exec if cur_exec is not None else _def_exec) != (cur_exec if cur_exec is not None else _def_exec) or
            abs(st.session_state.get("pbdata_shared_rest_user_pause_input", cur_rest_pause if cur_rest_pause is not None else _def_rest_pause) - (cur_rest_pause if cur_rest_pause is not None else _def_rest_pause)) > 1e-9
        )
        if st.button(":material/save:", key='pbdata_save_config_btn', type="primary" if _pbdata_dirty else "secondary"):
            _save_errors = []
            # Users
            try:
                pbdata.fetch_users = st.session_state.get("pbdata_users", [])
            except Exception as e:
                _save_errors.append(f'fetch_users: {e}')
            # Trades users
            try:
                pbdata.trades_users = st.session_state.get("pbdata_trades_users", [])
            except Exception as e:
                _save_errors.append(f'trades_users: {e}')
            # Log level
            try:
                v = st.session_state.get('pbdata_log_level_select', 'INFO')
                save_ini('pbdata', 'log_level', '' if v == 'NONE' else v)
            except Exception as e:
                _save_errors.append(f'log_level: {e}')
            # Timers
            try:
                save_ini('pbdata', 'ws_max', str(int(ws_max)))
                save_ini('pbdata', 'pollers_delay_seconds', str(int(pollers_delay_val)))
                save_ini('pbdata', 'poll_interval_combined_seconds', str(int(combined_val)))
                save_ini('pbdata', 'poll_interval_history_seconds', str(int(history_val)))
                save_ini('pbdata', 'poll_interval_executions_seconds', str(int(exec_val)))
                save_ini('pbdata', 'shared_rest_user_pause_seconds', str(float(rest_pause_val)))
            except Exception as e:
                _save_errors.append(f'timers: {e}')
            # Per-exchange overrides
            try:
                cleaned = {}
                for exid in Exchanges.list():
                    exid_str = str(exid)
                    value_key = f'pbdata_rest_pause_ex_{exid_str}'
                    try:
                        v = float(st.session_state.get(value_key, rest_pause_val))
                    except Exception:
                        v = float(rest_pause_val)
                    if abs(v - float(rest_pause_val)) > 1e-9:
                        cleaned[exid_str] = v
                save_ini('pbdata', 'shared_rest_pause_by_exchange_json', json.dumps(cleaned) if cleaned else '')
            except Exception as e:
                _save_errors.append(f'exchange overrides: {e}')

            if _save_errors:
                st.error('Errors saving config: ' + '; '.join(_save_errors))
            else:
                st.success('Config saved.')
                st.rerun()

    # Log viewer (always visible, outside Settings expander)
    view_log_filtered("PBData")

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

    with st.expander(":material/settings: Settings​​​​", expanded=not pbcoindata.api_key):
        # Initialize session state once so widgets don't reset on every rerun
        if "edit_coindata_api_key" not in st.session_state:
            st.session_state["edit_coindata_api_key"] = pbcoindata.api_key
        if "edit_coindata_fetch_limit" not in st.session_state:
            st.session_state["edit_coindata_fetch_limit"] = pbcoindata.fetch_limit
        if "edit_coindata_fetch_interval" not in st.session_state:
            st.session_state["edit_coindata_fetch_interval"] = pbcoindata.fetch_interval
        if "edit_coindata_metadata_interval" not in st.session_state:
            st.session_state["edit_coindata_metadata_interval"] = pbcoindata.metadata_interval
        if "edit_coindata_mapping_interval" not in st.session_state:
            st.session_state["edit_coindata_mapping_interval"] = pbcoindata.mapping_interval

        _cfg = st.columns([2, 1, 1, 1, 1])
        with _cfg[0]:
            st.text_input("CMC API Key", type="password", key="edit_coindata_api_key", help=pbgui_help.coindata_api_key)
        with _cfg[1]:
            st.number_input("Fetch Limit", min_value=200, max_value=5000, step=200, format="%.d", key="edit_coindata_fetch_limit", help=pbgui_help.coindata_fetch_limit)
        with _cfg[2]:
            st.number_input("Fetch Interval (h)", min_value=1, max_value=24, step=1, format="%.d", key="edit_coindata_fetch_interval", help=pbgui_help.coindata_fetch_interval)
        with _cfg[3]:
            st.number_input("Metadata Interval (d)", min_value=1, max_value=7, step=1, format="%.d", key="edit_coindata_metadata_interval", help=pbgui_help.coindata_metadata_interval)
        with _cfg[4]:
            st.number_input("Mapping Interval (h)", min_value=1, max_value=168, step=1, format="%.d", key="edit_coindata_mapping_interval", help=pbgui_help.coindata_mapping_interval)

        _pbcoindata_dirty = (
            st.session_state.get("edit_coindata_api_key", pbcoindata.api_key) != (pbcoindata.api_key or "") or
            st.session_state.get("edit_coindata_fetch_limit", pbcoindata.fetch_limit) != pbcoindata.fetch_limit or
            st.session_state.get("edit_coindata_fetch_interval", pbcoindata.fetch_interval) != pbcoindata.fetch_interval or
            st.session_state.get("edit_coindata_metadata_interval", pbcoindata.metadata_interval) != pbcoindata.metadata_interval or
            st.session_state.get("edit_coindata_mapping_interval", pbcoindata.mapping_interval) != pbcoindata.mapping_interval
        )
        if st.button(":material/save:", key="button_pbcoindata_save", type="primary" if _pbcoindata_dirty else "secondary"):
            pbcoindata.api_key = st.session_state.get("edit_coindata_api_key", pbcoindata.api_key)
            pbcoindata.fetch_limit = st.session_state.get("edit_coindata_fetch_limit", pbcoindata.fetch_limit)
            pbcoindata.fetch_interval = st.session_state.get("edit_coindata_fetch_interval", pbcoindata.fetch_interval)
            pbcoindata.metadata_interval = st.session_state.get("edit_coindata_metadata_interval", pbcoindata.metadata_interval)
            pbcoindata.mapping_interval = st.session_state.get("edit_coindata_mapping_interval", pbcoindata.mapping_interval)
            pbcoindata.save_config()
            st.success("Config saved.")
            st.rerun()

    # API status — cached for 5 minutes to avoid burning CMC credits on every rerun
    _API_CACHE_TTL = 300
    _api_cache = st.session_state.get("_coindata_api_status_cache")
    _api_cache_age = int(time.time() - _api_cache["ts"]) if _api_cache else None
    _api_stale = _api_cache is None or _api_cache_age >= _API_CACHE_TTL
    if pbcoindata.api_key:
        _btn_col, _age_col = st.columns([1, 4])
        with _btn_col:
            _force_refresh = st.button(":material/refresh: Check API", key="coindata_api_check_btn")
        if _api_stale or _force_refresh:
            _ok = pbcoindata.fetch_api_status()
            st.session_state["_coindata_api_status_cache"] = {
                "ok": _ok,
                "ts": time.time(),
                "error": pbcoindata.api_error if not _ok else None,
            }
            _api_cache = st.session_state["_coindata_api_status_cache"]
            _api_cache_age = 0
        with _age_col:
            if _api_cache_age is not None:
                st.caption(f"Last checked {_api_cache_age}s ago")
        if _api_cache and _api_cache["ok"]:
            st.success("**API Key is valid**", icon="✅")
            _api_cols = st.columns([1, 1, 1, 1, 2])
            with _api_cols[0]:
                st.metric("Monthly Limit", pbcoindata.credit_limit_monthly)
            with _api_cols[1]:
                st.metric("Used Today", pbcoindata.credits_used_day)
            with _api_cols[2]:
                st.metric("Used Monthly", pbcoindata.credits_used_month)
            with _api_cols[3]:
                st.metric("Credits Left", pbcoindata.credits_left)
            with _api_cols[4]:
                st.metric("Reset in", pbcoindata.credit_limit_monthly_reset.replace("In ", ""))
        elif _api_cache:
            st.error(_api_cache["error"] or "Unknown API error", icon="🚨")
    view_log_filtered("PBCoinData")

def pbmaster_details():
    pbmaster = st.session_state.pbmaster

    with st.expander(":material/settings: Settings", expanded=False):
        # Auto-restart toggle
        if "pbmaster_auto_restart" not in st.session_state:
            st.session_state.pbmaster_auto_restart = pbmaster.auto_restart

        st.toggle(
            "Auto-restart services",
            key="pbmaster_auto_restart",
            help=pbgui_help.pbmaster_auto_restart,
        )

        # Monitor interval
        if "pbmaster_monitor_interval" not in st.session_state:
            st.session_state.pbmaster_monitor_interval = pbmaster.monitor_interval

        st.number_input(
            "Monitor interval (seconds)",
            min_value=5, max_value=300, step=5,
            key="pbmaster_monitor_interval",
            help=pbgui_help.pbmaster_monitor_interval,
        )

        # WebSocket port
        if "pbmaster_ws_port" not in st.session_state:
            st.session_state.pbmaster_ws_port = pbmaster.ws_port

        st.number_input(
            "WebSocket port",
            min_value=1024, max_value=65535, step=1,
            key="pbmaster_ws_port",
            help="Port for the real-time WebSocket server (default: 8765). "
                 "Requires daemon restart to take effect.",
        )

        st.divider()

        # ── VPS Host Selection ──
        st.markdown("**Monitored VPS Hosts**")
        st.caption("Select which VPS servers PBMaster should monitor. "
                   "Default: all off.")

        available = pbmaster.available_hosts()
        current_enabled = pbmaster.enabled_hosts

        if not available:
            st.info("No VPS configured. Add VPS in **VPS Manager** first.")
        else:
            # Enable All / Disable All buttons
            _btn_cols = st.columns([0.2, 0.2, 0.6])
            with _btn_cols[0]:
                if st.button("Enable All", key="pbmaster_enable_all",
                             use_container_width=True):
                    for h in available:
                        st.session_state[f"pbmaster_host_{h}"] = True
                    st.rerun()
            with _btn_cols[1]:
                if st.button("Disable All", key="pbmaster_disable_all",
                             use_container_width=True):
                    for h in available:
                        st.session_state[f"pbmaster_host_{h}"] = False
                    st.rerun()

            # Individual host toggles
            for hostname in available:
                _hkey = f"pbmaster_host_{hostname}"
                if _hkey not in st.session_state:
                    st.session_state[_hkey] = hostname in current_enabled
                st.toggle(
                    f":material/computer: {hostname}",
                    key=_hkey,
                    help=pbgui_help.pbmaster_enabled_hosts,
                )

        st.divider()

        # ── Save button ──
        _new_enabled = {
            h for h in available
            if st.session_state.get(f"pbmaster_host_{h}", False)
        } if available else set()

        _pbmaster_dirty = (
            st.session_state.get("pbmaster_auto_restart", pbmaster.auto_restart) != pbmaster.auto_restart or
            st.session_state.get("pbmaster_monitor_interval", pbmaster.monitor_interval) != pbmaster.monitor_interval or
            st.session_state.get("pbmaster_ws_port", pbmaster.ws_port) != pbmaster.ws_port or
            _new_enabled != current_enabled
        )
        if st.button(":material/save:", key="pbmaster_save_config",
                     type="primary" if _pbmaster_dirty else "secondary"):
            pbmaster.auto_restart = st.session_state.pbmaster_auto_restart
            pbmaster.monitor_interval = st.session_state.pbmaster_monitor_interval
            pbmaster.ws_port = st.session_state.pbmaster_ws_port
            pbmaster.enabled_hosts = _new_enabled
            st.rerun()

    # Connection status
    if pbmaster.is_running() and pbmaster.pool:
        st.subheader("SSH Connections")
        summary = pbmaster.pool.get_status_summary()
        _conn_cols = st.columns(3)
        with _conn_cols[0]:
            st.metric("Total VPS", summary["total"])
        with _conn_cols[1]:
            st.metric("Connected", summary["connected"])
        with _conn_cols[2]:
            st.metric("Disconnected", summary["disconnected"] + summary["auth_failed"])

        if summary["connections"]:
            for hostname, info in sorted(summary["connections"].items()):
                status = info["status"]
                if status == "connected":
                    st.success(f"**{hostname}** ({info['ip']})", icon="✅")
                elif status == "auth_failed":
                    st.error(f"**{hostname}** ({info['ip']}): Auth failed", icon="🔒")
                else:
                    st.warning(
                        f"**{hostname}** ({info['ip']}): {info.get('last_error', 'disconnected')}",
                        icon="⚠️"
                    )
    else:
        st.info("Start PBMaster to see SSH connection status and service monitoring.")

    view_log_filtered("PBMaster")

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBGUI Services")

c_title, c_help = st.columns([0.95, 0.05], vertical_alignment="center")
with c_title:
    st.title("PBGUI Services")

_TABS = ["Overview", "PBRun", "PBRemote", "PBMon", "PBStat", "PBData", "PBCoinData", "PBMaster"]
active_tab = st.segmented_control("Tabs", _TABS, key="services_active_tab", default="Overview", label_visibility="collapsed")
if active_tab is None:
    active_tab = "Overview"

if active_tab == "Overview":
    with c_help:
        if st.button("📖", key="guide_btn_overview", help="Open Services help"):
            _help_modal("Services")
    overview()

elif active_tab == "PBRun":
    with st.sidebar:
        pbrun_overview(key_suffix="_det")
    with c_help:
        if st.button("📖", key="guide_btn_pbrun", help="Open PBRun help"):
            _help_modal("PBRun")
    pbrun_details()

elif active_tab == "PBRemote":
    with st.sidebar:
        api_sync = _pbremote_sidebar()
    with c_help:
        if st.button("📖", key="guide_btn_pbremote", help="Open PBRemote help"):
            _help_modal("PBRemote")
    if api_sync:
        st.warning("API not in sync with remote servers: " + ", ".join(s.name for s in api_sync))
    pbremote_details()

elif active_tab == "PBMon":
    with st.sidebar:
        pbmon_overview(key_suffix="_det")
    with c_help:
        if st.button("📖", key="guide_btn_pbmon", help="Open PBMon help"):
            _help_modal("PBMon")
    pbmon_details()

elif active_tab == "PBStat":
    with st.sidebar:
        pbstat_overview(key_suffix="_det")
    with c_help:
        if st.button("📖", key="guide_btn_pbstat", help="Open PBStat help"):
            _help_modal("PBStat")
    pbstat_details()

elif active_tab == "PBData":
    with st.sidebar:
        pbdata_overview(key_suffix="_det")
    with c_help:
        if st.button("📖", key="guide_btn_pbdata", help="Open PBData help"):
            _help_modal("PBData")
    pbdata_details()

elif active_tab == "PBCoinData":
    with st.sidebar:
        pbcoindata_overview(key_suffix="_det")
    with c_help:
        if st.button("📖", key="guide_btn_pbcoindata", help="Open PBCoinData help"):
            _help_modal("PBCoinData")
    pbcoindata_details()

elif active_tab == "PBMaster":
    with st.sidebar:
        pbmaster_overview(key_suffix="_det")
    with c_help:
        if st.button("📖", key="guide_btn_pbmaster", help="Open PBMaster help"):
            _help_modal("PBMaster")
    pbmaster_details()
