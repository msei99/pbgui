import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, is_pb7_installed, is_authenticted, get_navi_paths, render_header_with_guide, redirect_to_fastapi_v7_run
from RunV7 import V7Instances , V7Instance
from BacktestV7 import BacktestV7Item
from Config import BalanceCalculator
from pathlib import Path


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
def _run_v7_help_modal(default_topic: str = "Run"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="run_v7_help_lang")
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
        key="run_v7_help_sel",
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

def edit_v7_instance():
    # Init instance
    v7_instance = st.session_state.edit_v7_instance
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            del st.session_state.edit_v7_instance
            redirect_to_fastapi_v7_run()
        if st.button(":material/save:"):
            v7_instance.save()
        if st.button("Import"):
            v7_instance.import_instance()
        if st.button("Activate"):
            v7_instance.activate()
        if st.button("Backtest"):
            st.session_state.bt_v7 = BacktestV7Item(v7_instance.config.config_file)
            st.session_state.bt_v7.config.backtest.end_date = "now"
            del st.session_state.edit_v7_instance
            if "bt_v7_queue" in st.session_state:
                del st.session_state.bt_v7_queue
            if "bt_v7_results" in st.session_state:
                del st.session_state.bt_v7_results
            if "bt_v7_edit_symbol" in st.session_state:
                del st.session_state.bt_v7_edit_symbol
            if "config_v7_archives" in st.session_state:
                del st.session_state.config_v7_archives
            if "config_v7_config_archive" in st.session_state:
                del st.session_state.config_v7_config_archive
            st.switch_page(get_navi_paths()["V7_BACKTEST"])
        if st.button("Caclulate Balance"):
            exchange_id = v7_instance._users.find_exchange(v7_instance.user)
            st.session_state.bc_context_exchanges = [exchange_id] if exchange_id else []
            st.session_state.balance_calc = BalanceCalculator(v7_instance.config.config_file)
            st.switch_page(get_navi_paths()["V7_BALANCE_CALC"])
        if st.button("Strategy Explorer"):
            st.session_state.v7_strategy_explorer_config = v7_instance.config
            st.switch_page(get_navi_paths()["V7_STRATEGY_EXPLORER"])
            st.stop()
    v7_instance.edit()

@st.dialog("Delete Instance?")
def delete_instance(instance):
    st.warning(f"Delete Instance {instance.user} ?", icon="⚠️")
    # reason = st.text_input("Because...")
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(":green[Yes]"):
            services = st.session_state.services
            with st.spinner('Stop Services...'):
                services.stop_all_started()
            with st.spinner('Delete Instance...'):
                st.session_state.v7_instances.remove(instance)
            with st.spinner('Start Services...'):
                services.start_all_was_running()
            st.session_state.ed_key += 1
            st.rerun()
    with col2:
        if st.button(":red[No]"):
            st.session_state.ed_key += 1
            st.rerun()

def select_instance():
    # Init V7Instances
    v7_instances = st.session_state.v7_instances
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            del st.session_state.v7_instances
            if "remote" in st.session_state:
                del st.session_state.remote
            with st.spinner('Initializing V7 Instances...'):
                st.session_state.v7_instances = V7Instances()
                v7_instances = st.session_state.v7_instances
        if st.button("Add"):
            st.session_state.edit_v7_instance = V7Instance()
            st.rerun()
        if st.button("Activate ALL"):
            v7_instances.activate_all()
            st.rerun()
    if not "ed_key" in st.session_state:
        st.session_state.ed_key = 0
    if f'editor_select_v7_instance_{st.session_state.ed_key}' in st.session_state:
        ed = st.session_state[f"editor_select_v7_instance_{st.session_state.ed_key}"]
        for row in ed["edited_rows"]:
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_v7_instance = v7_instances.instances[row]
                st.rerun()
            if "Delete" in ed["edited_rows"][row]:
                instance = v7_instances.instances[row]
                running_on = instance.is_running_on()
                if running_on:
                    error_popup(f"Instance {instance.user} is running on {running_on} and can't be deleted")
                    st.session_state.ed_key += 1
                else:
                    delete_instance(instance)
    d = []
    for id, instance in enumerate(v7_instances):
        twe_str: str = (f"{ 'L=' + str( round(instance.config.bot.long.total_wallet_exposure_limit,2)) if instance.config.bot.long.n_positions > 0 else ''}"
                        f"{' | ' if instance.config.bot.long.n_positions > 0 and instance.config.bot.short.n_positions > 0 else ''}"
                        f"{ 'S=' + str( round(instance.config.bot.short.total_wallet_exposure_limit,2)) if instance.config.bot.short.n_positions > 0 else ''}")
        running_on = instance.is_running_on()
        if instance.enabled_on in running_on and (instance.version == instance.running_version):
            remote_str = f'✅ Running {instance.is_running_on()}'
        elif running_on:
            remote_str = f'🔄 Running {running_on}'
        elif instance.enabled_on != 'disabled':
            remote_str = '🔄 Activation required'
        else:
            remote_str = '❌'
        d.append({
            'id': id,
            'Edit': False,
            'User': instance.config.live.user,
            'Enabled On': instance.config.pbgui.enabled_on,
            'TWE': twe_str,
            'Version': instance.config.pbgui.version,
            'Remote': remote_str,
            'Remote Version': instance.running_version,
            'Note': instance.config.pbgui.note,
            'Delete': False,
        })
    column_config = {
        "id": None}
    st.data_editor(data=d, height=36+(len(d))*35, key=f"editor_select_v7_instance_{st.session_state.ed_key}", hide_index=None, column_order=None, column_config=column_config, disabled=['id','User'])
    
# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv7 Run")
render_header_with_guide(
    "PBv7 Run",
    guide_callback=lambda: _run_v7_help_modal(default_topic="PBv7 Run"),
    guide_key="run_v7_header_help_btn",
)

# Check if PB7 is installed
if not is_pb7_installed():
    st.warning('Passivbot Version 7.x is not installed', icon="⚠️")
    st.stop()

# Check if CoinData is configured
if st.session_state.pbcoindata.api_error:
    st.warning('Coin Data API is not configured / Go to Coin Data and configure your API-Key', icon="⚠️")
    st.stop()

if 'edit_v7_instance' in st.session_state:
    edit_v7_instance()
else:
    # Handle deep-link from FastAPI v7_run page (add_instance or edit_instance)
    _add = st.query_params.get("add_instance", "")
    if _add == "1":
        st.query_params.pop("add_instance", None)
        st.session_state.edit_v7_instance = V7Instance()
        st.rerun()
    _edit_name = st.query_params.get("edit_instance", "") or st.session_state.pop("_relay_edit_instance", "")
    if _edit_name and 'v7_instances' in st.session_state:
        for _inst in st.session_state.v7_instances.instances:
            if _inst.user == _edit_name:
                st.session_state.edit_v7_instance = _inst
                st.query_params.pop("edit_instance", None)
                st.rerun()
    # No edit mode — redirect to FastAPI list page
    redirect_to_fastapi_v7_run()
