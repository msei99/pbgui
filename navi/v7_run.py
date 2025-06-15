import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, is_pb7_installed, is_authenticted, get_navi_paths
from RunV7 import V7Instances , V7Instance
from BacktestV7 import BacktestV7Item
from Config import BalanceCalculator

def edit_v7_instance():
    # Init instance
    v7_instance = st.session_state.edit_v7_instance
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            v7_instance.clean_cf_session_state()
            del st.session_state.edit_v7_instance
            del st.session_state.v7_instances
            st.session_state.v7_instances = V7Instances()
            st.rerun()
        if st.button(":material/save:"):
            v7_instance.save()
        if st.button("Import"):
            v7_instance.import_instance()
        if st.button("Activate"):
            v7_instance.activate()
        if st.button("Backtest"):
            st.session_state.bt_v7 = BacktestV7Item(v7_instance.config.config_file)
            del st.session_state.edit_v7_instance
            del st.session_state.cf_data
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
            st.session_state.balance_calc = BalanceCalculator(v7_instance.config.config_file)
            st.switch_page(get_navi_paths()["V7_BALANCE_CALC"])
        if st.button("GridVis"):
            st.session_state.v7_grid_visualizer_config = v7_instance.config
            st.switch_page(get_navi_paths()["V7_GRID_VISUALIZER"])
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
    st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f"editor_select_v7_instance_{st.session_state.ed_key}", hide_index=None, column_order=None, column_config=column_config, disabled=['id','User'])
    
# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv7 Run")
st.header("PBv7 Run", divider="red")

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
    select_instance()
