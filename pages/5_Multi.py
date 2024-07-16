import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized, error_popup
from BacktestMulti import BacktestMultiItem
from Multi import MultiInstance, MultiInstances
from Instance import Instances, Instance
from PBRun import PBRun
from pathlib import PurePath

def edit_multi_config():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Init config
    multi_config = st.session_state.edit_multi_config
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.edit_multi_config
            st.rerun()
        if st.button(":floppy_disk:"):
            multi_config.save_config()
    symbol = PurePath(multi_config.config_file).stem
    st.header(f'{symbol}')
    multi_config.edit_config()

def edit_multi_instance():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Init instance
    multi_instance = st.session_state.edit_multi_instance
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
            del st.session_state.edit_multi_instance
            del st.session_state.multi_instances
            st.session_state.multi_instances = MultiInstances()
            st.rerun()
        if st.button(":floppy_disk:"):
            multi_instance.save()
        if st.button("Activate"):
            multi_instance.activate()
        if st.button("Refresh from Disk"):
            del st.session_state.pbgui_instances
            with st.spinner('Initializing Instances...'):
                st.session_state.pbgui_instances = Instances()
            multi_instance.initialize()
            st.rerun()
        if st.button("Backtest"):
            del st.session_state.edit_multi_instance
            st.session_state.bt_multi = BacktestMultiItem()
            st.session_state.bt_multi.create_from_multi(multi_instance.instance_path)
            if "bt_multi_queue" in st.session_state:
                del st.session_state.bt_multi_queue
            if "bt_multi_results" in st.session_state:
                del st.session_state.bt_multi_results
            if "bt_multi_edit_symbol" in st.session_state:
                del st.session_state.bt_multi_edit_symbol
            st.switch_page("pages/6_Multi Backtest.py")
    multi_instance.edit()
    if multi_instance.default_config.preview_grid:
        if "preview_grid_instance" not in st.session_state:
            st.session_state.preview_grid_instance = Instance()
        instance = st.session_state.preview_grid_instance
        instance.config = multi_instance.default_config.config
        instance.user = multi_instance.user
        instance.symbol = "BTCUSDT"
        instance.market_type = "futures"
        instance.view_grid(10000)

@st.experimental_dialog("Delete Instance?")
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
                st.session_state.multi_instances.remove(instance)
            with st.spinner('Start Services...'):
                services.start_all_was_running()
            st.session_state.ed_key += 1
            st.rerun()
    with col2:
        if st.button(":red[No]"):
            st.session_state.ed_key += 1
            st.rerun()

def select_instance():
    # Init MultiInstances
    multi_instances = st.session_state.multi_instances
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    if "confirm" in st.session_state:
        st.session_state.confirm = st.checkbox(st.session_state.confirm_text)
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            del st.session_state.pbgui_instances
            with st.spinner('Initializing Instances...'):
                st.session_state.pbgui_instances = Instances()
            del st.session_state.multi_instances
            with st.spinner('Initializing Multi Instances...'):
                st.session_state.multi_instances = MultiInstances()
                multi_instances = st.session_state.multi_instances
        if st.button("Add"):
            st.session_state.edit_multi_instance = MultiInstance()
            st.rerun()
        if st.button("Activate ALL"):
            multi_instances.activate_all()
            st.rerun()
    if not "ed_key" in st.session_state:
        st.session_state.ed_key = 0
    if f'editor_select_multi_instance_{st.session_state.ed_key}' in st.session_state:
        ed = st.session_state[f"editor_select_multi_instance_{st.session_state.ed_key}"]
        for row in ed["edited_rows"]:
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_multi_instance = multi_instances.instances[row]
                st.rerun()
            if "Delete" in ed["edited_rows"][row]:
                instance = multi_instances.instances[row]
                running_on = instance.is_running_on()
                if running_on:
                    error_popup(f"Instance {instance.user} is running on {running_on} and can't be deleted")
                    st.session_state.ed_key += 1
                else:
                    delete_instance(instance)
    d = []
    for id, instance in enumerate(multi_instances):
        twe_str: str = (f"{ 'L=' + str( round(instance.TWE_long,2)) if instance.long_enabled else ''}"
                        f"{' | ' if instance.long_enabled and instance.short_enabled else ''}"
                        f"{ 'S=' + str( round(instance.TWE_short,2)) if instance.short_enabled else ''}")
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
            'User': instance.user,
            'Enabled On': instance.enabled_on,
            'TWE': twe_str,
            'AU': bool(instance.loss_allowance_pct > 0.0),
            'Version': instance.version,
            'Remote': remote_str,
            'Remote Version': instance.running_version,
            'Delete': False,
        })
    column_config = {
        "id": None}
    st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key=f"editor_select_multi_instance_{st.session_state.ed_key}", hide_index=None, column_order=None, column_config=column_config, disabled=['id','User'])
    

set_page_config()

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

if 'edit_multi_config' in st.session_state:
    edit_multi_config()
elif 'edit_multi_instance' in st.session_state:
    edit_multi_instance()
else:
    select_instance()
