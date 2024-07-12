import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized
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
    if "editor_select_multi_instance" in st.session_state:
        ed = st.session_state["editor_select_multi_instance"]
        for row in ed["edited_rows"]:
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_multi_instance = multi_instances.instances[row]
                st.rerun()
            if "Delete" in ed["edited_rows"][row]:
                if not "confirm" in st.session_state:
                    st.session_state.confirm_text = f':red[Delete selected instance ({multi_instances.instances[row].user})?]'
                    st.session_state.confirm = False
                    st.rerun()
                elif "confirm" in st.session_state:
                    if st.session_state.confirm:
                        multi_instances.remove(multi_instances.instances[row])
                        PBRun().restart_pbrun()
                        del st.session_state.confirm
                        del st.session_state.confirm_text
                        st.rerun()
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
    st.data_editor(data=d, height=36+(len(d))*35, use_container_width=True, key="editor_select_multi_instance", hide_index=None, column_order=None, column_config=column_config, disabled=['id','User'])
    

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
