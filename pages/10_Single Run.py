import streamlit as st
from pbgui_func import set_page_config, upload_pbconfigdb, is_session_state_initialized, info_popup, error_popup, is_pb_installed
from Instance import Instances, Instance
from Backtest import BacktestItem
import pbgui_help

@st.dialog("Delete Instance?")
def delete_instance(instance):
    st.warning(f"Delete Instance {instance.user} {instance.symbol} {instance.market_type} ?", icon="⚠️")
    # reason = st.text_input("Because...")
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(":green[Yes]"):
            services = st.session_state.services
            with st.spinner('Stop Services...'):
                services.stop_all_started()
            with st.spinner('Delete Instance...'):
                st.session_state.pbgui_instances.remove(instance)
            with st.spinner('Start Services...'):
                services.start_all_was_running()
            st.session_state.ed_key += 1
            st.rerun()
    with col2:
        if st.button(":red[No]"):
            st.session_state.ed_key += 1
            st.rerun()

def bgcolor_positive_or_negative(value):
    bgcolor = "lightcoral" if value < 0 else "lightgreen"
    return f"background-color: {bgcolor};"

def select_instance():
    # Init Instances
    if "pbgui_instances" not in st.session_state:
        return
    instances = st.session_state.pbgui_instances
    # Init PBremote
    pbremote = st.session_state.pbremote
    # Navigation
    with st.sidebar:
        if st.button(":recycle:"):
            st.rerun()
        if st.button("Add"):
            st.session_state.edit_instance = Instance()
            st.rerun()
        if st.button("Activate ALL"):
            for instance in instances:
                if not instance.multi:
                    # Create running_on
                    running_on = []
                    if pbremote.local_run.instances_status_single.is_running(f'{instance.user}_{instance.symbol}_{instance.market_type}'):
                        running_on.append(pbremote.name)
                    for server in pbremote.list():
                        if pbremote.find_server(server).instances_status_single.is_running(f'{instance.user}_{instance.symbol}_{instance.market_type}'):
                            running_on.append(server)
                    # Find running_version
                    if instance.enabled_on == pbremote.name:
                        running_version = pbremote.local_run.instances_status_single.find_version(f'{instance.user}_{instance.symbol}_{instance.market_type}')
                    elif instance.enabled_on in pbremote.list():
                        running_version = pbremote.find_server(instance.enabled_on).instances_status_single.find_version(f'{instance.user}_{instance.symbol}_{instance.market_type}')
                    else:
                        running_version = 0
                    # Activate
                    if instance.enabled_on == 'disabled' and running_on:
                        pbremote.local_run.activate(f'{instance.user}_{instance.symbol}_{instance.market_type}', False)
                    elif instance.enabled_on != 'disabled' and instance.enabled_on not in running_on:
                        pbremote.local_run.activate(f'{instance.user}_{instance.symbol}_{instance.market_type}', False)
                    elif running_on and (instance.version != running_version):
                        pbremote.local_run.activate(f'{instance.user}_{instance.symbol}_{instance.market_type}', False)
            # st.rerun()
        if st.button("Refresh from Disk"):
            del st.session_state.pbgui_instances
            with st.spinner('Initializing Instances...'):
                st.session_state.pbgui_instances = Instances()
            st.rerun()
    if not "ed_key" in st.session_state:
        st.session_state.ed_key = 0
    if f'editor_select_instance_{st.session_state.ed_key}' in st.session_state:
        ed = st.session_state[f"editor_select_instance_{st.session_state.ed_key}"]
        for row in ed["edited_rows"]:
            selected_row = st.session_state.edit_single_instances_d[row]['id']
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_instance = instances.instances[selected_row]
                st.rerun()
            if "Delete" in ed["edited_rows"][row]:
                instance = instances.instances[selected_row]
                if st.session_state.edit_single_instances_d[row]['Enabled On'] != 'disabled':
                    error_popup(f"Instance {instance.user} {instance.symbol} {instance.market_type} is running on {st.session_state.edit_single_instances_d[row]['Enabled On']} and can't be deleted")
                    st.session_state.ed_key += 1
                else:
                    delete_instance(instances.instances[selected_row])
    if "editor_select_instance_multi" in st.session_state:
        ed = st.session_state["editor_select_instance_multi"]
        for row in ed["edited_rows"]:
            selected_row = st.session_state.edit_single_instances_d_multi[row]['id']
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_instance = instances.instances[selected_row]
                st.rerun()
    d = []
    d_multi = []
    for id, instance in enumerate(instances):
        # Find running_version
        if instance.enabled_on == pbremote.name:
            running_version = pbremote.local_run.instances_status_single.find_version(f'{instance.user}_{instance.symbol}_{instance.market_type}')
        elif instance.enabled_on in pbremote.list():
            running_version = pbremote.find_server(instance.enabled_on).instances_status_single.find_version(f'{instance.user}_{instance.symbol}_{instance.market_type}')
        else:
            running_version = 0
        # Create running_on
        running_on = []
        if pbremote.local_run.instances_status_single.is_running(f'{instance.user}_{instance.symbol}_{instance.market_type}'):
            running_on.append(pbremote.name)
        for server in pbremote.list():
            if pbremote.find_server(server).instances_status_single.is_running(f'{instance.user}_{instance.symbol}_{instance.market_type}'):
                running_on.append(server)
        # Create remote_str
        if instance.enabled_on in running_on and (instance.version == running_version):
            remote_str = f'✅ Running {running_on}'
        elif running_on:
            remote_str = f'🔄 Running {running_on}'
        elif instance.enabled_on != 'disabled':
            remote_str = '🔄 Activation required'
        else:
            remote_str = '❌'
        if not instance.multi:
            d.append({
                'id': id,
                'Edit': False,
                'User': instance.user,
                'Symbol': instance.symbol,
                'Market_type': instance.market_type,
                'Enabled On': instance.enabled_on,
                'Version': instance.version,
                'Remote': remote_str,
                'Remote Version': running_version,
                'Delete': False,
            })
        else:
            d_multi.append({
                'id': id,
                'Edit': False,
                'User': instance.user,
                'Symbol': instance.symbol,
                'Market_type': instance.market_type,
                'Enabled On': instance.enabled_on,
            })
    st.session_state.edit_single_instances_d = d
    st.session_state.edit_single_instances_d_multi = d_multi
    column_config = {
        "id": None}
    st.header("Single Instances")
    st.data_editor(data=d, width=None, height=36+(len(d))*35, use_container_width=True, key=f"editor_select_instance_{st.session_state.ed_key}", hide_index=None, column_order=None, column_config=column_config, disabled=['id','User','Symbol','Market_type','Enabled On','Version','Remote','Remote Version'])
    st.header("Instances used in Multi configuration")
    st.data_editor(data=d_multi, width=None, height=36+(len(d_multi))*35, use_container_width=True, key="editor_select_instance_multi", hide_index=None, column_order=None, column_config=column_config, disabled=['id','User','Symbol','Market_type','Enabled On'])

def edit_instance():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Init instance
    instance = st.session_state.edit_instance
    # Init PBremote
    pbremote = st.session_state.pbremote
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
            if instance.symbol == "Select Symbol":
                error_popup("Symbol not selected")
            elif not instance._config.config:
                error_popup("Config is empty")
            elif "error_config" in st.session_state:
                error_popup(st.session_state.error_config)
            else:
                st.session_state.edit_instance.save()
                if st.session_state.edit_instance not in st.session_state.pbgui_instances.instances:
                    st.session_state.pbgui_instances.instances.append(st.session_state.edit_instance)
                info_popup("Instance saved")
        if st.button("Activate"):
            pbremote.local_run.activate(f'{instance.user}_{instance.symbol}_{instance.market_type}', False)
        if st.button("Backtest"):
            st.session_state.my_bt = BacktestItem(instance._config.config)
            st.session_state.my_bt.user = instance.user
            st.session_state.my_bt.symbol = instance.symbol
            st.session_state.my_bt.market_type = instance.market_type
            del st.session_state.edit_instance
            if "bt_queue" in st.session_state:
                del st.session_state.bt_queue
            if "bt_compare" in st.session_state:
                del st.session_state.bt_compare
            st.switch_page("pages/3_Backtest.py")
        source_name = st.text_input('pbconfigdb by [Scud](%s)' % "https://pbconfigdb.scud.dedyn.io/", value="PBGUI", max_chars=16, key="name_input", help=pbgui_help.upload_pbguidb)
        if not "error_config" in st.session_state and not instance.symbol == "Select Symbol" and instance._config.config:
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
            enabled_on = ["disabled",pbremote.name] + pbremote.list()
            enabled_on_index = enabled_on.index(instance.enabled_on)
            st.selectbox('Enabled on',enabled_on, index = enabled_on_index, key="edit_instance_enabled_on")
    with col_2:
        st.number_input("config version", min_value=instance.version, value=instance.version, step=1, format="%.d", key="edit_instance_version", help=pbgui_help.config_version)
    instance.view_log()

set_page_config()

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

# Check if PB6 is installed
if not is_pb_installed():
    st.warning('Passivbot Version 6.x is not installed', icon="⚠️")
    st.stop()

if 'edit_instance' in st.session_state:
    edit_instance()
else:
    select_instance()
