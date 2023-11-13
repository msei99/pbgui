import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from streamlit_autorefresh import st_autorefresh
from pbgui_func import set_page_config, upload_pbconfigdb
from Instance import Instances, Instance
from PBRun import PBRun
import pbgui_help


def select_instance():
    instances = st.session_state.pbgui_instances
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    if "confirm" in st.session_state:
        st.session_state.confirm = st.checkbox(st.session_state.confirm_text)
    if st.toggle("PBRun", value=PBRun().is_running(), key="pbrun", help=pbgui_help.pbrun):
        if not PBRun().is_running():
            PBRun().run()
            st.experimental_rerun()
    else:
        if PBRun().is_running():
            PBRun().stop()
            st.experimental_rerun()
    d = []
    column_config = {
        "Show": st.column_config.CheckboxColumn('Show', default=False),
        "id": None}
    for id, instance in enumerate(instances):
        d.append({
            'id': id,
            'View': False,
            'Edit': False,
            'Running': instance.is_running(),
            'User': instance.user,
            'Symbol': instance.symbol,
            'Market_type': instance.market_type,
            'Delete': False,
        })
    selected = st.data_editor(data=d, width=None, height=1024, use_container_width=True, key="editor_select_instance", hide_index=None, column_order=None, column_config=column_config, disabled=['id','Running','User','Symbol','Market_type'])
    for line in selected:
        if line["View"]:
            st.session_state.view_instance = instances.instances[line["id"]]
            if "confirm" in st.session_state:
                del st.session_state.confirm
                del st.session_state.confirm_text
            st.experimental_rerun()
        if line["Edit"]:
            st.session_state.edit_instance = instances.instances[line["id"]]
            if "confirm" in st.session_state:
                del st.session_state.confirm
                del st.session_state.confirm_text
            st.experimental_rerun()
        if line["Delete"]:
            if not "confirm" in st.session_state:
                st.session_state.confirm_text = f':red[Delete selected instance ?]'
                st.session_state.confirm = False
                st.experimental_rerun()
            elif "confirm" in st.session_state:
                if st.session_state.confirm:
                    instances.remove(instances.instances[line["id"]])
                    del st.session_state.confirm
                    del st.session_state.confirm_text
                    st.experimental_rerun()

def view_instance():
    # Init instance
    instance = st.session_state.view_instance
    # Navigation
    with st.sidebar:
        if st.button(":back:"):
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
#            del st.session_state.edit_instance
            st.experimental_rerun()
        instance.enabled = st.toggle("enable", value=instance.enabled, key="live_enabled", help=pbgui_help.instance_enable)
        if instance.enabled:
            if st.button("restart", help=pbgui_help.instance_restart):
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
            instance.co = st.number_input("COUNTDOWN_OFFSET", min_value=-1, max_value=59, value=instance.co, step=1, format="%d", key="live_co", help=pbgui_help.co)
            instance.leverage = st.number_input("LEVERAGE", min_value=2, max_value=20, value=instance.leverage, step=1, format="%d", key="live_lev", help=pbgui_help.lev)
            instance.assigned_balance = st.number_input("ASSIGNED_BALANCE", key="live_assigned_balance", min_value=0, step=500, value=instance.assigned_balance, help=pbgui_help.assigned_balance)
            instance.price_distance_threshold = round(st.number_input("PRICE_DISTANCE_THRESHOLD", key="live_price_distance_threshold", min_value=0.00, step=0.05, value=instance.price_distance_threshold, help=pbgui_help.price_distance_threshold),2)
            instance.price_precision = round(st.number_input("PRICE_PRECISION_MULTIPLIER", key="live_price_precision", format="%.4f", min_value=0.0000, step=0.0001, value=instance.price_precision, help=pbgui_help.price_precision),4)
            instance.price_step = round(st.number_input("PRICE_STEP_CUSTOM", key="live_price_step", format="%.3f", min_value=0.000, step=0.001, value=instance.price_step, help=pbgui_help.price_step),3)
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
#instances = st.session_state.pbgui_instances

if 'view_history' in st.session_state:
    view_history()
elif 'view_instance' in st.session_state:
    view_instance()
elif 'edit_instance' in st.session_state:
    edit_instance()
elif 'import_instance' in st.session_state:
    import_instance()
else:
    select_instance()
    col_addinst, col_import = st.columns([1,1])
    with col_addinst:
        if st.button(":heavy_plus_sign: Add Instance"):
            st.session_state.edit_instance = Instance()
            st.experimental_rerun()
    with col_import:
        if st.button(":heavy_plus_sign: Import Instances from Live Bot Manager (Old Run Module)"):
            st.session_state.import_instance = True
            st.experimental_rerun()
