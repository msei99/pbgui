import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from streamlit_autorefresh import st_autorefresh
from pbgui_func import set_page_config
from Instance import Instances, Instance
from User import Users
import pbgui_help

# Cleanup session_state
def cleanup():
    if "view_instance" in st.session_state:
        del st.session_state.view_instance
    if "edit_instance" in st.session_state:
        del st.session_state.edit_instance

# handler for button clicks
def button_handler(button=None):
    if button == "back_instance":
        cleanup()
    if button == "back_history":
        del st.session_state.view_history
    if button == "back_edit_instance":
        cleanup()
        del st.session_state.pbgui_instances
    if button == "add_instance":
        cleanup()
        st.session_state.edit_instance = Instance()
    if button == "view_history":
        st.session_state.view_history = True
    if button == "save_instance":
        st.session_state.edit_instance.save()
        del st.session_state.edit_instance
        del st.session_state.pbgui_instances
    if button == "update_symbols":
        st.session_state.edit_instance.update_symbols()

def select_instance():
    instances = st.session_state.pbgui_instances
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    if "confirm" in st.session_state:
        st.session_state.confirm = st.checkbox(st.session_state.confirm_text)
    d = []
    column_config = {
        "Show": st.column_config.CheckboxColumn('Show', default=False),
        "id": None}
    for id, instance in enumerate(instances):
        d.append({
            'id': id,
            'View': False,
            'Edit': False,
            'User': instance.user,
            'Symbol': instance.symbol,
            'Market_type': instance.market_type,
            'Delete': False,
        })
    selected = st.data_editor(data=d, width=None, height=None, use_container_width=True, key="editor_select_instance", hide_index=None, column_order=None, column_config=column_config, disabled=['id','User','Symbol','Market_type'])
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
        st.button(":back:", key="back_instance", on_click=button_handler, args=["back_instance"])
        st.button("History", key="history", on_click=button_handler, args=["view_history"])
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
        st.button(":back:", key="back_history", on_click=button_handler, args=["back_history"])
    instance.compare_history()

def edit_instance():
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    # Init instance
    instance = st.session_state.edit_instance
    # Init users
    users = Users()
    if not instance.user:
        instance.user = users.list()[0]
    # Navigation
    with st.sidebar:
        st.button(":back:", key="back_edit_instance", on_click=button_handler, args=["back_edit_instance"])
        st.button(":floppy_disk:", key="save_instance", on_click=button_handler, args=["save_instance"])
    col_1, col_2, col_3 = st.columns([1,1,1])
    with col_1:
        instance.user = st.selectbox('User',users.list(), index = users.list().index(instance.user))
    with col_2:
        instance.symbol = st.selectbox('SYMBOL', instance.symbols, index=instance.symbols.index(instance.symbol))
        st.button("Update Symbols from Exchange", key="update_symbols", on_click=button_handler, args=["update_symbols"])
    with col_3:
        instance.market_type = st.radio("MARKET_TYPE", instance.market_types)
        with st.expander("Advanced configurations", expanded=False):
            instance.ohlcv = st.checkbox("OHLCV", value=instance.ohlcv, key="live_ohlcv", help=pbgui_help.ohlcv)
            instance.leverage = st.number_input("LEVERAGE", min_value=2, max_value=20, value=instance.leverage, step=1, format="%d", key="live_lev", help=pbgui_help.lev)
            instance.assigned_balance = st.number_input("ASSIGNED_BALANCE", key="live_assigned_balance", min_value=0, step=500, value=instance.assigned_balance, help=pbgui_help.assigned_balance)
            instance.price_distance_threshold = round(st.number_input("PRICE_DISTANCE_THRESHOLD", key="live_price_distance_threshold", min_value=0.00, step=0.05, value=instance.price_distance_threshold, help=pbgui_help.price_distance_threshold),2)
            instance.price_precision = round(st.number_input("PRICE_PRECISION_MULTIPLIER", key="live_price_precision", format="%.4f", min_value=0.0000, step=0.0001, value=instance.price_precision, help=pbgui_help.price_precision),4)
            instance.price_step = round(st.number_input("PRICE_STEP_CUSTOM", key="live_price_step", format="%.3f", min_value=0.000, step=0.001, value=instance.price_step, help=pbgui_help.price_step),3)
    instance.edit_config()

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
else:
    select_instance()
    st.button(":heavy_plus_sign: Add Instance", key='add', on_click=button_handler, args=["add_instance"])