import streamlit as st
from streamlit_autorefresh import st_autorefresh
from pbgui_func import set_page_config, is_session_state_initialized
from Instance import Instances
import pandas as pd


def bgcolor_positive_or_negative(value):
    bgcolor = "lightcoral" if value < 0 else "lightgreen"
    return f"background-color: {bgcolor};"

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
            with st.spinner('Initializing Instances...'):
                st.session_state.pbgui_instances = Instances()
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
            # 'Running': instance.is_running(),
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
            st.switch_page("pages/1_Single.py")
#            st.rerun()
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
        if st.button("Edit"):
            st.session_state.edit_instance = st.session_state.view_instance
            del st.session_state.view_instance
            del st.session_state.view_history
            st.switch_page("pages/1_Single.py")
        if st.button("View"):
            del st.session_state.view_history
            st.rerun()
    instance.compare_history()

set_page_config()

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

if 'view_history' in st.session_state:
    view_history()
elif 'view_instance' in st.session_state:
    view_instance()
else:
    select_instance()
