import streamlit as st
from pbgui_func import set_page_config, is_session_state_initialized
from User import User, Users
from Exchange import Exchange, Exchanges, Spot, Passphrase
from PBRemote import PBRemote

def edit_user():
    # Init
    user = st.session_state.edit_user
    users = st.session_state.users
    instances = st.session_state.pbgui_instances
    in_use = instances.is_user_used(user.name)
    balance_futures = None
    balance_spot = None
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    with st.sidebar:
        if st.button(":back:"):
            if "error" in st.session_state:
                del st.session_state.error
            del st.session_state.edit_user
            del st.session_state.users
            with st.spinner('Initializing Users...'):
               st.session_state.users = Users()

            st.rerun()
        if not in_use and not "error" in st.session_state:
            if st.button(":wastebasket:"):
                users.users.remove(user)
                users.save()
                if "error" in st.session_state:
                    del st.session_state.error
                del st.session_state.edit_user
                del st.session_state.users
                # cleanup for Remote Server Manager
                if "remote" in st.session_state:
                    del st.session_state.remote
                PBRemote().restart()
                st.rerun()
        if user.name and not "error" in st.session_state:
            if st.button(":floppy_disk:"):
                if not users.has_user(user):
                    users.users.append(user)
                users.save()
                # cleanup for Remote Server Manager
                # if "remote" in st.session_state:
                #     del st.session_state.remote
                # PBRemote().restart()
                # if "pbgui_instances" in st.session_state:
                #     del st.session_state.pbgui_instances
    col_1, col_2, col_3 = st.columns([1,1,1])
    with col_1:
        new_name = st.text_input("Username", value=user.name, max_chars=32, type="default", help=None, disabled=in_use)
        if new_name != user.name:
            user.name = new_name
            if users.has_user(user):
                st.session_state.error = "Username already in use"
            else:
                if "error" in st.session_state:
                    del st.session_state.error
            st.rerun()
        new_key = st.text_input("API-Key", value=user.key, type="default", help=None)
        if new_key != user.key:
            user.key = new_key
            st.rerun()
    with col_2:
        if user.exchange:
            index_exc = Exchanges.list().index(user.exchange)
        else:
            index_exc = 0
        new_exchange = st.selectbox('Exchange', Exchanges.list(), index=index_exc, disabled=in_use)
        if new_exchange != user.exchange:
            user.exchange = new_exchange
            st.rerun()
        new_secret = st.text_input("API-Secret", value=user.secret, type="password", help=None)
        if new_secret != user.secret:
            user.secret = new_secret
            st.rerun()
    with col_3:
        st.write("## ")
        if st.button("Test"):
            exchange = Exchange(user.exchange, user)
            balance_futures = exchange.fetch_balance('swap')
            if exchange.name in Spot.list():
                balance_spot = exchange.fetch_balance('spot')
        if user.exchange in Passphrase.list():
            new_passphrase = st.text_input("Passphrase", value=user.passphrase, type="password", help=None)
            if new_passphrase != user.passphrase:
                user.passphrase = new_passphrase
                st.rerun()
    with col_1:
        st.markdown(f'### <center>Futures Wallet Balance</center>', unsafe_allow_html=True)
        if type(balance_futures) == float:
            st.markdown(f'# <center>{balance_futures}</center>', unsafe_allow_html=True)
        elif balance_futures:
            st.error(balance_futures, icon="🚨")    
    with col_2:
        if user.exchange in Spot.list():
            st.markdown(f'### <center>Spot Wallet Balance</center>', unsafe_allow_html=True)
            if type(balance_spot) == float:
                st.markdown(f'# <center>{balance_spot}</center>', unsafe_allow_html=True)
            elif balance_spot:
                st.error(balance_spot, icon="🚨")    

def select_user():
    # Init
    users = st.session_state.users
    instances = st.session_state.pbgui_instances
    if not "ed_user_key" in st.session_state:
        st.session_state.ed_user_key = 0
    with st.sidebar:
        if st.button("Add"):
            st.session_state.edit_user = User()
            st.rerun()
    if f'editor_{st.session_state.ed_user_key}' in st.session_state:
        ed = st.session_state[f'editor_{st.session_state.ed_user_key}']
        for row in ed["edited_rows"]:
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_user = users.users[row]
                st.rerun()
            if "Delete" in ed["edited_rows"][row]:
                if not instances.is_user_used(users.users[row].name):
                    users.users.remove(users.users[row])
                    users.save()
                st.session_state.ed_user_key += 1
                st.rerun()
    d = []
    for id, user in enumerate(users):
        in_use = False
        if instances.is_user_used(user.name):
            in_use = None
        d.append({
            'id': id,
            'Edit': False,
            'User': user.name,
            'Exchange': user.exchange,
            'Delete': in_use,
        })
    column_config = {
        "id": None}
    st.data_editor(data=d, width=None, height=(len(users.users)+1)*36, use_container_width=True, key=f'editor_{st.session_state.ed_user_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','User','Exchange',])

set_page_config()

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

# Display Setup
if 'edit_user' in st.session_state:
    edit_user()
else:
    select_user()
