import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, is_authenticted, get_navi_paths
from User import User, Users
from Exchange import Exchange, Exchanges, Spot, Passphrase
from PBRemote import PBRemote

@st.dialog("Delete User?")
def delete_user(user):
    st.warning(f"Delete User {user} ?", icon="⚠️")
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(":green[Yes]"):
            st.session_state.users.remove_user(user)
            st.session_state.ed_user_key += 1
            st.rerun()
    with col2:
        if st.button(":red[No]"):
            st.session_state.ed_user_key += 1
            st.rerun()

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
    # Init session states for keys
    if "api_wallet_address" in st.session_state:
        if st.session_state.api_wallet_address != user.wallet_address:
            user.wallet_address = st.session_state.api_wallet_address
    if "api_private_key" in st.session_state:
        if st.session_state.api_private_key != user.private_key:
            user.private_key = st.session_state.api_private_key
    if "api_is_vault" in st.session_state:
        if st.session_state.api_is_vault != user.is_vault:
            user.is_vault = st.session_state.api_is_vault
    if "api_passphrase" in st.session_state:
        if st.session_state.api_passphrase != user.passphrase:
            user.passphrase = st.session_state.api_passphrase
    if "api_secret" in st.session_state:
        if st.session_state.api_secret != user.secret:
            user.secret = st.session_state.api_secret
    if "api_exchange" in st.session_state:
        if st.session_state.api_exchange != user.exchange:
            user.exchange = st.session_state.api_exchange
    if "api_key" in st.session_state:
        if st.session_state.api_key != user.key:
            user.key = st.session_state.api_key
    col_1, col_2, col_3 = st.columns([1,1,1],vertical_alignment="bottom")
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
    with col_2:
        if user.exchange:
            index_exc = Exchanges.list().index(user.exchange)
        else:
            index_exc = 0
        st.selectbox('Exchange', Exchanges.list(), index=index_exc, key = "api_exchange", disabled=in_use)
    with col_3:
        if st.button("Test"):
            exchange = Exchange(user.exchange, user)
            balance_futures = exchange.fetch_balance('swap')
            if exchange.name in Spot.list():
                balance_spot = exchange.fetch_balance('spot')

    col_1, col_2, col_3 = st.columns([1,1,1],vertical_alignment="bottom")
    with col_1:
        if user.exchange == "hyperliquid":
            st.text_input("Wallet Address", value=user.wallet_address, key="api_wallet_address", help=None)
        else:
            st.text_input("API-Key", value=user.key, type="default", key="api_key", help=None)
    with col_2:
        if user.exchange == "hyperliquid":
            st.text_input("Private Key", value=user.private_key, type="password", key="api_private_key",help=None)
        else:
            st.text_input("API-Secret", value=user.secret, type="password", key="api_secret", help=None)
    with col_3:
        if user.exchange == "hyperliquid":
            st.checkbox("Vault", value=user.is_vault, key="api_is_vault", help=None)
        if user.exchange in Passphrase.list():
            st.text_input("Passphrase", value=user.passphrase, type="password", key="api_passphrase", help=None)
    col_1, col_2, col_3 = st.columns([1,1,1],vertical_alignment="bottom")
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
    multi_instances = st.session_state.multi_instances
    v7_instances = st.session_state.v7_instances
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
                if not instances.is_user_used(users.users[row].name) and not multi_instances.is_user_used(users.users[row].name) and not v7_instances.is_user_used(users.users[row].name):
                    delete_user(users.users[row].name)
    d = []
    for id, user in enumerate(users):
        in_use = False
        if instances.is_user_used(user.name) or multi_instances.is_user_used(user.name) or v7_instances.is_user_used(user.name):
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

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("API-Keys")
st.header("API-Keys", divider="red")

# Display Setup
if 'edit_user' in st.session_state:
    edit_user()
else:
    select_user()
