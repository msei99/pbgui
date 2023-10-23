import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from pbgui_func import set_page_config
import pbgui_help
from pathlib import Path
import shutil
import json
from datetime import datetime
import ccxt

# Cleanup session_state
def cleanup():
    del st.session_state.edit_api
    if "del_api" in st.session_state:
        del st.session_state.del_api
    if "new_user" in st.session_state:
        del st.session_state.new_user
    if "error_api" in st.session_state:
        del st.session_state.error_api
    if "error_setup" in st.session_state:
        del st.session_state.error_setup
    del st.session_state.keyfile
    if "swap_balance" in st.session_state:
        del st.session_state.swap_balance
    if "spot_balance" in st.session_state:
        del st.session_state.spot_balance

# handler for button clicks
def button_handler(user, button=None):
    if button == "back":
        cleanup()
    elif button == "edit":
        st.session_state.edit_api = user
    else:
        print("other button")

# Display api-keys
def display_api():
    col1, col2, col3 = st.columns([1.5,1,4])
    with col1:
        st.write("#### **User**")
    with col2:
        st.write("#### **Exchange**")
    with col3:
        st.write("#### **Edit**")
    for user in st.session_state.keyfile:
        col1, col2, col3 = st.columns([1.5,1,4])
        with col1:
            if "exchange" in st.session_state.keyfile[user]: user
        with col2:
            if "exchange" in st.session_state.keyfile[user]: st.session_state.keyfile[user]["exchange"]
        with col3:
            if "exchange" in st.session_state.keyfile[user]:
                st.button("Edit", key=f'edit {user}', on_click=button_handler, args=[user, "edit"])

# Save api-keys
def save_api(user, new_user, exchange, key, secret, passphrase):
    # Check if new_user already used
    if st.session_state.new_user:
        if new_user in st.session_state.keyfile:
            st.session_state.error_setup = f'User: {new_user} is used in other API-Key'
            return
    # Add/Edit new_user to api-key
    if not "del_api" in st.session_state:
        if user in st.session_state.keyfile:
            field = st.session_state.keyfile.pop(user)
        st.session_state.keyfile[new_user] = ({
            "exchange": exchange,
            "key": key,
            "secret": secret
        })
        if exchange in ["bitget", "okx", "kucoin"]:
            st.session_state.keyfile[new_user]["passphrase"] = passphrase
    # Backup api-keys and save new version
    now = datetime.now()
    date = now.strftime("%Y-%m-%d_%H:%M:%S")
    source = Path(f'{st.session_state.pbdir}/api-keys.json')
    destpath = Path(f'{st.session_state.pbgdir}/data/api-keys')
    destination = Path(f'{st.session_state.pbgdir}/data/api-keys/api-keys_{date}.json')
    if not destpath.exists():
        destpath.mkdir(parents=True)
    shutil.copy(source, destination)
    with Path(f'{st.session_state.pbdir}/api-keys.json').open("w", encoding="UTF-8") as f:
        json.dump(st.session_state.keyfile, f, indent=4)
    # Cleanup session_state
    cleanup()

# del user from api-keys
def del_api(user):
    if user in st.session_state.keyfile:
        st.session_state.keyfile.pop(user)
        st.session_state.del_api = True

# Get spot and future balance from exchange
def get_balance(exchange, key, secret, passphrase):
    if exchange == "kucoin":
        exchange = "kucoinfutures"
        print(exchange)
    exchange_class = getattr(ccxt, exchange)
    exc = exchange_class({
        'apiKey': key,
        'secret': secret,
        'password': passphrase,
    })
    try:
        exc.checkRequiredCredentials()
    except Exception as e:
        st.session_state.error_api = (str(e))
        return
    if exchange in ["binance","bybit"]:
        param = {"type":"spot"}
        try:
            balance = exc.fetchBalance(param)
            st.session_state.spot_balance = f'${round(balance["USDT"]["total"],2)}'
        except Exception as e:
            st.session_state.spot_balance = ':red[API-Error]'
            st.session_state.error_api = (str(e))
    if exchange in ["binance", "bybit", "bitget", "kucoinfutures", "okx"]:
        param = {"type":"swap"}
        try:
            balance = exc.fetchBalance(param)
            st.session_state.swap_balance = f'${round(balance["USDT"]["total"],2)}'
        except Exception as e:
            st.session_state.swap_balance = ':red[API-Error]'
            st.session_state.error_api = (str(e))

# Edit/Add/Del User/API
def edit_api(user):
    # Display Setup Error
    if "error_setup" in st.session_state:
        st.error(st.session_state.error_setup, icon="🚨")
    # Init variables
    if user == "new_user" or "del_api" in st.session_state:
        st.session_state.new_user = True
        exchange = ""
        key = ""
        secret = ""
        passphrase = ""
        exchange_index = 0
    else:
        exchange = st.session_state.keyfile[user]["exchange"]
        key = st.session_state.keyfile[user]["key"]
        secret = st.session_state.keyfile[user]["secret"]
        if "passphrase" in st.session_state.keyfile[user]:
            passphrase = st.session_state.keyfile[user]["passphrase"]
        else: 
            passphrase = ""
        if exchange == "binance":
            exchange_index = 0
        elif exchange == "bybit":
            exchange_index = 1
        elif exchange == "bitget":
            exchange_index = 2
        elif exchange == "okx":
            exchange_index = 3
        elif exchange == "kucoin":
            exchange_index = 4
    if "del_api" in st.session_state:
        st.write(f':red[User: {user} deleted]')
        st.write(f'Press :floppy_disk: for save')
        new_user = user
    else:
        new_user = st.text_input("User", value=user, help=None)
        exchange = st.selectbox('Exchange',["binance", "bybit", "bitget", "okx", "kucoin"], index=exchange_index, help=None)
        key = st.text_input("Key", value=key, help=None)
        secret = st.text_input("Secret", value=secret, help=None)
        if exchange in ["bitget", "okx", "kucoin"]:
            passphrase = st.text_input("Passphrase", value=passphrase, help=None)
        get_balance(exchange, key, secret, passphrase)
        col_f, col_s = st.columns([1,1])
        with col_f:
            if exchange in ["binance", "bybit", "bitget", "kucoin", "okx"]:
                st.markdown(f'### <center>Future Wallet Balance</center>', unsafe_allow_html=True)
                if "swap_balance" in st.session_state:
                    st.markdown(f'# <center>{st.session_state.swap_balance}</center>', unsafe_allow_html=True)
        with col_s:
            if exchange in ["binance","bybit"]:
                st.markdown(f'### <center>Spot Wallet Balance</center>', unsafe_allow_html=True)
                if "spot_balance" in st.session_state:
                    st.markdown(f'# <center>{st.session_state.spot_balance}</center>', unsafe_allow_html=True)
        # Display Error
        if "error_api" in st.session_state:
            st.markdown(":red[Error message from exchange:]", help=pbgui_help.api_error)
            st.error(st.session_state.error_api, icon="🚨")
    # Navigation
    with st.sidebar:
        st.button(":wastebasket:", key="del", on_click=del_api, args=[user])
        st.button(":floppy_disk:", key="save", on_click=save_api, args=[user, new_user, exchange, key, secret, passphrase])
        st.button(":back:", key="back", on_click=button_handler, args=[user, "back"])

set_page_config()

# Init Session State
if 'pbdir' not in st.session_state or 'pbgdir' not in st.session_state:
    switch_page("pbgui")

# Load api-keys
if not Path(f'{st.session_state.pbdir}/api-keys.json').exists():
    shutil.copy(Path(f'{st.session_state.pbdir}/api-keys.example.json'), Path(f'{st.session_state.pbdir}/api-keys.json'))
if not "keyfile" in st.session_state:
    with Path(f'{st.session_state.pbdir}/api-keys.json').open(encoding="UTF-8") as f:
        st.session_state.keyfile = json.load(f)

# Display Setup
if 'edit_api' in st.session_state:
    edit_api(st.session_state.edit_api)
else:
    display_api()
    st.button(":heavy_plus_sign: Add User/API", key='add', on_click=button_handler, args=["new_user", "edit"])
