import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_pb7_installed, is_authenticted, get_navi_paths
from Config import BalanceCalculator

def balance_calculator():
    # Init balance calculator
    if "balance_calc" not in st.session_state:
        st.session_state.balance_calc = BalanceCalculator()
    balance_calc = st.session_state.balance_calc
    # View
    balance_calc.view()
    
# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv7 Balance Calculator")
st.header("PBv7 Balance Calculator", divider="red")

# Check if PB7 is installed
if not is_pb7_installed():
    st.warning('Passivbot Version 7.x is not installed', icon="⚠️")
    st.stop()

# Check if CoinData is configured
if st.session_state.pbcoindata.api_error:
    st.warning('Coin Data API is not configured / Go to Coin Data and configure your API-Key', icon="⚠️")
    st.stop()

balance_calculator()