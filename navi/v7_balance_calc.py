import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_pb7_installed, is_authenticted, get_navi_paths
from pbgui_func import PBGDIR, pb7dir
from BacktestV7 import BacktestV7Item, BacktestsV7, BacktestV7Queue, BacktestV7Results, ConfigV7Archives
from Config import ConfigV7, BalanceCalculator
import datetime
from Instance import Instance
from User import Users
from Exchange import Exchange, V7
import multiprocessing
import json

def bt_v7():
    # Init bt_v7
    bt_v7 = st.session_state.bt_v7
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            del st.session_state.bt_v7
            st.rerun()
        if st.button(":material/save:"):
            if bt_v7.name:
                with st.spinner("Saving..."):
                    bt_v7.save()
                    if "bt_v7_list" in st.session_state:
                        del st.session_state.bt_v7_list
            else:
                info_popup("Name is empty")
        if st.button("Import"):
            bt_v7.import_instance()
        if st.button("Results"):
            st.session_state.bt_v7_results = bt_v7.results
            del st.session_state.bt_v7
            st.rerun()
        if st.button("Queue"):
            del st.session_state.bt_v7
            st.session_state.bt_v7_queue = BacktestV7Queue()
            st.rerun()
        if st.button("Add to Backtest Queue"):
            if bt_v7.name:
                with st.spinner("Saving and adding to queue"):
                    bt_v7.save()
                    if "bt_v7_list" in st.session_state:
                        del st.session_state.bt_v7_list
                    bt_v7.save_queue()
                    info_popup(f"Added {bt_v7.name} to Queue")
                    # st.session_state.bt_v7_queue = BacktestV7Queue()
                    # del st.session_state.bt_v7
                    # st.rerun()
            else:
                if not bt_v7.name:
                    info_popup("Name is empty")
    st.subheader(f"Create/Edit: {bt_v7.name}")
    bt_v7.edit()

def bt_v7_list():
    # Init bt_v7_list
    if "bt_v7_list" not in st.session_state:
        st.session_state.bt_v7_list = BacktestsV7()
    bt_v7_list = st.session_state.bt_v7_list
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.session_state.bt_v7_list = BacktestsV7()
            st.rerun()
        if st.button("Config Archive"):
            st.session_state.config_v7_archives = ConfigV7Archives()
            st.rerun()
        if st.button("All Results"):
            results =  BacktestV7Results()
            results.results_path = f'{pb7dir()}/backtests/pbgui'
            results.name = "All Results"
            st.session_state.bt_v7_results = results
            st.rerun()    
        if st.button("Queue"):
            st.session_state.bt_v7_queue = BacktestV7Queue()
            st.rerun()
        if st.button("Add Backtest"):
            st.session_state.bt_v7 = BacktestV7Item()
            st.rerun()
    st.subheader("Available Configs")
    bt_v7_list.view_backtests()

def config_v7_archives():
    # Init bt_v7_list
    config_v7_archives = st.session_state.config_v7_archives
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            del st.session_state.config_v7_archives
            st.rerun()
        if st.button(":material/refresh:"):
            config_v7_archives.load()
            st.rerun()
        if st.button(":material/settings:"):
            st.session_state.setup_config_archive = True
            st.rerun()
        if st.button("Sync Github"):
            config_v7_archives.git_pull()
        if st.button("Push own Archive"):
            config_v7_archives.git_push()
    config_v7_archives.add()
    config_v7_archives.list()

def setup_config_archive():
    # Init bt_v7_list
    config_v7_archives = st.session_state.config_v7_archives
    # Navigation
    with st.sidebar:
        if st.button(":material/home:"):
            del st.session_state.setup_config_archive
            st.rerun()
        if st.button(":material/save:"):
            config_v7_archives.save_config()
            info_popup("Config saved")
    config_v7_archives.setup()

def config_v7_config_archive():
    # Init bt_v7_results
    config_v7_config_archive = st.session_state.config_v7_config_archive
    if not config_v7_config_archive.results:
        with st.spinner("Loading Results"):
            st.session_state.config_v7_config_archive.load()
    # Navigation
    with st.sidebar:
        if st.button(":material/refresh:"):
            config_v7_config_archive.results = []
            config_v7_config_archive.results_d = []
            st.rerun()
        if st.button(":material/home:"):
            del st.session_state.config_v7_config_archive
            del st.session_state.config_v7_archives
            st.rerun()
        if st.button(":material/arrow_upward_alt:"):
            del st.session_state.config_v7_config_archive
            st.rerun()
        if st.button("Queue"):
            st.session_state.bt_v7_queue = BacktestV7Queue()
            del st.session_state.config_v7_config_archive
            del st.session_state.config_v7_archives
            st.rerun()
        if st.button("BT selected"):
            config_v7_config_archive.backtest_selected_results()
        if st.button(":material/delete: selected"):
            config_v7_config_archive.remove_selected_results()
            config_v7_config_archive.results = []
            config_v7_config_archive.results_d = []
            st.rerun()
    st.subheader(f"Config Archive: {config_v7_config_archive.name}")
    config_v7_config_archive.view()

def balance_calculator():
    # Init balance calculator
    if "balance_calc" not in st.session_state:
        st.session_state.balance_calc = BalanceCalculator()
    balance_calc = st.session_state.balance_calc
    # View
    balance_calc.view()
    
    # # Init session state for balance calculator
    # if "edit_bc_config" in st.session_state:
    #     if st.session_state.edit_bc_config != json.dumps(bc_config.config, indent=4):
    #         try:
    #             bc_config.config = json.loads(st.session_state.edit_bc_config)
    #         except:
    #             error_popup("Invalid JSON")
    #     st.session_state.edit_bc_config = json.dumps(bc_config.config, indent=4)
    #     # if self.config.live.user in self._users.list_v7():
    #     #     self._user = self.config.live.user
    # else:
    #     st.session_state.edit_bc_config = ""
    # if "bc_exchange_id" in st.session_state:
    #     if st.session_state.bc_exchange_id != bc_exchange.id:
    #         bc_exchange = Exchange(st.session_state.bc_exchange_id, None)
    #         st.session_state.bc_exchange = bc_exchange

    # col1, col2 = st.columns([1, 1])
    # with col1:
    #     st.text_area(f'config', key="edit_bc_config", height=500)
    # with col2:
    #     st.markdown("### Balance Calculator")
    #     st.markdown("This tool allows you to calculate the balance for a given configuration.")
    #     st.markdown("You can edit the configuration in the left text area and click on 'Calculate' to see the results.")
    #     st.selectbox("Exchange", V7.list(), key="bc_exchange_id")
    #     if st.button("Calculate"):
    #         balance_long = 0.0
    #         balance_short = 0.0
    #         balance_long_d = []
    #         balance_short_d = []
    #         for coin in bc_config.live.approved_coins.long:
    #             min_order_price, price, contractSize, min_amount = bc_exchange.fetch_symbol_infos(coin)
    #             balance_coin = min_order_price / bc_config.bot.long.total_wallet_exposure_limit / bc_config.bot.long.entry_initial_qty_pct
    #             balance_long += balance_coin
    #             balance_long_d.append({
    #                 "coin": coin,
    #                 "currentPrice": price,
    #                 "contractSize": contractSize,
    #                 "min_amount": min_amount,
    #                 "min_order_price": min_order_price,
    #                 "balance_coin": balance_coin
    #             })
    #         for coin in bc_config.live.approved_coins.short:
    #             min_order_price, price, contractSize, min_amount = bc_exchange.fetch_symbol_infos(coin)
    #             balance_coin = min_order_price / bc_config.bot.short.total_wallet_exposure_limit / bc_config.bot.short.entry_initial_qty_pct
    #             balance_short += balance_coin
    #             balance_short_d.append({
    #                 "coin": coin,
    #                 "currentPrice": price,
    #                 "contractSize": contractSize,
    #                 "min_amount": min_amount,
    #                 "min_order_price": min_order_price,
    #                 "balance_coin": balance_coin
    #             })
    #         balance = balance_long + balance_short
    # if balance_long_d:
    #     st.dataframe(balance_long_d, hide_index=True)
    # if balance_short_d:
    #     st.dataframe(balance_short_d, hide_index=True)
    # st.write(f"**Total Balance for Long Positions:** {balance_long:.2f} USDT")
    # st.write(f"**Total Balance for Short Positions:** {balance_short:.2f} USDT")
    # st.write(f"**Total Balance Needed:** {balance:.2f} USDT")
                


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