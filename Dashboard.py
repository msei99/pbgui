import streamlit as st
from pathlib import Path
import json
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta, MO
import numpy as np
from Exchange import Exchange
from pbgui_func import PBGDIR
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from Database import Database
import time

class Dashboard():

    # Periods
    @property
    def now(self): return datetime.now()
    @property
    def now_ts(self): return int(self.now.timestamp()) * 1000
    @property
    def today(self): return date.today()
    @property
    def today_ts(self): return int(time.mktime(self.today.timetuple())) * 1000
    @property
    def yesterday(self): return self.today - timedelta(days = 1)
    @property
    def yesterday_ts(self): return int(time.mktime(self.yesterday.timetuple())) * 1000
    @property
    def lastMonday(self): return self.today + relativedelta(weekday=MO(-1))
    @property
    def lastMonday_ts(self): return int(time.mktime(self.lastMonday.timetuple())) * 1000
    @property
    def lastWeekMonday(self): return self.lastMonday - timedelta(days = 7)
    @property
    def lastWeekMonday_ts(self): return int(time.mktime(self.lastWeekMonday.timetuple())) * 1000
    @property
    def thisMonth(self): return self.today + relativedelta(day=1)
    @property
    def thisMonth_ts(self): return int(time.mktime(self.thisMonth.timetuple())) * 1000
    @property
    def lastMonth(self): return self.thisMonth - relativedelta(months=1)
    @property
    def lastMonth_ts(self): return int(time.mktime(self.lastMonth.timetuple())) * 1000
    @property
    def thisYear(self): return self.today + relativedelta(day=1, month=1)
    @property
    def thisYear_ts(self): return int(time.mktime(self.thisYear.timetuple())) * 1000
    @property
    def lastYear(self): return self.thisYear - relativedelta(years=1)
    @property
    def lastYear_ts(self): return int(time.mktime(self.lastYear.timetuple())) * 1000
    @property
    def TODAY(self): return [self.today_ts, self.now_ts]
    @property
    def YESTERDAY(self): return [self.yesterday_ts, self.today_ts]
    @property
    def THIS_WEEK(self): return [self.lastMonday_ts, self.now_ts]
    @property
    def LAST_WEEK(self): return [self.lastWeekMonday_ts, self.lastMonday_ts]
    @property
    def LAST_WEEK_NOW(self): return [self.lastWeekMonday_ts, self.now_ts]
    @property
    def THIS_MONTH(self): return [self.thisMonth_ts, self.now_ts]
    @property
    def LAST_MONTH(self): return [self.lastMonth_ts, self.thisMonth_ts]
    @property
    def LAST_MONTH_NOW(self): return [self.lastMonth_ts, self.now_ts]
    @property
    def THIS_YEAR(self): return [self.thisYear_ts, self.now_ts]
    @property
    def LAST_YEAR(self): return [self.lastYear_ts, self.thisYear_ts]
    @property
    def LAST_YEAR_NOW(self): return [self.lastYear_ts, self.now_ts]
    @property
    def ALL_TIME(self): return [0, self.now_ts]

    PERIOD = ['TODAY', 'YESTERDAY', 'THIS_WEEK', 'LAST_WEEK', 'LAST_WEEK_NOW', 'THIS_MONTH', 'LAST_MONTH', 'LAST_MONTH_NOW', 'THIS_YEAR', 'LAST_YEAR', 'LAST_YEAR_NOW',  'ALL_TIME']
    DASHBOARD_TYPES = ['NONE', 'PNL', 'TOP', 'POSITIONS', 'ORDERS', 'INCOME', 'BALANCE']

    def __init__(self, name : str = None):
        self.cleanup_dashboard_session_state()
        self.name = name
        self.cols = 1
        self.rows = 1
        self.dashboard_config = {}
        self.user = st.session_state.users.users[0]
        self.db = Database()
        self.view_orders_position = []
        if self.name:
            self.load(name)
        
    def cleanup_dashboard_session_state(self):
        dashboard_keys = {key: val for key, val in st.session_state.items()
            if key.startswith("dashboard_") or key.startswith("view_orders_")}
        for key in dashboard_keys:
            del st.session_state[key]

    def swap(self, from_row, to_row, from_col, to_col):
        dashboard_type_1 = st.session_state[f'dashboard_type_{from_row}_{from_col}']
        dashboard_type_2 = st.session_state[f'dashboard_type_{to_row}_{to_col}']
        del st.session_state[f'dashboard_type_{from_row}_{from_col}']
        del st.session_state[f'dashboard_type_{to_row}_{to_col}']
        st.session_state[f'dashboard_type_{from_row}_{from_col}'] = dashboard_type_2
        st.session_state[f'dashboard_type_{to_row}_{to_col}'] = dashboard_type_1
        move_1 = {}
        move_2 = {}
        if dashboard_type_1 == "PNL":
            pnl_users_1 = st.session_state[f'dashboard_pnl_users_{from_row}_{from_col}']
            pnl_period_1 = st.session_state[f'dashboard_pnl_period_{from_row}_{from_col}']
            pnl_mode_1 = st.session_state[f'dashboard_pnl_mode_{from_row}_{from_col}']
            del st.session_state[f'dashboard_pnl_users_{from_row}_{from_col}']
            del st.session_state[f'dashboard_pnl_period_{from_row}_{from_col}']
            del st.session_state[f'dashboard_pnl_mode_{from_row}_{from_col}']
            move_1 = {"pnl_users_1": pnl_users_1, "pnl_period_1": pnl_period_1, "pnl_mode_1": pnl_mode_1}
        if dashboard_type_1 == "INCOME":
            income_users_1 = st.session_state[f'dashboard_income_users_{from_row}_{from_col}']
            income_period_1 = st.session_state[f'dashboard_income_period_{from_row}_{from_col}']
            del st.session_state[f'dashboard_income_users_{from_row}_{from_col}']
            del st.session_state[f'dashboard_income_period_{from_row}_{from_col}']
            move_1 = {"income_users_1": income_users_1, "income_period_1": income_period_1}
        if dashboard_type_1 == "TOP":
            top_symbols_users_1 = st.session_state[f'dashboard_top_symbols_users_{from_row}_{from_col}']
            top_symbols_period_1 = st.session_state[f'dashboard_top_symbols_period_{from_row}_{from_col}']
            top_symbols_top_1 = st.session_state[f'dashboard_top_symbols_top_{from_row}_{from_col}']
            del st.session_state[f'dashboard_top_symbols_users_{from_row}_{from_col}']
            del st.session_state[f'dashboard_top_symbols_period_{from_row}_{from_col}']
            del st.session_state[f'dashboard_top_symbols_top_{from_row}_{from_col}']
            move_1 = {"top_symbols_users_1": top_symbols_users_1, "top_symbols_period_1": top_symbols_period_1, "top_symbols_top_1": top_symbols_top_1}
        if dashboard_type_1 == "BALANCE":
            balance_users_1 = st.session_state[f'dashboard_balance_users_{from_row}_{from_col}']
            del st.session_state[f'dashboard_balance_users_{from_row}_{from_col}']
            move_1 = {"balance_users_1": balance_users_1}
        if dashboard_type_1 == "POSITIONS":
            positions_users_1 = st.session_state[f'dashboard_positions_users_{from_row}_{from_col}']
            del st.session_state[f'dashboard_positions_users_{from_row}_{from_col}']
            move_1 = {"positions_users_1": positions_users_1}
        if dashboard_type_1 == "ORDERS":
            orders_1 = st.session_state[f'dashboard_orders_{from_row}_{from_col}']
            del st.session_state[f'dashboard_orders_{from_row}_{from_col}']
            move_1 = {"orders_1": orders_1}
        if dashboard_type_2 == "PNL":
            pnl_users_2 = st.session_state[f'dashboard_pnl_users_{to_row}_{to_col}']
            pnl_period_2 = st.session_state[f'dashboard_pnl_period_{to_row}_{to_col}']
            pnl_mode_2 = st.session_state[f'dashboard_pnl_mode_{to_row}_{to_col}']
            del st.session_state[f'dashboard_pnl_users_{to_row}_{to_col}']
            del st.session_state[f'dashboard_pnl_period_{to_row}_{to_col}']
            del st.session_state[f'dashboard_pnl_mode_{to_row}_{to_col}']
            move_2 = {"pnl_users_2": pnl_users_2, "pnl_period_2": pnl_period_2, "pnl_mode_2": pnl_mode_2}
        if dashboard_type_2 == "INCOME":
            income_users_2 = st.session_state[f'dashboard_income_users_{to_row}_{to_col}']
            income_period_2 = st.session_state[f'dashboard_income_period_{to_row}_{to_col}']
            del st.session_state[f'dashboard_income_users_{to_row}_{to_col}']
            del st.session_state[f'dashboard_income_period_{to_row}_{to_col}']
            move_2 = {"income_users_2": income_users_2, "income_period_2": income_period_2}
        if dashboard_type_2 == "TOP":
            top_symbols_users_2 = st.session_state[f'dashboard_top_symbols_users_{to_row}_{to_col}']
            top_symbols_period_2 = st.session_state[f'dashboard_top_symbols_period_{to_row}_{to_col}']
            top_symbols_top_2 = st.session_state[f'dashboard_top_symbols_top_{to_row}_{to_col}']
            del st.session_state[f'dashboard_top_symbols_users_{to_row}_{to_col}']
            del st.session_state[f'dashboard_top_symbols_period_{to_row}_{to_col}']
            del st.session_state[f'dashboard_top_symbols_top_{to_row}_{to_col}']
            move_2 = {"top_symbols_users_2": top_symbols_users_2, "top_symbols_period_2": top_symbols_period_2, "top_symbols_top_2": top_symbols_top_2}
        if dashboard_type_2 == "BALANCE":
            balance_users_2 = st.session_state[f'dashboard_balance_users_{to_row}_{to_col}']
            del st.session_state[f'dashboard_balance_users_{to_row}_{to_col}']
            move_2 = {"balance_users_2": balance_users_2}
        if dashboard_type_2 == "POSITIONS":
            positions_users_2 = st.session_state[f'dashboard_positions_users_{to_row}_{to_col}']
            del st.session_state[f'dashboard_positions_users_{to_row}_{to_col}']
            move_2 = {"positions_users_2": positions_users_2}
        if dashboard_type_2 == "ORDERS":
            orders_2 = st.session_state[f'dashboard_orders_{to_row}_{to_col}']
            del st.session_state[f'dashboard_orders_{to_row}_{to_col}']
            move_2 = {"orders_2": orders_2}
        for key, val in move_1.items():
            key_new = key.replace(f"_1", f"_{to_row}_{to_col}")
            st.session_state[f'dashboard_{key_new}'] = val
        for key, val in move_2.items():
            key_new = key.replace(f"_2", f"_{from_row}_{from_col}")
            st.session_state[f'dashboard_{key_new}'] = val
        dashboard_orders = {key: val for key, val in st.session_state.items()
            if key.startswith("dashboard_orders_")}
        for key, val in dashboard_orders.items():
            if val == f'view_orders_{from_row}_{from_col}':
                if key in st.session_state:
                    del st.session_state[key]
                st.session_state[key] = f'view_orders_{to_row}_{to_col}'
                order = st.session_state[f'view_orders_{from_row}_{from_col}']
                del st.session_state[f'view_orders_{from_row}_{from_col}']
                st.session_state[f'view_orders_{to_row}_{to_col}'] = order
            if val == f'view_orders_{to_row}_{to_col}':
                if key in st.session_state:
                    del st.session_state[key]
                st.session_state[key] = f'view_orders_{from_row}_{from_col}'
                order = st.session_state[f'view_orders_{to_row}_{to_col}']
                del st.session_state[f'view_orders_{to_row}_{to_col}']
                st.session_state[f'view_orders_{from_row}_{from_col}'] = order
        st.session_state.swap_rerun = True

    def create_dashboard(self):
        # Init session_state for keys
        if "dashboard_cols" in st.session_state:
            if st.session_state.dashboard_cols != self.cols:
                self.cols = st.session_state.dashboard_cols
        if "dashboard_rows" in st.session_state:
            if st.session_state.dashboard_rows != self.rows:
                self.rows = st.session_state.dashboard_rows
        if "dashboard_name" in st.session_state:
            if st.session_state.dashboard_name != self.name:
                self.name = st.session_state.dashboard_name
        col1, col2, col3 = st.columns([1,1,2])
        with col1:
            if not self.name:
                color = "red"
            else:
                color = None
            st.number_input('cols', value=self.cols, min_value=1, max_value=2, step=1, key="dashboard_cols")
        with col2:
            st.number_input('rows', value=self.rows, min_value=1, max_value=5, step=1, key="dashboard_rows")
        with col3:
            st.text_input(f":{color}[Dashboard Name]", value=self.name, max_chars=32, key="dashboard_name")
        if st.session_state.dashboard_cols == 2:
            for row in range(1, self.rows + 1):
                db_col1, db_col2 = st.columns([1,1])
                with db_col1:
                    bu_col1, bu_col2, bu_col_empty = st.columns([1,1,20])
                    with bu_col1:
                        if st.button(":material/arrow_right_alt:", key=f"swap_{row}_col1"):
                            self.swap(row, row, 1, 2)
                            # self.swap_col(row)
                    with bu_col2:
                        if row > 1:
                            if st.button(":material/arrow_upward_alt:", key=f"swap_vert_{row}_col1"):
                                self.swap(row, row -1, 1, 1)
                        else:
                            if st.button(":material/arrow_downward_alt:", key=f"swap_vert_{row}_col1"):
                                self.swap(row, row +1, 1, 1)
                    if f'dashboard_type_{row}_1' in st.session_state:
                        index = self.DASHBOARD_TYPES.index(st.session_state[f'dashboard_type_{row}_1'])
                        del st.session_state[f'dashboard_type_{row}_1']
                    else:
                        index = 0
                    st.selectbox('Dashboard Type', self.DASHBOARD_TYPES, index=index, key=f"dashboard_type_{row}_1")
                    if st.session_state[f'dashboard_type_{row}_1'] == "PNL":
                        if f'dashboard_pnl_users_{row}_1' in self.dashboard_config and f'dashboard_pnl_period_{row}_1' in self.dashboard_config and f'dashboard_pnl_mode_{row}_1' in self.dashboard_config:
                            self.view_pnl(f'{row}_1', self.dashboard_config[f'dashboard_pnl_users_{row}_1'], self.dashboard_config[f'dashboard_pnl_period_{row}_1'], self.dashboard_config[f'dashboard_pnl_mode_{row}_1'])
                        else:
                            self.view_pnl(f'{row}_1')
                    if st.session_state[f'dashboard_type_{row}_1'] == "INCOME":
                        if f'dashboard_income_users_{row}_1' in self.dashboard_config and f'dashboard_income_period_{row}_1' in self.dashboard_config:
                            self.view_income(f'{row}_1', self.dashboard_config[f'dashboard_income_users_{row}_1'], self.dashboard_config[f'dashboard_income_period_{row}_1'])
                        else:
                            self.view_income(f'{row}_1')
                    if st.session_state[f'dashboard_type_{row}_1'] == "TOP":
                        if f'dashboard_top_symbols_users_{row}_1' in self.dashboard_config and f'dashboard_top_symbols_period_{row}_1' in self.dashboard_config and f'dashboard_top_symbols_top_{row}_1' in self.dashboard_config:
                            self.view_top_symbols(f'{row}_1', self.dashboard_config[f'dashboard_top_symbols_users_{row}_1'], self.dashboard_config[f'dashboard_top_symbols_period_{row}_1'], self.dashboard_config[f'dashboard_top_symbols_top_{row}_1'])
                        else:
                            self.view_top_symbols(f'{row}_1')
                    if st.session_state[f'dashboard_type_{row}_1'] == "POSITIONS":
                        if f'dashboard_positions_users_{row}_1' in self.dashboard_config:
                            self.view_positions(f'{row}_1', self.dashboard_config[f'dashboard_positions_users_{row}_1'])
                        else:
                            self.view_positions(f'{row}_1')
                    if st.session_state[f'dashboard_type_{row}_1'] == "ORDERS":
                        if f'dashboard_orders_{row}_1' in self.dashboard_config:
                            self.view_orders(f'{row}_1', self.dashboard_config[f'dashboard_orders_{row}_1'], edit=True)
                        else:
                            self.view_orders(f'{row}_1', edit=True)
                    if st.session_state[f'dashboard_type_{row}_1'] == "BALANCE":
                        if f'dashboard_balance_users_{row}_1' in self.dashboard_config:
                            self.view_balance(f'{row}_1', self.dashboard_config[f'dashboard_balance_users_{row}_1'])
                        else:
                            self.view_balance(f'{row}_1')
                with db_col2:
                    bu_col1, bu_col2, bu_col_empty = st.columns([1,1,20])
                    with bu_col1:
                        if st.button(":material/arrow_left_alt:", key=f"swap_{row}_col2"):
                            self.swap(row, row, 2, 1)
                    with bu_col2:
                        if row > 1:
                            if st.button(":material/arrow_upward_alt:", key=f"swap_vert_{row}_col2"):
                                self.swap(row, row -1, 2, 2)
                        else:
                            if st.button(":material/arrow_downward_alt:", key=f"swap_vert_{row}_col2"):
                                self.swap(row, row +1, 2, 2)
                    if f'dashboard_type_{row}_2' in st.session_state:
                        index = self.DASHBOARD_TYPES.index(st.session_state[f'dashboard_type_{row}_2'])
                        del st.session_state[f'dashboard_type_{row}_2']
                    else:
                        index = 0
                    st.selectbox('Dashboard Type', self.DASHBOARD_TYPES, index=index, key=f"dashboard_type_{row}_2")
                    if st.session_state[f'dashboard_type_{row}_2'] == "PNL":
                        if f'dashboard_pnl_users_{row}_2' in self.dashboard_config and f'dashboard_pnl_period_{row}_2' in self.dashboard_config and f'dashboard_pnl_mode_{row}_2' in self.dashboard_config:
                            self.view_pnl(f'{row}_2', self.dashboard_config[f'dashboard_pnl_users_{row}_2'], self.dashboard_config[f'dashboard_pnl_period_{row}_2'], self.dashboard_config[f'dashboard_pnl_mode_{row}_2'])
                        else:
                            self.view_pnl(f'{row}_2')
                    if st.session_state[f'dashboard_type_{row}_2'] == "INCOME":
                        if f'dashboard_income_users_{row}_2' in self.dashboard_config and f'dashboard_income_period_{row}_2' in self.dashboard_config:
                            self.view_income(f'{row}_2', self.dashboard_config[f'dashboard_income_users_{row}_2'], self.dashboard_config[f'dashboard_income_period_{row}_2'])
                        else:
                            self.view_income(f'{row}_2')
                    if st.session_state[f'dashboard_type_{row}_2'] == "TOP":
                        if f'dashboard_top_symbols_users_{row}_2' in self.dashboard_config and f'dashboard_top_symbols_period_{row}_2' in self.dashboard_config and f'dashboard_top_symbols_top_{row}_2' in self.dashboard_config:
                            self.view_top_symbols(f'{row}_2', self.dashboard_config[f'dashboard_top_symbols_users_{row}_2'], self.dashboard_config[f'dashboard_top_symbols_period_{row}_2'], self.dashboard_config[f'dashboard_top_symbols_top_{row}_2'])
                        else:
                            self.view_top_symbols(f'{row}_2')
                    if st.session_state[f'dashboard_type_{row}_2'] == "POSITIONS":
                        if f'dashboard_positions_users_{row}_2' in self.dashboard_config:
                            self.view_positions(f'{row}_2', self.dashboard_config[f'dashboard_positions_users_{row}_2'])
                        else:
                            self.view_positions(f'{row}_2')
                    if st.session_state[f'dashboard_type_{row}_2'] == "ORDERS":
                        if f'dashboard_orders_{row}_2' in self.dashboard_config:
                            self.view_orders(f'{row}_2', self.dashboard_config[f'dashboard_orders_{row}_2'], edit=True)
                        else:
                            self.view_orders(f'{row}_2', edit=True)
                    if st.session_state[f'dashboard_type_{row}_2'] == "BALANCE":
                        if f'dashboard_balance_users_{row}_2' in self.dashboard_config:
                            self.view_balance(f'{row}_2', self.dashboard_config[f'dashboard_balance_users_{row}_2'])
                        else:
                            self.view_balance(f'{row}_2')
        else:
            for row in range(1, self.rows + 1):
                if row > 1:
                    if st.button(":material/swap_vert:", key=f"swap_vert_{row}_col1"):
                        self.swap(row, row -1, 1, 1)
                if f'dashboard_type_{row}_1' in st.session_state:
                    index = self.DASHBOARD_TYPES.index(st.session_state[f'dashboard_type_{row}_1'])
                    del st.session_state[f'dashboard_type_{row}_1']
                else:
                    index = 0
                st.selectbox('Dashboard Type', self.DASHBOARD_TYPES, index=index, key=f"dashboard_type_{row}_1")
                if st.session_state[f'dashboard_type_{row}_1'] == "PNL":
                    if f'dashboard_pnl_users_{row}_1' in self.dashboard_config and f'dashboard_pnl_period_{row}_1' in self.dashboard_config and f'dashboard_pnl_mode_{row}_1' in self.dashboard_config:
                        self.view_pnl(f'{row}_1', self.dashboard_config[f'dashboard_pnl_users_{row}_1'], self.dashboard_config[f'dashboard_pnl_period_{row}_1'], self.dashboard_config[f'dashboard_pnl_mode_{row}_1'])
                    else:
                        self.view_pnl(f'{row}_1')
                if st.session_state[f'dashboard_type_{row}_1'] == "INCOME":
                    if f'dashboard_income_users_{row}_1' in self.dashboard_config and f'dashboard_income_period_{row}_1' in self.dashboard_config:
                        self.view_income(f'{row}_1', self.dashboard_config[f'dashboard_income_users_{row}_1'], self.dashboard_config[f'dashboard_income_period_{row}_1'])
                    else:
                        self.view_income(f'{row}_1')
                if st.session_state[f'dashboard_type_{row}_1'] == "TOP":
                    if f'dashboard_top_symbols_users_{row}_1' in self.dashboard_config and f'dashboard_top_symbols_period_{row}_1' in self.dashboard_config and f'dashboard_top_symbols_top_{row}_1' in self.dashboard_config:
                        self.view_top_symbols(f'{row}_1', self.dashboard_config[f'dashboard_top_symbols_users_{row}_1'], self.dashboard_config[f'dashboard_top_symbols_period_{row}_1'], self.dashboard_config[f'dashboard_top_symbols_top_{row}_1'])
                    else:
                        self.view_top_symbols(f'{row}_1')
                if st.session_state[f'dashboard_type_{row}_1'] == "POSITIONS":
                    if f'dashboard_positions_users_{row}_1' in self.dashboard_config:
                        self.view_positions(f'{row}_1', self.dashboard_config[f'dashboard_positions_users_{row}_1'])
                    else:
                        self.view_positions(f'{row}_1')
                if st.session_state[f'dashboard_type_{row}_1'] == "ORDERS":
                    if f'dashboard_orders_{row}_1' in self.dashboard_config:
                        self.view_orders(f'{row}_1', self.dashboard_config[f'dashboard_orders_{row}_1'], edit=True)
                    else:
                        self.view_orders(f'{row}_1', edit=True)
                if st.session_state[f'dashboard_type_{row}_1'] == "BALANCE":
                    if f'dashboard_balance_users_{row}_1' in self.dashboard_config:
                        self.view_balance(f'{row}_1', self.dashboard_config[f'dashboard_balance_users_{row}_1'])
                    else:
                        self.view_balance(f'{row}_1')
        if "swap_rerun" in st.session_state:
            del st.session_state.swap_rerun
            st.rerun()

    def save(self):
        dashboard_config = {}
        dashboard_config["rows"] = self.rows
        dashboard_config["cols"] = self.cols
        for row in range(1, self.rows + 1):
            for col in range(1, self.cols + 1):
                dashboard_config[f'dashboard_type_{row}_{col}'] = st.session_state[f'dashboard_type_{row}_{col}']
                if st.session_state[f'dashboard_type_{row}_{col}'] == "PNL":
                    dashboard_config[f'dashboard_pnl_users_{row}_{col}'] = st.session_state[f'dashboard_pnl_users_{row}_{col}']
                    dashboard_config[f'dashboard_pnl_period_{row}_{col}'] = st.session_state[f'dashboard_pnl_period_{row}_{col}']
                    dashboard_config[f'dashboard_pnl_mode_{row}_{col}'] = st.session_state[f'dashboard_pnl_mode_{row}_{col}']
                if st.session_state[f'dashboard_type_{row}_{col}'] == "INCOME":
                    dashboard_config[f'dashboard_income_users_{row}_{col}'] = st.session_state[f'dashboard_income_users_{row}_{col}']
                    dashboard_config[f'dashboard_income_period_{row}_{col}'] = st.session_state[f'dashboard_income_period_{row}_{col}']
                if st.session_state[f'dashboard_type_{row}_{col}'] == "TOP":
                    dashboard_config[f'dashboard_top_symbols_users_{row}_{col}'] = st.session_state[f'dashboard_top_symbols_users_{row}_{col}']
                    dashboard_config[f'dashboard_top_symbols_period_{row}_{col}'] = st.session_state[f'dashboard_top_symbols_period_{row}_{col}']
                    dashboard_config[f'dashboard_top_symbols_top_{row}_{col}'] = st.session_state[f'dashboard_top_symbols_top_{row}_{col}']
                if st.session_state[f'dashboard_type_{row}_{col}'] == "POSITIONS":
                    dashboard_config[f'dashboard_positions_users_{row}_{col}'] = st.session_state[f'dashboard_positions_users_{row}_{col}']
                if st.session_state[f'dashboard_type_{row}_{col}'] == "ORDERS":
                    if f'dashboard_orders_{row}_{col}' in st.session_state:
                        dashboard_config[f'dashboard_orders_{row}_{col}'] = st.session_state[f'dashboard_orders_{row}_{col}']
                    else:
                        dashboard_config[f'dashboard_orders_{row}_{col}'] = None
                if st.session_state[f'dashboard_type_{row}_{col}'] == "BALANCE":
                    dashboard_config[f'dashboard_balance_users_{row}_{col}'] = st.session_state[f'dashboard_balance_users_{row}_{col}']
        self.dashboard_config = dashboard_config
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_path.mkdir(parents=True, exist_ok=True)
        dashboard_file = Path(f'{dashboard_path}/{self.name}.json')
        with dashboard_file.open("w") as f:
            json.dump(dashboard_config, f, indent=4)

    def load(self, name : str):
        self.cleanup_dashboard_session_state()
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_file = Path(f'{dashboard_path}/{name}.json')
        if dashboard_file.exists():
            with dashboard_file.open() as f:
                dashboard_config = json.load(f)
            self.dashboard_config = dashboard_config
            self.rows = dashboard_config["rows"]
            self.cols = dashboard_config["cols"]
            self.cleanup_dashboard_session_state()
            for row in range(1, self.rows + 1):
                for col in range(1, self.cols + 1):
                    st.session_state[f'dashboard_type_{row}_{col}'] = dashboard_config[f'dashboard_type_{row}_{col}']

    def delete(self):
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_file = Path(f'{dashboard_path}/{self.name}.json')
        if dashboard_file.exists():
            dashboard_file.unlink()

    def list_dashboards(self):
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboards = []
        for file in dashboard_path.glob("*.json"):
            dashboards.append(file.stem)
        dashboards.sort()
        return dashboards

    def view(self):
        # Init
        dashboard_config = self.dashboard_config
        self.rows = dashboard_config["rows"]
        self.cols = dashboard_config["cols"]
        # Titel
        st.title(f"Dashboard: {self.name}")
        for row in range(1, self.rows + 1):
            if self.cols == 2:
                db_col1, db_col2 = st.columns([1,1])
                with db_col1:
                    if dashboard_config[f'dashboard_type_{row}_1'] == "PNL":
                        self.view_pnl(f'{row}_1', dashboard_config[f'dashboard_pnl_users_{row}_1'], dashboard_config[f'dashboard_pnl_period_{row}_1'], dashboard_config[f'dashboard_pnl_mode_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "INCOME":
                        self.view_income(f'{row}_1', dashboard_config[f'dashboard_income_users_{row}_1'], dashboard_config[f'dashboard_income_period_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "TOP":
                        self.view_top_symbols(f'{row}_1', dashboard_config[f'dashboard_top_symbols_users_{row}_1'], dashboard_config[f'dashboard_top_symbols_period_{row}_1'], dashboard_config[f'dashboard_top_symbols_top_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "POSITIONS":
                        self.view_positions(f'{row}_1', dashboard_config[f'dashboard_positions_users_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "ORDERS":
                        self.view_orders(f'{row}_1', dashboard_config[f'dashboard_orders_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "BALANCE":
                        self.view_balance(f'{row}_1', dashboard_config[f'dashboard_balance_users_{row}_1'])
                with db_col2:
                    if dashboard_config[f'dashboard_type_{row}_2'] == "PNL":
                        self.view_pnl(f'{row}_2', dashboard_config[f'dashboard_pnl_users_{row}_2'], dashboard_config[f'dashboard_pnl_period_{row}_2'], dashboard_config[f'dashboard_pnl_mode_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "INCOME":
                        self.view_income(f'{row}_2', dashboard_config[f'dashboard_income_users_{row}_2'], dashboard_config[f'dashboard_income_period_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "TOP":
                        self.view_top_symbols(f'{row}_2', dashboard_config[f'dashboard_top_symbols_users_{row}_2'], dashboard_config[f'dashboard_top_symbols_period_{row}_2'], dashboard_config[f'dashboard_top_symbols_top_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "POSITIONS":
                        self.view_positions(f'{row}_2', dashboard_config[f'dashboard_positions_users_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "ORDERS":
                        self.view_orders(f'{row}_2', dashboard_config[f'dashboard_orders_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "BALANCE":
                        self.view_balance(f'{row}_2', dashboard_config[f'dashboard_balance_users_{row}_2'])
            else:
                if dashboard_config[f'dashboard_type_{row}_1'] == "PNL":
                    self.view_pnl(f'{row}_1', dashboard_config[f'dashboard_pnl_users_{row}_1'], dashboard_config[f'dashboard_pnl_period_{row}_1'], dashboard_config[f'dashboard_pnl_mode_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "INCOME":
                    self.view_income(f'{row}_1', dashboard_config[f'dashboard_income_users_{row}_1'], dashboard_config[f'dashboard_income_period_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "TOP":
                    self.view_top_symbols(f'{row}_1', dashboard_config[f'dashboard_top_symbols_users_{row}_1'], dashboard_config[f'dashboard_top_symbols_period_{row}_1'], dashboard_config[f'dashboard_top_symbols_top_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "POSITIONS":
                    self.view_positions(f'{row}_1', dashboard_config[f'dashboard_positions_users_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "ORDERS":
                    self.view_orders(f'{row}_1', dashboard_config[f'dashboard_orders_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "BALANCE":
                    self.view_balance(f'{row}_1', dashboard_config[f'dashboard_balance_users_{row}_1'])

    @st.fragment
    def view_pnl(self, position : str, user : str = None, period : str = None, mode : str = "bar"):
        users = st.session_state.users
        if f"dashboard_pnl_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_pnl_users_{position}'] = user
        if f"dashboard_pnl_period_{position}" not in st.session_state:
            if period:
                st.session_state[f'dashboard_pnl_period_{position}'] = period
        if f"dashboard_pnl_mode_{position}" not in st.session_state:
            if mode:
                st.session_state[f'dashboard_pnl_mode_{position}'] = mode
        st.markdown("#### :blue[Daily PNL]")
        col1, col2, col3 = st.columns([2,1,1])
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_pnl_users_{position}")
        with col2:
            st.selectbox('period', self.PERIOD, key=f"dashboard_pnl_period_{position}")
        with col3:
            st.selectbox('Mode', ['bar', 'line'], key=f"dashboard_pnl_mode_{position}")
        if st.session_state[f'dashboard_pnl_users_{position}']:
            if st.session_state[f'dashboard_pnl_period_{position}'] in self.PERIOD:
                period_index = self.PERIOD.index(st.session_state[f'dashboard_pnl_period_{position}'])
                period_range = getattr(self, self.PERIOD[period_index])
                pnl = self.db.select_pnl(st.session_state[f'dashboard_pnl_users_{position}'], period_range[0], period_range[1])
            df = pd.DataFrame(pnl, columns =['Date', 'Income'])
            if st.session_state[f'dashboard_pnl_mode_{position}'] == "line":
                if not pnl:
                    return
                if len(pnl) <= 31:
                    fig = px.line(df, x='Date', y='Income', markers=True, text='Income', hover_data={'Income':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                else:
                    fig = px.line(df, x='Date', y='Income', markers=True, hover_data={'Income':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                fig.update_traces(texttemplate='%{text:.2f}', textposition='top left')
            else:
                fig = px.bar(df, x='Date', y='Income', text='Income', hover_data={'Income':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                fig.update_traces(texttemplate='%{text:.2f}', textposition='auto')
            fig.update_traces(marker_color=['red' if val < 0 else 'green' for val in df['Income']])
            st.plotly_chart(fig, key=f"dashboard_pnl_plot_{position}")

    @st.fragment
    def view_income(self, position : str, user : str = None, period : str = None):
        users = st.session_state.users
        if f"dashboard_income_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_income_users_{position}'] = user
        if f"dashboard_income_period_{position}" not in st.session_state:
            if period:
                st.session_state[f'dashboard_income_period_{position}'] = period
        st.markdown("#### :blue[Income]")
        col1, col2 = st.columns([3,1])
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_income_users_{position}")
        with col2:
            st.selectbox('period', self.PERIOD, key=f"dashboard_income_period_{position}")
        if st.session_state[f'dashboard_income_users_{position}']:
            if st.session_state[f'dashboard_income_period_{position}'] in self.PERIOD:
                period_index = self.PERIOD.index(st.session_state[f'dashboard_income_period_{position}'])
                period_range = getattr(self, self.PERIOD[period_index])
                income = self.db.select_income_by_symbol(st.session_state[f'dashboard_income_users_{position}'], period_range[0], period_range[1])
            df = pd.DataFrame(income, columns=['Date', 'Symbol', 'Income'])
            df['Date'] = pd.to_datetime(df['Date'], unit='ms')
            income = df[['Date', 'Symbol', 'Income']].copy()
            income['Income'] = income['Income'].cumsum()
            fig = px.line(income, x='Date', y='Income', hover_data={'Income':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
            fig['data'][0]['showlegend'] = True
            fig['data'][0]['name'] = 'Total Income'
            # Sort df by Symbol
            df = df.sort_values(by=['Symbol', 'Date'])
            for symbol in df['Symbol'].unique():
                symbol_df = df[df['Symbol'] == symbol].copy()
                symbol_df['Income'] = symbol_df['Income'].cumsum()
                fig.add_trace(go.Scatter(x=symbol_df['Date'], y=symbol_df['Income'], name=symbol))
            st.plotly_chart(fig, key=f"dashboard_income_plot_{position}")

    @st.fragment
    def view_top_symbols(self, position : str, user : str = None, period : str = None, top : int = None):
        users = st.session_state.users
        if f"dashboard_top_symbols_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_top_symbols_users_{position}'] = user
        if f"dashboard_top_symbols_period_{position}" not in st.session_state:
            if period:
                st.session_state[f'dashboard_top_symbols_period_{position}'] = period
        if f"dashboard_top_symbols_top_{position}" not in st.session_state:
            if top:
                st.session_state[f'dashboard_top_symbols_top_{position}'] = top
        st.markdown("#### :blue[Top Symbols]")
        col1, col2, col3 = st.columns([2,1,1])
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_top_symbols_users_{position}")
        with col2:
            st.selectbox('period', self.PERIOD, key=f"dashboard_top_symbols_period_{position}")
        with col3:
            st.number_input('Top', value=10, min_value=1, step=5, key=f"dashboard_top_symbols_top_{position}")
        if st.session_state[f'dashboard_top_symbols_users_{position}']:
            if st.session_state[f'dashboard_top_symbols_period_{position}'] in self.PERIOD:
                period_index = self.PERIOD.index(st.session_state[f'dashboard_top_symbols_period_{position}'])
                period_range = getattr(self, self.PERIOD[period_index])
                top = self.db.select_top(st.session_state[f'dashboard_top_symbols_users_{position}'], period_range[0], period_range[1], st.session_state[f'dashboard_top_symbols_top_{position}'])
            df = pd.DataFrame(top, columns =['Date', 'Symbol', 'Income'])
            # st.write(df)
            fig = px.bar(df, x="Symbol", y="Income", title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
            fig.update_traces(marker_color=['red' if val < 0 else 'green' for val in df['Income']])
            st.plotly_chart(fig, key=f"dashboard_top_symbols_plot_{position}")

    def color_we(self, value):
        # bgcolor green < 10, yellow 100-200, red > 200
        color = "green" if value < 100 else "yellow" if value < 200 else "red"
        return f"color: {color};"

    def color_upnl(self, value):
        color = "red" if value < 0 else "green"
        return f"color: {color};"
    
    @st.fragment
    def view_balance(self, position : str, user : str = None):
        users = st.session_state.users
        if f"dashboard_balance_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_balance_users_{position}'] = user
        st.markdown("#### :blue[Balance]")
        st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_balance_users_{position}")
        if st.session_state[f'dashboard_balance_users_{position}']:
            if 'ALL' in st.session_state[f'dashboard_balance_users_{position}']:
                users_selected = users.list()
            else:
                users_selected = st.session_state[f'dashboard_balance_users_{position}']
            balances = self.db.fetch_balances(users_selected)
            df = pd.DataFrame(balances, columns=['Id', 'Date', 'Balance', 'User'])
            my_tz = datetime.now().astimezone().tzinfo
            df['Date'] = pd.to_datetime(df['Date'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(my_tz).dt.strftime('%Y-%m-%d %H:%M:%S')
            # print(df)
            # loop over df and select balance
            all_pprices = 0
            for index, balance in df.iterrows():
                # Fetch positions
                user = users.find_user(balance['User'])
                positions = self.db.fetch_positions(user)
                # calculate WE for user
                upnl = 0
                pprices = 0
                for pos in positions:
                    pprices += pos[3] * pos[5]
                    upnl += pos[4]
                all_pprices += pprices
                twe = 100 / balance['Balance'] * pprices
                # add twe to df as new column name we
                df.at[index, 'WE'] = twe
                df.at[index, 'uPnl'] = upnl
            total_balance = df['Balance'].sum()
            total_upnl = df['uPnl'].sum()
            total_twe = 100 / total_balance * all_pprices
            color = "green" if total_twe < 100 else "yellow" if total_twe < 200 else "red"
            color_upnl = "red" if total_upnl < 0 else "green"
            st.markdown(f"#### :blue[Total Balance:] :green[${total_balance:.2f} USDT]&emsp; :blue[Total uPnl:] :{color_upnl}[${total_upnl:.2f}]&emsp; :blue[Total TWE:] :{color}[{total_twe:.2f} %]")
            column_config = {
                "Id": None,
                "Balance": st.column_config.NumberColumn(f'Total Balance: ${total_balance:.2f}' ,format="%.2f"),
                "uPnl": st.column_config.NumberColumn(f'Total uPnl: ${total_upnl:.2f}' ,format="%.2f"),
                "WE": st.column_config.ProgressColumn(f'TWE: {total_twe:.2f} %', format="%.2f %%", min_value=0, max_value=300)
            }
            df = df[['Id', 'User', 'Date', 'Balance', 'uPnl', 'WE']]
            sdf = df.style.map(self.color_we, subset=['WE']).map(self.color_upnl, subset=['uPnl']).format({'Balance': "{:.2f}"})
            st.dataframe(sdf, height=36+(len(df))*35, use_container_width=True, key=f"dashboard_balance_{position}", on_select="rerun", selection_mode='single-row', hide_index=None, column_order=None, column_config=column_config)

    def bgcolor_positive_or_negative(self, value):
        bgcolor = "red" if value < 0 else "green"
        return f"background-color: {bgcolor};"

    @st.fragment
    def view_positions(self, position : str, user : str = None):
        users = st.session_state.users
        if f"dashboard_positions_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_positions_users_{position}'] = user
        # Init Orders View
        if f"dashboard_positions_{position}" in st.session_state:
            if st.session_state[f'dashboard_positions_{position}']["selection"]["rows"]:
                row = st.session_state[f'dashboard_positions_{position}']["selection"]["rows"][0]
                st.session_state[f'view_orders_{position}'] = st.session_state[f'dashboard_positions_sdf_{position}'].iloc[row]
                st.rerun()
        st.markdown("#### :blue[Positions]")
        col1, col2 = st.columns([5,0.2], vertical_alignment="bottom")
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_positions_users_{position}")
        with col2:
            if st.button(":material/refresh:", key=f"dashboard_positions_rerun_{position}"):
                st.rerun(scope="fragment")
        # Init view_orders that it can be selected in edit mode
        if f'view_orders_{position}' not in st.session_state:
            st.session_state[f'view_orders_{position}'] = None
        if st.session_state[f'dashboard_positions_users_{position}']:
            all_positions = []
            users = st.session_state.users
            if 'ALL' in st.session_state[f'dashboard_positions_users_{position}']:
                users_selected = users.list()
            else:
                users_selected = st.session_state[f'dashboard_positions_users_{position}']
            for user in users_selected:
                positions = self.db.fetch_positions(users.find_user(user))
                prices = self.db.fetch_prices(users.find_user(user))
                for pos in positions:
                    symbol = pos[1]
                    user = pos[6]
                    orders = self.db.fetch_orders_by_symbol(user, symbol)
                    dca = 0
                    next_tp = 0
                    next_dca = 0
                    for order in orders:
                        # print(order)
                        if order[5] == "buy":
                            dca += 1
                            if next_dca < order[4]:
                                next_dca = order[4]
                        elif order[5] == "sell":
                            if next_tp == 0 or next_tp > order[4]:
                                next_tp = order[4]
                    # Find price from prices
                    price = 0
                    for p in prices:
                        if p[1] == symbol:
                            price = p[3]
                    all_positions.append(tuple(pos) + (price,) + (dca,) + (next_dca,) + (next_tp,))
            df = pd.DataFrame(all_positions, columns =['Id', 'Symbol', 'PosId', 'Size', 'uPnl', 'Entry', 'User', 'Price', 'DCA', 'Next DCA', 'Next TP'])
            # sorty df by User, Symbol
            df = df.sort_values(by=['User', 'Symbol'])
            # Move User to second column
            df = df[['Id', 'User', 'Symbol', 'PosId', 'Size', 'uPnl', 'Entry', 'Price', 'DCA', 'Next DCA', 'Next TP']]
            sdf = df.style.map(self.color_upnl, subset=['uPnl']).format({'Size': "{:.3f}"})
            st.session_state[f'dashboard_positions_sdf_{position}'] = df
            column_config = {
                "Id": None,
                "PosId": None
            }
            st.dataframe(sdf, height=36+(len(df))*35, use_container_width=True, key=f"dashboard_positions_{position}", on_select="rerun", selection_mode='single-row', hide_index=None, column_order=None, column_config=column_config)

    @st.fragment
    def view_orders(self, pos : str, orders : str = None, tf : str = "4h", edit : bool = False):
        position = None
        view_orders = {key: val for key, val in st.session_state.items()
            if key.startswith("view_orders_")}
        if orders and orders in view_orders:
            position = st.session_state[f'{orders}']
            if f"dashboard_orders_{pos}" not in st.session_state:
                if pos:
                    st.session_state[f'dashboard_orders_{pos}'] = orders
        if view_orders and edit:
            index = 0
            if orders in list(view_orders.keys()):
                index = list(view_orders.keys()).index(orders)
            selected_pos = st.selectbox('From Positions', view_orders, index=index, key=f"dashboard_orders_{pos}")
            position = st.session_state[f'{selected_pos}']
        if f"dashboard_orders_tf_{pos}" not in st.session_state:
            if tf:
                st.session_state[f'dashboard_orders_tf_{pos}'] = tf
        st.markdown("#### :blue[Orders]")
        if position is None:
            return
        # Init Exchange
        users = st.session_state.users
        user = users.find_user(position["User"])
        exchange = Exchange(user.exchange, user)
        market_type = "futures"
        col1, col2, col3 = st.columns([1,1,8], vertical_alignment="bottom")
        with col1:
            st.selectbox('Timeframe',exchange.tf,index=exchange.tf.index(tf), key=f"dashboard_orders_tf_{pos}")
        with col2:
            since = None
            if f'dashboard_orders_leftclick_{pos}' not in st.session_state:
                st.session_state[f'dashboard_orders_leftclick_{pos}'] = 0
            if st.button(":material/arrow_left:", key=f"dashboard_orders_left{pos}"):
                since = st.session_state[f'dashboard_orders_since_{pos}'] - st.session_state[f'dashboard_orders_range_{pos}']
                st.session_state[f'dashboard_orders_leftclick_{pos}'] += 1
        with col3:
            if st.session_state[f'dashboard_orders_leftclick_{pos}'] > 0:
                if st.button(":material/arrow_right:", key=f"dashboard_orders_right{pos}"):
                    since = st.session_state[f'dashboard_orders_since_{pos}'] + st.session_state[f'dashboard_orders_range_{pos}']
                    st.session_state[f'dashboard_orders_leftclick_{pos}'] -= 1
        symbol = position["Symbol"]
        # symbol to ccxt_symbol
        if symbol[-4:] == "USDT":
            symbol_ccxt = f'{symbol[0:-4]}/USDT:USDT'
        elif symbol[-4:] == "USDC":
            symbol_ccxt = f'{symbol[0:-4]}/USDC:USDC'
        ohlcv = exchange.fetch_ohlcv(symbol_ccxt, market_type, timeframe=st.session_state[f'dashboard_orders_tf_{pos}'], limit=100, since=since)
        ohlcv_df = pd.DataFrame(ohlcv, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        st.session_state[f'dashboard_orders_since_{pos}'] = int(ohlcv_df.iloc[0]["timestamp"])
        st.session_state[f'dashboard_orders_range_{pos}'] = int(ohlcv_df.iloc[-1]["timestamp"] - ohlcv_df.iloc[0]["timestamp"])
        ohlcv_df["color"] = np.where(ohlcv_df["close"] > ohlcv_df["open"], "green", "red")
        # w = (ohlcv_df["timestamp"][1] - ohlcv_df["timestamp"][0]) * 0.8
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        col1, col2, col3, col4 = st.columns([1, 1, 1, 0.2])
        with col1:
            st.markdown(f"#### :blue[User:] :green[{user.name}]")
        with col2:
            st.markdown(f"#### :blue[Symbol:] :green[{symbol}]")
        with col3:
            st.markdown(f"#### :blue[Time:] :green[{time}]")
        with col4:
            if st.button(":material/refresh:", key=f"dashboard_orders_rerun_{pos}"):
                st.rerun(scope="fragment")
        # layout = go.Layout(title=f'{symbol} | {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC', title_font=dict(size=36), showlegend=True)
        fig = go.Figure(data=[go.Candlestick(x=pd.to_datetime(ohlcv_df["timestamp"], unit='ms'),
               open=ohlcv_df["open"], high=ohlcv_df["high"],
               low=ohlcv_df["low"], close=ohlcv_df["close"],
               increasing_line_color='green', decreasing_line_color='red')])
        # remove legend from trace 0
        fig.data[0].showlegend = False
        fig.update_layout(yaxis=dict(title='USDT', title_font=dict(size=24)), xaxis_rangeslider_visible=False, height=800, xaxis_type='category')
        fig.update_layout(xaxis_rangeslider_visible=False, xaxis_tickformat='%H:%M')
        fig.update_xaxes(tickangle=-90, tickfont=dict(size=14), dtick='8')
        # fig.update_layout(xaxis_rangeslider_visible=False, width=1280, height=1024)
        # balance = exchange.fetch_balance(market_type)
        prices = self.db.fetch_prices(user)
        price = 0
        for p in prices:
            if p[1] == symbol:
                price = p[3]
        orders = self.db.fetch_orders_by_symbol(user.name, symbol)
        color = "red" if price < ohlcv_df["open"].iloc[-1] else "green"
        # add price line to candlestick
        fig.add_trace(go.Scatter(x=pd.to_datetime(ohlcv_df["timestamp"], unit='ms'), y=[price] * len(ohlcv_df), mode='lines', line=dict(color=color, width=1), name=f'price: {str(round(price,5))}'))
        # position
        color = "red" if price < position["Entry"] else "green"
        size = position["Size"]
        fig.add_trace(go.Scatter(x=pd.to_datetime(ohlcv_df["timestamp"], unit='ms'),
                                y=[position["Entry"]] * len(ohlcv_df), mode='lines',
                                line=dict(color=color, width=1, dash = 'dash'),
                                name=f'position: {str(round(position["Entry"],5))} size: {str(size)}<br>Pnl: {str(round(position["uPnl"],5))}'))
        amount = 3
        price = 4
        side = 5
        orders = sorted(orders, key=lambda x: x[price], reverse=True)
        for order in orders:
            color = "red" if order[side] == "sell" else "green"
            legend = f'close: {str(order[price])} amount: {str(order[amount])}' if order[side] == "sell" else f'open: {str(order[price])} amount: {str(order[amount])}'
            fig.add_trace(go.Scatter(x=pd.to_datetime(ohlcv_df["timestamp"], unit='ms'),
                                    y=[order[price]] * len(ohlcv_df),
                                    mode='lines',
                                    line=dict(color=color, width=2, dash = 'dot'), name=legend))
        fig.update_layout(legend = dict(font = dict(size = 14)))
        st.plotly_chart(fig, key=f"dashboard_orders_{pos}")


def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
