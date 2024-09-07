import streamlit as st
from Exchange import Exchange
from User import Users
import pbgui_help

class Base:
    def __init__(self):
        self._users = Users()
        self._symbol = None
        self._market_type = "swap"
        self._ohlcv = True
        self._user = self._users.list()[0] # not saved
        self._market_types = ["futures"] # not saved
        self._exchange = None # not saved
        self._symbols = [] # not saved

    @property
    def user(self): return self._user
    @property
    def symbol(self): return self._symbol
    @property
    def market_type(self):
        if self._market_type == 'swap':
            return 'futures'
        return 'spot'
    @property
    def ohlcv(self): return self._ohlcv
    @property
    def market_types(self): return self._market_types
    @property
    def exchange(self): return self._exchange
    @property
    def symbols(self): return self._symbols

    @user.setter
    def user(self, new_user):
        if self._user != new_user or not self._exchange:
            if type(new_user) != str:
                raise ValueError("user must be str")
            self._user = new_user
            self._exchange = Exchange(self._users.find_exchange(self.user), self._users.find_user(self.user))
            self._exchange.load_symbols()
            if len(self._exchange.spot):
                self._market_types = ['futures', 'spot']
            else:
                self._market_types = ['futures']
                self._market_type = 'swap'
            if self._market_type == 'swap':
                self._symbols = ['Select Symbol'] + self._exchange.swap
            else:
                self._symbols = ['Select Symbol'] + self._exchange.spot
            if self._symbol not in self._symbols:
                self._symbol = self._symbols[0]

    @symbol.setter
    def symbol(self, new_symbol):
        if new_symbol:
            if type(new_symbol) != str:
                raise ValueError("symbol must be str")
        self._symbol = new_symbol

    @market_type.setter
    def market_type(self, new_market_type):
        if self.market_type != new_market_type:
            if new_market_type not in ["spot", "futures"]:
                raise ValueError("market_type must be futures or spot")
            if new_market_type == "futures":
                self._market_type = "swap"
                self._symbols = self._exchange.swap
            else:
                self._market_type = "spot"
                self._symbols = self._exchange.spot
            if self._symbol not in self._symbols:
                self._symbol = self._symbols[0]

    @ohlcv.setter
    def ohlcv(self, new_ohlcv):
        self._ohlcv = new_ohlcv

    def update_symbols(self):
        self.exchange.fetch_symbols()
        if self._market_type == 'swap':
            self._symbols = self._exchange.swap
        else:
            self._symbols = self._exchange.spot
        if self._symbol not in self._symbols:
            self._symbol = self._symbols[0]

    def edit_base(self):
        # Init session_state for keys
        if not self.symbols or not "base_user" in st.session_state:
            st.session_state.base_user = self.user
            self.user = self.user
            st.session_state.base_symbol = self.symbols[0]
            st.session_state.base_market_type = self.market_type
            st.session_state.base_ohlcv = self.ohlcv
        else:
            if "base_update_symbols" in st.session_state:
                if st.session_state.base_update_symbols:
                    self.update_symbols()
            if st.session_state.base_user != self.user:
                self.user = st.session_state.base_user
                st.session_state.base_market_type = self.market_type
                st.session_state.base_symbol = self.symbol
            if st.session_state.base_market_type != self.market_type:
                self.market_type = st.session_state.base_market_type
                st.session_state.base_symbol = self.symbol
            if st.session_state.base_symbol != self.symbol:
                self.symbol = st.session_state.base_symbol
            if st.session_state.base_ohlcv != self.ohlcv:
                self.ohlcv = st.session_state.base_ohlcv
        col_1, col_2, col_3 = st.columns([1,1,1])
        with col_1:
            print(self._users.list_single())
            st.selectbox('User',self._users.list_single(), key="base_user")
            st.session_state.placeholder = st.empty()
        with col_2:
            if self.symbol == "Select Symbol":
                color = "red"
            else:
                color = None
                st.session_state.base_symbol = self.symbol
            st.selectbox(f':{color}[SYMBOL]', self.symbols, key="base_symbol")
            st.button("Update Symbols from Exchange", key="base_update_symbols")
        with col_3:
            st.radio("MARKET_TYPE", self.market_types, key="base_market_type")
            st.checkbox("OHLCV", help=pbgui_help.ohlcv, key="base_ohlcv")
