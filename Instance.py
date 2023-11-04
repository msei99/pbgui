import streamlit as st
from pathlib import Path
#from Exchange import Exchange
from Base import Base
from Backtest import BacktestItem, BacktestResults
from streamlit_autorefresh import st_autorefresh
from Config import Config
import json
import glob
import pandas as pd
from datetime import datetime
from bokeh.plotting import figure
import numpy as np
from shutil import rmtree


class Instance(Base):
    def __init__(self):
        super().__init__()
        self._instance_path = None
        self._error = None # not saved
        self._symbol_ccxt = None # not saved
        self._config = Config() # not saved
        self._assigned_balance = 0
        self._leverage = 7
        self._price_distance_threshold = 0.5
        self._price_precision = 0.0
        self._price_step = 0.0
        self._long_mode = None
        self._long_min_markup = 0.0
        self._long_markup_range = 0.0
        self._short_mode = None
        self._short_min_markup = 0.0
        self._short_markup_range = 0.0
        self._ohlcv_df = None # not saved
        self._tf = None
        self._bt = None # not saved
        self._btresults = None # not saved
        self._trades = None # not saved
        self._sb = None # Not saved
        self._sd = None # Not saved
        self._ed = None # Not saved
        self._sb_change = False # Not saved
        self._sd_change = False # Not saved
        self._ed_change = False # Not saved
        self._balance = None # not saved

    @property
    def symbol_ccxt(self): return self._symbol_ccxt
    @property
    def tf(self):
        if not self._tf:
            self._tf = self.exchange.tf[0]
        return self._tf
    @property
    def leverage(self): return self._leverage
    @property
    def assigned_balance(self): return self._assigned_balance
    @property
    def price_distance_threshold(self): return self._price_distance_threshold
    @property
    def price_precision(self): return self._price_precision
    @property
    def price_step(self): return self._price_step
    @property
    def sb(self): return self._sb
    @property
    def sd(self): return self._sd
    @property
    def ed(self): return self._ed
    @property
    def sb_change(self): return self._sb_change
    @property
    def sd_change(self): return self._sd_change
    @property
    def ed_change(self): return self._ed_change
    @property
    def balance(self):
        self._balance = self._exchange.fetch_balance(self._market_type)
        return self._balance

    @leverage.setter
    def leverage(self, new_leverage):
        if self._leverage != new_leverage:
            self._leverage = new_leverage
            st.experimental_rerun()

    @assigned_balance.setter
    def assigned_balance(self, new_assigned_balance):
        if self._assigned_balance != new_assigned_balance:
            self._assigned_balance = new_assigned_balance
            st.experimental_rerun()

    @price_distance_threshold.setter
    def price_distance_threshold(self, new_price_distance_threshold):
        if self._price_distance_threshold != new_price_distance_threshold:
            self._price_distance_threshold = new_price_distance_threshold
            st.experimental_rerun()

    @price_precision.setter
    def price_precision(self, new_price_precision):
        if self._price_precision != new_price_precision:
            self._price_precision = new_price_precision
            st.experimental_rerun()

    @price_step.setter
    def price_step(self, new_price_step):
        if self._price_step != new_price_step:
            self._price_step = new_price_step
            st.experimental_rerun()

    @sb.setter
    def sb(self, new_sb):
        if self._sb != new_sb:
            self._sb = new_sb
            if self._bt:
                self._bt.sb = self.sb
            if self.sb != self._trades["balance"][0]:
                self._trades = self.trades_to_df()
            st.experimental_rerun()

    @sd.setter
    def sd(self, new_sd):
        if self._sd != new_sd:
            self._sd = new_sd
            if self._bt:
                self._bt.sd = self.sd
            if self.sd != datetime.fromtimestamp(self._trades["timestamp"][0]/1000).strftime("%Y-%m-%d"):
                self._trades = self.trades_to_df()
            st.experimental_rerun()

    @ed.setter
    def ed(self, new_ed):
        if self._ed != new_ed:
            self._ed = new_ed
            if self._bt:
                self._bt.ed = self.ed
            if self.ed != datetime.fromtimestamp(self._trades.iloc[-1]["timestamp"]/1000).strftime("%Y-%m-%d"):
                self._trades = self.trades_to_df()
            st.experimental_rerun()

    @sb_change.setter
    def sb_change(self, new_sb_change):
        if self._sb_change != new_sb_change:
            self._sb_change = new_sb_change
            if not self._sb_change:
                self._trades = self.trades_to_df()
            st.experimental_rerun()

    @sd_change.setter
    def sd_change(self, new_sd_change):
        if self._sd_change != new_sd_change:
            self._sd_change = new_sd_change
            if not self._sd_change:
                self._trades = self.trades_to_df()
            st.experimental_rerun()

    @ed_change.setter
    def ed_change(self, new_ed_change):
        if self._ed_change != new_ed_change:
            self._ed_change = new_ed_change
            if not self._ed_change:
                self._trades = self.trades_to_df()
            st.experimental_rerun()

    @tf.setter
    def tf(self, new_tf):
        if self._tf != new_tf:
            self._tf = new_tf
            self.save()
            st.experimental_rerun()

    def trades_to_df(self):
        file = Path(f'{self._instance_path}/trades.json')
        if not file.exists():
            return
        with open(file, "r", encoding='utf-8') as f:
            trades = json.load(f)
        if not trades:
            return
        data = {'timestamp': [],
                'psize': [],
                'pprice': [],
                'price': [],
                'balance': [],
                'equity': [],
                'wallet_exposure': []}
        df = pd.DataFrame(data)
        psize = 0
        price = 0
        pprice = 0
        balance = 0
        if self.sb_change:
            balance = self.sb
        if self.sd_change:
            new_trades = []
            for trade in trades:
                if trade["timestamp"]/1000 > datetime.timestamp(datetime.strptime(self.sd, "%Y-%m-%d")):
                    new_trades.append(trade)
            trades = new_trades
        if self.ed_change:
            new_trades = []
            for trade in trades:
                if trade["timestamp"]/1000 < datetime.timestamp(datetime.strptime(self.ed, "%Y-%m-%d"))+(24*60*60):
                    new_trades.append(trade)
            trades = new_trades
        if self.exchange.id == "bitget":
            for trade in trades:
                if psize < 0:
                    psize = 0
                    price = 0
                    pprice = 0
                    balance = 0
                    df = pd.DataFrame(data)
                    if self.sb_change:
                        balance = self.sb
                if trade["side"].startswith("open_"):
                    last_psize = psize
                    psize = round(psize + trade["amount"],10)
                    pprice = (pprice*last_psize + trade["amount"]*trade["price"])/psize
                    price = trade["price"]
                if trade["side"].startswith("close_") and psize > 0:
                    last_psize = psize
                    psize = round(psize - trade["amount"],10)
                    win = trade["amount"] * trade["price"] - trade["amount"] * pprice
                    price = trade["price"]
                    balance = balance + win
                timestamp = trade["timestamp"]
                if price:
                    df.loc[len(df.index)] = [timestamp, psize, pprice, price, balance, 0, 0]
        elif self.exchange.id == "bybit":
            for trade in trades:
                if psize < 0:
                    psize = 0
                    price = 0
                    pprice = 0
                    balance = 0
                    df = pd.DataFrame(data)
                    if self.sb_change:
                        balance = self.sb
                if trade["type"] and trade["side"] == "buy":
                    last_psize = psize
                    psize = round(psize + trade["amount"],10)
                    pprice = (pprice*last_psize + trade["amount"]*trade["price"])/psize
                    price = trade["price"]
                    balance = balance - trade["fee"]["cost"]
                if trade["type"] and trade["side"] == "sell" and psize > 0:
                    last_psize = psize
                    psize = round(psize - trade["amount"],10)
                    win = trade["amount"] * trade["price"] - trade["amount"] * pprice
                    price = trade["price"]
                    balance = balance - trade["fee"]["cost"]
                    balance = balance + win
                if not trade["type"]:
                    balance = balance - float(trade["info"]["execFee"])
# ccxt has a bug with negative fees on bybit. So I use the "info" "execFee" for fixing this
#                    balance = balance - trade["fee"]["cost"]
                timestamp = trade["timestamp"]
                df.loc[len(df.index)] = [timestamp, psize, pprice, price, balance, 0, 0]
        elif self.exchange.id == "binance":
            for trade in trades:
                if psize < 0:
                    psize = 0
                    price = 0
                    pprice = 0
                    balance = 0
                    df = pd.DataFrame(data)
                    if self.sb_change:
                        balance = self.sb
                if trade["side"] == "buy":
                    last_psize = psize
                    psize = round(psize + trade["amount"],10)
                    pprice = (pprice*last_psize + trade["amount"]*trade["price"])/psize
                    price = trade["price"]
                    balance = balance - trade["fee"]["cost"]
                if trade["side"] == "sell" and psize > 0:
                    last_psize = psize
                    psize = round(psize - trade["amount"],10)
                    win = trade["amount"] * trade["price"] - trade["amount"] * pprice
                    price = trade["price"]
                    balance = balance - trade["fee"]["cost"]
                    balance = balance + win
                timestamp = trade["timestamp"]
                df.loc[len(df.index)] = [timestamp, psize, pprice, price, balance, 0, 0]
        if not self.sb_change:
            my_balance = self.balance
            df["balance"] = df["balance"].apply(lambda x: x + my_balance - balance)
#        print(df)
        return df

    def remove(self):
        rmtree(self._instance_path, ignore_errors=True)

    def fetch_bill(self):
        file = Path(f'{self._instance_path}/bill.json')
        trades = []
        save = False
        if file.exists():
            with open(file, "r", encoding='utf-8') as f:
                trades = json.load(f)
            try:
                since = int(trades[-1]["cTime"])
            except ValueError:
                since = 1577840461000
        else:
            since = 1577840461000
        new_trades = self._exchange.fetch_bill(self.symbol_ccxt, self._market_type, since)
        if new_trades:
            for trade in new_trades:
                if not any(trade["id"] in sub["id"] for sub in trades):
                    trades.append(trade)
                    save = True
        if save:
            with open(file, "w", encoding='utf-8') as f:
                json.dump(trades, f, indent=4)

    def fetch_trades(self):
        file = Path(f'{self._instance_path}/trades.json')
        trades = []
        save = False
        if file.exists():
            with open(file, "r", encoding='utf-8') as f:
                trades = json.load(f)
            if type(trades[-1]["timestamp"]) == int:
                since = trades[-1]["timestamp"]
            else:
                since = 1577840461000
        else:
            since = 1577840461000
        new_trades = self._exchange.fetch_trades(self.symbol_ccxt, self._market_type, since)
        if new_trades:
            for trade in new_trades:
                if not any(trade["id"] in sub["id"] for sub in trades):
                    trades.append(trade)
                    save = True
        if save:
            with open(file, "w", encoding='utf-8') as f:
                json.dump(trades, f, indent=4)

    def view_ohlcv(self):
        ohlcv = self.exchange.fetch_ohlcv(self.symbol_ccxt, self._market_type, timeframe=self.tf, limit=100)
        self._ohlcv_df = pd.DataFrame(ohlcv, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        self._ohlcv_df["color"] = np.where(self._ohlcv_df["close"] > self._ohlcv_df["open"], "green", "red")
        w = (self._ohlcv_df["timestamp"][1] - self._ohlcv_df["timestamp"][0]) * 0.8
        p = figure(
            x_axis_label='date',
            y_axis_label='USDT',
            x_axis_type='datetime',
            tools = "pan,box_zoom,wheel_zoom,save,reset",
            active_scroll="wheel_zoom")
        p.segment(x0=self._ohlcv_df["timestamp"], y0=self._ohlcv_df["high"], x1=self._ohlcv_df["timestamp"], y1=self._ohlcv_df["low"], color=self._ohlcv_df["color"])
        p.vbar(x=self._ohlcv_df["timestamp"], width=w, top=self._ohlcv_df["open"], bottom=self._ohlcv_df["close"], color=self._ohlcv_df["color"])
        price = self.exchange.fetch_price(self.symbol_ccxt, self._market_type)
        position = self.exchange.fetch_position(self.symbol_ccxt, self._market_type)
#        st.write(position)
        st.markdown(f'### Symbol: {self.symbol} {self.balance} USDT')
        orders = self.exchange.fetch_open_orders(self.symbol_ccxt, self._market_type)
        # price
        color = "red" if price["last"] < self._ohlcv_df["open"].iloc[-1] else "green"
        self._last_price = price["last"]
        p.line(x=self._ohlcv_df["timestamp"], y=price["last"], color=color, legend_label=f'price: {str(price["last"])}')
        # position
        if position:
            if position["entryPrice"]:
                color = "red" if price["last"] < position["entryPrice"] else "green"
                p.line(x=self._ohlcv_df["timestamp"], y=position["entryPrice"], color=color, line_dash="dashed", legend_label=f'position: {str(position["entryPrice"])} qty: {str(position["contracts"])} Pnl: {str(position["unrealizedPnl"])}')
        # open/close orders
        for order in orders:
            color = "red" if order["side"] == "sell" else "green"
            legend = f'close: {str(order["price"])} qty: {str(order["amount"])}' if order["side"] == "sell" else f'open: {str(order["price"])} qty: {str(order["amount"])}'
            p.line(x=self._ohlcv_df["timestamp"], y=order["price"], color=color, line_width=2, line_dash="dotted", legend_label=legend)
        p.legend.location = "bottom_left"
        st.bokeh_chart(p, use_container_width=True)

    def compare_history(self):
        if self.exchange.id not in ["bybit", "bitget", "binance"]:
            st.write("History is only supported on bybit, bitget and binance (binance not fully tested)")
        if not isinstance(self._trades, pd.DataFrame):
            self.fetch_trades()
            self._trades = self.trades_to_df()
        self.sb = self._trades["balance"][0]
        self.sd = datetime.fromtimestamp(self._trades["timestamp"][0]/1000).strftime("%Y-%m-%d")
        self.ed = datetime.fromtimestamp(self._trades.iloc[-1]["timestamp"]/1000).strftime("%Y-%m-%d")
        if not self._bt:
            self._bt = BacktestItem(self._config.config)
            self._bt.user = self.user
            self._bt.symbol = self.symbol
            self._bt.market_type = self.market_type
            self._bt.sb = self.sb
            self._bt.sd = self.sd
            self._bt.ed = self.ed
        st.markdown(f'### Symbol: {self.symbol} {self.balance} USDT')
        col_1, col_2, col_3, col_4, col_end = st.columns([1,1,1,1,5])
        with col_1:
            self.sb_change = st.checkbox("Change", value=self.sb_change, key="sb_change", help=None, on_change=None)
            self.sb = st.number_input('STARTING_BALANCE',value=self.sb,step=500.0, disabled=not self.sb_change)
        with col_2:
            self.sd_change = st.checkbox("Change", value=self.sd_change, key="sd_change", help=None, on_change=None)
            self.sd = st.date_input("START_DATE", datetime.strptime(self.sd, '%Y-%m-%d'), format="YYYY-MM-DD", disabled=not self.sd_change).strftime("%Y-%m-%d")
        with col_3:
            self.ed_change = st.checkbox("Change", value=self.ed_change, key="ed_change", help=None, on_change=None)
            self.ed = st.date_input("END_DATE", datetime.strptime(self.ed, '%Y-%m-%d'), format="YYYY-MM-DD", disabled=not self.ed_change).strftime("%Y-%m-%d")
        with col_4:
            st.write("## ")
            if self._bt.is_running():
                if st.button("Stop"):
                    self._bt.stop()
                    st.experimental_rerun()
            elif self._bt.is_finish():
                self._bt.remove()
                self._btresults = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
                self._btresults.match_config(self.symbol, self._config.config)
                st.experimental_rerun()
            else:
                if st.button("Run"):
                    self._bt.save()
                    self._bt.log = Path(f'{self._bt.file}.log')
                    self._bt.run()
                    st.experimental_rerun()
        if not self._btresults:
            self._btresults = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
            self._btresults.match_config(self.symbol, self._config.config)
        self._btresults.view(trades = self._trades)
        if self._bt.is_running():
            st_autorefresh(interval=10000, limit=None, key="refresh_backtest_running")

    def edit_config(self):
        self._config.edit_config()

    def refresh(self):
        path = self._instance_path
        self.__init__()
        self.load(path)

    def load(self, path: Path):
        file = Path(f'{path}/instance.cfg')
        if file.exists():
            with open(file, "r", encoding='utf-8') as f:
                state = json.load(f)
                self.__dict__.update(state)
                self.user = state["_user"]
                if not self._symbol_ccxt:
                    self._symbol_ccxt = self.exchange.symbol_to_exchange_symbol(self.symbol, self._market_type)
                self._config = Config(f'{self._instance_path}/config.json')
                self._config.load_config()
        else:
            print(f'{file} not found')

    def save(self):
        if self.user and self.symbol and self.market_type:
            pbgdir = Path.cwd()
            instance_path = Path(f'{pbgdir}/data/instances/{self._user}_{self._symbol}_{self.market_type}')
            self._instance_path = str(instance_path)
            if not instance_path.exists():
                instance_path.mkdir(parents=True)
            self._config.config_file = f'{self._instance_path}/config.json'
            self._config.save_config()
            file = Path(f'{instance_path}/instance.cfg')
            state = self.__dict__.copy()
            del state['_error']
            del state['_market_types']
            del state['_users']
            del state['_symbols']
            del state['_exchange']
            del state['_ohlcv_df']
            del state['_config']
            del state['_bt']
            del state['_btresults']
            del state['_trades']
            del state['_balance']
            del state['_sb']
            del state['_sd']
            del state['_ed']
            del state['_sb_change']
            del state['_sd_change']
            del state['_ed_change']
            with open(file, "w", encoding='utf-8') as f:
                json.dump(state, f, indent=4)
        else:
            self._error = ""

class Instances:
    def __init__(self, backtest_path: str = None):
        self.instances = []
        self.index = 0
        pbgdir = Path.cwd()
        self.instances_path = f'{pbgdir}/data/instances'
        self.load()

    def __iter__(self):
        return iter(self.instances)

    def __next__(self):
        if self.index > len(self.instances):
            raise StopIteration
        self.index += 1
        return next(self)
    
    def list(self):
        return list(map(lambda c: c.user, self.instances))
    
    def remove(self, instance: Instance):
        instance.remove()
        self.instances.remove(instance)

    def load(self):
        p = str(Path(f'{self.instances_path}/*'))
        instances = glob.glob(p)
        for instance in instances:
            inst = Instance()
            inst.load(instance)
            self.instances.append(inst)
        self.instances = sorted(self.instances, key=lambda d: d.user) 


def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
