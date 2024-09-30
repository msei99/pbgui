import streamlit as st
from pathlib import Path
import streamlit_scrollable_textbox as stx
from Base import Base
from Backtest import BacktestItem, BacktestResults
import pbgui_help
from streamlit_autorefresh import st_autorefresh
from Config import Config
import shutil
import json
import glob
import pandas as pd
from datetime import datetime
from time import sleep
from bokeh.plotting import figure
import numpy as np
from shutil import rmtree
import sys
import traceback

class Instance(Base):
    def __init__(self, config: str = None):
        super().__init__()
        self._config = Config(config=config)
        self._instance_path = None
        self._enabled = False
        self._multi = False
        self._enabled_on = "disabled"
        self._version = 0
        self._error = None # not saved
        self._symbol_ccxt = None
        self._assigned_balance = 0
        self._co = -1
        self._leverage = 7
        self._price_distance_threshold = 0.5
        self._price_precision = 0.0
        self._price_step = 0.0
        self._long_mode = "normal"
        self._short_mode = "normal"
        self._tf = None
        self._ohlcv_df = None # not saved
        self._bt = None # not saved
        self._btresults = None # not saved
        self._trades = None # not saved
        self._sb = None # Not saved
        self._sd = None # Not saved
        self._ed = None # Not saved
        self._sb_change = False # Not saved
        self._sd_change = False # Not saved
        self._ed_change = False # Not saved
        self._status = {} # not saved
        self._statusll = 0 # not saved

    @property
    def instance_path(self): return self._instance_path
    @property
    def config(self): return self._config.config
    @config.setter
    def config(self, new_config):
        if self._config.config != new_config:
            self._config.config = new_config
    @property
    def enabled(self): return self._enabled
    @property
    def multi(self): return self._multi
    @property
    def enabled_on(self): return self._enabled_on
    @property
    def version(self): return self._version
    @property
    def preview_grid(self): return self._config.preview_grid
    @property
    def symbol_ccxt(self): return self._symbol_ccxt
    @property
    def tf(self):
        if not self._tf:
            self._tf = self.exchange.tf[0]
        return self._tf
    @property
    def co(self): return self._co
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
    def long_mode(self): return self._long_mode
    @property
    def short_mode(self): return self._short_mode
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
        self.load_status()
        if "balance" in self._status:
            return self._status["balance"]
        else:
            return 0
    @property
    def upnl(self):
        if not self.load_status(): return 0
        try: 
            if self.market_type == "spot":
                upnl = self._status["spot_balance"] * self._status["price"]["last"]
            elif self.market_type == "futures":
                if self._status["position"]:
                    upnl = self._status["position"]["unrealizedPnl"]
                else:
                    upnl = 0
            else:
                upnl = 0
            if not upnl:
                upnl = 0
            return upnl
        except Exception as e:
            print(f'Error calculating upnl: {self.user} {self.symbol} {self.market_type} {e}')
            return 0
    @property
    def psize(self):
        if not self.load_status(): return 0
        try:
            if self.market_type == "spot":
                if "spot_balance" in self._status:
                    psize = self._status["spot_balance"]
            elif self.market_type == "futures":
                if "position" in self._status:
                    if self._status["position"]:
                        if self._status["position"]["contracts"]:
                            psize = round(self._status["position"]["contracts"]*self._status["position"]["contractSize"],2)
                        else:
                            psize = 0
                    else:
                        psize = 0
            else:
                psize = 0
            return psize
        except Exception as e:
            print(f'Error calculating psize: {self.user} {self.symbol} {self.market_type} {e}')
            return 0
    @property
    def price(self):
        if not self.load_status(): return 0
        try: 
            price = self._status["price"]["last"]
            if not price: return 0
            return price
        except Exception as e:
            print(f'Error calculating price: {self.user} {self.symbol} {self.market_type} {e}')
            return 0
    @property
    def next_tp(self):
        next_tp = 0
        if not self.load_status(): return 0
        try:
            for order in self._status["orders"]:
                if order["side"] == "sell":
                    if next_tp == 0 or next_tp > order["price"]:
                        next_tp = order["price"]
            return next_tp
        except Exception as e:
            print(f'Error calculating next_tp: {self.user} {self.symbol} {self.market_type} {e}')
            return 0
    @property
    def next_dca(self):
        next_dca = 0
        if not self.load_status(): return 0
        try:
            for order in self._status["orders"]:
                if order["side"] == "buy":
                    if next_dca < order["price"]:
                        next_dca = order["price"]
            return next_dca
        except Exception as e:
            print(f'Error calculating next_dca: {self.user} {self.symbol} {self.market_type} {e}')
            return 0
    @property
    def dca(self):
        dca = 0
        if not self.load_status(): return 0
        try:
            for order in self._status["orders"]:
                if order["side"] == "buy":
                    dca += 1
            return dca
        except Exception as e:
            print(f'Error calculating dca: {self.user} {self.symbol} {self.market_type} {e}')
            return 0

    @multi.setter
    def multi(self, new_multi):
        self._multi = new_multi
        self.save()

    @enabled_on.setter
    def enabled_on(self, new_enabled_on):
        self._enabled_on = new_enabled_on

    @version.setter
    def version(self, new_version):
        self._version = new_version

    @co.setter
    def co(self, new_co):
        self._co = new_co

    @leverage.setter
    def leverage(self, new_leverage):
        self._leverage = new_leverage

    @assigned_balance.setter
    def assigned_balance(self, new_assigned_balance):
        self._assigned_balance = new_assigned_balance

    @price_distance_threshold.setter
    def price_distance_threshold(self, new_price_distance_threshold):
        self._price_distance_threshold = new_price_distance_threshold

    @price_precision.setter
    def price_precision(self, new_price_precision):
        self._price_precision = new_price_precision

    @price_step.setter
    def price_step(self, new_price_step):
        self._price_step = new_price_step

    @long_mode.setter
    def long_mode(self, new_long_mode):
        self._long_mode = new_long_mode

    @short_mode.setter
    def short_mode(self, new_short_mode):
        self._short_mode = new_short_mode

    @sb.setter
    def sb(self, new_sb):
        if self._sb != new_sb:
            self._sb = new_sb
            if self._bt:
                self._bt.sb = self.sb
            if self.sb != self._trades["balance"][0]:
                self._trades = self.trades_to_df()

    @sd.setter
    def sd(self, new_sd):
        if self._sd != new_sd:
            self._sd = new_sd
            if self._bt:
                self._bt.sd = self.sd
            if self.sd != datetime.fromtimestamp(self._trades["timestamp"][0]/1000).strftime("%Y-%m-%d"):
                self._trades = self.trades_to_df()

    @ed.setter
    def ed(self, new_ed):
        if self._ed != new_ed:
            self._ed = new_ed
            if self._bt:
                self._bt.ed = self.ed
            if self.ed != datetime.fromtimestamp(self._trades.iloc[-1]["timestamp"]/1000).strftime("%Y-%m-%d"):
                self._trades = self.trades_to_df()

    @sb_change.setter
    def sb_change(self, new_sb_change):
        if self._sb_change != new_sb_change:
            self._sb_change = new_sb_change
            if not self._sb_change:
                self._trades = self.trades_to_df()

    @sd_change.setter
    def sd_change(self, new_sd_change):
        if self._sd_change != new_sd_change:
            self._sd_change = new_sd_change
            if not self._sd_change:
                self._trades = self.trades_to_df()

    @ed_change.setter
    def ed_change(self, new_ed_change):
        if self._ed_change != new_ed_change:
            self._ed_change = new_ed_change
            if not self._ed_change:
                self._trades = self.trades_to_df()

    @tf.setter
    def tf(self, new_tf):
        if self._tf != new_tf:
            self._tf = new_tf
            self.save()

    def trades_to_df(self):
        ffile = Path(f'{self._instance_path}/fundings.json')
        fundings = []
        if ffile.exists():
            try:
                with open(ffile, "r", encoding='utf-8') as f:
                    fundings = json.load(f)
            except Exception as e:
                print(f'{str(ffile)} is corrupted {e}')
        file = Path(f'{self._instance_path}/trades.json')
        if not file.exists():
            return
        try:
            with open(file, "r", encoding='utf-8') as f:
                trades = json.load(f)
        except Exception as e:
            print(f'{str(file)} is corrupted {e}')
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
                if trade["info"]["tradeSide"].startswith("open"):
                    last_psize = psize
                    psize = round(psize + trade["amount"],10)
                    pprice = (pprice*last_psize + trade["amount"]*trade["price"])/psize
                    price = trade["price"]
                if trade["info"]["tradeSide"].startswith("close") and psize > 0:
                    last_psize = psize
                    psize = round(psize - trade["amount"],10)
                    win = trade["amount"] * trade["price"] - trade["amount"] * pprice
                    price = trade["price"]
                    balance = balance + win
                if len(fundings) > 0:
                    while fundings[0]["timestamp"] < trade["timestamp"]:
                        funding = fundings.pop(0)
                        balance = balance + funding["amount"]
                        if len(fundings) == 0:
                            break
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
                    if self.market_type == "spot":
                        psize = psize - trade["fee"]["cost"]
                    else:
                        balance = balance - trade["fee"]["cost"]
                    psize = round(psize + trade["amount"],10)
                    pprice = (pprice*last_psize + trade["amount"]*trade["price"])/psize
                    price = trade["price"]
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
        elif self.exchange.id == "kucoinfutures":
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
                if len(fundings) > 0:
                    while fundings[0]["timestamp"] < trade["timestamp"]:
                        funding = fundings.pop(0)
                        balance = balance + funding["amount"]
                        if len(fundings) == 0:
                            break
                timestamp = trade["timestamp"]
                df.loc[len(df.index)] = [timestamp, psize, pprice, price, balance, 0, 0]
        elif self.exchange.id == "bingx":
            for trade in trades:
                if psize < 0:
                    psize = 0
                    price = 0
                    pprice = 0
                    balance = 0
                    df = pd.DataFrame(data)
                    if self.sb_change:
                        balance = self.sb
                if trade["side"] == "BUY":
                    last_psize = psize
                    psize = round(psize + trade["amount"],10)
                    pprice = (pprice*last_psize + trade["amount"]*trade["price"])/psize
                    price = trade["price"]
                    balance = balance - trade["fee"]
                if trade["side"] == "SELL" and psize > 0:
                    last_psize = psize
                    psize = round(psize - trade["amount"],10)
                    win = trade["amount"] * trade["price"] - trade["amount"] * pprice
                    price = trade["price"]
                    balance = balance - trade["fee"]
                    balance = balance + win
                if len(fundings) > 0:
                    while fundings[0]["timestamp"] < trade["timestamp"]:
                        funding = fundings.pop(0)
                        balance = balance + funding["amount"]
                        if len(fundings) == 0:
                            break
                timestamp = trade["timestamp"]
                df.loc[len(df.index)] = [timestamp, psize, pprice, price, balance, 0, 0]
        elif self.exchange.id == "okx":
            if len(trades) > 0:
                size = trades[0]["cost"] / trades[0]["price"] / trades[0]["amount"]
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
                    psize = round(psize + (trade["amount"]*size),10)
                    pprice = (pprice*last_psize + (trade["amount"]*size)*trade["price"])/psize
                    price = trade["price"]
                    balance = balance - trade["fee"]["cost"]
                if trade["side"] == "sell" and psize > 0:
                    last_psize = psize
                    psize = round(psize - (trade["amount"]*size),10)
                    win = (trade["amount"]*size) * trade["price"] - (trade["amount"]*size) * pprice
                    price = trade["price"]
                    balance = balance - trade["fee"]["cost"]
                    balance = balance + win
                if len(fundings) > 0:
                    while fundings[0]["timestamp"] < trade["timestamp"]:
                        funding = fundings.pop(0)
                        balance = balance + funding["amount"]
                        if len(fundings) == 0:
                            break
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
                    if self.market_type == "spot":
                        psize = psize - trade["fee"]["cost"]+trade["fee"]["cost"]/100*10
                    else:
                        balance = balance - trade["fee"]["cost"]
                    psize = round(psize + trade["amount"],10)
                    pprice = (pprice*last_psize + trade["amount"]*trade["price"])/psize
                    price = trade["price"]
                if trade["side"] == "sell" and psize > 0:
                    last_psize = psize
                    psize = round(psize - trade["amount"],10)
                    win = trade["amount"] * trade["price"] - trade["amount"] * pprice
                    price = trade["price"]
                    balance = balance - trade["fee"]["cost"]
                    balance = balance + win
                if len(fundings) > 0:
                    while fundings[0]["timestamp"] < trade["timestamp"]:
                        funding = fundings.pop(0)
                        balance = balance + funding["amount"]
                        if len(fundings) == 0:
                            break
                timestamp = trade["timestamp"]
                df.loc[len(df.index)] = [timestamp, psize, pprice, price, balance, 0, 0]
        if not self.sb_change:
            if self.market_type == "spot":
                if self._status:
                    spot_balance = self._status["spot_balance"]
                    price = self.price
                else:
                    spot_balance = self.fetch_spot_balance()
                    price = self.fetch_price()["last"]
                my_balance = self.balance + (spot_balance * pprice)
            else:
                my_balance = self.balance
                if len(fundings) > 0:
                    for funding in fundings:
                        my_balance = my_balance + funding["amount"]
            if self.exchange.id == "kucoinfutures":
                df["balance"] = df["balance"].apply(lambda x: x + my_balance - balance - self.upnl)
            else:
                df["balance"] = df["balance"].apply(lambda x: x + my_balance - balance)
#        print(df)
        return df

    def fetch_balance(self):
        return self._exchange.fetch_balance(self._market_type)

    def fetch_position(self):
        return self.exchange.fetch_position(self.symbol_ccxt, self._market_type)

    def fetch_spot_balance(self):
        symbol = self._symbol_ccxt.split('/')[0]
        return self._exchange.fetch_balance(self._market_type, symbol)

    def fetch_price(self):
        return self.exchange.fetch_price(self.symbol_ccxt, self._market_type)

    def fetch_open_orders(self):
        return self.exchange.fetch_open_orders(self.symbol_ccxt, self._market_type)

    def fetch_timestamp(self):
        return self.exchange.fetch_timestamp()

    def remove(self):
        # Backup
        source = f'{self._instance_path}'
        pbgdir = Path.cwd()
        if Path(source).exists():
            date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            name = self._instance_path.split("/")[-1]
            destination = Path(f'{pbgdir}/data/backup/instances/{name}/{date}')
            if not destination.exists():
                destination.mkdir(parents=True)
        shutil.copytree(source, destination, dirs_exist_ok=True)
        # Remove
        rmtree(self._instance_path, ignore_errors=True)

    def fetch_trades(self):
        if self.exchange.id not in ["binance", "kucoinfutures", "bitget", "bybit", "bingx", "okx"]:
            return
        file = Path(f'{self._instance_path}/trades.json')
        file_lft = Path(f'{self._instance_path}/last_fetch_trades.json')
        trades = []
        save = False
        ltrades = 0
        since = 1577840461000
        if file_lft.exists():
            try:
                with open(file_lft, "r", encoding='utf-8') as f:
                    since = json.load(f)
            except Exception as e:
                print(f'{str(file_lft)} is corrupted {e}')
                file_lft.unlink()
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    trades = json.load(f)
                    ltrades = len(trades)
                if type(trades[-1]["timestamp"]) == int:
                    since = trades[-1]["timestamp"]
            except Exception as e:
                print(f'{str(file)} is corrupted {e}')
        now = self.fetch_timestamp()
        new_trades = self._exchange.fetch_trades(self.symbol_ccxt, self._market_type, since)
        if new_trades:
            for trade in new_trades:
                if not any(trade["id"] in sub["id"] for sub in trades):
                    trades.append(trade)
                    save = True
        since = now
        with open(file_lft, "w", encoding='utf-8') as f:
            json.dump(since, f, indent=4)
        if save:
            with open(file, "w", encoding='utf-8') as f:
                json.dump(trades, f, indent=4)
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} {self.user} {self.symbol} Fetched {len(trades) - ltrades} trades')

    def save_trades(self, trades : json):
        if trades:
            data = []
            for trade in trades:
                t = {
                    'time': trade["timestamp"],
                    'symbol': trade["symbol"]
                }

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
        if self._status:
            balance = self.balance
            price = self.price
            orders = self._status["orders"]
            if self.market_type == "futures":
                position = self._status["position"]
            else: 
                position = None
                spot_balance = self._status["spot_balance"]
        else:
            balance = self.fetch_balance()
            price = self.fetch_price()["last"]
            orders = self.fetch_open_orders()
            if self.market_type == "futures":
                position = self.fetch_position()
            else: 
                position = None
                spot_balance = self.fetch_spot_balance()
        # price
        color = "red" if price < self._ohlcv_df["open"].iloc[-1] else "green"
        p.line(x=self._ohlcv_df["timestamp"], y=price, color=color, legend_label=f'price: {str(price)}')
        # position
        if position:
            if position["entryPrice"]:
                color = "red" if price < position["entryPrice"] else "green"
                size = position["contractSize"]
                qty = position["contracts"] * size
                p.line(x=self._ohlcv_df["timestamp"], y=position["entryPrice"], color=color, line_dash="dashed", legend_label=f'position: {str(position["entryPrice"])} qty: {str(qty)} Pnl: {str(position["unrealizedPnl"])}')
            else:
                size = 1.0
        else:
            size = 1.0
        if self.market_type == "futures":
            st.markdown(f'### Symbol: {self.symbol} {round(balance,2)} USDT')
        else: 
            symbol = self._symbol_ccxt.split('/')[0]
            st.markdown(f'### Symbol: {self.symbol} {round(balance,2)} USDT')
            st.markdown(f'### Asset: {spot_balance} {symbol} = {round(spot_balance*price,2)} USDT')
        # open/close orders
        # sort orders by price reversed
        orders = sorted(orders, key=lambda x: x["price"], reverse=True)
        for order in orders:
            color = "red" if order["side"] == "sell" else "green"
            qty = order["amount"] * size
            legend = f'close: {str(order["price"])} qty: {str(qty)}' if order["side"] == "sell" else f'open: {str(order["price"])} qty: {str(qty)}'
            p.line(x=self._ohlcv_df["timestamp"], y=order["price"], color=color, line_width=2, line_dash="dotted", legend_label=legend)
        p.legend.location = "bottom_left"
        st.bokeh_chart(p, use_container_width=True)

    def view_grid(self, sb: float = None):
        if self._config.type != "recursive_grid" or self.exchange.id not in ["binance", "kucoinfutures", "bitget", "bybit", "bingx", "okx"]:
            return
        self._symbol_ccxt = self.exchange.symbol_to_exchange_symbol(self.symbol, self._market_type)
        print(self._symbol_ccxt)
        ohlcv = self.exchange.fetch_ohlcv(self.symbol_ccxt, self._market_type, timeframe="4h", limit=100)
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
        sys.path.insert(0,st.session_state.pbdir)
        sys.path.insert(0,f'{st.session_state.pbdir}')
        try:
            njit_funcs_recursive_grid = __import__("njit_funcs_recursive_grid")
            # njit_funcs = __import__("njit_funcs")
        except Exception as e:
            st.write("### Can not import grid functions from passivbot")
            traceback.print_exc()
            return
        print(self.symbol)
        symbol_info, min_costs, min_qtys, price_steps, qty_steps, c_mults = self.exchange.fetch_symbol_info(self.symbol, self.market_type)
        # print(symbol_info, min_costs, min_qtys, price_steps, qty_steps, c_mults)
        short = json.loads(self._config.config)["short"]
        long = json.loads(self._config.config)["long"]
        price = self.fetch_price()["last"]
        if sb:
            balance = sb
        else:
            balance = self.balance
        if balance == 0:
            balance = 1000
            # return
        if short["enabled"]:
            entries_short = njit_funcs_recursive_grid.calc_recursive_entries_short(
                balance,
                0.0,            # psize,
                0.0,            # pprice
                price,          # lowest_ask, self.tickers[symbol]["ask"],
                price,          # ema_band_upper, self.emas_short[symbol].max(),
                False,          # self.inverse,
                qty_steps,      # self.qty_steps[symbol],
                price_steps,    # self.price_steps[symbol],
                min_qtys,       # self.min_qtys[symbol],
                min_costs,      # self.min_costs[symbol],
                c_mults,        # self.c_mults[symbol],
                short["initial_qty_pct"],
                short["initial_eprice_ema_dist"],
                short["ddown_factor"],
                short["rentry_pprice_dist"],
                short["rentry_pprice_dist_wallet_exposure_weighting"],
                short["wallet_exposure_limit"],
                short["auto_unstuck_ema_dist"],
                short["auto_unstuck_wallet_exposure_threshold"],
                0,              # auto_unstuck_on_timer,
                whole_grid=True,
            )
            # print(entries_short)
            entries_short = sorted(entries_short, key=lambda x: x[1], reverse=True)
            for entry in entries_short:
                # print(entry)
                # print(entry[0])
                # print(entry[1])
                p.line(x=self._ohlcv_df["timestamp"], y=entry[1], color="red", line_width=2, line_dash="dotted", legend_label=f"{entry[1]}: {entry[0]}")
        if long["enabled"]:
            entries_long = njit_funcs_recursive_grid.calc_recursive_entries_long(
                balance,
                0.0,            # psize,
                0.0,            # pprice
                price,          # highest_bid, self.tickers[symbol]["bid"],
                price,          # ema_band_lower, self.emas_long[symbol].min(),
                False,          # self.inverse,
                qty_steps,      # self.qty_steps[symbol],
                price_steps,    # self.price_steps[symbol],
                min_qtys,       # self.min_qtys[symbol],
                min_costs,      # self.min_costs[symbol],
                c_mults,        # self.c_mults[symbol],
                long["initial_qty_pct"],
                long["initial_eprice_ema_dist"],
                long["ddown_factor"],
                long["rentry_pprice_dist"],
                long["rentry_pprice_dist_wallet_exposure_weighting"],
                long["wallet_exposure_limit"],
                long["auto_unstuck_ema_dist"],
                long["auto_unstuck_wallet_exposure_threshold"],
                0,              # auto_unstuck_on_timer,
                whole_grid=True,
            )
            # print(entries_long)
            for entry in entries_long:
                # print(entry)
                # print(entry[0])
                # print(entry[1])
                p.line(x=self._ohlcv_df["timestamp"], y=entry[1], color="green", line_width=2, line_dash="dotted", legend_label=f"{entry[1]}: {entry[0]}")
        p.legend.location = "bottom_left"
        st.bokeh_chart(p, use_container_width=True)

    def compare_history(self):
        if not isinstance(self._trades, pd.DataFrame):
            self.fetch_trades()
            self._trades = self.trades_to_df()
        if self._trades is None:
            st.write("### No Trades available.")
            return
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
            if "key_instance_sb_change" in st.session_state:
                self.sb_change = st.session_state.key_instance_sb_change
            st.checkbox("Change", value=self.sb_change, help=None, on_change=None, key="key_instance_sb_change")
            if "key_instance_sb" in st.session_state:
                self.sb = st.session_state.key_instance_sb
            st.number_input('STARTING_BALANCE',value=self.sb,step=500.0, disabled=not self.sb_change, key="key_instance_sb")
        with col_2:
            if "key_instance_sd_change" in st.session_state:
                self.sd_change = st.session_state.key_instance_sd_change
            st.checkbox("Change", value=self.sd_change, help=None, on_change=None, key="key_instance_sd_change")
            if "key_instance_sd" in st.session_state:
                self.sd = st.session_state.key_instance_sd.strftime("%Y-%m-%d")
            st.date_input("START_DATE", datetime.strptime(self.sd, '%Y-%m-%d'), format="YYYY-MM-DD", disabled=not self.sd_change, key="key_instance_sd")
        with col_3:
            if "key_instance_ed_change" in st.session_state:
                self.ed_change = st.session_state.key_instance_ed_change
            st.checkbox("Change", value=self.ed_change, help=None, on_change=None, key="key_instance_ed_change")
            if "key_instance_ed" in st.session_state:
                self.ed = st.session_state.key_instance_ed.strftime("%Y-%m-%d")
            st.date_input("END_DATE", datetime.strptime(self.ed, '%Y-%m-%d'), format="YYYY-MM-DD", disabled=not self.ed_change, key="key_instance_ed")
        with col_4:
            st.write("## ")
            if self._bt.is_running():
                if st.button("Stop"):
                    self._bt.stop()
                    st.rerun()
            elif self._bt.is_finish():
                self._bt.remove()
                self._btresults = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
                self._btresults.match_config(self.symbol, self._config.config, self.market_type)
                st.rerun()
            else:
                if st.button("Run"):
                    self._bt.save()
                    self._bt.log = Path(f'{self._bt.file}.log')
                    self._bt.run()
                    st.rerun()
        if not self._btresults:
            self._btresults = BacktestResults(f'{st.session_state.pbdir}/backtests/pbgui')
            self._btresults.match_config(self.symbol, self._config.config, self.market_type)
        if self.symbol in self._btresults.symbols:
            self._btresults.symbols_selected = self.symbol
        self._btresults.side_selected = self._btresults.SIDES
        self._btresults.mode_selected = self._btresults.MODES
        self._btresults.view(trades = self._trades)
        if self._bt.is_running():
            st_autorefresh(interval=10000, limit=None, key="refresh_backtest_running")

    def edit_config(self):
        self._config.edit_config()

    def edit_mode(self):
        if not self.long_mode:
            self.long_mode = "normal"
        if not self.short_mode:
            self.short_mode = "normal"
        modes = ['normal', 'graceful_stop', 'panic', 'tp_only']
        col_lm, col_sm, col_empty = st.columns([1,1,1])
        with col_lm:
            if "edit_long_mode" in st.session_state:
                self.long_mode = st.session_state.edit_long_mode
            st.radio("LONG_MODE",modes, key="edit_long_mode", index=modes.index(self.long_mode), help=pbgui_help.mode)
        with col_sm:
            if "edit_short_mode" in st.session_state:
                self.short_mode = st.session_state.edit_short_mode
            st.radio("SHORT_MODE",modes, key="edit_short_mode", index=modes.index(self.short_mode), help=pbgui_help.mode)

    def refresh(self):
        path = self._instance_path
        self.__init__()
        if path:
            self.load(path)

    def load(self, path: Path):
        file = Path(f'{path}/instance.cfg')
        if file.exists():
            try:
                with open(file, "r", encoding='utf-8') as f:
                    state = json.load(f)
                self.__dict__.update(state)
                self._instance_path = path
                self.user = state["_user"]
                if not self._symbol_ccxt or self._symbol_ccxt.endswith("_UMCBL") or self._symbol_ccxt.endswith("_DMCBL"):
                    self._symbol_ccxt = self.exchange.symbol_to_exchange_symbol(self.symbol, self._market_type)
                    state["_symbol_ccxt"] = self._symbol_ccxt
                    with open(file, "w", encoding='utf-8') as f:
                        json.dump(state, f, indent=4)
                self._config = Config(f'{self._instance_path}/config.json')
                self._config.load_config()
                return True
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()
        print(f'Error load Instance: {str(file)}')
        return False

    def load_status(self):
        file = Path(f'{self._instance_path}/status.json')
        if not file.exists():
            return False
        if self._statusll > datetime.now().timestamp() - 60:
            return True
        self._statusll = datetime.now().timestamp()
        with open(file, "r", encoding='utf-8') as f:
            try: 
                self._status = json.load(f)
                return True
            except Exception as e:
                print(f'Error load_status: {self.user} {self.symbol} {self.market_type} {e}')
                return False

    def save_status(self):
        file = Path(f'{self._instance_path}/status.json')
        status = {}
        status["timestamp"] = datetime.now().timestamp()
        status["balance"] = self.fetch_balance()
        status["price"] = self.fetch_price()
        if self.market_type == "futures":
            status["position"] = self.fetch_position()
        else:
            status["spot_balance"] = self.fetch_spot_balance()
        status["orders"] = self.fetch_open_orders()
        with open(file, "w", encoding='utf-8') as f:
            json.dump(status, f, indent=4)

    def save(self):
        if self.user and self.symbol and self.market_type:
            pbgdir = Path.cwd()
            instance_path = Path(f'{pbgdir}/data/instances/{self._user}_{self._symbol}_{self.market_type}')
            if self._instance_path and self._instance_path != str(instance_path):
                if Path(self._instance_path).exists():
                    Path(self._instance_path).rename(instance_path)
            self._instance_path = str(instance_path)
            if not instance_path.exists():
                instance_path.mkdir(parents=True)
            self._config.config_file = f'{self._instance_path}/config.json'
            self._config.save_config()
            self._version += 1
            file = Path(f'{instance_path}/instance.cfg')
            self._symbol_ccxt = self.exchange.symbol_to_exchange_symbol(self.symbol, self._market_type)
            state = self.__dict__.copy()
            # _enabled can be deleted in next version
            del state['_enabled']
            del state['_instance_path']
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
            del state['_status']
            del state['_statusll']
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

    def view_log(self):
        logfile = Path(f'{self._instance_path}/passivbot.log')
        logr = ""
        if logfile.exists():
            with open(logfile, 'r', encoding='utf-8', errors='ignore') as f:
                log = f.readlines()
                for line in reversed(log):
                    logr = logr+line
        st.button(':recycle: **passivbot logfile**')
        stx.scrollableTextbox(logr,height="300")

class Instances:
    def __init__(self, ipath: str = None):
        self.instances = []
        self.index = 0
        self._pbrun_log = False
        self._pbremote_log = False
        self._pbstat_log = False
        pbgdir = Path.cwd()
        if not ipath:
            self.instances_path = f'{pbgdir}/data/instances'
        else:
            self.instances_path = f'{pbgdir}/data/remote/instances_{ipath}'
        self.load()

    @property
    def pbrun_log(self): return self._pbrun_log
    @pbrun_log.setter
    def pbrun_log(self, new_pbrun_log):
        self._pbrun_log = new_pbrun_log

    @property
    def pbremote_log(self): return self._pbremote_log
    @pbremote_log.setter
    def pbremote_log(self, new_pbremote_log):
        self._pbremote_log = new_pbremote_log

    @property
    def pbstat_log(self): return self._pbstat_log
    @pbstat_log.setter
    def pbstat_log(self, new_pbstat_log):
        self._pbstat_log = new_pbstat_log

    def __iter__(self):
        return iter(self.instances)

    def __next__(self):
        if self.index > len(self.instances):
            raise StopIteration
        self.index += 1
        return next(self)
    
    def list(self):
        return list(map(lambda c: c.user, self.instances))
    
    def is_user_used(self, user: str):
        for instance in self.instances:
           if user == instance.user:
               return True
        return False

    def is_same(self, instance: Instance):
        if instance:
            local_instance = self.find_instance(instance.user, instance.symbol, instance.market_type)
            if local_instance:
                if (
                    instance.config == local_instance.config
                    and instance._market_type == local_instance._market_type
                    and instance._ohlcv == local_instance._ohlcv
                    and instance._assigned_balance == local_instance._assigned_balance
                    and instance._co == local_instance._co
                    and instance._leverage == local_instance._leverage
                    and instance._price_distance_threshold == local_instance._price_distance_threshold
                    and instance._price_precision == local_instance._price_precision
                    and instance._price_step == local_instance._price_step
                    and instance._long_mode == local_instance._long_mode
                    and instance._short_mode == local_instance._short_mode
                ):
                    return True
                else:
                    return False
            else:
                return False
        return None

    def find_instance(self, user: str, symbol: str, market_type: str):
        for instance in self.instances:
            if (
                instance.user == user
                and instance.symbol == symbol
                and instance.market_type == market_type
            ):
                return instance

    def add_wait(self, instance_name : str):
        p = Path(f'{self.instances_path}/{instance_name}')
        file = Path(f'{p}/instance.cfg')
        for i in range(15):
            if file.exists():
                inst = Instance()
                if inst.load(p):
                    print(f'{str(p)} loaded')
                    self.instances.append(inst)
                    self.instances = sorted(self.instances, key=lambda d: d.user)
                    return
            sleep(1)

    def remove(self, instance: Instance):
        instance.remove()
        self.instances.remove(instance)

    def refresh(self):
        for instance in self.instances:
            instance.load(instance._instance_path)
    
    def reload_instances(self):
        self.instances = []
        self.load()

    def load(self):
        p = str(Path(f'{self.instances_path}/*'))
        instances = glob.glob(p)
        for instance in instances:
            inst = Instance()
            if inst.load(instance):
                self.instances.append(inst)
        # sort instance by user and symbol
        self.instances = sorted(self.instances, key=lambda d: d.symbol) 
        self.instances = sorted(self.instances, key=lambda d: d.user) 

    def view_log(self, log_filename: str):
        pbgdir = Path.cwd()
        logfile = Path(f'{pbgdir}/data/logs/{log_filename}.log')
        col_log, col_del, col_empty = st.columns([5,1,18])
        with col_log:
            st.button(f':recycle: **{log_filename} logfile**', key=f'button_{log_filename}')
        with col_del:
            if st.button(f':wastebasket:', key=f'button__del_{log_filename}'):
                with open(logfile,'r+') as file:
                    file.truncate()
        logr = ""
        if logfile.exists():
            with open(logfile, 'r', encoding='utf-8') as f:
                log = f.readlines()
                for line in reversed(log):
                    logr = logr+line
        stx.scrollableTextbox(logr,height="800", key=f'stx_{log_filename}')

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
