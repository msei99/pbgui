import ccxt
import configparser
from User import User
from enum import Enum
from time import sleep
import json

class Exchanges(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'
    BITGET = 'bitget'
    OKX = 'okx'
    KUCOIN = 'kucoin'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Exchanges))


class Exchange:
    def __init__(self, id: str, user: User = None):
        self.name = id
        self.id = "kucoinfutures" if id == "kucoin" else id
        self.instance = None
        self._markets = None
        self._tf = None
        self.spot = []
        self.swap = []
        self._user = user
        self.error = None

    @property
    def user(self): return self._user

    @property
    def tf(self):
        if not self._tf:
            self.connect()
            self._tf = list(self.instance.timeframes.keys())
        return self._tf

    @user.setter
    def user(self, new_user):
        if self._user != new_user:
            self._user = new_user

    def connect(self):
        self.instance = getattr(ccxt, self.id) ()
        if self._user and self.user.key != 'key':
            self.instance.apiKey = self.user.key
            self.instance.secret = self.user.secret
            self.instance.password = self.user.passphrase
        try:
            self.instance.checkRequiredCredentials()
        except Exception as e:
            self.error = (str(e))
            return

    def fetch_ohlcv(self, symbol: str, market_type: str, timeframe: str, limit: int):
        if not self.instance: self.connect()
        ohlcv = self.instance.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)
        return ohlcv

    def fetch_price(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        price = self.instance.fetch_ticker(symbol=symbol)
        return price

    def fetch_open_orders(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        orders = self.instance.fetch_open_orders(symbol=symbol)
        return orders

    def fetch_position(self, symbol: str, market_type: str):
        if self.id == 'binance':
            position = self.instance.fetch_account_positions(symbols=[symbol])
            return position[0]
        if not self.instance: self.connect()
        position = self.instance.fetch_position(symbol=symbol)
        return position

    def fetch_balance(self, market_type: str):
        if not self.instance: self.connect()
        balance = self.instance.fetch_balance(params = {"type": market_type})
        if self.id == "bitget":
            return float(balance["info"][0]["available"])
        elif self.id == "bybit":
            return float(balance["total"]["USDT"])
        elif self.id == "binance":
            return float(balance["info"]["totalWalletBalance"])
        return float(balance["total"]["USDT"])

    def fetch_bill(self, symbol: str, market_type: str, since: int):
        all_trades = []
        last_trade_id = ""
        if not self.instance: self.connect()
        end_time = self.instance.milliseconds()
            # With ccxt >= 4.1.7 we can use pagination in one line
            # trades = self.instance.fetch_my_trades(symbol=symbol, since=since, params = {"type": market_type, "paginate": True, "paginationDirection": "forward", "until": self.instance.milliseconds()})
        while True:
            trades = self.instance.privateMixGetAccountAccountBill({
                                                            "symbol": symbol,
                                                            "marginCoin": "USDT",
                                                            "pageSize": "100",
                                                            "startTime": since,
                                                            "endTime": end_time,
                                                            })
            trades = trades['data']['result']
            if trades and trades[-1]['id'] != last_trade_id:
                first_trade = trades[0]
                last_trade = trades[len(trades) - 1]
                last_trade_id = trades[-1]['id']
                end_time = last_trade['cTime']
                all_trades = trades + all_trades
                print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', first_trade['cTime'], 'till', last_trade['cTime'])
            else:
                print(f'User:{self.user.name} Symbol:{symbol} Done')
                break
        if all_trades:
            all_trades = sorted(all_trades, key=lambda d: d['cTime'])
            return all_trades


    def fetch_trades(self, symbol: str, market_type: str, since: int):
        all_trades = []
        last_trade_id = ""
        if not self.instance: self.connect()
        if self.instance.has['fetchMyTrades']:
            end_time = self.instance.milliseconds()
            # With ccxt >= 4.1.7 we can use pagination in one line
            # trades = self.instance.fetch_my_trades(symbol=symbol, since=since, params = {"type": market_type, "paginate": True, "paginationDirection": "forward", "until": self.instance.milliseconds()})
            if self.id == "binance":
                week = 7 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds ()
                all_trades = []
                if since == 1577840461000:
                    first_trade = self.instance.fetch_my_trades(symbol, None, None, {'fromId': 0})
                    if first_trade:
                        since = first_trade[0]["timestamp"]
                while since < now:
                    print(f'User:{self.user.name} Symbol:{symbol} Fetching trades from', self.instance.iso8601(since))
                    end_time = since + week
                    if end_time > now:
                        end_time = now
                    trades = self.instance.fetch_my_trades(symbol, since, None, {
                        'endTime': end_time,
                    })
                    if len(trades):
                        last_trade = trades[len(trades) - 1]
                        since = last_trade['timestamp'] + 1
                        all_trades = all_trades + trades
                    else:
                        since = end_time
            elif self.id == "bybit":
                week = 7 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds ()
                all_trades = []
                if since == 1577840461000:
                    end_time = since + week
                    first_trade = self.instance.fetch_my_trades(symbol, since, 100, {"paginate": True, 'endTime': end_time })
                    if first_trade:
                        since = first_trade[0]["timestamp"]
                while since < now:
                    print(f'User:{self.user.name} Symbol:{symbol} Fetching trades from', self.instance.iso8601(since))
                    end_time = since + week
                    if end_time > now:
                        end_time = now
                    trades = self.instance.fetch_my_trades(symbol, since, 100, {'endTime': end_time })
                    if len(trades):
                        last_trade = trades[len(trades) - 1]
                        if "nextPageCursor" in last_trade["info"]:
                            cursor = last_trade["info"]["nextPageCursor"]
                            while True:
                                print(f'User:{self.user.name} Symbol:{symbol} Fetching trades from', cursor)
                                all_trades = all_trades + trades
                                trades = self.instance.fetch_my_trades(symbol, since, 100, {'cursor': cursor, 'endTime': end_time })
                                if len(trades):
                                    lpage = trades[len(trades) - 1]
                                    if "nextPageCursor" in lpage["info"]:
                                        cursor = lpage["info"]["nextPageCursor"]
                                    else:
                                        break
                                else:
                                    break
                        since = last_trade['timestamp'] + 1
                        all_trades = all_trades + trades
                    else:
                        since = end_time
            else:
                while True:
                    trades = self.instance.fetch_my_trades(symbol=symbol, since=since, params = {"type": market_type, "endTime": end_time})
                    if trades and trades[-1]['id'] != last_trade_id:
                        first_trade = trades[0]
                        last_trade = trades[len(trades) - 1]
                        last_trade_id = trades[-1]['id']
                        end_time = first_trade['timestamp']
                        all_trades = trades + all_trades
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', first_trade['datetime'], 'till', last_trade['datetime'])
                    else:
                        print(f'User:{self.user.name} Symbol:{symbol} Done')
                        break
        if all_trades:
            sort_trades = sorted(all_trades, key=lambda d: d['timestamp'])
            return sort_trades

    def symbol_to_exchange_symbol(self, symbol: str, market_type: str):
        if self.id == 'binance':
            if not self.instance: self.connect()
            if not self._markets: self._markets = self.instance.load_markets()
            for (k,v) in list(self._markets.items()):
                if market_type == "spot":
                    if v["id"] == symbol and v["spot"]:
                        return v["symbol"]
                if market_type == "swap":
                    if v["id"] == symbol and v["swap"]:
                        return v["symbol"]
        elif self.id == 'bitget':
            if symbol.endswith('USD'):
                return f'{symbol}_DMCBL'    
            return f'{symbol}_UMCBL'
        elif self.id == 'kucoinfutures':
            return f'{symbol}M'
        elif self.id == 'okx':
            return f'{symbol[0:-4]}-USDT-SWAP'
        else:
            return symbol


    def fetch_symbols(self):
        if not self.instance: self.connect()
        self._markets = self.instance.load_markets()
        self.swap = []
        self.spot = []
#        print(json.dumps(self._markets))
        for (k,v) in list(self._markets.items()):
            if v["swap"] and v["active"]:
                if self.id == "bitget":
                    if v["id"][-6:] == '_UMCBL' or v["id"][-6:] == '_DMCBL':
                        self.swap.append(v["id"].split("_")[0])
                elif self.id == "kucoinfutures":
                    if v["id"][-5:] == 'USDTM':
                        self.swap.append(v["id"][:len(v["id"])-1])
                elif self.id == "okx":
                    if v["id"].split("-")[1] == 'USDT':
                        self.swap.append(''.join(v["id"].split("-")[0:2]))
                elif self.id == "bybit":
                    if v["id"].endswith('USDT'):
                        self.swap.append(''.join(v["id"].split("-")[0:2]))
                else:
                    self.swap.append(v["id"])
            if v["spot"] and v["active"] and (self.id == "bybit" or self.id == "binance"):
                self.spot.append(v["id"])
        self.spot.sort()
        self.swap.sort()
        self.save_symbols()

    def save_symbols(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("exchanges"):
            pb_config.add_section("exchanges")
        pb_config.set("exchanges", f'{self.id}.swap', f'{self.swap}')
        if self.spot:
            pb_config.set("exchanges", f'{self.id}.spot', f'{self.spot}')
        with open('pbgui.ini', 'w') as f:
            pb_config.write(f)


    def load_symbols(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if pb_config.has_option("exchanges", f'{self.id}.spot'):
            self.spot = eval(pb_config.get("exchanges", f'{self.id}.spot'))
        if pb_config.has_option("exchanges", f'{self.id}.swap'):
            self.swap = eval(pb_config.get("exchanges", f'{self.id}.swap'))
        if not self.spot and not self.swap:
            self.fetch_symbols()



def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
