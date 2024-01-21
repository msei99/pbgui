import ccxt
import configparser
from User import User
from enum import Enum
import json
from pathlib import Path
from time import sleep
from datetime import datetime

class Exchanges(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'
    BITGET = 'bitget'
    OKX = 'okx'
    KUCOIN = 'kucoin'
    BINGX = 'bingx'
    
    @staticmethod
    def list():
        return list(map(lambda c: c.value, Exchanges))

class Spot(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Spot))

class Passphrase(Enum):
    BITGET = 'bitget'
    OKX = 'okx'
    KUCOIN = 'kucoin'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Passphrase))

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
            if "1s" in self._tf:
                self._tf.remove('1s')
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
        if self.id == "bybit" and market_type == "spot":
            orders = self.instance.fetch_open_orders(symbol=symbol, params = {"type": market_type})
        elif self.id == 'bingx':
            orders = self.instance.fetch_open_orders(symbol=symbol)
        else:    
            orders = self.instance.fetch_open_orders(symbol=symbol)
        return orders

    def fetch_position(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        if self.id in 'binance':
            position = self.instance.fetch_account_positions(symbols=[symbol])
            return position[0]
        elif self.id == 'bingx':
            positions = self.instance.fetch_positions(symbols=[symbol])
            for position in positions:
                if position["symbol"] == symbol:
                    return position
        else:
            position = self.instance.fetch_position(symbol=symbol)
            return position

    def fetch_balance(self, market_type: str, symbol : str = None):
        if not self.instance: self.connect()
        try:
            balance = self.instance.fetch_balance(params = {"type": market_type})
        except Exception as e:
            return e   
        if self.id == "bitget":
            return float(balance["info"][0]["available"])
        elif self.id == "bybit":
            if market_type == 'swap':
                if "USDT" in balance["total"]:
                    return float(balance["total"]["USDT"])
                else:
                    return float(0)
            else:
                if symbol:
                    if symbol.endswith('USDT'):
                        symbol = symbol.replace("USDT", "")
                    elif symbol.endswith('USDC'):
                        symbol = symbol.replace("USDC", "")
                    elif symbol.endswith('BTC'):
                        symbol = symbol.replace("BTC", "")
                    elif symbol.endswith('EUR'):
                        symbol = symbol.replace("EUR", "")
                    return float(balance["total"][symbol])    
                else:
                    if "USDT" in balance["total"]:
                        return float(balance["total"]["USDT"])
                    else:
                        return float(0)
        elif self.id == "binance":
            if market_type == 'swap': return float(balance["info"]["totalWalletBalance"])
            else:
                if symbol:
                    return float(balance["total"][symbol])    
                else:
                    return float(balance["total"]["USDT"])
        elif self.id == "bingx":
            return float(balance["info"]["data"]["balance"]["balance"])
        return float(balance["total"]["USDT"])

    def fetch_timestamp(self):
        if not self.instance: self.connect()
        return self.instance.milliseconds()

    def fetch_trades(self, symbol: str, market_type: str, since: int):
        all_trades = []
        last_trade_id = ""
        if not self.instance: self.connect()
        if self.instance.has['fetchMyTrades'] or self.instance.has['fetchTrades']:
            end_time = self.instance.milliseconds()
            # With ccxt >= 4.1.7 we can use pagination in one line
            # trades = self.instance.fetch_my_trades(symbol=symbol, since=since, params = {"type": market_type, "paginate": True, "paginationDirection": "forward", "until": self.instance.milliseconds()})
            if self.id == "binance":
                if market_type == "futures":
                    week = 7 * 24 * 60 * 60 * 1000
                else:
                    week = 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
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
                day = 24 * 60 * 60 * 1000
                week = 7 * day
                year = 365 * day
                now = self.instance.milliseconds()
                all_trades = []
                if since == 1577840461000:
                    since = now - 2 * year + day
                    end_time = since + week
                    first_trade = self.instance.fetch_my_trades(symbol, since, 100, params = {'type': market_type, "paginate": True, 'endTime': end_time })
                    if first_trade:
                        since = first_trade[0]["timestamp"]
                while since < now:
                    print(f'User:{self.user.name} Symbol:{symbol} Fetching trades from', self.instance.iso8601(since))
                    end_time = since + week
                    if end_time > now:
                        end_time = now
                    trades = self.instance.fetch_my_trades(symbol, since, 100, params = {'type': market_type, 'endTime': end_time })
                    if len(trades):
                        last_trade = trades[len(trades) - 1]
                        if "nextPageCursor" in last_trade["info"]:
                            cursor = last_trade["info"]["nextPageCursor"]
                            while True:
                                print(f'User:{self.user.name} Symbol:{symbol} Fetching trades from', cursor)
                                all_trades = all_trades + trades
                                trades = self.instance.fetch_my_trades(symbol, since, 100, params = {'type': market_type, 'cursor': cursor, 'endTime': end_time })
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
            elif self.id == "kucoinfutures":
                week = 7 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                limit = 50
                end = since + week
                while True:
                    trades = self.instance.fetch_my_trades(symbol=symbol, since=since, limit=limit, params = {"endAt": end})
                    if trades:
                        first_trade = trades[0]
                        last_trade = trades[-1]
                        all_trades = trades + all_trades
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', first_trade['timestamp'], 'till', last_trade['timestamp'])
                    if len(trades) == limit:
                        end = trades[0]['timestamp']
                    else:
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                        since += week
                        end = since + week
                    if since > now:
                        print(f'User:{self.user.name} Symbol:{symbol} Done')
                        break
            elif self.id == "okx":
                week = 7 * 24 * 60 * 60 * 1000
                max = 90 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                if since == 1577840461000:
                    since = now - max
                limit = 50
                end = since + week
                while True:
                    trades = self.instance.fetch_my_trades(symbol=symbol, since=since, limit=limit, params = {"end": end})
                    if trades:
                        first_trade = trades[0]
                        last_trade = trades[-1]
                        all_trades = trades + all_trades
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', first_trade['timestamp'], 'till', last_trade['timestamp'])
                    if len(trades) == limit:
                        end = trades[0]['timestamp']
                    else:
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                        since += week
                        end = since + week
                    if since > now:
                        print(f'User:{self.user.name} Symbol:{symbol} Done')
                        break
            elif self.id == "bingx":
                week = 7 * 24 * 60 * 60 * 1000
                max = 90 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                if since == 1577840461000:
                    since = now - max
                limit = 500
                end = since + week
                bingx_symbol = f'{symbol.split("/")[0]}-{symbol.split(":")[-1]}'
                while True:
                    now = self.instance.milliseconds()
                    orders = self.instance.swapV2PrivateGetTradeAllOrders({"symbol": bingx_symbol, "limit": limit, "startTime": since, "endTime": end, "timestamp": now})
                    trades = orders["data"]["orders"]
                    if trades:
                        first_trade = trades[0]
                        last_trade = trades[-1]
                        all_trades = trades + all_trades
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', first_trade['time'], 'till', last_trade['time'])
                    if len(trades) == limit:
                        since = int(trades[-1]['time'])
                    else:
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                        since += week
                        end = since + week
                    if since > now:
                        print(f'User:{self.user.name} Symbol:{symbol} Done')
                        break
                bingx_trades = []
                for trade in all_trades:
                    if trade["status"] == "FILLED":
                        trade["id"] = trade["orderId"]
                        trade["timestamp"] = int(trade["time"])
                        trade["amount"] = float(trade["executedQty"])
                        trade["fee"] = float(trade["commission"])
                        trade["price"] = float(trade["price"])
                        bingx_trades.append(trade)
                all_trades = bingx_trades
            # elif self.id == "bingx":
            #     since = 1700000461000
            #     week = 7 * 24 * 60 * 60 * 1000
            #     now = self.instance.milliseconds()
            #     limit = 50
            #     end = since + week
            #     self.instance.load_markets()
            #     self.instance.verbose = True
            #     while True:
            #         trades = self.instance.fetch_my_trades(symbol=symbol, since=since, limit=limit, params = {"endTs": end})
            #         if trades:
            #             first_trade = trades[0]
            #             last_trade = trades[-1]
            #             all_trades = trades + all_trades
            #             print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', first_trade['timestamp'], 'till', last_trade['timestamp'])
            #         if len(trades) == limit:
            #             end = trades[0]['timestamp']
            #         else:
            #             print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
            #             since += week
            #             end = since + week
            #         if since > now:
            #             print(f'User:{self.user.name} Symbol:{symbol} Done')
            #             break
            #     print(all_trades)
            #     bingx_trades = []
            #     for trade in all_trades:
            #         bingx_symbol = f'{symbol.split("/")[0]}-{symbol.split(":")[-1]}'
            #         if trade["info"]["symbol"] == bingx_symbol:
            #             order = self.instance.fetch_order(symbol=symbol, id=trade["order"])
            #             print(order)
            #             trade["id"] = trade["order"]
            #             bingx_trades.append(trade)
            #     all_trades = bingx_trades
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

    def fetch_fundings(self, symbol: str, market_type: str, since: int):
        all_fundings = []
        if not self.instance: self.connect()
        if self.id == "kucoinfutures":
            week = 7 * 24 * 60 * 60 * 1000
            now = self.instance.milliseconds()
            limit = 50
            end = since + week
            while True:
                fundings = self.instance.fetch_funding_history(symbol=symbol, since=since, limit=limit, params = {"endAt": end})
                if fundings:
                    first_funding = fundings[0]
                    last_funding = fundings[-1]
                    all_fundings = fundings + all_fundings
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'funding from', first_funding['timestamp'], 'till', last_funding['timestamp'])
                if len(fundings) == limit:
                    end = fundings[0]['timestamp']
                else:
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'funding from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since += week
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Symbol:{symbol} Done')
                    break
        elif self.id == "binance":
            week = 7 * 24 * 60 * 60 * 1000
            now = self.instance.milliseconds()
            while since < now:
                print(f'User:{self.user.name} Symbol:{symbol} Fetching fundings from', self.instance.iso8601(since))
                end_time = since + week
                if end_time > now:
                    end_time = now
                fundings = self.instance.fetch_funding_history(symbol, since, None, {
                    'endTime': end_time,
                })
                if len(fundings):
                    last_funding = fundings[-1]
                    since = last_funding['timestamp'] + 1
                    all_fundings = all_fundings + fundings
                else:
                    since = end_time
        elif self.id == "okx":
            week = 7 * 24 * 60 * 60 * 1000
            max = 90 * 24 * 60 * 60 * 1000
            now = self.instance.milliseconds()
            if since == 1577840461000:
                since = now - max
            limit = 50
            end = since + week
            while True:
                fundings = self.instance.fetch_funding_history(symbol=symbol, since=since, limit=limit, params = {"end": end})
                if fundings:
                    first_funding = fundings[0]
                    last_funding = fundings[-1]
                    all_fundings = fundings + all_fundings
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'fundings from', first_funding['timestamp'], 'till', last_funding['timestamp'])
                if len(fundings) == limit:
                    end = fundings[0]['timestamp']
                else:
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'fundings from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since += week
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Symbol:{symbol} Done')
                    break
                sleep(1)
        elif self.id == "bingx":
            week = 7 * 24 * 60 * 60 * 1000
            max = 90 * 24 * 60 * 60 * 1000
            now = self.instance.milliseconds()
            if since == 1577840461000:
                since = now - max
            limit = 500
            end = since + week
            bingx_symbol = f'{symbol.split("/")[0]}-{symbol.split(":")[-1]}'
            while True:
                now = self.instance.milliseconds()
                income = self.instance.swapV2PrivateGetUserIncome({"symbol": bingx_symbol, "limit": limit, "incomeType": "FUNDING_FEE", "startTime": since, "endTime": end, "timestamp": now})
                fundings = income["data"]
                if fundings:
                    first_funding = fundings[0]
                    last_funding = fundings[-1]
                    all_fundings = fundings + all_fundings
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'fundings from', first_funding['time'], 'till', last_funding['time'])
                else:
                    fundings = []
                if len(fundings) == limit:
                    since = int(fundings[-1]['time'])
                else:
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'fundings from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since += week
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Symbol:{symbol} Done')
                    break
            bingx_fundings = []
            for funding in all_fundings:
                funding["id"] = funding["tradeId"]
                funding["timestamp"] = int(funding["time"])
                funding["amount"] = float(funding["income"])
                bingx_fundings.append(funding)
            all_fundings = bingx_fundings
        elif self.id == "bitget":
            end_time = self.instance.milliseconds()
            last_funding_id = ""
            while True:
                fundings = self.instance.fetch_funding_history(symbol=symbol, since=since, limit=100, params = {"endTime": end_time})
                if fundings and fundings[-1]['id'] != last_funding_id:
                    first_funding = fundings[0]
                    last_funding = fundings[-1]
                    last_funding_id = fundings[-1]['id']
                    end_time = first_funding['timestamp']
                    all_fundings = fundings + all_fundings
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'fundings from', first_funding['datetime'], 'till', last_funding['datetime'])
                else:
                    print(f'User:{self.user.name} Symbol:{symbol} Done')
                    break
        if all_fundings:
            return all_fundings

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
        elif self.id == 'bingx':
            return f'{symbol[0:-4]}/USDT:USDT'
        else:
            return symbol


    def fetch_symbols(self):
        if not self.instance: self.connect()
        self._markets = self.instance.load_markets()
        self.swap = []
        self.spot = []
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
                        self.swap.append(v["id"])
                elif self.id == "bingx":
                    if v["id"].endswith('USDT'):
                        self.swap.append(''.join(v["id"].split("-")))
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
