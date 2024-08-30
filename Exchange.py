import ccxt
import configparser
from User import User, Users
from enum import Enum
import json
from pathlib import Path
from time import sleep
from datetime import datetime
from pbgui_func import PBGDIR

class Exchanges(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'
    BITGET = 'bitget'
    HYPERLIQUID = 'hyperliquid'
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

class Single(Enum):
    BINANCE = 'binance'
    BYBIT = 'bybit'
    OKX = 'okx'
    KUCOIN = 'kucoin'
    BINGX = 'bingx'

    @staticmethod
    def list():
        return list(map(lambda c: c.value, Single))

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
            self.instance.walletAddress = self.user.wallet_address
            self.instance.privateKey = self.user.private_key
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
        # if symbol == "ADAUSDT_UMCBL":
        #     symbol = "ADA/USDT:USDT"
        price = self.instance.fetch_ticker(symbol=symbol)
        return price

    def fetch_prices(self, symbols: list, market_type: str):
        if not self.instance: self.connect()
        prices = self.instance.fetch_tickers(symbols=symbols)
        return prices

    def fetch_open_orders(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        if self.id == "bybit" and market_type == "spot":
            orders = self.instance.fetch_open_orders(symbol=symbol, params = {"type": market_type})
        elif self.id == 'bingx':
            orders = self.instance.fetch_open_orders(symbol=symbol)
        else:
            orders = self.instance.fetch_open_orders(symbol=symbol)
        return orders

    def fetch_all_open_orders(self, symbol: str):
        if not self.instance: self.connect()
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

    def fetch_positions(self):
        if not self.instance: self.connect()
        positions = self.instance.fetch_positions()
        return positions

    def fetch_balance(self, market_type: str, symbol : str = None):
        if not self.instance: self.connect()
        try:
            balance = self.instance.fetch_balance(params = {"type": market_type})
        except Exception as e:
            return e
        if self.id == "hyperliquid":
            return float(balance["total"]["USDC"])
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

    def fetch_spot(self, since: int = None):
        if self.user.key == 'key':
            return []
        all_histories = []
        all = []
        if not self.instance: self.connect()
        if self.id == "bybit":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 2 * 365 * day - day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 100
            end = since + week
            while True:
                trades = self.instance.fetch_my_trades(since=since, limit=limit, params = {'type': 'spot', "endTime": end})
                print(trades)
                if trades:
                    first_trade = trades[0]
                    last_trade = trades[-1]
                    all_histories = trades + all_histories
                if len(trades) == limit:
                    print(f'User:{self.user.name} Fetched', len(trades), 'trades from', self.instance.iso8601(first_trade['timestamp']), 'till', self.instance.iso8601(last_trade['timestamp']))
                    end = trades[0]['timestamp']
                else:
                    print(f'User:{self.user.name} Fetched', len(trades), 'trades from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since = since + week
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Done')
                    break
            for history in all_histories:
                income = {}
                income["symbol"] = history["info"]["symbol"]
                income["timestamp"] = history["timestamp"]
                income["side"] = history["side"]
                income["income"] = history["cost"]
                income["fee"] = history["info"]["execFee"]
                income["uniqueid"] = history["info"]["orderId"]
                all.append(income)
        return all

    def save_income_other(self, history : list, exchange: str):
        dest = Path(f'{PBGDIR}/data/logs')
        if not dest.exists():
            dest.mkdir(parents=True)
        file = Path(f"{PBGDIR}/data/logs/income_other_{exchange}.json")
        with open(file, 'a') as f:
            json.dump(history, f, indent=4)

    def fetch_history(self, since: int = None):
        if self.user.key == 'key':
            return []
        all_histories = []
        all = []
        if not self.instance: self.connect()
        if self.id == "bybit":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 2 * 365 * day - day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 50
            end = since + week
            if self.instance.is_unified_enabled()[1]:
                UTA = True
            else:
                UTA = False
            cursor = None
            while True:
                if UTA:
                    transactions = self.instance.privateGetV5AccountTransactionLog(params = {"limit": limit, "startTime": since, "endTime": end, "cursor": cursor})
                else:
                    transactions = self.instance.privateGetV5AccountContractTransactionLog(params = {"limit": limit, "startTime": since, "endTime": end, "cursor": cursor})
                cursor = transactions["result"]["nextPageCursor"]
                positions = transactions["result"]["list"]
                if positions:
                    first_position = positions[0]
                    last_position = positions[-1]
                    all_histories = positions + all_histories
                if cursor:
                    print(f'User:{self.user.name} Fetched', len(positions), 'transactions from', self.instance.iso8601(int(first_position['transactionTime'])), 'till', self.instance.iso8601(int(last_position['transactionTime'])))
                else:
                    print(f'User:{self.user.name} Fetched', len(positions), 'transactions from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since = since + week
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Done')
                    break
            for history in all_histories:
                if history["type"] in ["TRADE","SETTLEMENT"]:
                    income = {}
                    income["symbol"] = history["symbol"]
                    income["timestamp"] = history["transactionTime"]
                    income["income"] = history["change"]
                    income["uniqueid"] = history["tradeId"]
                    all.append(income)
                else: 
                    self.save_income_other(history, self.user.name)
        elif self.id == "kucoinfutures":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 1 * 365 * day - day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 50
            end = since + day
            while True:
                positions = self.instance.futuresPrivateGetTransactionHistory(params = {"maxCount": limit, "startAt": since, "endAt": end})
                positions = positions["data"]["dataList"]
                if positions:
                    first_position = positions[0]
                    last_position = positions[-1]
                    all_histories = positions + all_histories
                if len(positions) == limit:
                    print(f'User:{self.user.name} Fetched', len(positions), 'income from', self.instance.iso8601(first_position['time']), 'till', self.instance.iso8601(last_position['time']))
                    end = positions[-1]['time']
                else:
                    print(f'User:{self.user.name} Fetched', len(positions), 'income from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since = since + day
                    end = since + day
                if since > now:
                    print(f'User:{self.user.name} Done')
                    break
            for history in all_histories:
                if history["type"] == "RealisedPNL":
                    income = {}
                    income["symbol"] = history["remark"][0:-2]
                    income["timestamp"] = history["time"]
                    income["income"] = history["amount"]
                    income["uniqueid"] = history["offset"]
                    all.append(income)
                else: 
                    self.save_income_other(history, self.user.name)
        elif self.id == "okx":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 120 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 100
            end = since + week
            while True:
                ledgers = self.instance.fetch_ledger(since=since, limit=limit, params = {"method": "privateGetAccountBillsArchive", "instType": "SWAP", "end": end})
                if ledgers:
                    first_ledger = ledgers[0]
                    last_ledger = ledgers[-1]
                    all_histories = ledgers + all_histories
                if len(ledgers) == limit:
                    print(f'User:{self.user.name} Fetched', len(ledgers), 'ledgers from', self.instance.iso8601(first_ledger['timestamp']), 'till', self.instance.iso8601(last_ledger['timestamp']))
                    end = ledgers[0]['timestamp']
                else:
                    print(f'User:{self.user.name} Fetched', len(ledgers), 'ledgers from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since = since + week
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Done')
                    break
                sleep(0.5)
            for history in all_histories:
                if history["type"] in ["trade","fee"]:
                    income = {}
                    income["symbol"] = history["symbol"][0:-5].replace("/", "").replace("-", "")
                    income["timestamp"] = history["timestamp"]
                    income["income"] = history["amount"]
                    income["uniqueid"] = history["id"]
                    all.append(income)
                else: 
                    self.save_income_other(history, self.user.name)
        elif self.id == "bitget":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 120 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 100
            end = since + week
            while True:
                ledgers = self.instance.fetch_ledger(since=since, limit=limit, params = {"type": "swap", "endTime": end})
                if ledgers:
                    first_ledger = ledgers[0]
                    last_ledger = ledgers[-1]
                    all_histories = ledgers + all_histories
                if len(ledgers) == limit:
                    print(f'User:{self.user.name} Fetched', len(ledgers), 'ledgers from', self.instance.iso8601(first_ledger['timestamp']), 'till', self.instance.iso8601(last_ledger['timestamp']))
                    end = ledgers[0]['timestamp']
                else:
                    print(f'User:{self.user.name} Fetched', len(ledgers), 'ledgers from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since = since + week
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Done')
                    break
            for history in all_histories:
                if history["info"]["symbol"] and history["info"]["amount"] != "0":
                    if history["type"] in ["trade","fee"]:
                        income = {}
                        income["symbol"] = history["info"]["symbol"]
                        income["timestamp"] = history["timestamp"]
                        income["income"] = history["info"]["amount"]
                        income["uniqueid"] = history["info"]["billId"]
                        all.append(income)
                    else: 
                        self.save_income_other(history, self.user.name)
        elif self.id == "binance":
            day = 24 * 60 * 60 * 1000
            week = 7 * day
            max = 124 * day
            now = self.instance.milliseconds()
            if not since:
                since = now - max
            limit = 1000
            end = since + week
            while True:
                imcomes = self.instance.fapiPrivateGetIncome({                        
                                                        "pageSize": "100",
                                                        "startTime": since,
                                                        "limit": limit,
                                                        "endTime": end,
                                                        "timestamp": self.instance.milliseconds()
                                                        })
                if imcomes:
                    first_imcome = imcomes[0]
                    last_imcome = imcomes[-1]
                    all_histories = imcomes + all_histories
                if len(imcomes) == limit:
                    print(f'User:{self.user.name} Fetched', len(imcomes), 'incomes from', self.instance.iso8601(int(first_imcome['time'])), 'till', self.instance.iso8601(int(last_imcome['time'])))
                    since = int(imcomes[-1]['time'])
                else:
                    print(f'User:{self.user.name} Fetched', len(imcomes), 'incomes from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since = end
                    end = since + week
                if since > now:
                    print(f'User:{self.user.name} Done')
                    break
            for history in all_histories:
                if history["incomeType"] == "REALIZED_PNL":
                    income = {}
                    income["symbol"] = history["symbol"]
                    income["timestamp"] = history["time"]
                    income["income"] = history["income"]
                    income["uniqueid"] = history["tradeId"]
                    all.append(income)
                else: 
                    self.save_income_other(history, self.user.name)
        return all
    
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
            elif self.id == "bitget":
                # week = 7 * 24 * 60 * 60 * 1000
                max = 90 * 24 * 60 * 60 * 1000
                now = self.instance.milliseconds()
                end = since + max
                limit = 100
                while True:
                    trades = self.instance.fetch_my_trades(symbol=symbol, since=since, limit=limit, params = {"type": market_type, "endTime": end})
                    if trades:
                        first_trade = trades[0]
                        last_trade = trades[-1]
                        all_trades = trades + all_trades
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', first_trade['timestamp'], 'till', last_trade['timestamp'])
                    if len(trades) == limit:
                        end = trades[0]['timestamp']
                    else:
                        print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(trades), 'trades from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                        since += max
                        end = since + max
                    if since > now:
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
            max = 90 * 24 * 60 * 60 * 1000
            now = self.instance.milliseconds()
            end = since + max
            limit = 100
            while True:
                fundings = self.instance.fetch_funding_history(symbol=symbol, since=since, limit=limit, params = {"endTime": end})
                if fundings:
                    first_funding = fundings[0]
                    last_funding = fundings[-1]
                    all_fundings = fundings + all_fundings
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'fundings from', first_funding['timestamp'], 'till', last_funding['timestamp'])
                if len(fundings) == limit:
                    end = fundings[0]['timestamp']
                else:
                    print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'fundings from', self.instance.iso8601(since), 'till', self.instance.iso8601(end))
                    since += max
                    end = since + max
                if since > now:
                    print(f'User:{self.user.name} Symbol:{symbol} Done')
                    break
            # end_time = self.instance.milliseconds()
            # last_funding_id = ""
            # while True:
            #     if since == end_time:
            #         print(f'User:{self.user.name} Symbol:{symbol} {since} Done')
            #         break
            #     fundings = self.instance.fetch_funding_history(symbol=symbol, since=since, limit=100, params = {"endTime": end_time})
            #     if fundings and fundings[-1]['id'] != last_funding_id:
            #         first_funding = fundings[0]
            #         last_funding = fundings[-1]
            #         last_funding_id = fundings[-1]['id']
            #         end_time = first_funding['timestamp']
            #         all_fundings = fundings + all_fundings
            #         print(f'User:{self.user.name} Symbol:{symbol} Fetched', len(fundings), 'fundings from', first_funding['datetime'], 'till', last_funding['datetime'])
            #     else:
            #         print(f'User:{self.user.name} Symbol:{symbol} Done')
            #         break
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
        elif self.id == 'hyperliquid':
            return f'{symbol[0:-4]}/USDC:USDC'
        elif self.id == 'bitget':
            return f'{symbol[0:-4]}/USDT:USDT'
            # if symbol.endswith('USD'):
            #     return f'{symbol}_DMCBL'
            # return f'{symbol}_UMCBL'
        elif self.id == 'kucoinfutures':
            return f'{symbol}M'
        elif self.id == 'okx':
            return f'{symbol[0:-4]}-USDT-SWAP'
        elif self.id == 'bingx':
            return f'{symbol[0:-4]}/USDT:USDT'
        else:
            if market_type == "spot":
                return f'{symbol[0:-4]}/USDT'
            else:
                return symbol

    def load_market(self):
        if not self.instance: self.connect()
        self._markets = self.instance.load_markets()
        return self._markets

    def fetch_symbol_info(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        if not self._markets: self._markets = self.instance.load_markets()
        if market_type == "spot":
            symbol = f'{symbol[0:-4]}/USDT'
        else:
            if symbol[-4:] == 'USDC':
                symbol = f'{symbol[0:-4]}/USDC:USDC'
            else:
                symbol = f'{symbol[0:-4]}/USDT:USDT'
        symbol_info = self._markets[symbol]
        if self.id == 'binance':
            if market_type == "futures":
                min_costs = (
                    0.1 if symbol_info["limits"]["cost"]["min"] is None else symbol_info["limits"]["cost"]["min"]
                )
                min_qtys = symbol_info["limits"]["amount"]["min"]
                for felm in symbol_info["info"]["filters"]:
                    if felm["filterType"] == "PRICE_FILTER":
                        price_steps = float(felm["tickSize"])
                    elif felm["filterType"] == "MARKET_LOT_SIZE":
                        qty_steps = float(felm["stepSize"])
                c_mults = symbol_info["contractSize"]
            else:
                for q in symbol_info["info"]["filters"]:
                    if q["filterType"] == "LOT_SIZE":
                        min_qtys = symbol_info["min_qty"] = float(q["minQty"])
                        qty_steps = symbol_info["qty_step"] = float(q["stepSize"])
                    elif q["filterType"] == "PRICE_FILTER":
                        price_steps = symbol_info["price_step"] = float(q["tickSize"])
                    elif q["filterType"] == "NOTIONAL":
                        min_costs = symbol_info["min_cost"] = float(q["minNotional"])
                c_mults = 1.0
        elif self.id == 'bybit':
            if market_type == "futures":
                min_costs = (
                    0.1 if symbol_info["limits"]["cost"]["min"] is None else symbol_info["limits"]["cost"]["min"]
                )
                min_qtys = symbol_info["limits"]["amount"]["min"]
                qty_steps = symbol_info["precision"]["amount"]
                price_steps = symbol_info["precision"]["price"]
                c_mults = symbol_info["contractSize"]
            else:
                min_costs = (
                    0.1 if symbol_info["limits"]["cost"]["min"] is None else symbol_info["limits"]["cost"]["min"]
                )
                min_qtys = symbol_info["limits"]["amount"]["min"]
                qty_steps = symbol_info["precision"]["amount"]
                price_steps = symbol_info["precision"]["price"]
                c_mults = 1.0
        else:
            min_costs = max(
                5.1, 0.1 if symbol_info["limits"]["cost"]["min"] is None else symbol_info["limits"]["cost"]["min"]
            )
            min_qtys = symbol_info["limits"]["amount"]["min"]
            qty_steps = symbol_info["precision"]["amount"]
            price_steps = symbol_info["precision"]["price"]
            c_mults = symbol_info["contractSize"]
        return symbol_info, min_costs, min_qtys, price_steps, qty_steps, c_mults

    def fetch_symbols(self):
        if not self.instance: self.connect()
        self._markets = self.instance.load_markets()
        self.swap = []
        self.spot = []
        for (k,v) in list(self._markets.items()):
            if v["swap"] and v["active"] and v["linear"]:
                if self.id == "hyperliquid":
                    if v["symbol"].endswith('USDC'):
                        self.swap.append(v["symbol"][0:-5].replace("/", "").replace("-", ""))
                if self.id == "bitget":
                    # print(k,v)
                    # if v["id"][-6:] == '_UMCBL' or v["id"][-6:] == '_DMCBL':
                    #     self.swap.append(v["id"].split("_")[0])
                    if v["id"][-4:] == 'USDT':
                        self.swap.append(v["id"])
                elif self.id == "kucoinfutures":
                    if v["id"][-5:] == 'USDTM':
                        self.swap.append(v["id"][:len(v["id"])-1])
                elif self.id == "okx":
                    if v["id"].split("-")[1] == 'USDT':
                        self.swap.append(''.join(v["id"].split("-")[0:2]))
                elif self.id == "bybit":
                    if v["id"].endswith('USDT'):
                        # print(v)
                        self.swap.append(v["id"])
                elif self.id == "binance":
                    if v["id"].endswith('USDT'):
                        # print(v)
                        self.swap.append(v["id"])
                elif self.id == "bingx":
                    if v["id"].endswith('USDT'):
                        self.swap.append(''.join(v["id"].split("-")))
                # else:
                #     self.swap.append(v["id"])
            if v["spot"] and v["active"] and (self.id == "bybit" or self.id == "binance"):
                self.spot.append(v["id"])
        self.spot.sort()
        self.swap.sort()
        # print(self.spot)
        # print(self.swap)
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
    # users = Users()
    # exchange = Exchange("hyperliquid", users.find_user("hl_manicpt"))
    # print(exchange.symbol_to_exchange_symbol("BTCUSDC", "swap"))
    # print(exchange.fetch_balance("swap"))
    # print(exchange.fetch_symbol_info("DOGEUSDC", "swap"))
    # print(exchange.fetch_price("DOGE/USDC:USDC", "swap"))
    # exchange.fetch_symbols()
    # spot = exchange.fetch_spot()
    # print(exchange.fetch_history())
    # print(exchange.fetch_history())
    # print(exchange.fetch_prices())
    # print(exchange.fetch_balance("swap"))
    # print(spot)

if __name__ == '__main__':
    main()
