import ccxt
import configparser
from User import User
from enum import Enum

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
        symbol = self.symbol_to_exchange_symbol(symbol)
        ohlcv = self.instance.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit, params = {"type": market_type})
        return ohlcv

    def fetch_price(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        symbol = self.symbol_to_exchange_symbol(symbol)
        price = self.instance.fetch_ticker(symbol=symbol, params = {"type": market_type})
        return price

    def fetch_open_orders(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        symbol = self.symbol_to_exchange_symbol(symbol)
        orders = self.instance.fetch_open_orders(symbol=symbol, params = {"type": market_type})
        return orders

    def fetch_position(self, symbol: str, market_type: str):
        if not self.instance: self.connect()
        symbol = self.symbol_to_exchange_symbol(symbol)
        position = self.instance.fetch_position(symbol=symbol, params = {"type": market_type})
        return position

    def fetch_balance(self, market_type: str):
        if not self.instance: self.connect()
        balance = self.instance.fetch_balance(params = {"type": market_type})
        if self.id == "bitget":
            return float(balance["info"][0]["available"])
        elif self.id == "bybit":
            return float(balance["total"]["USDT"])
        return float(balance["total"]["USDT"])

    def fetch_bill(self, symbol: str, market_type: str, since: int):
        all_trades = []
        last_trade_id = ""
        if not self.instance: self.connect()
        end_time = self.instance.milliseconds()
            # With ccxt >= 4.1.7 we can use pagination in one line
            # trades = self.instance.fetch_my_trades(symbol=symbol, since=since, params = {"type": market_type, "paginate": True, "paginationDirection": "forward", "until": self.instance.milliseconds()})
        symbol = self.symbol_to_exchange_symbol(symbol)
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
                print('Fetched', len(trades), 'trades from', first_trade['cTime'], 'till', last_trade['cTime'])
            else:
                print('Done')
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
            symbol = self.symbol_to_exchange_symbol(symbol)
            while True:
                trades = self.instance.fetch_my_trades(symbol=symbol, since=since, params = {"type": market_type, "endTime": end_time})
                if trades and trades[-1]['id'] != last_trade_id:
                    first_trade = trades[0]
                    last_trade = trades[len(trades) - 1]
                    last_trade_id = trades[-1]['id']
                    end_time = first_trade['timestamp']
                    all_trades = trades + all_trades
                    print('Fetched', len(trades), 'trades from', first_trade['datetime'], 'till', last_trade['datetime'])
                else:
                    print('Done')
                    break
        if all_trades:
            return all_trades

    def symbol_to_exchange_symbol(self, symbol: str):
        if self.id == 'bitget':
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
