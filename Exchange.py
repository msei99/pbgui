import ccxt
import configparser
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
    def __init__(self, id: str):
        self.name = id
        self.id = "kucoinfutures" if id == "kucoin" else id
        self.instance = None
        self._markets = None
        self.spot = []
        self.swap = []
    
    def connect(self):
        self.instance = getattr(ccxt, self.id) ()

    def fetch_symbols(self):
        if not self.instance: self.connect()
        self._markets = self.instance.load_markets()
        for (k,v) in list(self._markets.items()):
            if v["swap"] and v["active"]:
                if self.id == "bitget":
                    self.swap.append(v["id"].split("_")[0])
                elif self.id == "kucoinfutures":
                    self.swap.append(v["id"][:len(v["id"])-1])
                elif self.id == "okx":
                    self.swap.append(''.join(v["id"].split("-")[0:2]))
                else:    
                    self.swap.append(v["id"])
            if v["spot"] and v["active"] and (self.id == "bybit" or self.id == "binance"):
                self.spot.append(v["id"])

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
            self.save_symbols()
        self.spot.sort()
        self.swap.sort()



def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
