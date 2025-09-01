import psutil
import subprocess
from time import sleep
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import configparser
from pathlib import Path, PurePath
from datetime import datetime
import platform
import sys
import os
import traceback
from io import TextIOWrapper
from Exchange import Exchange, Exchanges

SYMBOLMAP = {
    #Binance
    "RONIN": "RON",
    "1000BONK": "BONK",
    "1000FLOKI": "FLOKI",
    "1000LUNC": "LUNC",
    "1000PEPE": "PEPE",
    "1000RATS": "rats",
    "1000SHIB": "SHIB",
    "1000SATS": "SATS",
    "1000CHEEMS": "CHEEMS",
    "1000WHY": "WHY",
    "1000X": "X",
    "SHIB1000": "SHIB",
    "1000XEC": "XEC",
    "BEAMX": "BEAM",
    "DODOX": "DODO",
    "LUNA2": "LUNA",
    # "NEIROETH": "NEIRO",
    "1MBABYDOGE": "BabyDoge",
    #Bybit
    "1000000BABYDOGE": "BabyDoge",
    "10000000AIDOGE": "AIDOGE",
    "1000000CHEEMS": "CHEEMS",
    "1000000MOG": "MOG",
    "1000000PEIPEI": "PEIPEI",
    "10000COQ": "COQ",
    "10000LADYS": "LADYS",
    "10000SATS": "SATS",
    "10000WEN": "WEN",
    "10000WHY": "WHY",
    "1000APU": "APU",
    "1000BEER": "BEER",
    "1000BTT": "BTT",
    "1000CATS": "CATS",
    "1000CAT": "CAT",
    "1000MUMU": "MUMU",
    "1000NEIROCTO": "NEIRO",
    "1000TURBO": "TURBO",
    "1000TOSHI": "TOSHI",
    "DOP1": "DOP",
    "RAYDIUM": "RAY",
    "USDE": "USDe",
    #Bitget
    "OMNI1": "OMNI",
    "VELO1": "VELO",
    "1MCHEEMS": "CHEEMS",
    #OKX
    #
    #Hyperliquid
    "kBONK": "BONK",
    "kFLOKI": "FLOKI",
    "kLUNC": "LUNC",
    "kPEPE": "PEPE",
    "kSHIB": "SHIB",
    "kDOGS": "DOGS",
    "kNEIRO": "NEIRO",
    #Kucoin
    "10000CAT": "CAT",
    "1000PEPE2": "PEPE2.0",
    "NEIROCTO": "NEIRO",
    "XBT": "BTC",
   }

class CoinData:
    def __init__(self):
        pbgdir = Path.cwd()
        self.piddir = Path(f'{pbgdir}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbcoindata.pid')
        self.my_pid = None
        self._api_key = None
        self.api_error = None
        self._fetch_limit = 5000
        self._fetch_interval = 24
        self._metadata_interval = 1
        self.ini_ts = 0
        self.load_config()
        self.data = None
        self.metadata = None
        self.data_ts = 0
        self.metadata_ts = 0
        self._exchange = Exchanges.list()[0]
        self.exchanges = Exchanges.list()
        self.exchange_index = self.exchanges.index(self.exchange)
        self.update_symbols_ts = 0
        self._symbols = []
        self._symbols_cpt = []
        self._symbols_all = []
        self._symbols_notice = []
        self._symbols_notices = {}
        self._symbols_data = []
        self.approved_coins = []
        self.ignored_coins = []
        self._all_tags = []
        self._tags = []
        self.load_symbols()
        self._market_cap = 0
        self._vol_mcap = 10.0
        self._only_cpt = False
        self._notices_ignore = False
    
    @property
    def api_key(self):
        return self._api_key
    @api_key.setter
    def api_key(self, new_api_key):
        self._api_key = new_api_key
    
    @property
    def fetch_limit(self):
        return self._fetch_limit
    @fetch_limit.setter
    def fetch_limit(self, new_fetch_limit):
        self._fetch_limit = new_fetch_limit
    
    @property
    def fetch_interval(self):
        return self._fetch_interval
    @fetch_interval.setter
    def fetch_interval(self, new_fetch_interval):
        self._fetch_interval = new_fetch_interval

    @property
    def metadata_interval(self):
        return self._metadata_interval
    @metadata_interval.setter
    def metadata_interval(self, new_metadata_interval):
        self._metadata_interval = new_metadata_interval

    @property
    def exchange(self):
        return self._exchange
    @exchange.setter
    def exchange(self, new_exchange):
        self._exchange = new_exchange
        self.load_symbols()
        self.list_symbols()

    @property
    def symbols(self):
        if not self._symbols:
            self.load_symbols()
        return self._symbols

    @property
    def symbols_cpt(self):
        if not self._symbols_cpt:
            self.load_symbols()
        return self._symbols_cpt

    @property
    def symbols_all(self):
        if not self._symbols_all:
            self.load_symbols_all()
        return self._symbols_all

    @property
    def symbols_notice(self):
        if not self._symbols_notice:
            self.list_symbols()
        return self._symbols_notice

    @property
    def symbols_notices(self):
        if not self._symbols_notices:
            self.list_symbols()
        return self._symbols_notices

    @property
    def symbols_data(self):
        if not self._symbols_data:
            self.list_symbols()
        return self._symbols_data
    
    @property
    def market_cap(self):
        return self._market_cap
    @market_cap.setter
    def market_cap(self, new_market_cap):
        if self._market_cap != new_market_cap:
            self._market_cap = new_market_cap
            self.list_symbols()
    
    @property
    def vol_mcap(self):
        return self._vol_mcap
    @vol_mcap.setter
    def vol_mcap(self, new_vol_mcap):
        if self._vol_mcap != new_vol_mcap:
            self._vol_mcap = new_vol_mcap
            self.list_symbols()
    
    @property
    def only_cpt(self):
        return self._only_cpt
    @only_cpt.setter
    def only_cpt(self, new_only_cpt):
        if self._only_cpt != new_only_cpt:
            self._only_cpt = new_only_cpt
            self.list_symbols()

    @property
    def notices_ignore(self):
        return self._notices_ignore
    @notices_ignore.setter
    def notices_ignore(self, new_notices_ignore):
        if self._notices_ignore != new_notices_ignore:
            self._notices_ignore = new_notices_ignore
            self.list_symbols()

    @property
    def all_tags(self):
        if not self._all_tags:
            self.list_symbols()
        return self._all_tags

    @property
    def tags(self):
        return self._tags
    @tags.setter
    def tags(self, new_tags):
        if self._tags != new_tags:
            self._tags = new_tags
            self.list_symbols()
    

    def run(self):
        if not self.is_running():
            pbgdir = Path.cwd()
            cmd = [sys.executable, '-u', PurePath(f'{pbgdir}/PBCoinData.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=pbgdir, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBCoinData')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBCoinData')
            psutil.Process(self.my_pid).kill()

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbcoindata.py") for sub in psutil.Process(self.my_pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

    def load_pid(self):
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read()
                self.my_pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        self.my_pid = os.getpid()
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))

    def has_new_config(self):
        if Path('pbgui.ini').exists():
            ini_ts = Path('pbgui.ini').stat().st_mtime
            if self.ini_ts < ini_ts:
                self.ini_ts = ini_ts
                return True
        return False
    
    def has_new_data(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if Path(f'{coin_path}/coindata.json').exists():
            data_ts = Path(f'{coin_path}/coindata.json').stat().st_mtime
            if data_ts > self.data_ts:
                return True
            else:
                return False
        return True

    def has_new_metadata(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if Path(f'{coin_path}/metadata.json').exists():
            metadata_ts = Path(f'{coin_path}/metadata.json').stat().st_mtime
            if metadata_ts > self.metadata_ts:
                return True
            else:
                return False
        return True

    def load_config(self):
        if self.has_new_config():
            pb_config = configparser.ConfigParser()
            pb_config.read('pbgui.ini')
            if pb_config.has_option("coinmarketcap", "api_key"):
                self._api_key = pb_config.get("coinmarketcap", "api_key")
            if pb_config.has_option("coinmarketcap", "fetch_limit"):
                self._fetch_limit = int(pb_config.get("coinmarketcap", "fetch_limit"))
            if pb_config.has_option("coinmarketcap", "fetch_interval"):
                self._fetch_interval = int(pb_config.get("coinmarketcap", "fetch_interval"))
            if pb_config.has_option("coinmarketcap", "metadata_interval"):
                self._metadata_interval = int(pb_config.get("coinmarketcap", "metadata_interval"))
    
    def save_config(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        if not pb_config.has_section("coinmarketcap"):
            pb_config.add_section("coinmarketcap")
        pb_config.set("coinmarketcap", "api_key", self.api_key)
        pb_config.set("coinmarketcap", "fetch_limit", str(self.fetch_limit))
        pb_config.set("coinmarketcap", "fetch_interval", str(self.fetch_interval))
        pb_config.set("coinmarketcap", "metadata_interval", str(self.metadata_interval))
        with open('pbgui.ini', 'w') as pbgui_configfile:
            pb_config.write(pbgui_configfile)

    def fetch_api_status(self):
        url = 'https://pro-api.coinmarketcap.com/v1/key/info'
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }
        session = Session()
        session.headers.update(headers)
        try:
            response = session.get(url)
            if response.status_code == 200:
                r = json.loads(response.text)
                self.credit_limit_monthly = r["data"]["plan"]["credit_limit_monthly"]
                self.credit_limit_monthly_reset = r["data"]["plan"]["credit_limit_monthly_reset"]
                self.credit_limit_monthly_reset_timestamp = r["data"]["plan"]["credit_limit_monthly_reset_timestamp"]
                self.credits_used_day = r["data"]["usage"]["current_day"]["credits_used"]
                self.credits_used_month = r["data"]["usage"]["current_month"]["credits_used"]
                self.credits_left = r["data"]["usage"]["current_month"]["credits_left"]
                self.api_error = None
                return True
            else:
                r = json.loads(response.text)
                self.api_error = r["status"]["error_message"]
                return False
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            return

    def fetch_data(self):
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        parameters = {
            'start':'1',
            'limit':self.fetch_limit
        }
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }
        session = Session()
        session.headers.update(headers)
        try:
            response = session.get(url, params=parameters)
            if response.status_code == 200:
                self.data = json.loads(response.text)
                self.fetch_api_status()
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetched CoinMarketCap data. Credits left this month: {self.credits_left}')
            else:
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not fetch CoinMarketCap data')
                self.data = None
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            return e
    
    def fetch_metadata(self):
        # make sure we have the latest coindata
        if self.has_new_data():
            self.load_data()
            self.load_symbols()
        if not self.data:
            return
        if "data" not in self.data:
            return
        # Create symbols_ids list
        symbols_ids = []
        for symbol in self.symbols_all:
            sym = symbol[0:-4]
            if sym in SYMBOLMAP:
                sym = SYMBOLMAP[sym]
            for coin in self.data["data"]:
                if coin["symbol"] == sym:
                    symbols_ids.append(coin["id"])
        # filter out duplicate ids
        symbols_ids = list(set(symbols_ids))
        # Fetch notice from coinmarketcap
        url = 'https://pro-api.coinmarketcap.com//v2/cryptocurrency/info'
        parameters = {
            'id': ','.join(map(str, symbols_ids))
        }
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }
        session = Session()
        session.headers.update(headers)
        try:
            response = session.get(url, params=parameters)
            if response.status_code == 200:
                self.metadata = json.loads(response.text)
                self.fetch_api_status()
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Fetched CoinMarketCap metadata. Credits left this month: {self.credits_left}')
            else:
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not fetch CoinMarketCap metadata')
                self.metadata = None
        except (ConnectionError, Timeout, TooManyRedirects) as e:
            return e

    def save_metadata(self):
        if not self.metadata:
            return
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if not Path(coin_path).exists():
            Path(coin_path).mkdir(parents=True)
        with Path(f'{coin_path}/metadata.json').open('w') as f:
            json.dump(self.metadata, f)

    def save_data(self):
        if not self.data:
            return
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if not Path(coin_path).exists():
            Path(coin_path).mkdir(parents=True)
        with Path(f'{coin_path}/coindata.json').open('w') as f:
            json.dump(self.data, f)
    
    def load_data(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        data_ts = 0
        if Path(f'{coin_path}/coindata.json').exists():
            data_ts = Path(f'{coin_path}/coindata.json').stat().st_mtime
        now_ts = datetime.now().timestamp()
        if data_ts < now_ts - 3600*self.fetch_interval:
            self.fetch_data()
            self.save_data()
            loadfromfile = False
        else:
            loadfromfile = True
        if not self.data or loadfromfile:
            try:
                with Path(f'{coin_path}/coindata.json').open() as f:
                    self.data = json.load(f)
                    self.data_ts = data_ts
                    return
            except Exception as e:
                print(f'Error loading coindata: {e}.')
    
    def load_metadata(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        metadata_ts = 0
        if Path(f'{coin_path}/metadata.json').exists():
            metadata_ts = Path(f'{coin_path}/metadata.json').stat().st_mtime
        now_ts = datetime.now().timestamp()
        if metadata_ts < now_ts - 3600*24*self.metadata_interval:
            self.fetch_metadata()
            self.save_metadata()
        if not self.metadata and Path(f'{coin_path}/metadata.json').exists():
            try:
                with Path(f'{coin_path}/metadata.json').open() as f:
                    self.metadata = json.load(f)
                    self.metadata_ts = metadata_ts
                    return
            except Exception as e:
                print(f'Error loading metadata: {e}. Retrying in 5 seconds...')

    def is_data_fresh(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if Path(f'{coin_path}/coindata.json').exists():
            data_ts = Path(f'{coin_path}/coindata.json').stat().st_mtime
            now_ts = datetime.now().timestamp()
            if data_ts > now_ts - 3600*self.fetch_interval:
                return True
        return
    
    def is_metadata_fresh(self):
        pbgdir = Path.cwd()
        coin_path = f'{pbgdir}/data/coindata'
        if Path(f'{coin_path}/metadata.json').exists():
            data_ts = Path(f'{coin_path}/metadata.json').stat().st_mtime
            now_ts = datetime.now().timestamp()
            if data_ts > now_ts - 3600*24*self.metadata_interval:
                return True
        return

    def update_symbols(self):
        now_ts = datetime.now().timestamp()
        if self.update_symbols_ts < now_ts - 3600*24:
            for exchange in self.exchanges:
                exc = Exchange(exchange)
                try:
                    exc.fetch_symbols()
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Update Symbols {exchange}')
                except Exception as e:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Failed to fetch symbols for {exchange}')
            self.update_symbols_ts = now_ts
            self._symbols = []
            self._symbols_cpt = []
            self._symbols_all = []

    def load_symbols(self):
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        exchange = "kucoinfutures" if self.exchange == "kucoin" else self.exchange
        if pb_config.has_option("exchanges", f'{exchange}.swap'):
            self._symbols = eval(pb_config.get("exchanges", f'{exchange}.swap'))
        if self.exchange in ["binance", "bybit", "bitget"]:
            if pb_config.has_option("exchanges", f'{exchange}.cpt'):
                self._symbols_cpt = eval(pb_config.get("exchanges", f'{exchange}.cpt'))
                return
        self._symbols_cpt = self._symbols
    
    def load_symbols_all(self):
        self._symbols_all = []
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        for exchange in self.exchanges:
            if pb_config.has_option("exchanges", f'{exchange}.swap'):
                # add symbol from symbols to symbols_all if not already in symbols_all
                self._symbols_all += eval(pb_config.get("exchanges", f'{exchange}.swap'))
        self._symbols_all = sorted(list(set(self._symbols_all)))

    def list_symbols(self):
        if self.has_new_data():
            self.load_data()
            self.load_symbols()
        if not self.data:
            return
        if "data" not in self.data:
            return
        if self.has_new_metadata():
            self.load_metadata()
        if not self.metadata:
            return
        if "data" not in self.metadata:
            return
        self._symbols_data = []
        self._symbols_notice = []
        self._symbols_notices = {}
        self.approved_coins = []
        self.ignored_coins = []
        coin_data = []
        for symbol in self.symbols:
            market_cap = 0
            sym = symbol[0:-4]
            if sym in SYMBOLMAP:
                sym = SYMBOLMAP[sym]
            for id, coin in enumerate(self.data["data"]):
                if coin["symbol"] == sym or (sym == "NEIROETH" and coin["id"] == 32461):
                    if coin["quote"]["USD"]["market_cap"]:
                        coin_data = coin
                        market_cap = coin["quote"]["USD"]["market_cap"]
                        break
                    elif coin["self_reported_market_cap"]:
                        coin_data = coin
                        market_cap = coin["self_reported_market_cap"]
                        break
            if symbol not in self._symbols_data:
                if market_cap > 0:
                    notice = None
                    # Find metadata for coin
                    symbol_id = str(coin_data["id"])
                    if symbol_id in self.metadata["data"]:
                        notice = self.metadata["data"][symbol_id]["notice"]
                        if notice:
                            self._symbols_notice.append(symbol)
                            self._symbols_notices[symbol] = notice
                    symbol_data = {
                        "id": id,
                        "symbol": symbol,
                        "name": coin_data["name"],
                        "tags": coin_data["tags"],
                        "price": coin_data["quote"]["USD"]["price"],
                        "volume_24h": int(coin_data["quote"]["USD"]["volume_24h"]),
                        "market_cap": int(market_cap),
                        "vol/mcap": coin_data["quote"]["USD"]["volume_24h"]/market_cap,
                        "copy_trading": symbol in self.symbols_cpt,
                        "notice": notice,
                        "link": f'https://coinmarketcap.com/currencies/{coin_data["slug"]}',
                    }
                else:
                    symbol_data = {
                        "id": 999999,
                        "symbol": symbol,
                        "name": "not found on CoinMarketCap",
                        "tags": [],
                        "price": 0,
                        "volume_24h": 0,
                        "market_cap": 0,
                        "vol/mcap": 0,
                        "copy_trading": symbol in self.symbols_cpt,
                        "notice": None,
                        "link": None,
                    }
                for tag in symbol_data["tags"]:
                    if tag not in self._all_tags:
                        self._all_tags.append(tag)
                cpt = True
                if self.only_cpt and not symbol_data["copy_trading"]:
                    cpt = False
                no_notice = True
                if self.notices_ignore and symbol in self._symbols_notice:
                    no_notice = False
                # if self.market_cap != 0 or self.vol_mcap != 10.0:
                if no_notice and cpt and market_cap >= self.market_cap*1000000 and symbol_data["vol/mcap"] < self.vol_mcap and (not self.tags or any(tag in symbol_data["tags"] for tag in self.tags)):
                    self._symbols_data.append(symbol_data)
                    self.approved_coins.append(symbol)
                else:
                    self.ignored_coins.append(symbol)
        #Sort approved and ignored coins and symbols_Data
        self.approved_coins.sort()
        self.ignored_coins.sort()
        self._symbols_data = sorted(self._symbols_data, key=lambda x: x["market_cap"], reverse=True)

    def filter_by_market_cap(self, symbols: list, mc: int):
        ignored_coins = []
        approved_coins = []
        self.load_data()
        for symbol in symbols:
            sym = symbol[0:-4]
            if sym in SYMBOLMAP:
                sym = SYMBOLMAP[sym]
            for coin in self.data["data"]:
                if coin["symbol"] == sym:
                    if coin["quote"]["USD"]["market_cap"] and coin["quote"]["USD"]["market_cap"] > mc:
                        approved_coins.append(symbol)
                        break
                    elif coin["self_reported_market_cap"] and coin["self_reported_market_cap"] > mc:
                        approved_coins.append(symbol)
                        break
            if symbol not in approved_coins:
                ignored_coins.append(symbol)
        return approved_coins, ignored_coins

def main():
    pbgdir = Path.cwd()
    dest = Path(f'{pbgdir}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBCoinData.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBCoinData')
    pbcoindata = CoinData()
    if pbcoindata.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBCoinData already started')
        exit(1)
    pbcoindata.save_pid()
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
            pbcoindata.update_symbols()
            pbcoindata.load_data()
            pbcoindata.load_metadata()
            sleep(60)
            pbcoindata.load_config()
        except Exception as e:
            print(f'Something went wrong, but continue {e}')
            traceback.print_exc()

if __name__ == '__main__':
    main()
