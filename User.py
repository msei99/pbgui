import json
import socket
from pathlib import Path, PurePath
from datetime import datetime, timezone
from api_key_state import strip_runtime_extra
from pbgui_purefunc import pb7dir, PBGDIR, is_pb7_installed
import shutil

class User:
    def __init__(self):
        self._name = None
        self._exchange = None
        self._key = None
        self._secret = None
        self._passphrase = None
        # CCXT/pb7 passthrough fields (optional)
        self._quote = None
        self._options = None
        self._extra = {}
        # Hyperliquid
        self._wallet_address = None
        self._private_key = None
        self._is_vault = False
    
    @property
    def name(self): return self._name
    @property
    def key(self): return self._key
    @property
    def secret(self): return self._secret
    @property
    def passphrase(self): return self._passphrase
    @property
    def exchange(self): return self._exchange
    @property
    def wallet_address(self): return self._wallet_address
    @property
    def private_key(self): return self._private_key
    @property
    def is_vault(self): return self._is_vault
    @property
    def quote(self): return self._quote
    @property
    def options(self): return self._options
    @property
    def extra(self): return self._extra

    @name.setter
    def name(self, new_name):
        self._name = new_name
    @exchange.setter
    def exchange(self, new_exchange):
        self._exchange = new_exchange
    @key.setter
    def key(self, new_key):
        self._key = new_key
    @secret.setter
    def secret(self, new_secret):
        self._secret = new_secret
    @passphrase.setter
    def passphrase(self, new_passphrase):
        self._passphrase = new_passphrase
    @wallet_address.setter
    def wallet_address(self, new_wallet_address):
        self._wallet_address = new_wallet_address
    @private_key.setter
    def private_key(self, new_private_key):
        self._private_key = new_private_key
    @is_vault.setter
    def is_vault(self, new_is_vault):
        self._is_vault = new_is_vault
    @quote.setter
    def quote(self, new_quote):
        self._quote = new_quote
    @options.setter
    def options(self, new_options):
        self._options = new_options
    @extra.setter
    def extra(self, new_extra):
        self._extra = new_extra


class Users:
    def __init__(self):
        self.users = []
        self.index = 0
        self.api7_path = f'{pb7dir()}/api-keys.json'
        self.api_backup = Path(f'{PBGDIR}/data/api-keys')
        self._top_level_extras = {}
        self.load()
    
    def __iter__(self):
        return iter(self.users)

    def __next__(self):
        if self.index > len(self.users):
            raise StopIteration
        self.index += 1
        return next(self)
    
    def list(self):
        return list(map(lambda c: c.name, self.users))

    @property
    def api_meta(self) -> dict:
        """Return _api_serial / _api_ts / _api_by from the loaded file."""
        return {
            "api_serial": self._top_level_extras.get("_api_serial", 0),
            "api_ts":     self._top_level_extras.get("_api_ts"),
            "api_by":     self._top_level_extras.get("_api_by"),
        }
    
    def list_v7(self):
        from Exchange import V7
        return list(map(lambda c: c.name, filter(lambda c: c.exchange in V7.list(), self.users)))

    def default(self):
        if self.users:
            return self.users[0].name
        else:
            return None

    def has_user(self, user: User):
        for u in self.users:
            if u != user and u.name == user.name:
                return True
        return False

    def remove_user(self, name: str):
        for user in self.users:
            if user.name == name:
                self.users.remove(user)
                self.save()

    def find_user(self, name: str):
        for user in self.users:
            if user.name == name:
                return user

    def find_exchange(self, name: str):
        for user in self.users:
            if user.name == name:
                return user.exchange

    def find_exchange_user(self, exchange: str):
        for user in self.users:
            if user.exchange == exchange:
                return user.name
    
    @property
    def tradfi(self) -> dict:
        """TradFi data provider config (alpaca, polygon, etc.) for stock perps backtesting."""
        val = self._top_level_extras.get("tradfi", {})
        return val if isinstance(val, dict) else {}

    @tradfi.setter
    def tradfi(self, value: dict):
        if value:
            self._top_level_extras["tradfi"] = value
        else:
            self._top_level_extras.pop("tradfi", None)

    def find_binance_user(self):
        for user in self.users:
            if user.exchange == "binance":
                if user.key and user.secret:
                    if len(user.key) > 20 and len(user.secret) > 20:
                        return user
        return None

    def find_binance_users(self):
        users = []
        for user in self.users:
            if user.exchange == "binance":
                if user.key and user.secret:
                    if len(user.key) > 20 and len(user.secret) > 20:
                        users.append(user)
        if users:
            return users
        return None

    def find_bitget_users(self):
        users = []
        for user in self.users:
            if user.exchange == "bitget":
                if user.key and user.secret and user.passphrase:
                    if len(user.key) > 20 and len(user.secret) > 20 and len(user.passphrase) > 20:
                        users.append(user)
        if users:
            return users                        
        return None

    def load(self):
        self.users = []
        users: dict = {}
        self._top_level_extras = {}
        try:
            if Path(self.api7_path).exists():
                with Path(self.api7_path).open(encoding="UTF-8") as f:
                    loaded = json.load(f)
                    if not isinstance(loaded, dict):
                        raise ValueError(
                            f"{self.api7_path} has invalid format: expected JSON object at top-level"
                        )
                    users = loaded
        except Exception as e:
            raise ValueError(f"Failed to load api-keys: {self.api7_path}: {e}")

        if not isinstance(users, dict):
            raise ValueError("api-keys data has invalid format: expected JSON object")

        def _get_first(dct: dict, keys: list[str]):
            for k in keys:
                if k in dct and dct[k] is not None:
                    return dct[k]
            return None

        for user_name, user_data in users.items():
            if user_name == "referrals" or user_name == "tradfi" or str(user_name).startswith("_"):
                self._top_level_extras[user_name] = user_data
                continue

            if not isinstance(user_data, dict):
                raise ValueError(
                    f"api-keys entry '{user_name}' has invalid format: expected object with 'exchange', got {type(user_data).__name__}"
                )

            if "exchange" in user_data:
                my_user = User()
                my_user.name = user_name
                if my_user.name not in self.list():
                    my_user.exchange = user_data["exchange"]

                    # Accept both PBGui-style and CCXT/pb7-style aliases
                    my_user.key = _get_first(user_data, ["key", "apiKey", "api_key"])
                    my_user.secret = _get_first(user_data, ["secret"])
                    my_user.passphrase = _get_first(user_data, ["passphrase", "password"])
                    my_user.wallet_address = _get_first(user_data, ["wallet_address", "walletAddress", "wallet"])
                    my_user.private_key = _get_first(user_data, ["private_key", "privateKey"])
                    if "is_vault" in user_data:
                        my_user.is_vault = user_data["is_vault"]

                    # Optional passthrough fields used by pb7/ccxt
                    if "quote" in user_data:
                        my_user.quote = user_data["quote"]
                    if "options" in user_data:
                        if isinstance(user_data["options"], dict):
                            my_user.options = user_data["options"]
                        else:
                            # Preserve invalid/unexpected types without crashing
                            my_user.options = None

                    # Preserve unknown fields so editing in PBGui doesn't break pb7 configs
                    canonical_keys = {
                        "exchange",
                        "key",
                        "apiKey",
                        "api_key",
                        "secret",
                        "passphrase",
                        "password",
                        "wallet_address",
                        "walletAddress",
                        "wallet",
                        "private_key",
                        "privateKey",
                        "is_vault",
                        "quote",
                        "options",
                    }
                    extras = {k: v for k, v in user_data.items() if k not in canonical_keys}
                    if "options" in user_data and not isinstance(user_data.get("options"), dict):
                        extras["options"] = user_data.get("options")
                    my_user.extra = extras
                    self.users.append(my_user)
        self.users.sort(key=lambda x: x.name)

    def save(self):
        save_users = dict(self._top_level_extras) if isinstance(self._top_level_extras, dict) else {}

        # Migrate old sync field names → new api field names (one-time, transparent)
        for old, new in (("_sync_serial", "_api_serial"),
                         ("_sync_ts", "_api_ts"),
                         ("_sync_by", "_api_by")):
            if old in save_users and new not in save_users:
                save_users[new] = save_users.pop(old)
            elif old in save_users:
                del save_users[old]

        # Bump api serial and record editor metadata
        save_users["_api_serial"] = save_users.get("_api_serial", 0) + 1
        save_users["_api_ts"] = datetime.now(timezone.utc).isoformat()
        save_users["_api_by"] = socket.gethostname()

        for user in self.users:
            save_users[user.name] = ({
                        "exchange": user.exchange
                    })
            if user.key:
                save_users[user.name]["key"] = user.key
            if user.secret:
                save_users[user.name]["secret"] = user.secret
            if user.passphrase:
                save_users[user.name]["passphrase"] = user.passphrase
            if user.wallet_address:
                save_users[user.name]["wallet_address"] = user.wallet_address
            if user.private_key:
                save_users[user.name]["private_key"] = user.private_key
            if user.exchange == "hyperliquid":
                save_users[user.name]["is_vault"] = user.is_vault
            if user.quote:
                save_users[user.name]["quote"] = user.quote
            if isinstance(user.options, dict) and user.options:
                save_users[user.name]["options"] = user.options
            clean_extra = strip_runtime_extra(user.extra)
            if clean_extra:
                for k, v in clean_extra.items():
                    if k not in save_users[user.name]:
                        save_users[user.name][k] = v
        # Backup api-keys and save new version
        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        if not self.api_backup.exists():
            self.api_backup.mkdir(parents=True)

        # Backup api-keys7 and save new version
        if is_pb7_installed():
            destination = Path(f'{self.api_backup}/api-keys7_{date}.json')
            if Path(self.api7_path).exists():
                shutil.copy(PurePath(self.api7_path), destination)
            with Path(f'{self.api7_path}').open("w", encoding="UTF-8") as f:
                json.dump(save_users, f, indent=4)
