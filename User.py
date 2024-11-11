import json
from pathlib import Path, PurePath
from datetime import datetime
from pbgui_purefunc import pbdir, pb7dir, PBGDIR, is_pb_installed, is_pb7_installed
import shutil

class User:
    def __init__(self):
        self._name = None
        self._exchange = None
        self._key = None
        self._secret = None
        self._passphrase = None
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


class Users:
    def __init__(self):
        self.users = []
        self.index = 0
        self.api_path = f'{pbdir()}/api-keys.json'
        self.api7_path = f'{pb7dir()}/api-keys.json'
        self.api_backup = Path(f'{PBGDIR}/data/api-keys')
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
    
    def list_single(self):
        from Exchange import Single
        return list(map(lambda c: c.name, filter(lambda c: c.exchange in Single.list(), self.users)))

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

    def load(self):
        self.users = []
        users = ""
        try:
            if Path(self.api_path).exists():
                with Path(self.api_path).open(encoding="UTF-8") as f:
                    users = json.load(f)
        except Exception as e:
            print(f'{self.api_path} is corrupted {e}')
        try:
            if Path(self.api7_path).exists():
                with Path(self.api7_path).open(encoding="UTF-8") as f:
                    if users:
                        users.update(json.load(f))
                    else:
                        users = json.load(f)
        except Exception as e:
            print(f'{self.api7_path} is corrupted {e}')
        for user in users:
            if "exchange" in users[user]:
                my_user = User()
                my_user.name = user
                if my_user.name not in self.list():
                    my_user.exchange = users[user]["exchange"]
                    if "key" in users[user]:
                        my_user.key = users[user]["key"]
                    if "secret" in users[user]:
                        my_user.secret = users[user]["secret"]
                    if "passphrase" in users[user]:
                        my_user.passphrase = users[user]["passphrase"]
                    if "wallet_address" in users[user]:
                        my_user.wallet_address = users[user]["wallet_address"]
                    if "private_key" in users[user]:
                        my_user.private_key = users[user]["private_key"]
                    if "is_vault" in users[user]:
                        my_user.is_vault = users[user]["is_vault"]
                    self.users.append(my_user)
        self.users.sort(key=lambda x: x.name)

    def save(self):
        save_users = {}
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
        # Backup api-keys and save new version
        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        if not self.api_backup.exists():
            self.api_backup.mkdir(parents=True)
        if is_pb_installed():
            destination = Path(f'{self.api_backup}/api-keys_{date}.json')
            if Path(self.api_path).exists():
                shutil.copy(PurePath(self.api_path), destination)
            with Path(f'{self.api_path}').open("w", encoding="UTF-8") as f:
                json.dump(save_users, f, indent=4)
        # Backup api-keys7 and save new version
        if is_pb7_installed():
            destination = Path(f'{self.api_backup}/api-keys7_{date}.json')
            if Path(self.api7_path).exists():
                shutil.copy(PurePath(self.api7_path), destination)
            with Path(f'{self.api7_path}').open("w", encoding="UTF-8") as f:
                json.dump(save_users, f, indent=4)

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
