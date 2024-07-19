import json
from pathlib import Path, PurePath
from datetime import datetime
import shutil
import configparser

class User:
    def __init__(self):
        self._name = None
        self._exchange = None
        self._key = None
        self._secret = None
        self._passphrase = None
    
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


class Users:
    def __init__(self):
        self.users = []
        self.index = 0
        pb_config = configparser.ConfigParser()
        pb_config.read('pbgui.ini')
        pbdir = pb_config.get("main", "pbdir")
        self.api_path = f'{pbdir}/api-keys.json'
        pbgdir = Path.cwd()
        self.api_backup = Path(f'{pbgdir}/data/api-keys')
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
        try:
            with Path(self.api_path).open(encoding="UTF-8") as f:
                users = json.load(f)
        except Exception as e:
            print(f'{self.api_path} is corrupted {e}')
            return
        for user in users:
            if "exchange" in users[user]:
                my_user = User()
                my_user.name = user
                my_user.exchange = users[user]["exchange"]
                if "key" in users[user]:
                    my_user.key = users[user]["key"]
                if "secret" in users[user]:
                    my_user.secret = users[user]["secret"]
                if "passphrase" in users[user]:
                    my_user.passphrase = users[user]["passphrase"]
                self.users.append(my_user)
        self.users.sort(key=lambda x: x.name)

    def save(self):
        save_users = {}
        for user in self.users:
            save_users[user.name] = ({
                        "exchange": user.exchange,
                        "key": user.key,
                        "secret": user.secret
                    })
            if user.passphrase:
                save_users[user.name]["passphrase"] = user.passphrase
        # Backup api-keys and save new version
        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        destination = Path(f'{self.api_backup}/api-keys_{date}.json')
        if not self.api_backup.exists():
            self.api_backup.mkdir(parents=True)
        if Path(self.api_path).exists():
            shutil.copy(PurePath(self.api_path), destination)
        with Path(f'{self.api_path}').open("w", encoding="UTF-8") as f:
            json.dump(save_users, f, indent=4)

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
