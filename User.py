import json
from pathlib import Path

class User:
    def __init__(self):
        self._name = None
        self._exchange = None
        self._key = None
        self._secret = None
    
    @property
    def name(self): return self._name
    @property
    def key(self): return self._key
    @property
    def secret(self): return self._secret
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


class Users:
    def __init__(self, api_path: Path = None):
        self.users = []
        self.index = 0
        self.api_path = api_path
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

    def find_user(self, name: str):
        for user in self.users:
            if user.name == name:
                return user.name

    def find_exchange(self, name: str):
        for user in self.users:
            if user.name == name:
                return user.exchange

    def load(self):
        with Path(self.api_path).open(encoding="UTF-8") as f:
            users = json.load(f)
        for user in users:
            if "exchange" in users[user]:
                my_user = User()
                my_user.name = user
                my_user.exchange = users[user]["exchange"]
                if "key" in users[user]:
                    my_user.key = users[user]["key"]
                if "secret" in users[user]:
                    my_user.secret = users[user]["secret"]
                self.users.append(my_user)


def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
