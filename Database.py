from pathlib import Path
from datetime import datetime, timezone
from time import sleep
from shutil import rmtree
from User import Users, User
from Exchange import Exchange
from pbgui_func import PBDIR, PBGDIR, error_popup, info_popup
import sqlite3
import pandas as pd

class Database():
    def __init__(self):
        self.db = Path(f'{PBGDIR}/data/pbgui.db')
        self.create_tables()

    def create_tables(self):
        sql_statements = [ 
            """CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    income INTEGER NOT NULL,
                    uniqueid text NOT NULL UNIQUE,
                    user TEXT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS position (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    psize REAL NOT NULL,
                    upnl REAL NOT NULL,
                    entry REAL NOT NULL,
                    user TEXT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    price REAL NOT NULL,
                    side TEXT NOT NULL,
                    uniqueid text NOT NULL UNIQUE,
                    user TEXT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS prices (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    price REAL NOT NULL,
                    user TEXT NOT NULL
            );""",
            """CREATE TABLE IF NOT EXISTS balances (
                    id INTEGER PRIMARY KEY,
                    timestamp INTEGER NOT NULL,
                    balance REAL NOT NULL,
                    user TEXT NOT NULL UNIQUE
            );"""
            ]
        # create a database connection
        try:
            with sqlite3.connect(self.db) as conn:
                cursor = conn.cursor()
                for statement in sql_statements:
                    cursor.execute(statement)
                conn.commit()
        except sqlite3.Error as e:
            print(e)

    def update_history(self, user: User):
        history = self.fetch_history(user)
        try:
            with sqlite3.connect(self.db) as conn:
                for line in history:
                    income = [
                        line['symbol'],
                        line['timestamp'],
                        line['income'],
                        line['uniqueid'],
                        user.name
                    ]
                    self.add_history(conn, income)
        except sqlite3.Error as e:
            print(e)
    
    def update_positions(self, user: User):
        positions_db = self.fetch_positions(user)
        exchange = Exchange(user.exchange, user)
        positions = exchange.fetch_positions()
        symbols = []
        for symbol in positions:
            symbols.append(symbol['symbol'][0:-5].replace("/", "").replace("-", ""))
        symbols_db = []
        for position in positions_db:
            symbols_db.append(position[1])
        try:
            with sqlite3.connect(self.db) as conn:
                # Remove positions that are not in the exchange
                for position in positions_db:
                    if position[1] not in symbols:
                        print(f"Removing {position[1]}")
                        self.remove_position(conn, position[0])
                # Update positions
                for position in positions:
                    pos = [
                        position['timestamp'],
                        position['contracts'] * position['contractSize'],
                        position['unrealizedPnl'],
                        position['entryPrice'],
                        position['symbol'][0:-5].replace("/", "").replace("-", ""),
                        user.name
                    ]
                    if pos[4] in symbols_db:
                        print(f"Updating {pos[4]}")
                        self.update_position(conn, pos)
                    else:
                        print(f"Adding {pos[4]}")
                        self.add_position(conn, pos)
        except sqlite3.Error as e:
            print(e)
    
    def update_orders(self, user: User):
        positions_db = self.fetch_positions(user)
        orders_db = self.fetch_orders(user)
        exchange = Exchange(user.exchange, user)
        all_orders = []
        for position in positions_db:
            orders = exchange.fetch_all_open_orders(position[1][0:-4] + "/USDT:USDT")
            all_orders.extend(orders)
        ids_db = []
        for order in orders_db:
            ids_db.append(order[6])
        ids = []
        for order in all_orders:
            ids.append(order['id'])
        try:
            with sqlite3.connect(self.db) as conn:
                # Remove orders that are not in the exchange
                for order in orders_db:
                    if order[6] not in ids:
                        print(f"Removing {order[6]}")
                        self.remove_order(conn, order[0])
                # Update orders
                for order in all_orders:
                    ord = [
                        order['timestamp'],
                        order['amount'],
                        order['price'],
                        order['side'],
                        order['id'],
                        order['symbol'][0:-5].replace("/", "").replace("-", ""),
                        user.name
                    ]
                    if ord[4] in ids_db:
                        print(f"Updating {ord[4]}")
                        self.update_order(conn, ord)
                    else:
                        print(f"Adding {ord[4]}")
                        self.add_order(conn, ord)
        except sqlite3.Error as e:
            print(e)

    def update_prices(self, user: User):
        positions_db = self.fetch_positions(user)
        prices_db = self.fetch_prices(user)
        symbols_db = []
        for price in prices_db:
            symbols_db.append(price[1])
        exchange = Exchange(user.exchange, user)
        symbols = []
        prices = {}
        for position in positions_db:
            symbol = position[1]
            symbol_ccxt = f'{symbol[0:-4]}/USDT:USDT'
            symbols.append(symbol_ccxt)
        if symbols:
            market_type = "futures"
            prices = exchange.fetch_prices(symbols, market_type)
        symbols = []
        for symbol_ccxt in prices:
            symbol = symbol_ccxt[0:-5].replace("/", "").replace("-", "")
            symbols.append(symbol)
        try:
            with sqlite3.connect(self.db) as conn:
                # Remove symbols that are not in the exchange
                for symbol in symbols_db:
                    if symbol not in symbols:
                        print(f"Removing {symbol}")
                        self.remove_price(conn, symbol, user.name)
                # Update prices
                for symbol in symbols:
                    symbol_ccxt = f'{symbol[0:-4]}/USDT:USDT'
                    timestamp = prices[symbol_ccxt]['timestamp']
                    if not timestamp:
                        timestamp = int(datetime.now().timestamp() * 1000)
                    price = [
                        timestamp,
                        prices[symbol_ccxt]['last'],
                        symbol,
                        user.name
                    ]
                    if symbol in symbols_db:
                        print(f"Updating {symbol}")
                        self.update_price(conn, price)
                    else:
                        print(f"Adding {symbol}")
                        self.add_price(conn, price)
        except sqlite3.Error as e:
            print(e)

    def update_balances(self, user: User):
        exchange = Exchange(user.exchange, user)
        market_type = "swap"
        balance = exchange.fetch_balance(market_type)
        try:
            with sqlite3.connect(self.db) as conn:
                balance_list = [
                    int(datetime.now().timestamp() * 1000),
                    balance,
                    user.name
                ]
                print(f"Updating balance {user.name}")
                self.update_balance(conn, balance_list)
        except sqlite3.Error as e:
            print(e)

    def add_history(self, conn: sqlite3.Connection, history: list):
        sql = '''INSERT INTO history(symbol,timestamp,income,uniqueid,user)
                VALUES(?,?,?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, history)
            conn.commit()
        except sqlite3.Error as e:
            print(e, history)
    
    def add_position(self, conn: sqlite3.Connection, position: list):
        sql = '''INSERT INTO position(timestamp,psize,upnl,entry,symbol,user)
                VALUES(?,?,?,?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, position)
            conn.commit()
        except sqlite3.Error as e:
            print(e, position)
        return cur.lastrowid

    def add_order(self, conn: sqlite3.Connection, order: list):
        sql = '''INSERT INTO orders(timestamp,amount,price,side,uniqueid,symbol,user)
                VALUES(?,?,?,?,?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, order)
            conn.commit()
        except sqlite3.Error as e:
            print(e, order)

    def add_price(self, conn: sqlite3.Connection, price: list):
        sql = '''INSERT INTO prices(timestamp,price,symbol,user)
                VALUES(?,?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, price)
            conn.commit()
        except sqlite3.Error as e:
            print(e, price)

    def remove_position(self, conn: sqlite3.Connection, id: int):
        sql = '''DELETE FROM position WHERE id = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, [id])
            conn.commit()
        except sqlite3.Error as e:
            print(e)
    
    def remove_order(self, conn: sqlite3.Connection, id: int):
        sql = '''DELETE FROM orders WHERE id = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, [id])
            conn.commit()
        except sqlite3.Error as e:
            print(e)

    def remove_price(self, conn: sqlite3.Connection, symbol: str, user: str):
        sql = '''DELETE FROM prices WHERE symbol = ? AND user = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, [symbol, user])
            conn.commit()
        except sqlite3.Error as e:
            print(e)

    def update_position(self, conn: sqlite3.Connection, position: list):
        sql = '''UPDATE position
                SET timestamp = ?,
                    psize = ?,
                    upnl = ?,
                    entry = ?
                WHERE symbol = ? AND user = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, position)
            conn.commit()
        except sqlite3.Error as e:
            print(e, position)

    def update_order(self, conn: sqlite3.Connection, order: list):
        sql = '''UPDATE orders
                SET timestamp = ?,
                    amount = ?,
                    price = ?,
                    side = ?
                WHERE uniqueid = ? AND symbol = ? AND user = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, order)
            conn.commit()
        except sqlite3.Error as e:
            print(e, order)

    def update_price(self, conn: sqlite3.Connection, price: list):
        sql = '''UPDATE prices
                SET timestamp = ?,
                    price = ?
                WHERE symbol = ? AND user = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, price)
            conn.commit()
        except sqlite3.Error as e:
            print(e, price)

    def update_balance(self, conn: sqlite3.Connection, balance: list):
        sql = '''INSERT OR REPLACE INTO balances(timestamp,balance,user)
                VALUES(?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, balance)
            conn.commit()
        except sqlite3.Error as e:
            print(e, balance)

    def fetch_history(self, user: User):
        exchange = Exchange(user.exchange, user)
        return exchange.fetch_history(self.find_last_timestamp(user))

    def fetch_positions(self, user: User):
        sql = '''SELECT * FROM "position"
                WHERE "position"."user" = ? '''
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, [user.name])
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)

    def fetch_orders(self, user: User):
        sql = '''SELECT * FROM "orders"
                WHERE "orders"."user" = ? '''
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, [user.name])
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)
    
    def fetch_orders_by_symbol(self, user: str, symbol: str):
        sql = '''SELECT * FROM "orders"
                WHERE "orders"."user" = ?
                    AND "orders"."symbol" = ? '''
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, [user, symbol])
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)

    def fetch_prices(self, user: User):
        sql = '''SELECT * FROM "prices"
                WHERE "prices"."user" = ? '''
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, [user.name])
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)

    def fetch_balances(self, user: list):
        sql = '''SELECT * FROM "balances"
                WHERE "balances"."user" IN ({}) '''.format(','.join('?'*len(user)))
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, user)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)

    def select_top(self, user: list, start: str, end: str, top: int):
        if 'ALL' in user:
            sql = '''SELECT strftime('%Y-%m-%d',"timestamp" / 1000, 'unixepoch') as date, "history"."symbol" AS symbol, SUM("history"."income") AS sum FROM "history"
                    WHERE "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    GROUP BY "history"."symbol"
                    ORDER BY "sum" DESC, "history"."symbol"
                    LIMIT ? '''
            sql_parameters = (start, end, top)
        else:
            sql = '''SELECT strftime('%Y-%m-%d',"timestamp" / 1000, 'unixepoch') as date, "history"."symbol" AS symbol, SUM("history"."income") AS sum FROM "history"
                    WHERE "history"."user" IN ({})
                        AND "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    GROUP BY "history"."symbol"
                    ORDER BY "sum" DESC, "history"."symbol"
                    LIMIT ? '''.format(','.join('?'*len(user)))
            sql_parameters = tuple(user) + (start, end, top)
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)
        
    def select_pnl(self, user: list, start: str, end: str):
        if 'ALL' in user:
            sql = '''SELECT strftime('%Y-%m-%d',"timestamp" / 1000, 'unixepoch') as date, SUM("income") AS "sum" FROM "history"
                    WHERE "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    GROUP BY date '''
            sql_parameters = (start, end)
        else:
            sql = '''SELECT strftime('%Y-%m-%d',"timestamp" / 1000, 'unixepoch') as date, SUM("income") AS "sum" FROM "history"
                    WHERE "history"."user" IN ({})
                        AND "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    GROUP BY date'''.format(','.join('?'*len(user)))
            sql_parameters = tuple(user) + (start, end)
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)
    
    def select_income(self, user: list, start: str, end: str):
        if 'ALL' in user:
            sql = '''SELECT "timestamp", "income" FROM "history"
                    WHERE "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    ORDER BY "timestamp" ASC '''
            sql_parameters = (start, end)
        else:
            sql = '''SELECT "timestamp", "income" FROM "history"
                    WHERE "history"."user" IN ({})
                        AND "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    ORDER BY "timestamp" ASC'''.format(','.join('?'*len(user)))
            sql_parameters = tuple(user) + (start, end)
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)
    
    # select income grouped by symbol not sum
    def select_income_by_symbol(self, user: list, start: str, end: str):
        if 'ALL' in user:
            sql = '''SELECT "timestamp", "symbol", "income" FROM "history"
                    WHERE "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    ORDER BY "timestamp" ASC '''
            sql_parameters = (start, end)
        else:
            sql = '''SELECT "timestamp", "symbol", "income" FROM "history"
                    WHERE "history"."user" IN ({})
                        AND "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    ORDER BY "timestamp" ASC'''.format(','.join('?'*len(user)))
            sql_parameters = tuple(user) + (start, end)
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            print(e)

    def find_last_timestamp(self, user: User):
        sql = '''SELECT MAX("history"."timestamp") FROM "history"
                WHERE "history"."user" = ? '''
        try:
            with sqlite3.connect(self.db) as conn:
                cur = conn.cursor()
                cur.execute(sql, [user.name])
                rows = cur.fetchall()
                if rows[0][0] is None:
                    return 0
                return rows[0][0]
        except sqlite3.Error as e:
            print(e)

def main():
    print("Don't Run this Class from CLI")
    # users = Users()
    # user = users.find_user("bitget_CRASH")
    # exchange = Exchange("bitget", user)
    # db = Database()
    # balances = db.fetch_balance(['bitget_CRASH','binance_CPT'])
    # print(balances)
    # db.update_positions(user)
    # db.update_prices(user)
    # db.update_balances(user)

if __name__ == '__main__':
    main()
