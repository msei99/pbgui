from pathlib import Path
from datetime import datetime
from User import Users, User
from Exchange import Exchange
from pbgui_func import PBGDIR
import shutil
import sqlite3
import json
import time
import threading

class Database():
    def __init__(self):
        self.db = Path(f'{PBGDIR}/data/pbgui.db')
        # Global write lock to serialize all write operations
        self._write_lock = threading.Lock()
        # Thread-local storage for per-thread SQLite connections to avoid
        # repeated open/close syscalls (reduces openat/read activity).
        self._local = threading.local()
        self.create_tables()

    def _log(self, msg: str, level: str = 'INFO'):
        """Print a log line with timestamp, module tag and level."""
        try:
            ts = datetime.now().isoformat(sep=" ", timespec="seconds")
        except TypeError:
            # Fallback if timespec unsupported
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            print(f"{ts} [Database] [{level}] {msg}")
        except Exception:
            # best-effort fallback
            print(f"{ts} [Database] {msg}")
    
    def _connect(self):
        """Return a per-thread cached SQLite connection.

        New connections get PRAGMA tuning applied. Connections are cached on
        `self._local.conn` so each worker thread reuses its own connection and
        we avoid repeated open/close syscalls that were visible in strace.
        """
        local = getattr(self, '_local', None)
        if local is None:
            # Fallback: create thread-local container
            self._local = threading.local()
            local = self._local

        conn = getattr(local, 'conn', None)
        try:
            # Quick health-check: if connection exists and is open, reuse it
            if conn is not None:
                try:
                    # lightweight check
                    conn.execute('SELECT 1')
                    return conn
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
        except Exception:
            conn = None

        # Create a fresh connection for this thread
        conn = sqlite3.connect(self.db, timeout=30)
        try:
            conn.execute('PRAGMA busy_timeout=30000')
        except Exception:
            pass
        # Apply journaling PRAGMAs on new connections for consistency
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
        except Exception:
            pass
        local.conn = conn
        return conn
    
    # Simple full DB backup: copies the SQLite file to backup/db with timestamp name
    def backup_full_db(self, keep_last: int = 10):
        try:
            backups_dir = Path(f'{PBGDIR}/data/backup/db')
            backups_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime('%Y%m%d-%H%M%S')
            backup_path = backups_dir / f'pbgui-{ts}.db'
            shutil.copy2(self.db, backup_path)
            # Rotate: keep only the last N backups by modified time
            try:
                backups = sorted(
                    [p for p in backups_dir.glob('pbgui-*.db') if p.is_file()],
                    key=lambda p: p.stat().st_mtime,
                    reverse=True
                )
                if keep_last is not None and keep_last > 0 and len(backups) > keep_last:
                    for old in backups[keep_last:]:
                        try:
                            old.unlink()
                        except Exception:
                            pass
            except Exception:
                pass
            return str(backup_path)
        except Exception as e:
            print(e)
            return None

    # Restore DB from a given backup file path
    def restore_db_from(self, backup_path: str):
        try:
            src = Path(backup_path)
            if not src.exists():
                return False
            # Replace current DB with backup
            shutil.copy2(src, self.db)
            return True
        except Exception as e:
            print(e)
            return False

    # --- Change-detection helpers removed: fragments auto-refresh panels ---

    def create_tables(self):
        sql_statements = [ 
            """CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    income REAL NOT NULL,
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
            with self._connect() as conn:
                cursor = conn.cursor()
                for statement in sql_statements:
                    cursor.execute(statement)
                conn.commit()
                # Bugfix old database
                # Check if the 'side' column exists in the 'position' table
                cursor.execute("PRAGMA table_info(position);")
                columns = [column[1] for column in cursor.fetchall()]
                if 'side' not in columns:
                    # Add the 'side' column if it does not exist
                    cursor.execute("ALTER TABLE position ADD COLUMN side TEXT;")
                    conn.commit()
                    # Update existing records in the 'position' table to set 'side' to 'long'
                    cursor.execute("UPDATE position SET side = 'long';")
                    conn.commit()
                # Improve concurrency across multiple writers
                try:
                    cursor.execute('PRAGMA journal_mode=WAL')
                    cursor.execute('PRAGMA synchronous=NORMAL')
                    conn.commit()
                except Exception:
                    pass
                # Create indexes to speed up frequent WHERE queries and avoid full-table scans
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_position_user ON position(user)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_user ON prices(user)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol_user ON prices(symbol, user)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_user_ts ON history(user, timestamp)")
                    conn.commit()
                except Exception:
                    pass
        except sqlite3.Error as e:
            self._log(f"DB create_tables error: {e}")

    def close_thread_connections(self):
        """Close any cached per-thread SQLite connection(s).

        This is safe to call from shutdown code to release file descriptors
        and ensure rotated/deleted files are freed promptly.
        """
        try:
            local = getattr(self, '_local', None)
            if local is None:
                return
            conn = getattr(local, 'conn', None)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    delattr(local, 'conn')
                except Exception:
                    pass
        except Exception:
            pass

    def update_history(self, user: User):
        self._log(f"update_history called for user={getattr(user, 'name', user)}")
        history = self.fetch_history(user)
        try:
            if history is None:
                self._log(f"fetch_history returned None for user={user.name}")
            else:
                self._log(f"fetch_history returned {len(history)} items for user={user.name}")
                if len(history) > 0:
                    try:
                        self._log(f"fetch_history sample[0] for {user.name}: {history[0]}")
                    except Exception:
                        pass
        except Exception:
            pass
        # Only hold the global write lock while actually writing rows to the
        # DB so that long-running REST history fetches don't block price
        # updates and other quick writes.
        try:
            if history:
                with self._write_lock:
                    with self._connect() as conn:
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
            self._log(f"DB update_history error for {user.name}: {e}")
    
    def update_positions(self, user: User):
        positions_db = self.fetch_positions(user)
        exchange = Exchange(user.exchange, user)
        positions = exchange.fetch_positions()

        # Live positions as set of (symbol, side) for correct membership checks
        symbols = set()
        for position in positions:
            if position['contracts'] == 0:
                continue
            sym = position['symbol'][0:-5].replace("/", "").replace("-", "")
            side = position['side']
            symbols.add((sym, side))

        # DB positions: ensure at most one row per (symbol, side) by
        # detecting duplicates and marking them for removal. We also
        # build the symbols_db set from the deduplicated view.
        symbols_db = set()
        latest_by_key = {}
        duplicate_ids = []
        for row in positions_db:
            key = (row[1], row[7])  # (symbol, side)
            if key in latest_by_key:
                # Older duplicate; mark for deletion
                duplicate_ids.append(row[0])  # id column
            else:
                latest_by_key[key] = row
                symbols_db.add(key)

        # Build quick lookup of live sides by symbol for debugging
        live_sides_by_symbol = {}
        for sym, side in symbols:
            live_sides_by_symbol.setdefault(sym, set()).add(side)

        with self._write_lock:
            try:
                with self._connect() as conn:
                    # First remove any duplicate DB rows for the same
                    # (symbol, side); keep only the latest row per key.
                    for dup_id in duplicate_ids:
                        self._log(f"Removing duplicate position id={dup_id} for {user.name}")
                        self.remove_position(conn, dup_id)

                    # Remove positions that are not in the exchange
                    for position in latest_by_key.values():
                        if (position[1], position[7]) not in symbols:
                            self._log(f"[positions] Removing position for user={user.name} symbol={position[1]} side={position[7]}")
                            self.remove_position(conn, position[0])

                    # Update positions
                    for position in positions:
                        upnl = position['unrealizedPnl']
                        if upnl is None:
                            upnl = 0.0
                        pos = [
                            position['timestamp'],
                            position['contracts'] * position['contractSize'],
                            upnl,
                            position['entryPrice'],
                            position['symbol'][0:-5].replace("/", "").replace("-", ""),
                            user.name,
                            position['side']
                        ]
                        if pos[1] == 0:
                            continue
                        # Use current timestamp if timestamp is None
                        if not pos[0]:
                            pos[0] = int(datetime.now().timestamp() * 1000)
                        key = (pos[4], pos[6])
                        if key in symbols_db:
                            self._log(f"[positions] Updating position for user={user.name} symbol={pos[4]} side={pos[6]}")
                            self.update_position(conn, pos)
                        else:
                            self._log(f"[positions] Adding position for user={user.name} symbol={pos[4]} side={pos[6]}")
                            self.add_position(conn, pos)
                            # Ensure we do not create multiple rows for the
                            # same (symbol, side) within one update run.
                            symbols_db.add(key)
            except sqlite3.Error as e:
                self._log(f"DB update_positions error for {user.name}: {e}")
    
    def update_orders(self, user: User):
        positions_db = self.fetch_positions(user)
        orders_db = self.fetch_orders(user)
        exchange = Exchange(user.exchange, user)
        all_orders = []
        for position in positions_db:
            try:
                stable_coin = position[1][-4:]
                orders = exchange.fetch_all_open_orders(position[1][0:-4] + f"/{stable_coin}:{stable_coin}")
                # If fetch returns None or an exception-like value, skip
                if orders is None:
                    continue
                all_orders.extend(orders)
            except Exception as e:
                # Fetch failed (possibly rate limit) — log and skip this position
                self._log(f"DB update_orders fetch_all_open_orders failed for {user.name} pos={position[1]}: {e}", level='WARNING')
                continue
        # Existing order IDs in DB (uniqueid column); use a set to avoid
        # inserting duplicates even if the DB uniqueness constraint is
        # missing or was added after the table was first created.
        ids_db = {order[6] for order in orders_db}
        ids = [order['id'] for order in all_orders]
        with self._write_lock:
            try:
                with self._connect() as conn:
                    # Remove orders that are not in the exchange
                    for order in orders_db:
                        if order[6] not in ids:
                            self._log(f"Removing order {order[6]} for user {user.name}")
                            self.remove_order(conn, order[0])
                    # Update orders
                    for order in all_orders:
                        uniqueid = order['id']
                        ord = [
                            order['timestamp'],
                            order['amount'],
                            order['price'],
                            order['side'],
                            uniqueid,
                            order['symbol'][0:-5].replace("/", "").replace("-", ""),
                            user.name
                        ]
                        if uniqueid in ids_db:
                            self._log(f"Updating order {uniqueid} for user {user.name}")
                            self.update_order(conn, ord)
                        else:
                            self._log(f"Adding order {uniqueid} for user {user.name}")
                            self.add_order(conn, ord)
                            # Ensure we don't insert the same order twice in
                            # a single update run if the exchange returns
                            # duplicates.
                            ids_db.add(uniqueid)
            except sqlite3.Error as e:
                self._log(f"DB update_orders error for {user.name}: {e}")

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
            if symbol[-4:] == "USDT":
                symbol_ccxt = f'{symbol[0:-4]}/USDT:USDT'
            elif symbol[-4:] == "USDC":
                symbol_ccxt = f'{symbol[0:-4]}/USDC:USDC'
            symbols.append(symbol_ccxt)
        if symbols:
            market_type = "futures"
            prices = exchange.fetch_prices(symbols, market_type)
        symbols = []
        for symbol_ccxt in prices:
            symbol = symbol_ccxt[0:-5].replace("/", "").replace("-", "")
            symbols.append(symbol)
        with self._write_lock:
            try:
                with self._connect() as conn:
                    # Remove symbols that are not in the exchange
                    for symbol in symbols_db:
                        if symbol not in symbols:
                            self._log(f"Removing price {symbol} for {user.name}")
                            self.remove_price(conn, symbol, user.name)
                    # Update prices
                    for symbol in symbols:
                        if symbol[-4:] == "USDT":
                            symbol_ccxt = f'{symbol[0:-4]}/USDT:USDT'
                        elif symbol[-4:] == "USDC":
                            symbol_ccxt = f'{symbol[0:-4]}/USDC:USDC'
                        timestamp = prices[symbol_ccxt]['timestamp']
                        if not timestamp:
                            timestamp = exchange.fetch_timestamp()
                        price = [
                            timestamp,
                            prices[symbol_ccxt]['last'],
                            symbol,
                            user.name
                        ]
                        if symbol in symbols_db:
                            self._log(f"Updating price {symbol} for {user.name}")
                            self.update_price(conn, price)
                        else:
                            self._log(f"Adding price {symbol} for {user.name}")
                            self.add_price(conn, price)
            except sqlite3.Error as e:
                self._log(f"DB update_prices error for {user.name}: {e}")

    def update_balances(self, user: User):
        exchange = Exchange(user.exchange, user)
        market_type = "swap"
        try:
            balance = exchange.fetch_balance(market_type)
        except Exception as e:
            # Exchange fetch failed (rate limit or other). Log and skip writing.
            self._log(f"DB update_balances fetch_balance failed for {user.name}: {e}", level='WARNING')
            return
        with self._write_lock:
            try:
                with self._connect() as conn:
                    balance_list = [
                        int(datetime.now().timestamp() * 1000),
                        balance,
                        user.name
                    ]
                    self._log(f"Updating balance {user.name}")
                    self.update_balance(conn, balance_list)
            except sqlite3.Error as e:
                self._log(f"DB update_balances error for {user.name}: {e}")

    def add_history(self, conn: sqlite3.Connection, history: list):
        sql = '''INSERT INTO history(symbol,timestamp,income,uniqueid,user)
                VALUES(?,?,?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, history)
            conn.commit()
        except sqlite3.Error as e:
            # Ignore duplicate uniqueid (already stored), log others
            msg = str(e).lower()
            if 'unique constraint failed' in msg and 'history.uniqueid' in msg:
                return
            self._log(f"DB add_history error {e} data={history}")
    
    def add_position(self, conn: sqlite3.Connection, position: list):
        sql = '''INSERT INTO position(timestamp,psize,upnl,entry,symbol,user,side)
                VALUES(?,?,?,?,?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, position)
            conn.commit()
        except sqlite3.Error as e:
            self._log(f"DB add_position error {e} data={position}")
        return cur.lastrowid

    def add_order(self, conn: sqlite3.Connection, order: list):
        sql = '''INSERT INTO orders(timestamp,amount,price,side,uniqueid,symbol,user)
                VALUES(?,?,?,?,?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, order)
            conn.commit()
        except sqlite3.Error as e:
            self._log(f"DB add_order error {e} data={order}")

    def add_price(self, conn: sqlite3.Connection, price: list):
        sql = '''INSERT INTO prices(timestamp,price,symbol,user)
                VALUES(?,?,?,?) '''
        try:
            cur = conn.cursor()
            cur.execute(sql, price)
            conn.commit()
        except sqlite3.Error as e:
            self._log(f"DB add_price error {e} data={price}")

    def remove_position(self, conn: sqlite3.Connection, id: int):
        sql = '''DELETE FROM position WHERE id = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, [id])
            conn.commit()
        except sqlite3.Error as e:
            self._log(f"DB remove_position error {e} id={id}")
    
    def remove_order(self, conn: sqlite3.Connection, id: int):
        sql = '''DELETE FROM orders WHERE id = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, [id])
            conn.commit()
        except sqlite3.Error as e:
            self._log(f"DB remove_order error {e} id={id}")

    def remove_price(self, conn: sqlite3.Connection, symbol: str, user: str):
        sql = '''DELETE FROM prices WHERE symbol = ? AND user = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, [symbol, user])
            conn.commit()
        except sqlite3.Error as e:
            self._log(f"DB remove_price error {e} symbol={symbol} user={user}")

    def update_position(self, conn: sqlite3.Connection, position: list):
        sql = '''UPDATE position
                SET timestamp = ?,
                    psize = ?,
                    upnl = ?,
                    entry = ?
                WHERE symbol = ? AND user = ? AND side = ? '''
        try:
            cur = conn.cursor()
            cur.execute(sql, position)
            conn.commit()
        except sqlite3.Error as e:
            self._log(f"DB update_position error {e} data={position}")

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
            self._log(f"DB update_order error {e} data={order}")

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
            self._log(f"DB update_price error {e} data={price}")

    def upsert_price(self, user: User, symbol: str, timestamp: int, price_value: float):
        """Insert or update a single price tick for (user, symbol)."""
        start_ts = datetime.now().timestamp()
        with self._write_lock:
            attempts = 0
            while True:
                try:
                    with self._connect() as conn:
                        cur = conn.cursor()
                        cur.execute('SELECT id FROM prices WHERE symbol = ? AND user = ?', (symbol, user.name))
                        exists = cur.fetchone() is not None
                        price_row = [timestamp, price_value, symbol, user.name]
                        if exists:
                            self.update_price(conn, price_row)
                        else:
                            self.add_price(conn, price_row)
                    break
                except sqlite3.OperationalError as e:
                    if 'database is locked' in str(e).lower() and attempts < 5:
                        attempts += 1
                        time.sleep(0.05 * attempts)
                        continue
                    self._log(f"DB upsert_price error {e} user={user.name} symbol={symbol} price={price_value} attempts={attempts}")
                    break
                except sqlite3.Error as e:
                    self._log(f"DB upsert_price error {e} user={user.name} symbol={symbol} price={price_value} attempts={attempts}")
                    break

    def batch_upsert_prices(self, rows: list):
        """Batch upsert a list of price rows.

        rows: iterable of (user, symbol, timestamp, price)
        This uses a single connection and transaction to reduce locking and
        improve throughput compared to many per-row connections.
        """
        if not rows:
            return
        with self._write_lock:
            attempts = 0
            while True:
                try:
                    with self._connect() as conn:
                        cur = conn.cursor()
                        for user, symbol, timestamp, price in rows:
                            try:
                                cur.execute('UPDATE prices SET timestamp = ?, price = ? WHERE symbol = ? AND user = ?', (timestamp, price, symbol, user))
                                if cur.rowcount == 0:
                                    cur.execute('INSERT INTO prices(timestamp,price,symbol,user) VALUES(?,?,?,?)', (timestamp, price, symbol, user))
                            except sqlite3.Error as e:
                                # Log and continue with other rows
                                self._log(f"DB batch_upsert_prices row error {e} user={user} symbol={symbol} price={price}")
                        conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if 'database is locked' in str(e).lower() and attempts < 5:
                        attempts += 1
                        time.sleep(0.05 * attempts)
                        continue
                    self._log(f"DB batch_upsert_prices error {e} attempts={attempts}")
                    break
                except sqlite3.Error as e:
                    self._log(f"DB batch_upsert_prices error {e} attempts={attempts}")
                    break

    def update_balance(self, conn: sqlite3.Connection, balance: list):
        sql = '''INSERT OR REPLACE INTO balances(timestamp,balance,user)
                VALUES(?,?,?) '''
        try:
            if not balance or len(balance) < 3:
                self._log(f"DB update_balance called with invalid balance data: {balance}")
                return
            # Defensive checks: avoid writing Exception objects or other
            # unserializable types into the DB which cause SQLite bind errors.
            # If the balance payload contains an Exception, skip and warn.
            try:
                payload = balance[1]
            except Exception:
                payload = None
            if isinstance(payload, Exception):
                self._log(f"DB update_balance aborting: balance fetch returned exception object for user={balance[2]}: {payload}", level='WARNING')
                return
            # Try to coerce numeric-like payloads to float; otherwise stringify
            try:
                if not isinstance(payload, (int, float)):
                    balance[1] = float(payload)
            except Exception:
                try:
                    balance[1] = str(payload)
                    self._log(f"DB update_balance coerced non-numeric balance to string for user={balance[2]}", level='WARNING')
                except Exception:
                    self._log(f"DB update_balance aborting: cannot coerce balance for user={balance[2]}: {payload}", level='WARNING')
                    return
            cur = conn.cursor()
            cur.execute(sql, balance)

            # Robust cleanup: keep only the newest row (by timestamp, then by id)
            # for this user and delete any other rows. This handles cases where
            # the DB schema did not previously enforce UNIQUE(user) or when
            # older duplicate rows exist from imports/migrations.
            try:
                user = str(balance[2])
                cleanup_sql = '''
                DELETE FROM balances
                WHERE user = ?
                  AND id NOT IN (
                    SELECT id FROM balances WHERE user = ? ORDER BY timestamp DESC, id DESC LIMIT 1
                  )
                '''
                cur.execute(cleanup_sql, (user, user))
                removed = cur.rowcount
                if removed and removed > 0:
                    self._log(f"DB update_balance removed {removed} older balances for user={user}")
            except sqlite3.Error as e:
                # Log but don't raise; allow normal flow to continue
                self._log(f"DB update_balance cleanup error {e} user={balance[2]}")

            conn.commit()
        except sqlite3.Error as e:
            msg = str(e).lower()
            if 'binding parameter' in msg or 'unsupported type' in msg:
                self._log(f"DB update_balance error {e} data={balance}", level='WARNING')
            else:
                self._log(f"DB update_balance error {e} data={balance}")

    def fetch_history(self, user: User):
        """Fetch history from the exchange starting at the DB's last timestamp."""
        exchange = Exchange(user.exchange, user)
        try:
            since = self.find_last_timestamp(user) or 0
        except Exception:
            since = 0
        self._log(f"fetch_history: user={user.name} exchange={user.exchange} since={since} (type={type(since)})")
        # Time the exchange.fetch_history call to help diagnose slow history polls.
        try:
            start_ts = time.time()
            history = exchange.fetch_history(int(since))
            dur = time.time() - start_ts
            try:
                length = len(history) if history is not None else 0
            except Exception:
                length = 0
            self._log(f"fetch_history DONE: user={user.name} exchange={user.exchange} duration_s={dur:.3f} items={length}")
            return history
        except Exception as e:
            # Do not swallow exceptions here — re-raise so callers (and tests)
            # can inspect the full traceback and we can debug exchange.fetch_history.
            self._log(f"fetch_history ERROR for user={user.name} exchange={user.exchange}: {e}")
            raise

    def fetch_positions(self, user: User):
        sql = '''SELECT * FROM "position"
                WHERE "position"."user" = ? '''
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, [user.name])
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB fetch_positions error {e} user={user.name}")

    def fetch_orders(self, user: User):
        sql = '''SELECT * FROM "orders"
                WHERE "orders"."user" = ? '''
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, [user.name])
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB fetch_orders error {e} user={user.name}")
    
    def fetch_orders_by_symbol(self, user: str, symbol: str):
        sql = '''SELECT * FROM "orders"
                WHERE "orders"."user" = ?
                    AND "orders"."symbol" = ? '''
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, [user, symbol])
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB fetch_orders_by_symbol error {e} user={user} symbol={symbol}")

    def fetch_prices(self, user: User):
        sql = '''SELECT * FROM "prices"
                WHERE "prices"."user" = ? '''
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, [user.name])
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB fetch_prices error {e} user={user.name}")

    def fetch_balances(self, user: list):
        sql = '''SELECT * FROM "balances"
                WHERE "balances"."user" IN ({}) '''.format(','.join('?'*len(user)))
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, user)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB fetch_balances error {e} users={user}")

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
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB select_top error {e}")
        
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
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB select_pnl error {e}")
    
    def select_ppl(self, user: list, start: str, end: str, sum_period: str):
    # Define date formats for different sum_period values
        date_formats = {
            'DAY': "'%Y-%m-%d'",
            'WEEK': "'%Y-%W'",
            'MONTH': "'%Y-%m'",
            'YEAR': "'%Y'",
        }

        if sum_period == 'ALL_TIME':
            select_period = "'ALL_TIME' AS period"
            group_by_clause = ''
        else:
            date_format = date_formats.get(sum_period, "'%Y-%m-%d'")
            select_period = f"strftime({date_format}, \"timestamp\" / 1000, 'unixepoch') AS period"
            group_by_clause = 'GROUP BY period'

        if 'ALL' in user:
            sql = f'''
            SELECT
                {select_period},
                SUM(CASE WHEN "income" >= 0 THEN "income" ELSE 0 END) AS "sum_positive",
                SUM(CASE WHEN "income" < 0 THEN "income" ELSE 0 END) AS "sum_negative"
            FROM "history"
            WHERE "history"."timestamp" >= ?
                AND "history"."timestamp" <= ?
            {group_by_clause}
            '''
            sql_parameters = (start, end)
        else:
            placeholders = ','.join('?' * len(user))
            sql = f'''
            SELECT
                {select_period},
                SUM(CASE WHEN "income" >= 0 THEN "income" ELSE 0 END) AS "sum_positive",
                SUM(CASE WHEN "income" < 0 THEN "income" ELSE 0 END) AS "sum_negative"
            FROM "history"
            WHERE "history"."user" IN ({placeholders})
                AND "history"."timestamp" >= ?
                AND "history"."timestamp" <= ?
            {group_by_clause}
            '''
            sql_parameters = tuple(user) + (start, end)
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB select_ppl error {e}")

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
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB select_income error {e}")
    
    # select income grouped by symbol not sum
    def select_income_by_symbol(self, user: list, start: str, end: str):
        if 'ALL' in user:
            sql = '''SELECT "timestamp", "symbol", "income", "user" FROM "history"
                    WHERE "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    ORDER BY "timestamp" ASC '''
            sql_parameters = (start, end)
        else:
            sql = '''SELECT "timestamp", "symbol", "income", "user" FROM "history"
                    WHERE "history"."user" IN ({})
                        AND "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    ORDER BY "timestamp" ASC'''.format(','.join('?'*len(user)))
            sql_parameters = tuple(user) + (start, end)
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB select_income_by_symbol error {e}")

    # Same as select_income_by_symbol but includes row id for deletion mapping
    def select_income_by_symbol_with_id(self, user: list, start: str, end: str):
        if 'ALL' in user:
            sql = '''SELECT "id", "timestamp", "symbol", "income", "user" FROM "history"
                    WHERE "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    ORDER BY "timestamp" ASC '''
            sql_parameters = (start, end)
        else:
            sql = '''SELECT "id", "timestamp", "symbol", "income", "user" FROM "history"
                    WHERE "history"."user" IN ({})
                        AND "history"."timestamp" >= ?
                        AND "history"."timestamp" <= ?
                    ORDER BY "timestamp" ASC'''.format(','.join('?'*len(user)))
            sql_parameters = tuple(user) + (start, end)
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, sql_parameters)
                rows = cur.fetchall()
                return rows
        except sqlite3.Error as e:
            self._log(f"DB select_income_by_symbol_with_id error {e}")

    # Delete specific income rows by primary key ids
    def delete_income_by_ids(self, ids: list):
        if not ids:
            return 0
        placeholders = ','.join('?' * len(ids))
        sql = f'''DELETE FROM history WHERE id IN ({placeholders})'''
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, ids)
                conn.commit()
                return cur.rowcount
        except sqlite3.Error as e:
            self._log(f"DB delete_income_by_ids error {e}")
            return 0

    # Delete all income rows for a user older than or equal to a timestamp (ms)
    def delete_income_older_than_user(self, user: str, timestamp_ms: int):
        sql = '''DELETE FROM history WHERE user = ? AND timestamp <= ?'''
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, (user, int(timestamp_ms)))
                conn.commit()
                return cur.rowcount
        except sqlite3.Error as e:
            self._log(f"DB delete_income_older_than_user error {e}")
            return 0

    # Delete all income rows older than or equal to a timestamp for given users list.
    # If users contains 'ALL', deletes across all users.
    def delete_income_older_than(self, users: list, timestamp_ms: int):
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                if not users or 'ALL' in users:
                    sql = 'DELETE FROM history WHERE timestamp <= ?'
                    cur.execute(sql, (int(timestamp_ms),))
                else:
                    placeholders = ','.join('?' * len(users))
                    sql = f'DELETE FROM history WHERE user IN ({placeholders}) AND timestamp <= ?'
                    cur.execute(sql, tuple(users) + (int(timestamp_ms),))
                conn.commit()
                return cur.rowcount
        except sqlite3.Error as e:
            self._log(f"DB delete_income_older_than error {e}")
            return 0

    def find_last_timestamp(self, user: User):
        sql = '''SELECT MAX("history"."timestamp") FROM "history"
                WHERE "history"."user" = ? '''
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(sql, [user.name])
                rows = cur.fetchall()
                if rows[0][0] is None:
                    return 0
                return rows[0][0]
        except sqlite3.Error as e:
            self._log(f"DB find_last_timestamp error {e} user={user.name}")

    def fetch_history2(self, user: User):
        exchange = Exchange(user.exchange, user)
        return exchange.fetch_transactions(1724390528161)

    def fetch_futures(self, user: User):
        exchange = Exchange(user.exchange, user)
        return exchange.fetch_futures(1724390528161)
    
    def import_from_save_income_other(self, user: User):
        # Load data from file
        data = []
        src = Path(f'{PBGDIR}/data/logs')
        with open(f'{src}/income_other_{user.name}.json', 'r') as file:
            data = file.read()
            data = '[' + data.replace('}{', '},{') + ']'
            for item in json.loads(data):
                if item['incomeType'] in ['COMMISSION', 'FUNDING_FEE']:
                    try:
                        with self._connect() as conn:
                            income = [
                                item['symbol'],
                                item['time'],
                                item['income'],
                                item['tranId'],
                                user.name
                            ]
                            self.add_history(conn, income)
                    except sqlite3.Error as e:
                        self._log(f"DB import_from_save_income_other error {e}")
                else:
                    print("not import")
                    print(item)

def main():
    print("Don't Run this Class from CLI")
    # users = Users()
    # user = users.find_user("hl_mani_crash_hunter")
    # db = Database()
    # db.fetch_history(user)
    # db.import_from_save_income_other(user)
    # exchange = Exchange("gateio", user)
    # history = exchange.fetch_history()
    # print(history)
    # balance = exchange.fetch_balance("swap")
    # print(balance)
    # positions = exchange.fetch_positions()
    # print(positions)
    # db = Database()
    # db.update_history(user)
    # db.update_positions(user)
    # db.update_orders(user)
    # db.update_prices(user)
    # db.update_balances(user)
    # exchange.connect()
    # print(db.find_last_timestamp(user))
    # history = db.fetch_history2(user)
    # print(history)
    # history = db.fetch_history2(user2)
    # print(history)
    # balances = db.fetch_balances(['gateio_cpt'])
    # print(balances)
    # db.update_positions(user)
    # db.update_prices(user)
    # db.update_balances(user)

if __name__ == '__main__':
    main()
