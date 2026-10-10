"""Price snapshot connection ownership and maintenance admission on temporary SQLite."""

import ast
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from database_lock import acquire_database_lock, DatabaseBusyError


@pytest.fixture
def snapshot(tmp_path, monkeypatch):
    """Load the actual route body without service startup or credential-store access."""
    source = Path(__file__).resolve().parents[1] / 'api/services.py'
    tree = ast.parse(source.read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'get_prices_snapshot')
    node.decorator_list = []
    node.args.defaults = [ast.Constant(None)]
    builder = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_build_prices_snapshot')
    future = ast.parse('from __future__ import annotations').body
    namespace = {'Path': Path, 'PBGDIR': tmp_path, 'HTTPException': HTTPException,
                 '_log': Mock(), 'SERVICE': 'Services', 'time': time,
                 '_prices_snapshot_cache': {}, '_prices_snapshot_lock': threading.Lock(),
                 '_PRICES_SNAPSHOT_CACHE_TTL_S': 3.0,
                 '_fetch_summary_snapshot': {'prices': {'bybit': {'symbols': 1, 'symbol_list': ['BTCUSDT']}}}}
    exec(compile(ast.fix_missing_locations(ast.Module(body=future + [node, builder], type_ignores=[])), str(source), 'exec'), namespace)

    class Users:
        """Supply harmless account-to-exchange metadata in memory."""

        def load(self):
            """No credential files are needed."""

        def __iter__(self):
            """Return two accounts on the same exchange."""
            return iter([SimpleNamespace(name=name, exchange='bybit') for name in ['a', 'b']])

    monkeypatch.setitem(sys.modules, 'User', SimpleNamespace(Users=Users))
    path = tmp_path / 'data/pbgui.db'
    path.parent.mkdir()
    with closing(sqlite3.connect(path)) as conn:
        conn.execute('CREATE TABLE prices(symbol TEXT, user TEXT, price REAL, timestamp INTEGER)')
        conn.executemany('INSERT INTO prices VALUES(?,?,?,?)', [('BTCUSDT','a',10,100), ('BTCUSDT','b',12,200), ('ETHUSDT','a',5,300)])
        conn.commit()
    return SimpleNamespace(read=namespace['get_prices_snapshot'], root=tmp_path, path=path, namespace=namespace)


@pytest.mark.parametrize('query_error', [False, True])
def test_connection_closed_before_maintenance_lease_released(snapshot, monkeypatch, query_error):
    """Success and SQL failure close the connection while the shared lease is held."""
    if query_error:
        with closing(sqlite3.connect(snapshot.path)) as conn:
            conn.execute('DROP TABLE prices')
            conn.commit()
    original = sqlite3.connect
    connections = []
    closed = []

    class TrackedConnection(sqlite3.Connection):
        """Check maintenance cannot start until the SQLite handle is closed."""

        def close(self):
            """The lease must still block exclusive admission at close time."""
            with pytest.raises(DatabaseBusyError):
                acquire_database_lock(snapshot.root, exclusive=True)
            closed.append(self)
            super().close()

    def connect(*args, **kwargs):
        """Assert admission precedes opening a read-only database connection."""
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(snapshot.root, exclusive=True)
        assert kwargs['uri'] is True
        assert args[0].endswith('?mode=ro')
        conn = original(*args, **kwargs, factory=TrackedConnection)
        connections.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, 'connect', connect)
    if query_error:
        with pytest.raises(HTTPException) as error:
            snapshot.read()
        assert error.value.status_code == 500
    else:
        for _ in range(3):
            assert snapshot.read() == {'rows': [{'symbol': 'BTCUSDT', 'exchange': 'bybit', 'price': 12.0, 'ts': 200}]}
    assert closed == connections
    assert connections
    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            conn.execute('SELECT 1')
    with acquire_database_lock(snapshot.root, exclusive=True):
        pass


def test_exclusive_maintenance_returns_conflict_before_connect(snapshot, monkeypatch):
    """Restore admission prevents a snapshot from opening a connection."""
    connect = Mock(side_effect=AssertionError('must not connect'))
    monkeypatch.setattr(sqlite3, 'connect', connect)
    with acquire_database_lock(snapshot.root, exclusive=True):
        with pytest.raises(HTTPException) as error:
            snapshot.read()
    assert error.value.status_code == 409
    connect.assert_not_called()


def test_missing_database_is_not_created(snapshot):
    """An absent database retains the empty response without recreating a file."""
    snapshot.path.unlink()
    assert snapshot.read() == {'rows': []}
    assert not snapshot.path.exists()


def test_legacy_top_n_snapshot(snapshot):
    """Legacy summaries without symbol lists retain their top-N query behavior."""
    snapshot.namespace['_fetch_summary_snapshot'] = {'prices': {'bybit': {'symbols': 1}}}
    assert snapshot.read() == {'rows': [{'symbol': 'ETHUSDT', 'exchange': 'bybit', 'price': 5.0, 'ts': 300}]}


def test_cache_reuses_reads_until_expiry(snapshot, monkeypatch):
    """Warm requests avoid both credential loading and SQLite until the TTL expires."""
    clock = SimpleNamespace(value=10.0)
    snapshot.namespace['time'] = SimpleNamespace(monotonic=lambda: clock.value)
    users = sys.modules['User'].Users
    load = Mock()
    monkeypatch.setattr(users, 'load', load)
    original = sqlite3.connect
    connect = Mock(wraps=original)
    monkeypatch.setattr(sqlite3, 'connect', connect)
    first = snapshot.read()
    clock.value = 12.99
    assert snapshot.read() == first
    assert load.call_count == connect.call_count == 1
    with closing(original(snapshot.path)) as conn:
        conn.execute("UPDATE prices SET price=20, timestamp=400 WHERE symbol='BTCUSDT' AND user='b'")
        conn.commit()
    clock.value = 13.0
    assert snapshot.read()['rows'][0]['price'] == 20
    assert load.call_count == connect.call_count == 2


def test_changed_active_symbols_invalidate_cache(snapshot):
    """A new summary cannot serve the previously active symbol list from cache."""
    assert snapshot.read()['rows'][0]['symbol'] == 'BTCUSDT'
    snapshot.namespace['_fetch_summary_snapshot'] = {'prices': {'bybit': {'symbols': 1, 'symbol_list': ['ETHUSDT']}}}
    assert snapshot.read()['rows'][0]['symbol'] == 'ETHUSDT'
    snapshot.namespace['_fetch_summary_snapshot'] = {}
    assert snapshot.read() == {'rows': []}


def test_cache_coalesces_concurrent_readers(snapshot):
    """Simultaneous callers build only one snapshot and release all waiting threads."""
    from concurrent.futures import ThreadPoolExecutor

    barrier = threading.Barrier(6)
    build = Mock(wraps=snapshot.namespace['_build_prices_snapshot'])
    snapshot.namespace['_build_prices_snapshot'] = build

    def read():
        """Start the callers together to exercise the cache's ownership lock."""
        barrier.wait(timeout=5)
        return snapshot.read()

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: read(), range(6)))
    assert all(result == results[0] for result in results)
    assert build.call_count == 1


def test_failed_read_is_not_cached(snapshot, monkeypatch):
    """A maintenance conflict is retried and does not poison the cache."""
    with acquire_database_lock(snapshot.root, exclusive=True):
        with pytest.raises(HTTPException) as error:
            snapshot.read()
    assert error.value.status_code == 409
    assert snapshot.namespace['_prices_snapshot_cache'] == {}
    assert snapshot.read()['rows']
