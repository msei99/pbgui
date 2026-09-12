"""Price snapshot connection ownership and maintenance admission on temporary SQLite."""

import ast
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
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
    future = ast.parse('from __future__ import annotations').body
    namespace = {'Path': Path, 'PBGDIR': tmp_path, 'HTTPException': HTTPException,
                 '_log': Mock(), 'SERVICE': 'Services',
                 '_fetch_summary_snapshot': {'prices': {'bybit': {'symbols': 1, 'symbol_list': ['BTCUSDT']}}}}
    exec(compile(ast.fix_missing_locations(ast.Module(body=future + [node], type_ignores=[])), str(source), 'exec'), namespace)

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
