"""Isolated SQLite and balance endpoint regressions for issue #499."""

import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Database as database_module
from api import dashboard


@pytest.fixture
def position_db(monkeypatch):
    """Use only an in-memory database, including the migrated side column."""
    db = object.__new__(database_module.Database)
    with closing(sqlite3.connect(':memory:')) as conn:
        conn.execute('CREATE TABLE position (id INTEGER PRIMARY KEY, symbol TEXT, timestamp INTEGER, '
                     'psize REAL, upnl REAL, entry REAL, user TEXT)')
        conn.execute('ALTER TABLE position ADD COLUMN side TEXT')
        db._local = SimpleNamespace(conn=conn)
        connect = Mock(wraps=db._connect)
        monkeypatch.setattr(db, '_connect', connect)
        yield db, conn, connect


def _insert_position(conn, name, size=1.0, upnl=2.345):
    """Insert a position into the synthetic database using bound values."""
    conn.execute('INSERT INTO position(symbol,timestamp,psize,upnl,entry,user,side) '
                 'VALUES(?,?,?,?,?,?,?)', ('ETHUSDT', 0, size, upnl, 100.0, name, None))


@pytest.mark.parametrize('count', [1, 5, 10])
def test_batch_positions_use_one_query(position_db, count):
    """Selected accounts require one position SELECT and retain complete tuples."""
    db, conn, connect = position_db
    names = [f'user_{i}' for i in range(count)]
    for name in names:
        _insert_position(conn, name)
        _insert_position(conn, name, -2.0)
    _insert_position(conn, 'unselected')
    expected = conn.execute('SELECT * FROM position WHERE user != ?', ('unselected',)).fetchall()
    queries = []
    conn.set_trace_callback(queries.append)
    result = db.fetch_positions_for_users(names + names + ['missing'])
    assert result == {name: [row for row in expected if row[6] == name] for name in names}
    assert all(isinstance(row, tuple) and len(row) == 8 for rows in result.values() for row in rows)
    assert result.get('missing', []) == []
    assert len([sql for sql in queries if sql.startswith('SELECT * FROM "position"')]) == 1
    assert len([sql for sql in queries if sql == 'SELECT 1']) == 1
    connect.assert_called_once_with()


def test_empty_batch_never_connects(position_db):
    """An empty selection returns immediately without opening any database."""
    db, _, connect = position_db
    assert db.fetch_positions_for_users([]) == {}
    connect.assert_not_called()


def test_batch_binds_names(position_db):
    """Quotes and SQL-like names match literally without broadening the query."""
    db, conn, _ = position_db
    name = "alice') OR 1=1 --"
    _insert_position(conn, name)
    _insert_position(conn, 'other')
    result = db.fetch_positions_for_users([name, name])
    assert list(result) == [name]
    assert len(result[name]) == 1
    assert conn.execute('SELECT count(*) FROM position').fetchone()[0] == 2


def test_batch_sqlite_failure_is_logged(position_db, monkeypatch):
    """Database errors retain the empty-result fallback and are logged."""
    db, conn, _ = position_db
    conn.execute('DROP TABLE position')
    log = Mock()
    monkeypatch.setattr(database_module, '_human_log', log)
    assert db.fetch_positions_for_users(['alice']) == {}
    log.assert_called_once()
    assert log.call_args.kwargs['level'] == 'ERROR'
    assert 'fetch_positions_for_users' in log.call_args.args[1]


@pytest.fixture
def balance_data(monkeypatch):
    """Block runtime access and provide deterministic account snapshots."""
    accounts = {name: SimpleNamespace(name=name) for name in ('alice', 'bob', 'empty')}
    balances = [(2, 1000, 500.0, 'bob'), (1, 0, 1000.0, 'alice'), (3, 0, 0.0, 'empty')]
    positions = {
        'alice': [(1, 'ETHUSDT', 0, 1.0, 2.345, 100.0, 'alice', None)],
        'bob': [(2, 'ETHUSDT', 0, -2.0, -1.234, 100.0, 'bob', None)],
    }
    db = SimpleNamespace(
        fetch_balances=Mock(side_effect=lambda names: [row for row in balances if row[3] in names]),
        fetch_positions_for_users=Mock(side_effect=lambda names: {n: positions[n] for n in names if n in positions}),
        fetch_positions=Mock(side_effect=AssertionError('Per-account position query')),
    )
    monkeypatch.setattr(dashboard, '_get_db', lambda: db)
    monkeypatch.setattr(dashboard, '_get_users', lambda: SimpleNamespace(
        list=lambda: list(accounts), find_user=lambda name: accounts.get(name)))
    monkeypatch.setattr(dashboard, '_get_exchange', Mock(side_effect=AssertionError('No real exchange')))
    monkeypatch.setattr(dashboard, '_log', Mock())
    live = Mock(side_effect=RuntimeError('isolated offline exchange'))
    monkeypatch.setattr(dashboard, '_live_balance_for_user', live)
    return db, accounts, live


def test_db_rows_and_totals_unchanged(balance_data):
    """Database order, IDs, dates, rounding and gross exposure remain stable."""
    db, _, live = balance_data
    result = dashboard.get_balance(users='ALL', live=False, session=None)
    assert result == {
        'rows': [
            {'id': 2, 'user': 'bob', 'date': '1970-01-01 00:00:01', 'balance': 500.0, 'upnl': -1.23, 'we': 40.0},
            {'id': 1, 'user': 'alice', 'date': '1970-01-01 00:00:00', 'balance': 1000.0, 'upnl': 2.35, 'we': 10.0},
            {'id': 3, 'user': 'empty', 'date': '1970-01-01 00:00:00', 'balance': 0.0, 'upnl': 0.0, 'we': 0.0},
        ],
        'totals': {'balance': 1500.0, 'upnl': 1.11, 'we': 20.0}, 'source': 'db',
    }
    db.fetch_positions_for_users.assert_called_once_with(['alice', 'bob', 'empty'])
    db.fetch_positions.assert_not_called()
    live.assert_not_called()


@pytest.mark.parametrize('mode', ['live', 'mixed', 'db'])
def test_live_sources_and_lazy_batching(balance_data, mode):
    """Live-only requests skip DB reads; multiple fallbacks share both batches."""
    db, _, live = balance_data

    def fetch_live(user, _db):
        """Choose synthetic successes without network or real credentials."""
        if mode == 'db' or (mode == 'mixed' and user.name == 'bob'):
            raise RuntimeError('isolated failure')
        return (1000.0, 2.345, 100.0) if user.name == 'alice' else (500.0, -1.234, 200.0)

    live.side_effect = fetch_live
    result = dashboard.get_balance(users='alice,bob', live=True, session=None)
    assert result['source'] == mode
    assert result['totals'] == {'balance': 1500.0, 'upnl': 1.11, 'we': 20.0}
    assert [row['user'] for row in result['rows']] == ['alice', 'bob']
    assert [row['id'] for row in result['rows']] == [0, 0]
    assert [row['we'] for row in result['rows']] == [10.0, 40.0]
    for row in result['rows']:
        assert len(row['date']) == 19
    if mode == 'live':
        db.fetch_balances.assert_not_called()
        db.fetch_positions_for_users.assert_not_called()
    else:
        db.fetch_balances.assert_called_once_with(['alice', 'bob'])
        db.fetch_positions_for_users.assert_called_once_with(['alice', 'bob'])
    if mode == 'db':
        assert [row['date'] for row in result['rows']] == ['1970-01-01 00:00:00', '1970-01-01 00:00:01']
    db.fetch_positions.assert_not_called()


@pytest.mark.parametrize('balances', [None, []])
def test_empty_fallback_does_not_repeat_reads(balance_data, balances):
    """Empty or failed balance reads skip rows and never reload the batches."""
    db, _, _ = balance_data
    db.fetch_balances.side_effect = None
    db.fetch_balances.return_value = balances
    db.fetch_positions_for_users.side_effect = None
    db.fetch_positions_for_users.return_value = {}
    result = dashboard.get_balance(users='alice,bob', live=True, session=None)
    assert result == {'rows': [], 'totals': {'balance': 0.0, 'upnl': 0.0, 'we': 0.0}, 'source': 'db'}
    db.fetch_balances.assert_called_once()
    db.fetch_positions_for_users.assert_called_once()


@pytest.mark.parametrize('live', [False, True])
def test_empty_positions_are_zero_exposure(balance_data, live):
    """An empty position batch still produces balance rows without errors."""
    db, _, _ = balance_data
    db.fetch_positions_for_users.side_effect = None
    db.fetch_positions_for_users.return_value = {}
    result = dashboard.get_balance(users='alice,bob', live=live, session=None)
    assert result['totals'] == {'balance': 1500.0, 'upnl': 0.0, 'we': 0.0}
    assert len(result['rows']) == 2
    db.fetch_positions_for_users.assert_called_once()


def test_duplicate_live_selection_preserves_rows(balance_data):
    """Duplicate selected rows remain while fallback query names are unique."""
    db, _, _ = balance_data
    result = dashboard.get_balance(users='alice,alice', live=True, session=None)
    assert [row['user'] for row in result['rows']] == ['alice', 'alice']
    assert result['totals'] == {'balance': 2000.0, 'upnl': 4.69, 'we': 10.0}
    db.fetch_balances.assert_called_once_with(['alice'])
    db.fetch_positions_for_users.assert_called_once_with(['alice'])


@pytest.mark.parametrize('live', [False, True])
def test_unknown_and_disappeared_accounts(balance_data, live, monkeypatch):
    """Listed accounts that disappeared have no positions; unknown names are filtered."""
    db, accounts, _ = balance_data
    accounts.pop('bob')
    # Retain bob in the listing to model deletion between list and lookup.
    monkeypatch.setattr(dashboard, '_get_users', lambda: SimpleNamespace(
        list=lambda: ['alice', 'bob'], find_user=lambda name: accounts.get(name)))
    result = dashboard.get_balance(users='unknown,bob,alice', live=live, session=None)
    assert result['totals'] == {'balance': 1000.0 if live else 1500.0, 'upnl': 2.35,
                                'we': 10.0 if live else 6.67}
    assert [row['user'] for row in result['rows']] == (['alice'] if live else ['bob', 'alice'])
    db.fetch_positions_for_users.assert_called_once_with(['alice'])


def test_fallback_uses_last_balance_and_safe_numbers(balance_data):
    """Fallback retains the last account snapshot and numeric string conversions."""
    db, _, _ = balance_data
    db.fetch_balances.side_effect = None
    db.fetch_balances.return_value = [(1, 0, 50.0, 'alice'), (2, 'bad-date', 1000.0, 'alice')]
    db.fetch_positions_for_users.side_effect = None
    db.fetch_positions_for_users.return_value = {'alice': [(1, 'ETHUSDT', 0, '-1', '2.345', '100', 'alice')]}
    result = dashboard.get_balance(users='alice,bob', live=True, session=None)
    assert result['rows'] == [{'id': 0, 'user': 'alice', 'date': 'bad-date', 'balance': 1000.0, 'upnl': 2.35, 'we': 10.0}]
    assert result['totals'] == {'balance': 1000.0, 'upnl': 2.35, 'we': 10.0}


@pytest.mark.parametrize('users', ['unknown', 'alice'])
def test_no_balance_rows_skip_position_batch(balance_data, users):
    """Unknown selections or missing balance snapshots return a stable empty result."""
    db, _, _ = balance_data
    db.fetch_balances.side_effect = None
    db.fetch_balances.return_value = None
    result = dashboard.get_balance(users=users, live=False, session=None)
    assert result['rows'] == []
    assert result['totals'] == {'balance': 0, 'upnl': 0, 'we': 0}
    db.fetch_positions_for_users.assert_not_called()
