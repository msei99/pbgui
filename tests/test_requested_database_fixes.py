"""Isolated regressions for maintenance restart and DB schema/filter fixes."""

from contextlib import closing
import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import Database as database_module
import db_maintenance as maintenance
from api import db_tools
from database_lock import DatabaseBusyError


@pytest.mark.parametrize('states', [
    ['inactive', DatabaseBusyError('activating'), 'active'],
    ['inactive', 'inactive', 'active'],
    ['inactive', 'active'],
])
def test_restart_waits_for_active(tmp_path, monkeypatch, states):
    """A transitional service state does not abort a successful restart."""
    control = maintenance.PBDataControl(tmp_path)
    monkeypatch.setattr(control, '_legacy', lambda: None)
    monkeypatch.setattr(control, '_service_state', Mock(side_effect=states))
    command = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr(control, '_service_command', command)
    monkeypatch.setattr(maintenance.time, 'sleep', Mock())
    control.start('systemd')
    command.assert_called_once_with('start')


@pytest.mark.parametrize('command_failed', [False, True])
def test_restart_failure_is_bounded(tmp_path, monkeypatch, command_failed):
    """Failed starts cannot claim success or poll forever."""
    control = maintenance.PBDataControl(tmp_path)
    monkeypatch.setattr(control, 'inspect', lambda: 'none')
    state = Mock(side_effect=DatabaseBusyError('activating'))
    monkeypatch.setattr(control, '_service_state', state)
    monkeypatch.setattr(control, '_service_command', Mock(return_value=SimpleNamespace(returncode=int(command_failed))))
    sleep = Mock()
    monkeypatch.setattr(maintenance.time, 'sleep', sleep)
    with pytest.raises(DatabaseBusyError, match='retry recovery'):
        control.start('systemd')
    assert state.call_count == (0 if command_failed else 15)
    assert sleep.call_count == (0 if command_failed else 14)


@pytest.mark.parametrize('name', [db_tools.MAIN_DB_NAME, db_tools.TRADES_DB_NAME])
def test_schema_uses_wal_and_closes_connection(tmp_path, monkeypatch, name):
    """Schema connections close, and WAL readers work during an uncommitted write."""
    connect = sqlite3.connect
    connections = []

    def tracked_connect(*args, **kwargs):
        """Retain test handles to verify deterministic closure."""
        conn = connect(*args, **kwargs)
        connections.append(conn)
        return conn

    monkeypatch.setattr(db_tools.sqlite3, 'connect', tracked_connect)
    path = tmp_path / name
    db_tools._ensure_schema(path, name)
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        connections[0].execute('SELECT 1')
    with closing(connect(path)) as writer, closing(connect(path, timeout=0)) as reader:
        assert reader.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
        writer.execute('BEGIN EXCLUSIVE')
        assert reader.execute('SELECT count(*) FROM sqlite_master').fetchone()[0] > 0
        writer.rollback()


@pytest.mark.parametrize('method,args', [('fetch_balances', ([],)), ('select_top', ([], 0, 100, 5))])
def test_empty_users_avoid_database_access(monkeypatch, method, args):
    """Empty selections return stable empty lists without opening runtime files."""
    db = object.__new__(database_module.Database)
    connection = Mock(side_effect=AssertionError('No database access expected'))
    monkeypatch.setattr(db, '_connect', connection)
    assert getattr(db, method)(*args) == []
    connection.assert_not_called()
