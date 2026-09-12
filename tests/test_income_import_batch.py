"""Legacy income imports use a single atomic transaction and outer lease."""

import json
import sqlite3
import threading
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import Database as database_module


@pytest.mark.parametrize('invalid', [False, True])
def test_import_batch_atomic_and_idempotent(tmp_path, monkeypatch, invalid):
    """Thousands of rows commit once; a bad row rolls back the entire batch."""
    monkeypatch.setattr(database_module, 'PBGDIR', tmp_path)
    source = tmp_path / 'data/logs/income_other_alice.json'
    source.parent.mkdir(parents=True)
    records = [dict(incomeType='COMMISSION',symbol='BTCUSDT',time=i,income=1,tranId=str(i)) for i in range(3000)]
    records.append(dict(records[0]))
    if invalid:
        records[-1] = dict(records[-1],symbol=None,tranId='bad')
    source.write_text(''.join(json.dumps(row) for row in records))
    db = object.__new__(database_module.Database)
    db._write_lock = threading.Lock()
    lease = Mock(wraps=database_module.acquire_database_lock)
    monkeypatch.setattr(database_module, 'acquire_database_lock', lease)
    with closing(sqlite3.connect(':memory:')) as conn:
        conn.execute('CREATE TABLE history(symbol TEXT NOT NULL,timestamp INTEGER,income REAL,uniqueid TEXT UNIQUE,user TEXT)')
        trace = []
        conn.set_trace_callback(trace.append)
        monkeypatch.setattr(db, '_connect', lambda: conn)
        db.import_from_save_income_other(SimpleNamespace(name='alice'))
        assert lease.call_count == 1
        assert conn.execute('SELECT count(*) FROM history').fetchone()[0] == (0 if invalid else 3000)
        assert sum(sql == 'COMMIT' for sql in trace) == (0 if invalid else 1)
        assert sum(sql == 'ROLLBACK' for sql in trace) == int(invalid)
        if not invalid:
            db.import_from_save_income_other(SimpleNamespace(name='alice'))
            assert conn.execute('SELECT count(*) FROM history').fetchone()[0] == 3000
