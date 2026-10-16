"""Owner-scoped learning reset must not erase audit evidence or Holdout accounting."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pb8_loop_controller import LoopController
from pb8_loop_evidence import EvidenceStore
from pb8_loop_store import digest
from secure_files import atomic_write_private_text
from tests.test_pb8_loop_optimizer import OWNER, record


def test_reset_archives_learning_preserves_holdouts_and_blocks_archive_rehydration(record, monkeypatch):
    """Cached and uncached old archives stay readable but cannot reenter new AI context."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current.update(status='stopped'))
    monkeypatch.setattr('pb8_loop_store.time.time', lambda: 100.0)
    path = store.owner_dir(OWNER) / 'knowledge.json'
    ledger = {'schema': 1, 'entries': [], 'used_holdouts': [{'start_date': '2020-01-01'}],
              'reserved_holdouts': [{'loop_id': 'old', 'windows': []}]}
    atomic_write_private_text(path, json.dumps(ledger))
    entry = {'knowledge': 'Old test hypothesis', 'coins': [], 'status': 'observed_exact'}
    store.remember(row, 'old_cached', entry)
    evidence = EvidenceStore(store)
    assert len(evidence.findings(row)) == 1
    store.remember(row, 'old_uncached', entry)
    monkeypatch.setattr('pb8_loop_store.time.time', lambda: 200.0)
    result = store.reset_knowledge(OWNER)
    backup = Path(result['backup'])
    assert result['cleared_entries'] == 2 and backup.stat().st_mode & 0o777 == 0o600
    assert len(json.loads(backup.read_text())['entries']) == 2
    current = store.knowledge(OWNER)
    assert current['entries'] == [] and current['used_holdouts'] == ledger['used_holdouts']
    assert current['reserved_holdouts'] == ledger['reserved_holdouts']
    assert evidence.findings(row) == [] and store.context(row) == []
    store.remember(row, 'old_cached', entry)
    assert store.knowledge(OWNER)['entries'] == []
    assert len(list((path.parent / 'evidence').glob('*.json'))) == 2
    monkeypatch.setattr('pb8_loop_store.time.time', lambda: 300.0)
    store.remember(row, 'new', dict(entry, knowledge='Fresh hypothesis'))
    assert [e['id'] for e in evidence.findings(row)] == ['new']


@pytest.mark.parametrize('status', ['running', 'finishing', 'paused', 'queued'])
def test_reset_rejects_owner_with_unfinished_loop(record, status):
    """No reset races a decision or silently changes the context of a paused run."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current.update(status=status))
    with pytest.raises(ValueError, match='Stop all'):
        store.reset_knowledge(OWNER)
    assert not list((store.owner_dir(OWNER) / 'knowledge_resets').glob('*.json'))


def test_reset_epoch_invalidates_cached_ai_decision(record):
    """A pre-reset decision cannot bypass the fresh context after a later continuation."""
    store, row = record
    evidence = {'winner_options': {}}
    cached = {'stage': 'evaluate', 'round': 0, 'evidence_digest': digest(evidence),
              'repair_error': None, 'decision': {'old': True}}
    row.update(last_ai_decision=cached)
    path = store.owner_dir(OWNER) / 'knowledge.json'
    atomic_write_private_text(path, json.dumps({'entries': [], 'used_holdouts': [], 'learning_reset_at': 200.0}))
    ai = SimpleNamespace(decide=AsyncMock(return_value={'fresh': True}))
    controller = LoopController(store.root, store=store, ai=ai)
    result = asyncio.run(controller.decision(row, 'evaluate', evidence))
    assert result == {'fresh': True}
    assert ai.decide.call_args.args[2]['learning_reset_at'] == 200.0
