"""Installed manuals stay immutable, bounded and isolated from runtime data."""
import copy
import hashlib

import pytest

from ai_loop_manual import installed_manual
from pb8_loop_evidence import EvidenceStore, EvidenceSession, LoopAIPause, required_content
from tests.test_pb8_loop_optimizer import record


def checkout(tmp_path, monkeypatch):
    """Configure only a synthetic PB8 documentation tree."""
    root = tmp_path / 'pb8'
    docs = root / 'docs'
    docs.mkdir(parents=True)
    for name in ['optimizing', 'metrics', 'config.bot']:
        (docs / (name + '.md')).write_text('# ' + name + '\nOriginal explanation: ' + name)
    monkeypatch.setattr('pbgui_purefunc.pb8dir', lambda: str(root))
    return root


def test_original_manual_is_readable_through_frozen_evidence(record, tmp_path, monkeypatch):
    """Full reference text remains available without copying it into initial context."""
    root = checkout(tmp_path, monkeypatch)
    manual = installed_manual()
    raw = {'stage': 'interpret', 'rubric': [], 'evidence': {}, 'passivbot_manual': manual}
    compact = required_content(raw)
    assert 'documents' not in compact['passivbot_manual']
    store, row = record
    evidence = EvidenceStore(store)
    frozen = evidence.create(row, compact, raw, [])
    entry = next(item for item in manual['index'] if item['path'] == 'docs/optimizing.md')
    expected = (root / 'docs/optimizing.md').read_text()
    (root / 'docs/optimizing.md').write_text('changed after snapshot')
    assert evidence.resolve(frozen, entry['evidence_path']) == expected
    assert entry['sha256'] == hashlib.sha256(expected.encode()).hexdigest()
    session = EvidenceSession(evidence, frozen, 1500)
    result = session.read({'path': entry['evidence_path'], 'offset': 0}, 'manual-read')
    assert result['entries'][0]['value'] == expected
    assert compact['passivbot_manual']['digest'] != installed_manual()['digest']


def test_manual_excludes_symlinks_history_plans_and_non_docs(tmp_path, monkeypatch):
    """The reader cannot expose configs, arbitrary files or old development advice."""
    root = checkout(tmp_path, monkeypatch)
    secret = tmp_path / 'outside.md'
    secret.write_text('must never reach evidence')
    (root / 'docs/leak.md').symlink_to(secret)
    (root / 'docs/plans').mkdir()
    (root / 'docs/plans/future.md').write_text('unimplemented plan')
    (root / 'docs/release_notes_v7.0.md').write_text('historical')
    (root / 'config.json').write_text('private runtime config')
    manual = installed_manual()
    assert 'must never' not in str(manual)
    assert len(manual['documents']) == 3
    assert 'docs/leak.md' in manual['omitted']


@pytest.mark.parametrize('missing', ['optimizing', 'metrics', 'config.bot'])
def test_missing_core_manual_pauses_without_guessing(tmp_path, monkeypatch, missing):
    """A missing original reference must be visible before provider dispatch."""
    root = checkout(tmp_path, monkeypatch)
    (root / 'docs' / (missing + '.md')).unlink()
    with pytest.raises(LoopAIPause) as error:
        installed_manual()
    assert error.value.reason == 'manual_unavailable'
