"""Mandatory background reports stay independent of search and model evidence."""
import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

from pb8_loop_controller import LoopController
from pb8_loop_evidence import EvidenceStore
from pb8_loop_ai_v2 import run_decision
from pb8_loop_observer import ObserverJobs
from tests.test_ai_loop_windows import run_record


def test_prior_loop_holdout_cannot_block_new_template_run(tmp_path, monkeypatch):
    """Two independent runs can report the same reviewed dates despite old usage."""
    store, original = run_record(tmp_path)
    windows = copy.deepcopy(original['holdouts'])
    monkeypatch.setattr(store, 'knowledge', lambda _owner: {
        'entries': [], 'used_holdouts': windows, 'reserved_holdouts': [{'loop_id': original['id'], 'windows': windows}]})

    async def preflight(*_args):
        """Avoid providers, billing and network access."""
        return {}

    backend = SimpleNamespace(initial=lambda *_args: (
        original['initial_config'], {}, original['fingerprint'], windows, original['comparison']))
    controller = LoopController(tmp_path, store=store, backend=backend, ai=SimpleNamespace(preflight=preflight))
    first = asyncio.run(controller.create(original['owner'], original['settings']))
    second = asyncio.run(controller.create(original['owner'], original['settings']))
    for record in (first, second):
        assert record['holdouts'] == windows and record['observer_holdouts'] == windows
        record['baseline'] = {'assessment': {'simulation_complete': True}}
        assert {kind for _, _, kind in ObserverJobs.targets(record)} == {'observer_holdout', 'observer_full_range'}
    assert first['id'] != second['id']


@pytest.mark.parametrize('stage', ['interpret', 'bootstrap', 'evaluate'])
def test_real_model_snapshot_excludes_background_reports(tmp_path, monkeypatch, stage):
    """Inspect actual frozen model input/history before any provider can be called."""
    monkeypatch.setattr('ai_loop_manual.installed_manual', lambda: {
        'source': 'offline_test_fixture', 'digest': 'test-manual',
        'index': [{'path': 'docs/optimizing.md', 'sha256': 'fixture-hash',
                   'evidence_path': '/data/passivbot_manual/documents/0/text'}],
        'documents': [{'path': 'docs/optimizing.md', 'text': 'Original synthetic scoring manual'}], 'omitted': []})
    store, row = run_record(tmp_path)
    marker = 'USER_REPORT_PRIVATE_987654321'
    row = store.update(row['owner'], row['id'], lambda current: current.update(observer_jobs=[
        {'observer_only': True, 'kind': kind, 'operation': marker, 'status': 'complete',
         'metrics': {'gain_strategy_eq': 987654321}, 'assessment': {'reason': marker}}
        for kind in ('observer_holdout', 'observer_full_range')]))
    store.remember(row, 'observer_finding', {'observer_only': True, 'kind': 'observer_full_range',
                   'coins': row['settings']['goals'].get('coins', []), 'knowledge': marker})
    captured = {}

    class Captured(Exception):
        """End at the actual snapshot boundary, before billing or external calls."""

    async def preflight(*_args):
        """Use a synthetic model window with no provider interaction."""
        return {'loop_profile': {'context': 256000}, 'protocol': 'chat'}

    def snapshot(self, record, required, content, history):
        """Capture exactly what model evidence paging would expose."""
        captured.update(required=required, content=content, history=history)
        raise Captured()

    monkeypatch.setattr(EvidenceStore, 'create', snapshot)
    with pytest.raises(Captured):
        asyncio.run(run_decision(SimpleNamespace(store=store, preflight=preflight), row, stage, {}))
    payload = json.dumps(captured)
    assert marker not in payload and '987654321' not in payload
    assert 'observer_jobs' not in payload
    assert captured['required']['passivbot_manual']['index'][0]['path'] == 'docs/optimizing.md'
    assert 'documents' not in captured['required']['passivbot_manual']
    assert captured['content']['passivbot_manual']['documents'][0]['text'] == 'Original synthetic scoring manual'
    assert store.context(row) == []
