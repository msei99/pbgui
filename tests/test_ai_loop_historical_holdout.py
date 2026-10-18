"""Historical evidence must not expose protected Holdout data to adaptive decisions."""
import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

from api import optimize_v8 as opt
from pb8_loop_archive import select_sources
from pb8_loop_backend import LoopBackend
from pb8_loop_controller import LoopController
from pb8_loop_evidence import LoopAIPause, select_findings
from tests.test_ai_loop_windows import run_record
from tests.test_pb8_loop_optimizer import config, RULE


@pytest.fixture
def historical_sources(tmp_path, monkeypatch):
    """Create native-looking artifacts only below the test's result directory."""
    store, row = run_record(tmp_path)
    root = tmp_path / 'results'
    sources = []
    specs = {
        'full_range': {'start_date': '2024-01-01', 'end_date': '2024-01-10', 'suite_enabled': False},
        'boundary': {'start_date': '2024-01-06', 'end_date': '2024-01-08', 'suite_enabled': False},
        'missing_dates': {'suite_enabled': False},
        'missing_backtest': None,
        'invalid_suite': {'start_date': '2024-01-01', 'end_date': '2024-01-07', 'suite_enabled': True, 'scenarios': []},
        'outside_training': {'start_date': '2023-12-01', 'end_date': '2023-12-03', 'suite_enabled': False},
        'training_only': {'start_date': '2024-01-01', 'end_date': '2024-01-07', 'suite_enabled': False},
        'training_suite': {'start_date': '2024-01-01', 'end_date': '2024-01-10', 'suite_enabled': True,
                           'scenarios': [{'start_date': '2024-01-01', 'end_date': '2024-01-02'},
                                         {'start_date': '2024-01-03', 'end_date': '2024-01-07'}]},
    }
    for index, (name, backtest) in enumerate(specs.items()):
        directory = root / name
        (directory / 'pareto').mkdir(parents=True)
        value = config()
        value['metrics'] = {'gain': index + 1, 'drawdown_worst': .1}
        if backtest is None:
            value.pop('backtest')
        else:
            value['backtest'] = backtest
        (directory / 'pareto' / 'candidate.json').write_text(json.dumps(value))
        sources.append({'name': row['settings']['config_name'], 'path': str(directory),
                        'result': name, 'modified': 100 + index})
    monkeypatch.setattr(opt, '_results_root', lambda: root)
    monkeypatch.setattr(opt, '_list_results', lambda: sources)
    monkeypatch.setattr(opt, '_all_results_first', lambda _path: None)
    return store, row, LoopBackend(tmp_path, store)


def test_native_history_requires_real_training_window_provenance(historical_sources):
    """Full range, shared boundary days and fallback configs are not training proof."""
    store, row, backend = historical_sources
    before = {str(path): path.read_bytes() for path in backend.root.rglob('candidate.json')}
    found = backend.existing_candidates(row)
    assert {item['source']['run'] for item in found} == {'training_only', 'training_suite'}
    assert all(item['source']['training_windows'] for item in found)
    assert {str(path): path.read_bytes() for path in backend.root.rglob('candidate.json')} == before
    legacy = store.create(row['owner'], row['settings'] | {'source': {}}, config(), {}, {}, [], {})
    assert len(backend.existing_candidates(legacy)) == 8


def test_native_safe_history_is_the_only_bootstrap_evidence(historical_sources):
    """Drive the real import and controller boundary without starting optimizer jobs."""
    store, row, backend = historical_sources
    row = store.update(row['owner'], row['id'], lambda current: current.update(
        phase='interpret', baseline_required=False, best=None))
    controller = LoopController(backend.root, store=store, backend=backend, ai=object())
    seen = []

    class Captured(Exception):
        """Stop before a fake AI decision could schedule anything."""

    async def decision(record, stage, evidence):
        """Collect exactly the metrics offered to adaptive bootstrap."""
        if stage == 'interpret':
            return {'rubric': [dict(RULE[0], goal='custom')]}
        assert stage == 'bootstrap'
        seen.extend(evidence['existing_results'])
        raise Captured()

    controller.decision = decision
    asyncio.run(controller._tick(row))
    row = store.read(row['owner'], row['id'])
    with pytest.raises(Captured):
        asyncio.run(controller._tick(row))
    assert {item['source']['run'] for item in seen} == {'training_only', 'training_suite'}
    assert not store.read(row['owner'], row['id'])['jobs']


def test_unverified_restored_sources_cannot_enter_archive_selection(historical_sources):
    """The archive reader also rejects old persisted candidates that predate proof."""
    _, row, backend = historical_sources
    unsafe = backend._result_candidates(row, opt._list_results()[0], {
        'operation': 'saved_unknown', 'config': row['initial_config'], 'overrides': {}})
    selected, preview = select_sources(row, {'stage': 'bootstrap', 'evidence': {'existing_results': unsafe}}, {}, 100)
    assert selected == []
    assert preview['sources'] == []


def test_foreign_findings_cannot_bypass_window_checks(historical_sources):
    """Cross-loop prose lacks complete training lineage and cannot guide a protected Loop."""
    store, row, _ = historical_sources
    other = copy.deepcopy(row)
    other['id'] = 'c' * 32
    common = {'coins': row['settings']['goals'].get('coins', []), 'status': 'observed_exact',
              'knowledge': 'A previous full-period result favored this search bound.'}
    store.remember(other, 'unsafe_prior', common)
    store.remember(row, 'safe_current', common | {'knowledge': 'Current training-only observation.'})
    entries = store.knowledge(row['owner'])['entries']
    package, history = select_findings(entries, row)
    assert [item['id'] for item in package['findings']] == ['safe_current']
    assert [item['id'] for item in history] == ['safe_current']
    assert [item['id'] for item in store.context(row)] == ['safe_current']
    legacy = copy.deepcopy(row)
    legacy['settings']['source'] = {}
    legacy.pop('final_holdout_policy')
    assert len(select_findings(entries, legacy)[1]) == 2


def test_pre_fix_protected_state_requires_fresh_run(historical_sources):
    """Old decisions/snapshots with unverified lineage must not resume silently."""
    store, row, backend = historical_sources
    row = store.update(row['owner'], row['id'], lambda current: current.pop('holdout_evidence_policy', None))
    controller = LoopController(backend.root, store=store, backend=backend, ai=SimpleNamespace())
    with pytest.raises(LoopAIPause, match='fresh reviewed Loop'):
        asyncio.run(controller._tick(row))
    store.update(row['owner'], row['id'], lambda current: current.update(status='paused'))
    with pytest.raises(ValueError, match='fresh reviewed Loop'):
        controller.action(row['owner'], row['id'], 'resume')


def test_bootstrap_snapshot_cannot_page_unverified_original_sources(historical_sources, monkeypatch):
    """Filtering applies before both mandatory input and optional /history archives."""
    monkeypatch.setattr('ai_loop_manual.installed_manual', lambda: {
        'source': 'offline_test_fixture', 'digest': 'test-manual', 'index': [],
        'documents': [], 'omitted': []})
    from pb8_loop_ai_v2 import run_decision
    from pb8_loop_evidence import EvidenceStore
    store, row, backend = historical_sources
    unsafe = backend._result_candidates(row, opt._list_results()[0], {
        'operation': 'saved_old', 'config': row['initial_config'], 'overrides': {}})
    captured = {}

    class Captured(Exception):
        """End before provider transport or reservation is entered."""

    async def preflight(*_args):
        """No model or resource checks leave this synthetic test boundary."""
        return {'loop_profile': {'context': 256000}, 'protocol': 'chat'}

    def snapshot(self, record, required, content, history):
        """Capture exactly the original sources made reachable by evidence paging."""
        captured.update(content=content, history=history)
        raise Captured()

    monkeypatch.setattr(EvidenceStore, 'create', snapshot)
    ai = SimpleNamespace(store=store, preflight=preflight)
    with pytest.raises(Captured):
        asyncio.run(run_decision(ai, row, 'bootstrap', {'existing_results': unsafe}))
    assert captured['content']['evidence']['existing_results'] == []
    assert captured['history'] == []
    assert unsafe  # The original supplied list was not overwritten to make the assertion pass.
    row.pop('holdout_evidence_policy')
    with pytest.raises(LoopAIPause, match='fresh reviewed Loop'):
        asyncio.run(run_decision(SimpleNamespace(), row, 'bootstrap', {'existing_results': unsafe}))


def test_historical_dates_cannot_reintroduce_manually_unused_days(historical_sources):
    """Allowed history is limited to the reviewed Training union, including manual gaps."""
    from ai_loop_windows import historical_training_windows, MARKER
    store, row, _ = historical_sources
    binding = row['settings']['source']['loop_request']
    value = copy.deepcopy(binding['window_plan'])
    value['windows'][1]['start_date'] = '2024-01-04'
    binding['window_request'] = 'Reviewed dates\n' + MARKER + json.dumps(value)
    assert historical_training_windows(row, {'start_date': '2024-01-01', 'end_date': '2024-01-07'}) is None
    assert historical_training_windows(row, {'suite_enabled': True, 'scenarios': [
        {'start_date': '2024-01-01', 'end_date': '2024-01-02'},
        {'start_date': '2024-01-04', 'end_date': '2024-01-07'}]}) is not None
