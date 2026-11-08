"""Lossless bounded candidate evidence and immutable snapshot migration, entirely offline."""
import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pb8_loop_ai_v2 import run_decision, validate_snapshot_budget
from pb8_loop_evidence import (EvidenceStore, LoopAIPause, required_content,
                               selection_snapshot_stale, tokens)
from pb8_loop_ai_policy import check_context
from tests.test_pb8_loop_optimizer import record


@pytest.fixture
def selection():
    """Model a large native Pareto batch without reading any real reports."""
    metrics = ['gain', 'drawdown_worst_strategy_eq', 'liquidated', 'backtest_completion_ratio', 'exposure']
    candidates = []
    for index in range(248):
        stats = {metric: {reducer: (0.0 if metric == 'liquidated' else
                    1.0 if metric == 'backtest_completion_ratio' else
                    (index + offset + 1) / 371.1234567890123)
                  for offset, reducer in enumerate(('mean', 'min', 'max', 'std', 'median'))}
                 for metric in metrics}
        candidates.append({'id': 'candidate_' + str(index), 'optimizer_job': 'optimizer',
            'config': {'distinct': index}, 'overrides': {},
            'metrics': {metric: stats[metric]['mean'] for metric in metrics},
            'metric_statistics': stats,
            'metric_basis': {'source': 'native_optimizer', 'window_count': 4,
                             'reducers': {metric: 'mean' for metric in metrics}, 'unavailable': {}},
            'source': {'run': 'native_run', 'path': '/isolated/' + 'abcdef0123456789/' * 32,
                       'artifact': f'candidate_{index}.json', 'modified': 12345.6789,
                       'loaded_artifacts': 248, 'available_artifacts': 248}})
    return {'stage': 'select', 'initial_config': {},
            'rubric': [{'metrics': metrics}], 'evidence': {'candidates': candidates}}


def snapshot(content):
    """Bind synthetic raw records to a queryable immutable identity."""
    return {'id': 'a' * 32, 'sha256': 'b' * 64, 'data': content, 'required': required_content(content)}


def test_large_selection_fits_real_profile_without_discarding_candidates(selection):
    """Initial context has global coverage; exact rankings can reach the very last record."""
    from pb8_loop_selection import query
    original = copy.deepcopy(selection)
    compact = required_content(selection)
    profile = {'context': 475000, 'page_tokens': 5000}
    with pytest.raises(LoopAIPause, match='safe context budget'):
        check_context(selection, profile, 32768, codex=True)
    check_context(compact, profile, 32768, codex=True)
    assert tokens(compact) < 4000
    assert 'candidates' not in compact['evidence']
    assert compact['evidence']['selection']['candidate_count'] == 248
    page = query(snapshot(selection), '/selection/rank/gain/value/max', 0, 1500)
    assert page['entries'][0]['id'] == 'candidate_247'
    assert page['entries'][0]['metric_statistics'] == selection['evidence']['candidates'][-1]['metric_statistics']
    assert tokens(page) <= 1500
    assert selection == original


def test_selection_preserves_absent_null_zero_and_empty_objects(selection):
    """Global counters and exact detail reads keep different forms of missingness distinct."""
    from pb8_loop_selection import query
    candidates = selection['evidence']['candidates']
    candidates[0]['metrics']['gain'] = None
    candidates[1]['metrics']['gain'] = 0
    candidates[2]['metrics'].pop('gain')
    compact = required_content(selection)['evidence']['selection']
    assert compact['metrics']['gain']['null'] == 1
    assert compact['metrics']['gain']['missing'] == 1
    assert compact['metrics']['gain']['min'] == 0
    page = query(snapshot(selection), '/selection/id/candidate_0', 0, 5000)
    assert page['entries'][0]['metrics']['gain'] is None
    page = query(snapshot(selection), '/selection/id/candidate_2', 0, 5000)
    assert 'gain' not in page['entries'][0]['metrics']


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code'])
def test_resume_reprojects_old_selection_without_mutating_snapshot(record, selection, monkeypatch, provider):
    """Resume checks new bounded evidence, preserving the old signed snapshot and budgets."""
    _, row = record
    row['settings']['provider'] = provider
    old = copy.deepcopy(selection)
    snapshot = {'id': 'a' * 32, 'sha256': 'b' * 64, 'data': selection, 'required': old}
    before = copy.deepcopy(snapshot)
    assert selection_snapshot_stale(snapshot)
    validate_snapshot_budget(row, snapshot, {'loop_profile': {'context': 475000, 'page_tokens': 5000,
                             'tool_output_tokens': 8192}, 'output_limit': 32768})
    assert snapshot == before
    assert not selection_snapshot_stale({'data': selection, 'required': required_content(selection)})


def test_old_selection_is_refrozen_even_with_identical_input_digest(record, monkeypatch):
    """A matching old raw digest cannot silently dispatch the oversized old projection."""
    store, row = record
    evidence = {'candidates': [{'id': 'candidate', 'optimizer_job': 'optimizer', 'metrics': {'gain': 1}}]}
    ai = SimpleNamespace(store=store, preflight=AsyncMock(return_value={
        'loop_profile': {'context': 475000, 'page_tokens': 5000}}))
    captured = []
    class SnapshotBoundary(Exception):
        """Stop before any model request or job launch."""
    def capture(self, current, required, raw, history):
        """Capture the next immutable evidence version without dispatching."""
        captured.append((copy.deepcopy(required), copy.deepcopy(raw), history))
        raise SnapshotBoundary()
    original_create = EvidenceStore.create
    monkeypatch.setattr(EvidenceStore, 'create', capture)
    with pytest.raises(SnapshotBoundary):
        asyncio.run(run_decision(ai, row, 'select', evidence))
    required, raw, history = captured[0]
    required.pop('selection_projection_version')
    frozen = original_create(EvidenceStore(store), row, required, raw, history)
    saved = store.update(row['owner'], row['id'], lambda r: r.update(
        ai_snapshot_id=frozen['id'], ai_snapshot_stage='select', ai_snapshot_round=row['round']))
    with pytest.raises(SnapshotBoundary):
        asyncio.run(run_decision(ai, saved, 'select', evidence))
    assert len(captured) == 2
    assert captured[1][1]['input_digest'] == raw['input_digest']
    assert captured[1][0]['selection_projection_version'] == 2
    assert EvidenceStore(store).load(row['owner'], frozen['id']) == frozen
    assert store.read(row['owner'], row['id'])['jobs'] == row['jobs']


@pytest.mark.parametrize('count', [1, 248, 10000])
def test_context_cost_does_not_scale_with_population(count):
    """Dataset size changes counters, not the number of candidate rows in context."""
    content = {'stage': 'select', 'rubric': [{'metrics': ['gain']}], 'initial_config': {},
               'evidence': {'candidates': [{'id': str(i), 'metrics': {'gain': i}} for i in range(count)]}}
    required = required_content(content)
    assert required['evidence']['selection']['candidate_count'] == count
    assert required['evidence']['selection']['metrics']['gain']['max'] == count - 1
    assert tokens(required) < 1200


def test_population_overview_streams_a_million_rows():
    """The summary consumes an iterator once rather than retaining a shortlist or whole copy."""
    from pb8_loop_selection import overview
    result = overview(({'metrics': {'gain': i}} for i in range(1000000)), {'gain'})
    assert result['candidate_count'] == 1000000
    assert result['metrics']['gain']['min'] == 0 and result['metrics']['gain']['max'] == 999999
    assert tokens(result) < 600


def test_rank_queries_page_exact_integers_with_stable_ties_and_no_prefix_limit():
    """Large integer scores retain exact ordering and every page resumes without duplicates."""
    from pb8_loop_selection import query
    candidates = [{'id': str(i), 'metrics': {'gain': 2**60 + i // 2}} for i in range(101)]
    content = {'stage': 'select', 'rubric': [{'metrics': ['gain']}], 'evidence': {'candidates': candidates}}
    frozen = snapshot(content)
    ids, cursor = [], 0
    while cursor is not None:
        page = query(frozen, '/selection/rank/gain/value/max', cursor, 1500)
        assert page['matched_count'] == 101 and tokens(page) <= 1500
        ids.extend(row['id'] for row in page['entries'])
        cursor = page['next_offset']
    assert ids == [str(i) for i in sorted(range(101), key=lambda i: (-candidates[i]['metrics']['gain'], i))]


def test_rank_statistics_and_proxy_target_filter_are_explicit():
    """Queries use the requested reducer and screen the entire dataset, never CPU-qualify it."""
    from pb8_loop_selection import query
    candidates = [{'id': str(i), 'metrics': {'gain': i, 'drawdown': i / 10},
                   'metric_statistics': {'gain': {'min': -i}}} for i in range(10)]
    content = {'stage': 'select', 'rubric': [{'goal': 'dd', 'metrics': ['drawdown'], 'target': .3, 'direction': 'min'},
                                           {'goal': 'gain', 'metrics': ['gain'], 'target': None, 'direction': 'max'}],
               'evidence': {'candidates': candidates}}
    frozen = snapshot(content)
    page = query(frozen, '/selection/rank/gain/value/max/goals_met', 0, 1500)
    assert page['matched_count'] == 4 and page['entries'][0]['id'] == '3'
    page = query(frozen, '/selection/rank/gain/min/max', 0, 1500)
    assert page['entries'][0]['id'] == '0'
    for path in ['/selection/rank/gain/value/drop_table', '/selection/rank/gain/value/max/other', '/selection/unknown']:
        with pytest.raises(ValueError):
            query(frozen, path, 0, 1500)
    with pytest.raises(ValueError):
        query(frozen, '/selection/rows', 2**100, 1500)


def test_selection_queries_use_real_tool_quota_and_survive_compaction(record, selection):
    """Queries remain read-only and share the normal ledger and mandatory recovery budget."""
    from pb8_loop_evidence import EvidenceSession
    store, row = record
    archive = EvidenceStore(store)
    frozen = archive.create(row, required_content(selection), selection, [])
    session = EvidenceSession(archive, frozen, 1500)
    calls = []
    session.charge = lambda: calls.append(True)
    first = session.read({'path': '/selection/rank/gain/value/max', 'offset': 0}, 'first')
    assert first['entries'][0]['id'] == 'candidate_247'
    assert calls == [True] and session.total_calls == 1
    assert session.read({'path': '/selection/rank/gain/value/max', 'offset': 0}, 'first') == first
    assert calls == [True]
    session.compacted()
    assert session.missing()
    again = session.read({'path': '/selection/id/candidate_247', 'offset': 0}, 'second')
    assert again['entries'][0]['id'] == 'candidate_247'
    assert calls == [True, True]
    assert archive.load(row['owner'], frozen['id']) == frozen
    from pathlib import Path
    directory = Path(session.selection_index.directory.name)
    session.close()
    session.close()
    assert not directory.exists()



def test_growing_configs_jobs_and_history_are_references_not_initial_context(selection, monkeypatch):
    """Other growing state cannot reintroduce a population-sized mandatory prompt."""
    from pb8_loop_evidence import pointer
    selection['initial_config'] = {'scenarios': [{'date': '2020-01-01'}] * 10000}
    selection['knowledge'] = [{'long': 'history ' * 1000}] * 1000
    for key in ('jobs', 'optimizer_research', 'exploration_policy', 'jev', 'future_large_collection'):
        selection['evidence'][key] = [{'long': 'record ' * 1000}] * 1000
    import pb8_loop_evidence as module
    original_encode = module.encode
    def bounded_encode(value):
        """A size check must stop before serializing a population-sized optional result."""
        assert value is not selection['evidence']['jev']
        return original_encode(value)
    monkeypatch.setattr(module, 'encode', bounded_encode)
    required = required_content(selection)
    assert tokens(required) < 4000
    for key in ('initial_config', 'knowledge'):
        path = required['context_references'][key]['path']
        assert pointer({'data': selection}, path) == selection[key]
    for key in ('jobs', 'optimizer_research', 'exploration_policy', 'jev', 'future_large_collection'):
        path = required['evidence'][key + '_reference']['path']
        assert pointer({'data': selection}, path) == selection['evidence'][key]


def test_rank_index_reuses_scanned_dataset_and_always_closes(selection, monkeypatch):
    """Further pages and opposite directions reuse disk indexes and leave no open cache."""
    from pathlib import Path
    import pb8_loop_selection as selection_module
    candidates = selection['evidence']['candidates']
    index = selection_module.SelectionIndex(candidates, selection['rubric'])
    directory = Path(index.directory.name)
    calls = []
    original_numeric = selection_module.numeric
    def measured(value):
        """Count dataset score scans without inspecting SQLite implementation details."""
        calls.append(True)
        return original_numeric(value)
    monkeypatch.setattr(selection_module, 'numeric', measured)
    frozen = snapshot(selection)
    calls.clear()
    try:
        first = selection_module.query(frozen, '/selection/rank/gain/value/max', 0, 1500, index)
        assert len(calls) == 248
        second = selection_module.query(frozen, '/selection/rank/gain/value/max', first['next_offset'], 1500, index)
        third = selection_module.query(frozen, '/selection/rank/gain/value/min', 0, 1500, index)
        assert len(calls) == 248
        assert second['entries'][0]['id'] != first['entries'][0]['id']
        assert third['entries'][0]['id'] == 'candidate_0'
        with pytest.raises(ValueError, match='another dataset'):
            selection_module.query(snapshot(copy.deepcopy(selection)), '/selection/rank/gain/value/max', 0, 1500, index)
    finally:
        index.close()
    assert not directory.exists()
    index.close()


def test_optional_collection_read_constructs_only_requested_page(record, monkeypatch):
    """A broad detail read cannot materialize or tokenize every record before returning page one."""
    from pb8_loop_evidence import EvidenceSession
    import pb8_loop_evidence as module
    store, row = record
    content = {'stage': 'select', 'rubric': [{'metrics': ['gain']}], 'evidence': {
        'candidates': [{'id': str(i), 'metrics': {'gain': i}} for i in range(10000)]}}
    archive = EvidenceStore(store)
    frozen = archive.create(row, required_content(content), content, [])
    session = EvidenceSession(archive, frozen, 512)
    original_records, original_tokens = module.records, module.tokens
    visits = []
    def observed_records(value, path='', record_tokens=900):
        """Observe traversal rather than mirroring the pager's representation."""
        for entry in original_records(value, path, record_tokens):
            visits.append(True)
            yield entry
    def bounded_tokens(value):
        """Forbid tokenizing the whole dataset merely to decide that it is large."""
        assert not (isinstance(value, list) and len(value) == 10000)
        return original_tokens(value)
    monkeypatch.setattr(module, 'records', observed_records)
    monkeypatch.setattr(module, 'tokens', bounded_tokens)
    try:
        page = session.read({'path': '/data/evidence/candidates', 'offset': 0}, 'first')
        assert page['complete'] is False and page['next_offset'] > 0
        assert len(visits) < 100 and tokens(page) <= 512
    finally:
        session.close()


def test_oversized_individual_record_requires_exact_detail_read(selection):
    """Unusually large records become explicit pointers rather than disappearing or overflowing a page."""
    from pb8_loop_selection import query
    selection['evidence']['candidates'][0]['metric_basis']['explanation'] = 'Original detail ' * 5000
    result = query(snapshot(selection), '/selection/id/candidate_0', 0, 1500)
    assert result['entries'] == [{'id': 'candidate_0', 'details_path': '/data/evidence/candidates/0',
                                  'requires_detail_read': True}]
    assert tokens(result) <= 1500


def test_global_rankings_can_cover_every_optimizer_source():
    """A lower-scoring trial remains directly queryable without walking another trial's prefix."""
    from pb8_loop_selection import query, SelectionIndex
    candidates = [{'id': str(i), 'optimizer_job': 'a' if i < 50 else 'b',
                   'metrics': {'gain': 1000 - i if i < 50 else i - 1000}} for i in range(100)]
    content = {'stage': 'select', 'rubric': [{'metrics': ['gain']}], 'evidence': {'candidates': candidates}}
    frozen = snapshot(content)
    sources = query(frozen, '/selection/sources', 0, 1500)
    assert sources['entries'] == [{'optimizer_job': 'a', 'candidate_count': 50},
                                  {'optimizer_job': 'b', 'candidate_count': 50}]
    index = SelectionIndex(candidates, content['rubric'])
    try:
        global_page = query(frozen, '/selection/rank/gain/value/max', 0, 1500, index)
        trial_page = query(frozen, '/selection/rank/gain/value/max/source/b', 0, 1500, index)
        assert global_page['entries'][0]['id'] == '0'
        assert trial_page['entries'][0]['id'] == '99' and trial_page['matched_count'] == 50
    finally:
        index.close()
