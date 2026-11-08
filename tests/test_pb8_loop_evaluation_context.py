"""Bounded research evaluation retains exact complete training comparisons offline."""
import asyncio
import copy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ai_loop_evaluation import evaluation_comparison
from pb8_loop_evaluation_evidence import EvaluationIndex, PROJECTION_VERSION, query, decision_brief
from pb8_loop_evidence import (EvidenceSession, EvidenceStore, decision_snapshot_stale,
                               required_content, tokens)
from pb8_loop_ai_policy import check_context
from pb8_loop_ai_v2 import run_decision, validate_snapshot_budget
from tests.test_pb8_loop_optimizer import record


CRITERIA = [{'metric': 'gain', 'direction': 'max', 'reducer': 'mean', 'reason': 'Common CPU gain'}]


def content(count=100):
    """Generate only synthetic, complete CPU options with different report distributions."""
    options = {f'op{i}': {'candidate_id': f'candidate{i}', 'metrics': {'gain': i, 'liquidated': 0},
        'per_report': [{'gain': i - 1, 'liquidated': 0}, {'gain': i + 1, 'liquidated': 0}],
        'assessment': {'comparable': True, 'hard_targets_met': True, 'simulation_complete': True}}
        for i in range(count)}
    return {'stage': 'evaluate', 'initial_config': {'unchanged': True},
        'rubric': [{'metrics': ['gain']}],
        'evidence': {'winner_options': options, 'best': {'operation': 'op0'},
                     'optimizer_research': {'groups': [], 'trials': {}, 'metric_coverage': {}},
                     'evaluation_policy': {'mode': 'adaptive'}}}


def frozen(raw):
    """Bind temporary values to the same read protocol as immutable runtime evidence."""
    return {'id': 'a' * 32, 'sha256': 'b' * 64, 'data': raw, 'required': required_content(raw)}


@pytest.mark.parametrize('count', [3, 10000])
def test_decision_brief_previews_exact_leaders_without_pruning_options(count):
    """Use independent report reducers, keep the incumbent and retain the whole query dataset."""
    raw = content(count)
    raw['evidence']['evaluation_policy']['previous'] = CRITERIA
    raw['evidence']['winner_options']['op0']['metrics']['gain'] = 999999
    brief = decision_brief(raw)
    assert brief['option_count'] == count and len(brief['leaders']) == 3
    assert brief['leaders'][0]['operation'] == f'op{count-1}'
    assert brief['leaders'][0]['values'] == [count-1]
    assert brief['references']['best']['values'] == [0]
    assert len(raw['evidence']['winner_options']) == count
    assert tokens(brief) < 1500


def test_decision_brief_group_preview_preserves_basis_budget_and_coverage():
    """Do not compare different criteria silently or hide nomination/measurement coverage."""
    raw = content(3)
    groups = [{'id': f'g{i}', 'criteria': CRITERIA, 'distribution': [{'mean': i}],
        'budget': {'mode': 'proxy', 'value': 100000, 'fraction': 1},
        'per_trial_best': [{'operation': f'optimizer{i}'}], 'measurement_coverage': 1,
        'selection_coverage': .02, 'technical_failure_count': i} for i in range(5)]
    groups[-1]['criteria'] = [{**CRITERIA[0], 'reducer': 'max'}]
    raw['evidence']['optimizer_research']['groups'] = groups
    raw['optimizer_sources'] = {f'optimizer{i}': {'config_path': f'/history/owned{i}/config'} for i in range(5)}
    brief = decision_brief(raw)
    assert brief['research_group_count'] == 5
    assert [row['id'] for row in brief['research_leaders']] == ['g3', 'g2', 'g1']
    best = brief['research_leaders'][0]
    assert best['budget']['value'] == 100000
    assert best['measurement_coverage'] == 1 and best['selection_coverage'] == .02
    assert best['technical_failure_count'] == 3
    assert best['source_controls']['bounds'] == '/history/owned3/config/optimize/bounds'


@pytest.mark.parametrize('windows', [1, 10000])
def test_decision_brief_does_not_inline_per_window_reports(windows):
    """Preview size stays bounded independently of the number of training reports."""
    raw = content(3)
    raw['evidence']['evaluation_policy']['previous'] = CRITERIA
    raw['evidence']['winner_options']['op2']['per_report'] = [{'gain': 2}] * windows
    brief = decision_brief(raw)
    assert brief['leaders'][0]['report_count'] == windows
    assert 'per_report' not in brief['leaders'][0]
    assert tokens(brief) < 1500


def test_decision_brief_does_not_promote_incompletely_measured_options():
    """A high primary metric cannot hide a missing secondary evaluation criterion."""
    raw = content(3)
    raw['evidence']['evaluation_policy']['previous'] = [*CRITERIA, {
        'metric': 'secondary', 'direction': 'max', 'reducer': 'mean'}]
    for key, item in raw['evidence']['winner_options'].items():
        for report in item['per_report']:
            report['secondary'] = None if key == 'op2' else 1
    brief = decision_brief(raw)
    assert brief['option_count'] == 3
    assert [row['operation'] for row in brief['leaders']] == ['op1', 'op0']


@pytest.mark.parametrize('count', [1, 100, 10000])
def test_research_evaluation_context_is_independent_of_history_count(count):
    """All options remain queryable while start context contains fixed rules and aggregate counts."""
    raw = content(count)
    raw['evidence']['exploration_policy'] = {'past': ['large history ' * 1000] * count}
    raw['evidence']['evaluation_policy']['comparison_operations'] = list(raw['evidence']['winner_options'])
    required = required_content(raw)
    assert required['evidence']['evaluation']['option_count'] == count
    assert required['evidence']['evaluation']['aggregate_metrics']['gain']['max'] == count - 1
    assert 'winner_options' not in required['evidence']
    assert 'exploration_policy' not in required['evidence']
    assert tokens(required) < 1800
    check_context(required, {'context': 256000}, 32768, codex=True)
    assert query(frozen(raw), '/evaluation/rank/gain/mean/max', 0, 1500)['entries'][0]['operation'] == f'op{count-1}'


@pytest.mark.parametrize('reducer,expected', [('mean', 99), ('min', 98), ('max', 100), ('median', 99)])
def test_cpu_queries_use_exact_requested_report_reducer(reducer, expected):
    """Ranking is recomputed from reports even when the old aggregate is misleading."""
    raw = content()
    raw['evidence']['winner_options']['op0']['metrics']['gain'] = 10000
    page = query(frozen(raw), f'/evaluation/rank/gain/{reducer}/max', 0, 1500)
    assert page['matched_count'] == 100
    assert page['entries'][0]['operation'] == 'op99'
    assert page['entries'][0]['comparison']['value'] == expected
    assert page['entries'][0]['per_report'] == raw['evidence']['winner_options']['op99']['per_report']
    assert tokens(page) <= 1500


def test_cpu_queries_preserve_missing_null_and_skip_nonfinite_ranking():
    """Unavailable metrics never become zero or a fabricated complete comparison."""
    raw = content(4)
    options = raw['evidence']['winner_options']
    options['op0']['per_report'][0].pop('gain')
    options['op1']['per_report'][0]['gain'] = None
    page = query(frozen(raw), '/evaluation/rank/gain/mean/max', 0, 1500)
    assert page['matched_count'] == 2
    assert 'gain' not in query(frozen(raw), '/evaluation/id/op0', 0, 1500)['entries'][0]['per_report'][0]
    assert query(frozen(raw), '/evaluation/id/op1', 0, 1500)['entries'][0]['per_report'][0]['gain'] is None
    assert 'gain' not in query(frozen(raw), '/evaluation/metrics', 0, 1500)['entries']


def test_all_cpu_option_ids_are_reachable_with_stable_bounded_pages():
    """No hard-coded shortlist hides prior candidates or changes the qualified option set."""
    raw = content(123)
    snapshot = frozen(raw)
    before = copy.deepcopy(snapshot)
    cursor, operations = 0, []
    while cursor is not None:
        page = query(snapshot, '/evaluation/options', cursor, 1000)
        assert tokens(page) <= 1000
        operations.extend(row['operation'] for row in page['entries'])
        cursor = page['next_offset']
    assert operations == list(raw['evidence']['winner_options'])
    assert snapshot == before


def test_compact_shared_assessment_recomputes_every_option_identically():
    """Compression affects repeated prose, not CPU criteria, values or candidate coverage."""
    options = content(10000)['evidence']['winner_options']
    explicit = {'evaluation_criteria': CRITERIA,
                'candidate_assessments': dict.fromkeys(options, 'Same CPU criteria')}
    compact = {'evaluation_criteria': CRITERIA, 'candidate_assessment_policy': {
        'scope': 'all_training_candidates', 'reason': 'Same CPU criteria'}}
    assert evaluation_comparison(compact, options) == evaluation_comparison(explicit, options)
    options['op9999']['per_report'][1]['gain'] = None
    with pytest.raises(ValueError, match='unavailable'):
        evaluation_comparison(compact, options)


@pytest.mark.parametrize('policy', [{}, {'scope': 'some', 'reason': 'Not all'},
                                     {'scope': 'all_training_candidates', 'reason': ''}])
def test_compact_assessment_cannot_weaken_existing_contract(policy):
    """An invalid shared policy does not bypass exact complete reassessment."""
    with pytest.raises(ValueError, match='common assessment'):
        evaluation_comparison({'evaluation_criteria': CRITERIA, 'candidate_assessment_policy': policy},
                              content(2)['evidence']['winner_options'])


def test_research_group_rank_reaches_last_group_and_closes_both_indexes(record):
    """CPU and optimizer-setting datasets remain separate, globally ranked and session-owned."""
    store, row = record
    raw = content(100)
    raw['evidence']['optimizer_research']['groups'] = [
        {'id': str(i), 'signature': str(i), 'trial_count': 2, 'distribution': [{'mean': i}],
         'criteria': CRITERIA, 'per_seed_best': [{'operation': 'optimizer' + str(i)}]}
        for i in range(1000)]
    archive = EvidenceStore(store)
    snapshot = archive.create(row, required_content(raw), raw, [])
    session = EvidenceSession(archive, snapshot, 1500)
    try:
        cpu = session.read({'path': '/evaluation/rank/gain/mean/max', 'offset': 0}, 'cpu')
        group = session.read({'path': '/evaluation/research_rank/0/max', 'offset': 0}, 'group')
        assert cpu['entries'][0]['operation'] == 'op99'
        assert group['entries'][0]['representative_operation'] == 'optimizer999'
        assert group['matched_count'] == 1000 and tokens(group) <= 1500
        assert session.total_calls == 2
        first = Path(session.evaluation_index.directory.name)
        second = Path(session.evaluation_index.research_index.directory.name)
        session.compacted()
        assert session.missing()
    finally:
        session.close()
    assert not first.exists() and not second.exists()
    assert archive.load(row['owner'], snapshot['id']) == snapshot


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code'])
def test_resume_reprojects_obsolete_evaluation_without_touching_signed_snapshot(record, provider):
    """An already paused run can pass resume without a larger model or rerunning optimizer jobs."""
    _, row = record
    row['settings']['provider'] = provider
    row['optimizer_research_version'] = 1
    raw = content(1000)
    snapshot = {'id': 'a' * 32, 'sha256': 'b' * 64, 'data': raw, 'required': raw}
    before = copy.deepcopy(snapshot)
    assert decision_snapshot_stale(snapshot)
    validate_snapshot_budget(row, snapshot, {'loop_profile': {'context': 256000, 'page_tokens': 1500},
                                            'output_limit': 32768})
    assert snapshot == before
    assert not decision_snapshot_stale(frozen(raw))


def test_old_evaluation_snapshot_is_refrozen_before_dispatch_with_same_raw_digest(record, monkeypatch):
    """Projection upgrades require a new signed snapshot, not rewriting or dispatching old evidence."""
    store, row = record
    row['optimizer_research_version'] = 1
    evidence = content(2)['evidence']
    captured = []
    class Boundary(Exception):
        """Stop before any real model call or native job."""
    def capture(self, current, required, raw, history):
        """Inspect the real decision boundary in the temporary store."""
        captured.append((copy.deepcopy(required), copy.deepcopy(raw), history))
        raise Boundary()
    original_create = EvidenceStore.create
    monkeypatch.setattr(EvidenceStore, 'create', capture)
    ai = SimpleNamespace(store=store, preflight=AsyncMock(return_value={
        'loop_profile': {'context': 475000, 'page_tokens': 5000}}))
    with pytest.raises(Boundary):
        asyncio.run(run_decision(ai, row, 'evaluate', evidence))
    required, raw, history = captured[0]
    required.pop('evaluation_projection_version')
    snapshot = original_create(EvidenceStore(store), row, required, raw, history)
    saved = store.update(row['owner'], row['id'], lambda r: r.update(
        optimizer_research_version=1, ai_snapshot_id=snapshot['id'], ai_snapshot_stage='evaluate',
        ai_snapshot_round=row['round']))
    with pytest.raises(Boundary):
        asyncio.run(run_decision(ai, saved, 'evaluate', evidence))
    assert captured[1][1]['input_digest'] == raw['input_digest']
    assert captured[1][0]['evaluation_projection_version'] == PROJECTION_VERSION
    assert EvidenceStore(store).load(row['owner'], snapshot['id']) == snapshot


@pytest.mark.parametrize('outcome', ['complete', 'cancel', 'error'])
def test_real_decision_boundary_validates_shared_policy_and_releases_query_owners(record, outcome):
    """The actual orchestrator closes CPU and research indexes on every exit path."""
    import json
    from pb8_loop_ai_v2 import _ACTIVE
    store, row = record
    row = store.update(row['owner'], row['id'], lambda r: r.update(optimizer_research_version=1,
        settings={**r['settings'], 'provider': 'chatgpt'}))
    evidence = content(2)['evidence'] | {'remaining_runs': 0, 'remaining_rounds': 0}
    evidence['optimizer_research']['groups'] = [{'id': 'group', 'distribution': [{'mean': 1}]}]
    directories = []
    async def request(*_):
        """Read through the live evidence session, without external models or native jobs."""
        active = _ACTIVE.get()
        active['session'].read({'path': '/evaluation/rank/gain/mean/max', 'offset': 0}, 'cpu')
        active['session'].read({'path': '/evaluation/research_rank/0/max', 'offset': 0}, 'research')
        directories.extend([Path(active['session'].evaluation_index.directory.name),
                            Path(active['session'].evaluation_index.research_index.directory.name)])
        if outcome == 'cancel':
            raise asyncio.CancelledError()
        if outcome == 'error':
            raise RuntimeError('Synthetic model boundary failure')
        return json.dumps({'winner_operation': 'op1', 'finish': True, 'reason': 'Higher measured CPU gain',
            'evaluation_criteria': CRITERIA,
            'candidate_assessment_policy': {'scope': 'all_training_candidates', 'reason': 'Compare both exactly'},
            'optimizer_assessment': {'criteria': CRITERIA, 'recommendations': [], 'reason': 'No optimizer trials'}}), {}
    ai = SimpleNamespace(store=store, preflight=AsyncMock(return_value={
        'loop_profile': {'context': 475000, 'page_tokens': 1500}, 'output_limit': 1000}), _request=request)
    if outcome == 'complete':
        result = asyncio.run(run_decision(ai, row, 'evaluate', evidence))
        assert result['winner_operation'] == 'op1'
        assert evaluation_comparison(result, evidence['winner_options'])['candidates']['op1']['values'] == [1]
    else:
        with pytest.raises(asyncio.CancelledError if outcome == 'cancel' else RuntimeError):
            asyncio.run(run_decision(ai, row, 'evaluate', evidence))
    assert len(directories) == 2 and all(not path.exists() for path in directories)
    assert store.read(row['owner'], row['id'])['jobs'] == row['jobs']
