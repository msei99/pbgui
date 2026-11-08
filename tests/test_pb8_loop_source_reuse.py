"""Source-owned research replay and bounded immutable input lookup stay offline."""
import copy
import json

import pytest

from pb8_loop_ai_v2 import validate_decision
from pb8_loop_evidence import EvidenceSession, EvidenceStore, decision_snapshot_stale, required_content
from pb8_loop_research import resolve_variant, signature, source_config, source_inputs
from tests.test_pb8_loop_research import research, optimizer


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code', 'opencode-go', 'opencode-zen'])
@pytest.mark.parametrize('purpose', ['replicate', 'confirm'])
def test_source_only_provider_decisions_reject_repetitions(research, provider, purpose):
    """Every provider rejects historical source replay as a new experiment."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    row['settings']['provider'] = provider
    source['executed_config']['bot']['short']['native_default'] = 0
    source['executed_config']['backtest']['ohlcv_source_dir'] = '/remote/old-job/input'
    before = copy.deepcopy(row)
    experiment = {'purpose': purpose, 'source_operation': 'original'}
    if purpose == 'confirm':
        experiment['budget_fraction'] = 1
    with pytest.raises(ValueError, match='forbidden'):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps({
            'variants': [{'experiment': experiment, 'reason': 'Repeat measured settings'}]}))
    with pytest.raises(ValueError, match='forbidden'):
        controller.job(row, 'optimizer', None, None, 'Replay source', experiment=experiment)
    assert row == before


@pytest.mark.parametrize('source', ['unknown', None, '../outside'])
def test_source_only_replay_cannot_resolve_unowned_inputs(research, source):
    """An operation reference never becomes an arbitrary filesystem read."""
    _, row, _ = research
    with pytest.raises(ValueError, match='forbidden'):
        resolve_variant(row, {'experiment': {'purpose': 'replicate', 'source_operation': source}})


def test_replay_retains_reviewed_workers_and_source_overrides(research):
    """Native GPU sizing and old worker values never become AI-owned replay settings."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    source['overrides'] = {'HYPE': {'bot': {'long': {'example': 1}}}}
    source['executed_config']['optimize']['gpu'] = {'population_size': 8192}
    config = source_config(row, source)
    assert config['optimize'].get('gpu') == row['initial_config']['optimize'].get('gpu')
    assert source_config(row, source)['optimize'].get('gpu') == row['initial_config']['optimize'].get('gpu')
    with pytest.raises(ValueError, match='forbidden'):
        resolve_variant(row, {'experiment': {'purpose': 'replicate', 'source_operation': 'original'}})


def test_source_input_archives_are_shared_bound_and_seed_free(research):
    """Every old source remains exactly readable without growing mandatory model context."""
    store, row, controller = research
    source = optimizer(row, controller, 'old-source')
    source['config']['optimize']['seed'] = 7654321
    source['executed_config']['optimize']['seed'] = 7654321
    references, archives = source_inputs(row)
    raw = {'stage': 'evaluate', 'rubric': [], 'optimizer_sources': references,
           'evidence': {'optimizer_research': {}, 'winner_options': {}}}
    required = required_content(raw)
    snapshots = [EvidenceStore(store).create(row, required, raw, archives) for _ in range(2)]
    assert snapshots[0]['history'] == snapshots[1]['history']
    assert 'old-source' not in json.dumps(required)
    assert required['context_references']['optimizer_sources']['path'] == '/data/optimizer_sources'
    path = references['old-source']['config_path']
    resolved = EvidenceStore(store).resolve(snapshots[0], path)
    assert signature(resolved, {}) == signature(source['executed_config'], {})
    assert resolved['optimize']['seed'] is None
    source['executed_config']['optimize']['bounds']['changed_later'] = [0, 1]
    assert EvidenceStore(store).resolve(snapshots[0], path) == resolved
    session = EvidenceSession(EvidenceStore(store), snapshots[0], 1500)
    try:
        page = session.read({'path': path, 'offset': 0}, 'source-config')
        assert page and '7654321' not in json.dumps(page)
    finally:
        session.close()
    with pytest.raises(ValueError, match='outside the decision manifest'):
        EvidenceStore(store).resolve({**snapshots[0], 'history': {}}, path)
    old = copy.deepcopy(snapshots[0])
    old['required'].pop('optimizer_source_version')
    assert decision_snapshot_stale(old)



def test_modified_source_archive_cannot_be_rebound_as_original(research):
    """Storage corruption cannot silently turn an original-input reference into another config."""
    from pb8_loop_evidence import LoopAIPause
    store, row, controller = research
    optimizer(row, controller, 'original')
    references, archives = source_inputs(row)
    raw = {'stage': 'evaluate', 'rubric': [], 'optimizer_sources': references,
           'evidence': {'optimizer_research': {}, 'winner_options': {}}}
    snapshot = EvidenceStore(store).create(row, required_content(raw), raw, archives)
    binding = next(iter(snapshot['history'].values()))
    path = store.owner_dir(row['owner']) / 'evidence' / (binding['key'] + '.json')
    corrupt = json.loads(path.read_text())
    corrupt['config']['optimize']['bounds']['changed'] = [0, 1]
    path.write_text(json.dumps(corrupt))
    with pytest.raises(LoopAIPause, match='safely stored'):
        EvidenceStore(store).create(row, required_content(raw), raw, archives)


def test_legacy_variants_still_require_explicit_complete_configs(research):
    """Source-only replay is restricted to the versioned research contract."""
    _, row, _ = research
    row.pop('optimizer_research_version')
    variant = {'experiment': {'purpose': 'replicate', 'source_operation': 'original'}}
    assert resolve_variant(row, variant) == variant
    with pytest.raises(ValueError):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps({'variants': [variant]}))
