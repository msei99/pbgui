"""Offline native recovery planning preserves evidence and real output/call limits."""
import copy
import math

import pytest

from pb8_loop_ai_policy import check_context, evidence_session
from pb8_loop_ai_v2 import validate_snapshot_budget
from pb8_loop_evidence import EvidenceSession, LoopAIPause, tokens
from tests.test_pb8_loop_optimizer import record


@pytest.fixture(scope='module')
def large_snapshot():
    """Represent a growing exact comparison without production data or model calls."""
    return {'id': 'a' * 32, 'sha256': 'b' * 64, 'required': {
        'stage': 'evaluate', 'safety': {'liquidated': 0},
        'measurements': '0123456789abcdef ' * 40000}}


@pytest.mark.parametrize('provider', ['claude-code', 'chatgpt'])
def test_native_resume_and_compaction_fit_growing_exact_evidence(record, large_snapshot, provider):
    """More than 48 old pages fit without dropping values or expanding call quotas."""
    _, row = record
    row['settings']['provider'] = provider
    profile = {'context': 1000000, 'page_tokens': 5000, 'tool_output_tokens': 8192, 'output_tokens': 1000}
    before = copy.deepcopy(large_snapshot)
    old = EvidenceSession(None, large_snapshot)
    assert len(old.required_pages) > 48
    with pytest.raises(LoopAIPause, match='recovery call allowance'):
        check_context({}, profile, 1000, codex=True, required_pages=len(old.required_pages))
    validate_snapshot_budget(row, large_snapshot, {'loop_profile': profile, 'output_limit': 1000})
    planned = evidence_session(None, large_snapshot, profile, native=True)
    assert 5000 < planned.page_tokens <= 6144
    assert len(planned.required_pages) <= 48
    assert all(math.ceil(tokens(page) * 1.25) + 512 <= 8192 for page in planned.required_pages)
    fragments = [entry for page in planned.required_pages for entry in page['entries']
                 if entry['path'] == '/measurements']
    assert ''.join(entry['value'] for entry in fragments) == large_snapshot['required']['measurements']
    assert large_snapshot == before and profile['page_tokens'] == 5000
    planned.calls = planned.total_calls = 60 - len(planned.required_pages)
    planned.compacted()
    notice = planned.read({'path': '/data', 'offset': 0}, 'optional')
    assert notice['error'] == 'optional_evidence_budget_reserved'
    assert len(notice['required_reads']) == len(planned.required_pages)
    for number, args in enumerate(notice['required_reads']):
        planned.read(args, f'recover-{number}')
    assert not planned.missing() and planned.calls <= 64 and planned.total_calls <= 128


def test_larger_pages_do_not_bypass_measured_recovery_passes(record, large_snapshot):
    """An input that cannot safely recover still pauses before model dispatch."""
    _, row = record
    row['settings']['provider'] = 'claude-code'
    profile = {'context': 1000000, 'page_tokens': 5000, 'recovery_passes': 2}
    with pytest.raises(LoopAIPause, match='recovery call allowance'):
        validate_snapshot_budget(row, large_snapshot, {'loop_profile': profile, 'output_limit': 1000})


def test_small_and_narrow_output_profiles_keep_their_bounds():
    """Normal paging is unchanged and smaller native tool limits remain binding."""
    snapshot = {'id': 'c' * 32, 'sha256': 'd' * 64, 'required': {'values': list(range(500))}}
    assert evidence_session(None, snapshot, {}, native=True).page_tokens == 5000
    profile = {'page_tokens': 5000, 'tool_output_tokens': 2048}
    planned = evidence_session(None, snapshot, profile, native=True)
    assert all(math.ceil(tokens(page) * 1.25) + 512 <= 2048 for page in planned.required_pages)
    assert evidence_session(None, snapshot, profile).page_tokens == 5000
    with pytest.raises(LoopAIPause, match='tool-output allowance'):
        evidence_session(None, snapshot, {'tool_output_tokens': 1024}, native=True)
