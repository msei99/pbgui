"""Offline adversarial verification of the explicit user-reviewed change bridge."""
import asyncio
import copy
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from ai_capabilities import AICapabilityError, AICapabilityService
from ai_reviewed_changes import ACTION, ReviewedConfigChanges
from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text

OWNER, CID = 'a' * 32, 'b' * 32
CONFIG = {'bot': {'long': {'n_positions': 3}, 'short': {'n_positions': 0}},
          'live': {'approved_coins': {'long': ['BTC', 'ETH'], 'short': []}, 'ignored_coins': {'long': [], 'short': []}}}


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Use only temporary configs, real file locks and mocked offline runtime validation."""
    import ai_capabilities
    import pb8_config
    import pb7_config
    from api import optimize_v8, backtest_v8
    monkeypatch.setattr(ai_capabilities, 'PBGDIR', str(tmp_path))
    caps = AICapabilityService(tmp_path / 'ai')
    roots = {('optimizer', 'v8'): tmp_path / 'data/opt_v8',
             ('backtest', 'v8'): tmp_path / 'data/bt_v8',
             ('backtest', 'v7'): tmp_path / 'data/bt_v7',
             ('run', 'v8'): tmp_path / 'data/run_v8', ('run', 'v7'): tmp_path / 'data/run_v7'}
    paths = {}
    for (kind, version), root in roots.items():
        path = root / 'test' / ('backtest.json' if kind == 'backtest' else 'config.json')
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(CONFIG))
        paths[(kind, version)] = path
    for kind, module in [('optimizer', optimize_v8), ('backtest', backtest_v8)]:
        root = roots[(kind, 'v8')]
        monkeypatch.setattr(module, '_configs_dir', lambda root=root: root)
        monkeypatch.setattr(module, '_config_file', lambda name, kind=kind: paths[(kind, 'v8')].parent.parent / name / paths[(kind, 'v8')].name)
        monkeypatch.setattr(module, '_config_lock', lambda root=root: advisory_file_lock(root / '.write'))
    monkeypatch.setattr(pb8_config, 'load_pb8_config', lambda path: json.loads(Path(path).read_text()))
    monkeypatch.setattr(pb7_config, 'load_pb7_config', lambda path: json.loads(Path(path).read_text()))
    monkeypatch.setattr(pb8_config, 'prepare_pb8_config', lambda config, **kw: copy.deepcopy(config))
    monkeypatch.setattr(caps, '_validate_pb8_config', lambda name, config: copy.deepcopy(config))
    monkeypatch.setattr(caps, '_runtime_fingerprint', lambda version: {'state_digest': 'test'})
    monkeypatch.setattr('api.pb7_bridge.prepare_override_config', lambda config, **kw: copy.deepcopy(config))
    writes = []
    def save(config, path):
        """Stand in for the canonical atomic writer, recording every actual effect."""
        writes.append(str(path))
        atomic_write_private_text(path, json.dumps(config))
        return config
    monkeypatch.setattr(pb8_config, 'save_prepared_pb8_config', save)
    # A live save or remote operation would fail this test immediately.
    monkeypatch.setattr('api.v8_instances.save_v8_instance_config', AsyncMock(side_effect=AssertionError('Live save forbidden')))
    monkeypatch.setattr('api.v7_instances.save_instance_config', AsyncMock(side_effect=AssertionError('Live save forbidden')))
    return caps, paths, writes


def args(kind='backtest', version='v8', path='/live/approved_coins/long', value=None):
    """Build a small typed proposal without hidden command or execution arguments."""
    return {'kind': kind, 'version': version, 'name': 'test', 'operations': [
        {'op': 'replace', 'path': path, 'value': ['BTC'] if value is None else value}]}


async def propose(caps, **kwargs):
    """Create through the model-facing dispatcher, not privileged executor methods."""
    receipt = await caps.dispatch(OWNER, CID, 'propose_reviewed_config_change', args(**kwargs))
    assert 'payload_digest' not in receipt and 'review_token' not in receipt
    return next(item for item in await caps.list_proposals(OWNER, CID) if item['proposal_id'] == receipt['proposal_id'])


async def ticket(caps, item, **kwargs):
    """Simulate the authenticated browser-only review endpoint."""
    return await caps.reviewed_changes.review(kwargs.get('owner', OWNER), item['proposal_id'], kwargs.get('cid', CID), kwargs.get('digest', item['payload_digest']))


async def apply(caps, item, token='', **kwargs):
    """Submit exactly the approved binding, optionally with attack substitutions."""
    return await caps.approve(kwargs.get('owner', OWNER), item['proposal_id'], kwargs.get('digest', item['payload_digest']), kwargs.get('cid', CID), review_token=token)


@pytest.mark.parametrize('kind', ['backtest', 'optimizer'])
def test_only_real_review_ticket_allows_exact_save_and_no_unrestriction(env, kind):
    """Staging and review are inert; a consumed ticket permits only the bound field change."""
    caps, paths, writes = env
    async def run():
        item = await propose(caps, kind=kind)
        assert caps.analysis_only(OWNER, CID)
        assert not writes
        assert item['preview']['changes'][0]['path'] == 'live.approved_coins.long'
        with pytest.raises(AICapabilityError, match='review'):
            await apply(caps, item)
        reviewed = await ticket(caps, item)
        assert not writes and 'review_token' not in json.dumps(item)
        result = await apply(caps, item, reviewed['review_token'])
        expected = copy.deepcopy(CONFIG)
        expected['live']['approved_coins']['long'] = ['BTC']
        assert json.loads(paths[(kind, 'v8')].read_text()) == expected
        assert result['mode'] == 'save' and len(writes) == 1
        assert caps.analysis_only(OWNER, CID)
        with pytest.raises(AICapabilityError):
            await apply(caps, item, reviewed['review_token'])
        assert len(writes) == 1
        assert not list(caps.journal_root.glob('*.json'))
        for tool in ('perform_page_action', 'propose_python_analysis', 'propose_queue_pb8_config'):
            with pytest.raises(AICapabilityError, match='analysis-only'):
                await caps.dispatch(OWNER, CID, tool, {})
    asyncio.run(run())


@pytest.mark.parametrize('kind,version', [('run', 'v8'), ('run', 'v7'), ('backtest', 'v7')])
def test_live_sources_only_create_exact_private_draft(env, kind, version):
    """No live-save or deployment entry point is called, even after explicit approval."""
    caps, paths, writes = env
    async def run():
        item = await propose(caps, kind=kind, version=version)
        assert item['preview']['mode'] == 'draft'
        reviewed = await ticket(caps, item)
        result = await apply(caps, item, reviewed['review_token'])
        assert result['mode'] == 'draft' and not writes
        assert json.loads(paths[(kind, version)].read_text()) == CONFIG
        draft = caps._load_draft(OWNER, result['draft_id'])
        assert draft['config']['live']['approved_coins']['long'] == ['BTC']
        assert draft['validation']['valid']
    asyncio.run(run())


@pytest.mark.parametrize('attack', ['owner', 'conversation', 'digest', 'token', 'expired', 'restart'])
def test_review_binding_and_expiry_fail_closed(env, attack):
    """Wrong identity, modified content, stolen/replayed or expired tokens cannot apply."""
    caps, _, writes = env
    async def run():
        item = await propose(caps)
        reviewed = await ticket(caps, item)
        token = reviewed['review_token']
        kw = {}
        if attack == 'owner': kw['owner'] = 'c' * 32
        if attack == 'conversation': kw['cid'] = 'd' * 32
        if attack == 'digest': kw['digest'] = 'sha256:' + '0' * 64
        if attack == 'token': token = 'forged'
        if attack == 'expired': caps.reviewed_changes.tickets[item['proposal_id']]['expires'] = 0
        if attack == 'restart': caps.reviewed_changes = ReviewedConfigChanges(caps)
        with pytest.raises(AICapabilityError):
            await apply(caps, item, token, **kw)
        assert not writes
    asyncio.run(run())


@pytest.mark.parametrize('when', ['before_review', 'after_review', 'override'])
def test_changed_config_or_override_invalidates_review(env, when):
    """The full saved bundle is checked again under the same lock used by GUI writers."""
    caps, paths, writes = env
    async def run():
        item = await propose(caps)
        reviewed = await ticket(caps, item) if when != 'before_review' else None
        path = paths[('backtest', 'v8')]
        if when == 'override':
            (path.parent / 'BTC.json').write_text('{}')
        else:
            changed = copy.deepcopy(CONFIG)
            changed['bot']['long']['n_positions'] = 10
            path.write_text(json.dumps(changed))
        with pytest.raises(AICapabilityError, match='changed'):
            if reviewed: await apply(caps, item, reviewed['review_token'])
            else: await ticket(caps, item)
        assert not writes
    asyncio.run(run())


@pytest.mark.parametrize('part', ['config', 'preview', 'mode', 'target'])
def test_proposal_tampering_is_detected(env, part):
    """The exact displayed review and target are part of the immutable payload binding."""
    caps, _, writes = env
    async def run():
        item = await propose(caps)
        reviewed = await ticket(caps, item)
        stored = caps.proposals[item['proposal_id']]
        if part == 'config': stored.config['config']['bot']['long']['n_positions'] = 999
        if part == 'preview': stored.preview['changes'] = []
        if part == 'mode': stored.config['mode'] = 'deploy'
        if part == 'target': stored.name = 'another'
        with pytest.raises(AICapabilityError):
            await apply(caps, item, reviewed['review_token'])
        assert not writes
    asyncio.run(run())


@pytest.mark.parametrize('path,value', [
    ('/pbgui/enabled_on', 'host'), ('/live/user', 'other'), ('/live/api_key', 'secret'),
    ('/backtest/base_dir', '/tmp/escape'), ('/bot/long', {'n_positions': 99}),
    ('/live/approved_coins', '/etc/passwd'), ('/live/approved_coins/long', ['../secret']),
    ('/live/approved_coins/long', ['BTC; restart']), ('/bot/long/n_positions', 'exec(code)'),
    ('/bot/long/n_positions', float('inf')), ('/bot/long/n_positions', True),
])
def test_unsafe_paths_and_values_are_rejected_before_staging(env, path, value):
    """Trading changes cannot become command execution, credentials or resource paths."""
    caps, _, writes = env
    async def run():
        with pytest.raises((AICapabilityError, ValueError)):
            await propose(caps, path=path, value=value)
        assert not caps.proposals and not writes
    asyncio.run(run())


def test_forged_model_approval_tool_and_extra_arguments_are_rejected(env):
    """There is no model-facing tool for reviewing, issuing a ticket or executing changes."""
    caps, _, writes = env
    async def run():
        caps.restrict_to_analysis(OWNER, CID)
        for tool in ('approve', 'review_config_change', 'execute_reviewed_config_change'):
            with pytest.raises(AICapabilityError):
                await caps.dispatch(OWNER, CID, tool, {})
        request = args()
        request['action'] = 'save_and_queue'
        with pytest.raises(AICapabilityError):
            await caps.dispatch(OWNER, CID, 'propose_reviewed_config_change', request)
        assert not writes
    asyncio.run(run())


def test_concurrent_replay_executes_at_most_once(env):
    """Two racing approval requests cannot share a consumed one-use ticket."""
    caps, _, writes = env
    async def run():
        item = await propose(caps)
        reviewed = await ticket(caps, item)
        results = await asyncio.gather(*(apply(caps, item, reviewed['review_token']) for _ in range(2)), return_exceptions=True)
        assert sum(isinstance(result, dict) for result in results) == 1
        assert sum(isinstance(result, AICapabilityError) for result in results) == 1
        assert len(writes) == 1
    asyncio.run(run())


def test_credentials_and_symlink_sources_never_enter_proposals(env):
    """Credential-bearing or redirected resources are rejected without copying secrets."""
    caps, paths, writes = env
    async def run():
        path = paths[('backtest', 'v8')]
        config = copy.deepcopy(CONFIG)
        config['credentials'] = {'password': 'do-not-copy'}
        path.write_text(json.dumps(config))
        with pytest.raises(AICapabilityError, match='Credential'):
            await propose(caps)
        assert not list(caps.proposal_root.rglob('*.json'))
        target = path.with_suffix('.real')
        path.rename(target)
        path.symlink_to(target)
        with pytest.raises(AICapabilityError):
            await propose(caps)
        assert not writes
    asyncio.run(run())


def test_interrupted_changes_are_not_replayed_after_restart(env):
    """An uncertain in-flight outcome is marked interrupted, never retried automatically."""
    caps, _, writes = env
    async def run():
        item = await propose(caps)
        stored = caps.proposals[item['proposal_id']]
        stored.status = 'executing'
        caps._persist_proposal(stored)
        restored = AICapabilityService(caps.root)
        await restored.startup()
        proposal = await restored._owned_proposal(OWNER, item['proposal_id'])
        assert proposal.status == 'interrupted'
        with pytest.raises(AICapabilityError):
            await restored.reviewed_changes.review(OWNER, item['proposal_id'], CID, item['payload_digest'])
        assert not writes
    asyncio.run(run())


def test_proposal_expiry_and_rejection_revoke_approval(env):
    """A review ticket cannot revive an expired or rejected proposal."""
    caps, _, writes = env
    async def run():
        item = await propose(caps)
        reviewed = await ticket(caps, item)
        caps.proposals[item['proposal_id']].created_at = time.time() - 601
        with pytest.raises(AICapabilityError, match='expired'):
            await apply(caps, item, reviewed['review_token'])
        caps.proposals[item['proposal_id']].created_at = time.time()
        await caps.reject(OWNER, item['proposal_id'], item['payload_digest'], CID)
        with pytest.raises(AICapabilityError):
            await apply(caps, item, reviewed['review_token'])
        assert not writes
    asyncio.run(run())


def test_approved_api_change_has_no_automatic_model_continuation(env, monkeypatch):
    """The authenticated API consumes the explicit ticket and returns only an inert receipt."""
    from types import SimpleNamespace
    from api import ai as module
    caps, _, writes = env
    chat = SimpleNamespace(_ensure_owner_loaded=AsyncMock(), _owned_conversation=Mock(return_value=SimpleNamespace(busy=False)), start_turn=AsyncMock())
    monkeypatch.setattr(module, 'get_ai_capability_service', lambda: caps)
    monkeypatch.setattr(module, 'get_ai_chat_service', lambda: chat)
    monkeypatch.setattr(module, '_owner', lambda session: OWNER)
    async def run():
        item = await propose(caps)
        body = module.ProposalDecisionRequest(payload_digest=item['payload_digest'], conversation_id=CID)
        reviewed_response = await module.review_config_change(item['proposal_id'], body, object())
        assert reviewed_response.headers['cache-control'] == 'no-store'
        review = json.loads(reviewed_response.body)
        body.review_token = review['review_token']
        response = await module.approve_proposal(item['proposal_id'], body, object())
        assert response.headers['cache-control'] == 'no-store'
        assert json.loads(response.body)['action'] == ACTION
        assert len(writes) == 1
        chat.start_turn.assert_not_awaited()
    asyncio.run(run())


def test_file_overrides_are_not_silently_dropped_from_drafts(env):
    """Unsupported file references fail before a proposal can misrepresent a live strategy."""
    caps, paths, writes = env
    async def run():
        config = copy.deepcopy(CONFIG)
        config['coin_overrides'] = {'BTC': {'override_config_path': 'BTC.json'}}
        paths[('run', 'v8')].write_text(json.dumps(config))
        with pytest.raises(AICapabilityError, match='overrides'):
            await propose(caps, kind='run')
        assert not caps.proposals and not writes
    asyncio.run(run())
