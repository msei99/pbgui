"""Offline tests for reviewed AI Loop and Vast.ai configuration capabilities."""
import asyncio
import copy
from types import SimpleNamespace

import pytest

from ai_capabilities import AICapabilityService, AICapabilityError
from pb8_loop_store import LoopStore

OWNER = 'a' * 32
CHAT = 'b' * 32


@pytest.fixture
def environment(tmp_path, monkeypatch):
    """Isolate definitions, source configs, rentals and proposals from live data."""
    from api import loop_optimizer_v8 as loops, optimize_v8, vast
    import vast_queue
    import vast_credentials
    service = AICapabilityService(tmp_path / 'ai')
    store = LoopStore(tmp_path / 'loops')
    monkeypatch.setattr(loops, '_controller', SimpleNamespace(store=store))
    bundle = {'config': {'config_version': 'v8.6.0', 'bot': {}, 'backtest': {}}, 'override_configs': {}}
    monkeypatch.setattr(optimize_v8, 'get_config', lambda *a, **k: copy.deepcopy(bundle))
    from contextlib import nullcontext
    monkeypatch.setattr(optimize_v8, '_config_lock', nullcontext)
    monkeypatch.setattr(loops, 'migrate_loop_bundle', lambda value: value)
    monkeypatch.setattr(loops, 'config_defaults', lambda value: {'strategy': 'ema_anchor'})
    rental = vast.RentalPreferences(hours=12, budget=2).model_dump()
    applied = []

    class Queue:
        """Persist only in test memory; the lock uses a temporary directory."""
        root = tmp_path / 'vast'

        def read(self):
            """Return a detached snapshot like the real queue."""
            return {'gpu_preferences': copy.deepcopy(rental)}

    def apply(queue, values):
        """Record approval effects without starting a rental."""
        applied.append(copy.deepcopy(values))
        rental.update(values)

    monkeypatch.setattr(vast_queue, 'CloudQueue', Queue)
    monkeypatch.setattr(vast, '_apply_gpu_preferences', apply)
    monkeypatch.setattr(vast_credentials, 'VastCredentialStore', lambda root: SimpleNamespace(metadata=lambda: {'configured': True}))
    return service, store, rental, applied


def approve(service, result):
    """Approve exactly the immutable proposal returned to this test owner."""
    proposal = service.proposals[result['proposal_id']]
    return service.approve(OWNER, proposal.id, proposal.payload_digest, CHAT)


def test_vast_rtx3060_settings_require_approval_and_preserve_other_limits(environment):
    """Two rentals must change GPU requirements, not optimizer variant counts."""
    service, _, rental, applied = environment

    async def scenario():
        current = await service.dispatch(OWNER, CHAT, 'get_vast_preferences', {})
        created = await service.dispatch(OWNER, CHAT, 'propose_vast_preferences', {
            'settings_digest': current['settings_digest'], 'settings': {'gpu_name': 'RTX 3060', 'max_rentals': 2}})
        assert applied == []
        assert rental['max_rentals'] == 1
        result = await approve(service, created)
        assert result['settings']['gpu_name'] == 'RTX 3060'
        assert result['settings']['max_rentals'] == 2
        assert result['settings']['budget'] == 2
        assert result['settings']['auto_rent'] is False
        await approve(service, created)
        assert len(applied) == 1
        # Recovery after persistence but before journal completion is idempotent too.
        service.loop_tools.execute(service.proposals[created['proposal_id']])
        assert len(applied) == 1
    asyncio.run(scenario())


def test_loop_setup_tools_expose_pinned_gpu_metric_policy(environment):
    """The assistant can inspect eligibility before preparing a fresh source config."""
    from vast_config_validation import METRICS, _METRIC_CONTRACT
    service, _, _, _ = environment
    async def scenario():
        """Read the setup catalog without creating proposals or jobs."""
        response = await service.dispatch(OWNER, CHAT, 'get_ai_loop_configs', {})
        policy = response['optimizer_metric_policies']['vast']
        assert set(policy['allowed_metrics']) == METRICS
        assert set(policy['exact_only_metrics']) == set(_METRIC_CONTRACT['exact_only_metrics'])
        assert not set(policy['allowed_metrics']) & set(policy['exact_only_metrics'])
        assert policy['applies_to'] == ['optimize.scoring', 'optimize.limits']
    asyncio.run(scenario())


def test_cloud_loop_proposal_rejects_unsupported_scoring_before_review(environment, monkeypatch):
    """No approval card or definition is created for an incompatible GPU metric."""
    from api import optimize_v8
    service, store, _, _ = environment
    bundle = {'config': {'optimize': {'scoring': [{'metric': 'gain_strategy_eq', 'goal': 'max'}]}},
              'override_configs': {}}
    monkeypatch.setattr(optimize_v8, 'get_config', lambda *args, **kwargs: copy.deepcopy(bundle))
    async def scenario():
        """Exercise the public capability dispatcher without model or rental access."""
        with pytest.raises(AICapabilityError, match='gain_strategy_eq'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
                'name': 'invalid_cloud', 'config_name': 'HYPE', 'settings': {'execution': 'vast', 'hours': 12}})
        assert store.definitions(OWNER) == []
        assert not service.proposals
    asyncio.run(scenario())


def test_loop_create_edit_and_owner_isolation(environment):
    """Definitions are reviewable, revision protected, and never start a run."""
    service, store, _, _ = environment

    async def scenario():
        result = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
            'name': 'HYPE_loop', 'config_name': 'HYPE', 'settings': {
                'execution': 'vast', 'hours': 12, 'max_runs': 25, 'run_limit_mode': 'proxy',
                'run_proxy': 100000, 'goals': {'coins': ['HYPE'], 'direction': 'long',
                    'presets': ['gain','drawdown','uptrend'], 'targets': {'drawdown': 0.5}}}})
        assert store.definitions(OWNER) == []
        await approve(service, result)
        saved = store.definition(OWNER, 'HYPE_loop')
        assert saved['settings']['max_runs'] == 25
        assert saved['settings']['run_proxy'] == 100000
        assert saved['settings']['parallel'] == 1
        # Replaying the same persisted action does not create a new revision.
        service.loop_tools.execute(service.proposals[result['proposal_id']])
        assert store.definition(OWNER, 'HYPE_loop')['revision'] == 1
        other = await service.dispatch('c'*32, CHAT, 'get_ai_loop_configs', {})
        assert other['configs'] == []
        current = await service.dispatch(OWNER, CHAT, 'get_ai_loop_configs', {'name': 'HYPE_loop'})
        changed = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
            'name': 'HYPE_loop', 'config_name': 'HYPE', 'revision': current['revision'], 'settings': {'max_runs': 30}})
        await approve(service, changed)
        saved = store.definition(OWNER, 'HYPE_loop')
        assert saved['revision'] == 2
        assert saved['settings']['goals']['coins'] == ['HYPE']
    asyncio.run(scenario())


def test_vast_stale_approval_cannot_overwrite_new_user_settings(environment):
    """A settings edit made during review invalidates the cloud proposal."""
    service, _, rental, applied = environment

    async def scenario():
        current = await service.dispatch(OWNER, CHAT, 'get_vast_preferences', {})
        proposal = await service.dispatch(OWNER, CHAT, 'propose_vast_preferences', {
            'settings_digest': current['settings_digest'], 'settings': {'max_rentals': 2}})
        rental['budget'] = 3
        with pytest.raises(AICapabilityError, match='preferences changed'):
            await approve(service, proposal)
        assert applied == []
    asyncio.run(scenario())


def test_loop_stale_revision_is_rejected(environment):
    """An approved old proposal cannot overwrite newer manual definition edits."""
    service, store, _, _ = environment

    async def scenario():
        p = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
            'name': 'demo', 'config_name': 'HYPE', 'settings': {}})
        await approve(service, p)
        p = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
            'name': 'demo', 'config_name': 'HYPE', 'revision': 1, 'settings': {'max_runs': 25}})
        row = store.definition(OWNER, 'demo')
        row['settings']['max_runs'] = 12
        store.save_definition(OWNER, 'demo', row['settings'], row['bundle'], row['source'], 1)
        with pytest.raises(AICapabilityError, match='changed'):
            await approve(service, p)
        assert store.definition(OWNER, 'demo')['settings']['max_runs'] == 12
    asyncio.run(scenario())


@pytest.mark.parametrize('settings', [{'max_rentals': 0}, {'api_key': 'not-allowed'}, {'budget': float('inf')}])
def test_invalid_rental_settings_cannot_create_proposals(environment, settings):
    """Native bounds and unknown-field rejection apply to model-authored changes."""
    service, _, rental, _ = environment
    with pytest.raises(ValueError):
        service.loop_tools._prepare_vast({'settings_digest': service._digest(service.loop_tools._rental()[1].model_dump()), 'settings': settings})
    assert service.proposals == {}


def test_tools_are_exposed_to_all_provider_catalogs(environment):
    """ChatGPT, Responses, Chat Completions and Messages see the same tools."""
    service, _, _, _ = environment
    expected = {'get_ai_loop_configs', 'get_vast_preferences', 'propose_ai_loop_config', 'propose_vast_preferences'}
    assert expected <= {tool['function']['name'] for tool in service.chat_completion_tools()}
    assert expected <= {tool['name'] for tool in service.responses_tools()}
    assert expected <= {tool['name'] for tool in service.messages_tools()}
    assert expected <= {tool['name'] for namespace in service.codex_dynamic_tools() for tool in namespace['tools']}


def test_loop_and_vast_mutations_remain_blocked_in_analysis_only_chat(environment):
    """Research evidence must not unlock new configuration mutation capabilities."""
    service, _, _, _ = environment
    service.restrict_to_analysis(OWNER, CHAT)

    async def scenario():
        for tool in ('propose_ai_loop_config', 'propose_vast_preferences'):
            with pytest.raises(AICapabilityError):
                await service.dispatch(OWNER, CHAT, tool, {})
    asyncio.run(scenario())


def test_vast_auto_rent_review_discloses_paid_and_shared_effects(environment):
    """Automation is never enabled by constructing a GPU configuration proposal."""
    service, _, rental, applied = environment
    _, preview = service.loop_tools._prepare_vast({
        'settings_digest': service._digest(service.loop_tools._rental()[1].model_dump()),
        'settings': {'auto_rent': True, 'gpu_name': 'RTX 3060', 'max_rentals': 2}})
    assert preview['may_start_immediately'] is True
    assert preview['maximum_rental_budgets_usd'] == 4
    assert 'all cloud jobs' in preview['scope']
    assert rental['auto_rent'] is False
    assert applied == []


def test_recovery_after_loop_save_does_not_duplicate_definition(environment):
    """An interrupted approval journal can complete without a second config save."""
    service, store, _, _ = environment

    async def scenario():
        created = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
            'name': 'recovered', 'config_name': 'HYPE', 'settings': {}})
        proposal = service.proposals[created['proposal_id']]
        journal = service._journal_payload(proposal, phase='prepared')
        service._write_private_json(service.journal_root / f'{proposal.id}.json', journal)
        service.loop_tools.execute(proposal)
        service._recover_journals()
        assert store.definition(OWNER, 'recovered')['revision'] == 1
        assert service._load_journal(proposal.id)['phase'] == 'completed'
    asyncio.run(scenario())


def test_followup_preserves_configuration_approval_but_delete_rejects_it(environment):
    """A clarification answer retains the card; explicit conversation removal does not."""
    service, _, _, _ = environment

    async def scenario():
        current = await service.dispatch(OWNER, CHAT, 'get_vast_preferences', {})
        created = await service.dispatch(OWNER, CHAT, 'propose_vast_preferences', {
            'settings_digest': current['settings_digest'], 'settings': {'max_rentals': 2}})
        await service.reject_conversation(OWNER, CHAT, preserve_configurations=True)
        pending = await service.list_proposals(OWNER, CHAT)
        assert any(p['proposal_id'] == created['proposal_id'] for p in pending)
        current = await service.dispatch(OWNER, CHAT, 'get_vast_preferences', {})
        assert current['pending_proposals'][0]['proposal_id'] == created['proposal_id']
        await service.reject_conversation(OWNER, CHAT)
        assert not await service.list_proposals(OWNER, CHAT)
        current = await service.dispatch(OWNER, CHAT, 'get_vast_preferences', {})
        assert current['pending_proposals'] == []
    asyncio.run(scenario())


@pytest.fixture
def native_runs(environment, monkeypatch):
    """Exercise reviewed tools through native interfaces using isolated controller state."""
    from api import loop_optimizer_v8 as runtime
    from fastapi.responses import JSONResponse
    service, store, rental, applied = environment
    selection = {'provider': 'chatgpt', 'model': 'gpt-test', 'profile': 'main', 'effort': 'high', 'service_tier': ''}
    monkeypatch.setattr(runtime, 'get_ai_chat_service', lambda: SimpleNamespace(get_preferences=lambda owner: {'selection': selection}))
    settings = runtime.LoopStart(config_name='HYPE', provider='chatgpt', model='gpt-test',
        execution='vast', hours=12, max_runs=25, parallel=1, run_limit_mode='proxy', run_proxy=100000).model_dump(
            exclude={'authorization', 'provider', 'model', 'profile', 'effort', 'service_tier'})
    store.save_definition(OWNER, 'native_loop', settings, {'config': {}, 'override_configs': {}}, {'kind': 'config', 'id': 'HYPE'}, None)
    calls = []

    async def queue(body, session=None, definition=None, *, owner=None, queue_proposal=None):
        """Create only a temporary native run; no AI or rental controller exists."""
        calls.append(('queue', owner, body.authorization, body.model))
        values = body.model_dump(exclude={'authorization'})
        values.update(loop_name=definition['name'], definition_name=definition['name'], ai_queue_proposal=queue_proposal)
        row = store.create(owner, values, {}, {}, {}, [], {}, queued=True)
        return JSONResponse({'id': row['id']}, status_code=201)

    async def start(owner, loop_id, body):
        """Record native start arguments without scheduling a real job."""
        calls.append(('start', owner, loop_id, body.selection))
        def change(row):
            row['status'] = 'running'
            row['control_generation'] += 1
        row = store.update(owner, loop_id, change)
        return JSONResponse({'id': row['id'], 'status': row['status']})

    monkeypatch.setattr(runtime, '_start_loop', queue)
    monkeypatch.setattr(runtime, '_loop_action_owned', start)
    return service, store, rental, calls, start


def test_native_queue_proposal_rejects_incompatible_saved_source(native_runs):
    """Existing definitions are rechecked before a queue/start approval is offered."""
    service, store, _, calls, _ = native_runs
    row = store.definition(OWNER, 'native_loop')
    row['bundle']['config'] = {'optimize': {'scoring': [{'metric': 'gain_strategy_eq', 'goal': 'max'}]}}
    updated = store.save_definition(OWNER, 'native_loop', row['settings'], row['bundle'],
                                    {'kind': 'config', 'id': 'HYPE'}, row['revision'])
    async def scenario():
        """Never reach the native queue backend for an unsupported metric."""
        with pytest.raises(AICapabilityError, match='gain_strategy_eq'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
                'operation': 'queue_and_start', 'name': 'native_loop', 'revision': updated['revision']})
        assert calls == [] and store.list(OWNER) == []
    asyncio.run(scenario())


@pytest.mark.parametrize('operation', ['queue', 'queue_and_start'])
def test_native_loop_queue_requires_approval_and_is_idempotent(native_runs, operation):
    """Approved operations call native APIs once, retain owner and freeze model choice."""
    service, store, _, calls, _ = native_runs

    async def scenario():
        created = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
            'operation': operation, 'name': 'native_loop', 'revision': 1})
        assert store.list(OWNER) == [] and calls == []
        assert created['preview']['may_start_immediately'] == (operation == 'queue_and_start')
        with pytest.raises(AICapabilityError):
            await service.approve('c' * 32, created['proposal_id'], service.proposals[created['proposal_id']].payload_digest, CHAT)
        result = await approve(service, created)
        assert result['run_status'] == ('queued' if operation == 'queue' else 'running')
        assert result['id'] == store.list(OWNER)[0]['id']
        assert all(call[1] == OWNER for call in calls)
        assert calls[0][2:] == (True, 'gpt-test')
        assert len(calls) == (1 if operation == 'queue' else 2)
        assert await approve(service, created) == result
        replay = await service.loop_tools.execute_run(service.proposals[created['proposal_id']])
        assert replay['id'] == result['id'] and len(store.list(OWNER)) == 1
        assert len(calls) == (1 if operation == 'queue' else 2)
        listed = await service.dispatch(OWNER, CHAT, 'get_ai_loop_runs', {'loop_id': result['id']})
        assert listed['runs'][0]['status'] == result['run_status']
        with pytest.raises(AICapabilityError):
            await service.dispatch('c' * 32, CHAT, 'get_ai_loop_runs', {'loop_id': result['id']})
    asyncio.run(scenario())


@pytest.mark.parametrize('operation', ['queue', 'queue_and_start', 'start'])
def test_native_gpu_loop_uses_owned_chat_without_saved_selection(native_runs, tmp_path, monkeypatch, operation):
    """Local GPU proposals freeze the active chat selection when preferences are absent."""
    from ai_chat import AIChatService, Conversation
    from api import loop_optimizer_v8 as runtime
    service, store, _, calls, _ = native_runs
    chat = AIChatService(tmp_path / 'chat')
    conversation = Conversation(id=CHAT, owner=OWNER, provider='chatgpt', model='chat-model',
                                chatgpt_profile='gpu-profile', effort='high', service_tier='priority')
    chat.conversations[CHAT] = conversation
    chat.loaded_owners.add(OWNER)
    monkeypatch.setattr(runtime, 'get_ai_chat_service', lambda: chat)
    definition = store.definition(OWNER, 'native_loop')
    definition['settings']['execution'] = 'gpu'
    store.save_definition(OWNER, 'native_loop', definition['settings'], definition['bundle'], definition['source'], 1)

    async def scenario():
        """Use isolated native calls and change the chat after review to check freezing."""
        assert 'selection' not in chat.get_preferences(OWNER)
        args = {'operation': operation, 'name': 'native_loop', 'revision': 2}
        if operation == 'start':
            queued = store.create(OWNER, definition['settings'], {}, {}, {}, [], {}, queued=True)
            args = {'operation': 'start', 'loop_id': queued['id']}
        created = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', args)
        settings = created['preview']['settings']
        assert settings['execution'] == 'gpu'
        assert settings['provider'] == 'chatgpt' and settings['model'] == 'chat-model'
        assert (settings['profile'], settings['effort'], settings['service_tier']) == ('gpu-profile', 'high', 'priority')
        assert calls == []
        conversation.model = 'changed-model'
        result = await approve(service, created)
        assert result['run_status'] == ('queued' if operation == 'queue' else 'running')
        if operation != 'start':
            assert calls[0][2:] == (True, 'chat-model')
        if operation != 'queue':
            assert calls[-1][3]['model'] == 'chat-model'
        conversation.owner = 'c' * 32
        with pytest.raises(AICapabilityError, match='owned chat'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', args)
    asyncio.run(scenario())


@pytest.mark.parametrize('changed', ['definition', 'rental'])
def test_native_loop_run_rejects_changed_review(native_runs, changed):
    """A changed revision or rental exposure cannot start the reviewed job."""
    service, store, rental, calls, _ = native_runs

    async def scenario():
        created = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
            'operation': 'queue_and_start', 'name': 'native_loop', 'revision': 1})
        if changed == 'definition':
            row = store.definition(OWNER, 'native_loop')
            store.save_definition(OWNER, row['name'], row['settings'], row['bundle'], row['source'], 1)
        else:
            rental['budget'] = 3
        with pytest.raises(AICapabilityError, match='changed since review'):
            await approve(service, created)
        assert calls == [] and store.list(OWNER) == []
    asyncio.run(scenario())


def test_failed_native_start_reuses_existing_queue_entry(native_runs, monkeypatch):
    """A partial queue/start failure is inspected and retried without creating another run."""
    from api import loop_optimizer_v8 as runtime
    from fastapi import HTTPException
    service, store, _, calls, original_start = native_runs

    async def fail(*args):
        """Reject a simulated transient runtime preflight."""
        raise HTTPException(409, 'AI runtime unavailable')

    monkeypatch.setattr(runtime, '_loop_action_owned', fail)

    async def scenario():
        created = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
            'operation': 'queue_and_start', 'name': 'native_loop', 'revision': 1})
        with pytest.raises(AICapabilityError, match='AI runtime unavailable'):
            await approve(service, created)
        queued = (await service.dispatch(OWNER, CHAT, 'get_ai_loop_runs', {}))['runs'][0]
        assert queued['status'] == 'queued' and len(store.list(OWNER)) == 1
        monkeypatch.setattr(runtime, '_loop_action_owned', original_start)
        start = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {'operation': 'start', 'loop_id': queued['id']})
        result = await approve(service, start)
        assert result['run_status'] == 'running' and result['id'] == queued['id']
        assert [call[0] for call in calls] == ['queue', 'start']
    asyncio.run(scenario())


@pytest.fixture
def diagnostic_run(environment, tmp_path, monkeypatch):
    """Create an owned failed run with isolated native job/log evidence."""
    from api import optimize_v8 as opt, backtest_v8 as bt
    from api.loop_optimizer_v8 import LoopStart
    import vast_jobs
    service, store, _, _ = environment
    row = store.create(OWNER, LoopStart(config_name='source', provider='chatgpt', model='test').model_dump(), {}, {}, {}, [], {})
    operation = 'd' * 32
    native = {'loop_id': row['id'], 'loop_owner': OWNER}
    root = tmp_path / 'logs'; root.mkdir()
    for api in (opt, bt):
        monkeypatch.setattr(api, '_read_json', lambda path: native)
        monkeypatch.setattr(api, '_queue_file', lambda name: root / (name + '.json'))
        monkeypatch.setattr(api, '_log_dir', lambda: root)
    monkeypatch.setattr(vast_jobs, 'JobStore', lambda: SimpleNamespace(read=lambda key: native, directory=lambda key: root))
    job = {'operation': operation, 'name': 'optimizer', 'kind': 'optimizer', 'round': 0,
           'status': 'failed', 'error': 'GPU safety check failed token=secret123',
           'backend': {'id': 'e' * 32, 'execution': 'cpu'}}
    store.update(OWNER, row['id'], lambda value: value.update(status='failed', phase='evaluate', jobs=[job],
        reason='Next cycle blocked', last_error='GPU safety check failed token=secret123',
        failure={'detail': 'GPU safety check failed token=secret123', 'stage': 'evaluate'}))
    return service, store, row['id'], operation, root, native


def test_loop_diagnostics_return_termination_and_job_errors(diagnostic_run):
    """Models receive the actual stop reason, not just status and round number."""
    service, _, loop_id, operation, _, _ = diagnostic_run
    result = asyncio.run(service.dispatch(OWNER, CHAT, 'get_ai_loop_runs', {'loop_id': loop_id}))['runs'][0]
    assert result['phase'] == 'evaluate'
    assert result['reason'] == 'Next cycle blocked'
    assert 'GPU safety check failed' in result['termination_details']['detail']
    assert result['jobs'][0]['operation'] == operation
    assert 'secret123' not in str(result)


@pytest.mark.parametrize('execution,kind,collected', [('cpu', 'optimizer', False), ('cpu', 'validation', False),
                                                        ('vast', 'optimizer', True), ('vast', 'optimizer', False)])
def test_owned_loop_logs_return_real_redacted_text(diagnostic_run, execution, kind, collected):
    """Both local job types and collected/live cloud logs are read without browser actions."""
    service, store, loop_id, operation, root, _ = diagnostic_run
    store.update(OWNER, loop_id, lambda row: row['jobs'][0].update(kind=kind, backend={'id': 'e' * 32, 'execution': execution}))
    path = root / (('vast_' if execution == 'vast' else '') + 'e' * 32 + '.log')
    if collected:
        path = root / 'final-results' / 'optimizer.log'; path.parent.mkdir()
    path.write_text('ERROR GPU constraint mismatch token=secret123\n')
    result = asyncio.run(service.dispatch(OWNER, CHAT, 'read_ai_loop_log', {'loop_id': loop_id, 'operation': operation}))
    assert result['exists'] and 'GPU constraint mismatch' in result['text']
    assert 'secret123' not in str(result)
    assert result['next_before'] is None


@pytest.mark.parametrize('boundary', ['owner', 'native_owner', 'operation', 'symlink', 'parent_symlink'])
def test_loop_log_read_enforces_ownership_and_paths(diagnostic_run, boundary):
    """Foreign runs/jobs and symlink escapes cannot expose log evidence."""
    service, _, loop_id, operation, root, native = diagnostic_run
    owner = OWNER
    if boundary == 'owner': owner = 'f' * 32
    if boundary == 'native_owner': native['loop_owner'] = 'f' * 32
    if boundary == 'operation': operation = 'f' * 32
    path = root / ('e' * 32 + '.log')
    if boundary == 'symlink':
        target = root.parent / 'secret'; target.write_text('private')
        path.symlink_to(target)
    elif boundary == 'parent_symlink':
        target = root.parent / 'other'; target.mkdir()
        (target / path.name).write_text('private')
        root.rmdir(); root.symlink_to(target, target_is_directory=True)
    else: path.write_text('private')
    with pytest.raises(AICapabilityError):
        asyncio.run(service.dispatch(owner, CHAT, 'read_ai_loop_log', {'loop_id': loop_id, 'operation': operation}))


def test_loop_log_chunks_cover_older_evidence_without_unbounded_reads(tmp_path):
    """Backwards byte pagination returns every complete line with a 32 KiB read cap."""
    from ai_loop_tools import _log_chunk
    path = tmp_path / 'job.log'
    expected = ''.join(f'{index}: evidence\n' for index in range(6000))
    path.write_text(expected)
    chunks = []; before = None
    for _ in range(10):
        result = _log_chunk(path, tmp_path, before)
        assert result['read_end'] - result['read_start'] <= 32768
        chunks.insert(0, result['text'])
        before = result['next_before']
        if before is None: break
    assert ''.join(chunks) == expected
