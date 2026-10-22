"""Isolated host proposals, local target binding, concurrency and crash recovery."""
import asyncio
import copy
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ai_capabilities import AICapabilityError, AICapabilityService, restart_block_reason
from api import vast
from api.auth import require_auth
from vast_jobs import JobStore, write_json
from vast_provider import VastClient, VastError
from vast_queue import CloudQueue
from vast_host_actions import execute, prepare, read_hosts

OWNER, CHAT, JOB, LEASE = ('a' * 32, 'b' * 32, 'c' * 32, 'd' * 32)


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Only temporary state, with provider access and legacy fallback forbidden."""
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    for identifier, row in ((JOB, {'lease_id': LEASE}), (LEASE, {'kind': 'worker', 'host_machine_id': 7})):
        directory = queue.root / 'jobs' / identifier
        directory.mkdir(parents=True)
        write_json(directory / 'state.json', {'id': identifier, 'status': 'running', 'rental_state': 'active', **row})
        write_json(directory / 'intent.json', {'id': identifier, 'offer': {'machine_id': 7}})
        write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    def forbidden(*args, **kwargs):
        """A local policy action must never talk to Vast or resolve legacy metadata."""
        pytest.fail('Unexpected provider or legacy resolution')
    monkeypatch.setattr(VastClient, 'request', forbidden)
    monkeypatch.setattr(queue.store, 'launch_service', forbidden)
    monkeypatch.setattr(vast, '_rental_machine', forbidden)
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    import vast_queue
    monkeypatch.setattr(vast_queue, 'CloudQueue', lambda: queue)
    return AICapabilityService(tmp_path / 'ai'), queue


def proposal(service, queue, blocked=True):
    """Create an immutable proposal via the production review helper."""
    return asyncio.run(service.loop_tools.propose_host_block(OWNER, CHAT, {'job_id': JOB, 'blocked': blocked}))


def approved(service, created):
    """Approve with the exact owner, conversation and digest."""
    row = service.proposals[created['proposal_id']]
    return asyncio.run(service.approve(OWNER, row.id, row.payload_digest, CHAT))


def staged(service, queue, status='executing'):
    """Persist a prepared journal at the same boundary as approve, without executing."""
    created = proposal(service, queue)
    row = service.proposals[created['proposal_id']]
    row.status = status
    service._persist_proposal(row)
    service._write_private_json(service.journal_root / (row.id + '.json'), service._journal_payload(row, phase='prepared'))
    return row


def test_block_replay_new_decision_unblock_and_current_state(env):
    """Replay never toggles; new explicit decisions advance revision even if unchanged."""
    service, queue = env
    before = {path: path.read_bytes() for path in queue.root.glob('jobs/*/*.json')}
    created = proposal(service, queue)
    first = approved(service, created)
    assert first['machine_id'] == 7 and first['blocked'] is True
    assert first['current_host_state']['blocked'] is True
    assert first['host_block_revision'] == 1
    assert approved(service, created)['host_block_revision'] == 1
    same = approved(service, proposal(service, queue))
    assert same['host_block_revision'] == 2 and same['blocked'] is True
    unblocked = approved(service, proposal(service, queue, False))
    assert unblocked['blocked'] is False and unblocked['host_block_revision'] == 3
    replay = approved(service, created)
    assert replay['blocked'] is True and replay['host_block_revision'] == 1
    assert replay['current_host_state']['blocked'] is False
    assert replay['current_host_state']['host_block_revision'] == 3
    assert read_hosts(CloudQueue(JobStore(queue.root)))['blocked_machine_ids'] == []
    assert {path: path.read_bytes() for path in before} == before  # No rental/control/process changes.


@pytest.mark.parametrize('decision', ['same', 'aba', 'other_machine', 'lease', 'machine'])
def test_stale_approval_fails_without_mutation(env, decision):
    """Human decisions and job/lease identity changes invalidate an old snapshot."""
    service, queue = env
    created = proposal(service, queue)
    if decision == 'same':
        queue.set_host_block(7, False)
    elif decision == 'aba':
        queue.set_host_block(7, True)
        queue.set_host_block(7, False)
    elif decision == 'other_machine':
        queue.set_host_block(8, True)
    elif decision == 'lease':
        queue.store.update(JOB, lease_id=JOB)
    else:
        queue.store.update(LEASE, host_machine_id=8)
    before = queue.read()
    result = approved(service, created)
    assert result['status'] == 'failed'
    assert result['error']['code'] == 'target_or_revision_conflict'
    assert queue.read() == before
    service._recover_journals()
    assert queue.read() == before and not restart_block_reason(service.root)


@pytest.mark.parametrize('status', ['failed', 'rejected', 'expired', 'cancelled'])
def test_terminal_proposal_with_prepared_journal_never_recovers_mutation(env, status):
    """A durable terminal decision closes the journal without creating a restart blocker."""
    service, queue = env
    row = staged(service, queue, status)
    before = queue.read()
    service._recover_journals()
    service._recover_journals()
    assert queue.read() == before
    assert service._load_journal(row.id)['phase'] == 'completed'
    assert not restart_block_reason(service.root)
    durable = service._read_private_json(service._owner_path(service.proposal_root, OWNER, row.id), service.proposal_root)
    assert durable['status'] == status
    replacement = AICapabilityService(service.root)
    assert asyncio.run(replacement.list_proposals(OWNER, CHAT)) == []


@pytest.mark.parametrize('crash', ['before_commit', 'after_commit', 'after_completion', 'missing', 'corrupt'])
def test_recovery_crash_windows_and_lost_response(env, crash):
    """Recovery reconstructs historical success, or refuses missing durable permission."""
    service, queue = env
    row = staged(service, queue)
    if crash in {'after_commit', 'after_completion'}:
        result = execute(queue, row)
        queue.set_host_block(7, False)
        if crash == 'after_completion':
            service._complete_journal(service._load_journal(row.id), result)
    if crash in {'missing', 'corrupt'}:
        path = service._owner_path(service.proposal_root, OWNER, row.id)
        if crash == 'missing':
            path.unlink()
        else:
            path.write_text('{')
    service._recover_journals()
    current = read_hosts(queue)
    if crash == 'before_commit':
        assert current['blocked_machine_ids'] == [7] and current['host_block_revision'] == 1
    elif crash in {'after_commit', 'after_completion'}:
        assert current['blocked_machine_ids'] == [] and current['host_block_revision'] == 2
        result = service._load_journal(row.id)['result']
        assert result['blocked'] is True and result['host_block_revision'] == 1
    else:
        assert current['blocked_machine_ids'] == [] and current['host_block_revision'] == 0
    assert not restart_block_reason(service.root)


def test_receipt_pruning_does_not_authorize_old_proposal(env):
    """Even without an old proof, the monotone revision prevents another effect."""
    service, queue = env
    row = staged(service, queue)
    execute(queue, row)
    queue.set_host_block(7, False)
    queue.update(host_block_receipts={})
    before = queue.read()
    service._recover_journals()
    assert queue.read() == before
    assert service._load_journal(row.id)['result']['status'] == 'failed'


def test_receipts_have_hard_limit_and_pruned_replays_fail_closed(env, monkeypatch):
    """The receipt store is bounded without giving an old decision a second effect."""
    import vast_host_actions
    service, queue = env
    monkeypatch.setattr(vast_host_actions, 'MAX_RECEIPTS', 2)
    old = staged(service, queue)
    execute(queue, old)
    old.status = 'executed'
    service._persist_proposal(old)
    for blocked in (False, True, False):
        approved(service, proposal(service, queue, blocked))
    assert len(queue.read()['host_block_receipts']) == 2
    assert old.id not in queue.read()['host_block_receipts']
    before = queue.read()
    with pytest.raises(VastError, match='revision changed'):
        execute(queue, old)
    assert queue.read() == before


def test_payload_change_is_rejected_before_any_effect(env):
    """The execution boundary validates immutable payload even after in-memory tampering."""
    service, queue = env
    created = proposal(service, queue)
    service.proposals[created['proposal_id']].config['blocked'] = False
    result = approved(service, created)
    assert result['status'] == 'failed' and result['error']['code'] == 'invalid_proposal'
    assert queue.read().get('host_block_revision', 0) == 0


def test_competing_approved_proposals_cannot_both_commit_same_revision(env):
    """Independent approval identities serialize at the real queue lock."""
    from dataclasses import replace
    service, queue = env
    row = staged(service, queue)
    competitor = replace(row, id='f'*32, payload_digest='other-digest')
    def decide(item):
        """Return the observable conflict instead of swallowing a concurrent exception."""
        try:
            return execute(queue, item)['status']
        except VastError as exc:
            return exc.status
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(decide, (row, competitor)))
    assert set(results) == {'executed', 409}
    assert queue.read()['host_block_revision'] == 1


@pytest.mark.parametrize('after_commit', [False, True])
def test_storage_failure_checks_proof_before_reporting(env, monkeypatch, after_commit):
    """A lost return after atomic replacement is success; a rejected write is terminal."""
    import vast_host_actions
    service, queue = env
    original = vast_host_actions.write_json
    def broken(path, value):
        """Simulate both sides of the atomic commit boundary using isolated state."""
        if after_commit:
            original(path, value)
        raise OSError('test-only path must not escape in diagnostics')
    monkeypatch.setattr(vast_host_actions, 'write_json', broken)
    created = proposal(service, queue)
    result = approved(service, created)
    assert result['status'] == ('executed' if after_commit else 'failed')
    if after_commit:
        assert result['blocked'] is True
    else:
        assert result['error']['code'] == 'persistence_unconfirmed'
    before = queue.read()
    service._recover_journals()
    assert queue.read() == before


@pytest.mark.parametrize('changes', [{'blocked_machine_ids': ['7']}, {'host_block_revision': True},
                                   {'host_block_revision': -1}, {'host_block_revision': '2'}])
def test_corrupt_policy_is_not_empty_and_is_structured(env, changes):
    """Damaged host policy cannot appear as an empty list or be overwritten."""
    service, queue = env
    queue.update(**changes)
    with pytest.raises(VastError):
        read_hosts(queue)
    result = proposal(service, queue)
    assert result['status'] == 'failed' and result['error']['code'] == 'invalid_host_state'
    assert not service.proposals


def test_worker_log_identity_and_unknown_legacy_target(env):
    """A Worker-ID is itself the lease; no provider fallback or metadata cache write."""
    _, queue = env
    assert read_hosts(queue, LEASE)['target'] == {'job_id': LEASE, 'lease_id': LEASE, 'machine_id': 7, 'blocked': False}
    queue.store.update(LEASE, host_machine_id=None)
    write_json(queue.store.directory(LEASE) / 'intent.json', {'id': LEASE})
    with pytest.raises(VastError, match='unknown locally'):
        read_hosts(queue, LEASE)


def test_owner_conversation_digest_and_branch_rejection(env):
    """Host proposals reuse the existing approval boundaries and are never preserved."""
    service, queue = env
    created = proposal(service, queue)
    row = service.proposals[created['proposal_id']]
    for owner, chat, digest in [('f' * 32, CHAT, row.payload_digest), (OWNER, 'e' * 32, row.payload_digest), (OWNER, CHAT, 'bad')]:
        with pytest.raises(AICapabilityError):
            asyncio.run(service.approve(owner, row.id, digest, chat))
    asyncio.run(service.reject_conversation(OWNER, CHAT, preserve_configurations=True))
    assert row.status == 'rejected' and queue.read().get('host_block_revision', 0) == 0


def test_host_tool_registry_read_projection_and_settings_digest(env, monkeypatch):
    """Real dispatch exposes exclusions separately from editable rental preferences."""
    import vast_credentials
    service, queue = env
    monkeypatch.setattr(vast_credentials, 'VastCredentialStore', lambda root: SimpleNamespace(metadata=lambda:{'configured':True}))
    assert 'propose_vast_host_block' in {item['name'] for item in service._tool_specs()}
    first = asyncio.run(service.dispatch(OWNER, CHAT, 'get_vast_preferences', {'job_id':JOB}))
    assert first['host_exclusions']['target']['machine_id'] == 7
    assert 'blocked_machine_ids' not in first['settings']
    queue.set_host_block(7, True)
    second = asyncio.run(service.dispatch(OWNER, CHAT, 'get_vast_preferences', {'job_id':JOB}))
    assert first['settings_digest'] == second['settings_digest']
    assert second['host_exclusions']['target']['blocked'] is True
    created = asyncio.run(service.dispatch(OWNER, CHAT, 'propose_vast_host_block', {'job_id':JOB, 'blocked':True}))
    assert created['preview']['before_blocked'] is True


def test_unchanged_preferences_recovery_behavior_is_retained(env):
    """The new terminal-status guard does not redesign legacy preference recovery."""
    service, queue = env
    created = asyncio.run(service._create_custom_proposal(OWNER, CHAT, 'save_vast_preferences', 'Preferences', {}, {'action':'save_vast_preferences'}))
    row = service.proposals[created['proposal_id']]
    row.status = 'failed'
    service._persist_proposal(row)
    service._write_private_json(service.journal_root / (row.id + '.json'), service._journal_payload(row, phase='prepared'))
    operations = []
    service.loop_tools.execute = lambda item: operations.append(item.action) or {'status':'executed'}
    service._recover_journals()
    assert operations == ['save_vast_preferences']


def test_read_endpoint_is_authenticated_no_store_and_conflict_status(env):
    """The read-only route exposes persisted policy and preserves target HTTP errors."""
    _, queue = env
    app = FastAPI()
    app.include_router(vast.router, prefix='/api/vast')
    with TestClient(app) as http:
        assert http.get('/api/vast/blocked-hosts').status_code in (401, 403)
        app.dependency_overrides[require_auth] = lambda: object()
        queue.set_host_block(7, True)
        response = http.get('/api/vast/blocked-hosts', params={'job_id': JOB})
        assert response.headers['cache-control'] == 'no-store'
        assert response.json()['target']['blocked'] is True
        queue.store.update(LEASE, host_machine_id=8)
        assert http.get('/api/vast/blocked-hosts', params={'job_id': JOB}).status_code == 409


def test_human_storage_error_does_not_expose_paths_or_claim_success(env, monkeypatch):
    """Human HTTP controls report an unconfirmed write and preserve HTTP 500."""
    from fastapi import HTTPException
    _, queue = env
    def failure(*args):
        """Simulate a local write failure, never touch real runtime data."""
        raise OSError('/test-only/internal-path')
    monkeypatch.setattr(queue, 'set_host_block', failure)
    with pytest.raises(HTTPException) as result:
        vast.set_host_block(vast.MachineBlockRequest(machine_id=7, blocked=True), session=None)
    assert result.value.status_code == 500
    assert 'unconfirmed' in result.value.detail and 'internal-path' not in result.value.detail


def test_host_policy_read_storage_error_is_safe_and_not_success(env, monkeypatch):
    """An unavailable local policy read returns a safe HTTP failure without mutation."""
    from fastapi import HTTPException, Response
    _, queue = env
    def failure(*args):
        """Simulate unavailable local storage without touching production data."""
        raise OSError('/test-only/internal-path')
    monkeypatch.setattr(queue, 'read', failure)
    response = Response()
    with pytest.raises(HTTPException) as result:
        vast.get_host_blocks(response, session=None)
    assert result.value.status_code == 500
    assert 'could not be read' in result.value.detail and 'internal-path' not in result.value.detail
    assert response.headers['cache-control'] == 'no-store'
    assert not (queue.root / 'queue.json').exists()


def test_browser_queue_projection_omits_execution_receipts(env, monkeypatch):
    """Private recovery proofs do not inflate browser polling or queue-control payloads."""
    from fastapi import Response
    import vast_calibration
    service, queue = env
    execute(queue, staged(service, queue))
    monkeypatch.setattr(queue.store, 'list_with_errors', lambda:([], []))
    monkeypatch.setattr(vast_calibration, 'calibration_worker_available', lambda *args:False)
    monkeypatch.setattr(vast, 'services_available', lambda:True)
    assert 'host_block_receipts' not in vast.jobs(Response(), session=None)['queue']
    assert 'host_block_receipts' not in vast.queue_action('pause', session=None)
    assert queue.read()['host_block_receipts']


def test_state_writer_lock_prevents_concurrent_target_change(env):
    """A real JobStore.update cannot change the target while commit holds its state lock."""
    import threading
    from vast_host_actions import locked_target
    _, queue = env
    entered, done = threading.Event(), threading.Event()
    def change():
        """Wait on the production state lock, rather than a mocked transaction."""
        entered.set()
        queue.store.update(JOB, lease_id=JOB)
        done.set()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with locked_target(queue, JOB):
            future = pool.submit(change)
            assert entered.wait(1)
            assert not done.wait(.05)
        future.result(timeout=2)
    assert done.is_set()


def test_queue_writers_preserve_revision_and_receipts(env, monkeypatch):
    """Queue updates, host marks, rental start and orphan recovery merge policy fields."""
    from vast_hosts import set_host_preference
    service, queue = env
    row = staged(service, queue)
    execute(queue, row)
    retained = {key: copy.deepcopy(queue.read()[key]) for key in ('host_block_revision', 'host_block_receipts')}
    queue.update(paused=True)
    set_host_preference(queue, 8, preferred=True)
    monkeypatch.setattr(queue, 'waiting', lambda *args: [])
    monkeypatch.setattr(queue.store, 'start', lambda identifier, *args: {'id': identifier})
    queue.start({'id': 99, 'machine_id': 8}, 1, 1, 300, manual=True)
    # Exercise the writer branch, with only isolated verified-closed test data.
    queue.store.update(JOB, lease_id=None)
    queue.store.update(LEASE, rental_state='deletion_verified')
    queue.update(worker_id=LEASE)
    queue.recover_orphaned_workers()
    for key, value in retained.items():
        assert queue.read()[key] == value


def test_context_anchor_is_bounded_untrusted_and_survives_model_serialization(tmp_path):
    """Eight entities and the full control budget do not consume the separate anchor."""
    from ai_chat import AIChatError, AIChatService
    anchor = dict(job_id=JOB, lease_id=LEASE, machine_id=7, blocked=True, host_block_revision=2)
    context = {'current_cloud_job': anchor, 'entities': [{'kind':'optimizer_config', 'version':'v8', 'name':str(i)} for i in range(8)],
               'controls': [{'id':'control_'+str(i), 'role':'button', 'name':'A', 'label':'A', 'operations':['activate']} for i in range(2048)]}
    valid = AIChatService._validate_page_context(context)
    assert valid['current_cloud_job'] == anchor
    assert json.loads(AIChatService._context_prompt_suffix(valid).split('\n')[-1])['current_cloud_job'] == anchor
    async def persist_and_reload():
        """Exercise real owner-scoped context storage without a model/provider request."""
        first = AIChatService(tmp_path / 'chat')
        conversation = await first._conversation(OWNER, 'opencode-go', 'test-model', None)
        conversation.context = valid
        first._persist_conversation(conversation)
        await first.shutdown()
        second = AIChatService(tmp_path / 'chat')
        restored = await second.get_conversation(OWNER, conversation.id)
        assert restored['context']['current_cloud_job'] == anchor
        assert anchor['job_id'] in second._context_prompt_suffix(restored['context'])
        await second.shutdown()
    asyncio.run(persist_and_reload())
    for invalid in ({'job_id':'../secret'}, {'job_id':JOB,'machine_id':True}, {'job_id':JOB,'token':'secret'}, {'job_id':JOB,'lease_id':'other'}):
        with pytest.raises(AIChatError):
            AIChatService._validate_page_context({'current_cloud_job':invalid})


def test_run_native_identity_is_owner_checked_and_read_off_event_loop(env, monkeypatch):
    """Detailed Loop reads expose only matching native jobs, using the existing owner boundary."""
    import threading
    from unittest.mock import AsyncMock
    import vast_jobs
    service, queue = env
    loop = 'e' * 32
    queue.store.update(JOB, loop_owner=OWNER, loop_id=loop)
    row = {'id':loop, 'status':'running', 'settings':{'config_name':'test', 'execution':'vast'},
           'jobs':[{'operation':'f'*32, 'backend':{'execution':'vast','id':JOB}}]}
    monkeypatch.setattr(service.loop_tools, '_runtime', lambda: SimpleNamespace(_controller=SimpleNamespace(store=SimpleNamespace(read=lambda *args: row, list=lambda *args:[row]))))
    monkeypatch.setattr(service.loop_tools, '_pending', AsyncMock(return_value=[]))
    main = threading.get_ident()
    original = queue.store.read
    def checked_read(*args):
        """Fail if native filesystem inspection runs on the calling event loop thread."""
        assert threading.get_ident() != main
        return original(*args)
    monkeypatch.setattr(queue.store, 'read', checked_read)
    monkeypatch.setattr(vast_jobs, 'JobStore', lambda: queue.store)
    result = asyncio.run(service.loop_tools.read_runs(OWNER, CHAT, {'loop_id':loop}))
    assert result['runs'][0]['jobs'][0]['cloud_job_id'] == JOB
    # Change retained ownership with the unpatched updater's reader only for setup.
    monkeypatch.setattr(queue.store, 'read', original)
    queue.store.update(JOB, loop_owner='f'*32)
    monkeypatch.setattr(queue.store, 'read', checked_read)
    result = asyncio.run(service.loop_tools.read_runs(OWNER, CHAT, {'loop_id':loop}))
    assert 'cloud_job_id' not in result['runs'][0]['jobs'][0]


@pytest.mark.parametrize('tool', ['get_vast_preferences', 'get_vast_host_action_status'])
def test_host_reads_bypass_codex_cache_in_same_turn(tmp_path, tool):
    """Two identical host-state reads can observe a later persisted decision."""
    from ai_chat import AIChatService, CodexRuntime
    calls = []
    async def dispatch(*args):
        """Return a changing revision without any real provider or runtime access."""
        calls.append(args)
        return {'host_block_revision':len(calls)}
    async def scenario():
        """Run the real tool handler twice within a single active Codex turn."""
        service = AIChatService(tmp_path / 'ai')
        service.capabilities = SimpleNamespace(dispatch=dispatch)
        runtime = CodexRuntime(OWNER, tmp_path / 'codex')
        runtime.active_turn_id = 'turn-1'
        conversation = await service._conversation(OWNER, 'chatgpt', 'gpt-test', None)
        conversation.codex_thread_id = 'thread-1'
        conversation.codex_runtime = runtime
        conversation.busy = True
        request = {'namespace':'pbgui', 'tool':tool, 'threadId':'thread-1', 'turnId':'turn-1', 'arguments':{'job_id':JOB}}
        first = await service._handle_codex_tool(OWNER, runtime, request)
        second = await service._handle_codex_tool(OWNER, runtime, request)
        assert first != second and len(calls) == 2
        await service.shutdown()
    asyncio.run(scenario())


@pytest.mark.parametrize('status', ['executed', 'failed'])
def test_host_result_reaches_chat_continuation(env, monkeypatch, status):
    """The API forwards identity, historical outcome and fresh policy without losing failure."""
    from api import ai as ai_api
    from unittest.mock import AsyncMock
    service, queue = env
    created = proposal(service, queue)
    if status == 'failed':
        queue.set_host_block(8, True)
    result = approved(service, created)
    chat = SimpleNamespace(start_turn=AsyncMock(return_value={'status':'queued'}))
    monkeypatch.setattr(ai_api, 'get_ai_chat_service', lambda: chat)
    monkeypatch.setattr(ai_api, 'get_ai_capability_service', lambda: SimpleNamespace(approve=AsyncMock(return_value=result)))
    response = asyncio.run(ai_api.approve_proposal(created['proposal_id'],
        ai_api.ProposalDecisionRequest(payload_digest=service.proposals[created['proposal_id']].payload_digest, conversation_id=CHAT),
        SimpleNamespace(user_id='owner')))
    message = chat.start_turn.call_args.args[2]
    forwarded = json.loads(message.split('Approved action result:\n')[1])
    assert forwarded['status'] == status and forwarded['current_host_state'] == result['current_host_state']
    if status == 'executed':
        assert forwarded['machine_id'] == 7 and forwarded['blocked'] is True
    else:
        assert forwarded['error']['code'] == 'target_or_revision_conflict'
        assert 'completed successfully' not in message
    assert response.headers['cache-control'] == 'no-store'


@pytest.mark.parametrize('boundary', ['_persist_proposal', '_persist_history', '_complete_journal'])
@pytest.mark.parametrize('persistent', [False, True])
@pytest.mark.parametrize('fault', [OSError, RuntimeError])
def test_post_commit_finalization_error_preserves_success(env, monkeypatch, boundary, persistent, fault):
    """Metadata failures after the queue commit cannot report failure or replay a decision."""
    service, queue = env
    created = proposal(service, queue)
    original = getattr(service, boundary)
    faults = []
    def faulty(*args, **kwargs):
        """Fail only after an authoritative receipt exists, once or until recovery."""
        if queue.read().get('host_block_receipts') and (persistent or not faults):
            faults.append(True)
            raise fault('isolated metadata fault')
        return original(*args, **kwargs)
    monkeypatch.setattr(service, boundary, faulty)
    result = approved(service, created)
    assert faults and result['status'] == 'executed' and result['blocked'] is True
    assert result.get('finalization_pending', False) is persistent
    assert queue.read()['host_block_revision'] == 1
    queue.set_host_block(7, False)
    monkeypatch.setattr(service, boundary, original)
    service._recover_journals()
    assert not restart_block_reason(service.root)
    assert queue.read()['host_block_revision'] == 2
    snapshot = asyncio.run(service.dispatch(OWNER, CHAT, 'get_vast_host_action_status', {'proposal_id':created['proposal_id']}))
    assert snapshot['status'] == 'executed' and snapshot['blocked'] is True
    assert snapshot['current_host_state']['blocked'] is False


@pytest.mark.parametrize('phase', ['awaiting_approval', 'executing', 'executed', 'failed', 'rejected', 'pruned'])
def test_host_status_read_is_owned_read_only_and_fresh(env, phase):
    """A lost-response lookup is available in analysis-only chats and never recovers work."""
    service, queue = env
    created = proposal(service, queue)
    row = service.proposals[created['proposal_id']]
    if phase in {'executed', 'pruned'}:
        approved(service, created)
        queue.set_host_block(7, False)
        if phase == 'pruned':
            state = queue.read()
            state['host_block_receipts'] = {}
            write_json(queue.root / 'queue.json', state)
    elif phase == 'executing':
        row.status = phase
        service._persist_proposal(row)
        service._write_private_json(service.journal_root / (row.id + '.json'), service._journal_payload(row, phase='prepared'))
    elif phase == 'failed':
        queue.set_host_block(8, True)
        approved(service, created)
    elif phase == 'rejected':
        asyncio.run(service.reject(OWNER, row.id, row.payload_digest, CHAT))
    service.restrict_to_analysis(OWNER, CHAT)
    before = {path: path.read_bytes() for root in (service.root, queue.root) for path in root.rglob('*.json')}
    result = asyncio.run(service.dispatch(OWNER, CHAT, 'get_vast_host_action_status', {'proposal_id':row.id}))
    assert result['proposal_status'] == ('executed' if phase == 'pruned' else phase)
    if phase in {'executed', 'pruned'}:
        assert result['blocked'] is True and result['execution_status'] == 'succeeded'
        assert result['current_host_state']['blocked'] is False
        assert result['current_host_state']['host_block_revision'] == 2
    else:
        assert result['status'] != 'executed'
    assert {path: path.read_bytes() for path in before} == before
    assert {path for root in (service.root, queue.root) for path in root.rglob('*.json')} == set(before)
    for owner, cid in [(OWNER, 'e'*32), ('f'*32, CHAT)]:
        with pytest.raises(AICapabilityError):
            asyncio.run(service.dispatch(owner, cid, 'get_vast_host_action_status', {'proposal_id':row.id}))
    assert not (service.proposal_root / ('f'*32)).exists()


def test_host_status_http_is_authenticated_scoped_and_no_store(env, monkeypatch):
    """The drawer's recovery request exposes only an owned read, never an approval POST."""
    from api import ai as ai_api
    service, queue = env
    created = proposal(service, queue)
    approved(service, created)
    monkeypatch.setattr(ai_api, 'get_ai_capability_service', lambda: service)
    monkeypatch.setattr(ai_api, '_owner', lambda session: OWNER)
    app = FastAPI()
    app.include_router(ai_api.router, prefix='/api/ai')
    url = f"/api/ai/proposals/{created['proposal_id']}/host-status"
    with TestClient(app) as client:
        assert client.get(url, params={'conversation_id':CHAT}).status_code == 401
        app.dependency_overrides[require_auth] = lambda: object()
        response = client.get(url, params={'conversation_id':CHAT})
        assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
        assert response.json()['machine_id'] == 7 and response.json()['blocked'] is True
        assert client.get(url, params={'conversation_id':'e'*32}).status_code != 200
    assert queue.read()['host_block_revision'] == 1


@pytest.mark.parametrize('damage', ['receipts', 'digest', 'missing_result', 'result_list',
                                   'false_success', 'machine_boolean', 'blocked_string',
                                   'revision_boolean', 'job_identity', 'bound_machine', 'timestamp'])
def test_host_status_read_rejects_unreadable_proof_without_mutation(env, damage):
    """Corrupt proof is an unconfirmed outcome, never a success or a replay permission."""
    service, queue = env
    created = proposal(service, queue)
    approved(service, created)
    state = queue.read()
    if damage == 'receipts':
        state['host_block_receipts'] = []
    elif damage == 'digest':
        state['host_block_receipts'][created['proposal_id']]['payload_digest'] = 'tampered'
    else:
        proof = state['host_block_receipts'][created['proposal_id']]
        if damage == 'missing_result':
            proof.pop('result')
        elif damage == 'result_list':
            proof['result'] = []
        elif damage == 'timestamp':
            proof['at'] = True
        else:
            key, value = {'false_success': ('execution_status', 'failed'),
                          'machine_boolean': ('machine_id', True),
                          'blocked_string': ('blocked', 'true'),
                          'revision_boolean': ('host_block_revision', True),
                          'job_identity': ('job_id', '../other'),
                          'bound_machine': ('machine_id', 8)}[damage]
            proof['result'][key] = value
    write_json(queue.root / 'queue.json', state)
    result = asyncio.run(service.dispatch(OWNER, CHAT, 'get_vast_host_action_status', {'proposal_id':created['proposal_id']}))
    assert result['status'] == 'unconfirmed' and result['execution_status'] == 'unconfirmed'
    assert result['error']['code'] == 'invalid_host_state'
    assert queue.read() == state


@pytest.mark.parametrize('offer', [[], 'damaged', 7, False])
def test_corrupt_target_metadata_fails_structured_without_mutation(env, offer):
    """Malformed local offers cannot become fallback identities or tool exceptions."""
    service, queue = env
    write_json(queue.store.directory(LEASE) / 'intent.json', {'id': LEASE, 'offer': offer})
    before = {path: path.read_bytes() for path in queue.root.glob('jobs/*/*.json')}
    result = asyncio.run(service.loop_tools.read_vast(OWNER, CHAT, {'job_id': JOB}))
    assert result['status'] == 'failed' and result['error']['code'] == 'invalid_host_state'
    rejected = proposal(service, queue)
    assert rejected['status'] == 'failed' and not service.proposals
    assert {path: path.read_bytes() for path in queue.root.glob('jobs/*/*.json')} == before
    assert not (queue.root / 'queue.json').exists()


def test_host_receipt_projects_only_validated_execution_fields(env):
    """Status and repeated approval cannot forward unrelated stored receipt fields."""
    service, queue = env
    created = proposal(service, queue)
    approved(service, created)
    state = queue.read()
    state['host_block_receipts'][created['proposal_id']]['result']['unexpected'] = 'private diagnostic'
    write_json(queue.root / 'queue.json', state)
    result = asyncio.run(service.dispatch(OWNER, CHAT, 'get_vast_host_action_status', {'proposal_id': created['proposal_id']}))
    assert result['execution_status'] == 'succeeded' and 'unexpected' not in result
    assert queue.read() == state


@pytest.mark.parametrize('timestamp', [10 ** 400, -(10 ** 400)], ids=['huge_positive', 'huge_negative'])
def test_host_status_read_rejects_overflowing_receipt_timestamp(env, timestamp):
    """Valid JSON integers outside float range produce structured, read-only uncertainty."""
    service, queue = env
    created = proposal(service, queue)
    approved(service, created)
    state = queue.read()
    state['host_block_receipts'][created['proposal_id']]['at'] = timestamp
    write_json(queue.root / 'queue.json', state)
    before = (queue.root / 'queue.json').read_bytes()
    result = asyncio.run(service.dispatch(OWNER, CHAT, 'get_vast_host_action_status',
                                         {'proposal_id': created['proposal_id']}))
    assert result['status'] == 'unconfirmed' and result['execution_status'] == 'unconfirmed'
    assert result['error']['code'] == 'invalid_host_state'
    assert result['current_host_state']['blocked_machine_ids'] == [7]
    assert result['current_host_state']['host_block_revision'] == 1
    assert (queue.root / 'queue.json').read_bytes() == before


@pytest.mark.parametrize('timestamp', [10 ** 400, -(10 ** 400)], ids=['huge_positive', 'huge_negative'])
def test_receipt_pruning_discards_overflowing_timestamps_without_replay(env, timestamp):
    """A fresh explicit decision prunes damaged dates without reviving the old proposal."""
    service, queue = env
    old = proposal(service, queue)
    approved(service, old)
    retained = proposal(service, queue)
    approved(service, retained)
    state = queue.read()
    state['host_block_receipts'][old['proposal_id']]['at'] = timestamp
    write_json(queue.root / 'queue.json', state)
    rental_before = {path: path.read_bytes() for path in queue.root.glob('jobs/*/*.json')}
    current = proposal(service, queue, False)
    result = approved(service, current)
    assert result['status'] == 'executed' and result['blocked'] is False
    state = queue.read()
    assert state['host_block_revision'] == 3 and state['blocked_machine_ids'] == []
    assert set(state['host_block_receipts']) == {retained['proposal_id'], current['proposal_id']}
    with pytest.raises(VastError, match='revision changed'):
        execute(queue, service.proposals[old['proposal_id']])
    assert queue.read() == state
    assert {path: path.read_bytes() for path in rental_before} == rental_before


def test_integer_receipt_timestamp_preserves_historical_success_and_retention(env):
    """Ordinary integer epoch dates remain readable and survive the next receipt cleanup."""
    service, queue = env
    created = proposal(service, queue)
    approved(service, created)
    state = queue.read()
    proof = state['host_block_receipts'][created['proposal_id']]
    proof['at'] = int(proof['at'])
    write_json(queue.root / 'queue.json', state)
    result = asyncio.run(service.dispatch(OWNER, CHAT, 'get_vast_host_action_status',
                                         {'proposal_id': created['proposal_id']}))
    assert result['execution_status'] == 'succeeded' and queue.read() == state
    approved(service, proposal(service, queue, False))
    assert queue.read()['host_block_receipts'][created['proposal_id']] == proof
