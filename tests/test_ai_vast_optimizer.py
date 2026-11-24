"""Offline approval and rental isolation for normal AI-requested Vast optimizers."""
import asyncio
import copy
from pathlib import Path

import pytest

from ai_capabilities import AICapabilityError, AICapabilityService
import ai_vast_optimizer as capability
from api.vast import RentalPreferences
from vast_credentials import VastCredentialStore
from vast_jobs import JobStore, native_job_config
from vast_queue import CloudQueue


@pytest.fixture
def cloud(tmp_path, monkeypatch, request):
    """Use native durable proposals and job state with inert preparation/provider boundaries."""
    from api import optimize_v8
    version = getattr(request, 'param', 'v8')
    import vast_pool
    from vast_direction_fixtures import direction_config
    service = AICapabilityService(tmp_path / 'ai')
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    settings = RentalPreferences(gpu_name='RTX 3060', max_price=.3, budget=5, hours=24).model_dump()
    queue.update(gpu_preferences=settings)
    source = direction_config()
    if version == 'v9':
        import json
        source = json.loads((Path(__file__).parent / 'fixtures/pb9_optimize_metadata.json').read_text())['template']
    source['optimize'].update(iters=20_000_000, backend='gpu', n_cpus=21)
    source['optimize']['scoring'][0]['reducer'] = 'mean'
    source['backtest'].update(start_date='2024-12-05', end_date='2026-10-30')
    current = {'source': source,
               'digest': 'd' * 64, 'generation': 1, 'overrides': {}}
    calls = []
    monkeypatch.setattr(capability, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(VastCredentialStore, 'metadata', lambda self: {
        'configured': True, 'generation': current['generation']})
    monkeypatch.setattr(service, '_current_pb8_bundle', lambda name: (
        copy.deepcopy(current['source']), current['overrides'], current['digest']))
    monkeypatch.setattr(service, '_validate_pb8_config', lambda name, config: copy.deepcopy(config))
    monkeypatch.setattr(optimize_v8, '_results_root', lambda: tmp_path / 'results')
    if version == 'v9':
        import vast_pb9
        monkeypatch.setattr(vast_pb9, 'validate', lambda *args, **kwargs: [])
        monkeypatch.setattr(service, '_current_pb8_bundle', lambda *_: pytest.fail('PB8 source used for PB9'))
        monkeypatch.setattr(service, '_current_optimizer_bundle', lambda selected, name: (
            copy.deepcopy(current['source']), current['overrides'], current['digest']) if selected == 'v9' else pytest.fail('Wrong source generation'))
        monkeypatch.setattr(service, '_validate_optimizer_config', lambda selected, name, config:
            copy.deepcopy(config) if selected == 'v9' else pytest.fail('PB8 validator used for PB9'))
        from api.optimize_v9 import engine
        monkeypatch.setattr(engine, '_results_root', lambda: tmp_path / 'results-v9')

    def prepare(name, source, digest, market, mappings, results, iterations, workers, adg, *, identifier):
        """Capture only isolated native input preparation, never read market runtime data."""
        calls.append(('prepare', copy.deepcopy(source), iterations, workers))
        return queue.store.update(identifier, status='ready')

    def select(queue, settings, *, job_authorization):
        """Assert the offer search uses only the exact reviewed scope."""
        assert queue.waiting() == []
        assert [row['id'] for row in queue.waiting(authorization_id=job_authorization['id'])] == [job_authorization['job_id']]
        calls.append(('offer', settings))
        return {'id': 123}

    def start(offer, hours, budget, idle_seconds, *, job_authorization):
        """Reserve one inert worker so process-recovery replay can be tested."""
        from vast_queue import single_job_worker_id
        assert idle_seconds == 0
        calls.append(('rent', hours, budget, copy.deepcopy(job_authorization)))
        worker = {'id': single_job_worker_id(job_authorization['id']), 'kind': 'worker',
                  'rental_state': 'creation_pending', 'status': 'provisioning'}
        if version == 'v9':
            worker['runtime'] = 'pb9'
        queue.store.publish(worker, intent={'job_authorization': job_authorization})
        return worker

    monkeypatch.setattr(queue.store, 'prepare', prepare)
    monkeypatch.setattr(vast_pool, 'select_offer', select)
    monkeypatch.setattr(queue, 'start', start)
    return service, queue, current, calls


@pytest.mark.parametrize('paused', [False, True])
@pytest.mark.parametrize('cloud', ['v8', 'v9'], indirect=True)
def test_normal_vast_run_approval_and_recovery_are_scoped_and_idempotent(cloud, paused):
    """20M iterations survive; no local process, AI Loop or global Auto rent is needed."""
    service, queue, current, calls = cloud
    version = 'v9' if str(current['source'].get('config_version', '')).startswith('v9.') else 'v8'

    async def scenario():
        """Exercise the common provider dispatch and human approval boundary."""
        queue.update(paused=paused)
        before = queue.read()
        created = await service.dispatch('a' * 32, 'b' * 32, 'propose_vast_optimizer_run', {
            'name': 'hype', 'version': version, 'stop_when_no_improvement': True})
        assert calls == [] and queue.store.list() == []
        preview = created['preview']
        assert preview['execution'] == 'vast' and preview['iterations'] == 20_000_000
        assert preview['budget'] == 5 and preview['hours'] == 24
        proposal = service.proposals[created['proposal_id']]
        with pytest.raises(AICapabilityError):
            await service.approve('c' * 32, proposal.id, proposal.payload_digest, 'b' * 32)
        with pytest.raises(AICapabilityError):
            await service.approve('a' * 32, proposal.id, 'wrong digest', 'b' * 32)
        assert calls == []
        result = await service.approve('a' * 32, proposal.id, proposal.payload_digest, 'b' * 32)
        assert result['execution'] == 'vast' and result['ai_loop'] is False
        assert result['rental_state'] == 'creation_pending'
        assert result['version'] == version
        stored = queue.store.read(result['job_id'])
        assert stored.get('runtime', 'pb8') == ('pb9' if version == 'v9' else 'pb8')
        shown = await service.dispatch('a'*32, 'b'*32, 'get_vast_optimizer_job', {
            'version': version, 'job_id': result['job_id'], 'open_log': True})
        assert shown['ui_action']['target']['page_key'] == version + '_optimize'
        assert shown['ui_action']['payload']['entity']['name'] == 'vast:' + result['job_id']
        with pytest.raises(AICapabilityError, match='selected generation'):
            await service.dispatch('a'*32, 'b'*32, 'get_vast_optimizer_job', {
                'version': 'v9' if version == 'v8' else 'v8', 'job_id': result['job_id']})
        observed = await service.dispatch('a'*32, 'b'*32, 'get_vast_optimizer_job', {
            'version': version, 'job_id': result['job_id'], 'open_log': True})
        assert observed['version'] == version
        assert observed['ui_action']['target']['page_key'] == version + '_optimize'
        assert [row[0] for row in calls] == ['prepare', 'offer', 'rent']
        assert calls[0][2:] == (20_000_000, 21)
        assert calls[0][1]['backtest'] == current['source']['backtest']
        assert calls[-1][-1]['convergence']['convergence_enabled'] is True
        assert queue.read() == before
        assert await service.approve('a' * 32, proposal.id, proposal.payload_digest, 'b' * 32) == result
        # Simulate recovery after native reservation but before journal completion.
        assert capability.execute(service, proposal) == result
        assert len(calls) == 3
        await service.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize('changed', ['source', 'settings', 'account', 'new_pause'])
def test_changed_review_inputs_never_prepare_or_rent(cloud, changed):
    """Approval cannot silently use a different config, price limit or account."""
    service, queue, current, calls = cloud

    async def scenario():
        """Modify one approved dependency before the native execution boundary."""
        created = await capability.propose(service, 'a' * 32, 'b' * 32, {
            'name': 'hype', 'version': 'v8', 'stop_when_no_improvement': True})
        if changed == 'source':
            current['digest'] = 'e' * 64
        elif changed == 'account':
            current['generation'] += 1
        elif changed == 'new_pause':
            queue.action('pause')
        else:
            settings = queue.read()['gpu_preferences']
            settings['budget'] = 10
            queue.update(gpu_preferences=settings)
        proposal = service.proposals[created['proposal_id']]
        with pytest.raises(AICapabilityError, match='changed after review'):
            await service.approve('a' * 32, proposal.id, proposal.payload_digest, 'b' * 32)
        assert calls == [] and queue.store.list() == []
        from ai_capabilities import restart_block_reason
        assert restart_block_reason(service.root) == ''
        await service.shutdown()

    asyncio.run(scenario())


def test_cloud_proposal_requires_explicit_stop_and_blocks_analysis_context(cloud):
    """All providers share the same analysis-only and stopping-choice boundaries."""
    service, queue, current, calls = cloud

    async def scenario():
        """Invalid intent and research-only turns never reach preparation."""
        with pytest.raises(AICapabilityError, match='explicitly'):
            await capability.propose(service, 'a' * 32, 'b' * 32, {'name': 'hype', 'version': 'v8'})
        with pytest.raises(AICapabilityError, match='PB8 or PB9'):
            await capability.propose(service, 'a' * 32, 'b' * 32, {'name': 'hype', 'stop_when_no_improvement': True})
        service.restrict_to_analysis('a' * 32, 'b' * 32)
        with pytest.raises(AICapabilityError):
            await service.dispatch('a' * 32, 'b' * 32, 'propose_vast_optimizer_run', {
                'name': 'hype', 'version': 'v8', 'stop_when_no_improvement': True})
        assert calls == []
        await service.shutdown()

    asyncio.run(scenario())


def test_local_tools_require_explicit_local_target(cloud):
    """Ambiguous or explicit cloud requests cannot accidentally launch the local GPU."""
    service, queue, current, calls = cloud

    async def scenario():
        """Reject before examining any real local queue or config file."""
        for tool in ('propose_queue_pb8_config', 'propose_start_pb8_optimizer_queue'):
            for execution in (None, 'vast'):
                with pytest.raises(AICapabilityError, match='execution=local'):
                    await service.dispatch('a' * 32, 'b' * 32, tool, {'execution': execution})
        await service.shutdown()

    asyncio.run(scenario())


def test_preparation_operation_replay_keeps_one_input_and_checks_identity(tmp_path):
    """Recovery reuses the original frozen job, including its use_adg choice."""
    from vast_provider import VastError
    store = JobStore(tmp_path / 'vast')
    first = store.create_preparation('hype', 20_000_000, 21, False, operation_id='a' * 32)
    assert store.create_preparation('hype', 20_000_000, 21, False, operation_id='a' * 32) == first
    with pytest.raises(VastError, match='another operation'):
        store.create_preparation('hype', 20_000_000, 21, True, operation_id='a' * 32)
    assert len(store.list()) == 1


def test_twenty_million_iterations_pass_real_cloud_validation_without_local_gpu():
    """The normal request must survive both public validation and native export."""
    from api.vast import PrepareJobRequest
    from vast_config_validation import validate_cloud_config
    from vast_direction_fixtures import direction_config
    from vast_provider import VastError
    source = direction_config()
    source['optimize']['iters'] = 20_000_000
    assert PrepareJobRequest(config_name='hype', iterations=20_000_000).iterations == 20_000_000
    assert validate_cloud_config(source) == []
    exported = native_job_config(source, 20_000_000, 4, False)
    assert exported['optimize']['iters'] == 20_000_000
    assert exported['backtest']['offline'] is True
    with pytest.raises(VastError, match='20,000,000'):
        native_job_config(source, 20_000_001, 4, False)


@pytest.mark.parametrize('evidence', ['unstarted', 'started_at', 'pid', 'runner'])
def test_failed_local_action_retires_only_with_no_launch_evidence(cloud, tmp_path, monkeypatch, evidence):
    """A prelaunch failure may unblock restart; uncertain or started work must remain recoverable."""
    from contextlib import nullcontext
    from ai_capabilities import ActionProposal, restart_block_reason
    from api import optimize_v8
    service, queue, current, calls = cloud
    proposal = ActionProposal(id='e' * 32, owner='a' * 32, conversation_id='b' * 32,
        action='start_optimize_queue', name='hype', config={'jobs': [{'queue_id': 'isolated'}]},
        expected_digest=None, create_only=True, preview={}, payload_digest='f' * 64)
    service._write_private_json(service.journal_root / f'{proposal.id}.json', service._journal_payload(proposal, phase='prepared'))
    data = {'status_override': 'error', 'error_code': 'prelaunch_failed',
            'started_at': 123 if evidence == 'started_at' else None}
    monkeypatch.setattr(optimize_v8, '_queue_lock', nullcontext)
    monkeypatch.setattr(optimize_v8, '_queue_file', lambda key: tmp_path / 'isolated.json')
    monkeypatch.setattr(optimize_v8, '_read_json', lambda path: data)
    monkeypatch.setattr(optimize_v8, '_read_process_record', lambda key: {'pid': 123} if evidence == 'pid' else None)
    monkeypatch.setattr(optimize_v8, '_pid_file', lambda key: tmp_path / 'absent.pid')
    monkeypatch.setattr(optimize_v8, '_read_runner_state', lambda key: {'status': 'running'} if evidence == 'runner' else None)
    assert service._retire_unstarted_optimizer_journal(proposal, RuntimeError('prelaunch failed')) == (evidence == 'unstarted')
    assert bool(restart_block_reason(service.root)) == (evidence != 'unstarted')
