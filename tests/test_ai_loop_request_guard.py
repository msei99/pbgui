"""Chat Loop requests cannot inherit foreign restrictions or start stale definitions."""
import asyncio
import copy
from concurrent.futures import ThreadPoolExecutor

import pytest

from ai_capabilities import AICapabilityError
from ai_loop_request import claim_request
from tests.test_ai_loop_tools import (OWNER, CHAT, REQUEST, approve, environment, native_runs)


def test_new_definition_preserves_user_words_and_removes_extra_presets(environment):
    """Only the original request supplies objectives, never assistant-added structured goals."""
    from api import loop_optimizer_v8 as runtime
    service, store, _, _ = environment
    chat = runtime.get_ai_chat_service()
    chat.get_conversation.return_value['messages'].append({'role': 'assistant', 'content': 'Keep mean ADG fixed.'})

    async def scenario():
        """Save without starting any native work."""
        result = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
            'name': 'fresh', 'config_name': 'HYPE', 'settings': {'max_rounds': 10, 'goals': {
                'coins': ['HYPE'], 'presets': ['uptrend'], 'targets': {'uptrend': 0.9}}}})
        await approve(service, result)
        row = store.definition(OWNER, 'fresh')
        assert row['settings']['goals']['text'] == REQUEST
        assert row['settings']['goals']['presets'] == [] and row['settings']['goals']['targets'] == {}
        assert row['source']['loop_request']['mode'] == 'new'
        assert result['preview']['original_request'] == REQUEST
        assert any(change['path'] == 'request.definition_mode' for change in result['preview']['changes'])
        assert any(change['path'] == 'request.instructions' for change in result['preview']['changes'])
        assert store.list(OWNER) == []

    asyncio.run(scenario())


def test_assistant_cannot_append_a_proxy_pin_to_user_goals(environment):
    """Reproduce the added ADG-only instruction from the incident."""
    service, store, _, _ = environment

    async def scenario():
        """Reject before any save proposal or native execution."""
        with pytest.raises(AICapabilityError, match='rewrite or append'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
                'name': 'restricted', 'config_name': 'HYPE', 'settings': {'max_rounds': 10,
                    'goals': {'text': REQUEST + '\nKeep mean adg_strategy_eq as the GPU proxy.'}}})
        assert not store.definitions(OWNER) and not service.proposals

    asyncio.run(scenario())


def test_old_definition_cannot_start_without_reconciliation(native_runs):
    """An old definition is rejected even if its visible safety values look suitable."""
    service, store, _, calls, _ = native_runs
    row = store.definition(OWNER, 'native_loop')
    store.save_definition(OWNER, row['name'], row['settings'], row['bundle'], {'kind': 'config', 'id': 'HYPE'}, 1)

    async def scenario():
        """Never queue or rent for the stale definition."""
        with pytest.raises(AICapabilityError, match='current chat request'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
                'operation': 'queue_and_start', 'name': 'native_loop', 'revision': 2})
        assert calls == [] and store.list(OWNER) == []

    asyncio.run(scenario())


def test_reuse_requires_explicit_mode_and_replaces_all_old_settings(native_runs):
    """Old goal pins, patience and validation caps cannot survive a sparse settings merge."""
    service, store, _, _, _ = native_runs
    row = store.definition(OWNER, 'native_loop')
    row['settings'].update(max_validations=1, patience=1)
    row['settings']['goals'].update(text='Keep mean ADG fixed.', presets=['uptrend'], targets={'uptrend': 0.9})
    store.save_definition(OWNER, row['name'], row['settings'], row['bundle'], row['source'], 1)

    async def scenario():
        """Review reuse separately, with the untouched original request visible."""
        args = {'name': 'native_loop', 'config_name': 'HYPE', 'revision': 2,
                'settings': {'max_rounds': 10}}
        with pytest.raises(AICapabilityError, match='reuse_existing=true'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', args)
        result = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {**args, 'reuse_existing': True})
        assert result['preview']['definition_mode'] == 'reconciled_reuse'
        assert 'Explicit reuse' in result['preview']['effect']
        await approve(service, result)
        fresh = store.definition(OWNER, 'native_loop')
        assert fresh['settings']['goals']['text'] == REQUEST
        assert fresh['settings']['goals']['presets'] == [] and fresh['settings']['goals']['targets'] == {}
        assert fresh['settings']['max_validations'] == 30 and fresh['settings']['patience'] == 3
        assert fresh['source']['loop_request']['binding_id'] != row['source']['loop_request']['binding_id']

    asyncio.run(scenario())


@pytest.mark.parametrize('change', ['new_request', 'foreign_chat'])
def test_definition_is_bound_to_current_owned_request(native_runs, change):
    """Same-name definitions from old requests or other conversations are unusable."""
    from api import loop_optimizer_v8 as runtime
    service, store, _, calls, _ = native_runs
    chat = runtime.get_ai_chat_service()
    if change == 'new_request':
        chat.get_conversation.return_value = {'messages': [{'role': 'user', 'content': 'A different Loop request'}]}

    async def scenario():
        """Reject even before approval is offered."""
        with pytest.raises(AICapabilityError, match='current chat request'):
            await service.dispatch(OWNER, CHAT if change == 'new_request' else 'c' * 32,
                                   'propose_ai_loop_run', {'operation': 'queue', 'name': 'native_loop', 'revision': 1})
        assert calls == []

    asyncio.run(scenario())


def test_changed_request_invalidates_approved_start(native_runs):
    """Approval cannot start an old request after the user changes their instructions."""
    from api import loop_optimizer_v8 as runtime
    service, store, _, calls, _ = native_runs

    async def scenario():
        """Freeze, then replace the user request before execution."""
        proposal = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
            'operation': 'queue_and_start', 'name': 'native_loop', 'revision': 1})
        runtime.get_ai_chat_service().get_conversation.return_value = {
            'messages': [{'role': 'user', 'content': 'Use different goals now'}]}
        with pytest.raises(AICapabilityError, match='request changed'):
            await approve(service, proposal)
        assert calls == [] and store.list(OWNER) == []

    asyncio.run(scenario())


def test_two_approved_proposals_cannot_consume_one_definition_twice(native_runs):
    """Separate proposals cannot duplicate a run; same-proposal replay stays idempotent."""
    service, store, _, calls, _ = native_runs

    async def scenario():
        """Both proposals exist before either can create a run."""
        args = {'operation': 'queue', 'name': 'native_loop', 'revision': 1}
        first = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', args)
        assert any(change['path'] == 'request.original_text' and change['after'] == REQUEST
                   for change in first['preview']['changes'])
        # Simulate a second approval process beyond the chat's in-memory
        # single-pending-action guard; the durable binding claim must still win.
        second = copy.deepcopy(service.proposals[first['proposal_id']])
        second.id = 'c' * 32
        await approve(service, first)
        await approve(service, first)
        with pytest.raises(AICapabilityError, match='already been used'):
            await service.loop_tools.execute_run(second)
        assert len([call for call in calls if call[0] == 'queue']) == 1
        assert len(store.list(OWNER)) == 1
        with pytest.raises(AICapabilityError, match='already been used'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', args)

    asyncio.run(scenario())


def test_new_chat_run_uses_default_not_old_selected_custom_instructions(native_runs):
    """A previous global custom instruction version cannot smuggle in an ADG-only pin."""
    service, store, _, _, _ = native_runs
    version = store.save_instructions(OWNER, 'Old pin', 'Keep mean ADG fixed forever.', 0)
    assert store.instruction_version(OWNER)['id'] == version['id']

    async def scenario():
        """The selected global version is retained for other workflows, not inherited here."""
        proposal = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
            'operation': 'queue', 'name': 'native_loop', 'revision': 1})
        result = await approve(service, proposal)
        row = store.read(OWNER, result['id'])
        assert row['ai_instructions']['id'] == 'default'
        assert 'Keep mean ADG fixed forever.' not in row['ai_instructions']['text']
        assert store.instruction_version(OWNER)['id'] == version['id']

    asyncio.run(scenario())


def test_request_claim_is_cross_thread_exclusive_and_replayable(tmp_path):
    """Only one proposal wins the persistent claim under concurrent approval."""
    binding = 'd' * 32

    def claim(proposal):
        """Return only the successful claimant ID; losers must fail closed."""
        try:
            claim_request(tmp_path, OWNER, binding, proposal)
            return proposal
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        winners = list(pool.map(claim, ['e' * 32, 'f' * 32]))
    assert len([winner for winner in winners if winner]) == 1
    claim_request(tmp_path, OWNER, binding, next(winner for winner in winners if winner))


@pytest.mark.parametrize('extra', ['text', 'presets', 'targets'])
def test_saved_definition_cannot_add_constraints_after_binding(native_runs, extra):
    """An apparently current binding does not authorize altered or extra goals."""
    service, store, _, calls, _ = native_runs
    row = store.definition(OWNER, 'native_loop')
    row['settings']['goals'][extra] = {
        'text': REQUEST + '\nKeep mean ADG fixed.', 'presets': ['uptrend'], 'targets': {'gain': 100}}[extra]
    store.save_definition(OWNER, row['name'], row['settings'], row['bundle'], row['source'], 1)

    async def scenario():
        """Fail closed before offering any start approval."""
        with pytest.raises(AICapabilityError, match='goals differ'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
                'operation': 'queue_and_start', 'name': 'native_loop', 'revision': 2})
        assert calls == []

    asyncio.run(scenario())


def test_durable_run_creation_rejects_inconsistent_request_hash(native_runs):
    """Native creation independently validates provenance, not just the chat wrapper."""
    _, store, _, _, _ = native_runs
    definition = store.definition(OWNER, 'native_loop')
    settings = {**definition['settings'], 'source': copy.deepcopy(definition['source'])}
    settings['source']['loop_request']['digest'] = 'f' * 64
    with pytest.raises(ValueError, match='provenance'):
        store.create(OWNER, settings, {}, {}, {}, [], {}, queued=True)
    assert store.list(OWNER) == []


@pytest.mark.parametrize('messages', [[], [{'role': 'assistant', 'content': REQUEST}],
                                    [{'role': 'user', 'content': 'x' * 8001}]])
def test_missing_or_oversized_original_request_is_not_replaced_by_ai_text(environment, messages):
    """A missing user message cannot fall back to assistant prose or silent truncation."""
    from api import loop_optimizer_v8 as runtime
    service, store, _, _ = environment
    runtime.get_ai_chat_service().get_conversation.return_value = {'messages': messages}

    async def scenario():
        """No prepared definition survives this missing provenance."""
        with pytest.raises(AICapabilityError):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_config', {
                'name': 'fresh', 'config_name': 'HYPE', 'settings': {'max_rounds': 10}})
        assert store.definitions(OWNER) == [] and not service.proposals

    asyncio.run(scenario())
