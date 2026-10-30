"""Offline unchanged-definition reuse through the real proposal and claim paths."""
import asyncio
import copy
import json

import pytest

from ai_capabilities import AICapabilityError
from ai_loop_request import bind_request, current_request, preserve_request, validate_request
from ai_loop_windows import bound_plan
from secure_files import atomic_write_private_text
from tests.test_ai_loop_tools import OWNER, CHAT, REQUEST, approve, environment, native_runs

START_REQUEST = 'Start the saved native_loop unchanged with the selected Claude model. Create no definition.'


@pytest.mark.parametrize('legacy', [False, True])
def test_review_existing_definition_preserves_goals_and_limits(native_runs, legacy):
    """A short follow-up can review the saved goals without replacing or saving them."""
    from api import loop_optimizer_v8 as runtime
    service, store, rental, calls, _ = native_runs
    saved = store.definition(OWNER, 'native_loop')
    saved['settings']['goals'].update(presets=['uptrend'], targets={'uptrend': 0.9})
    source = {'kind': 'config', 'id': 'HYPE'} if legacy else saved['source']
    store.save_definition(OWNER, saved['name'], saved['settings'], saved['bundle'], source, 1)
    before = store.definition(OWNER, 'native_loop')
    chat = runtime.get_ai_chat_service()
    chat.get_conversation.return_value = {'messages': [{'role': 'user', 'content': START_REQUEST}]}
    selection = {'provider': 'claude-code', 'model': 'claude-sonnet-5-5', 'profile': 'main',
                 'effort': 'high', 'service_tier': ''}
    chat.get_preferences = lambda _owner: {'selection': selection}

    async def scenario():
        """Approve only in an isolated native controller with no rental or model calls."""
        args = {'operation': 'queue_and_start', 'name': 'native_loop', 'revision': 2,
                'reuse_existing': True}
        proposal = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', args)
        preview = proposal['preview']
        assert preview['definition_mode'] == 'preserved_reuse'
        assert preview['saved_goals'] == before['settings']['goals']
        assert preview['original_request'] == START_REQUEST
        assert preview['rental']['budget'] == rental['budget']
        assert preview['settings']['provider'] == 'claude-code'
        changes = {change['path']: change['after'] for change in preview['changes']}
        assert changes['run.saved_limits']['max_rounds'] == before['settings']['max_rounds']
        assert changes['run.ai_selection'] == selection
        assert changes['run.rental_preferences']['budget'] == rental['budget']
        assert all(preview['settings'][key] == value for key, value in before['settings'].items())
        assert not calls and not store.list(OWNER)
        assert store.definition(OWNER, 'native_loop') == before
        result = await approve(service, proposal)
        row = store.read(OWNER, result['id'])
        assert row['status'] == 'running'
        assert row['settings']['goals']['text'] == REQUEST
        assert row['settings']['source']['loop_request']['text'] == START_REQUEST
        assert row['settings']['goals'] == before['settings']['goals']
        assert row['initial_config'] == before['bundle']['config']
        assert store.definition(OWNER, 'native_loop') == before
        await approve(service, proposal)
        assert len([call for call in calls if call[0] == 'queue']) == 1
        with pytest.raises(AICapabilityError, match='already been used'):
            await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', args)

    asyncio.run(scenario())


@pytest.mark.parametrize('change', ['settings', 'bundle', 'source', 'request', 'rental'])
def test_changed_reuse_snapshot_cannot_execute(native_runs, change):
    """Approval must still match the request, full definition and rental settings."""
    from api import loop_optimizer_v8 as runtime
    service, store, rental, calls, _ = native_runs
    chat = runtime.get_ai_chat_service()
    chat.get_conversation.return_value = {'messages': [{'role': 'user', 'content': START_REQUEST}]}

    async def scenario():
        """Even same-revision changes fail before native work."""
        proposal = await service.dispatch(OWNER, CHAT, 'propose_ai_loop_run', {
            'operation': 'queue_and_start', 'name': 'native_loop', 'revision': 1, 'reuse_existing': True})
        if change == 'request':
            chat.get_conversation.return_value['messages'].append({'role': 'user', 'content': 'Different request'})
        elif change == 'rental':
            rental['budget'] += 1
        else:
            definition = store.definition(OWNER, 'native_loop')
            if change == 'settings':
                definition['settings']['patience'] += 1
            elif change == 'bundle':
                definition['bundle']['config']['backtest']['starting_balance'] = 12345
            else:
                definition['source']['id'] = 'another_source'
            atomic_write_private_text(store.definition_path(OWNER, 'native_loop'), json.dumps(definition, indent=4))
        with pytest.raises(AICapabilityError, match='changed since review'):
            await approve(service, proposal)
        assert not calls and not store.list(OWNER)

    asyncio.run(scenario())


def test_preserved_reuse_keeps_original_holdout_contract(native_runs):
    """A short start request cannot discard the stored training/holdout policy."""
    from tests.test_ai_loop_windows import plan, bundle, request
    service, store, _, _, _ = native_runs
    original = request(plan())
    definition = store.definition(OWNER, 'native_loop')
    definition['settings']['goals']['text'] = original['text']
    definition['source']['loop_request'] = bind_request(original, store.instruction_version(OWNER, 'default'))
    definition['bundle']['config'] = bundle(plan())
    latest = current_request(CHAT, {'messages': [{'role': 'user', 'content': START_REQUEST}]})
    binding = preserve_request(latest, store.instruction_version(OWNER, 'default'), definition, OWNER)
    settings = {**definition['settings'], 'source': {**definition['source'], 'loop_request': binding}}
    assert validate_request(settings, latest, config=definition['bundle']['config']) == binding
    assert bound_plan(settings) == plan()
    changed = copy.deepcopy(settings)
    changed['goals']['text'] = START_REQUEST
    with pytest.raises(ValueError, match='goals changed'):
        validate_request(changed, latest, config=definition['bundle']['config'])
    with pytest.raises(ValueError, match='reviewed Loop windows'):
        validate_request(settings, latest, config=bundle(plan(False)))


def test_reuse_is_explicit_and_owner_scoped(native_runs):
    """No default reuse, foreign-owner access, invalid revision or settings injection."""
    from api import loop_optimizer_v8 as runtime
    service, store, _, calls, _ = native_runs
    runtime.get_ai_chat_service().get_conversation.return_value = {
        'messages': [{'role': 'user', 'content': START_REQUEST}]}

    async def scenario():
        """Every rejected proposal leaves temporary run state empty."""
        args = {'operation': 'queue_and_start', 'name': 'native_loop', 'revision': 1}
        for owner, extra in [(OWNER, {}), ('c' * 32, {'reuse_existing': True}),
                             (OWNER, {'reuse_existing': True, 'revision': 99}),
                             (OWNER, {'reuse_existing': True, 'settings': {'patience': 1000}})]:
            with pytest.raises(AICapabilityError):
                await service.dispatch(owner, CHAT, 'propose_ai_loop_run', {**args, **extra})
        assert not calls and not store.list(OWNER)

    asyncio.run(scenario())
