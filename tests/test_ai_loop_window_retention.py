"""Offline conversation lifecycle coverage for reviewed AI Loop window bindings."""
import asyncio
import copy
import json

import pytest

from ai_chat import AIChatError, AIChatService, _MAX_HISTORY_MESSAGES
from ai_capabilities import AICapabilityError, AICapabilityService
from ai_loop_request import bind_request, current_request, validate_request
from ai_loop_windows import MARKER, stored_plan
from pb8_loop_store import LoopStore
from tests.test_ai_loop_windows import bundle, plan, request
from tests.test_pb8_loop_optimizer import OWNER, settings


@pytest.fixture(autouse=True)
def isolated_capabilities(tmp_path, monkeypatch):
    """Keep the shared capability service under the same temporary chat root."""
    import ai_chat
    capabilities = AICapabilityService(tmp_path / 'ai' / 'capabilities')
    monkeypatch.setattr(ai_chat, 'get_ai_capability_service', lambda: capabilities)
    yield capabilities
    asyncio.run(capabilities.shutdown())


@pytest.mark.parametrize('enabled', [True, False])
def test_reviewed_windows_survive_real_turn_trimming_and_reload(tmp_path, monkeypatch, enabled):
    """New definitions and save proposals remain protected after the original turn is gone."""
    async def scenario():
        """Use only fake model replies, temporary credentials and isolated Loop records."""
        service = AIChatService(tmp_path / 'ai')
        service.credentials.save_go_key(OWNER, 'sk-test-0123456789abcdef')
        conversation = await service._conversation(OWNER, 'opencode-go', 'model', None)
        chat_id = conversation.id
        value = plan(enabled)

        async def reply(*_args):
            """Assistant-suggested plans never replace the user's reviewed contract."""
            return 'Untrusted assistant suggestion\n' + MARKER + json.dumps(plan(not enabled))

        async def send(chat, text, **kwargs):
            """Drive the same accepted-message and detached-turn path as the GUI."""
            await chat.start_turn(OWNER, chat_id, text, **kwargs)
            await chat.active_tasks[chat_id]

        monkeypatch.setattr(service, '_go_chat', reply)
        try:
            await send(service, request(value)['text'])
            for index in range(14):
                await send(service, f'Keep the reviewed dates. Follow-up {index}.')
            snapshot = await service.get_conversation(OWNER, chat_id)
            assert len(snapshot['messages']) <= _MAX_HISTORY_MESSAGES
            assert not any(MARKER in item['content'] for item in snapshot['messages'] if item['role'] == 'user')
            assert current_request(chat_id, snapshot)['window_plan'] == value
            assert stored_plan(service.root, OWNER, chat_id) == value
        finally:
            await service.shutdown()

        restored = AIChatService(tmp_path / 'ai')
        monkeypatch.setattr(restored, '_go_chat', reply)
        try:
            snapshot = await restored.get_conversation(OWNER, chat_id)
            req = current_request(chat_id, snapshot)
            assert req['window_plan'] == value
            assert req['window_request'] == request(value)['text']
            store = LoopStore(tmp_path / 'loops')
            opts = settings(max_validations=8)
            opts['goals'] = {**opts['goals'], 'text': req['text'], 'presets': [], 'targets': {}}
            opts['source'] = {'loop_request': bind_request(req, store.instruction_version(OWNER, 'default'))}
            with pytest.raises(ValueError, match='reviewed Loop windows'):
                store.create(OWNER, opts, bundle(plan(not enabled)), {}, {}, [], {})
            row = store.create(OWNER, opts, bundle(value), {}, {}, [], {})
            assert row['observer_enabled']
            assert row['final_holdout_policy'] == 'observer_only'

            capabilities = restored.capabilities
            monkeypatch.setattr(capabilities, '_current_pb8_bundle', lambda _name: (None, {}, None))
            monkeypatch.setattr(capabilities, '_validate_pb8_config', lambda _name, candidate: copy.deepcopy(candidate))
            monkeypatch.setattr(capabilities, '_queue_preview', lambda *_args: {})
            raw = bundle(value)
            raw.pop('pbgui')
            proposed = await capabilities._propose_pb8_optimizer_config(OWNER, chat_id, {
                'name': 'retained', 'config': raw,
                'scenario_template': dict(value, template='ai_loop_windows')})
            assert capabilities.proposals[proposed['proposal_id']].preview['reviewed_window_plan'] == value
            with pytest.raises(AICapabilityError, match='reviewed Loop windows'):
                await capabilities._propose_pb8_optimizer_config(OWNER, chat_id, {
                    'name': 'drifted', 'config': raw,
                    'scenario_template': dict(plan(not enabled), template='ai_loop_windows')})

            await send(restored, request(plan(not enabled))['text'], internal=True)
            assert stored_plan(restored.root, OWNER, chat_id) == value
            with pytest.raises(AIChatError):
                await restored.get_conversation('c' * 32, chat_id)
            assert stored_plan(restored.root, 'c' * 32, chat_id) is None

            await send(restored, request(plan(not enabled))['text'])
            for index in range(14):
                await send(restored, f'Keep the new reviewed dates. Follow-up {index}.')
            replacement = current_request(chat_id, await restored.get_conversation(OWNER, chat_id))
            assert replacement['window_plan'] == plan(not enabled)
            assert stored_plan(restored.root, OWNER, chat_id) == plan(not enabled)
            with pytest.raises(ValueError, match='current chat request'):
                validate_request(opts, replacement, config=bundle(value))
        finally:
            await restored.shutdown()
    asyncio.run(scenario())


def test_legacy_conversation_migrates_windows_before_reload_truncation(tmp_path):
    """Recover an existing owned user contract before bounding a legacy transcript."""
    async def scenario():
        """Persist only synthetic messages under the temporary owner directory."""
        first = AIChatService(tmp_path / 'ai')
        try:
            conversation = await first._conversation(OWNER, 'opencode-go', 'model', None)
            path = first._conversation_path(OWNER, conversation.id)
            saved = json.loads(path.read_text())
            saved.pop('loop_window_request', None)
            saved['messages'] = [{'role': 'user', 'content': request(plan())['text']},
                                 {'role': 'assistant', 'content': 'Acknowledged'}]
            saved['messages'] += [{'role': role, 'content': 'Keep reviewed dates'}
                                  for _ in range(14) for role in ('user', 'assistant')]
            path.write_text(json.dumps(saved))
        finally:
            await first.shutdown()
        restored = AIChatService(tmp_path / 'ai')
        try:
            snapshot = await restored.get_conversation(OWNER, conversation.id)
            assert len(snapshot['messages']) == _MAX_HISTORY_MESSAGES
            assert current_request(conversation.id, snapshot)['window_plan'] == plan()
            await restored.record_local_action(OWNER, conversation.id, 'Keep reviewed dates')
            assert stored_plan(restored.root, OWNER, conversation.id) == plan()
        finally:
            await restored.shutdown()
    asyncio.run(scenario())


@pytest.mark.parametrize('invalid', ['', 'No reviewed contract', MARKER + 'invalid', {}, 42])
def test_invalid_durable_binding_never_falls_back_to_unprotected_history(tmp_path, invalid):
    """A corrupt stored binding fails closed for both readers, even without a marker in history."""
    conversation = {'owner': OWNER, 'conversation_id': 'b' * 32,
                    'loop_window_request': invalid,
                    'messages': [{'role': 'user', 'content': 'Keep the reviewed dates'}]}
    directory = tmp_path / 'conversations' / OWNER
    directory.mkdir(parents=True)
    (directory / ('b' * 32 + '.json')).write_text(json.dumps(conversation))
    with pytest.raises(ValueError):
        current_request('b' * 32, conversation)
    with pytest.raises(ValueError):
        stored_plan(tmp_path, OWNER, 'b' * 32)


def test_provider_failure_rewind_and_invalid_replacement_keep_protected_windows(tmp_path, monkeypatch):
    """Provider errors and transcript edits cannot silently turn a reviewed chat into a legacy chat."""
    async def scenario():
        """Use the real failure, rewind and synchronous chat paths with a fake provider."""
        service = AIChatService(tmp_path / 'ai')
        service.credentials.save_go_key(OWNER, 'sk-test-0123456789abcdef')
        conversation = await service._conversation(OWNER, 'opencode-go', 'model', None)

        async def unavailable(*_args):
            """Fail without contacting a model or executing a tool."""
            raise AIChatError('Fixture provider unavailable')

        async def reply(*_args):
            """A normal marker-free follow-up does not remove accepted constraints."""
            return 'Fixture answer'

        monkeypatch.setattr(service, '_go_chat', unavailable)
        try:
            await service.start_turn(OWNER, conversation.id, request(plan())['text'])
            await service.active_tasks[conversation.id]
            assert conversation.messages[-1]['failed']
            assert stored_plan(service.root, OWNER, conversation.id) == plan()
            await service.rewind_conversation(OWNER, conversation.id, 0)
            assert not conversation.messages
            assert stored_plan(service.root, OWNER, conversation.id) == plan()
            with pytest.raises(AIChatError, match='Invalid reviewed Loop window plan'):
                await service.start_turn(OWNER, conversation.id, MARKER + 'invalid')
            assert not conversation.busy and conversation.id not in service.active_tasks
            assert stored_plan(service.root, OWNER, conversation.id) == plan()
            monkeypatch.setattr(service, '_go_chat', reply)
            await service.chat(OWNER, 'opencode-go', 'model', 'Keep the reviewed dates', conversation.id)
            snapshot = await service.get_conversation(OWNER, conversation.id)
            assert current_request(conversation.id, snapshot)['window_plan'] == plan()
            await service.chat(OWNER, 'opencode-go', 'model', request(plan(False))['text'], conversation.id)
            assert stored_plan(service.root, OWNER, conversation.id) == plan(False)
        finally:
            await service.shutdown()
    asyncio.run(scenario())
