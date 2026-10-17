"""Offline image boundary, provider input and persistent-chat regressions."""
from __future__ import annotations

import asyncio
import base64
from pathlib import Path

import pytest

from ai_chat import AIChatError, AIChatService, CodexRuntime
from ai_chat_images import ChatImageStore
from api.ai import TurnCreateRequest

PNG = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1kAAAAASUVORK5CYII=')
OWNER = 'a' * 32
CONVERSATION = 'b' * 32
IMAGE = {'media_type': 'image/png', 'data_base64': base64.b64encode(PNG).decode()}


@pytest.mark.parametrize('protocol,kind', [('responses', 'input_image'), ('chat', 'image_url'), ('messages', 'image')])
def test_native_images_and_owner_boundary(tmp_path, protocol, kind):
    """Protocols receive real image bytes; references never become remote URLs or paths."""
    store = ChatImageStore(tmp_path / 'images')
    images = store.save(OWNER, CONVERSATION, [IMAGE])
    path, data = store.read(OWNER, CONVERSATION, images[0])
    assert data == PNG and path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    history = [{'role': 'user', 'content': 'Look here', 'images': images}, {'role': 'assistant', 'content': 'Seen'}]
    native = store.native_history(OWNER, CONVERSATION, history, protocol)
    assert native[0]['content'][0]['text'] == 'Look here'
    block = native[0]['content'][1]
    assert block['type'] == kind
    assert IMAGE['data_base64'] in str(block)
    assert history[0]['content'] == 'Look here'
    with pytest.raises(ValueError):
        store.read('c' * 32, CONVERSATION, images[0])
    store.clear(OWNER, CONVERSATION)
    assert not path.exists()


@pytest.mark.parametrize('image', [
    {'media_type': 'image/svg+xml', 'data_base64': IMAGE['data_base64']},
    {'media_type': 'image/jpeg', 'data_base64': IMAGE['data_base64']},
    {'media_type': 'image/png', 'data_base64': 'https://example.invalid/image.png'},
    {'media_type': 'image/png', 'data_base64': '!!!!'},
])
def test_invalid_images_are_rejected_before_storage(tmp_path, image):
    """URLs, wrong MIME types and invalid encodings never create image files."""
    store = ChatImageStore(tmp_path / 'images')
    with pytest.raises(ValueError):
        store.save(OWNER, CONVERSATION, [image])
    assert not (tmp_path / 'images').exists()


def test_image_bounds_symlinks_and_api_schema(tmp_path, monkeypatch):
    """Batch/byte limits and identifier checks protect both API and disk boundaries."""
    import ai_chat_images
    from pydantic import ValidationError

    store = ChatImageStore(tmp_path / 'images')
    with pytest.raises(ValidationError):
        TurnCreateRequest(message='look', images=[IMAGE] * 6)
    with pytest.raises(ValueError):
        store.save(OWNER, CONVERSATION, [IMAGE] * 6)
    with pytest.raises(ValueError):
        store.save('../escape', CONVERSATION, [IMAGE])
    monkeypatch.setattr(ai_chat_images, 'MAX_CONVERSATION_BYTES', len(PNG) - 1)
    with pytest.raises(ValueError, match='20 MiB'):
        store.save(OWNER, CONVERSATION, [IMAGE])
    assert not list((tmp_path / 'images').rglob('*.png'))
    with pytest.raises(ValueError):
        store.read(OWNER, CONVERSATION, {'id': '../image.png'})
    # A separate conversation path cannot redirect reads/writes outside the image root.
    (tmp_path / 'images' / OWNER / ('d' * 32)).symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError):
        store.save(OWNER, 'd' * 32, [IMAGE])


def test_images_survive_turn_reload_retry_and_delete(tmp_path, monkeypatch):
    """Real detached turns retain image references, including provider failures and reloads."""
    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        service.credentials.save_go_key(OWNER, 'sk-test-0123456789abcdef')
        conversation = await service._conversation(OWNER, 'opencode-go', 'model', None)
        captured = []

        async def fake_go_chat(owner, model, history, provider, conversation_id, effort):
            captured.extend(history)
            raise AIChatError('Image model fixture failure')

        monkeypatch.setattr(service, '_go_chat', fake_go_chat)
        await service.start_turn(OWNER, conversation.id, 'Show this', images=[IMAGE])
        await service.active_tasks[conversation.id]
        snapshot = await service.get_conversation(OWNER, conversation.id)
        assert snapshot['messages'][-1]['failed']
        assert snapshot['retry_images'] == captured[-1]['images']
        assert 'data_base64' not in str(snapshot)
        reference = snapshot['retry_images'][0]
        path, _ = service.image_store.read(OWNER, conversation.id, reference)
        await service.shutdown()
        restored = AIChatService(tmp_path / 'ai')
        loaded = await restored.get_conversation(OWNER, conversation.id)
        assert loaded['messages'][-1]['images'] == [reference]
        with pytest.raises(AIChatError):
            await restored.get_conversation('c' * 32, conversation.id)
        with pytest.raises(AIChatError, match='Jev does not accept images'):
            await restored.start_turn(OWNER, conversation.id, 'Choose', provider='openrouter', model='jev', images=[IMAGE])
        await restored.delete_conversation(OWNER, conversation.id)
        assert not path.exists()
        await restored.shutdown()

    asyncio.run(scenario())


def test_codex_native_turn_has_local_image_inputs(tmp_path, monkeypatch):
    """ChatGPT receives generated localImage paths alongside the actual user text."""
    async def scenario():
        runtime = CodexRuntime(OWNER, tmp_path / 'codex')
        requests = []

        async def request(method, params, **kwargs):
            requests.append((method, params))
            runtime.notifications.put_nowait({'method': 'item/completed', 'params': {'turnId': 'turn', 'item': {'type': 'agentMessage', 'text': 'Visible dashboard'}}})
            runtime.notifications.put_nowait({'method': 'turn/completed', 'params': {'turn': {'id': 'turn', 'status': 'completed'}}})
            return {'turn': {'id': 'turn'}}

        monkeypatch.setattr(runtime, 'request', request)
        result = await runtime.chat('thread', 'Look', image_paths=['/generated/private/image.png'])
        assert result == 'Visible dashboard'
        parts = requests[0][1]['input']
        assert parts[0]['text'] == 'Look'
        assert parts[1] == {'type': 'localImage', 'path': '/generated/private/image.png'}

    asyncio.run(scenario())


@pytest.mark.parametrize('protocol,agent,kind', [
    ('responses', '_go_responses_agent', 'input_image'),
    ('chat', '_go_chat_completion_agent', 'image_url'),
    ('messages', '_go_messages_agent', 'image'),
])
def test_opencode_native_tool_agents_keep_images(tmp_path, monkeypatch, protocol, agent, kind):
    """Image inputs survive dispatch to every existing tool-enabled provider protocol."""
    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        service.credentials.save_go_key(OWNER, 'sk-test-0123456789abcdef')
        references = service.image_store.save(OWNER, CONVERSATION, [IMAGE])
        captured = []

        async def models(*args):
            return [{'id': 'model', 'protocol': protocol, 'tools': True, 'image_input': True}]

        async def native(*args):
            captured.extend(args[5])
            return 'Dashboard recognized'

        monkeypatch.setattr(service, '_go_models', models)
        monkeypatch.setattr(service, agent, native)
        history = [{'role': 'user', 'content': 'Which page?', 'images': references}]
        answer = await service._go_chat(OWNER, 'model', history, 'opencode-go', CONVERSATION)
        assert answer == 'Dashboard recognized'
        assert captured[0]['content'][1]['type'] == kind
        assert IMAGE['data_base64'] in str(captured)
        assert 'images' not in captured[0]
        assert history[0]['content'] == 'Which page?'
        await service.shutdown()

    asyncio.run(scenario())


def test_image_api_requires_retained_owner_reference(tmp_path, monkeypatch):
    """Authenticated previews cannot cross owners or fetch unreferenced image files."""
    from types import SimpleNamespace
    from fastapi import HTTPException
    from ai_chat import owner_key
    import api.ai as api

    async def scenario():
        owner = owner_key('image-user')
        service = AIChatService(tmp_path / 'ai')
        conversation = await service._conversation(owner, 'opencode-go', 'model', None)
        reference = service.image_store.save(owner, conversation.id, [IMAGE])[0]
        conversation.messages = [{'role': 'user', 'content': 'look', 'images': [reference]}]
        monkeypatch.setattr(api, 'get_ai_chat_service', lambda: service)
        session = SimpleNamespace(user_id='image-user')
        response = await api.chat_image(conversation.id, reference['id'], session)
        assert response.body == PNG
        assert response.headers['cache-control'] == 'no-store'
        assert response.headers['x-content-type-options'] == 'nosniff'
        with pytest.raises(HTTPException):
            await api.chat_image(conversation.id, reference['id'], SimpleNamespace(user_id='other-user'))
        conversation.messages.clear()
        with pytest.raises(HTTPException):
            await api.chat_image(conversation.id, reference['id'], session)
        await service.shutdown()

    asyncio.run(scenario())
