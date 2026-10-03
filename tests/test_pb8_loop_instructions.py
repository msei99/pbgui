"""Offline instruction version ownership, immutable run snapshots and provider delivery."""
import asyncio

import pytest

from pb8_loop_ai import INSTRUCTIONS, LoopAI, instructions_for_run
from pb8_loop_store import LoopInstructionConflict, LoopStore
from tests.test_pb8_loop_optimizer import OWNER, config, settings, _loop_chat_for_payloads
from tests.test_pb8_loop_workflow import workflow, save
from tests.test_pb8_loop_gpt_timeout import native_ai, stop_feeders


def test_instruction_versions_are_owned_immutable_and_revision_checked(tmp_path):
    """Activation never replaces a saved version or another owner's instructions."""
    store = LoopStore(tmp_path / 'loops')
    assert store.instruction_version(OWNER)['text'] == INSTRUCTIONS
    first = store.save_instructions(OWNER, 'Experiment', 'First private prompt', 0)
    with pytest.raises(LoopInstructionConflict):
        store.save_instructions(OWNER, 'Stale edit', 'Keep existing versions', 0)
    second = store.save_instructions(OWNER, 'Experiment 2', 'Second private prompt', 1)
    assert first['id'] != second['id']
    assert store.instruction_version(OWNER, first['id']) == first
    assert store.instruction_version('b' * 32)['text'] == INSTRUCTIONS
    with pytest.raises(FileNotFoundError):
        store.instruction_version('b' * 32, first['id'])
    store.activate_instructions(OWNER, first['id'], 2)
    assert store.instruction_version(OWNER) == first
    path = store._instructions_path(OWNER)
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert store.list() == []


@pytest.mark.parametrize('name,text', [('', 'text'), ('x', ''), ('x', ' '), ('x', '\x00'), ('x', 'a' * 64001), ('x\n', 'text')])
def test_invalid_instruction_versions_do_not_write(tmp_path, name, text):
    """Reject empty, unbounded and control-character inputs before persistence."""
    store = LoopStore(tmp_path / 'loops')
    with pytest.raises(ValueError):
        store.save_instructions(OWNER, name, text, 0)
    assert not list(tmp_path.rglob('versions.json'))


def test_instruction_storage_rejects_symlinks(tmp_path):
    """A persisted symlink cannot redirect private reads or writes outside the owner."""
    store = LoopStore(tmp_path / 'loops')
    path = store._instructions_path(OWNER)
    outside = tmp_path / 'outside.json'
    outside.write_text('{}')
    path.symlink_to(outside)
    with pytest.raises(ValueError):
        store.save_instructions(OWNER, 'Unsafe', 'Text', 0)
    assert outside.read_text() == '{}'


def test_run_snapshots_survive_selection_and_builtin_changes(tmp_path, monkeypatch):
    """Queued runs freeze exact text; new/continued runs use the current selection."""
    store = LoopStore(tmp_path / 'loops')
    first = store.save_instructions(OWNER, 'First', 'First prompt', 0)
    value = config()
    row = store.create(OWNER, settings(), value, {}, {}, [], value['backtest'], queued=True)
    store.save_instructions(OWNER, 'Second', 'Second prompt', 1)
    reloaded = LoopStore(store.root).read(OWNER, row['id'])
    assert reloaded['ai_instructions'] == first
    assert instructions_for_run(reloaded)['text'] == 'First prompt'
    another = store.create(OWNER, settings(), value, {}, {}, [], value['backtest'], queued=True)
    assert instructions_for_run(another)['text'] == 'Second prompt'
    store.activate_instructions(OWNER, 'default', 2)
    default_run = store.create(OWNER, settings(), value, {}, {}, [], value['backtest'])
    monkeypatch.setattr('pb8_loop_ai.INSTRUCTIONS', 'Updated built-in')
    assert instructions_for_run(default_run)['text'] == INSTRUCTIONS
    assert instructions_for_run({})['legacy'] is True
    assert instructions_for_run({})['text'] == 'Updated built-in'


def test_instruction_routes_save_activate_and_show_run_snapshot(workflow):
    """The authenticated API scopes every version and run snapshot to its owner."""
    client, controller, source, session, _ = workflow
    response = client.get('/loops/instructions')
    assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
    assert all('text' not in item for item in response.json()['versions'])
    payload = {'name': 'My version', 'text': 'My exact instructions', 'revision': 0}
    response = client.post('/loops/instructions', json=payload)
    assert response.status_code == 201
    version = response.json()
    assert client.post('/loops/instructions', json=payload).status_code == 409
    assert client.get('/loops/instructions/' + version['id']).json()['text'] == payload['text']
    assert save(client).status_code == 200
    queued = client.post('/loops/configs/Named/queue', json={'provider':'chatgpt', 'model':'pinned', 'authorization':True})
    assert queued.status_code == 201
    run_id = queued.json()['id']
    assert client.get('/loops/' + run_id + '/instructions').json()['text'] == payload['text']
    selected = client.post('/loops/instructions/active', json={'version_id':'default', 'revision':1})
    assert selected.status_code == 200
    assert client.get('/loops/' + run_id + '/instructions').json()['text'] == payload['text']
    session.user_id = 'b' * 32
    assert client.get('/loops/instructions/' + version['id']).status_code == 404
    assert client.get('/loops/' + run_id + '/instructions').status_code == 404
    assert client.post('/loops/instructions/active', json={'version_id':version['id'], 'revision':0}).status_code == 404
    assert client.post('/loops/instructions/active', json={'version_id':'../outside', 'revision':0}).status_code == 422


@pytest.mark.parametrize('protocol,payload', [
    ('chat', {'choices':[{'finish_reason':'stop', 'message':{'content':'{}'}}]}),
    ('responses', {'status':'completed', 'output':[{'type':'message', 'content':[{'type':'output_text', 'text':'{}'}]}]}),
    ('messages', {'stop_reason':'end_turn', 'content':[{'type':'text', 'text':'{}'}]}),
])
def test_saved_instructions_reach_each_http_protocol(tmp_path, protocol, payload):
    """Check actual outgoing system instructions without contacting any provider."""
    store = LoopStore(tmp_path / 'loops')
    version = store.save_instructions(OWNER, 'Chosen', 'Saved custom instructions', 0)
    value = config()
    row = store.create(OWNER, settings(provider='opencode-go'), value, {}, {}, [], value['backtest'])
    chat = _loop_chat_for_payloads([payload])
    original_session = chat._http_session
    captured = []
    async def session():
        """Capture the exact request passed to the isolated response fixture."""
        result = await original_session()
        post = result.post
        def capture(url, **kwargs):
            """Retain only the JSON body, never request credentials."""
            captured.append(kwargs['json'])
            return post(url, **kwargs)
        result.post = capture
        return result
    chat._http_session = session
    asyncio.run(LoopAI(store, chat)._request(row, {'protocol':protocol}, {}, 1000))
    body = captured[0]
    assert (body['messages'][0]['content'] if protocol == 'chat' else body['instructions'] if protocol == 'responses' else body['system']) == version['text']


def test_saved_instructions_reach_native_gpt(tmp_path, monkeypatch):
    """A native GPT request uses the selected frozen text and closes its runtime."""
    store = LoopStore(tmp_path / 'loops')
    version = store.save_instructions(OWNER, 'GPT version', 'Frozen GPT instructions', 0)
    value = config()
    row = store.create(OWNER, settings(), value, {}, {}, [], value['backtest'])
    async def feed(runtime):
        """Complete a local fake native turn without a remote model."""
        runtime.notifications.put_nowait({'method':'item/completed', 'params':{'turnId':'turn', 'item':{'type':'agentMessage', 'text':'{}'}}})
        runtime.notifications.put_nowait({'method':'turn/completed', 'params':{'turnId':'turn', 'turn':{'id':'turn', 'status':'completed'}}})
    ai, runtimes, feeders = native_ai(store, tmp_path, monkeypatch, feed)
    async def scenario():
        """Join all test-owned producer tasks."""
        try:
            await ai.decide(row, 'evaluate', {})
        finally:
            await stop_feeders(feeders)
    asyncio.run(scenario())
    assert runtimes[0].loop_instructions == version['text']
    runtimes[0].close.assert_awaited_once()


@pytest.mark.parametrize('name', ['PBGui default', ' pbgui DEFAULT ', ' Experiment '])
def test_instruction_names_are_unique(tmp_path, name):
    """Reserved and existing names cannot impersonate immutable saved versions."""
    store = LoopStore(tmp_path / 'loops')
    original = store.save_instructions(OWNER, 'Experiment', 'Original', 0)
    with pytest.raises(ValueError, match='unique'):
        store.save_instructions(OWNER, name, 'Replacement', 1)
    assert store.instruction_version(OWNER) == original
    assert store.instructions(OWNER)['revision'] == 1


def test_delete_instruction_version_retains_run_snapshot(tmp_path):
    """Deleting active/inactive versions preserves snapshots and checks revision/owner."""
    store = LoopStore(tmp_path / 'loops')
    first = store.save_instructions(OWNER, 'First', 'First prompt', 0)
    value = config()
    row = store.create(OWNER, settings(), value, {}, {}, [], value['backtest'])
    second = store.save_instructions(OWNER, 'Second', 'Second prompt', 1)
    with pytest.raises(ValueError, match='cannot be deleted'):
        store.delete_instructions(OWNER, 'default', 2)
    with pytest.raises(LoopInstructionConflict):
        store.delete_instructions(OWNER, first['id'], 1)
    with pytest.raises(FileNotFoundError):
        store.delete_instructions('b' * 32, first['id'], 0)
    assert store.delete_instructions(OWNER, first['id'], 2) == {'active':second['id'], 'revision':3}
    assert store.delete_instructions(OWNER, second['id'], 3) == {'active':'default', 'revision':4}
    assert store.instruction_version(OWNER)['text'] == INSTRUCTIONS
    assert store.read(OWNER, row['id'])['ai_instructions'] == first
    assert len(store.instructions(OWNER)['versions']) == 1


def test_instruction_delete_api(workflow):
    """Authenticated deletion protects default, ownership and optimistic revision."""
    client, controller, source, session, _ = workflow
    payload = {'name':'Personal', 'text':'Private prompt', 'revision':0}
    version = client.post('/loops/instructions', json=payload).json()
    path = '/loops/instructions/' + version['id']
    assert client.post('/loops/instructions', json={**payload,'revision':1}).status_code == 422
    assert client.request('DELETE', '/loops/instructions/default', json={'revision':1}).status_code == 422
    assert client.request('DELETE', path, json={'revision':0}).status_code == 409
    session.user_id = 'b' * 32
    assert client.request('DELETE', path, json={'revision':0}).status_code == 404
    session.user_id = OWNER
    response = client.request('DELETE', path, json={'revision':1})
    assert response.status_code == 200 and response.json()['active'] == 'default'
    assert response.headers['cache-control'] == 'no-store'
    assert client.get(path).status_code == 404
