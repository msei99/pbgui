"""Claude subscription Loop decisions retain the shared evidence and approval boundaries."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ai_chat import AIChatError, AIChatService
from api import loop_optimizer_v8 as routes
from pb8_loop_ai import LoopAI, LoopAIRetryScheduled
from pb8_loop_evidence import LoopAIPause
from tests.test_pb8_loop_optimizer import OWNER, RULE, record
from tests.test_ai_loop_tools import environment, native_runs


def test_loop_schema_and_model_route_accept_claude_without_authorizing_jobs(monkeypatch):
    """A Claude selection is valid but never grants optimizer/rental authorization."""
    settings = routes.LoopStart(config_name='fixture', provider='claude-code', model='claude-sonnet-5-5', max_rounds=1)
    assert settings.authorization is False
    models = AsyncMock(return_value=[{'id': settings.model, 'tools': True}])
    monkeypatch.setattr(routes, 'get_ai_chat_service', lambda: SimpleNamespace(models=models))
    response = asyncio.run(routes.models('claude-code', session=SimpleNamespace(user_id='fixture')))
    assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
    assert json.loads(response.body)['models'][0]['id'] == settings.model
    assert models.await_args.args[1] == 'claude-code'


@pytest.mark.parametrize('ending', ['complete', 'compaction', 'invalid', 'cancel', 'too_small', 'timeout', 'structured_failure'])
def test_claude_loop_owned_evidence_decision_and_recovery(record, ending):
    """Interpret through the production Loop orchestrator, without chat tools or a real provider."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings'].update(
        provider='claude-code', model='claude-sonnet-5-5', effort='high'))
    attempts = []

    async def reply(owner, model, history, instructions, **options):
        """Exercise the actual read-only callback and SDK event boundary."""
        attempts.append(model)
        assert owner == OWNER and model == row['settings']['model']
        assert options['effort'] == 'high' and options['handler_with_id'] is True
        assert options['output_schema']['type'] == 'object'
        assert set(options['output_schema']['required']) == {'reason', 'rubric'}
        assert [tool['name'] for tool in options['tools']] == ['loop_evidence']
        assert 'autonomous PB8' in instructions and history[0]['role'] == 'user'
        handler = options['handler']
        assert await handler('approve_proposal', {}, 'unavailable') == {'error': 'tool_not_allowed'}
        assert await handler('loop_evidence', {'path': '../../pbgui.ini', 'offset': 0}, 'invalid') == {'error': 'invalid_evidence_request'}
        if ending == 'compaction':
            await options['on_event']({'event': 'compaction'})
        offset = 0
        while offset is not None:
            page = await handler('loop_evidence', {'path': '/required', 'offset': offset}, f'page-{len(attempts)}-{offset}')
            offset = page.get('next_offset')
        if ending == 'cancel':
            raise asyncio.CancelledError
        if ending == 'timeout':
            raise AIChatError('Claude request timed out')
        if ending == 'structured_failure':
            raise AIChatError('Claude did not return the required structured decision.')
        await options['on_event']({'event': 'usage', 'input_tokens': 1000, 'output_tokens': 100, 'context_window': 1000000})
        return 'not JSON' if ending == 'invalid' else json.dumps({'rubric': RULE, 'reason': 'Fixture interpretation'})

    chat = SimpleNamespace(
        _validate_provider_model=AsyncMock(return_value={'id': 'claude-sonnet-5-5', 'tools': True,
            'reasoning_variants': [{'id': 'high', 'label': 'High'}]}),
        _selected_reasoning_variant=AIChatService._selected_reasoning_variant,
        _validate_model_service_tier=AIChatService._validate_model_service_tier,
        claude=SimpleNamespace(model_context=AsyncMock(return_value={'context': 200000 if ending == 'too_small' else 1000000})),
        _claude_reply=reply)
    ai = LoopAI(store, chat=chat)
    if ending == 'cancel':
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(ai.decide(row, 'interpret', {}))
        saved = store.read(OWNER, row['id'])
        assert saved['pending_ai'] and saved['ai_calls'] == 1
        with pytest.raises(LoopAIPause, match='interrupted'):
            asyncio.run(ai.decide(saved, 'interpret', {}))
        assert len(attempts) == 1
    elif ending == 'timeout':
        with pytest.raises(LoopAIRetryScheduled):
            asyncio.run(ai.decide(row, 'interpret', {}))
        saved = store.read(OWNER, row['id'])
        assert saved['ai_retry']['attempts'] == 1 and saved['pending_ai'] is None
        assert saved['ai_tokens_reserved'] > 0 and len(attempts) == 1
    elif ending in {'invalid', 'too_small', 'structured_failure'}:
        with pytest.raises(LoopAIPause) as paused:
            asyncio.run(ai.decide(row, 'interpret', {}))
        assert len(attempts) == (2 if ending == 'invalid' else 1 if ending == 'structured_failure' else 0)
        saved = store.read(OWNER, row['id'])
        assert saved.get('pending_ai') is None
        if ending == 'structured_failure':
            assert paused.value.details['kind'] == 'structured_output'
            assert saved['last_ai_request']['decision_status'] == 'rejected'
            assert saved['last_ai_request']['rejection'] == {'kind': 'structured_output'}
        if ending == 'invalid':
            assert paused.value.details['attempts'] == 2
            failures = saved['claude_decision_rejections']
            assert len(failures) == 2 and all(item['kind'] == 'invalid_json' for item in failures)
            assert failures[-1]['line'] == 1 and failures[-1]['column'] == 1
            assert failures[-1]['response_characters'] == len('not JSON')
            assert saved['last_ai_request']['decision_status'] == 'rejected'
            assert all('not JSON' not in json.dumps(item) for item in failures)
    else:
        decision = asyncio.run(ai.decide(row, 'interpret', {}))
        assert decision['rubric'] == RULE
        saved = store.read(OWNER, row['id'])
        assert saved['pending_ai'] is None and saved['ai_calls'] == 1
        assert saved['last_usage']['status'] == 'reported'
        assert saved['ai_model_profile']['context'] == 1000000
        assert saved['ai_evidence_status']['tool_calls'] > 0


def test_claude_start_proposal_preserves_selected_provider(native_runs):
    """Claude queues/starts only after approval through the existing native interfaces."""
    from tests.test_ai_loop_tools import approve
    service, store, _rental, calls, _start = native_runs
    chat = routes.get_ai_chat_service()
    selection = {'provider': 'claude-code', 'model': 'claude-sonnet-5-5', 'profile': 'default', 'effort': 'high', 'service_tier': ''}
    chat.get_preferences = lambda _owner: {'selection': selection}

    async def scenario():
        """Persist only the isolated definition; inspect the unapproved start proposal."""
        try:
            proposal = await service.dispatch(OWNER, 'b' * 32, 'propose_ai_loop_run', {'name': 'native_loop', 'revision': 1, 'operation': 'queue_and_start'})
            assert {key: proposal['preview']['settings'][key] for key in selection} == selection
            assert store.list(OWNER) == [] and calls == []
            await approve(service, proposal)
            assert store.list(OWNER)[0]['settings']['provider'] == 'claude-code'
            assert calls[0][2:] == (True, selection['model'])
            assert calls[1][-1] == selection
        finally:
            await service.shutdown()
    asyncio.run(scenario())
