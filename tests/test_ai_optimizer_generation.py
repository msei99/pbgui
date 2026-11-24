"""Keep AI optimizer mutations and cloud-log handoffs on the user's generation."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from ai_capabilities import AICapabilityService, AICapabilityError


@pytest.mark.parametrize('tool,args', [
    ('propose_pb8_optimizer_config', {'name': 'hype'}),
    ('propose_pb8_config_patch', {'name': 'hype'}),
    ('create_config_draft', {'version': 'v8', 'config': {}}),
    ('propose_reviewed_config_change', {'kind': 'optimizer', 'version': 'v8'}),
    ('propose_vast_optimizer_run', {'version': 'v8', 'name': 'hype'}),
    ('perform_page_action', {'page_key': 'v8_optimize', 'entity_name': 'hype'}),
    ('perform_page_action', {'page_key': 'v9_optimize', 'entity_name': 'v8_optimize'}),
    ('list_pb8_optimizer_queue', {}),
])
def test_native_page_rejects_legacy_creation_and_navigation(tool, args):
    """PB8 tools cannot silently create a config or move the browser out of PB9."""
    with pytest.raises(AICapabilityError, match='Current page is PB9'):
        AICapabilityService.page_arguments(tool, args, {'page_key': 'v9_optimize'})


def test_vast_and_queue_inherit_current_generation_without_mutating_arguments():
    """An omitted legacy version is bound to the actual page rather than default PB8."""
    original = {'name': 'hype'}
    assert AICapabilityService.page_arguments('propose_vast_optimizer_run', original, {'page_key': 'v9_optimize'}) == {'name': 'hype', 'version': 'v9'}
    assert original == {'name': 'hype'}
    assert AICapabilityService.page_arguments('list_optimizer_queue', {}, {'page_key': 'v9_optimize'}) == {'version': 'v9'}
    # Explicit read-only comparisons remain possible.
    assert AICapabilityService.page_arguments('get_optimizer_config', {'version': 'v8'}, {'page_key': 'v9_optimize'}) == {'version': 'v8'}


def test_claude_agent_guard_runs_before_tool_can_create_wrong_config(tmp_path, monkeypatch):
    """Exercise the provider's real dispatcher, not only the pure policy helper."""
    from ai_chat import AIChatService
    capabilities = AICapabilityService(tmp_path / 'capabilities')
    monkeypatch.setattr('ai_chat.get_ai_capability_service', lambda: capabilities)
    async def run():
        """No provider call is made; recorded tool invocations are inert."""
        service = AIChatService(tmp_path / 'chat')
        owner = 'a'*32
        conversation = await service._conversation(owner, 'claude-code', 'fixture', None)
        conversation.context = {'page_key': 'v9_optimize'}
        dispatch = AsyncMock(return_value={'fixture': True})
        monkeypatch.setattr(capabilities, 'dispatch', dispatch)
        result = await service._agent_capability_result(owner, conversation.id, 'propose_pb8_optimizer_config', {'name': 'hype'}, {})
        assert result['success'] is False and 'PB9' in result['error']
        assert dispatch.await_count == 0
        result = await service._agent_capability_result(owner, conversation.id, 'propose_vast_optimizer_run', {'name': 'hype'}, {})
        assert result['success'] is True
        assert dispatch.await_args.args[-1]['version'] == 'v9'
        seen = {}
        conversation.context = {'page_key': 'v8_optimize'}
        accepted = await service._agent_capability_result(owner, conversation.id, 'propose_pb8_optimizer_config', {'name': 'hype'}, seen)
        assert accepted['success'] is True
        calls = dispatch.await_count
        conversation.context = {'page_key': 'v9_optimize'}
        refused = await service._agent_capability_result(owner, conversation.id, 'propose_pb8_optimizer_config', {'name': 'hype'}, seen)
        assert refused['success'] is False and dispatch.await_count == calls
        await service.shutdown()
    asyncio.run(run())


def test_legacy_non_optimizer_actions_keep_their_existing_generation():
    """Native guards do not change explicit PB7 run/backtest drafts on legacy pages."""
    args = {'version': 'v7', 'kind': 'backtest'}
    assert AICapabilityService.page_arguments('propose_reviewed_config_change', args, {'page_key':'v8_optimize'}) == args
    assert AICapabilityService.page_arguments('propose_vast_optimizer_run', {}, {'page_key':'v7_optimize'}) == {}
