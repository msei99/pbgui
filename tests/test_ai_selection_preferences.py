"""Owner-scoped durable model selection without provider or runtime access."""
import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from ai_chat import AIChatService, AIChatError
from api import ai as ai_api


SELECTION = {'provider': 'chatgpt', 'profile': 'b' * 32, 'model': 'gpt-6.1-sol',
             'effort': 'high', 'service_tier': 'fast'}


def test_selection_survives_reconstruction_geometry_updates_and_owner_isolation(tmp_path):
    """Unrelated drawer writes and another account cannot reset the saved model."""
    service = AIChatService(tmp_path / 'ai')
    owner = 'a' * 32
    service.save_preferences(owner, selection=SELECTION)
    service.save_preferences(owner, drawer_width=612, drawer_open=True)
    reconstructed = AIChatService(tmp_path / 'ai')
    assert reconstructed.get_preferences(owner)['selection'] == SELECTION
    assert reconstructed.get_preferences(owner)['drawer_width'] == 612
    assert 'selection' not in reconstructed.get_preferences('c' * 32)
    assert service._preference_path(owner).stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize('patch', [
    {'provider': 'unknown'}, {'model': ''}, {'model': 'x' * 201},
    {'model': 'bad\nmodel'}, {'effort': False}, {'api_key': 'unexpected'},
])
def test_selection_validation_preserves_previous_preferences(tmp_path, patch):
    """Invalid or secret-shaped payloads fail before any preference replacement."""
    service = AIChatService(tmp_path / 'ai')
    owner = 'a' * 32
    service.save_preferences(owner, selection=SELECTION)
    with pytest.raises(AIChatError, match='Invalid AI model selection'):
        service.save_preferences(owner, selection={**SELECTION, **patch})
    assert service.get_preferences(owner)['selection'] == SELECTION


def test_api_selection_is_owner_scoped_no_store_and_reports_invalid_values(tmp_path, monkeypatch):
    """Use the real service through authenticated route logic with isolated storage."""
    service = AIChatService(tmp_path / 'ai')
    monkeypatch.setattr(ai_api, 'get_ai_chat_service', lambda: service)
    session = SimpleNamespace(user_id='test-user')
    response = asyncio.run(ai_api.save_preferences(ai_api.AIPreferencesRequest(selection=SELECTION), session))
    assert response.headers['cache-control'] == 'no-store'
    assert json.loads(response.body)['selection'] == SELECTION
    assert json.loads(asyncio.run(ai_api.get_preferences(session)).body)['selection'] == SELECTION
    with pytest.raises(HTTPException) as error:
        asyncio.run(ai_api.save_preferences(ai_api.AIPreferencesRequest(selection={**SELECTION, 'model': ''}), session))
    assert error.value.status_code == 400
