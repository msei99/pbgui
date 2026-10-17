"""Isolated, fixed-input AI connection tests without conversations or PBGui tools."""
from __future__ import annotations

import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from logging_helpers import _redact_text
from support_issue_ai import IssueDraftRequest, draft_for_request, provider_reply


class ModelTestRequest(BaseModel):
    """Validate an explicit model selection, never accept prompts or application data."""
    model_config = ConfigDict(extra='forbid')
    provider: Literal['chatgpt','opencode-go','opencode-zen','openrouter']
    profile: str = Field(default='default', pattern=r'^(?:default|[a-f0-9]{32})$')
    model: str = Field(min_length=1,max_length=128)
    effort: str = Field(default='',max_length=64)
    service_tier: str = Field(default='',max_length=64)


async def test_model(chat, owner, selection, request):
    """Own a bounded provider test through disconnect/shutdown and reuse native transports."""
    body=IssueDraftRequest(description='PBGui connection test')
    async def operation(service, user, unused_body):
        """Send only fixed synthetic input; never read source, logs or conversation history."""
        started=time.monotonic()
        if selection.provider=='openrouter':
            from ai_openrouter import decide_general_choice
            metadata=await service._validate_provider_model(user,selection.provider,selection.model,selection.profile)
            service._validate_model_effort(metadata,selection.effort)
            service._validate_model_service_tier(metadata,selection.provider,selection.service_tier)
            reply=await decide_general_choice(await service._http_session(),service.credentials.load_openrouter_key(user),
                selection.model,'Connection test: select the option stating PBGui connection OK.',
                ['PBGui connection OK','Unrelated option'],max_cost_usd=service.get_preferences(user)['jev_max_cost_usd'])
        else:
            reply,_,_=await provider_reply(service,user,body,selection=selection.model_dump(),
                instructions='This is a connection test. Reply briefly with PBGui connection OK. Do not use tools.',
                content={'connection_test':'Reply PBGui connection OK.'})
        return {'provider':selection.provider,'model':selection.model,'profile':selection.profile,
                'reply':_redact_text(reply)[:4000],'elapsed_seconds':round(time.monotonic()-started,2),
                'tools_enabled':False,'conversation_created':False}
    return await draft_for_request(chat,owner,body,request,operation=operation,timeout=90)
