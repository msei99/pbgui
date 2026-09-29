"""Offline transport regression: fragmented JSON, precise failures and read-only preflight."""
import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest

import ai_openrouter as jev


class Response:
    """Use a real asynchronous byte stream split across separate network-like deliveries."""
    def __init__(self, raw, status=200):
        self.status = status
        self.content = asyncio.StreamReader()
        self.raw = raw
        self.handle = None

    async def __aenter__(self):
        """Deliver the first byte now and the rest after the consumer begins reading."""
        self.content.feed_data(self.raw[:1])
        def finish():
            self.content.feed_data(self.raw[1:])
            self.content.feed_eof()
        self.handle = asyncio.get_running_loop().call_later(.001, finish)
        return self

    async def __aexit__(self, *args):
        """Do not leave delayed callbacks alive after cancellation or HTTP errors."""
        self.handle.cancel()


def test_fragmented_jev_json_is_read_to_eof():
    """A valid split JSON response must not be reported as Jev unavailable."""
    async def run():
        raw = json.dumps({'answers': {'risk': {'type': 'noul', 'noul': .5}}}).encode()
        session = Mock(post=Mock(return_value=Response(raw)))
        result = await jev._send_structured_jev_request(session, 'test-key', {
            'questions': {'risk': {'type': 'noul', 'instructions': 'Assess risk'}}})
        assert '50%' in result
        session.post.assert_called_once()
    asyncio.run(run())


@pytest.mark.parametrize('raw,message', [(b'{not json', 'invalid JSON'), (b'x' * (2 * 1024 * 1024 + 1), 'too large')])
def test_invalid_or_oversized_jev_body_is_not_a_provider_outage(raw, message):
    """Provider data stays bounded and failures are classified without leaking the body."""
    async def run():
        session = Mock(post=Mock(return_value=Response(raw)))
        with pytest.raises(jev.OpenRouterDecisionError, match=message):
            await jev._send_structured_jev_request(session, 'test-key', {'questions': {}})
        session.post.assert_called_once()
    asyncio.run(run())


@pytest.mark.parametrize('status,remaining,expected', [
    (200, None, None), (200, 1, None), (401, None, 'HTTP 401'),
    (200, 0, 'spending limit exhausted'), (200, 'invalid', 'invalid quota'),
])
def test_jev_preflight_checks_auth_quota_and_pricing_without_decision(status, remaining, expected):
    """Only GET metadata is sent; no research evidence or billable decision is submitted."""
    async def run():
        def get(url, **kwargs):
            assert kwargs['allow_redirects'] is False
            assert 'json' not in kwargs and 'data' not in kwargs
            if url.endswith('/v1/key'):
                return Response(json.dumps({'data': {'limit_remaining': remaining}}).encode(), status)
            assert url.endswith('/v1/model/' + jev.JEV_MODEL)
            return Response(b'{"data":{"pricing":{"prompt":"0.000000042","completion":"0"}}}')
        session = Mock(get=Mock(side_effect=get), post=Mock())
        if expected:
            with pytest.raises(jev.OpenRouterDecisionError, match=expected):
                await jev.check_jev_connection(session, 'test-key')
        else:
            await jev.check_jev_connection(session, 'test-key')
            assert session.get.call_count == 2
        session.post.assert_not_called()
    asyncio.run(run())
