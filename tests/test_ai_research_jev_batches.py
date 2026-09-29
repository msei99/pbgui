"""Offline regressions: multi-asset Jev research uses one bounded Decisions request."""
import asyncio
import json
from unittest.mock import Mock

import pytest
import ai_openrouter as jev
from ai_token_budget import estimate_jev_input_tokens, JEV_CONTEXT_TOKENS


def questions(count=39):
    """Build one explicit risk classification per asset."""
    return {f'coin_{i}': {'type': 'choice', 'instructions': f'Classify public risk for asset {i}',
            'criteria': {'low': 'Evidence supports low risk', 'high': 'Evidence supports high risk',
                         'unknown': 'Evidence is insufficient'}} for i in range(count)}


class Response:
    """Serve a bounded JSON HTTP response without network access."""
    status = 200

    def __init__(self, payload):
        self.payload = payload
        self.content = self

    async def __aenter__(self):
        """Enter the fake HTTP context."""
        return self

    async def __aexit__(self, *args):
        """Close the fake HTTP context."""
        return False

    async def readexactly(self, size):
        """Simulate EOF after the complete bounded response."""
        raw = await self.read(size)
        if len(raw) < size:
            raise asyncio.IncompleteReadError(raw, size)
        return raw

    async def read(self, size):
        """Return serialized bytes within the caller's read limit."""
        return json.dumps(self.payload).encode()[:size]


def session(price='0.000000042'):
    """Capture the exact pricing and decision calls made by production code."""
    def answer(url, **kwargs):
        return Response({'answers': {key: {'type': 'choice', 'choice': 'unknown',
            'confidence': 1, 'probabilities': {'low': 0, 'high': 0, 'unknown': 1}}
            for key in kwargs['json']['questions']}})
    return Mock(get=Mock(return_value=Response({'data': {'pricing': {'prompt': price, 'completion': '0'}}})),
                post=Mock(side_effect=answer))


def test_39_assets_use_one_price_check_and_one_decision():
    """All questions and the shared report are transferred once, without slicing."""
    client = session()
    spec = {'state': 'Untrusted evidence', 'questions': questions()}
    result = asyncio.run(jev.decide_user_jev_request(client, 'test-only', jev.JEV_MODEL, spec, max_cost_usd=.01))
    client.get.assert_called_once()
    client.post.assert_called_once()
    assert client.post.call_args.kwargs['json'] == dict(model=jev.JEV_MODEL, **spec)
    assert all(f'coin_{i} —' in result for i in range(39))


@pytest.mark.parametrize('criteria', [['low', 'high'], {'low': ''}, {str(i): 'coin' for i in range(39)}])
def test_invalid_choice_shapes_still_rejected(criteria):
    """No coercion or permissive fallback weakens the typed decision boundary."""
    source = questions(1)
    source['coin_0']['criteria'] = criteria
    with pytest.raises(jev.OpenRouterDecisionError, match='described options'):
        jev.prepare_user_jev_payload(jev.JEV_MODEL, {'state': 'Evidence', 'questions': source})


def test_model_schema_documents_exact_option_shape():
    """Models see typed options and the single-request semantics."""
    schema = jev.research_jev_question_schema()
    choice = schema['additionalProperties']['anyOf'][0]
    assert 'criteria' in choice['required']
    assert choice['properties']['criteria']['type'] == 'object'
    assert 'No fixed coin-count limit' in schema['description']
    assert 'maxProperties' not in schema


def test_budget_failure_sends_no_decision():
    """Pricing failure blocks the complete request before any evidence is sent."""
    client = session(price='1')
    with pytest.raises(jev.OpenRouterDecisionError, match='budget'):
        asyncio.run(jev.decide_user_jev_request(client, 'test-only', jev.JEV_MODEL,
            {'state': 'Evidence', 'questions': questions()}, max_cost_usd=.01))
    client.get.assert_called_once()
    client.post.assert_not_called()


def test_invalid_last_question_sends_nothing():
    """All questions are validated before even the pricing lookup."""
    client = session()
    source = questions()
    source['coin_38']['criteria'] = ['low', 'high']
    with pytest.raises(jev.OpenRouterDecisionError):
        asyncio.run(jev.decide_user_jev_request(client, 'test-only', jev.JEV_MODEL,
            {'state': 'Evidence', 'questions': source}))
    client.get.assert_not_called()
    client.post.assert_not_called()


def test_combined_payload_size_limit_is_preserved():
    """Oversized data is rejected rather than silently split or truncated."""
    with pytest.raises(jev.OpenRouterDecisionError, match='token'):
        jev.prepare_user_jev_payload(jev.JEV_MODEL, {'state': 'Risk evidence ' * 20000, 'questions': questions()})


def test_150_assets_split_only_by_size_and_share_one_cost_check():
    """Large sets keep every question and evidence, with one aggregate preflight."""
    source = questions(150)
    requests = jev.prepare_research_jev_payloads('Shared full evidence', source)
    assert len(requests) == 1
    assert {k: v for request in requests for k, v in request['questions'].items()} == source
    assert all(request['state'] == 'Shared full evidence' for request in requests)
    assert all(estimate_jev_input_tokens(request) <= JEV_CONTEXT_TOKENS for request in requests)
    client = session()
    result = asyncio.run(jev.decide_research_jev_requests(client, 'test-only', requests, .01))
    client.get.assert_called_once()
    assert client.post.call_count == len(requests)
    assert all(f'coin_{i} —' in result for i in range(150))


def test_small_150_questions_fit_one_request():
    """Question count alone never causes splitting."""
    source = {f'coin_{i}': {'type': 'noul', 'instructions': f'Is asset {i} high risk?'} for i in range(150)}
    assert len(jev.prepare_research_jev_payloads('Evidence', source)) == 1


def test_split_plan_over_budget_sends_nothing():
    """New approval carries the total estimate including repeated report input."""
    source = questions(150)
    for question in source.values():
        question['instructions'] = 'Check public liquidity, concentration, and security evidence. ' * 15
        question['criteria'] = {key: 'Risk classification supported by public evidence. ' * 9 for key in question['criteria']}
    requests = jev.prepare_research_jev_payloads('Evidence', source)
    client = session()
    with pytest.raises(jev.JevBudgetExceeded) as exc:
        asyncio.run(jev.decide_research_jev_requests(client, 'test-only', requests, .000001))
    assert exc.value.estimated_cost_usd > .000001
    client.post.assert_not_called()


def test_adaptive_requests_cancel_owned_workers(monkeypatch):
    """Cancellation drains every in-flight request and leaves no background sends."""
    async def run():
        started, closed = [], []
        gate = asyncio.Event()
        async def send(session, key, request):
            started.append(request)
            if len(started) == 2:
                gate.set()
            try:
                await asyncio.Event().wait()
            finally:
                closed.append(request)
        monkeypatch.setattr(jev, '_send_structured_jev_request', send)
        source = questions(150)
        for question in source.values():
            question['instructions'] = 'Check liquidity, concentration and security evidence. ' * 18
            question['criteria'] = {key: 'Evidence supports this risk category. ' * 13 for key in question['criteria']}
        requests = jev.prepare_research_jev_payloads('Evidence', source)
        task = asyncio.create_task(jev.decide_research_jev_requests(session(), 'test-only', requests, .01))
        await asyncio.wait_for(gate.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(started) == len(closed) == 2
    asyncio.run(run())


def test_invalid_price_cannot_create_nonfinite_approval():
    """A malformed extreme provider price cannot poison a persisted cost proposal."""
    client = session(price='1e308')
    with pytest.raises(jev.OpenRouterDecisionError, match='cannot be verified'):
        asyncio.run(jev.decide_user_jev_request(client, 'test-only', jev.JEV_MODEL,
            {'state': 'Evidence', 'questions': questions()}))
    client.post.assert_not_called()


def test_cost_check_uses_tokens_instead_of_bytes():
    """A 48 KB report below the token budget must not incur the old byte-based quote."""
    client = session()
    spec = {'state': 'Public evidence about liquidity and volatility. ' * 1000,
            'questions': questions(1)}
    result = asyncio.run(jev.decide_user_jev_request(client, 'test-only', jev.JEV_MODEL, spec, max_cost_usd=.001))
    assert 'coin_0' in result
    client.post.assert_called_once()
