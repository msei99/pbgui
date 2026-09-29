"""Offline token estimates, vocabulary integrity and model-context packing regressions."""
import asyncio
from unittest.mock import Mock

import pytest

import ai_openrouter as jev
import ai_token_budget as budget


@pytest.mark.parametrize('report', ['Public evidence about liquidity and volatility. ' * 1000,
                                  '风险 😀 ' * 3000])
def test_large_byte_reports_fit_by_token_estimate(report):
    """UTF-8 payload size alone no longer forces splits or rejection."""
    assert len(report.encode()) > 30000
    questions = {'risk': {'type': 'noul', 'instructions': 'Is this asset high risk?'}}
    requests = jev.prepare_research_jev_payloads(report, questions)
    assert len(requests) == 1
    assert budget.estimate_jev_input_tokens(requests[0]) <= budget.JEV_CONTEXT_TOKENS
    assert requests[0]['state'] == report


def test_proxy_is_local_even_on_first_load(monkeypatch):
    """No network or tiktoken download cache is used to count public or private input."""
    import socket
    import tiktoken
    budget._proxy_encoding.cache_clear()
    monkeypatch.setattr(socket, 'create_connection', Mock(side_effect=AssertionError('No network')))
    monkeypatch.setattr(tiktoken, 'get_encoding', Mock(side_effect=AssertionError('No download cache')))
    assert budget.estimate_jev_input_tokens({'state': 'Test', 'questions': {}}) > 512


def test_vocabulary_integrity_failure_is_not_a_byte_fallback(monkeypatch):
    """Corrupt resources stop analysis with a safe error instead of guessed byte counts."""
    monkeypatch.setattr(jev, 'estimate_jev_input_tokens', Mock(side_effect=ValueError('Corrupt vocabulary')))
    with pytest.raises(jev.OpenRouterDecisionError, match='estimator unavailable'):
        jev.prepare_user_jev_payload(jev.JEV_MODEL, {'state': 'Test', 'questions': {
            'risk': {'type': 'noul', 'instructions': 'Is this high risk?'}}})


def test_context_boundary_uses_estimated_tokens(monkeypatch):
    """The same token boundary applies to full preparation and adaptive packing."""
    question = {'risk': {'type': 'noul', 'instructions': 'Is this high risk?'}}
    monkeypatch.setattr(jev, 'estimate_jev_input_tokens', lambda request: budget.JEV_CONTEXT_TOKENS)
    assert len(jev.prepare_research_jev_payloads('Test', question)) == 1
    monkeypatch.setattr(jev, 'estimate_jev_input_tokens', lambda request: budget.JEV_CONTEXT_TOKENS + 1)
    with pytest.raises(jev.OpenRouterDecisionError, match='token budget'):
        jev.prepare_research_jev_payloads('Test', question)


def test_150_detailed_questions_pack_without_loss():
    """A genuinely token-heavy plan splits with complete evidence and question coverage."""
    source = {f'coin_{i}': {'type': 'choice', 'instructions': (f'Classify coin {i}. ' + 'Use public risk evidence. ' * 30).strip(),
              'criteria': {key: ('Risk category supported by public evidence. ' * 10).strip()
                           for key in ('low', 'high', 'unknown')}} for i in range(150)}
    requests = jev.prepare_research_jev_payloads('Shared evidence', source)
    assert len(requests) > 1
    assert {key: value for request in requests for key, value in request['questions'].items()} == source
    assert all(request['state'] == 'Shared evidence' for request in requests)
    assert all(budget.estimate_jev_input_tokens(request) <= budget.JEV_CONTEXT_TOKENS for request in requests)


def test_bundled_vocabulary_hash_is_enforced(monkeypatch):
    """The encoding cannot be built from an unverified vocabulary file."""
    budget._proxy_encoding.cache_clear()
    monkeypatch.setattr(budget, '_PROXY_HASH', '0' * 64)
    with pytest.raises(ValueError, match='integrity'):
        budget.estimate_jev_input_tokens({'state': 'Test'})
    budget._proxy_encoding.cache_clear()
