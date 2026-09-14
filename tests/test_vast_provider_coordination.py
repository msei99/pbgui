"""Offline request coordination and rate-limit handling across client lifetimes."""
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import urllib.error

import pytest

import vast_provider as provider


def test_instances_share_cache_but_cleanup_reads_are_fresh(monkeypatch):
    """Concurrent controllers share a bounded snapshot; cleanup never reuses absence."""
    calls = []
    def request(self, method, path, body=None):
        """Return a provider record including data that must never be cached."""
        calls.append((method, path))
        return {'instances':[{'id':1,'label':'owned','actual_status':'running','env':{'secret':'must-not-cache'}}]}
    monkeypatch.setattr(provider.VastClient, '_request', request)
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(lambda _: provider.VastClient('test-key').instances(), range(8)))
    assert all(row[0]['id'] == 1 for row in rows)
    assert len(calls) == 1
    state = (provider.COORDINATION_ROOT / 'state.json').read_text()
    assert 'must-not-cache' not in state and 'test-key' not in state
    provider.VastClient('test-key').instances(fresh=True)
    provider.VastClient('test-key').instances(fresh=True)
    assert len(calls) == 3
    provider.VastClient('test-key').request('DELETE', '/instances/1')
    provider.VastClient('test-key').instances()
    assert len(calls) == 5


def test_shared_backoff_honors_retry_after_and_resets(monkeypatch):
    """429 blocks every client/endpoint until the common retry deadline."""
    now = [1000.0]
    monkeypatch.setattr(provider, 'time', SimpleNamespace(time=lambda: now[0]))
    calls = []
    failures = [True]
    def request(self, method, path, body=None):
        """Emulate a rate-limited provider and eventual recovery."""
        calls.append(path)
        if failures[0]:
            raise provider.VastRateLimit(120, request_sent=True)
        return {'instances':[]}
    monkeypatch.setattr(provider.VastClient, '_request', request)
    with pytest.raises(provider.VastRateLimit) as error:
        provider.VastClient('key').instances()
    assert error.value.retry_after == 120
    with pytest.raises(provider.VastRateLimit) as error:
        provider.VastClient('key').request('PUT', '/asks/1/')
    assert not error.value.request_sent
    assert len(calls) == 1
    now[0] += 120
    with pytest.raises(provider.VastRateLimit):
        provider.VastClient('key').instances()
    assert len(calls) == 2
    now[0] += 120
    failures[0] = False
    assert provider.VastClient('key').instances() == []
    assert json.loads((provider.COORDINATION_ROOT/'state.json').read_text())['retry_at'] == 0


def test_http_retry_after_date_and_seconds(monkeypatch):
    """HTTP 429 preserves safe retry timing without disclosing response payloads."""
    class Opener:
        """Reject requests without using the network."""
        def open(self, req, timeout):
            """Return a provider Retry-After header."""
            raise urllib.error.HTTPError(req.full_url, 429, 'secret-body', {'Retry-After':'90'}, None)
    monkeypatch.setattr(provider.urllib.request, 'build_opener', lambda *_: Opener())
    with pytest.raises(provider.VastRateLimit) as error:
        provider.VastClient('key').account()
    assert error.value.retry_after == 90
    assert 'secret-body' not in str(error.value)
    monkeypatch.setattr(provider, 'time', SimpleNamespace(time=lambda: 0))
    assert provider.retry_after_seconds('Thu, 01 Jan 1970 00:01:00 GMT') == 60
    assert provider.retry_after_seconds('invalid') == 0


@pytest.mark.parametrize('status', [400, 403, 404, 500])
def test_http_failure_preserves_status_without_payload(monkeypatch, status):
    """Rental rejection diagnostics retain HTTP status but never upstream secrets."""
    class Opener:
        """Return an isolated HTTP failure."""
        def open(self, req, timeout):
            """Fail without sending a request."""
            raise urllib.error.HTTPError(req.full_url, status, 'secret-body', {}, None)
    monkeypatch.setattr(provider.urllib.request, 'build_opener', lambda *_: Opener())
    with pytest.raises(provider.VastError) as error:
        provider.VastClient('secret-key').request('PUT', '/asks/1/', {})
    assert f'HTTP {status}' in str(error.value)
    assert 'secret' not in str(error.value)
