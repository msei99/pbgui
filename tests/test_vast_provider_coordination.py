"""Offline request coordination and rate-limit handling across client lifetimes."""
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import urllib.error
import urllib.parse

import pytest

import vast_provider as provider


def test_log_requests_keep_instance_cache(monkeypatch):
    """Reading either provider log must not force controllers to refetch instances."""
    calls = []
    def request(self, method, path, body=None):
        """Record upstream activity without network access."""
        calls.append((method, path))
        return {'instances': [{'id': 1, 'actual_status': 'running'}]} if method == 'GET' else {'success': True}
    monkeypatch.setattr(provider.VastClient, '_request', request)
    client = provider.VastClient('test-key')
    client.instances()
    for body in ({'tail': '1000'}, {'tail': '1000', 'daemon_logs': 'true'}):
        client.request('PUT', '/instances/request_logs/1/', body)
        assert client.instances()[0]['actual_status'] == 'running'
    assert len(calls) == 3


def test_valid_cache_remains_readable_during_backoff(monkeypatch):
    """Only unsent cache reads bypass cooldown; fresh or expired reads stay blocked."""
    now = [1000.0]
    monkeypatch.setattr(provider, 'time', SimpleNamespace(time=lambda: now[0]))
    calls = []
    def request(self, method, path, body=None):
        """Cache one listing, then rate-limit an independent provider operation."""
        calls.append(path)
        if path != provider.INSTANCE_LIST_PATH:
            raise provider.VastRateLimit(120, request_sent=True)
        return {'instances': [{'id': 1}]}
    monkeypatch.setattr(provider.VastClient, '_request', request)
    client = provider.VastClient('test-key')
    client.instances()
    with pytest.raises(provider.VastRateLimit):
        client.request('PUT', '/instances/request_logs/1/', {'tail': '1000'})
    assert client.instances() == [{'id': 1}]
    with pytest.raises(provider.VastRateLimit):
        client.instances(fresh=True)
    now[0] += provider.INSTANCE_TTL
    with pytest.raises(provider.VastRateLimit):
        client.instances()
    assert len(calls) == 2


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


def test_instances_follow_every_official_v1_page(monkeypatch):
    """The retired v0 list is never used and a cursor cannot hide active rentals."""
    calls = []

    def request(self, method, path, body=None):
        """Return two deterministic provider pages."""
        calls.append((method, path, dict(body or {})))
        if body and body.get('after_token') == 'next-page':
            return {'instances': [{'id': 2}], 'next_token': None}
        return {'instances': [{'id': 1}], 'next_token': 'next-page'}

    monkeypatch.setattr(provider.VastClient, '_request', request)

    assert provider.VastClient('test-key').instances(fresh=True) == [{'id': 1}, {'id': 2}]
    assert [call[1] for call in calls] == [provider.INSTANCE_LIST_PATH, provider.INSTANCE_LIST_PATH]
    assert calls[1][2]['after_token'] == 'next-page'


def test_v1_instance_query_uses_root_api_url_and_encoded_filters(monkeypatch):
    """The v1 path must not be appended below the legacy v0 API root."""
    requests = []

    class Response:
        """Return one bounded provider page."""
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _limit):
            return b'{"instances": [], "next_token": null}'

    class Opener:
        """Capture the complete request without network access."""
        def open(self, request, timeout):
            requests.append(request)
            return Response()

    monkeypatch.setattr(provider.urllib.request, 'build_opener', lambda *_: Opener())
    result = provider.VastClient('test-key')._request(
        'GET',
        provider.INSTANCE_LIST_PATH,
        {'select_filters': {}, 'order_by': [{'col': 'id', 'dir': 'asc'}], 'limit': 25},
    )

    assert result['instances'] == []
    parsed = urllib.parse.urlsplit(requests[0].full_url)
    assert parsed.path == '/api/v1/instances/'
    assert '/api/v0/' not in parsed.path
    query = urllib.parse.parse_qs(parsed.query)
    assert json.loads(query['select_filters'][0]) == {}
    assert json.loads(query['order_by'][0]) == [{'col': 'id', 'dir': 'asc'}]
    assert query['limit'] == ['25']


def test_instances_reject_repeated_v1_cursor(monkeypatch):
    """A broken provider cursor cannot turn one listing into an unbounded loop."""
    monkeypatch.setattr(
        provider.VastClient,
        '_request',
        lambda *_args, **_kwargs: {'instances': [], 'next_token': 'same-page'},
    )

    with pytest.raises(provider.VastError, match='cursor'):
        provider.VastClient('test-key').instances(fresh=True)


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
