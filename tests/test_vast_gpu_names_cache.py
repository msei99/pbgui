"""Offline catalog-cache regressions for GPU searches across Vast client lifetimes."""

from concurrent.futures import ThreadPoolExecutor
import threading
from unittest.mock import Mock

import pytest

import vast_provider as provider
from vast_hosts import search_host_offers


def test_catalog_cache_expiry_refresh_and_copy(monkeypatch):
    """Clients share validated names until TTL expiry and cannot mutate cached data."""
    clock = [100.0]
    monkeypatch.setattr(provider.time, 'monotonic', lambda: clock[0])
    request = Mock(side_effect=[{'gpu_names': ['RTX 3090']}, {'gpu_names': ['RTX 4090']}, {'gpu_names': []}])
    monkeypatch.setattr(provider.VastClient, 'request', request)
    first = provider.VastClient('first-key')
    second = provider.VastClient('second-key')
    names = first.gpu_names()
    names.append('changed by caller')
    clock[0] += provider.GPU_NAMES_TTL - 1
    assert second.gpu_names() == ['RTX 3090']
    assert request.call_count == 1
    clock[0] += 1
    assert second.gpu_names() == ['RTX 4090']
    assert request.call_count == 2
    assert first.gpu_names(fresh=True) == []
    assert second.gpu_names() == []
    assert request.call_count == 3
    assert set(provider._GPU_NAMES_CACHE) == {'names', 'timestamp'}


@pytest.mark.parametrize('names', [None, 'RTX 4090', [123], ['RTX 4090'] * 2001],
                         ids=['missing', 'not-list', 'not-string', 'too-many'])
def test_invalid_catalog_is_not_cached(monkeypatch, names):
    """Malformed or oversized responses cannot poison later searches."""
    request = Mock(side_effect=[{'gpu_names': names}, {'gpu_names': ['RTX 4090']}])
    monkeypatch.setattr(provider.VastClient, 'request', request)
    client = provider.VastClient('test-key')
    with pytest.raises(provider.VastError, match='invalid GPU model list'):
        client.gpu_names()
    assert provider._GPU_NAMES_CACHE == {}
    assert client.gpu_names() == ['RTX 4090']
    assert request.call_count == 2


@pytest.mark.parametrize('failure', [provider.VastError('offline'), provider.VastRateLimit(30, request_sent=True)],
                         ids=['network-error', 'rate-limit'])
def test_provider_error_is_not_cached(monkeypatch, failure):
    """Network and rate-limit errors retain provider semantics and allow later retry."""
    request = Mock(side_effect=[failure, {'gpu_names': ['RTX 4090']}])
    monkeypatch.setattr(provider.VastClient, 'request', request)
    client = provider.VastClient('test-key')
    with pytest.raises(type(failure)) as error:
        client.gpu_names()
    assert error.value is failure
    assert provider._GPU_NAMES_CACHE == {}
    assert client.gpu_names() == ['RTX 4090']


def test_concurrent_clients_fetch_catalog_once(monkeypatch):
    """A cold catalog request is shared by simultaneous offer-search callers."""
    request = Mock(return_value={'gpu_names': ['RTX 4090']})
    monkeypatch.setattr(provider.VastClient, 'request', request)
    barrier = threading.Barrier(6)

    def fetch(_):
        """Align client calls without using any real provider connection."""
        barrier.wait(timeout=5)
        return provider.VastClient('test-key').gpu_names()

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(fetch, range(6)))
    assert all(names == ['RTX 4090'] for names in results)
    assert request.call_count == 1


def test_preferred_host_search_reuses_catalog_but_searches_current_offers(monkeypatch):
    """The normal and preferred-host searches each query offers, sharing only names."""
    calls = []
    phase = [0]

    def request(self, method, path, body=None):
        """Supply current offer prices and a fixed catalog without network access."""
        calls.append((method, path, body))
        if path == '/gpu_names/unique/':
            return {'gpu_names': ['RTX 4090', 'RTX 3090']}
        machine = 7 if body.get('machine_id') else 8
        return {'offers': [{'id': machine, 'machine_id': machine, 'gpu_name': 'RTX 4090',
                            'dph_total': .2 + phase[0] * .1}]}

    monkeypatch.setattr(provider.VastClient, 'request', request)
    state = {'host_preferences': {'7': {'preferred': True}}}
    rows = search_host_offers(provider.VastClient('test-key'), state, gpu_name='4090')
    assert [row['machine_id'] for row in rows] == [7, 8]
    assert [call[:2] for call in calls] == [('GET', '/gpu_names/unique/'), ('POST', '/bundles/'), ('POST', '/bundles/')]
    phase[0] = 1
    calls.clear()
    rows = search_host_offers(provider.VastClient('test-key'), state, gpu_name='RTX_4090')
    assert [row['machine_id'] for row in rows] == [7, 8]
    assert all(row['price_hour_usd'] == pytest.approx(.3) for row in rows)
    assert [call[:2] for call in calls] == [('POST', '/bundles/'), ('POST', '/bundles/')]
    assert all(call[2]['gpu_name'] == {'in': ['RTX 4090']} for call in calls)


def test_blank_or_unknown_model_preserves_query_behavior(monkeypatch):
    """Blank filters skip the catalog and unknown filters do not search all offers."""
    request = Mock(side_effect=[{'offers': []}, {'gpu_names': ['RTX 4090']}])
    monkeypatch.setattr(provider.VastClient, 'request', request)
    client = provider.VastClient('test-key')
    assert client.offers(gpu_name=' ') == []
    assert request.call_args.args[:2] == ('POST', '/bundles/')
    assert client.offers(gpu_name='unknown model') == []
    assert request.call_count == 2
