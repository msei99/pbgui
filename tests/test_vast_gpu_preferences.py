"""Offline GPU requirement persistence and live-at-start offer selection contracts."""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Response
from pydantic import ValidationError

from api import vast
from vast_jobs import JobStore
from vast_queue import CloudQueue


@pytest.fixture
def rental(monkeypatch):
    """Substitute the marketplace and rental boundary; never create an instance."""
    calls = []
    rows = []
    queue = SimpleNamespace(worker=lambda: None, waiting=lambda: [{'workers':8}],
                            read=lambda: {}, store=SimpleNamespace(read=lambda *args: {}),
                            start=lambda *args: calls.append(('start', args)) or args[0])
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(vast, 'VastCredentialStore', lambda: SimpleNamespace(secrets=lambda:{'api_key':'fake'}))
    def offers(**kwargs):
        """Record current search criteria and return the mock live marketplace."""
        calls.append(('search', kwargs))
        return rows
    monkeypatch.setattr(vast, 'VastClient', lambda key: SimpleNamespace(offers=offers))
    return queue, rows, calls


def offer(identifier=2, **changes):
    """Produce one available matching offer with explicit hardware capabilities."""
    return {'id':identifier,'gpu_name':'RTX 3090','num_gpus':1,'cuda_max_good':13,
            'price_hour_usd':.2,'vram_gb':24,'ram_gb':32,'cpu_cores':8,
            'disk_gb':40,'verified':True, **changes}


def request():
    """Authorize only the chosen type, limits and automatic cleanup."""
    return vast.StartJobRequest(preferences=vast.GpuPreferences(gpu_name='RTX 3090'),
                                accept_rental_and_cleanup=True)


def test_preferences_persist_without_offer_id(tmp_path, monkeypatch):
    """Requirements survive reload without mutating unrelated queue controls."""
    queue = CloudQueue(JobStore(tmp_path/'vast'))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    queue.update(paused=True)
    saved = vast.save_gpu_preferences(vast.RentalPreferences(gpu_name=' RTX 3090 '), session=None)
    assert saved['gpu_name'] == 'RTX 3090'
    assert vast.gpu_preferences(Response(), session=None) == saved
    assert queue.read()['paused'] is True
    assert 'offer_id' not in saved


def test_start_uses_current_matching_offer_not_preview(rental):
    """A replacement host of the same type is chosen from fresh offers at start."""
    _, rows, calls = rental
    rows.extend([offer(9, price_hour_usd=.3), offer(42)])
    assert vast.start_queue(request(), session=None)['id'] == 42
    assert calls[0][0] == 'search'
    assert calls[0][1]['gpu_name'] == 'RTX 3090'
    assert calls[0][1]['min_cpu'] == 8
    assert 'offer_id' not in calls[0][1]
    assert calls[1][0] == 'start'


@pytest.mark.parametrize('changes', [
    {'gpu_name':'RTX 4090'}, {'price_hour_usd':.6}, {'cpu_cores':4},
    {'vram_gb':8}, {'ram_gb':8}, {'disk_gb':20}, {'verified':False},
    {'cuda_max_good':12.9}, {'num_gpus':2},
])
def test_provider_filter_mismatch_cannot_rent(rental, changes):
    """Never silently broaden hardware or spending requirements."""
    _, rows, calls = rental
    rows.append(offer(**changes))
    with pytest.raises(HTTPException) as error:
        vast.start_queue(request(), session=None)
    assert error.value.status_code == 409
    assert [kind for kind, _ in calls] == ['search']


def test_no_current_offer_leaves_queue_waiting(rental):
    """Unavailable inventory never starts a rental."""
    _, _, calls = rental
    with pytest.raises(HTTPException, match='No available GPU'):
        vast.start_queue(request(), session=None)
    assert len(calls) == 1


def test_active_worker_reused_without_marketplace_search(rental):
    """Another start on an active rental returns it without looking for a GPU."""
    queue, _, calls = rental
    queue.worker = lambda: {'id':'existing', 'rental_state':'running'}
    assert vast.start_queue(request(), session=None)['id'] == 'existing'
    assert calls == []


def test_no_jobs_does_not_search(rental):
    """An empty queue cannot authorize a new rental."""
    queue, _, calls = rental
    queue.waiting = lambda: []
    with pytest.raises(HTTPException):
        vast.start_queue(request(), session=None)
    assert calls == []


def test_old_offer_request_rejected():
    """Stale browser requests must reload instead of binding an old offer."""
    with pytest.raises(ValidationError):
        vast.StartJobRequest(offer_id=42, max_price=.2)


@pytest.mark.parametrize('search', ['3090', 'rtx 3090', 'RTX_3090', 'rTx-3090'])
def test_partial_model_search_filters_before_marketplace_limit(monkeypatch, search):
    """Resolve fragments against the model catalog before searching bounded offers."""
    from vast_provider import VastClient
    queries = []
    def provider(self, method, path, body=None):
        """Supply canonical GPU names and isolated hardware metadata."""
        if path == '/gpu_names/unique/':
            return {'gpu_names':['RTX 3060', 'RTX 3090', 'RTX 4090']}
        queries.append(body)
        return {'offers':[{'id':42, 'gpu_name':'RTX 3090', 'dph_total':.2,
                          'cpu_name':'Xeon test','gpu_mem_bw':805.4, 'pci_gen':3,
                          'gpu_lanes':8, 'pcie_bw':5.5,'disk_name':'NVMe test',
                          'disk_bw':1810,'duration':86400,'cuda_max_good':13}]}
    monkeypatch.setattr(VastClient, 'request', provider)
    rows = VastClient('fake').offers(gpu_name=search, min_cuda=13, min_duration=3600)
    assert queries[0]['gpu_name'] == {'in':['RTX 3090']}
    assert queries[0]['cuda_max_good'] == {'gte':13}
    assert queries[0]['duration'] == {'gte':3600}
    assert rows[0]['cpu_name'] == 'Xeon test'
    assert rows[0]['gpu_mem_bw_gbps'] == 805.4
    assert rows[0]['disk_bw_mbps'] == 1810
    assert rows[0]['pcie_bw_gbps'] == 5.5
    assert rows[0]['duration_seconds'] == 86400


def test_compatibility_filter_and_unknown_metadata(monkeypatch):
    """Preview hides old CUDA/short leases but can show them explicitly."""
    from vast_provider import VastClient
    payload = {'offers':[
        {'id':1,'dph_total':.1,'cuda_max_good':12.6,'duration':90000},
        {'id':2,'dph_total':.1,'cuda_max_good':13,'duration':600},
        {'id':3,'dph_total':.1,'cuda_max_good':13,'duration':90000,'disk_bw':'nan'},
        {'id':4,'dph_total':.1},
    ]}
    monkeypatch.setattr(VastClient, 'request', lambda *args: payload)
    client = VastClient('fake')
    rows = client.offers(min_cuda=13, min_duration=3600)
    assert [row['id'] for row in rows] == [3]
    assert rows[0]['disk_bw_mbps'] is None
    assert rows[0]['cpu_name'] == ''
    assert len(client.offers()) == 4


def test_partial_model_is_valid_at_rental_start(rental):
    """Stored short model requirements accept the canonical matching offer."""
    _, rows, _ = rental
    rows.append(offer())
    body = request()
    body.preferences.gpu_name = '3090'
    assert vast.start_queue(body, session=None)['id'] == 2


def test_insufficient_duration_cannot_rent(rental):
    """Recheck a known short availability even if a provider ignores filters."""
    _, rows, calls = rental
    rows.append(offer(duration_seconds=600))
    with pytest.raises(HTTPException):
        vast.start_queue(request(), session=None)
    assert [kind for kind, _ in calls] == ['search']


def test_full_model_name_does_not_broaden_to_other_variants(monkeypatch):
    """An exact canonical type must take precedence over a partial variant match."""
    from vast_provider import VastClient
    queries = []
    def provider(self, method, path, body=None):
        """Provide similar model variants without any real provider access."""
        if path == '/gpu_names/unique/':
            return {'gpu_names':['RTX 3090', 'RTX 3090 Ti']}
        queries.append(body)
        return {'offers':[]}
    monkeypatch.setattr(VastClient, 'request', provider)
    VastClient('fake').offers(gpu_name='rtx_3090')
    assert queries[0]['gpu_name'] == {'in':['RTX 3090']}


def test_start_rejects_cleanup_pending_worker(rental):
    """A stale lease under cleanup must not masquerade as a successful start."""
    queue, _, calls = rental
    queue.worker = lambda: {'id':'old', 'rental_state':'creation_pending'}
    queue.store = SimpleNamespace(read=lambda *args: {'cleanup':True})
    with pytest.raises(HTTPException) as error:
        vast.start_queue(request(), session=None)
    assert error.value.status_code == 409
    assert 'No new job has started' in error.value.detail
    assert calls == []


@pytest.mark.parametrize('field,value', [('convergence_patience', 0), ('convergence_min_exact', 1),
                                        ('convergence_tolerance_pct', float('nan')),
                                        ('convergence_tolerance_pct', 0)])
def test_invalid_convergence_settings_rejected(field, value):
    """No zero windows or nonfinite tolerances can enable accidental stopping."""
    with pytest.raises(ValidationError):
        vast.RentalPreferences(**{field: value})


def test_manual_rent_selects_exact_offer_without_jobs(rental):
    """Rent immediately binds the selected offer instead of the cheapest alternative."""
    queue, rows, calls = rental
    queue.waiting = lambda: []
    queue.start = lambda *args, **kwargs: calls.append(('manual', (args, kwargs))) or args[0]
    rows.extend([offer(9, price_hour_usd=.1), offer(42)])
    body = request().model_copy(update={'rent_only': True, 'offer_id': 42})
    assert vast.start_queue(body, session=None)['id'] == 42
    assert calls[-1][1][1] == {'manual': True}
    assert calls[0][1]['offer_id'] == 42


def test_manual_rent_never_substitutes_unavailable_offer(rental):
    """A disappeared manual selection cannot silently rent another host."""
    _, rows, calls = rental
    rows.append(offer(9))
    with pytest.raises(HTTPException) as error:
        vast.start_queue(request().model_copy(update={'rent_only': True, 'offer_id': 42}), session=None)
    assert error.value.status_code == 409
    assert 'no replacement' in error.value.detail
    assert [item[0] for item in calls] == ['search']


def test_manual_rent_retry_does_not_resume_queue(rental):
    """Repeated Rent reuses the existing lease without dispatching waiting jobs."""
    queue, _, calls = rental
    current = {'id': 'existing', 'rental_state': 'active'}
    queue.worker = lambda: current
    queue.read = lambda: {'paused': True}
    queue.action = lambda action: calls.append(action)
    assert vast.start_queue(request().model_copy(update={'rent_only': True, 'offer_id': 42}), session=None) == current
    assert calls == []


def test_queue_start_reuses_manual_rental_without_saved_preferences(rental):
    """A reserved GPU needs no fresh search or saved filter to start the queue."""
    queue, _, calls = rental
    current = {'id': 'existing', 'rental_state': 'active'}
    queue.worker = lambda: current
    queue.read = lambda: {'paused': True}
    queue.action = lambda action: calls.append(action)
    assert vast.start_queue(vast.StartJobRequest(use_saved_settings=True, accept_rental_and_cleanup=True), session=None) == current
    assert calls == ['resume']
