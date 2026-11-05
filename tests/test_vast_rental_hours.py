"""Finite rental authorizations constrained by the active published worker image."""
import pytest
from pydantic import ValidationError

from api.vast import RentalPreferences


@pytest.mark.parametrize('hours', [120, 720, 8760])
def test_single_job_authorization_accepts_long_finite_windows(hours):
    """Paid job approvals preserve long hours instead of reintroducing an API-only cap."""
    import vast_queue
    convergence = {'fixture': True}
    value = dict(id='a' * 32, job_id='b' * 32, hours=hours, budget=10,
                 convergence=convergence, settings={}, credential_generation=0)
    from unittest.mock import patch
    with patch('optimize_convergence.validate_config', return_value=convergence):
        assert vast_queue.validated_job_authorization(value)['hours'] == hours


@pytest.mark.parametrize('hours', [float('inf'), float('nan'), -1, .1, '120', True])
def test_single_job_authorization_rejects_unsafe_hours(hours):
    """Removing the ceiling keeps finite numeric consent and the minimum duration."""
    from vast_provider import VastError
    from vast_queue import validated_job_authorization
    value = dict(id='a' * 32, job_id='b' * 32, hours=hours, budget=10,
                 convergence={}, settings={}, credential_generation=0)
    with pytest.raises(VastError):
        validated_job_authorization(value)


@pytest.mark.parametrize('feature_supported', [False, True])
def test_maximum_deadline_requires_verified_image_capability(monkeypatch, feature_supported):
    """Old images retain their ceiling; verified new images use the budget boundary."""
    from vast_deadline import maximum_deadline
    monkeypatch.setattr('vast_worker_capabilities.supports', lambda *args: feature_supported)
    intent = dict(accepted_at=1000, deadline=2000, budget_usd=10, bundle_bytes=0,
                  image='candidate', offer=dict(download_gb_usd=0, upload_gb_usd=0, price_hour_usd=.01))
    deadline = maximum_deadline(intent)
    assert deadline == 1000 + ((10 - .05) / .01 * 3600 if feature_supported else 86400)


@pytest.mark.parametrize('feature_supported', [False, True])
def test_persisted_multi_day_intent_requires_verified_image(monkeypatch, feature_supported):
    """A source-only worker update must not authorize long rentals on the old image."""
    from vast_jobs import IMAGE, REVISION
    from vast_job_runner import validate_intent
    from vast_provider import VastError
    monkeypatch.setattr('vast_worker_capabilities.supports', lambda *args: feature_supported)
    identifier = 'a' * 32
    intent = dict(id=identifier, image=IMAGE, pb8_revision=REVISION,
                  label='pbgui-vast-' + identifier, accepted_at=1000,
                  deadline=1000 + 120 * 3600, budget_usd=10, offer={'id': 1})
    if feature_supported:
        assert validate_intent(intent, identifier) == intent
    else:
        with pytest.raises(VastError):
            validate_intent(intent, identifier)


def test_candidate_worker_protocol_offline():
    """Exercise the exact candidate source's independent guard and delta snapshot."""
    from pathlib import Path
    from setup.vast_gpu_benchmark.verify_rental_protocol import verify
    verify(Path(__file__).resolve().parents[1] / 'setup/vast_gpu_benchmark/cloud_worker.py')


def test_rental_form_has_no_hardcoded_duration_ceiling():
    """The server capability sets native validity instead of a static legacy cap."""
    from html.parser import HTMLParser
    from pathlib import Path
    class Inputs(HTMLParser):
        """Collect only the rental hours input attributes."""
        hours = None
        def handle_starttag(self, tag, attrs):
            """Record the real input so unrelated max attributes do not affect this check."""
            fields = dict(attrs)
            if tag == 'input' and fields.get('id') == 'job-hours':
                self.hours = fields
    parser = Inputs()
    parser.feed((Path(__file__).resolve().parents[1] / 'frontend/vast.html').read_text())
    assert parser.hours is not None and 'max' not in parser.hours


@pytest.mark.parametrize('value', ['inf', 'nan', '-inf'])
def test_offers_query_rejects_non_finite_rental_duration(value):
    """Query parameters cannot bypass the finite guard applied to request bodies."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import api.vast as vast_api
    app = FastAPI()
    app.include_router(vast_api.router)
    app.dependency_overrides[vast_api.require_auth] = lambda: object()
    with TestClient(app) as client:
        assert client.get('/offers', params={'rental_hours': value}).status_code == 422


def test_multi_day_windows_are_accepted(monkeypatch):
    """Finite multi-day windows require a verified published image."""
    monkeypatch.setattr('vast_worker_capabilities.supports', lambda *args: True)
    assert RentalPreferences(hours=120).hours == 120
    assert RentalPreferences(hours=720).hours == 720


def test_very_long_windows_are_accepted(monkeypatch):
    """A finite year-long window is not rejected by an arbitrary duration cap."""
    monkeypatch.setattr('vast_worker_capabilities.supports', lambda *args: True)
    assert RentalPreferences(hours=8760).hours == 8760


def test_non_finite_hours_are_rejected():
    """Infinite durations cannot enter saved rental preferences."""
    with pytest.raises(ValidationError):
        RentalPreferences(hours=float("inf"))


def test_rental_preferences_reject_too_short_window():
    """The minimum rental window remains fifteen minutes."""
    with pytest.raises(ValidationError):
        RentalPreferences(hours=0.1)


@pytest.mark.parametrize('model', ['RentalPreferences', 'StartJobRequest', 'CalibrationStartRequest',
                                  'ConfigurableCalibrationRequest', 'CalibrationWatchRequest'])
@pytest.mark.parametrize('supported', [False, True])
def test_api_duration_rejected_before_any_provider_action(monkeypatch, model, supported):
    """Every rental or watch request checks the active image before side effects."""
    from api import vast
    from vast_jobs import IMAGE
    from vast_worker_capabilities import PUBLISHED_WORKER_CAPABILITIES
    monkeypatch.setitem(PUBLISHED_WORKER_CAPABILITIES, IMAGE,
                        frozenset({'multi_day'}) if supported else frozenset())
    payload = dict(hours=120, budget=30)
    if model == 'CalibrationWatchRequest':
        payload['preferences'] = dict(gpu_name='RTX 3090')
    if model in {'CalibrationStartRequest', 'ConfigurableCalibrationRequest'}:
        payload['offer'] = dict(id=1, machine_id=2, gpu_name='RTX 3090',
                                vram_gb=24, price_hour_usd=.1)
    if supported:
        assert getattr(vast, model).model_validate(payload).hours == 120
    else:
        with pytest.raises(ValidationError, match='active worker image supports at most 24'):
            getattr(vast, model).model_validate(payload)


def test_stored_long_preferences_remain_readable_but_require_correction(tmp_path, monkeypatch):
    """Former long settings can be displayed and fixed, but never reauthorized."""
    from api import vast
    from fastapi import Response
    from vast_jobs import JobStore
    from vast_queue import CloudQueue
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    queue.update(gpu_preferences=dict(hours=120))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr('vast_worker_capabilities.supports', lambda *args: False)
    assert vast.gpu_preferences(Response(), object())['hours'] == 120
    with pytest.raises(ValidationError):
        RentalPreferences.model_validate(queue.read()['gpu_preferences'])


@pytest.mark.parametrize('supported, maximum', [(False, 24), (True, None)])
def test_settings_expose_image_duration_capability(monkeypatch, supported, maximum):
    """The browser receives the same current image bound that API validation uses."""
    from api import vast
    from fastapi import Response
    from types import SimpleNamespace
    monkeypatch.setattr('vast_worker_capabilities.supports', lambda *args: supported)
    monkeypatch.setattr(vast, 'VastCredentialStore', lambda: SimpleNamespace(metadata=lambda: {}))
    assert vast.settings(Response(), object())['maximum_rental_hours'] == maximum
