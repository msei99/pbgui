"""Offline image upgrades preserve old rental ownership and strict image pins."""
import pytest
from vast_jobs import IMAGE, REVISION, SUPPORTED_RENTAL_IMAGES
from vast_job_runner import validate_intent, rental_payload, guard_step
from vast_provider import VastError
from tests.test_vast_jobs import job, Provider


@pytest.mark.parametrize('image', SUPPORTED_RENTAL_IMAGES)
def test_known_image_keeps_intent_and_creation_image(job, image):
    """Recovery and creation retain the exact previously authorized image."""
    store, identifier, intent = job
    intent['image'] = image
    assert validate_intent(intent, identifier) is intent
    assert rental_payload(intent)['image'] == image


@pytest.mark.parametrize('image', ['untrusted/image', IMAGE.split('@')[0] + ':latest', None, []])
def test_unknown_images_are_rejected(job, image):
    """Neither persisted nor mutable registry identities bypass the allowlist."""
    _, identifier, intent = job
    intent['image'] = image
    with pytest.raises(VastError):
        validate_intent(intent, identifier)
    with pytest.raises(VastError):
        rental_payload(intent)


def test_old_rental_can_still_be_cleaned_up(job):
    """A wrapper release must not strand the old instance's independent guard."""
    store, identifier, intent = job
    intent['image'] = SUPPORTED_RENTAL_IMAGES[1]
    validate_intent(intent, identifier)
    store.control(identifier, 'cleanup')
    provider = Provider([{'id': 123, 'label': intent['label']}])
    guard_step(store, identifier, provider, intent, '', now=1100)
    assert ('DELETE', '/instances/123') in provider.calls
    assert not any(method == 'PUT' for method, _ in provider.calls)
