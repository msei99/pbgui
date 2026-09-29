"""Offline image upgrades preserve old rental ownership and strict image pins."""
from pathlib import Path
import pytest
from vast_jobs import IMAGE, REVISION, SUPPORTED_RENTAL_IMAGES, SUPPORTED_RENTAL_IMAGE_REVISIONS
from vast_job_runner import validate_intent, rental_payload, guard_step
from vast_provider import VastError
from tests.test_vast_jobs import job, Provider


def test_published_worker_build_uses_pinned_official_pb8():
    """The published worker build must not inherit an obsolete PB8 image."""
    root = Path(__file__).resolve().parents[1] / 'setup' / 'vast_gpu_benchmark'
    revision = '7b639e1180fa6bfe02e110429d4f931933c73089'
    base_tag = 'pbgui-pb8-worker:upstream-7b639e1-base'
    base = (root / 'Dockerfile').read_text()
    assert 'git clone https://github.com/enarjord/passivbot.git' in base
    assert f'ARG PB8_REVISION={revision}' in base
    assert 'git rev-parse HEAD > /opt/pb8-revision' in base
    assert 'rsync' in base
    wrapper = (root / 'Dockerfile.calibration').read_text()
    assert f'FROM {base_tag}' in wrapper
    assert f'test "$(cat /opt/pb8-revision)" = "{revision}"' in wrapper
    assert 'COPY cloud_worker.py /opt/pbgui/worker.py' in wrapper
    assert not (root / 'Dockerfile.worker').exists()


def test_creation_and_validation_use_same_image():
    """The config preflight must accept the image selected for new rentals."""
    from vast_config_validation import PROFILE_IMAGE
    assert IMAGE == PROFILE_IMAGE


def test_current_image_has_offline_layer_sizes():
    """The active immutable image keeps byte-weighted pull progress available."""
    from vast_image_layers import IMAGE_LAYERS
    assert IMAGE in IMAGE_LAYERS
    assert len(IMAGE_LAYERS[IMAGE]) == 26


def test_new_calibration_pin_keeps_previous_worker_image_known():
    """Calibration selects the new image while recognizing old image records."""
    from vast_calibration import CALIBRATION_WORKER_DIGEST, COMPATIBLE_CALIBRATION_IMAGES, calibration_worker_available
    old_image = "ghcr.io/msei99/pbgui-pb8-worker@sha256:d715bf7596215f9664ab3c439b463f7cc265753775a226f23e308db38ed0676d"
    assert IMAGE.endswith(CALIBRATION_WORKER_DIGEST)
    assert calibration_worker_available(IMAGE)
    assert IMAGE in COMPATIBLE_CALIBRATION_IMAGES
    assert old_image in SUPPORTED_RENTAL_IMAGE_REVISIONS
    assert old_image in COMPATIBLE_CALIBRATION_IMAGES


@pytest.mark.parametrize('digest', [
    'd715bf7596215f9664ab3c439b463f7cc265753775a226f23e308db38ed0676d',
    'a1a458b296653e438d2dac5a1cbfd4fa990045ce28cd4a2f70684fc2a404a0bf',
    '09cb0f9ba004db44f3ca02a7b3b03ea3211fd9e6c3c9ea33794cd4c6d24dee30',
])
def test_old_worker_images_keep_their_original_pb8_revision(digest):
    """Published pre-merge images remain bound to their own PB8 commit."""
    image = 'ghcr.io/msei99/pbgui-pb8-worker@sha256:' + digest
    assert SUPPORTED_RENTAL_IMAGE_REVISIONS[image] == '903ed11153ce82d1b6760604eaa3a553309a752a'


@pytest.mark.parametrize('image', SUPPORTED_RENTAL_IMAGES)
def test_known_image_keeps_intent_and_creation_image(job, image):
    """Recovery and creation retain the exact previously authorized image."""
    store, identifier, intent = job
    intent['image'] = image
    intent['pb8_revision'] = SUPPORTED_RENTAL_IMAGE_REVISIONS[image]
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
    intent['pb8_revision'] = SUPPORTED_RENTAL_IMAGE_REVISIONS[intent['image']]
    validate_intent(intent, identifier)
    store.control(identifier, 'cleanup')
    provider = Provider([{'id': 123, 'label': intent['label']}])
    guard_step(store, identifier, provider, intent, '', now=1100)
    assert ('DELETE', '/instances/123') in provider.calls
    assert not any(method == 'PUT' for method, _ in provider.calls)

def test_known_image_with_wrong_revision_is_rejected(job):
    """A valid image cannot authorize a different PB8 revision."""
    _, identifier, intent = job
    intent['image'] = next(
        image for image, revision in SUPPORTED_RENTAL_IMAGE_REVISIONS.items()
        if revision != REVISION
    )
    assert intent['pb8_revision'] == REVISION
    with pytest.raises(VastError):
        validate_intent(intent, identifier)
