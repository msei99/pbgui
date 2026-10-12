"""Offline image upgrades preserve old rental ownership and strict image pins."""
from pathlib import Path
import pytest
from vast_jobs import IMAGE, REVISION, SUPPORTED_RENTAL_IMAGES, SUPPORTED_RENTAL_IMAGE_REVISIONS
from vast_job_runner import validate_intent, rental_payload, guard_step
from vast_provider import VastError
from tests.test_vast_jobs import job, Provider


def test_published_worker_build_uses_pinned_official_pb8():
    """The candidate pins official PB8 including the merged fix without overlays."""
    root = Path(__file__).resolve().parents[1] / 'setup' / 'vast_gpu_benchmark'
    revision = 'b9d6692ea41266ecb4ab81d40bb5e5baef29b50a'
    base_tag = 'pbgui-pb8-worker:upstream-b9d6692-base'
    base = (root / 'Dockerfile').read_text()
    assert 'git clone https://github.com/enarjord/passivbot.git' in base
    assert f'ARG PB8_REVISION={revision}' in base
    assert 'git rev-parse HEAD > /opt/pb8-revision' in base
    assert 'git apply' not in base
    assert 'gpu-suite-invalid-metrics.patch' not in base
    assert 'rsync' in base
    wrapper = (root / 'Dockerfile.calibration').read_text()
    assert f'FROM {base_tag}' in wrapper
    assert f'test "$(cat /opt/pb8-revision)" = "{revision}"' in wrapper
    assert 'COPY cloud_worker.py /opt/pbgui/worker.py' in wrapper
    assert not (root / 'Dockerfile.worker').exists()


def test_creation_and_validation_use_same_image():
    """The config preflight must accept the image selected for new rentals."""
    from vast_config_validation import PROFILE_IMAGE, PROFILE_REVISION
    assert IMAGE == PROFILE_IMAGE
    assert REVISION == PROFILE_REVISION


def test_current_image_has_offline_layer_sizes():
    """The active immutable image keeps byte-weighted pull progress available."""
    from vast_image_layers import IMAGE_LAYERS
    assert IMAGE in IMAGE_LAYERS
    import json
    receipt = json.loads((Path(__file__).resolve().parents[1] / 'setup/vast_gpu_benchmark/worker_release_hsl_cpu_rolling_b9d6692.json').read_text())
    assert IMAGE == 'ghcr.io/msei99/pbgui-pb8-worker@' + receipt['digest']
    assert IMAGE_LAYERS[IMAGE] == {key.split(':')[1][:12]: size for key, size in receipt['layers'].items()}


def test_cpu_hsl_overlay_preserves_verified_gpu_base_and_dependency_lock():
    """The CPU overlay retains GPU fixes and reproduces the tested Rust sources."""
    import hashlib
    import json
    root = Path(__file__).resolve().parents[1] / 'setup/vast_gpu_benchmark'
    receipt = json.loads((root / 'worker_release_hsl_cpu_rolling_b9d6692.json').read_text())
    recipe = (root / 'Dockerfile.hsl-cpu-rolling').read_text()
    assert recipe.count('FROM ghcr.io/msei99/pbgui-pb8-worker@' + receipt['base_digest']) == 2
    assert '--locked' in recipe
    assert receipt['source_fingerprint'] in recipe
    assert 'cargo test --release --locked --no-default-features --features abi3-py312 hsl --lib' in recipe
    assert hashlib.sha256((root / 'hsl_cpu_Cargo.lock').read_bytes()).hexdigest() == receipt['cargo_lock_sha256']
    assert hashlib.sha256((root / 'hsl_cpu_rolling.patch').read_bytes()).hexdigest() == receipt['patch_sha256']
    old_image = 'ghcr.io/msei99/pbgui-pb8-worker@' + receipt['base_digest']
    from vast_calibration import COMPATIBLE_CALIBRATION_IMAGES
    assert old_image in SUPPORTED_RENTAL_IMAGE_REVISIONS
    assert old_image in COMPATIBLE_CALIBRATION_IMAGES


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


def test_previous_upstream_worker_keeps_its_revision_and_calibration_evidence():
    """A schema upgrade must retain ownership of the preceding immutable image."""
    from vast_calibration import COMPATIBLE_CALIBRATION_IMAGES
    old = 'ghcr.io/msei99/pbgui-pb8-worker@sha256:3fadf2220b4b19ee58aff6df95fa62a5e27e30d5a8058015e326ad4c229f55e0'
    assert SUPPORTED_RENTAL_IMAGE_REVISIONS[old] == '7b639e1180fa6bfe02e110429d4f931933c73089'
    assert old in COMPATIBLE_CALIBRATION_IMAGES


def test_current_metric_contract_excludes_removed_hsl_tier_metrics():
    """The v8.6 worker contract cannot advertise retired HSL tier durations."""
    import json
    path = Path(__file__).resolve().parents[1] / 'setup/vast_gpu_benchmark/gpu_metric_contract.json'
    contract = json.loads(path.read_text())
    retired = {'hard_stop_time_in_orange_pct', 'hard_stop_time_in_yellow_pct'}
    for key in ('supported_metrics', 'allowed_metrics', 'exact_only_metrics'):
        assert not retired.intersection(contract[key])


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
    assert ('DELETE', '/instances/123/') in provider.calls
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


def test_suite_metric_hotfix_is_retired_after_upstream_merge():
    """New candidate provenance retires overlays while published evidence remains."""
    import json
    root = Path(__file__).resolve().parents[1] / 'setup' / 'vast_gpu_benchmark'
    candidate = json.loads((root / 'worker_candidate.json').read_text())
    published = json.loads((root / 'worker_release.json').read_text())
    assert candidate['overlays'] == []
    assert candidate['state'] == 'unpublished_candidate'
    assert published['base_revision'] == '00ce7d0ddf123071ae67db149753d6b3619f52ac'
    current = json.loads((root / 'worker_release_b9d6692.json').read_text())
    assert current['base_revision'] == REVISION
    assert current['overlays'] == []
    assert current['anonymous_pull_verified'] is True
    assert not (root / 'Dockerfile.suite-metrics').exists()
    assert not (root / 'patches/gpu-suite-invalid-metrics.patch').exists()
    assert 'gpu-suite-invalid-metrics.patch' not in (root / '.dockerignore').read_text()


def test_unpatched_v86_worker_retains_ownership_and_calibration_evidence():
    """Introducing a backport does not orphan existing official worker rentals."""
    from vast_calibration import COMPATIBLE_CALIBRATION_IMAGES
    image = 'ghcr.io/msei99/pbgui-pb8-worker@sha256:32d7ee7a00330e01e4a2b8856275289eb8ee4b067542b09cfdce5e81c3b3691c'
    assert SUPPORTED_RENTAL_IMAGE_REVISIONS[image] == '061e472e3d400cb3a740583d52f9d46e02eaf781'
    assert image in COMPATIBLE_CALIBRATION_IMAGES


def test_previous_suite_metric_worker_keeps_original_revision():
    """The latest upgrade preserves recovery for the preceding patched image."""
    from vast_calibration import COMPATIBLE_CALIBRATION_IMAGES
    image = 'ghcr.io/msei99/pbgui-pb8-worker@sha256:792a11f40098a3fffe9365c46e38f60c7bddaa35179c9bc608d130b5e1449dbe'
    assert SUPPORTED_RENTAL_IMAGE_REVISIONS[image] == '061e472e3d400cb3a740583d52f9d46e02eaf781'
    assert image in COMPATIBLE_CALIBRATION_IMAGES


def test_hsl_cache_worker_retains_previous_upstream_image():
    """The performance wrapper must preserve previous rental and calibration identities."""
    from vast_calibration import COMPATIBLE_CALIBRATION_IMAGES
    previous = "ghcr.io/msei99/pbgui-pb8-worker@sha256:dc83342c2d4b9d643164bc064a14fa944fdc8f0d0834b25de8379343736365ae"
    assert SUPPORTED_RENTAL_IMAGE_REVISIONS[previous] == REVISION
    assert previous in COMPATIBLE_CALIBRATION_IMAGES
    root = Path(__file__).resolve().parents[1] / 'setup/vast_gpu_benchmark'
    recipe = (root / 'Dockerfile.hsl-history-cache').read_text()
    assert 'FROM ' + previous in recipe
    assert 'hsl_history_cache.patch' in recipe
    assert 'hsl_history_cache.py' in recipe
