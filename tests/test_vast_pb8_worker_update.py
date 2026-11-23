"""Candidate-only PB8 updates bind sources and retain the published contract."""
import hashlib
import json
from pathlib import Path
import shutil

import pytest

from setup.vast_gpu_benchmark import build_worker_context as builder
from vast_worker_profiles import worker_profile

ROOT = Path(__file__).resolve().parents[1]/'setup/vast_gpu_benchmark'
METADATA = json.loads((ROOT/'worker_candidate_pb8_current.json').read_text())


def test_current_pb8_build_context_is_allowlisted_and_keeps_active_profile(tmp_path):
    """Preparing a new revision never activates an unpublished image for rentals."""
    previous = worker_profile('pb8')
    context = builder.prepare('pb8', tmp_path/'context', revision=METADATA['base_revision'],
                              reuse_runtime_base=True)
    assert worker_profile('pb8') == previous
    assert sorted(p.name for p in context.iterdir()) == [
        'Dockerfile', 'cloud_worker.py', 'pb8_current_Cargo.lock', 'pb8_current_worker.patch',
        'run.py', 'verify_pb8_current.py', 'worker_candidate.json',
    ]
    assert json.loads((context/'worker_candidate.json').read_text()) == METADATA
    recipe = (context/'Dockerfile').read_text()
    assert '%%' not in recipe
    assert f'FROM {METADATA["runtime_base"]} AS runtime' in recipe
    assert METADATA['base_revision'] in recipe
    assert '--locked' in recipe and '--fuzz=0' in recipe
    assert 'COPY cloud_worker.py' not in recipe  # Preserve the shipped wrapper.
    for name, checksum in METADATA['build_input_sha256'].items():
        assert hashlib.sha256((context/name).read_bytes()).hexdigest() == checksum


@pytest.mark.parametrize('revision', ['a'*40, 'master', 'a'*40+';exit 0'])
def test_unreviewed_pb8_target_rejected_before_context_creation(tmp_path, revision):
    """Worker-specific overlays cannot be applied to an unrelated revision."""
    with pytest.raises(ValueError):
        builder.prepare('pb8', tmp_path/'context', revision=revision, reuse_runtime_base=True)
    assert not (tmp_path/'context').exists()


def test_published_pin_and_historical_images_keep_separate_revisions():
    """New jobs use the publication, while old rentals keep their actual source."""
    from vast_jobs import IMAGE, REVISION, SUPPORTED_RENTAL_IMAGE_REVISIONS
    from vast_calibration import calibration_worker_available, COMPATIBLE_CALIBRATION_IMAGES
    from vast_worker_compatibility import worker_compatibility_errors

    receipt = json.loads((ROOT/'worker_release_15d6404.json').read_text())
    assert IMAGE == 'ghcr.io/msei99/pbgui-pb8-worker@'+receipt['digest']
    assert REVISION == receipt['pb8_revision'] == METADATA['base_revision']
    assert receipt['anonymous_pull_verified'] is True
    assert receipt['publication']['published'] is True
    assert SUPPORTED_RENTAL_IMAGE_REVISIONS[METADATA['runtime_base']] == 'b9d6692ea41266ecb4ab81d40bb5e5baef29b50a'
    assert METADATA['runtime_base'] in COMPATIBLE_CALIBRATION_IMAGES
    assert calibration_worker_available(IMAGE)
    assert not calibration_worker_available(METADATA['runtime_base'])
    config = {'bot': {'long': {'hsl': {'scale_budget_with_excess_allowance': True}}}}
    assert worker_compatibility_errors(config, REVISION) == []


def test_changed_pb8_overlay_rejected_before_context_creation(tmp_path, monkeypatch):
    """A changed patch cannot reuse a verified candidate receipt silently."""
    source = tmp_path/'inputs'
    source.mkdir()
    for name in ('worker_candidate_pb8_current.json', *METADATA['build_input_sha256']):
        shutil.copy2(ROOT/name, source/name)
    with (source/'pb8_current_worker.patch').open('ab') as stream:
        stream.write(b'changed')
    monkeypatch.setattr(builder, '__file__', str(source/'build_worker_context.py'))
    with pytest.raises(ValueError, match='checksum'):
        builder.prepare('pb8', tmp_path/'context', revision=METADATA['base_revision'],
                        reuse_runtime_base=True)
    assert not (tmp_path/'context').exists()


def test_old_revision_calibration_is_not_reused_for_new_worker(monkeypatch):
    """Historical measurements stay readable without sizing a different revision."""
    import vast_calibration
    from vast_gpu_tuning import resolve_gpu_settings
    from tests.test_vast_gpu_tuning import workload

    class Profiles:
        """Return a genuine old image/revision pair from an isolated fake store."""

        def __init__(self, *args, **kwargs):
            pass

        def match(self, *args, **kwargs):
            """Old data must not become a measurement of the new runtime."""
            return {'id': 'a'*64, 'source': 'local', 'protocol': 1,
                    'worker_image': METADATA['runtime_base'],
                    'pb8_revision': 'b9d6692ea41266ecb4ab81d40bb5e5baef29b50a',
                    'population_size': 12288, 'max_dispatch_candidate_bars': 8_000_000_000}

    monkeypatch.setattr(vast_calibration, 'CalibrationProfiles', Profiles)
    result = resolve_gpu_settings(
        workload(),
        {'workers': 16, 'hardware': {'gpu': 'NVIDIA RTX 3090', 'vram_bytes': 24*1024**3,
                                     'compute_capability': '8.6'},
         'runtime_metrics': {'gpu_power_limit_watts': 170}},
        {'gpu_name': 'RTX 3090', 'vram_gb': 24, 'gpu_mem_bw_gbps': 936, 'tflops': 35},
    )
    assert result['applied_by'] != 'local_calibration'
    assert result.get('calibration_profile_id') != 'a'*64
