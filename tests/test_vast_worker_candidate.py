"""Preservation contracts for the PB8 rental-protocol candidate image."""

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'setup/vast_gpu_benchmark'


def test_candidate_inherits_the_published_patched_worker_without_modifying_pb8():
    """The protocol overlay must retain the exact current PB8 image and its fixes."""
    receipt = json.loads((ROOT / 'worker_release_fresh_population_b9d6692.json').read_text())
    candidate = json.loads((ROOT / 'worker_candidate_rental_protocol.json').read_text())
    dockerfile = (ROOT / 'Dockerfile.rental-protocol').read_text()
    base = receipt['image'].split(':', 1)[0] + '@' + receipt['digest']
    assert candidate['base_image'] == base
    assert dockerfile.splitlines()[0] == 'FROM ' + base
    copies = [line.split()[-1] for line in dockerfile.splitlines() if line.startswith('COPY ')]
    assert copies and all(path.startswith('/opt/pbgui/') for path in copies)
    assert 'verify_fresh_population.py' in dockerfile
    assert 'PYTHONPATH=/opt/passivbot/src' in dockerfile
    assert receipt['pb8_revision'] in dockerfile


def test_candidate_records_retained_source_and_native_fingerprints():
    """Candidate evidence binds the existing PB8 fixes and the new protocol separately."""
    prior = json.loads((ROOT / 'worker_candidate_fresh_population.json').read_text())
    candidate = json.loads((ROOT / 'worker_candidate_rental_protocol.json').read_text())
    retained = candidate['retained_pb8']
    assert retained['gpu_source_sha256'] == prior['patched_gpu_source_sha256']
    assert retained['rust_source_fingerprint'] == prior['retained_rust_source_fingerprint']
    assert candidate['base_revision'] == prior['base_revision']
    assert candidate['worker_source_sha256'] == hashlib.sha256((ROOT / 'cloud_worker.py').read_bytes()).hexdigest()
