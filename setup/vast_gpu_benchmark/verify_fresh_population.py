"""Verify the candidate population policy and retained worker provenance offline."""
from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path
import pickle

import numpy as np
from pymoo.core.problem import Problem

from config.schema import get_template_config
from optimization.backends.gpu_backend import _build_gpu_nsga2, _build_random_sampling, run_backend
from rust_utils import source_fingerprint, verify_loaded_runtime_extension
import passivbot_rust  # noqa: F401 -- verify the actual compiled worker extension


def verify() -> dict:
    """Exercise real NSGA-II sampling and checkpoint restore without GPU or network."""
    metadata = json.loads(Path(__file__).with_name('worker_candidate_fresh_population.json').read_text())
    source = Path(inspect.getfile(inspect.unwrap(run_backend)))
    assert hashlib.sha256(source.read_bytes()).hexdigest() == metadata['patched_gpu_source_sha256']
    assert 'sampling[0] = normalize_vector(base_vector)' not in inspect.getsource(run_backend)
    fingerprint = source_fingerprint(source.parents[3] / 'passivbot-rust')
    assert fingerprint == metadata['retained_rust_source_fingerprint']
    native = verify_loaded_runtime_extension(fingerprint=fingerprint)
    assert native['runtime_compiled_source_stamp'] == metadata['retained_rust_source_fingerprint']
    assert native['expected_source_fingerprint'] == metadata['retained_rust_source_fingerprint']
    config = get_template_config()
    previous = None
    for seed in (1, 19, 2026):
        sampling = _build_random_sampling(np.random.default_rng(seed), 8, 5)
        algorithm = _build_gpu_nsga2(config, sampling=sampling, population_size=8, n_params=5)
        algorithm.setup(Problem(n_var=5, n_obj=2, n_ieq_constr=1, xl=0.0, xu=1.0), seed=seed)
        restored = pickle.loads(pickle.dumps(algorithm))
        expected = np.random.default_rng(seed).random((8, 5))
        np.testing.assert_array_equal(algorithm.ask().get('X'), expected)
        np.testing.assert_array_equal(restored.ask().get('X'), expected)
        if previous is not None:
            assert np.all(np.any(previous != sampling, axis=1))
        previous = sampling
    explicit = _build_random_sampling(np.random.default_rng(19), 8, 5)
    explicit[0] = np.array([0.1, 0.2, 0.3, 0.4, 0.5])
    algorithm = _build_gpu_nsga2(config, sampling=explicit, population_size=8, n_params=5)
    algorithm.setup(Problem(n_var=5, n_obj=2, n_ieq_constr=1, xl=0.0, xu=1.0), seed=19)
    np.testing.assert_array_equal(algorithm.ask().get('X'), explicit)
    return {'random_population_seeds': 3, 'checkpoint_restore': True,
            'explicit_seed_sampling': True, 'rust_fingerprint_preserved': True,
            'gpu_source_sha256': metadata['patched_gpu_source_sha256'],
            'gpu_hardware_test': 'pending', 'real_loop_test': 'pending'}


if __name__ == '__main__':
    print(json.dumps(verify(), indent=4))
