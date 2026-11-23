"""Check an updated PB8 worker offline without GPU, network or user data."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import pickle
import subprocess

import numpy as np
from pymoo.core.problem import Problem


def verify() -> dict:
    """Verify exact sources, native stamp, worker and seeded population behavior."""
    from config.schema import get_template_config
    from optimization.backends.gpu_backend import _build_gpu_nsga2, _build_random_sampling, run_backend
    from rust_utils import source_fingerprint, verify_loaded_runtime_extension
    import inspect
    import passivbot_rust  # noqa: F401 -- validate the loaded binary, not its filename

    root = Path('/opt/passivbot')
    metadata = json.loads(Path('/opt/pb8-worker-provenance.json').read_text())
    revision = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
    assert revision == metadata['base_revision'] == Path('/opt/pb8-revision').read_text().strip()
    for name, expected in metadata['source_sha256'].items():
        assert hashlib.sha256((root/name).read_bytes()).hexdigest() == expected, name
    worker = Path('/opt/pbgui/worker.py')
    assert hashlib.sha256(worker.read_bytes()).hexdigest() == metadata['worker_source_sha256']
    spec = importlib.util.spec_from_file_location('verified_cloud_worker', worker)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fingerprint = source_fingerprint(root/'passivbot-rust')
    assert fingerprint == '25ab6c28a3379eb067b55c2702e504d8f26c3dbd29898ad7a7654857d04bf089'
    native = verify_loaded_runtime_extension(fingerprint=fingerprint)
    assert native['runtime_compiled_source_stamp'] == native['expected_source_fingerprint'] == fingerprint
    assert 'sampling[0] = normalize_vector(base_vector)' not in inspect.getsource(run_backend)
    config = get_template_config()
    assert config['config_version'].startswith('v8.')
    for seed in (1, 19, 2026):
        sampling = _build_random_sampling(np.random.default_rng(seed), 8, 5)
        algorithm = _build_gpu_nsga2(config, sampling=sampling, population_size=8, n_params=5)
        algorithm.setup(Problem(n_var=5, n_obj=2, n_ieq_constr=1, xl=0.0, xu=1.0), seed=seed)
        restored = pickle.loads(pickle.dumps(algorithm))
        expected = np.random.default_rng(seed).random((8, 5))
        np.testing.assert_array_equal(algorithm.ask().get('X'), expected)
        np.testing.assert_array_equal(restored.ask().get('X'), expected)
    return {'revision': revision, 'schema': config['config_version'], 'source_fingerprint': fingerprint,
            'worker_sha256': metadata['worker_source_sha256'], 'population_seeds': 3,
            'checkpoint_restore': True, 'gpu_hardware_test': 'not run'}


if __name__ == '__main__':
    print(json.dumps(verify(), indent=4))
