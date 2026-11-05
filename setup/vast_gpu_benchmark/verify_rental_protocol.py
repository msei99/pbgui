"""Offline candidate-image verification without GPU, network or real credentials."""

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import tarfile
import tempfile
from unittest.mock import patch

SERVICE = "VastWorker"


def verify(worker_path: Path) -> None:
    """Exercise long guard deadlines and delta snapshots from the shipped source."""
    receipt = json.loads(worker_path.with_name('worker_candidate_rental_protocol.json').read_text())
    assert hashlib.sha256(worker_path.read_bytes()).hexdigest() == receipt['worker_source_sha256']
    spec = importlib.util.spec_from_file_location('candidate_worker', worker_path)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        pareto = root / 'output/optimize_results/run/pareto'
        pareto.mkdir(parents=True)
        for name in ('old.json', 'new.json'):
            (pareto / name).write_text('{}')
        (root / 'partial-skip.json').write_text(json.dumps({'skip': ['old.json']}))
        with patch.object(worker, 'ROOT', root):
            worker.snapshot(False)
            with tarfile.open(root / 'partial.tar.gz') as archive:
                names = archive.getnames()
            assert 'optimize_results/run/pareto/new.json' in names
            assert 'optimize_results/run/pareto/old.json' not in names
            deadline = 1000 + 120 * 3600
            environment = {'PBGUI_DEADLINE': str(deadline), 'PBGUI_MAX_DEADLINE': str(deadline),
                           'PBGUI_HARD_DEADLINE': str(deadline + 3600),
                           'CONTAINER_ID': '123', 'CONTAINER_API_KEY': 'offline-fixture',
                           'PBGUI_JOB_ID': 'a' * 32}
            class Verified(Exception):
                """Stop before any provider interaction after the first guard tick."""
            with patch.dict(os.environ, environment, clear=True), patch.object(worker.time, 'time', return_value=1000), patch.object(worker.time, 'sleep', side_effect=Verified):
                try:
                    worker.guard()
                except Verified:
                    pass
                else:
                    raise AssertionError('Independent guard did not enter its wait loop')
            state = json.loads((root / 'guard.json').read_text())
            assert state['deadline'] == deadline and state['deadline_protocol'] == 3
            assert state['max_deadline'] <= state['hard_deadline']


if __name__ == '__main__':
    verify(Path('/opt/pbgui/worker.py'))
