"""Offline image probe of the real PB9 log format; this is not a CUDA benchmark."""
import importlib.util
import io
import logging
from pathlib import Path
import tempfile
import sys
import subprocess

SERVICE = 'VastWorker'


def verify():
    """Generate native diagnostics with a simulated clock and verify their contract."""
    from optimization.progress import CohortDiagnostics
    from vast_native_calibration import native_calibration_progress, validate_native_case
    log = io.StringIO()
    handler = logging.StreamHandler(log)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    counts = {'completed': 0, 'screened': 0, 'prefilter_rejected': 0}

    class Session:
        """Supply only the transport snapshot API consumed by native diagnostics."""
        service = None

        def diagnostics_snapshot(self):
            """Expose cumulative replay rows as the actual transport does."""
            return {'replay_rows': counts['completed']}

    try:
        for generation in (1, 2):
            diagnostics = CohortDiagnostics(generation=generation, phase='generation',
                                             counts=lambda: dict(counts), session=Session())
            diagnostics.started = (diagnostics.started[0] - 301, diagnostics.started[1])
            counts['completed'] += 4096
            diagnostics.finish('complete')
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'optimizer.log'
            path.write_text(log.getvalue())
            progress = native_calibration_progress(path)
            validate_native_case(dict(progress, population_size=4096, requested_batch_size=4096), 4096, 300)
            assert progress['completed_candidates'] == 4096
            assert progress['completed_cohorts'] == 1
    finally:
        logger.removeHandler(handler)
    spec = importlib.util.spec_from_file_location('cloud_worker', '/opt/pbgui/worker.py')
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    plan = {'protocol': 5, 'populations': [4096, 4608], 'population_step': 512,
            'sample_seconds': 300, 'case_timeout_seconds': 600, 'max_population': 8192,
            'min_scale_gain': .1, 'vram_reserve_ratio': .15}
    assert worker.configurable_calibration_plan(plan) == plan
    assert worker.configurable_calibration_decision({}, plan)['next_population'] == 4096
    small = dict(plan, populations=[128, 256], population_step=128, max_population=256)
    assert worker.configurable_calibration_plan(small) == small
    class ImportsVerified(Exception):
        """Stop before market preparation, subprocesses or CUDA can execute."""

    def imports_verified(*_args):
        """Reach preparation only after the worker has imported its native helpers."""
        raise ImportsVerified

    # Reproduce the SSH control-script location without the probe's PYTHONPATH
    # or cached native module hiding a broken /work -> /opt import boundary.
    sys.modules.pop('vast_native_calibration', None)
    sys.path[:] = [path for path in sys.path if path != '/opt/pbgui']
    worker.seed_calibration_case_caches = imports_verified
    with tempfile.TemporaryDirectory() as temporary:
        worker.ROOT = worker.GUARD_ROOT = Path(temporary)
        (worker.ROOT/'guard.json').write_text('{"deadline": 9999999999}')
        try:
            worker.run_calibration({'config_version': 'v9.0.0', 'backtest': {'exchanges': ['binance']}},
                                   {}, {}, plan)
        except ImportsVerified:
            pass
        else:
            raise AssertionError('Expected bounded calibration preparation probe')
        (worker.ROOT/'input').mkdir(exist_ok=True)
        (worker.ROOT/'input/launch_options.json').write_text('{}')
        command = worker.optimizer_command(worker.ROOT/'config.json')
        assert command[:3] == [sys.executable, str(Path(sys.executable).with_name('passivbot')), 'optimize']
        help_result = subprocess.run(command[:3] + ['--help'], stdin=subprocess.DEVNULL,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        assert help_result.returncode == 0, 'Native optimizer CLI help failed offline'
        identity_probe = '''import sys, optimize
from optimization.backends.gpu_backend import _Search
def probe_main():
    search = _Search(state={}, session=None, registry=None, recorder=None, template={},
        build_config_fn=optimize.individual_to_config, overrides_fn=None, overrides_list=[],
        checkpoint_path=None, checkpoint_interval=60, interrupt_check=lambda: None,
        screening_policy={})
    assert search._reuse_record_configs, 'Native CLI lost prepared-config reuse'
    return 0
optimize.main = probe_main
from passivbot_cli.main import console_main
sys.argv = ['passivbot', 'optimize']
raise SystemExit(console_main())
'''
        identity_result = subprocess.run([sys.executable, '-c', identity_probe], stdin=subprocess.DEVNULL,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        assert identity_result.returncode == 0, 'Real native CLI/backend module-identity probe failed'
    print('Native cohort log contract and protocol 5 verified offline; no CUDA execution.')


if __name__ == '__main__':
    verify()
