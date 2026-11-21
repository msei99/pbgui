"""Offline PB9 cohort evidence, runtime isolation and worker contract tests."""
import json
from types import SimpleNamespace

import pytest

from vast_calibration import (calibration_decision, calibration_plan_for_runtime,
                              configurable_calibration_plan, finalize_calibration_result)
from vast_native_calibration import native_calibration_progress, validate_native_case


def cohort(generation, attempts=4096, seconds=300, *, status='complete', rejected=0, rows=None):
    """Use the exact INFO record format emitted by native CohortDiagnostics."""
    rows = attempts if rows is None else rows
    return (f'INFO GPU cohort | gen={generation} phase=generation status={status} | '
            f'attempts={attempts} prefilter={rejected} gpu_rows={rows} wall_s={seconds:.3f} '
            f'attempts/s={attempts / seconds:.4f} gpu_rows/s=1.0\n')


def test_native_progress_excludes_cold_and_non_generation_work(tmp_path):
    """First-use/compilation and incomplete/scouting work cannot inflate throughput."""
    log = tmp_path/'optimizer.log'
    log.write_text(cohort(1, seconds=900) + cohort(2) + cohort(3, status='interrupted')
                   + cohort(4).replace('phase=generation', 'phase=screening'))
    result = native_calibration_progress(log)
    assert result['completed_cohorts'] == 1
    assert result['rate_window_seconds'] == 300
    assert result['completed_candidates'] == 4096
    assert result['rate_per_second'] == 4096 / 300
    assert 'candidate_bars' not in result and 'all_dispatches_full' not in result
    validate_native_case(dict(result, population_size=4096, requested_batch_size=4096), 4096, 300)


@pytest.mark.parametrize('bad', [
    {'rejected': 1}, {'rows': 0}, {'rows': 'unknown'}, {'status': 'failed'},
])
def test_native_progress_rejects_unproven_work(tmp_path, bad):
    """CPU-only/pre-filtered/missing/failed telemetry cannot become GPU evidence."""
    log = tmp_path/'optimizer.log'
    log.write_text(cohort(1) + cohort(2, **bad))
    assert native_calibration_progress(log) is None


def test_cold_only_and_duplicate_cohorts_are_not_measurements(tmp_path):
    """No warm steady-state sample or replayed log rows means no rate."""
    log = tmp_path/'optimizer.log'
    log.write_text(cohort(1))
    assert native_calibration_progress(log) is None
    log.write_text(cohort(1) + cohort(1))
    assert native_calibration_progress(log) is None


@pytest.mark.parametrize('change', [
    {'rate_per_second': 999}, {'rate_per_second': float('inf')}, {'rate_window_seconds': 1},
    {'source': 'generation_profile'}, {'requested_batch_size': 8192},
    {'gpu_rows': 1}, {'all_dispatches_full': True}, {'candidate_bars': 100},
])
def test_native_case_revalidates_rate_and_scope(tmp_path, change):
    """Host import refuses fabricated rates, insufficient windows and PB8 claims."""
    log = tmp_path/'optimizer.log'
    log.write_text(cohort(1) + cohort(2))
    row = dict(native_calibration_progress(log), population_size=4096, requested_batch_size=4096)
    with pytest.raises(ValueError, match='native cohort evidence'):
        validate_native_case(dict(row, **change), 4096, 300)


def test_native_plan_and_worker_share_scaling_decision():
    """Shared scaling logic retains PB8 protocol and selects native protocol 5."""
    from setup.vast_gpu_benchmark.cloud_worker import configurable_calibration_decision
    legacy = configurable_calibration_plan(4096, 512, 8192, .1)
    plan = calibration_plan_for_runtime(legacy, 'pb9')
    assert plan['protocol'] == 5 and legacy['protocol'] == 4
    assert calibration_plan_for_runtime(legacy, 'pb8') is legacy
    cases = {4096: {'valid': True, 'rate_per_second': 100},
             4608: {'valid': True, 'rate_per_second': 105}}
    host = calibration_decision(cases, protocol=5, plan=plan)
    assert host == configurable_calibration_decision({str(k): v for k, v in cases.items()}, plan)
    assert host['recommended_population'] == 4608


def test_native_small_populations_do_not_relax_pb8_limits():
    """PB9's streamed preparation uses bounded native sizes; PB8 retains full batches."""
    from setup.vast_gpu_benchmark.cloud_worker import configurable_calibration_plan as worker_plan
    native = configurable_calibration_plan(128, 128, 256, .1, 300, 600, protocol=5)
    assert worker_plan(native) == native
    with pytest.raises(ValueError):
        configurable_calibration_plan(128, 128, 256, .1)
    with pytest.raises(ValueError):
        worker_plan(dict(native, protocol=4))


def test_native_profiles_bind_cpu_preparation_allocation():
    """Whole-cohort throughput is not portable across different preparation counts."""
    from vast_calibration import calibration_workload_fingerprint
    def config(version, cpus):
        """Build only the resource fields needed to establish this boundary."""
        return {'config_version': version, 'optimize': {'n_cpus': cpus}}
    assert calibration_workload_fingerprint(config('v9.0.0', 4)) != calibration_workload_fingerprint(config('v9.0.0', 7))
    assert calibration_workload_fingerprint(config('v8.6.0', 4)) == calibration_workload_fingerprint(config('v8.6.0', 7))


def test_native_override_and_fallback_use_native_sizes(tmp_path):
    """PB9 can apply its measured small widths without relaxing PB8 overrides."""
    from vast_gpu_tuning import _validated_rental_gpu_profile, rental_card_profile
    values = {'population_size': 128, 'batch_size': 128, 'max_dispatch_candidate_bars': 500000000}
    assert _validated_rental_gpu_profile(values, runtime='pb9') == values
    with pytest.raises(ValueError):
        _validated_rental_gpu_profile(values)
    preview = rental_card_profile({'vram_gb': 24}, {'runtime': 'pb9'}, tmp_path)
    assert preview['population_size'] == 128 and preview['max_dispatch_candidate_bars'] == 500000000


def test_finalize_native_evidence_preserves_scope_and_identity(tmp_path, monkeypatch):
    """Valid native cohorts import through the real store without claiming a work cap."""
    import vast_calibration
    import vast_pb9
    from vast_jobs import JobStore, write_json
    from vast_worker_profiles import worker_profile
    store = JobStore(tmp_path/'vast')
    row = store.create_preparation('native', 512, 4, False, runtime='pb9')
    identifier = row['id']
    root = store.directory(identifier)
    profile = worker_profile('pb9')
    plan = configurable_calibration_plan(4096, 512, 8192, .1, protocol=5)
    store.update(identifier, kind='calibration', calibration_plan=plan,
                 runtime_metrics={'gpu_power_limit_watts': 250})
    write_json(root/'intent.json', {'image': profile.image, 'pb8_revision': profile.revision,
                                    'offer': {'gpu_name': 'RTX 3090', 'vram_gb': 24}})
    (root/'input').mkdir()
    (root/'input/optimize.json').write_text('{}')
    (root/'final-results').mkdir()
    log = tmp_path/'optimizer.log'
    cases = {}
    for width, seconds in ((4096, 300), (4608, 330)):
        log.write_text(cohort(1, width) + cohort(2, width, seconds))
        cases[str(width)] = dict(native_calibration_progress(log), population_size=width,
                                requested_batch_size=width)
    result = {'schema': 1, 'protocol': 5, 'status': 'completed', 'population_size': 4608,
              'stop_reason': 'scaling_plateau', 'cases': cases,
              'hardware': {'gpu': 'RTX 3090', 'vram_bytes': 24*1024**3,
                           'pci_device_id': '2204', 'compute_capability': '8.6'}}
    write_json(root/'final-results/calibration-result.json', result)
    monkeypatch.setattr(vast_calibration, 'NATIVE_CALIBRATION_IMAGES', {profile.image})
    monkeypatch.setattr(vast_pb9, '_adapter', lambda: SimpleNamespace(load=lambda _: {
        'config_version': 'v9.0.0', 'optimize': {}}))
    evidence = finalize_calibration_result(store, identifier)
    assert evidence['runtime'] == 'pb9' and evidence['measurement'] == 'native_cohort_v1'
    assert 'max_dispatch_candidate_bars' not in evidence
    result['protocol'] = 4
    write_json(root/'final-results/calibration-result.json', result)
    with pytest.raises(ValueError, match='runtime'):
        finalize_calibration_result(store, identifier)


def test_profiles_do_not_cross_runtime_boundaries(tmp_path):
    """Accepting or listing native measurements preserves PB8 rows, but never selects them."""
    from vast_calibration import CalibrationProfiles
    shipped = tmp_path/'shipped.json'
    shipped.write_text(json.dumps({'schema': 1, 'profiles': []}))
    profiles = CalibrationProfiles(tmp_path, shipped)
    identity = {'fingerprint': 'b'*64, 'gpu_power_limit_watts': 250,
                'gpu_power_limit_source': 'runtime_nvidia_smi', 'runtime_verified': True}
    legacy = {'id': 'a'*64, 'gpu_variant_fingerprint': identity['fingerprint'],
              'population_size': 8192, 'hardware_identity': identity}
    profiles.accept(legacy)
    profiles.accept(dict(legacy, id='c'*64, runtime='pb9', population_size=4096, protocol=5,
                         measurement='native_cohort_v1', requested_batch_size=4096))
    assert profiles.match(identity)['population_size'] == 8192
    assert CalibrationProfiles(tmp_path, shipped, runtime='pb9').match(identity)['population_size'] == 4096
    assert len(profiles.list()) == 2


def test_native_worker_controller_measures_and_stops_fresh_processes(tmp_path, monkeypatch):
    """Exercise the actual case loop without CUDA, network, rentals or user data."""
    import time
    from pathlib import Path
    from setup.vast_gpu_benchmark import cloud_worker as worker
    monkeypatch.setattr(worker, 'ROOT', tmp_path)
    monkeypatch.setattr(worker, 'GUARD_ROOT', tmp_path)
    monkeypatch.setattr(worker, 'gpu_memory_used_bytes', lambda: 100)
    monkeypatch.setattr(worker, 'seed_calibration_case_caches', lambda *_: None)
    monkeypatch.setattr(worker, 'optimizer_command', lambda path: [str(path)])
    (tmp_path/'guard.json').write_text(json.dumps({'deadline': time.time()+3600}))
    seen = []

    class Process:
        """Own a fake fresh process whose completed native records are already flushed."""
        def __init__(self, args, **kwargs):
            case = json.loads(Path(args[0]).read_text())
            seen.append(case)
            width = case['optimize']['population_size']
            seconds = 300 if len(seen) == 1 else 330
            kwargs['stdout'].write((cohort(1, width) + cohort(2, width, seconds)).encode())
            kwargs['stdout'].flush()
            self.returncode = None

        def poll(self):
            """Keep the process live until the worker acknowledges its measurement."""
            return self.returncode

        def wait(self):
            """Return the recorded termination result."""
            return self.returncode

    def stop(process):
        """Simulate acknowledged SIGTERM, retaining native measurements."""
        process.returncode = -15

    monkeypatch.setattr(worker.subprocess, 'Popen', Process)
    monkeypatch.setattr(worker, 'stop_process', stop)
    config = {'config_version': 'v9.0.0', 'backtest': {'exchanges': ['binance']},
              'optimize': {'gpu': {'max_dispatch_candidate_bars': 777}}}
    result = worker.run_calibration(config, {'vram_bytes': 10000}, {},
                                   configurable_calibration_plan(4096, 512, 8192, .1, protocol=5))
    assert result['status'] == 'completed' and result['population_size'] == 4608
    assert result['stop_reason'] == 'scaling_plateau' and len(seen) == 2
    for case in seen:
        assert case['optimize']['gpu']['max_dispatch_candidate_bars'] == 777
        assert case['optimize']['gpu']['tuning_mode'] == 'off'
    for width, case in result['cases'].items():
        validate_native_case(case, int(width), 300)


@pytest.mark.parametrize('population,batch,mode,applied', [
    (None, None, 'conservative', True), (128, None, 'conservative', False),
    (None, 8, 'conservative', False), (None, None, 'off', False),
])
def test_native_profile_application_preserves_explicit_settings(tmp_path, monkeypatch, population, batch, mode, applied):
    """Only an accepted exact-workload profile supplies native Auto widths."""
    import vast_calibration as calibration
    from vast_calibration import CalibrationProfiles, calibration_workload_fingerprint, gpu_variant_identity
    from vast_pb9 import resolve_gpu_settings
    from vast_worker_profiles import worker_profile
    profile = worker_profile('pb9')
    config = {'config_version': 'v9.0.0', 'optimize': {'population_size': population,
              'gpu': {'batch_size': batch, 'tuning_mode': mode, 'max_dispatch_candidate_bars': 777}}}
    state = {'hardware': {'gpu': 'RTX 3090', 'vram_bytes': 24*1024**3,
                         'compute_capability': '8.6', 'pci_device_id': '2204'},
             'runtime_metrics': {'gpu_power_limit_watts': 250}}
    identity = gpu_variant_identity({'gpu_name': 'RTX 3090', 'vram_gb': 24},
                                   dict(state['hardware'], gpu_power_limit_watts=250))
    shipped = tmp_path/'shipped.json'
    shipped.write_text(json.dumps({'schema': 1, 'profiles': []}))
    profiles = CalibrationProfiles(tmp_path, shipped, runtime='pb9')
    profiles.accept({'id': 'a'*64, 'gpu_variant_fingerprint': identity['fingerprint'],
                     'workload_fingerprint': calibration_workload_fingerprint(config),
                     'population_size': 4096, 'protocol': 5, 'runtime': 'pb9',
                     'measurement': 'native_cohort_v1', 'requested_batch_size': 4096,
                     'worker_image': profile.image, 'pb8_revision': profile.revision})
    monkeypatch.setattr(calibration, 'CalibrationProfiles', lambda *_args, **_kwargs: profiles)
    monkeypatch.setattr(calibration, 'NATIVE_CALIBRATION_IMAGES', {profile.image})
    result = resolve_gpu_settings(config, state, {})
    assert bool(result['calibration_profile_id']) == applied
    assert config['optimize']['gpu']['max_dispatch_candidate_bars'] == 777
    assert config['optimize']['population_size'] == (4096 if applied else population)
    assert config['optimize']['gpu']['batch_size'] == (4096 if applied else batch)


def test_persisted_native_plan_is_checked_before_credentials(tmp_path, monkeypatch):
    """Even a published worker cannot rent with damaged persisted calibration limits."""
    import vast_calibration
    import vast_jobs
    from vast_jobs import JobStore, write_json
    from vast_provider import VastError
    from vast_worker_profiles import worker_profile
    profile = worker_profile('pb9')
    monkeypatch.setattr(vast_calibration, 'NATIVE_CALIBRATION_IMAGES', {profile.image})
    monkeypatch.setattr(vast_jobs, 'services_available', lambda: True)
    monkeypatch.setattr(vast_jobs, 'VastCredentialStore', lambda: pytest.fail('Credentials consulted'))
    store = JobStore(tmp_path/'vast')
    row = store.create_preparation('native', 512, 4, False, runtime='pb9')
    plan = configurable_calibration_plan(4096, 512, 8192, .1, protocol=5)
    plan['sample_seconds'] = 1
    store.update(row['id'], kind='calibration', status='ready', calibration_plan=plan)
    write_json(store.directory(row['id'])/'intent.json', {'image': profile.image,
                                                        'pb8_revision': profile.revision})
    with pytest.raises(VastError, match='Invalid native calibration plan'):
        store.start(row['id'], {'id': 1, 'price_hour_usd': .1, 'cuda_max_good': 13,
                               'disk_gb': 40, 'cpu_cores': 4}, .75, .1)


def test_incremental_native_reader_retains_warmup_and_completed_time(tmp_path):
    """Large logs, partial writes and repeated polls cannot erase or double-count work."""
    from vast_native_calibration import NativeCohortReader
    log = tmp_path/'optimizer.log'
    reader = NativeCohortReader(4096)
    log.write_text(cohort(1))
    assert reader.read(log) is None
    with log.open('a') as stream:
        stream.write(('unrelated optimizer output\n' * 60000) + cohort(2)[:-5])
    assert reader.read(log) is None
    with log.open('a') as stream:
        stream.write(cohort(2)[-5:] + cohort(3, attempts=123) + cohort(4, rejected=1))
    result = reader.read(log)
    assert result['completed_candidates'] == 4096 and result['completed_cohorts'] == 1
    assert reader.read(log) == result
    with log.open('a') as stream:
        stream.write(cohort(5) + cohort(5))
    assert reader.read(log)['rate_window_seconds'] == 600
    log.write_text(cohort(1))
    assert reader.read(log) is None


def test_native_worker_only_advertises_native_protocol(monkeypatch):
    """A new native image never advertises compatibility with PB8 dispatch evidence."""
    import vast_calibration as calibration
    from vast_worker_profiles import worker_profile
    image = worker_profile('pb9').image
    monkeypatch.setattr(calibration, 'NATIVE_CALIBRATION_IMAGES', {image})
    assert calibration.calibration_worker_available(image)
    assert calibration.calibration_worker_available(image, protocol=5)
    assert not calibration.calibration_worker_available(image, protocol=4)
    assert not calibration.calibration_worker_available(image, protocol=3)


@pytest.mark.parametrize('populations', [None, [], 1, {}, '4096'])
def test_malformed_native_plan_is_a_value_error(populations):
    """Malformed controls report validation errors rather than indexing exceptions."""
    plan = dict(configurable_calibration_plan(4096, 512, 8192, .1), populations=populations)
    with pytest.raises(ValueError, match='native calibration plan'):
        calibration_plan_for_runtime(plan, 'pb9')
