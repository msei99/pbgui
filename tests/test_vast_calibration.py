"""Tests for deterministic Vast GPU calibration identities and decisions."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from vast_calibration import (
    CalibrationProfiles,
    WORKLOAD_VERSION,
    calibration_decision,
    configurable_calibration_plan,
    calibration_workload_fingerprint,
    canonical_calibration_config,
    gpu_variant_identity,
    finalize_calibration_result,
    recommended_population,
)


def offer(vram_gb: float = 24.0) -> dict:
    """Return one public provider identity for focused tests."""
    return {
        'id': 10, 'machine_id': 20, 'gpu_name': 'RTX 3090', 'vram_gb': vram_gb,
        'gpu_mem_bw_gbps': 936.2,
    }


def runtime(vram_mib: int) -> dict:
    """Return one runtime-verified identity payload."""
    return {
        'gpu': 'NVIDIA GeForce RTX 3090', 'vram_bytes': vram_mib * 1024**2,
        'pci_device_id': '0X220410DE', 'compute_capability': '8.6',
    }


def test_gpu_variant_identity_separates_modified_vram_cards() -> None:
    """Same-name cards with standard and modified VRAM never share a fingerprint."""
    standard = gpu_variant_identity(offer(24), runtime(24576))
    modified = gpu_variant_identity(offer(48), runtime(49152))
    assert standard['runtime_verified'] is True
    assert standard['fingerprint'] != modified['fingerprint']
    assert modified['vram_mib'] == 49152


def test_power_limit_separates_exact_gpu_variants_and_requires_runtime_measurement() -> None:
    """Only the nvidia-smi runtime measurement enters profile identity."""
    advertised = dict(offer(), gpu_max_power_watts=370)
    low_power = gpu_variant_identity(advertised, dict(runtime(24576), gpu_power_limit_watts=200))
    high_power = gpu_variant_identity(advertised, dict(runtime(24576), gpu_power_limit_watts=350))
    advertised_only = gpu_variant_identity(advertised, runtime(24576))
    assert low_power['gpu_power_limit_watts'] == 200
    assert low_power['gpu_power_limit_source'] == 'runtime_nvidia_smi'
    assert low_power['provider']['gpu_max_power_watts'] == 370
    assert low_power['fingerprint'] != high_power['fingerprint']
    assert advertised_only['gpu_power_limit_watts'] is None
    assert advertised_only['gpu_power_limit_source'] is None
    assert advertised_only['provider']['gpu_max_power_watts'] == 370
    assert low_power['fingerprint'] != advertised_only['fingerprint']


def test_match_uses_nearest_power_only_for_same_runtime_card_variant(tmp_path: Path) -> None:
    """Nearest profiles may vary by power cap but never cross a mod-VRAM card identity."""
    shipped_path = tmp_path / 'shipped.json'
    profiles_data = []
    for cap, marker, population in ((200, 'a', 8192), (370, 'b', 24576)):
        hardware = gpu_variant_identity(offer(), dict(runtime(24576), gpu_power_limit_watts=cap))
        profiles_data.append({
            'id': marker * 64, 'gpu_variant_fingerprint': hardware['fingerprint'],
            'workload_fingerprint': 'c' * 64, 'population_size': population,
            'hardware_identity': hardware, 'protocol': 2,
        })
    shipped_path.write_text(json.dumps({'schema': 1, 'profiles': profiles_data}))
    profiles = CalibrationProfiles(tmp_path / 'local', shipped_path)

    target = gpu_variant_identity(offer(), dict(runtime(24576), gpu_power_limit_watts=350))
    nearest = profiles.match(target)
    assert nearest['population_size'] == 24576
    assert nearest['match_type'] == 'nearest_power_limit'
    assert nearest['power_limit_delta_watts'] == 20

    mod_target = gpu_variant_identity(offer(48), dict(runtime(49152), gpu_power_limit_watts=350))
    assert profiles.match(mod_target) is None

    preliminary = profiles.preliminary_match(dict(offer(), gpu_max_power_watts=350))
    assert preliminary['population_size'] == 24576
    assert preliminary['match_type'] == 'preliminary_nearest'
    assert preliminary['power_limit_delta_watts'] == 20


def test_new_ema_profile_wins_same_card_and_power_tie(tmp_path: Path) -> None:
    """A later protocol-3 test supersedes legacy evidence without deleting it."""
    identity = gpu_variant_identity(offer(), dict(runtime(24576), gpu_power_limit_watts=350))
    shipped_path = tmp_path / 'shipped.json'
    shipped_path.write_text(json.dumps({'schema': 1, 'profiles': []}))
    root = tmp_path / 'local'
    root.mkdir()
    rows = [
        {'id': marker * 64, 'gpu_variant_fingerprint': identity['fingerprint'],
         'population_size': population, 'hardware_identity': identity,
         'protocol': protocol}
        for marker, population, protocol in [('a', 8192, 2), ('b', 12288, 3)]
    ]
    (root / 'calibration_profiles.json').write_text(json.dumps({'schema': 1, 'profiles': rows}))
    profiles = CalibrationProfiles(root, shipped_path)

    assert [row['protocol'] for row in profiles.list()] == [2, 3]
    assert profiles.match(identity)['protocol'] == 3
    assert profiles.preliminary_match(dict(offer(), gpu_max_power_watts=350))['protocol'] == 3


def test_exact_profile_does_not_match_without_measured_runtime_power(tmp_path: Path) -> None:
    """A profile with the same unmeasured identity is not reusable evidence."""
    identity = gpu_variant_identity(offer(), runtime(24576))
    row = {'id': 'a' * 64, 'gpu_variant_fingerprint': identity['fingerprint'],
           'population_size': 8192, 'hardware_identity': identity}
    shipped_path = tmp_path / 'shipped.json'
    shipped_path.write_text(json.dumps({'schema': 1, 'profiles': [row]}))
    profiles = CalibrationProfiles(tmp_path / 'local', shipped_path)
    assert profiles.match(identity) is None

@pytest.mark.parametrize('rate8', [105.0, 120.0])
def test_adaptive_first_probe_always_advances_from_8k_to_12k(rate8: float) -> None:
    """Protocol 3 starts at 4k and always probes 12k after 8k."""
    cases = {population: {'valid': True, 'rate_per_second': rate}
             for population, rate in [(4096, 100), (8192, rate8)]}
    assert calibration_decision(cases)['next_population'] == 12288
    assert calibration_decision({})['next_population'] == 4096


def test_legacy_recommendation_remains_compatible() -> None:
    """Already-paid protocol-v1 evidence retains its original recommendation."""
    cases = {
        4096: {'valid': True, 'rate_per_second': 90},
        8192: {'valid': True, 'rate_per_second': 96},
        12288: {'valid': True, 'rate_per_second': 100},
        16384: {'valid': True, 'rate_per_second': 99},
    }
    assert recommended_population(cases, protocol=1) == 8192
    legacy_v2 = {1024: {'valid': True, 'rate_per_second': 40},
                 2048: {'valid': True, 'rate_per_second': 50}, **cases}
    assert recommended_population(legacy_v2, protocol=2, stop_reason='scaling_plateau') == 12288


@pytest.mark.parametrize(
    ('rate12', 'expected_next', 'recommended'),
    [(111.0, 16384, None), (109.0, None, 12288), (99.0, None, 8192)],
)
def test_linear_probe_uses_ten_percent_knee(rate12, expected_next, recommended) -> None:
    """Keep a positive plateau step, roll back a regression, and grow on ten percent."""
    cases = {population: {'valid': True, 'rate_per_second': rate}
             for population, rate in [(1024, 50), (2048, 75), (4096, 90), (8192, 100), (12288, rate12)]}
    assert calibration_decision(cases)['next_population'] == expected_next
    assert recommended_population(cases) == recommended


def test_workload_fingerprint_ignores_only_parallelism() -> None:
    """Population overrides compare directly while semantic workload changes do not."""
    first = {'live': {'approved_coins': {'long': ['BTC'], 'short': []}},
             'backtest': {'start_date': '2025-01-01', 'base_dir': 'a'},
             'optimize': {'population_size': 1024, 'n_cpus': 4,
                          'gpu': {'population_size': 1024, 'batch_size': 1024, 'exact_workers': 4}}}
    second = json.loads(json.dumps(first))
    second['optimize']['population_size'] = 16384
    second['optimize']['gpu'].update(population_size=16384, batch_size=16384, exact_workers=16)
    second['backtest']['base_dir'] = 'b'
    assert calibration_workload_fingerprint(first) == calibration_workload_fingerprint(second)
    second['live']['approved_coins']['long'].append('ETH')
    assert calibration_workload_fingerprint(first) != calibration_workload_fingerprint(second)


def test_canonical_workload_is_fixed_and_does_not_use_user_jobs() -> None:
    """PBGui derives one versioned comparison input from PB8's installed schema."""
    template = {
        'live': {'approved_coins': {'long': ['DOGE'], 'short': []}, 'ignored_coins': {}, 'user': 'alice'},
        'backtest': {'start_date': '2026-01-01', 'end_date': '2026-02-01', 'exchanges': ['bybit']},
        'optimize': {'backend': 'pymoo', 'scoring': [], 'gpu': {}},
    }
    metadata = {
        'strategies': ['ema_anchor'],
        'strategy_defaults': {
            'long': {'ema_anchor': {'ema_span_0': 12}},
            'short': {'ema_anchor': {'ema_span_0': 12}},
        },
        'active_bounds': {'ema_anchor': {
            'long': {'strategy': {'ema_anchor': {'ema_span_0': [5, 50, 1]}}},
            'short': {'strategy': {'ema_anchor': {'ema_span_0': [5, 50, 1]}}},
        }},
    }
    result = canonical_calibration_config(template, optimize_metadata=metadata)
    assert result['live']['strategy_kind'] == 'ema_anchor'
    assert result['bot']['long']['strategy'] == {'ema_anchor': {'ema_span_0': 12}}
    assert result['optimize']['bounds']['short']['strategy'] == metadata['active_bounds']['ema_anchor']['short']['strategy']
    assert result['optimize']['gpu']['population_size'] == 4096
    assert result['pbgui']['calibration_workload'] == WORKLOAD_VERSION
    assert result['live']['approved_coins']['long'] == ['BTC', 'ETH', 'SOL']
    assert result['backtest']['exchanges'] == ['binance']
    assert result['backtest']['start_date'] == '2024-01-01'
    assert result['optimize']['seed'] == 7
    assert result['optimize']['scoring'] == [{'metric': 'adg_strategy_eq', 'goal': 'max'}]
    assert result['optimize']['bounds']['long']['risk']['n_positions'] == [1, 3, 1]
    assert result['optimize']['bounds']['short']['risk']['n_positions'] == [1, 3, 1]
    assert template['live']['user'] == 'alice'


def test_local_profile_precedes_shipped_reference(tmp_path: Path) -> None:
    """An accepted exact local profile overrides but never edits bundled evidence."""
    identity = gpu_variant_identity(offer(), dict(runtime(24576), gpu_power_limit_watts=170))
    shipped_path = tmp_path / 'shipped.json'
    shipped = {
        'id': 'a' * 64, 'gpu_variant_fingerprint': identity['fingerprint'],
        'workload_fingerprint': 'b' * 64, 'population_size': 8192,
        'hardware_identity': identity, 'protocol': 1,
    }
    shipped_path.write_text(json.dumps({'schema': 1, 'profiles': [shipped]}))
    profiles = CalibrationProfiles(tmp_path / 'local', shipped_path)
    local = dict(shipped, id='c' * 64, population_size=12288)
    profiles.accept(local)
    assert profiles.match(identity, 'b' * 64)['population_size'] == 12288
    assert json.loads(shipped_path.read_text())['profiles'][0]['population_size'] == 8192


def test_finalize_requires_runtime_identity_and_recomputes_recommendation(tmp_path: Path, monkeypatch) -> None:
    """Downloaded worker evidence becomes pending only after local validation."""
    from secure_files import ensure_private_directory
    from vast_jobs import IMAGE, REVISION, JobStore, write_json

    store = JobStore(tmp_path / 'vast')
    worker_id, job_id = 'd' * 32, 'e' * 32
    worker = ensure_private_directory(store.root / 'jobs' / worker_id)
    write_json(worker / 'state.json', {'id': worker_id, 'kind': 'worker', 'status': 'running',
                                       'rental_state': 'active'})
    write_json(worker / 'intent.json', {'id': worker_id, 'offer': offer()})
    job = ensure_private_directory(store.root / 'jobs' / job_id)
    ensure_private_directory(job / 'input')
    (job / 'input/optimize.json').write_text('{}')
    write_json(job / 'state.json', {'id': job_id, 'kind': 'calibration', 'status': 'collecting',
                                    'rental_state': 'none', 'lease_id': worker_id,
                                    'runtime_metrics': {'gpu_power_limit_watts': 200.0}})
    write_json(job / 'intent.json', {'id': job_id, 'image': IMAGE, 'pb8_revision': REVISION})
    final = ensure_private_directory(job / 'final-results')
    write_json(final / 'calibration-result.json', {
        'schema': 1, 'protocol': 1, 'status': 'completed',
        'hardware': runtime(24576), 'population_size': 8192, 'finished_at': 1000,
        'cases': {
            '4096': {'population_size': 4096, 'valid': True, 'rate_per_second': 90},
            '8192': {'population_size': 8192, 'valid': True, 'rate_per_second': 96},
            '12288': {'population_size': 12288, 'valid': True, 'rate_per_second': 100},
        },
    })
    monkeypatch.setattr('pb8_config.load_pb8_config', lambda _path: {
        'live': {'approved_coins': {'long': ['BTC'], 'short': []}},
        'backtest': {'start_date': '2025-01-01'}, 'optimize': {'gpu': {}},
    })
    candidate = finalize_calibration_result(store, job_id)
    assert candidate['population_size'] == 8192
    assert candidate['hardware_identity']['runtime_verified'] is True
    assert candidate['hardware_identity']['gpu_power_limit_watts'] == 200.0
    assert candidate['hardware_identity']['gpu_power_limit_source'] == 'runtime_nvidia_smi'
    assert len(candidate['workload_fingerprint']) == 64
    assert 'max_dispatch_candidate_bars' not in candidate
    measured_work = 20_713_476_096
    cases = {
        str(population): {
            'population_size': population, 'valid': True, 'rate_per_second': rate,
            'dispatch_batch_size': population,
            'candidate_bars': measured_work * population // 12288,
            'tested_dispatch_limit': 40_000_000_000,
        }
        for population, rate in ((4096, 50), (8192, 60), (12288, 63))
    }
    write_json(final / 'calibration-result.json', {
        'schema': 1, 'protocol': 3, 'status': 'completed',
        'hardware': runtime(24576), 'population_size': 12288,
        'stop_reason': 'scaling_plateau', 'finished_at': 1001, 'cases': cases,
    })
    candidate = finalize_calibration_result(store, job_id)
    assert candidate['max_dispatch_candidate_bars'] == measured_work
    protocol4_plan = configurable_calibration_plan(5632, 512, 7168, .05, 300, 3600)
    state = store.read(job_id)
    state['calibration_plan'] = protocol4_plan
    write_json(job / 'state.json', state)
    protocol4_cases = {
        str(population): {
            'population_size': population, 'valid': True, 'rate_per_second': rate,
            'dispatch_batch_size': population, 'all_dispatches_full': True,
            'candidate_bars': work, 'tested_dispatch_limit': 20_000_000,
        }
        for population, rate, work in ((5632, 100, 7_000_000), (6144, 103, 8_000_000))
    }
    write_json(final / 'calibration-result.json', {
        'schema': 1, 'protocol': 4, 'status': 'completed',
        'hardware': runtime(24576), 'population_size': 6144,
        'stop_reason': 'scaling_plateau', 'finished_at': 1002, 'cases': protocol4_cases,
    })
    candidate = finalize_calibration_result(store, job_id)
    assert candidate['max_dispatch_candidate_bars'] == 8_000_000
    assert candidate['population_size'] == 6144
    protocol4_cases['6144']['all_dispatches_full'] = False
    write_json(final / 'calibration-result.json', {
        'schema': 1, 'protocol': 4, 'status': 'completed',
        'hardware': runtime(24576), 'population_size': 6144,
        'stop_reason': 'scaling_plateau', 'finished_at': 1002, 'cases': protocol4_cases,
    })
    with pytest.raises(ValueError, match='full population dispatch'):
        finalize_calibration_result(store, job_id)
    cases['12288']['dispatch_batch_size'] = 1186
    write_json(final / 'calibration-result.json', {
        'schema': 1, 'protocol': 3, 'status': 'completed',
        'hardware': runtime(24576), 'population_size': 12288,
        'stop_reason': 'scaling_plateau', 'finished_at': 1001, 'cases': cases,
    })
    with pytest.raises(ValueError, match='full population dispatch'):
        finalize_calibration_result(store, job_id)
    cases['12288']['dispatch_batch_size'] = 12288
    write_json(final / 'calibration-result.json', {
        'schema': 1, 'protocol': 3, 'status': 'completed',
        'hardware': runtime(24576), 'population_size': 12288,
        'stop_reason': 'scaling_plateau', 'finished_at': 1001, 'cases': cases,
    })
    write_json(job / 'state.json', {'id': job_id, 'kind': 'calibration', 'status': 'collecting',
                                    'rental_state': 'none', 'lease_id': worker_id,
                                    'runtime_metrics': {}})
    with pytest.raises(ValueError, match='nvidia-smi'):
        finalize_calibration_result(store, job_id)

@pytest.mark.parametrize(
    ('rates', 'recommended', 'next_population'),
    [
        ((100.0, 106.0), None, 6656),
        ((100.0, 103.0), 6144, None),
        ((100.0, 98.0), 5632, None),
    ],
)
def test_configurable_plan_uses_measured_half_k_steps(
    rates: tuple[float, float], recommended: int | None, next_population: int | None
) -> None:
    """Protocol 4 retains a positive plateau step and rolls back only a regression."""
    plan = configurable_calibration_plan(5632, 512, 7168, .05, 300, 3600)
    cases = {population: {'valid': True, 'rate_per_second': rate}
             for population, rate in zip((5632, 6144), rates)}
    decision = calibration_decision(cases, protocol=4, plan=plan)
    assert decision['recommended_population'] == recommended
    assert decision['next_population'] == next_population


def test_configurable_plan_rejects_unbounded_or_too_short_cases() -> None:
    """Search parameters are validated before a provider rental is requested."""
    with pytest.raises(ValueError, match='Invalid configurable'):
        configurable_calibration_plan(5632, 512, 131073, .05, 300, 3600)
    with pytest.raises(ValueError, match='Invalid configurable'):
        configurable_calibration_plan(5632, 512, 7168, .05, 300, 390)

@pytest.mark.parametrize(('preset', 'coins', 'exchanges', 'scenarios'), [
    ('small', 3, 1, 1), ('medium', 10, 2, 2), ('large', 41, 2, 4),
])
def test_preset_workloads_have_fixed_distinct_shapes(
    monkeypatch, preset: str, coins: int, exchanges: int, scenarios: int
) -> None:
    """Reference sizes are generated by PBGui, not taken from arbitrary user jobs."""
    from vast_calibration import preset_calibration_config

    def fake_canonical(_template: dict, *, optimize_metadata: dict | None = None) -> dict:
        """Supply only the canonical fields modified by a preset."""
        return {'live': {'approved_coins': {}}, 'backtest': {}, 'pbgui': {}}

    monkeypatch.setattr('vast_calibration.canonical_calibration_config', fake_canonical)
    config = preset_calibration_config({}, preset)
    assert len(config['live']['approved_coins']['long']) == coins
    assert len(config['backtest']['exchanges']) == exchanges
    assert len(config['backtest']['scenarios']) == scenarios
    assert config['pbgui']['calibration_workload'].endswith('-' + preset)


def test_configurable_api_does_not_rent_without_pinned_worker(monkeypatch) -> None:
    """An unpublished protocol-4 image is rejected before any provider access."""
    from fastapi import HTTPException
    from api.vast import ConfigurableCalibrationRequest, start_configurable_calibration

    monkeypatch.setattr('vast_calibration.CALIBRATION_WORKER_DIGEST', None)
    body = ConfigurableCalibrationRequest(
        offer=dict(offer(), price_hour_usd=.25), hours=1, budget=1,
        accept_rental_and_cleanup=True,
    )
    with pytest.raises(HTTPException) as error:
        start_configurable_calibration(body, session=None)
    assert error.value.status_code == 409
    assert 'no GPU was rented' in error.value.detail

def test_one_worker_supports_both_calibration_protocols(monkeypatch) -> None:
    """The pinned worker image accepts fixed and configurable tests."""
    from vast_calibration import calibration_worker_available

    digest = 'f' * 64
    monkeypatch.setattr('vast_calibration.CALIBRATION_WORKER_DIGEST', digest)
    image = 'ghcr.io/msei99/pbgui-pb8-worker@sha256:' + digest
    assert calibration_worker_available(image)
    assert calibration_worker_available(image, protocol=4)
    assert not calibration_worker_available('ghcr.io/msei99/pbgui-pb8-worker@sha256:' + 'e' * 64,
                                            protocol=4)
