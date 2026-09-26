"""Offline checks for explicit GPU sizing and narrow measured suggestions."""

from copy import deepcopy
import json

import pytest

import vast_gpu_recommendation as recommendation
from vast_gpu_tuning import resolve_gpu_settings


def workload():
    """Return a minimal fixed workload with explicit GPU dispatch settings."""
    return {
        'live': {'approved_coins': {'long': ['BTC'], 'short': []}},
        'backtest': {'start_date': '2024-01-01', 'end_date': '2024-01-31'},
        'bot': {'long': {'risk': {'n_positions': 1}}, 'short': {'risk': {'n_positions': 0}}},
        'optimize': {'backend': 'gpu', 'iters': 512, 'n_cpus': 8,
                     'gpu': {'population_size': 7168, 'batch_size': 7168,
                             'max_dispatch_candidate_bars': 130_170_880_000}},
    }


def test_fingerprint_ignores_sizing_and_runtime_but_not_workload():
    """One measured workload does not silently become a generic GPU profile."""
    original = workload()
    changed = deepcopy(original)
    changed['pbgui'] = {'name': 'some other job'}
    changed['optimize'].update(iters=1024, population_size=8192, n_cpus=23)
    changed['optimize']['gpu'].update(population_size=8192, batch_size=8192,
                                       max_dispatch_candidate_bars=200_000_000_000)
    changed['backtest']['ohlcv_source_dir'] = '/a/different/path'
    assert recommendation.workload_fingerprint(changed) == recommendation.workload_fingerprint(original)
    changed['live']['approved_coins']['long'].append('ETH')
    assert recommendation.workload_fingerprint(changed) != recommendation.workload_fingerprint(original)


@pytest.mark.parametrize(('population', 'batch', 'cap', 'expected_batch', 'expected_chunks'), [
    (7168, 7168, 130_170_880_000, 7168, 1),
    (7168, 4096, 130_170_880_000, 4096, 2),
    (7168, 7168, 65_085_440_000, 3584, 2),
])
def test_dispatch_preview_respects_all_three_manual_limits(
    population, batch, cap, expected_batch, expected_chunks
):
    """Preview the binding population, batch, and work-cap constraints."""
    config = workload()
    config['optimize']['gpu'].update(population_size=population, batch_size=batch,
                                      max_dispatch_candidate_bars=cap)
    preview = recommendation.dispatch_preview(config, measured_candidate_bars=18_160_000)
    assert preview['effective_batch_estimate'] == expected_batch
    assert preview['dispatches_per_largest_scenario_estimate'] == expected_chunks


def test_measured_suggestion_requires_workload_power_and_vram(monkeypatch, tmp_path):
    """Only the exact workload and advertised hardware class may use a profile."""
    config = workload()
    payload = {'schema': 1, 'profiles': [{
        'workload_fingerprint': recommendation.workload_fingerprint(config),
        'gpu_name_key': 'rtx3060', 'vram_gb': 12.0,
        'measured_power_limit_watts': 170,
        'population_size': 7168, 'batch_size': 7168,
        'max_dispatch_candidate_bars': 130_170_880_000,
        'candidate_bars_per_candidate': 18_160_000,
        'candidates_per_second': 3.565,
    }]}
    path = tmp_path / 'profiles.json'
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(recommendation, 'REFERENCE_PATH', path)
    offer = {'gpu_name': 'RTX 3060', 'vram_gb': 12.0, 'gpu_max_power_watts': 170}
    assert recommendation.measured_suggestion(config, offer)['profile']['population_size'] == 7168
    for changed_offer in ({**offer, 'gpu_max_power_watts': 200},
                          {**offer, 'vram_gb': 24},
                          {**offer, 'gpu_name': 'RTX 3090'}):
        assert recommendation.measured_suggestion(config, changed_offer)['profile'] is None
    other_workload = deepcopy(config)
    other_workload['live']['approved_coins']['long'].append('ETH')
    assert recommendation.measured_suggestion(other_workload, offer)['profile'] is None


def test_resolver_preserves_explicit_gpu_sizing_and_audits_batch():
    """Generic optimize.population_size cannot override explicit CUDA sizing."""
    config = workload()
    config['optimize']['population_size'] = 2048
    config['optimize']['gpu']['batch_size'] = 4096
    state = {'workers': 8, 'hardware': {'gpu': 'RTX 3060', 'vram_bytes': 12 * 1024**3},
             'runtime_metrics': {'gpu_power_limit_watts': 170}}
    record = resolve_gpu_settings(config, state, {'gpu_name': 'RTX 3060', 'vram_gb': 12.0})
    assert config['optimize']['gpu']['population_size'] == 7168
    assert config['optimize']['gpu']['batch_size'] == 4096
    assert config['optimize']['gpu']['max_dispatch_candidate_bars'] == 130_170_880_000
    assert record['preserved']['population_size'] == 7168
    assert record['preserved']['batch_size'] == 4096
    assert record['dispatch_candidates'] == 4096
    assert record['dispatch_chunks'] == 2

def test_matching_measured_profiles_retains_power_limit_evidence(tmp_path):
    """Exact workload measurements remain selectable when offered watts differ."""
    config = workload()
    fingerprint = recommendation.workload_fingerprint(config)
    profiles = {'schema': 1, 'profiles': [{
        'workload_fingerprint': fingerprint, 'gpu_name_key': 'rtx3060',
        'vram_gb': 12.0, 'measured_power_limit_watts': 170,
        'population_size': 7168, 'batch_size': 7168,
        'max_dispatch_candidate_bars': 130_170_880_000,
        'candidates_per_second': 3.565, 'peak_vram_gib': 6.62,
    }]}
    path = tmp_path / 'profiles.json'
    path.write_text(json.dumps(profiles))
    offer = {'gpu_name': 'RTX 3060', 'vram_gb': 12, 'gpu_max_power_watts': 160}
    matches = recommendation.matching_measured_profiles(config, offer, path)
    assert len(matches) == 1
    assert matches[0]['measured_power_limit_watts'] == 170
    assert matches[0]['offered_power_limit_watts'] == 160
    assert matches[0]['max_dispatch_candidate_bars'] == 130_170_880_000
    assert recommendation.matching_measured_profiles(config, {**offer, 'vram_gb': 24}, path) == []


def test_queued_gpu_previews_are_computed_per_waiting_job(tmp_path, monkeypatch):
    """Two prepared jobs receive independent Auto settings for one selected offer."""
    from types import SimpleNamespace
    import pb8_config
    import vast_gpu_tuning

    identifiers = ('a' * 32, 'b' * 32)
    configs = {identifiers[0]: {'test_bars': 18_000_000, 'optimize': {'gpu': {}}},
               identifiers[1]: {'test_bars': 3_000_000, 'optimize': {'gpu': {}}}}
    for identifier in identifiers:
        path = tmp_path / identifier / 'input' / 'optimize.json'
        path.parent.mkdir(parents=True)
        path.write_text('{}')
    monkeypatch.setattr(pb8_config, 'load_pb8_config',
                        lambda path: deepcopy(configs[path.parents[1].name]))
    def fake_resolve(config, _state, _offer):
        """Give each synthetic workload its own computed work cap."""
        bars = config['test_bars']
        config['optimize']['gpu'].update(population_size=8192, batch_size=8192,
                                          max_dispatch_candidate_bars=8192 * bars)
        return {'automatic': {'population_size': 8192}, 'largest_candidate_bars': bars}
    monkeypatch.setattr(vast_gpu_tuning, 'resolve_gpu_settings', fake_resolve)
    waiting = [{'id': identifier, 'config_name': 'job-' + identifier[0],
                'estimated_coin_candles': configs[identifier]['test_bars'], 'workers': 8}
               for identifier in identifiers]
    queue = SimpleNamespace(waiting=lambda: waiting,
                            store=SimpleNamespace(directory=lambda identifier: tmp_path / identifier))
    result = recommendation.queued_gpu_previews(queue,
        {'gpu_name': 'RTX 3060', 'vram_gb': 12, 'gpu_max_power_watts': 170})
    assert result['total_jobs'] == 2
    assert [row['auto']['max_dispatch_candidate_bars'] for row in result['jobs']] == [
        147_456_000_000, 24_576_000_000]
    assert all(row['mode'] == 'auto' for row in result['jobs'])
