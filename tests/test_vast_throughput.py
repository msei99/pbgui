"""Offline statistics checks against native GPU counter log formats."""

import json

import pytest

from vast_throughput import parse_throughput


def line(seconds, proxy, exact, *, complete=False, reported_rate=9999.0):
    """Emit a native counter record with a deterministic UTC observation time."""
    prefix = f"2026-09-16T10:{seconds // 60:02}:{seconds % 60:02}Z INFO "
    text = (f"GPU optimization complete | generations=100 proxy={proxy} exact={exact}" if complete
            else f"GPU optimize | gen=100 proxy={proxy} ({reported_rate}/s) exact={exact} inflight=4")
    return (prefix + text + '\n').encode()


def modern_line(seconds, proxy, exact, phase='generation_complete'):
    """Emit current PB8 progress with separate seed and completed evolution counters."""
    return (f'2026-09-16T10:{seconds // 60:02}:{seconds % 60:02}Z INFO '
            f'GPU optimizer progress | gen=2 phase={phase} | '
            f'evolution_proxy_completed_run={proxy} seed_proxy=999 seed_exact=99 '
            f'evolution_exact={exact}/10000000 evolution_pending=64 front=47\n').encode()


def test_current_pb8_progress_measures_completed_evolution_work():
    """Current progress uses logged work rather than seeds, pending tasks or population."""
    raw=modern_line(0,8192,64)+b'2026-09-16T10:00:00Z INFO GPU optimizer progress | gen=2 phase=generation_complete | run_elapsed=58s\n'+modern_line(60,16384,128)
    result=parse_throughput(raw)
    assert result['proxy_total']==16384 and result['exact_total']==128
    assert result['proxy_per_minute']==8192 and result['exact_per_minute']==64
    assert result['proxy_per_exact']==128 and result['window_seconds']==60


def test_current_progress_phases_resume_completion_and_legacy_logs():
    """Phase changes and terminal progress retain compatible timestamped histories."""
    previous=parse_throughput(line(0,100,10))
    current=parse_throughput(modern_line(60,700,40,'gpu_proxy'),previous)
    assert current['proxy_per_minute']==600 and current['exact_per_minute']==30
    assert parse_throughput(modern_line(0,100,10),current)==current
    final=parse_throughput(modern_line(120,1300,70,'complete'),current)
    assert final['proxy_per_minute']==600 and final['exact_per_minute']==30
    assert parse_throughput(modern_line(120,1300,70,'complete'),final)==final
    reset=parse_throughput(modern_line(150,0,0),final)
    assert reset['proxy_per_minute'] is None and reset['exact_per_minute'] is None
    assert len(reset['samples'])==1


@pytest.mark.parametrize('counters',[
    'evolution_proxy_completed_run=8192',
    'evolution_exact=64/10000000',
    'population=8192 seed_proxy=8192 seed_exact=64 evolution_pending=64',
    'evolution_proxy_completed_run=-1 evolution_exact=64/10000000',
    'evolution_proxy_completed_run=1.5 evolution_exact=64/10000000',
    'evolution_proxy_completed_run=8192 evolution_exact=-1/10000000',
    'evolution_proxy_completed_run=8192 evolution_exact=1.5/10000000',
    f'evolution_proxy_completed_run={2**54} evolution_exact=64/10000000',
])
def test_current_progress_rejects_partial_invalid_and_estimated_work(counters):
    """Only complete browser-safe native count pairs can establish a measurement."""
    assert parse_throughput(('2026-09-16T10:00:00Z INFO GPU optimizer progress | gen=1 phase=gpu_proxy | '+counters+'\n').encode()) is None


def test_rates_use_counter_deltas_not_reported_rate_or_total_runtime():
    """Use the last minute of actual counters, including nonzero resume baselines."""
    result = parse_throughput(line(0, 10000, 100) + line(30, 14000, 120)
                              + line(60, 17000, 130) + line(90, 20000, 150))
    assert result['window_seconds'] == 60
    assert result['proxy_per_minute'] == 6000
    assert result['exact_per_minute'] == 30
    assert result['proxy_per_exact'] == pytest.approx(20000 / 150)


def test_repeated_logs_and_partial_records_do_not_refresh_sample():
    """Refetching unchanged output preserves the timestamp and ignores incomplete data."""
    raw = line(0, 0, 0) + line(60, 600, 6)
    result = parse_throughput(raw)
    assert parse_throughput(raw + b'2026-09-16T10:01:20Z INFO GPU optimize | gen=100 proxy=') == result
    assert parse_throughput(raw) == result


@pytest.mark.parametrize('raw', [b'', b'[gpu-profile] {"generation":100,"population_size":1000}',
    b'2026-99-16T10:00:00Z INFO GPU optimize | gen=1 proxy=30 (2.0/s) exact=1'])
def test_missing_or_invalid_native_counters_remain_unknown(raw):
    """Population estimates and broken timestamps must not become measured work."""
    assert parse_throughput(raw) is None


@pytest.mark.parametrize('raw', [line(0, 100, 10), line(0, 100, 10) + line(5, 200, 20),
    line(0, 100, 10) + line(0, 200, 20), line(0, 100, 10) + line(60, 50, 5),
    line(60, 100, 10) + line(0, 200, 20)])
def test_insufficient_intervals_or_resets_do_not_fabricate_rates(raw):
    """Handle initial observations, clock rollback, same-second records and restarts."""
    result = parse_throughput(raw)
    assert result['proxy_per_minute'] == 9999 * 60
    assert result['exact_per_minute'] is None


def test_zero_work_is_a_valid_measurement_and_zero_exact_has_no_ratio():
    """Measured idle intervals are zero; division by zero is unavailable."""
    result = parse_throughput(line(0, 100, 0, reported_rate=0.0) + line(60, 100, 0, reported_rate=0.0))
    assert result['proxy_per_minute'] == result['exact_per_minute'] == 0
    assert result['proxy_per_exact'] is None


def test_completion_and_sparse_log_interval_are_preserved():
    """A final counter line measures a real longer interval without extrapolating a minute."""
    result = parse_throughput(line(0, 100, 10) + line(180, 1000, 100, complete=True))
    assert result['window_seconds'] == 180
    assert result['proxy_per_minute'] == 300
    assert result['exact_per_minute'] == 30


def test_counter_input_is_bounded_and_browser_safe():
    """Ignore oversized counts and old samples outside the bounded tail."""
    assert parse_throughput(line(0, 2**54, 1)) is None
    raw = line(0, 100, 10) + b'x' * (512 * 1024) + b'\n' + line(60, 200, 20)
    assert parse_throughput(raw)['exact_per_minute'] is None


def test_counter_pair_survives_more_than_64_kib_of_intervening_log():
    """Read the longer synchronized tail to retain a valid earlier baseline."""
    raw = line(0, 100, 10) + b'x' * (70 * 1024) + b'\n' + line(60, 700, 40)
    result = parse_throughput(raw)
    assert result['proxy_per_minute'] == 600
    assert result['exact_per_minute'] == 30


def test_disjoint_tails_preserve_rates_and_reject_delayed_snapshots():
    """A replacement log tail can be joined to earlier counters without refreshing old data."""
    first = parse_throughput(line(0, 100, 10))
    second = parse_throughput(line(60, 700, 40), first)
    assert second['proxy_per_minute'] == 600
    assert second['exact_per_minute'] == 30
    assert parse_throughput(line(0, 100, 10), second) == second
    assert parse_throughput(line(60, 700, 40), second) == second
    assert parse_throughput(b'partial output', second) == second
    reset = parse_throughput(line(90, 10, 0), second)
    assert reset['proxy_per_minute'] == 9999 * 60
    assert len(reset['samples']) == 1


def test_native_exact_progress_and_last_proxy_rate_survive_sparse_gpu_summaries():
    """One native GPU rate and frequent CPU completions remain useful between generations."""
    def progress(seconds, exact):
        """Emit one cumulative native Exact progress event."""
        stamp = f"2026-09-16T10:{seconds // 60:02}:{seconds % 60:02}Z INFO [gpu-profile] "
        return (stamp + json.dumps({'event': 'exact_progress', 'exact_completed': exact}) + '\n').encode()

    first = parse_throughput(line(0, 8192, 0, reported_rate=47.1)
                             + progress(30, 10) + progress(60, 30))
    assert first['proxy_total'] == 8192
    assert first['exact_total'] == 30
    assert first['proxy_per_minute'] == 2826
    assert first['exact_per_minute'] == 30
    assert first['proxy_per_exact'] == pytest.approx(8192 / 30)
    second = parse_throughput(progress(90, 45), first)
    assert second['proxy_per_minute'] == 2826
    assert second['exact_per_minute'] == 35
    assert second['exact_total'] == 45
    stalled = parse_throughput(progress(180, 45), second)
    assert stalled['proxy_per_minute'] == 2826
    assert stalled['exact_per_minute'] == 35  # Keep the last valid rate during a quiet interval.


def test_history_is_bounded_across_many_snapshots():
    """The persisted job record cannot grow with run duration."""
    result = None
    for second in range(200):
        result = parse_throughput(line(second, second * 100, second), result)
    assert len(result['samples']) == 128
    assert result['window_seconds'] == 60
    assert result['proxy_per_minute'] == 6000


def test_observations_survive_store_reconstruction_without_duplicate_writes(tmp_path):
    """Repeated polls are read-only; new measurements preserve unrelated job state."""
    from secure_files import ensure_private_directory
    from vast_jobs import JobStore, write_json
    from vast_throughput import observe_throughput
    store = JobStore(tmp_path)
    identifier = 'd' * 32
    folder = ensure_private_directory(tmp_path / 'jobs' / identifier)
    write_json(folder / 'state.json', {'id':identifier, 'status':'running'})
    first = observe_throughput(store, identifier, line(0, 100, 10))
    assert 'samples' not in first
    generation = store.read(identifier)['generation']
    assert observe_throughput(store, identifier, line(0, 100, 10)) == first
    assert store.read(identifier)['generation'] == generation
    store.update(identifier, stop_requested=True)
    second = observe_throughput(JobStore(tmp_path), identifier, line(60, 700, 40))
    assert second['proxy_per_minute'] == 600
    assert store.read(identifier)['stop_requested'] is True
    assert store.read(identifier)['status'] == 'running'


def test_overlapping_multi_line_tails_keep_earlier_persisted_baseline():
    """Parsing several current lines must not shadow the previous persisted history."""
    previous = parse_throughput(line(0, 0, 0) + line(30, 300, 30))
    result = parse_throughput(line(30, 300, 30) + line(60, 600, 60), previous)
    assert result['window_seconds'] == 60
    assert len(result['samples']) == 3
    assert result['proxy_per_minute'] == 600
