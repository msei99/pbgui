"""Offline statistics checks against native GPU counter log formats."""

import pytest

from vast_throughput import parse_throughput


def line(seconds, proxy, exact, *, complete=False):
    """Emit a native counter record with a deterministic UTC observation time."""
    prefix = f"2026-09-16T10:{seconds // 60:02}:{seconds % 60:02}Z INFO "
    text = (f"GPU optimization complete | generations=100 proxy={proxy} exact={exact}" if complete
            else f"GPU optimize | gen=100 proxy={proxy} (9999.0/s) exact={exact} inflight=4")
    return (prefix + text + '\n').encode()


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
    assert result['proxy_per_minute'] is None
    assert result['exact_per_minute'] is None


def test_zero_work_is_a_valid_measurement_and_zero_exact_has_no_ratio():
    """Measured idle intervals are zero; division by zero is unavailable."""
    result = parse_throughput(line(0, 100, 0) + line(60, 100, 0))
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
    raw = line(0, 100, 10) + b'x' * 65536 + b'\n' + line(60, 200, 20)
    assert parse_throughput(raw)['proxy_per_minute'] is None


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
    assert reset['proxy_per_minute'] is None
    assert len(reset['samples']) == 1


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
