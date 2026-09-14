"""Offline tests for generation-level CPU backlog observations."""

import json

import pytest

from vast_exact_queue import parse_exact_queue


def profile(**values):
    """Build a real-format PB8 profile line."""
    return ('2026-09-13T22:30:00Z INFO [gpu-profile] ' + json.dumps(
        dict(event='generation', exact_inflight=18, exact_completed=120, **values))).encode()


def test_profile_preserves_observation_time_and_count():
    """Repeated fetches cannot make an old generation measurement fresh."""
    result = parse_exact_queue(profile() + b'\npartial [gpu-profile] {')
    assert result == {'outstanding': 18, 'completed': 120, 'sampled_at': 1789338600.0}
    assert parse_exact_queue(profile()) == result


@pytest.mark.parametrize('value', [-1, True, None, '18', 1.5])
def test_invalid_counts_are_not_zero(value):
    """Malformed telemetry must not manufacture a zero queue."""
    line = profile().decode().replace('"exact_inflight": 18', '"exact_inflight": ' + json.dumps(value))
    assert parse_exact_queue(line.encode()) is None


def test_newest_complete_generation_wins():
    """The selected sample includes zero and ignores unrelated profile events."""
    zero = profile().replace(b'"exact_inflight": 18', b'"exact_inflight": 0')
    unrelated = profile().replace(b'"generation"', b'"kernel"')
    assert parse_exact_queue(profile() + b'\n' + zero + b'\n' + unrelated)['outstanding'] == 0
