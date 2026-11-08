"""Controller-owned random seeds and seed-free model evidence."""
from __future__ import annotations

import secrets

SERVICE = 'PB8LoopRNG'
VISIBILITY_VERSION = 1


def allocate_seed(record):
    """Draw only at durable job creation, avoiding previous/planned seeds in this run."""
    used = {record.get('initial_config', {}).get('optimize', {}).get('seed')}
    for job in record.get('jobs', []):
        used.add(job.get('config', {}).get('optimize', {}).get('seed'))
        used.add((job.get('executed_config') or {}).get('optimize', {}).get('seed'))
        used.add((job.get('research') or {}).get('seed'))
    seed = secrets.randbelow(2**31)
    while seed in used:
        seed = secrets.randbelow(2**31)
    return seed


def seed_path(path):
    """Recognize native seed fields and historical seed-index metadata."""
    if isinstance(path, list):
        return bool(path) and seed_path(str(path[-1]))
    return isinstance(path, str) and any(part in {
        'seed', 'seed_index', 'seeds', 'paired_seed_policy', 'independent_seed_count',
        'measured_seed_count', 'successful_seed_rate'} for part in path.replace('/', '.').split('.'))


def model_view(value, path=''):
    """Mask RNG values in bounded model records, including old native-config deltas."""
    if seed_path(path) and not isinstance(value, dict):
        return None
    if isinstance(value, dict):
        is_seed_delta = seed_path(value.get('path'))
        return {key: (None if is_seed_delta and key in {'value', 'before', 'after'}
                      else model_view(child, key)) for key, child in value.items()}
    if isinstance(value, list):
        return [model_view(child) for child in value]
    return value
