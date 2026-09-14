"""Bounded, read-only utilization sampling for an existing GPU worker."""

import json
import math
import shlex
import time

from logging_helpers import human_log as _log
from vast_provider import VastError

SERVICE = 'VastRunner'
PROBE = r'''
import json, os, time, subprocess
from pathlib import Path
r = {'clock': time.monotonic()}
try:
    cores = float(len(os.sched_getaffinity(0)))
    root = Path('/sys/fs/cgroup')
    if (root / 'cpu.stat').exists():
        stat = dict(line.split() for line in (root / 'cpu.stat').read_text().splitlines())
        r['cpu_seconds'] = int(stat['usage_usec']) / 1e6
        quota, period = (root / 'cpu.max').read_text().split()
        if quota != 'max': cores = min(cores, int(quota) / int(period))
    else:
        r['cpu_seconds'] = int((root / 'cpuacct/cpuacct.usage').read_text()) / 1e9
        quota = int((root / 'cpu/cpu.cfs_quota_us').read_text())
        period = int((root / 'cpu/cpu.cfs_period_us').read_text())
        if quota > 0: cores = min(cores, quota / period)
    r['cpu_cores'] = cores
except (OSError, ValueError, KeyError): pass
try:
    root = Path('/sys/fs/cgroup')
    current = root / 'memory.current'
    limit = root / 'memory.max'
    if not current.exists():
        current = root / 'memory/memory.usage_in_bytes'
        limit = root / 'memory/memory.limit_in_bytes'
    used = int(current.read_text()); maximum = limit.read_text().strip()
    host_total = next(int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:'))
    r['ram_used_bytes'] = used
    r['ram_total_bytes'] = host_total if maximum == 'max' else min(int(maximum), host_total)
except (OSError, ValueError, StopIteration): pass
try:
    p = subprocess.run(['nvidia-smi', '--query-gpu=utilization.gpu,memory.used,memory.total', '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=3)
    if p.returncode == 0:
        values = p.stdout.splitlines()[0].split(',')
        for key, value, scale in zip(['gpu_percent','vram_used_bytes','vram_total_bytes'], values, [1,1048576,1048576]):
            try: r[key] = float(value.strip()) * scale
            except ValueError: pass
except (OSError, subprocess.TimeoutExpired, IndexError): pass
print(json.dumps(r, allow_nan=False))
'''


def normalize_sample(raw, previous, now):
    """Whitelist finite values and derive CPU percent against allocated capacity."""
    result = {'sampled_at': now, 'available': True}
    for key in ('clock', 'cpu_seconds', 'cpu_cores', 'gpu_percent', 'ram_used_bytes',
                'ram_total_bytes', 'vram_used_bytes', 'vram_total_bytes'):
        value = raw.get(key)
        if type(value) in (int, float) and math.isfinite(value) and value >= 0:
            result[key] = value
    if result.get('gpu_percent', 0) > 100:
        result.pop('gpu_percent')
    old = previous or {}
    elapsed = result.get('clock', 0) - old.get('clock', 0)
    consumed = result.get('cpu_seconds', 0) - old.get('cpu_seconds', 0)
    if ('cpu_seconds' in old and 'cpu_seconds' in result and elapsed > 0 and consumed >= 0
            and result.get('cpu_cores', 0) > 0 and result.get('cpu_cores') == old.get('cpu_cores')):
        result['cpu_percent'] = min(100.0, 100 * consumed / elapsed / result['cpu_cores'])
    return result


def sample_metrics(connection, store, identifier):
    """Persist a sample; telemetry failure must never interrupt optimization."""
    row = store.read(identifier)
    previous = row.get('runtime_metrics') or {}
    now = time.time()
    try:
        raw = connection.command('python3 -c ' + shlex.quote(PROBE), timeout=10, max_output=4096)
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError('Invalid metrics response')
        result = normalize_sample(data, previous, now)
    except (VastError, OSError, ValueError, TypeError):
        if previous.get('available') is not False:
            _log(SERVICE, f'{identifier}: utilization temporarily unavailable', level='WARNING')
        result = {**previous, 'available': False, 'checked_at': now}
    store.update(identifier, runtime_metrics=result)
