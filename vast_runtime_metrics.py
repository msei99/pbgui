"""Bounded, read-only utilization sampling for an existing GPU worker."""

import json
import math
import re
import shlex
import time
from pathlib import Path

from logging_helpers import human_log as _log
from secure_files import atomic_write_private_text, ensure_private_directory
from vast_provider import VastError

SERVICE = 'VastRunner'
OPTIMIZER_OBSERVATION_MAX_AGE = 45
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
    p = subprocess.run(['nvidia-smi', '--query-gpu=utilization.gpu,memory.used,memory.total,power.draw,power.limit', '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=3)
    if p.returncode == 0:
        values = p.stdout.splitlines()[0].split(',')
        for key, value, scale in zip(
            ['gpu_percent','vram_used_bytes','vram_total_bytes','gpu_power_watts','gpu_power_limit_watts'],
            values, [1,1048576,1048576,1,1]
        ):
            try: r[key] = float(value.strip()) * scale
            except ValueError: pass
except (OSError, subprocess.TimeoutExpired, IndexError): pass
print(json.dumps(r, allow_nan=False))
'''

OPTIMIZER_PROBE = r'''
import json, os, re, time
from pathlib import Path

root = Path('/work/pbgui').resolve()
matches = []
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():
        continue
    try:
        args = [value.decode('utf-8') for value in (proc / 'cmdline').read_bytes().split(b'\0') if value]
        index = next(index for index, value in enumerate(args) if Path(value).name == 'optimize.py')
        config_arg = Path(args[index + 1])
        process_cwd = (proc / 'cwd').resolve(strict=True)
        config = (config_arg if config_arg.is_absolute()
                  else process_cwd / config_arg).resolve(strict=True)
        relative = config.relative_to(root)
        if config.name != 'optimize.json' or config.is_symlink():
            continue
        log = (proc / 'fd/1').resolve(strict=True)
        log.relative_to(root)
        if log.name != 'optimizer.log' or log.is_symlink() or not log.is_file():
            continue
        container = relative.parts[1] if relative.parts[:1] == ('jobs',) and len(relative.parts) > 1 else relative.parts[0]
        name = re.sub(r'[^A-Za-z0-9_.-]+', '_', container)[:128] or 'PB8 optimizer'
        matches.append({'pid': int(proc.name), 'ppid': os.getppid() if int(proc.name) == os.getpid()
                        else int((proc / 'stat').read_text().split()[3]), 'name': name, 'log_path': log})
    except (OSError, ValueError, StopIteration, IndexError, UnicodeDecodeError):
        continue
pids = {match['pid'] for match in matches}
roots = [match for match in matches if match['ppid'] not in pids]
result = {'running': len(roots) == 1, 'ambiguous': len(roots) > 1, 'sampled_at': time.time()}
if len(roots) == 1:
    match = roots[0]
    with match.pop('log_path').open('rb') as stream:
        stream.seek(max(0, stream.seek(0, 2) - 256 * 1024))
        match['log'] = stream.read().decode('utf-8', errors='replace')
    match.pop('ppid', None)
    result.update(match)
print(json.dumps(result, allow_nan=False))
'''


def normalize_optimizer_observation(raw: dict, now: float) -> tuple[dict | None, str | None]:
    """Keep a bounded public snapshot and local log from one observed optimizer."""
    if not isinstance(raw, dict) or raw.get('running') is not True:
        return None, None
    pid = raw.get('pid')
    name = raw.get('name')
    log = raw.get('log')
    if (type(pid) is not int or pid <= 0 or not isinstance(name, str)
            or not name or len(name) > 128 or not isinstance(log, str)
            or len(log.encode('utf-8')) > 256 * 1024):
        raise ValueError('Invalid optimizer observation')
    exact_completed = 0
    gpu_candidates = 0
    activity = 'Optimizer process detected'
    for line in log.splitlines():
        if ("GPU proxy dispatch progress |" in line
                or "GPU temporal replay " in line):
            activity = line.split('INFO', 1)[-1].strip()[:500]
            candidates = re.search(r'\bcandidates=(\d+)(?:/\d+)?\b', line)
            if candidates:
                gpu_candidates = max(gpu_candidates, int(candidates.group(1)))
        match = re.search(r'Iter:\s*(\d+)', line)
        if match:
            exact_completed = max(exact_completed, int(match.group(1)))
        if '[gpu-profile] ' in line:
            try:
                event = json.loads(line.split('[gpu-profile] ', 1)[1])
                exact_completed = max(exact_completed, int(event.get('exact_completed') or 0))
                if event.get('generation'):
                    gpu_candidates = max(
                        gpu_candidates,
                        int(event['generation']) * int(event.get('population_size') or 0),
                    )
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
    return ({'running': True, 'pid': pid, 'name': name, 'sampled_at': now,
             'activity': activity, 'exact_completed': exact_completed,
             'gpu_candidates': gpu_candidates}, log)


def observe_optimizer(connection, store, identifier: str) -> dict | None:
    """Observe one otherwise-unowned PB8 process without taking lifecycle control."""
    now = time.time()
    try:
        raw = connection.command('/usr/local/bin/python -c ' + shlex.quote(OPTIMIZER_PROBE),
                                 timeout=10, max_output=600 * 1024)
        observation, log = normalize_optimizer_observation(json.loads(raw), now)
        if observation is not None and log is not None:
            log_root = ensure_private_directory(Path(store.root).parent / 'logs/optimizes_v8')
            atomic_write_private_text(log_root / ('vast_' + identifier + '.log'), log)
        store.update(identifier, observed_optimizer=observation)
        return observation
    except (VastError, OSError, ValueError, TypeError, json.JSONDecodeError):
        _log(SERVICE, f'{identifier}: external optimizer observation temporarily unavailable', level='WARNING')
        previous = store.read(identifier).get('observed_optimizer')
        sampled_at = previous.get('sampled_at') if isinstance(previous, dict) else None
        if (type(sampled_at) not in (int, float)
                or now - sampled_at > OPTIMIZER_OBSERVATION_MAX_AGE):
            store.update(identifier, observed_optimizer=None)
            return None
        return previous


def normalize_sample(raw, previous, now):
    """Whitelist finite values and derive CPU percent against allocated capacity."""
    result = {'sampled_at': now, 'available': True}
    for key in ('clock', 'cpu_seconds', 'cpu_cores', 'gpu_percent', 'ram_used_bytes',
                'ram_total_bytes', 'vram_used_bytes', 'vram_total_bytes',
                'gpu_power_watts', 'gpu_power_limit_watts'):
        value = raw.get(key)
        if type(value) in (int, float) and math.isfinite(value) and value >= 0:
            result[key] = value
    if result.get('gpu_percent', 0) > 100:
        result.pop('gpu_percent')
    if result.get('gpu_power_limit_watts', 0) <= 0:
        result.pop('gpu_power_limit_watts', None)
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
