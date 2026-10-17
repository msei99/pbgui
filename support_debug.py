"""Bounded, read-only support snapshots across PBGui's registered subsystems."""
from __future__ import annotations

import importlib.metadata
from contextlib import closing
import json
import math
import os
import platform
import stat
import subprocess
import re
import shutil
import sqlite3
import sys
import time
from pathlib import Path

from logging_helpers import _redact_text, human_log as _log

SERVICE = 'SupportDebug'
MAX_FILES = 400
MAX_READ = 2_000_000
MAX_LOG_BYTES = 16000
MAX_TOTAL = 8_000_000
MAX_SECONDS = 12
ID = re.compile(r'[a-f0-9]{32}')
SECRET_PATH = re.compile(r'(^|[/_.-])(auth|credentials?|secrets?|vault|tokens?|sessions?|keys?|passwords?|private|\.env)([/_.-]|$)', re.I)
SECRET_KEY = re.compile(r'password|passwd|secret|token|cookie|authorization|credential|private.?key|api.?key', re.I)
SKIP_KEYS = {'config', 'initial_config', 'executed_config', 'overrides', 'initial_overrides',
             'candidate', 'content', 'text', 'messages', 'prompt', 'instructions', 'knowledge',
             'request', 'response', 'payload', 'command', 'argv', 'env', 'user', 'owner', 'user_id',
             'source', 'initial_results', 'config_paths', 'path', 'local_path'}
FIELDS = set('id name kind type status phase round reason error last_error repair_error historical_error '
             'created_at updated_at started_at ended_at run_started_at finished_at returncode pid '
             'execution provider model profile effort service_tier max_rounds max_runs hours parallel '
             'run_limit_mode run_proxy run_iters max_validations candidates patience validation_count '
             'operation exact reports simulation_complete comparable achieved hard_targets_met qualification '
             'goal metric metrics value target direction reducer confirmed requirement '
             'proxy_evaluations limit_reached native_iters backtest_completion_ratio '
             'liquidated liquidated_usd liquidated_btc pb8 pb8_version pb8_commit '
             'failure errors jobs history observations rejected_observations assessment per_report rubric '
             'baseline best runner backend progress runtime health summary state results workers data '
             'ready available connected stale timestamp generated_at last_seen heartbeat version branch '
             'revision serial success failed completed total count rows missing warnings checks duration_seconds '
             'exit_code result_code filename config_name native_status data_status ai_calls '
             'retryable retry_count attempts stage steps active pending running stopped settings goals '
             'presets targets traceback exception error_type error_message detail details diagnostics '
             'result counters statistics queue idle enabled mode service instances processes '
             'ai_tokens_reserved input_tokens output_tokens context_tokens ai_decision_token_limit'.split())


def text(value):
    """Redact credentials and host-specific absolute paths in diagnostic strings."""
    value = _redact_text(str(value))
    value = re.sub(r'https?://[^\s"\'<>]+', '[URL]', value)
    value = re.sub(r'(?i)(?:[A-Z]:\\Users\\[^\\\s]+|/(?:home|Users)/[^/\s]+)', '[HOME]', value)
    return value[:3000].encode('utf-8', errors='replace').decode('utf-8')


def scrub(value, depth=0, metrics=False):
    """Export operational fields only; never export configs, chats or arbitrary payloads."""
    if depth > 12:
        return '[DEPTH LIMIT]'
    if value is None or type(value) in (bool, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return text(value)
    if isinstance(value, list):
        return ([{'debug_truncated_items': len(value) - 100, 'kept': 'last 100'}] if len(value) > 100 else []) + [scrub(item, depth + 1, metrics) for item in value[-100:]]
    if not isinstance(value, dict):
        return None
    result = {'debug_truncated_fields': len(value) - 2000} if len(value) > 2000 else {}
    for key, item in list(value.items())[:2000]:
        if not isinstance(key, str) or key.lower() in SKIP_KEYS:
            continue
        if SECRET_KEY.search(key) and not (key in FIELDS and type(item) is int):
            continue
        if metrics:
            if type(item) in (int, float, bool) and (not isinstance(item, float) or math.isfinite(item)):
                result[text(key)] = item
            elif isinstance(item, dict):
                result[text(key)] = scrub(item, depth + 1, True)
        elif key in FIELDS:
            result[key] = scrub(item, depth + 1, key in {'metrics', 'per_report'})
        elif isinstance(item, (dict, list)):
            child = scrub(item, depth + 1)
            if child:
                result[text(key)] = child
        elif type(item) in (bool, int, float):
            result[text(key)] = scrub(item, depth + 1)
    return result


def git_revision(root):
    """Read exact Git revision metadata without executing Git or reading its config."""
    git = Path(root) / '.git'
    if not git.is_dir() or git.is_symlink():
        return None
    head = git / 'HEAD'
    if not head.is_file() or head.is_symlink():
        return None
    reader = Collector(Path(root).resolve(), '0' * 32)
    raw = (reader.read(head) or '')[:300].strip()
    if re.fullmatch(r'[a-f0-9]{40,64}', raw):
        return raw
    if raw.startswith('ref: refs/') and re.fullmatch(r'refs/[A-Za-z0-9_./-]+', raw[5:]):
        ref = git / raw[5:]
        if '..' not in Path(raw[5:]).parts and ref.is_file() and not ref.is_symlink():
            revision = (reader.read(ref) or '')[:100].strip()
            if re.fullmatch(r'[a-f0-9]{40,64}', revision):
                return revision
    return None


class Collector:
    """Own all per-request limits and record missing, excluded and truncated sources."""
    def __init__(self, root, owner):
        self.root = Path(root).resolve()
        self.owner = owner
        self.start = time.monotonic()
        self.used = 0
        self.files = []
        self.omitted = []
        self.records = {}
        self.logs = {}
        self.state_bytes = 0
        self.state_files = 0
        self.area_bytes = {}
        self.area_files = {}

    def safe(self, path, root=None):
        """Reject symlink components, traversal and non-regular files before reads."""
        root = Path(root or self.root).resolve()
        path = Path(path)
        try:
            relative = path.relative_to(root)
        except ValueError:
            return False
        current = root
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                return False
        return path.is_file()

    def budget(self):
        """Apply shared byte, source and elapsed-time caps across every subsystem."""
        return (self.used < MAX_TOTAL and len(self.files) < MAX_FILES
                and time.monotonic() - self.start < MAX_SECONDS)

    def read(self, path, root=None, tail=False, priority=False):
        """Read bounded regular files; log tails never pull entire large files into RAM."""
        root = Path(root or self.root).resolve()
        label = path.relative_to(root).as_posix()
        if (not self.safe(path, root) or SECRET_PATH.search(label)
                or (not tail and re.search(r'(^|[_-])(config|settings|preferences|mapping)([_-]|\.)', path.name, re.I))):
            return None
        parts = path.relative_to(root).parts
        area = parts[1] if len(parts) > 1 and parts[0] == 'data' else parts[0]
        if not tail and (self.state_files >= MAX_FILES * 3 // 4 or self.state_bytes >= MAX_TOTAL * 3 // 4
                         or (not priority and self.area_files.get(area, 0) >= 40)):
            self.omitted.append({'source': text(label), 'reason': 'reserved coverage budget'})
            return None
        if not self.budget():
            self.omitted.append({'source': text(label), 'reason': 'collection limit'})
            return None
        try:
            directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                for part in path.relative_to(root).parts[:-1]:
                    next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory_fd)
                    os.close(directory_fd); directory_fd = next_fd
                fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
            finally:
                os.close(directory_fd)
            with os.fdopen(fd, 'rb') as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode):
                    return None
                size = info.st_size
                if not tail and size > MAX_READ:
                    self.omitted.append({'source': text(label), 'reason': 'file size limit'})
                    return None
                if not tail and not priority and self.area_bytes.get(area, 0) + size > MAX_TOTAL // 8:
                    self.omitted.append({'source': text(label), 'reason': 'area byte limit'})
                    return None
                if not tail and self.state_bytes + size > MAX_TOTAL * 3 // 4:
                    self.omitted.append({'source': text(label), 'reason': 'reserved log budget'})
                    return None
                limit = min(MAX_LOG_BYTES if tail else MAX_READ, MAX_TOTAL - self.used)
                if tail and size > limit:
                    handle.seek(-limit, os.SEEK_END)
                    handle.readline()
                raw = handle.read(limit)
            self.used += len(raw)
            if not tail:
                self.state_bytes += len(raw); self.state_files += 1
                self.area_bytes[area] = self.area_bytes.get(area, 0) + len(raw)
                self.area_files[area] = self.area_files.get(area, 0) + 1
            self.files.append({'source': text(label), 'bytes_read': len(raw), 'source_bytes': size,
                               'truncated': size > len(raw), 'mtime': info.st_mtime})
            return raw.decode('utf-8', errors='replace')
        except OSError as exc:
            self.omitted.append({'source': text(label), 'reason': type(exc).__name__})
            return None

    def walk(self, root, limit=2500):
        """Bound traversal itself and never descend into links or secret directories."""
        root = Path(root)
        if not root.is_dir() or any(part.is_symlink() for part in [root, *root.parents]):
            return
        visited = 0
        for folder, directories, names in os.walk(root, followlinks=False, onerror=lambda exc: self.omitted.append({'source': text(root.name), 'reason': type(exc).__name__})):
            visited += 1
            if visited > limit or time.monotonic() - self.start >= MAX_SECONDS:
                self.omitted.append({'source': text(root.name), 'reason': 'scan limit'})
                return
            directories[:] = sorted(d for d in directories if not SECRET_PATH.search(d)
                                    and not (Path(folder) / d).is_symlink())
            def mtime(name):
                try:
                    return (Path(folder) / name).lstat().st_mtime
                except OSError:
                    return 0
            for name in sorted(names, key=mtime, reverse=True):
                visited += 1
                if visited > limit or time.monotonic() - self.start >= MAX_SECONDS:
                    self.omitted.append({'source': text(root.name), 'reason': 'scan limit'})
                    return
                yield Path(folder) / name

    def state(self, path, label, *, root=None, owned=False, priority=False):
        """Read approved operational records, applying owner checks before export."""
        raw = self.read(path, root, priority=priority)
        if raw is None:
            return
        try:
            record = json.loads(raw)
            if not isinstance(record, dict):
                return
            if (owned and record.get('owner') not in (None, self.owner)) or record.get('loop_owner') not in (None, self.owner):
                return
            self.records[text(label)] = scrub(record)
        except ValueError:
            self.omitted.append({'source': text(label), 'reason': 'invalid JSON'})


def collect(root, owner, routes, *, loop_id=None, runtime_roots=None):
    """Collect all application areas using shared logs, operational stores and route inventory."""
    if not ID.fullmatch(owner) or (loop_id is not None and not ID.fullmatch(loop_id)):
        raise ValueError('Invalid diagnostic owner or Loop ID')
    collector = Collector(root, owner)
    root = collector.root
    data = root / 'data'
    if data.is_symlink():
        raise ValueError('Data root must not be a symlink')
    try:
        data_areas = sorted(data.iterdir()) if data.is_dir() else []
    except OSError as exc:
        data_areas = []
        collector.omitted.append({'source': 'data', 'reason': type(exc).__name__})
    if loop_id:
        target = data / 'loop_optimizer' / owner / (loop_id + '.json')
        if not collector.safe(target):
            raise FileNotFoundError('Loop not found for this user')
        collector.state(target, 'selected_loop', owned=True, priority=True)
    selected = collector.records.get('selected_loop', {})
    for job in selected.get('jobs', []):
        if not isinstance(job, dict) or job.get('round') != selected.get('round') and job.get('kind') != 'baseline':
            continue
        backend = job.get('backend') or {}
        identifier = backend.get('id', '')
        log_dir = 'optimizes_v8' if job.get('kind') == 'optimizer' else 'backtests_v8'
        if re.fullmatch(r'[a-f0-9-]{32,36}', str(identifier)):
            label = log_dir + '/' + ('vast_' if backend.get('execution') == 'vast' else '') + identifier + '.log'
            raw = collector.read(data / 'logs' / label, tail=True)
            if raw is not None:
                collector.logs[text(label)] = [text(line) for line in raw.splitlines()[-120:]
                                        if not re.fullmatch(r'[A-Za-z0-9+/=]{40,}', line.strip())
                                        and not re.search(r'[\"\'](?:messages|prompt|content|instructions|config|request_body)[\"\']\s*:', line, re.I)]
        name = job.get('name', '')
        if re.fullmatch(r'[A-Za-z0-9_-]{1,150}', str(name)) and job.get('kind') != 'optimizer':
            for kind, bot_path in (runtime_roots or {}).items():
                bot_root = Path(bot_path).resolve()
                for path in collector.walk(bot_root / 'backtests' / 'pbgui' / name, limit=100):
                    if path.name == 'analysis.json':
                        raw = collector.read(path, bot_root)
                        if raw:
                            try:
                                collector.records[text(kind + '/' + path.relative_to(bot_root).as_posix())] = scrub(json.loads(raw), metrics=True)
                            except ValueError:
                                collector.omitted.append({'source': name, 'reason': 'invalid JSON'})
    # Prioritize the selected loop and its full exact-comparison evidence.
    for namespace in ('loop_optimizer', 'ai/conversations', 'ai/research'):
        folder = data / namespace / owner
        if folder.is_dir() and not folder.is_symlink():
            for path in sorted(folder.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)[:40]:
                if ID.fullmatch(path.stem):
                    collector.state(path, f'{namespace}/{path.stem}', owned=True)
    # Queue records and runner states are shared operational data, not config snapshots.
    for namespace in ('bt_v7_queue', 'bt_v8_queue', 'opt_v7_queue', 'opt_v8_queue',
                      'tasks', 'task_queue', 'vps_monitor', 'monitor', 'cluster'):
        folder = data / namespace
        for path in collector.walk(folder):
            relative = path.relative_to(folder)
            if 'configs' in relative.parts or 'config' in relative.parts:
                continue
            if path.suffix == '.json' and (len(relative.parts) == 1 or relative.parts[0] in
                                          {'state', 'pending', 'running', 'done', 'failed', 'completed', 'cache'}):
                collector.state(path, f'{namespace}/{relative.as_posix()}')
    for path in collector.walk(data / 'vast' / 'jobs'):
        if path.name in {'job.json', 'state.json'}:
            collector.state(path, 'vast/' + path.relative_to(data / 'vast').as_posix(), owned=True)
    for area in data_areas:
        if not area.is_dir() or area.name in {'logs', 'loop_optimizer', 'ai', 'ohlcv', 'coindata'}:
            continue
        for path in collector.walk(area, limit=1500):
            if path.name in {'status.json', 'state.json', 'health.json', 'manifest.json', 'summary.json',
                              'instance_snapshot.json', 'host_meta.json', 'package_status.json',
                              'service_status.json', 'collector_status.json', 'live_metrics.latest.json'}:
                if 'configs' not in path.parts and 'input' not in path.parts:
                    collector.state(path, 'cached/' + path.relative_to(data).as_posix(), owned=True)
    for path in collector.walk(data / 'ohlcv' / '_tasks', limit=2000):
        if path.suffix == '.json':
            collector.state(path, 'market_tasks/' + path.relative_to(data / 'ohlcv' / '_tasks').as_posix())
    for path in collector.walk(data / 'coindata', limit=500):
        if path.name in {'status.json', 'state.json', 'health.json', 'manifest.json'}:
            collector.state(path, 'market_cache/' + path.relative_to(data / 'coindata').as_posix())
    # All service/native logs automatically cover existing and newly registered subsystems.
    for path in collector.walk(data / 'logs', limit=6000):
        if not re.fullmatch(r'.+\.(?:log|jsonl)(?:\.[12])?', path.name, re.I):
            continue
        label = path.relative_to(data / 'logs').as_posix()
        if label in collector.logs:
            continue
        raw = collector.read(path, tail=True)
        if raw is not None:
            collector.logs[text(label)] = [text(line) for line in raw.splitlines()[-120:]
                                    if not re.fullmatch(r'[A-Za-z0-9+/=]{40,}', line.strip())
                                    and not re.search(r'[\"\'](?:messages|prompt|content|instructions|config|request_body)[\"\']\s*:', line, re.I)]
    databases = []
    for path in collector.walk(data, limit=4000):
        if path.suffix not in {'.db', '.sqlite', '.sqlite3'} or SECRET_PATH.search(path.as_posix()):
            continue
        if not collector.safe(path):
            continue
        if len(databases) >= 12 or not collector.budget():
            collector.omitted.append({'source': path.relative_to(root).as_posix(), 'reason': 'database collection limit'})
            continue
        started = time.monotonic()
        item = {'source': text(path.relative_to(root).as_posix()), 'snapshot': 'immutable; uncheckpointed WAL excluded'}
        try:
            with closing(sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True, timeout=.2)) as db:
                db.set_progress_handler(lambda: int(time.monotonic() - started > .5), 1000)
                item['quick_check'] = [text(row[0]) for row in db.execute('PRAGMA quick_check').fetchmany(10)]
                item['tables'] = [text(row[0]) for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchmany(100)]
        except sqlite3.Error as exc:
            item['error'] = text(exc)
        databases.append(item)
    versions = {'pb7': {'configured': False}, 'pb8': {'configured': False}}
    for kind, bot_root in (runtime_roots or {}).items():
        bot_root = Path(bot_root).resolve()
        versions[kind] = {'configured': True, 'git_revision': git_revision(bot_root)}
        # Native analysis is quantitative evidence; never include trading configs or keys.
        for path in collector.walk(bot_root / 'backtests' / 'pbgui', limit=4000):
            if path.name == 'analysis.json':
                if kind + '/' + path.relative_to(bot_root).as_posix() in collector.records:
                    continue
                raw = collector.read(path, bot_root)
                if raw:
                    try:
                        report = json.loads(raw)
                        collector.records[text(kind + '/' + path.relative_to(bot_root).as_posix())] = scrub(report, metrics=True)
                    except ValueError:
                        collector.omitted.append({'source': kind + '/analysis.json', 'reason': 'invalid JSON'})
    # Directory-level inventory covers remaining areas without reading secret/config/DB contents.
    inventory = []
    if data.is_dir():
        for directory in data_areas:
            if directory.is_dir() and not directory.is_symlink() and not SECRET_PATH.search(directory.name):
                inventory.append({'area': text(directory.name), 'present': True})
    packages = {}
    for name in ('fastapi', 'uvicorn', 'pydantic', 'numpy', 'numba', 'psutil', 'ccxt', 'torch', 'cupy-cuda12x'):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    gpu = {'status': 'unavailable'}
    try:
        probe = subprocess.run(['nvidia-smi', '--query-gpu=name,driver_version,memory.total,memory.used',
                                '--format=csv,noheader,nounits'], capture_output=True, text=True,
                               timeout=2, check=False)
        gpu = {'status': 'available' if probe.returncode == 0 else 'probe_failed',
               'devices': [text(line) for line in probe.stdout.splitlines()[:16]]}
    except (OSError, subprocess.TimeoutExpired):
        pass
    resources = {'disk': dict(zip(['total', 'used', 'free'], shutil.disk_usage(root)))}
    try:
        import psutil
        memory = psutil.virtual_memory()
        resources.update(memory={'total': memory.total, 'available': memory.available, 'percent': memory.percent},
                         api_rss=psutil.Process().memory_info().rss, boot_time=psutil.boot_time())
    except (ImportError, OSError):
        pass
    serial_file = root / 'api' / 'serial.txt'
    current_serial = None
    if collector.safe(serial_file):
        value = serial_file.read_text()[:20].strip()
        current_serial = int(value) if value.isdigit() else None
    running_module = sys.modules.get('PBApiServer') or sys.modules.get('__main__')
    startup_serial = getattr(running_module, '_startup_serial', None)
    from pbgui_purefunc import PBGUI_VERSION
    return {'format': 'pbgui-support-debug-v1', 'created_at': time.time(),
            'system': {'pbgui_version': PBGUI_VERSION, 'git_revision': git_revision(root),
                       'python': platform.python_version(), 'os': platform.system(),
                       'os_release': platform.release(), 'architecture': platform.machine(),
                       'cpu_count': os.cpu_count(), 'resources': resources, 'startup_serial': startup_serial,
                       'current_serial': current_serial, 'gpu': gpu, 'packages': packages, 'bots': versions},
            'coverage': {'registered_routes': sorted(set(routes)), 'data_areas': inventory,
                         'sources': collector.files, 'omitted': collector.omitted,
                         'limits': {'files': MAX_FILES, 'input_bytes': MAX_TOTAL, 'seconds': MAX_SECONDS},
                         'excluded': ['credentials/auth/session files', 'configuration contents',
                                      'private keys', 'chat messages/prompts', 'database contents',
                                      'remote/network probes', 'raw market/trade datasets']},
            'databases': databases, 'records': collector.records, 'logs': collector.logs}
