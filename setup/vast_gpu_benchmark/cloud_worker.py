"""Self-contained PBGui worker protocol for the pinned PB8 CUDA image.

Only the guard consumes Vast's per-container credential. No PBGui, registry
write, exchange or Vast account credentials belong in this worker.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
from datetime import date
import os
import re
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

SERVICE = "VastWorker"
ROOT = Path(os.environ.get("PBGUI_WORKDIR", "/work/pbgui"))
GUARD_ROOT = Path("/work/pbgui")


def write_record(path: Path, value: dict) -> None:
    """Publish a private JSON record atomically."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=4, allow_nan=False) + "\n")
    os.replace(temporary, path)


def file_hash(path: Path) -> str:
    """Hash immutable transfer content in bounded chunks."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def safe_path(root: Path, value: str) -> Path:
    """Reject absolute, traversal, symlink and noncanonical archive paths."""
    if not isinstance(value, str) or not value or value.startswith("/") or "\\" in value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError("Invalid transfer path")
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError("Invalid transfer path")
    path = root.joinpath(*parts)
    for ancestor in [path, *path.parents]:
        if ancestor == root.parent:
            break
        if ancestor.is_symlink():
            raise ValueError("Symlink in transfer path")
    path.resolve().relative_to(root.resolve())
    return path


def apply_deadline_request(state: dict, request: dict, now: float) -> dict:
    """Validate a compare-and-set request inside the independent guard process."""
    import math
    target = request.get('deadline')
    requested_max = request.get('max_deadline', state['max_deadline'])
    valid = (isinstance(request.get('id'), str) and re.fullmatch(r'[0-9a-f]{32}', request['id'])
             and request.get('job_id') == state['job_id'] and request.get('expected') == state['deadline']
             and type(target) in (int, float) and math.isfinite(target)
             and type(requested_max) in (int, float) and math.isfinite(requested_max)
             and now + 300 < min(state['deadline'], target) and target <= requested_max
             and requested_max <= state.get('hard_deadline', now + 86400))
    if request.get('id') == state.get('request_id'):
        return state
    if not valid:
        return dict(state, rejected_request_id=str(request.get('id', ''))[:32])
    return dict(state, deadline=target, max_deadline=requested_max, request_id=request['id'], rejected_request_id=None)


def guard() -> None:
    """Independently enforce a bounded deadline, acknowledging explicit adjustments."""
    deadline = float(os.environ["PBGUI_DEADLINE"])
    instance = int(os.environ["CONTAINER_ID"])
    key = os.environ["CONTAINER_API_KEY"]
    if not 0 < deadline <= time.time() + 86400 or instance <= 0:
        raise ValueError("Invalid guard authorization")
    maximum = float(os.environ.get("PBGUI_MAX_DEADLINE", str(deadline)))
    if not deadline <= maximum <= time.time() + 86400:
        raise ValueError("Invalid maximum deadline")
    hard_deadline = float(os.environ.get('PBGUI_HARD_DEADLINE', str(time.time() + 86400)))
    state = {"instance_id": instance, "deadline": deadline, "job_id": os.environ["PBGUI_JOB_ID"],
             "max_deadline": maximum, "hard_deadline": hard_deadline, "deadline_protocol": 2, "guard_pid": os.getpid()}
    saved_path = ROOT / 'guard.json'
    if saved_path.is_file() and not saved_path.is_symlink():
        saved = json.loads(saved_path.read_text())
        if (saved.get('instance_id') == instance and saved.get('job_id') == state['job_id']
                and saved.get('deadline_protocol') in (1, 2)
                and type(saved.get('max_deadline')) in (int, float)
                and type(saved.get('deadline')) in (int, float)
                and 0 < saved['deadline'] <= saved['max_deadline'] <= hard_deadline):
            state = dict(saved, deadline_protocol=2, guard_pid=os.getpid(), hard_deadline=min(hard_deadline, saved.get('hard_deadline', hard_deadline)))
            deadline = state['deadline']
    monotonic_deadline = time.monotonic() + max(0, deadline - time.time())
    write_record(ROOT / "guard.json", state)
    while time.time() < state['deadline'] and time.monotonic() < monotonic_deadline:
        request_path = ROOT / 'deadline-request.json'
        if request_path.is_file() and not request_path.is_symlink():
            try:
                if request_path.stat().st_size > 4096:
                    raise ValueError('Oversized deadline request')
                request = json.loads(request_path.read_text())
                if not isinstance(request, dict):
                    raise ValueError('Invalid deadline request')
                updated = apply_deadline_request(state, request, time.time())
                if updated != state:
                    monotonic_deadline += updated['deadline'] - state['deadline']
                    state = updated
                    write_record(ROOT / "guard.json", state)
            except (OSError, ValueError, TypeError):
                write_record(ROOT / 'guard-error.json', {'error': 'Invalid deadline request', 'at': time.time()})
        time.sleep(2)
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        """Never forward a container credential to another endpoint."""
        def redirect_request(self, *args):
            """Refuse redirects."""
            return None
    while True:
        try:
            req = urllib.request.Request("https://console.vast.ai/api/v0/instances/" + str(instance),
                method="DELETE", headers={"Authorization": "Bearer " + key})
            with urllib.request.build_opener(NoRedirect()).open(req, timeout=15) as response:
                response.read(1024)
        except Exception:
            write_record(ROOT / "guard-error.json", {"error": "Container deletion failed; retrying", "at": time.time()})
        time.sleep(10)


def cache_missing() -> dict:
    """Identify immutable data blobs not already verified in this worker cache."""
    manifest = json.loads((ROOT / 'cache-request.json').read_text())
    missing = []
    for item in manifest['files']:
        key = item['sha256']
        if not re.fullmatch(r'[0-9a-f]{64}', key):
            raise ValueError('Invalid data cache key')
        target = safe_path(GUARD_ROOT / 'cache', key)
        if not target.is_file() or target.stat().st_size != item['bytes'] or file_hash(target) != key:
            missing.append(key)
    return {'missing': sorted(set(missing))}


def install_cached_file(path: Path, cached: Path, size: int, checksum: str) -> None:
    """Hard-link one verified immutable blob into a job without copying its bytes."""
    if cached.is_symlink() or not cached.is_file() or cached.stat().st_size != size or file_hash(cached) != checksum:
        raise ValueError('Required data cache entry is missing or corrupt')
    path.parent.mkdir(parents=True, exist_ok=True)
    os.link(cached, path)


def install() -> dict:
    """Unpack a bounded input archive into a fresh staging directory."""
    marker = ROOT / "input-ready.json"
    if marker.exists():
        return json.loads(marker.read_text())
    total = 0
    with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
        stage = Path(temporary)
        with tarfile.open(ROOT / "input.tar.gz", "r:gz") as archive:
            for entry in archive:
                if not (entry.isdir() or entry.isfile()) or not (entry.name == "input" or entry.name.startswith("input/")):
                    raise ValueError("Unexpected input archive entry")
                path = safe_path(stage, entry.name)
                total += entry.size
                if total > 12 * 1024**3:
                    raise ValueError("Input archive is too large")
                if entry.isdir():
                    path.mkdir(parents=True, exist_ok=True)
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with archive.extractfile(entry) as source, path.open("xb") as target:
                        shutil.copyfileobj(source, target, 1024 * 1024)
        manifest = json.loads((stage / "input/manifest.json").read_text())
        if manifest["pb8_revision"] != Path("/opt/pb8-revision").read_text().strip():
            raise ValueError("PB8 image revision mismatch")
        if file_hash(stage / "input/optimize.json") != manifest["config_sha256"]:
            raise ValueError("Config checksum mismatch")
        for item in manifest["files"]:
            path = safe_path(stage / "input", item["path"])
            key = item['sha256']
            if not re.fullmatch(r'[0-9a-f]{64}', key):
                raise ValueError('Invalid cache hash')
            cache_root = GUARD_ROOT / 'cache'
            cache_root.mkdir(exist_ok=True)
            cached = safe_path(cache_root, key)
            if not path.exists():
                install_cached_file(path, cached, item["bytes"], key)
            elif path.stat().st_size != item["bytes"] or file_hash(path) != key:
                raise ValueError("Data checksum mismatch")
            if not cached.exists():
                temporary_blob = cache_root / (key + '.tmp')
                shutil.copyfile(path, temporary_blob)
                os.replace(temporary_blob, cached)
        if (ROOT / "input").exists():
            raise ValueError("Unverified input directory already exists")
        os.replace(stage / "input", ROOT / "input")
    result = {"ready": True, "config_sha256": manifest["config_sha256"]}
    write_record(marker, result)
    return result


def health() -> dict:
    """Check the actual GPU, image identity and effective container allocation."""
    import torch
    if not torch.cuda.is_available():
        raise ValueError("CUDA is unavailable")
    sys.path.insert(0, "/opt/passivbot/src")
    from rust_utils import verify_loaded_runtime_extension
    import passivbot_rust
    identity = verify_loaded_runtime_extension()
    if not identity.get("runtime_compiled_source_stamp") or identity["runtime_compiled_source_stamp"] != identity["expected_source_fingerprint"]:
        raise ValueError("Rust image stamp mismatch")
    cpus = float(len(os.sched_getaffinity(0)))
    quota_path = Path("/sys/fs/cgroup/cpu.max")
    if quota_path.exists():
        quota, period = quota_path.read_text().split()
        if quota != "max":
            cpus = min(cpus, int(quota) / int(period))
    properties = torch.cuda.get_device_properties(0)
    pci_device_id = None
    try:
        value = subprocess.run(
            ['nvidia-smi', '--query-gpu=pci.device_id', '--format=csv,noheader,nounits'],
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout.strip().splitlines()
        if len(value) == 1 and re.fullmatch(r'0x[0-9A-Fa-f]{8}', value[0]):
            pci_device_id = value[0].upper()
    except (OSError, subprocess.SubprocessError):
        pass
    return {"protocol": 1, "revision": Path("/opt/pb8-revision").read_text().strip(),
            "gpu": torch.cuda.get_device_name(0), "cpu_cores": cpus,
            "vram_bytes": properties.total_memory,
            "compute_capability": f'{properties.major}.{properties.minor}',
            "pci_device_id": pci_device_id,
            "guard": json.loads((GUARD_ROOT / "guard.json").read_text())}


def stop_process(process: subprocess.Popen) -> None:
    """Reap only the optimizer group owned by this worker."""
    if process.poll() is not None:
        process.wait()
        return
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            break
        try:
            process.wait(timeout=10)
            break
        except subprocess.TimeoutExpired:
            continue
    process.wait()


_CALIBRATION_PROGRESS = re.compile(
    r"Optimizer population progress\s*\|\s*"
    r"completed=(\d+)/(\d+|\?)\s+pending=(\d+)\s+submitted=(\d+)\s+"
    r"elapsed=([0-9]+(?:\.[0-9]+)?)s\s+rate=([0-9]+(?:\.[0-9]+)?)/s"
)
_CALIBRATION_GENERATION = re.compile(
    r"GPU optimize\s*\|\s*gen=\d+\s+proxy=\d+\s+\(([0-9]+(?:\.[0-9]+)?)/s\)"
)


def calibration_progress(log_path: Path) -> dict | None:
    """Read the last bounded PB8 heartbeat or completed-generation rate."""
    if not log_path.is_file():
        return None
    with log_path.open('rb') as stream:
        stream.seek(max(0, log_path.stat().st_size - 1024 * 1024))
        text = stream.read().decode(errors='replace')
    heartbeat = list(_CALIBRATION_PROGRESS.finditer(text))
    if heartbeat:
        match = heartbeat[-1]
        elapsed, rate = float(match[5]), float(match[6])
        result = {
            'source': 'heartbeat', 'completed': int(match[1]),
            'total': None if match[2] == '?' else int(match[2]),
            'pending': int(match[3]), 'submitted': int(match[4]),
            'elapsed_seconds': elapsed, 'rate_window_seconds': elapsed,
            'rate_per_second': rate, 'valid': elapsed >= 295.0 and rate > 0.0,
        }
        profile = calibration_generation_profile(log_path)
        if not result['valid'] and profile:
            return profile
        if profile:
            for field in ('generation_count', 'proxy_seconds', 'kernel_seconds',
                          'dispatch_count', 'dispatch_batch_size', 'last_dispatch_size',
                          'strategy', 'coin_count', 'side_count', 'candidate_bars',
                          'scenario_count', 'all_dispatches_full'):
                result[field] = profile.get(field)
        return result
    profile = calibration_generation_profile(log_path)
    if profile:
        return profile
    generations = list(_CALIBRATION_GENERATION.finditer(text))
    if generations:
        rate = float(generations[-1][1])
        return {'source': 'completed_generation', 'rate_per_second': rate, 'valid': rate > 0.0}
    return None


def calibration_generation_profile(log_path: Path) -> dict | None:
    """Summarize completed PB8 generations, excluding unfinished dispatches."""
    if not log_path.is_file():
        return None
    with log_path.open('rb') as stream:
        stream.seek(max(0, log_path.stat().st_size - 1024 * 1024))
        lines = stream.read().decode(errors='replace').splitlines()
    candidate_count = 0
    wall_seconds = 0.0
    proxy_seconds = 0.0
    kernel_seconds = 0.0
    dispatch_count = 0
    generation_count = 0
    scenario_count = 0
    dispatch_size = None
    last_dispatch_size = None
    strategy = None
    coin_count = None
    side_count = None
    candidate_bars = None
    all_dispatches_full = True
    for line in lines:
        marker = '[gpu-profile] '
        if marker not in line:
            continue
        try:
            record = json.loads(line.split(marker, 1)[1])
        except (ValueError, TypeError):
            continue
        if record.get('event') != 'generation':
            continue
        candidates = record.get('full_history_candidate_count')
        seconds = (record.get('timings_seconds') or {}).get('wall')
        if (type(candidates) is not int or candidates <= 0
                or type(seconds) not in (int, float) or not 0 < seconds < 36000):
            continue
        profiles = record.get('proxy_profiles') or []
        proxy = profiles[0] if profiles and isinstance(profiles[0], dict) else {}
        candidate_count += candidates
        wall_seconds += seconds
        proxy_seconds += float((record.get('timings_seconds') or {}).get('proxy_evaluation') or 0)
        kernel_seconds += sum(float((item.get('timings_seconds') or {}).get('kernel_execution') or 0)
                              for item in profiles if isinstance(item, dict))
        dispatch_count += sum(int(item.get('dispatch_count') or 0)
                              for item in profiles if isinstance(item, dict))
        scenario_count += len(profiles)
        dispatch_size = proxy.get('dispatch_batch_size') or dispatch_size
        actual = proxy.get('actual_dispatch_batch_sizes') or []
        if actual:
            last_dispatch_size = actual[-1]
        strategy = proxy.get('strategy') or strategy
        coin_count = proxy.get('coin_count') or coin_count
        side_count = proxy.get('side_count') or side_count
        measured_work = proxy.get('candidate_bars')
        if type(measured_work) is int and measured_work > 0:
            candidate_bars = max(candidate_bars or 0, measured_work)
        if not profiles:
            all_dispatches_full = False
        for scenario in profiles:
            if not isinstance(scenario, dict):
                all_dispatches_full = False
                continue
            work = scenario.get('candidate_bars')
            if type(work) is int and work > 0:
                candidate_bars = max(candidate_bars or 0, work)
            if (scenario.get('dispatch_count') != 1
                    or scenario.get('dispatch_batch_size') != candidates):
                all_dispatches_full = False
        generation_count += 1
    if not generation_count or not wall_seconds:
        return None
    return {
        'source': 'generation_profile', 'rate_per_second': round(candidate_count / wall_seconds, 3),
        'rate_window_seconds': round(wall_seconds, 3), 'generation_count': generation_count,
        'proxy_seconds': round(proxy_seconds, 3), 'kernel_seconds': round(kernel_seconds, 3),
        'dispatch_count': dispatch_count, 'dispatch_batch_size': dispatch_size,
        'scenario_count': scenario_count,
        'last_dispatch_size': last_dispatch_size, 'strategy': strategy,
        'coin_count': coin_count, 'side_count': side_count, 'valid': True,
        'candidate_bars': candidate_bars,
        'all_dispatches_full': all_dispatches_full,
    }


def calibration_dispatch_limit(config: dict, population: int) -> int:
    """Allow a full batch for the fixed workload; PB8 evidence verifies it fit."""
    backtest = config['backtest']
    days = (date.fromisoformat(backtest['end_date']) - date.fromisoformat(backtest['start_date'])).days + 1
    interval = int(backtest.get('candle_interval_minutes') or 1)
    approved = config['live']['approved_coins']
    coins = set(approved['long']) | set(approved['short'])
    if days <= 0 or interval <= 0 or not coins or population <= 0:
        raise ValueError('Invalid calibration dispatch workload')
    # Allow warmup bars and both sides. The requested batch still bounds work;
    # only PB8's measured candidate-bars are saved as the production limit.
    estimated_bars = (days * 1440 + interval - 1) // interval
    return 2 * estimated_bars * len(coins) * 2 * population


def configurable_calibration_plan(plan: dict) -> dict:
    """Accept only bounded protocol-4 plans created by PBGui."""
    if not isinstance(plan, dict):
        raise ValueError('Invalid configurable calibration plan')
    populations = plan.get('populations')
    step = plan.get('population_step')
    maximum = plan.get('max_population')
    gain = plan.get('min_scale_gain')
    sample = plan.get('sample_seconds')
    timeout = plan.get('case_timeout_seconds')
    if (plan.get('protocol') != 4 or not isinstance(populations, list) or len(populations) != 2
            or any(type(value) is not int for value in populations)
            or type(step) is not int or type(maximum) is not int
            or populations[0] < 4096 or step < 512 or populations[1] != populations[0] + step
            or maximum < populations[1] or maximum > 131072
            or (maximum - populations[0]) // step > 64
            or not isinstance(gain, (int, float)) or not 0 <= gain <= .5
            or type(sample) is not int or not 300 <= sample <= 1800
            or type(timeout) is not int or not 600 <= timeout <= 7200
            or plan.get('vram_reserve_ratio') != .15):
        raise ValueError('Invalid configurable calibration plan')
    return plan


def configurable_calibration_decision(cases: dict, plan: dict) -> dict:
    """Stop on regression or sub-threshold gain and keep the best measured case."""
    plan = configurable_calibration_plan(plan)
    first, second = plan['populations']
    for population in (first, second):
        if str(population) not in cases:
            return {'next_population': population, 'recommended_population': None, 'stop_reason': None}
    valid = {int(key): value for key, value in cases.items()
             if isinstance(value, dict) and value.get('valid') is True
             and isinstance(value.get('rate_per_second'), (int, float)) and value['rate_per_second'] > 0}
    if first not in valid or second not in valid:
        return {'next_population': None, 'recommended_population': None, 'stop_reason': 'invalid_evidence'}
    latest = max(valid)
    previous = latest - plan['population_step']
    if previous not in valid:
        return {'next_population': None, 'recommended_population': None, 'stop_reason': 'invalid_evidence'}
    gain = float(valid[latest]['rate_per_second']) / float(valid[previous]['rate_per_second']) - 1
    if gain < 0:
        return {'next_population': None, 'recommended_population': previous,
                'stop_reason': 'performance_regression', 'gain': gain}
    if gain < plan['min_scale_gain']:
        return {'next_population': None, 'recommended_population': latest,
                'stop_reason': 'scaling_plateau', 'gain': gain}
    next_population = latest + plan['population_step']
    if next_population > plan['max_population']:
        return {'next_population': None, 'recommended_population': latest,
                'stop_reason': 'maximum_safety_limit', 'gain': gain}
    return {'next_population': next_population, 'recommended_population': None,
            'stop_reason': None, 'gain': gain}


def calibration_decision(cases: dict) -> dict:
    """Mirror PBGui's linear scaling-knee selection without imports."""
    anchors = (4096, 8192)
    for population in anchors:
        if str(population) not in cases:
            return {'next_population': population, 'recommended_population': None, 'stop_reason': None}
    valid = {int(key): value for key, value in cases.items()
             if isinstance(value, dict) and value.get('valid') is True
             and isinstance(value.get('rate_per_second'), (int, float)) and value['rate_per_second'] > 0}
    if any(population not in valid for population in anchors):
        return {'next_population': None, 'recommended_population': None, 'stop_reason': 'invalid_evidence'}
    latest = max(population for population in valid if population >= 8192)
    if latest == 8192:
        return {'next_population': 12288, 'recommended_population': None, 'stop_reason': None}
    previous = latest - 4096
    if previous not in valid:
        return {'next_population': None, 'recommended_population': None, 'stop_reason': 'invalid_evidence'}
    gain = float(valid[latest]['rate_per_second']) / float(valid[previous]['rate_per_second']) - 1.0
    if gain < 0.0:
        return {'next_population': None, 'recommended_population': previous,
                'stop_reason': 'performance_regression', 'gain': gain}
    if gain < .10:
        return {'next_population': None, 'recommended_population': latest,
                'stop_reason': 'scaling_plateau', 'gain': gain}
    if latest >= 131072:
        return {'next_population': None, 'recommended_population': latest,
                'stop_reason': 'maximum_safety_limit', 'gain': gain}
    return {'next_population': latest + 4096, 'recommended_population': None,
            'stop_reason': None, 'gain': gain}


def gpu_memory_used_bytes() -> int | None:
    """Return current device memory use without exposing provider state."""
    try:
        value = subprocess.check_output(
            ['nvidia-smi', '--query-gpu=memory.used', '--format=csv,noheader,nounits'],
            text=True, timeout=5,
        ).splitlines()[0].strip()
        used_mib = int(value)
        return used_mib * 1024**2 if used_mib >= 0 else None
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return None


def calibration_failure_reason(log_path: Path) -> str | None:
    """Classify only the bounded CUDA capacity failure used for safe fallback."""
    try:
        with log_path.open('rb') as stream:
            stream.seek(max(0, log_path.stat().st_size - 1024 * 1024))
            text = stream.read().decode(errors='replace').lower()
        return 'vram_limit' if 'out of memory' in text and ('cuda' in text or 'gpu' in text) else None
    except OSError:
        return None


def mirror_log_delta(source: Path, destination, offset: int) -> int:
    """Append newly written case-log bytes to the user-visible combined log."""
    try:
        with source.open('rb') as stream:
            size = source.stat().st_size
            stream.seek(offset if 0 <= offset <= size else 0)
            while True:
                chunk = stream.read(64 * 1024)
                if not chunk:
                    break
                destination.write(chunk)
            destination.flush()
            return stream.tell()
    except FileNotFoundError:
        return offset


def seed_calibration_case_caches(output: Path, case_root: Path, exchanges: list[str]) -> None:
    """Give each fresh optimizer process PBGui's verified offline metadata."""
    source = output / 'caches'
    names = [Path(exchange) / 'markets.json' for exchange in exchanges]
    names += [Path(name) for name in (
        'first_ohlcv_timestamps_unified.json',
        'first_ohlcv_timestamps_unified_exchange_specific.json',
        'first_ohlcv_timestamps_unified_exchange_specific_symbols.json',
        'first_ohlcv_timestamps_unified.version',
    )]
    if source.is_symlink() or not source.is_dir() or not exchanges:
        raise ValueError('Calibration market and inception caches are not staged')
    for relative in names:
        if (any(not re.fullmatch(r'[a-z][a-z0-9_]*', part) for part in relative.parts[:-1])
                or relative.is_absolute() or '..' in relative.parts):
            raise ValueError('Invalid calibration exchange cache path')
        original = source / relative
        if (relative.parts[:-1] and (source / relative.parts[0]).is_symlink()) or original.is_symlink() or not original.is_file():
            raise ValueError('Calibration cache missing: ' + str(relative))
        if relative.name == 'markets.json' and not 0 <= time.time() - original.stat().st_mtime < 86400:
            raise ValueError('Calibration market cache expired: ' + str(relative))
        destination = case_root / 'caches' / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, destination)


def run_calibration(config: dict, hardware: dict, environment: dict, plan: dict) -> dict:
    """Run bounded fresh-process population probes on one immutable input."""
    protocol = plan.get('protocol')
    if protocol == 4:
        configurable_calibration_plan(plan)
    else:
        if (protocol != 3 or plan.get('populations') != [4096, 8192]
                or plan.get('sample_seconds') != 300 or plan.get('population_step') != 4096
                or plan.get('min_scale_gain') != .10 or plan.get('max_population') != 131072
                or plan.get('vram_reserve_ratio') != .15):
            raise ValueError('Unsupported calibration protocol')
    cases = {}
    cancelled = False
    stop_reason = None
    recommendation = None
    started = time.monotonic()
    output = ROOT / 'output'
    output.mkdir(exist_ok=True)
    combined_log = output / 'optimizer.log'
    while True:
        decision = (configurable_calibration_decision(cases, plan) if protocol == 4
                    else calibration_decision(cases))
        population = decision['next_population']
        if population is None:
            recommendation = decision.get('recommended_population')
            stop_reason = decision.get('stop_reason')
            break
        if (ROOT / 'stop').exists() or time.time() >= json.loads((GUARD_ROOT / 'guard.json').read_text())['deadline'] - 240:
            cancelled = True
            stop_reason = 'user_cancelled' if (ROOT / 'stop').exists() else 'rental_deadline'
            break
        previous_population = max((int(key) for key in cases), default=0)
        if previous_population:
            previous = cases[str(previous_population)]
            baseline = int(previous.get('baseline_vram_bytes') or 0)
            peak = int(previous.get('peak_vram_bytes') or 0)
            total = int(hardware.get('vram_bytes') or 0)
            if total > 0 and peak > 0:
                estimated = baseline + max(0, peak - baseline) * population / previous_population
                if estimated > total * .85:
                    recommendation = previous_population
                    stop_reason = 'vram_limit'
                    break
        case_root = ROOT / 'calibration-cases' / str(population)
        case_root.mkdir(parents=True, exist_ok=False)
        seed_calibration_case_caches(output, case_root, config['backtest']['exchanges'])
        case_config = json.loads(json.dumps(config))
        optimize = case_config['optimize']
        optimize['iters'] = max(int(optimize.get('iters') or 0), 10_000_000)
        optimize['population_size'] = population
        gpu = optimize.setdefault('gpu', {})
        gpu.update(population_size=population, batch_size=population, auto_lean_parallelism=False)
        if protocol >= 3:
            gpu['max_dispatch_candidate_bars'] = calibration_dispatch_limit(case_config, population)
        config_path = case_root / 'optimize.json'
        write_record(config_path, case_config)
        case_started = time.monotonic()
        baseline_vram = gpu_memory_used_bytes()
        peak_vram = baseline_vram
        last_memory_sample = 0.0
        case_log = case_root / 'optimizer.log'
        with case_log.open('ab') as log, combined_log.open('ab') as combined:
            combined.write((f'PBGUI calibration case | population={population}\n').encode())
            combined.flush()
            mirrored_bytes = 0
            process = subprocess.Popen(
                [sys.executable, '/opt/passivbot/src/optimize.py', str(config_path)],
                cwd=case_root, env=environment, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            requested_stop = False
            try:
                while process.poll() is None:
                    mirrored_bytes = mirror_log_delta(case_log, combined, mirrored_bytes)
                    if time.monotonic() - last_memory_sample >= 10.0:
                        current_vram = gpu_memory_used_bytes()
                        if current_vram is not None:
                            peak_vram = current_vram if peak_vram is None else max(peak_vram, current_vram)
                        last_memory_sample = time.monotonic()
                    progress = (calibration_generation_profile(case_log) if protocol == 4
                                else calibration_progress(case_log))
                    if ((protocol == 4 and progress and progress.get('all_dispatches_full')
                         and progress.get('dispatch_batch_size') == population)
                            or (protocol != 4 and progress and progress.get('source') == 'heartbeat'
                                and progress.get('valid')
                                and (protocol < 3 or (progress.get('dispatch_batch_size') == population
                                                     and type(progress.get('candidate_bars')) is int
                                                     and progress['candidate_bars'] > 0)))):
                        requested_stop = True
                        break
                    if time.monotonic() - case_started >= (plan['case_timeout_seconds'] if protocol == 4 else 390):
                        requested_stop = True
                        break
                    if (ROOT / 'stop').exists() or time.time() >= json.loads((GUARD_ROOT / 'guard.json').read_text())['deadline'] - 240:
                        requested_stop = True
                        cancelled = True
                        stop_reason = 'user_cancelled' if (ROOT / 'stop').exists() else 'rental_deadline'
                        break
                    time.sleep(2)
            finally:
                if process.poll() is None:
                    stop_process(process)
                else:
                    process.wait()
                mirror_log_delta(case_log, combined, mirrored_bytes)
        progress = (calibration_generation_profile(case_log) if protocol == 4
                    else calibration_progress(case_log)) or {}
        case_valid = bool(progress.get('valid')) and not cancelled
        if protocol >= 3:
            case_valid = (case_valid and progress.get('dispatch_batch_size') == population
                          and type(progress.get('candidate_bars')) is int
                          and progress['candidate_bars'] > 0
                          and (protocol != 4 or progress.get('all_dispatches_full') is True))
        cases[str(population)] = {
            **progress, 'population_size': population,
            'tested_dispatch_limit': gpu.get('max_dispatch_candidate_bars'),
            'wall_seconds': time.monotonic() - case_started,
            'exit_code': process.returncode,
            'intentionally_stopped': requested_stop and not cancelled,
            'valid': case_valid,
            'baseline_vram_bytes': baseline_vram,
            'peak_vram_bytes': peak_vram,
        }
        if not cancelled and not case_valid:
            failure_reason = calibration_failure_reason(case_log)
            if failure_reason == 'vram_limit' and previous_population:
                cases.pop(str(population), None)
                recommendation = previous_population
                stop_reason = failure_reason
        write_record(ROOT / 'calibration-result.json', {
            'schema': 1, 'protocol': protocol, 'status': 'running', 'hardware': hardware,
            'cases': cases, 'updated_at': time.time(),
        })
        if cancelled or stop_reason == 'vram_limit' or not case_valid:
            break
        shutil.rmtree(case_root, ignore_errors=True)
    if cancelled:
        cases = {key: value for key, value in cases.items() if value.get('valid')}
    rates = {int(key): float(value['rate_per_second']) for key, value in cases.items()
             if value.get('valid') and isinstance(value.get('rate_per_second'), (int, float))}
    if recommendation is None and rates and stop_reason in {'rental_deadline', 'maximum_safety_limit'}:
        recommendation = max(rates, key=rates.get)
    result = {
        'schema': 1, 'protocol': protocol,
        'status': ('cancelled' if stop_reason == 'user_cancelled' else 'completed' if recommendation else 'failed'),
        'hardware': hardware, 'cases': cases, 'population_size': recommendation,
        'stop_reason': stop_reason, 'wall_seconds': time.monotonic() - started, 'finished_at': time.time(),
    }
    write_record(ROOT / 'calibration-result.json', result)
    return result


def run() -> None:
    """Run at most one optimizer; duplicate starts cannot reset its state."""
    with (ROOT / "run.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if (ROOT / "finished.json").exists():
            return
        if (ROOT / "started.json").exists():
            # An earlier wrapper disappeared: do not launch a second optimizer.
            return
        install()
        hardware = health()
        config = json.loads((ROOT / "input/optimize.json").read_text())
        workers = config["optimize"]["n_cpus"]
        if workers > max(1, int(hardware["cpu_cores"])):
            raise ValueError("Requested exact workers exceed the container CPU quota")
        output = ROOT / "output"
        output.mkdir(exist_ok=True)
        write_record(ROOT / "started.json", {"started_at": time.time()})
        environment = {k: v for k, v in os.environ.items() if k not in {"CONTAINER_API_KEY"}}
        environment.update(PASSIVBOT_GPU_PROFILE="1", PYTHONUNBUFFERED="1", OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1")
        started = time.monotonic()
        cancelled = False
        calibration_path = ROOT / 'calibration.json'
        if calibration_path.is_file() and not calibration_path.is_symlink():
            if calibration_path.stat().st_size > 4096:
                raise ValueError('Oversized calibration plan')
            result = run_calibration(config, hardware, environment, json.loads(calibration_path.read_text()))
            write_record(ROOT / 'finished.json', {
                'exit_code': 0 if result['status'] == 'completed' else 1,
                'cancelled': result['status'] == 'cancelled',
                'wall_seconds': result['wall_seconds'], 'finished_at': result['finished_at'],
                'calibration': True,
            })
            return
        with (output / "optimizer.log").open("ab") as log:
            process = subprocess.Popen([sys.executable, "/opt/passivbot/src/optimize.py", str(ROOT / "input/optimize.json")],
                cwd=output, env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                while process.poll() is None:
                    cancelled = (ROOT / "stop").exists()
                    if cancelled or time.time() >= json.loads((GUARD_ROOT / "guard.json").read_text())["deadline"] - 180:
                        cancelled = True
                        break
                    time.sleep(2)
            finally:
                stop_process(process)
        write_record(ROOT / "finished.json", {"exit_code": process.returncode, "cancelled": cancelled,
                     "wall_seconds": time.monotonic() - started, "finished_at": time.time()})


def status() -> dict:
    """Return bounded progress without credential-bearing environment or files."""
    result = {"started": (ROOT / "started.json").exists(), "finished": None, "exact_completed": 0, "gpu_candidates": 0}
    if (ROOT / "finished.json").exists():
        result["finished"] = json.loads((ROOT / "finished.json").read_text())
    calibration = ROOT / 'calibration-result.json'
    if calibration.is_file() and not calibration.is_symlink() and calibration.stat().st_size <= 1024 * 1024:
        result['calibration'] = json.loads(calibration.read_text())
    case_root = ROOT / 'calibration-cases'
    if not (ROOT / 'finished.json').exists() and case_root.is_dir() and not case_root.is_symlink():
        active_cases = [path for path in case_root.iterdir()
                        if path.is_dir() and not path.is_symlink() and path.name.isdigit()]
        if active_cases:
            active = max(active_cases, key=lambda path: path.stat().st_mtime)
            live = calibration_progress(active / 'optimizer.log')
            calibration_state = dict(result.get('calibration') or {})
            calibration_state.update(status='running', active_population=int(active.name), active_progress=live)
            result['calibration'] = calibration_state
    log = ROOT / "output/optimizer.log"
    if log.exists():
        with log.open("rb") as stream:
            stream.seek(max(0, log.stat().st_size - 512 * 1024))
            lines = stream.read().decode(errors="replace").splitlines()
        import re
        for line in lines:
            match = re.search(r"Iter: (\d+)", line)
            if match:
                result["exact_completed"] = max(result["exact_completed"], int(match[1]))
            if "[gpu-profile] " in line:
                try:
                    event = json.loads(line.split("[gpu-profile] ", 1)[1])
                except ValueError:
                    continue
                result["exact_completed"] = max(result["exact_completed"], int(event.get("exact_completed") or 0))
                if event.get("generation"):
                    result["gpu_candidates"] = int(event["generation"]) * int(event.get("population_size") or 0)
    return result


def snapshot(final: bool) -> dict:
    """Archive immutable Pareto copies; include complete native files only at finish."""
    if final and not (ROOT / "finished.json").exists():
        raise ValueError("Optimizer must finish before final collection")
    with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
        stage = Path(temporary)
        files = []
        source_root = ROOT / "output"
        candidates = list(source_root.glob("optimize_results/*/pareto/*.json"))
        if final:
            candidates += list(source_root.glob("optimize_results/*/all_results.bin"))
            candidates += list(source_root.glob("optimize_results/*/checkpoint.pkl"))
        log = source_root / "optimizer.log"
        if log.exists():
            candidates.append(log)
        total = 0
        for source in candidates:
            relative = source.relative_to(source_root)
            if source.is_symlink():
                raise ValueError("Symlink in results")
            target = stage / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                before = source.stat()
                total += before.st_size
                if total > 2 * 1024**3:
                    raise ValueError("Result collection exceeds 2 GB limit")
                shutil.copyfile(source, target)
            except FileNotFoundError:
                if final:
                    raise
                continue
            files.append({"path": str(relative), "bytes": target.stat().st_size, "sha256": file_hash(target)})
        if final:
            shutil.copyfile(ROOT / "finished.json", stage / "finished.json")
            files.append({"path": "finished.json", "bytes": (stage / "finished.json").stat().st_size,
                          "sha256": file_hash(stage / "finished.json")})
            calibration = ROOT / 'calibration-result.json'
            if calibration.is_file() and not calibration.is_symlink():
                shutil.copyfile(calibration, stage / 'calibration-result.json')
                files.append({'path': 'calibration-result.json', 'bytes': (stage / 'calibration-result.json').stat().st_size,
                              'sha256': file_hash(stage / 'calibration-result.json')})
        write_record(stage / "manifest.json", {"files": files, "final": final})
        destination = ROOT / ("final.tar.gz" if final else "partial.tar.gz")
        temporary_archive = destination.with_suffix(".tmp")
        with tarfile.open(temporary_archive, "w:gz") as archive:
            for path in stage.rglob("*"):
                if path.is_file():
                    archive.add(path, arcname=str(path.relative_to(stage)), recursive=False)
        os.replace(temporary_archive, destination)
        return {"bytes": destination.stat().st_size, "sha256": file_hash(destination), "final": final}


def protocol_call(operation) -> dict:
    """Return actionable preflight errors without environment or traceback dumps."""
    try:
        return operation()
    except (ValueError, RuntimeError, ImportError, OSError) as exc:
        return {"error": type(exc).__name__ + ": " + str(exc)[:400]}


def main() -> None:
    """Expose only fixed operations used by the local supervisor."""
    os.umask(0o077)
    ROOT.mkdir(parents=True, exist_ok=True)
    action = sys.argv[1]
    if action == "guard":
        guard()
    elif action == "run":
        try:
            run()
        except Exception as exc:
            write_record(ROOT / "finished.json", {"exit_code": 1, "cancelled": False, "error": type(exc).__name__, "finished_at": time.time()})
    elif action == "health":
        print(json.dumps(protocol_call(health)))
    elif action == "cache-missing":
        print(json.dumps(protocol_call(cache_missing)))
    elif action == "install":
        print(json.dumps(protocol_call(install)))
    elif action == "status":
        print(json.dumps(status()))
    elif action == "stop":
        (ROOT / "stop").touch()
        print('{"stop_requested":true}')
    elif action in ("partial", "final"):
        print(json.dumps(snapshot(action == "final")))
    else:
        raise ValueError("Unknown operation")


if __name__ == "__main__":
    main()
