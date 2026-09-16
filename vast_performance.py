"""Private SQLite benchmark history for frozen Vast workloads and observed counters."""
from __future__ import annotations

from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import zipfile

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from secure_files import ensure_private_directory, secure_private_file
from vast_jobs import job_id
from vast_provider import VastError
from vast_throughput import observe_throughput

SERVICE = 'VastPerformance'
TERMINAL = {'completed', 'failed', 'cancelled'}
HARDWARE = ('machine_id', 'gpu_name', 'vram_gb', 'cpu_cores', 'cpu_name', 'ram_gb',
            'gpu_mem_bw_gbps', 'pcie_bw_gbps', 'price_hour_usd', 'download_gb_usd', 'upload_gb_usd')
METRICS = ('gpu_percent', 'cpu_percent', 'ram_used_bytes', 'ram_total_bytes',
           'vram_used_bytes', 'vram_total_bytes')


def encoded(value):
    """Use stable JSON for hashes and strict database payloads."""
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    """Identify a workload without persisting its complete configuration."""
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def load_config(path):
    """Use PBGui's canonical PB8 config loader."""
    from pb8_config import load_pb8_config
    return load_pb8_config(path)


def candle_rows(path):
    """Read only a NumPy header, never materialize candle arrays or allow pickle."""
    from numpy.lib import format as fmt

    def header(stream):
        """Return the row count of a numeric two-dimensional candle array."""
        version = fmt.read_magic(stream)
        if version not in ((1, 0), (2, 0)):
            raise ValueError('Unsupported candle header')
        shape, _, dtype = (fmt.read_array_header_1_0(stream) if version == (1, 0)
                           else fmt.read_array_header_2_0(stream))
        if dtype.hasobject or len(shape) != 2 or shape[1] < 5:
            raise ValueError('Invalid candle shape')
        return int(shape[0])

    if path.suffix == '.npy':
        with path.open('rb') as stream:
            return header(stream)
    with zipfile.ZipFile(path) as archive:
        with archive.open('candles.npy') as stream:
            return header(stream)


def workload_metadata(store, row, image, stop=None):
    """Describe frozen exported input; consumed runtime candle counts remain unknown."""
    folder = store.directory(row['id']) / 'input'
    config_path = folder / 'optimize.json'
    if (not (folder / 'manifest.json').is_file() or not config_path.is_file()
            or folder.is_symlink() or config_path.is_symlink()):
        return {'fingerprint': None, 'reason': 'Frozen input unavailable'}
    manifest = store.read(row['id'], 'input/manifest.json')
    if config_path.stat().st_size > 16 * 1024**2:
        raise ValueError('Oversized optimizer config')
    if hashlib.sha256(config_path.read_bytes()).hexdigest() != manifest.get('config_sha256'):
        return {'fingerprint': None, 'reason': 'Frozen config checksum mismatch'}
    config = copy.deepcopy(load_config(config_path))
    config.pop('pbgui', None)
    config.get('live', {}).pop('user', None)
    bt, opt = config.get('backtest', {}), config.get('optimize', {})
    for key in ('ohlcv_source_dir', 'hlcvs_data_dir', 'base_dir'):
        bt.pop(key, None)
    opt.pop('n_cpus', None)
    opt.get('gpu', {}).pop('exact_workers', None)
    files = []
    rows, counted, symbols, exchanges, resolutions = 0, 0, set(), set(), set()
    for entry in manifest.get('files', []):
        if stop and stop.is_set():
            raise InterruptedError('Performance collection stopped')
        relative = Path(entry.get('path', ''))
        if (relative.is_absolute() or '..' in relative.parts or '\\' in str(relative)
                or any(ord(c) < 32 for c in str(relative)) or len(relative.parts) != 5
                or relative.parts[0] != 'ohlcv' or relative.suffix not in ('.npz', '.npy')
                or not re.fullmatch(r'[a-f0-9]{64}', str(entry.get('sha256', '')))):
            return {'fingerprint': None, 'reason': 'Unsupported input manifest'}
        files.append({key: entry.get(key) for key in ('path', 'sha256', 'bytes')})
        exchanges.add(relative.parts[1]); resolutions.add(relative.parts[2])
        symbols.add((relative.parts[1], relative.parts[3]))
        path = folder / relative
        try:
            path.resolve(strict=True).relative_to(folder.resolve(strict=True))
            if path.is_symlink():
                raise ValueError('Linked candle file')
            rows += candle_rows(path); counted += 1
        except (OSError, ValueError, KeyError, zipfile.BadZipFile):
            # Metadata explicitly records incomplete coverage; no estimated row count.
            _log(SERVICE, 'Exported candle header unavailable for ' + row['id'], level='WARNING')
    revision = manifest.get('pb8_revision')
    identity = {'schema': 1, 'config': config, 'files': sorted(files, key=lambda x: x['path']),
                'revision': revision, 'image': image,
                'sweep_plan': manifest.get('sweep_plan'), 'validation_plan': manifest.get('validation_plan')}
    approved = config.get('live', {}).get('approved_coins', {})
    coins = sorted(set(approved.get('long', []) + approved.get('short', []))) if isinstance(approved, dict) else []
    bounds = opt.get('bounds', {})
    parameters = sum(isinstance(value, list) and len(value) >= 2 and value[0] != value[1]
                     for value in bounds.values()) if isinstance(bounds, dict) else None
    fingerprint = digest(identity) if files and revision and image else None
    return dict(fingerprint=fingerprint, reason=None if fingerprint else 'Incomplete workload identity',
                revision=revision, image=image, coins=coins, coin_count=len(coins), exchanges=sorted(exchanges),
                exported_symbols=len(symbols), resolutions=sorted(resolutions),
                scenario_count=len(bt.get('scenarios') or []) or 1, scenarios=bt.get('scenarios') or [],
                start_date=bt.get('start_date'), end_date=bt.get('end_date'),
                exported_candles=rows if counted == len(files) and files else None,
                counted_files=counted, data_files=len(files), data_bytes=sum(x.get('bytes') or 0 for x in files),
                consumed_candles=None, parameter_count=parameters, objectives=opt.get('scoring', []),
                population_size=opt.get('population_size') or opt.get('gpu', {}).get('population_size'),
                algorithm=opt.get('backend'), seed=opt.get('seed'), iterations=opt.get('iters'),
                gpu_settings={key: value for key, value in opt.get('gpu', {}).items()
                              if key in {'population_size', 'batch_size', 'dtype', 'precision', 'fidelity',
                                         'exact_batch_size', 'proxy_batch_size', 'auto_lean_parallelism'}},
                dataset_fingerprint=digest(identity['files']))


def summarize(samples, price=None):
    """Weight throughput by covered seconds and never bridge reset counters."""
    previous = None
    duration = proxy = exact = 0
    proxy_rates, exact_rates = [], []
    for sample in samples:
        if previous:
            seconds = sample['sampled_at'] - previous['sampled_at']
            dp, de = sample['proxy_total'] - previous['proxy_total'], sample['exact_total'] - previous['exact_total']
            if seconds >= 10 and dp >= 0 and de >= 0:
                duration += seconds; proxy += dp; exact += de
                proxy_rates.append(60 * dp / seconds); exact_rates.append(60 * de / seconds)
        previous = sample
    return dict(covered_seconds=duration, proxy_delta=proxy, exact_delta=exact,
                proxy_per_minute=60 * proxy / duration if duration else None,
                exact_per_minute=60 * exact / duration if duration else None,
                proxy_rate_min=min(proxy_rates) if proxy_rates else None,
                proxy_rate_max=max(proxy_rates) if proxy_rates else None,
                exact_rate_min=min(exact_rates) if exact_rates else None,
                exact_rate_max=max(exact_rates) if exact_rates else None,
                exact_per_usd=3600 * exact / duration / price if duration and price and price > 0 else None,
                samples=len(samples))


class PerformanceHistory:
    """Own short-lived SQLite connections; history is independent of queue job retention."""

    def __init__(self, root):
        """Place the private database beside the Vast queue."""
        self.root = Path(root)
        self.path = self.root / 'performance.sqlite3'

    @contextmanager
    def connection(self):
        """Serialize schema and transactions across API/collector processes."""
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root / '.performance-db'):
            secure_private_file(self.path)
            if not self.path.exists():
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(fd)
            connection = sqlite3.connect(self.path, timeout=10)
            connection.row_factory = sqlite3.Row
            try:
                if connection.execute('PRAGMA user_version').fetchone()[0] not in (0, 1):
                    raise ValueError('Unsupported performance database version')
                connection.executescript('''
                    CREATE TABLE IF NOT EXISTS runs (
                      id TEXT PRIMARY KEY, fingerprint TEXT, updated REAL NOT NULL, payload TEXT NOT NULL);
                    CREATE INDEX IF NOT EXISTS runs_workload ON runs(fingerprint,updated);
                    CREATE TABLE IF NOT EXISTS samples (
                      run_id TEXT NOT NULL, kind TEXT NOT NULL, bucket INTEGER NOT NULL,
                      stamp REAL NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(run_id,kind,bucket));
                    PRAGMA user_version=1;
                ''')
                with connection:
                    yield connection
            finally:
                connection.close()
                secure_private_file(self.path)

    def get(self, identifier):
        """Return a retained run including aggregate statistics."""
        with self.connection() as db:
            row = db.execute('SELECT payload FROM runs WHERE id=?', (job_id(identifier),)).fetchone()
        return json.loads(row['payload']) if row else None

    def record(self, run, samples, telemetry=None):
        """Idempotently keep the latest counter/telemetry observation per minute."""
        identifier = job_id(run['id'])
        with self.connection() as db:
            for kind, values in (('counter', samples), ('telemetry', [telemetry] if telemetry else [])):
                for sample in values:
                    stamp = sample['sampled_at']
                    db.execute('''INSERT INTO samples VALUES (?,?,?,?,?)
                      ON CONFLICT(run_id,kind,bucket) DO UPDATE SET stamp=excluded.stamp,payload=excluded.payload
                      WHERE excluded.stamp > samples.stamp''',
                               (identifier, kind, int(stamp // 60), stamp, encoded(sample)))
            counters = [json.loads(row[0]) for row in db.execute(
                "SELECT payload FROM samples WHERE run_id=? AND kind='counter' ORDER BY stamp", (identifier,))]
            old = db.execute('SELECT payload FROM runs WHERE id=?', (identifier,)).fetchone()
            old = json.loads(old[0]) if old else {}
            if (old.get('source_generation') or 0) > (run.get('source_generation') or 0):
                run = copy.deepcopy(old)
            run['summary'] = summarize(counters, run.get('hardware', {}).get('price_hour_usd'))
            if counters:
                run['first_counter_at'] = min([item['sampled_at'] for item in samples] +
                                               [old.get('first_counter_at') or counters[0]['sampled_at']])
                start = run.get('setup_started_at') or run.get('created_at')
                run['startup_to_first_counter_seconds'] = max(0, run['first_counter_at'] - start) if start else None
                run['summary']['proxy_total'] = counters[-1]['proxy_total']
                run['summary']['exact_total'] = counters[-1]['exact_total']
            run['fingerprint'] = run.get('workload', {}).get('fingerprint')
            db.execute('''INSERT INTO runs VALUES (?,?,?,?) ON CONFLICT(id) DO UPDATE SET
                fingerprint=excluded.fingerprint,updated=excluded.updated,payload=excluded.payload''',
                       (identifier, run['fingerprint'], run['captured_at'], encoded(run)))
        return run

    def list_runs(self, limit=100, offset=0, fingerprint=None):
        """Page retained runs with optional strict workload filtering."""
        clause, args = (' WHERE fingerprint=?', [fingerprint]) if fingerprint else ('', [])
        with self.connection() as db:
            total = db.execute('SELECT count(*) FROM runs' + clause, args).fetchone()[0]
            rows = db.execute('SELECT payload FROM runs' + clause + ' ORDER BY updated DESC,id LIMIT ? OFFSET ?',
                              args + [limit, offset]).fetchall()
            counts = {(row[0], row[1]): row[2] for row in db.execute(
                "SELECT fingerprint,json_extract(payload,'$.hardware.machine_id'),count(*) FROM runs "
                "WHERE fingerprint IS NOT NULL AND json_extract(payload,'$.summary.covered_seconds') > 0 "
                "GROUP BY fingerprint,json_extract(payload,'$.hardware.machine_id')")}
        values = [json.loads(row[0]) for row in rows]
        for value in values:
            machine = value.get('hardware', {}).get('machine_id')
            value['comparable_host_runs'] = counts.get((value.get('fingerprint'), machine), 0) if machine else None
        return {'runs': values, 'total': total, 'offset': offset, 'limit': limit}

    def series(self, identifier):
        """Return one rental's bounded, minute-resolution counter and utilization history."""
        with self.connection() as db:
            rows = db.execute('SELECT kind,payload FROM samples WHERE run_id=? ORDER BY stamp LIMIT 10000',
                              (job_id(identifier),)).fetchall()
        return {kind: [json.loads(row['payload']) for row in rows if row['kind'] == kind]
                for kind in ('counter', 'telemetry')}


def collect_run(store, history, row, log_root, stop=None):
    """Snapshot a job from local immutable inputs and already synchronized logs only."""
    identifier = job_id(row['id'])
    prior = history.get(identifier)
    if (prior and row.get('status') in TERMINAL and prior.get('source_generation') == row.get('generation')
            and prior.get('workload', {}).get('fingerprint')):
        return prior
    lease = row.get('lease_id') or identifier
    try:
        intent_path = store.directory(lease) / 'intent.json'
        intent = store.read(lease, 'intent.json') if intent_path.is_file() else {}
    except VastError as exc:
        if exc.status != 404:
            raise
        intent = {}  # Already-retained history does not depend on rental retention.
    offer = intent.get('offer') or (prior or {}).get('hardware', {})
    image = intent.get('image') or (prior or {}).get('workload', {}).get('image')
    workload = prior.get('workload') if prior else None
    if not workload or not workload.get('dataset_fingerprint') or workload.get('image') != image:
        try:
            workload = workload_metadata(store, row, image, stop)
        except InterruptedError:
            raise
        except Exception as exc:
            _log(SERVICE, 'Workload identity unavailable: ' + type(exc).__name__, level='WARNING')
            workload = {'fingerprint': None, 'reason': 'Workload metadata unavailable', 'image': image}
    log = Path(log_root) / ('vast_' + identifier + '.log')
    if log.is_file() and not log.is_symlink():
        with log.open('rb') as stream:
            stream.seek(max(0, os.fstat(stream.fileno()).st_size - 65536))
            observe_throughput(store, identifier, stream.read(65536))
        row = store.read(identifier)
    throughput = row.get('throughput') or {}
    now = time.time()
    run = {key: row.get(key) for key in ('id', 'config_name', 'status', 'created_at', 'started_at',
        'setup_started_at', 'finished_at', 'elapsed_seconds', 'stop_reason', 'deleted_at')}
    run.update(captured_at=now, source_generation=row.get('generation'), workload=workload,
               hardware={key: offer.get(key) for key in HARDWARE}, workers=row.get('workers'),
               cpu_allocation_resolved=row.get('cpu_allocation_resolved', False))
    if row.get('status') in TERMINAL:
        run['ended_at'] = row.get('finished_at') or (prior or {}).get('ended_at') or row.get('updated_at')
    start = row.get('setup_started_at')
    end = run.get('ended_at') or now
    price = offer.get('price_hour_usd')
    incoming = row.get('transfer_input_bytes', 0) if row.get('uploaded') else (row.get('upload_progress') or {}).get('bytes', 0)
    transfer = (incoming or 0) / 1e9 * (offer.get('download_gb_usd') or 0) + (row.get('downloaded_bytes') or 0) / 1e9 * (offer.get('upload_gb_usd') or 0)
    compute = max(0, min(end, intent.get('deadline') or end) - start) / 3600 * price if start and price else None
    run['cost_estimate'] = {'compute_usd': compute, 'transfer_usd': transfer,
                            'total_usd': compute + transfer if compute is not None else None}
    metrics = row.get('runtime_metrics') or {}
    stamp = metrics.get('sampled_at')
    fresh = metrics.get('available') and isinstance(stamp, (int, float)) and 0 <= now - stamp <= 90
    telemetry = {'sampled_at': run.get('ended_at') or now, 'phase': row.get('status'),
                 'metrics_sampled_at': stamp if fresh else None,
                 **{key: metrics.get(key) if fresh else None for key in METRICS},
                 'exact_outstanding': (row.get('exact_queue') or {}).get('outstanding')
                     if now - (row.get('exact_queue') or {}).get('sampled_at', 0) <= 90 else None}
    run['last_metrics_sampled_at'] = stamp if fresh else (prior or {}).get('last_metrics_sampled_at')
    if prior and prior.get('status') == row.get('status') and (not fresh or stamp == prior.get('last_metrics_sampled_at')):
        telemetry = None
    return history.record(run, throughput.get('samples') or [], telemetry)


class PerformanceCollector:
    """API-owned local sampler with interruptible scheduling and deterministic shutdown."""

    def __init__(self, store, log_root, interval=60):
        """Bind the collector to one store; constructors do not start background work."""
        self.store, self.log_root, self.interval = store, Path(log_root), interval
        self.history = PerformanceHistory(store.root)
        self.stop_event = threading.Event()
        self.thread = None

    def scan(self):
        """Isolate individual corrupt/deleted jobs and observe cancellation between jobs."""
        folder = self.store.root / 'jobs'
        if not folder.exists():
            return
        for directory in folder.iterdir():
            if self.stop_event.is_set():
                break
            if not re.fullmatch(r'[a-f0-9]{32}', directory.name) or directory.is_symlink() or not directory.is_dir():
                continue
            try:
                row = self.store.read(directory.name)
                if row.get('kind') == 'worker':
                    continue
                collect_run(self.store, self.history, row, self.log_root, self.stop_event)
            except InterruptedError:
                break
            except Exception as exc:
                _log(SERVICE, 'Performance snapshot unavailable: ' + type(exc).__name__, level='WARNING')

    def run(self):
        """Keep recording without an open browser while the API is running."""
        while not self.stop_event.is_set():
            try:
                self.scan()
            except Exception as exc:
                _log(SERVICE, 'Performance scan unavailable: ' + type(exc).__name__, level='WARNING')
            if self.stop_event.wait(self.interval):
                break

    def start(self):
        """Start exactly one owned thread."""
        if self.thread is None:
            self.thread = threading.Thread(target=self.run, name='vast-performance', daemon=False)
            try:
                self.thread.start()
            except Exception:
                self.thread = None
                raise

    def stop(self):
        """Signal immediately; the API joins outside its event loop."""
        self.stop_event.set()

    def join(self):
        """Drain local reads and release the thread reference."""
        if self.thread is not None:
            self.thread.join()
            self.thread = None
