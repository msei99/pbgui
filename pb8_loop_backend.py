"""Owned PB8 optimizer/backtest jobs and scoped Vast rentals for loop experiments."""
from __future__ import annotations

import copy
import hashlib
from pathlib import Path
import time

from fastapi import HTTPException

from pb8_loop_store import digest, evaluate, validate_change, apply_run_limit, windows, configured_direction
from file_lock import advisory_file_lock

SERVICE = 'PB8Loop'


def migrate_loop_bundle(bundle):
    """Normalize a captured input through native PB8 without writing its source."""
    from tempfile import TemporaryDirectory
    from api import optimize_v8 as opt
    from pb8_config import PB8ConfigurationError, preview_pb8_hsl_migration, save_prepared_pb8_config

    config = copy.deepcopy(bundle['config'])
    if bundle.get('migration_unresolved'):
        raise ValueError('Starting config requires an explicit HSL choice: '
                         + ', '.join(bundle['migration_unresolved']))
    overrides = opt._resolve_override_payloads(config, bundle.get('override_configs'), None)
    with TemporaryDirectory(prefix='pbgui-loop-migration-') as temporary:
        path = Path(temporary) / 'optimize.json'
        opt._write_override_payloads(path.parent, overrides)
        save_prepared_pb8_config(config, path)
        try:
            # Run the native migration directly; a temporary snapshot must not
            # inherit a prepared-config cache entry from writing the input.
            prepared = preview_pb8_hsl_migration(path, {})
        except PB8ConfigurationError as exc:
            if getattr(exc, 'retryable', False):
                raise HTTPException(status_code=getattr(exc, 'status_code', 503), detail=str(exc)) from exc
            raise ValueError(str(exc)) from exc
        if prepared.get('migration_unresolved'):
            raise ValueError('Starting config requires an explicit HSL choice: '
                             + ', '.join(prepared['migration_unresolved'])
                             + '. Open the PB8 starting config, select its HSL policy and Save.')
        config = prepared['config']
        overrides = opt._load_override_payloads(config, path.parent)
    return {'config': config, 'override_configs': overrides}


def normalize_suite_coins(config):
    """Mirror a disabled side's coin selection without changing trading direction."""
    if not config.get('backtest', {}).get('suite_enabled'):
        return
    approved = config.get('live', {}).get('approved_coins')
    if not isinstance(approved, dict) or approved.get('long') == approved.get('short'):
        return
    direction = configured_direction(config)
    if direction == 'both':
        raise ValueError('PB8 Suite requires identical live.approved_coins.long and .short lists. '
                         'Both directions are active; select a common coin universe before starting.')
    disabled = 'short' if direction == 'long' else 'long'
    approved[disabled] = copy.deepcopy(approved.get(direction, []))


class LoopBackend:
    """Use existing managed PB8 APIs without modifying a user's source configuration."""
    def __init__(self, root, store):
        self.root = Path(root)
        self.store = store

    def initial(self, name, settings, bundle=None):
        """Resolve a managed input bundle and apply explicit user goal selections."""
        from api import optimize_v8 as opt
        opt._validate_name(name)
        path = opt._config_file(name)
        if bundle is None:
            if not path.is_file() or path.is_symlink():
                raise ValueError('Select an existing PB8 optimizer configuration')
            bundle = opt.get_config(name, session=None)
        bundle = migrate_loop_bundle(bundle)
        config, overrides = bundle['config'], bundle['override_configs']
        if settings['execution'] == 'vast':
            from vast_jobs import REVISION
            # This immutable worker revision supports only schema v8.4.0.
            # Do not rent it for a newly migrated, incompatible local snapshot.
            version = config.get('config_version')
            if (REVISION == '7b639e1180fa6bfe02e110429d4f931933c73089' and version
                    and tuple(int(part) for part in version.removeprefix('v').split('.')) > (8, 4, 0)):
                raise ValueError(f'Vast.ai worker supports schema v8.4.0, but the migrated starting config uses {version}. '
                                 'Update the Vast.ai worker image before queuing, or use Local CPU/GPU. Save remains available.')
        config = apply_run_limit(settings, config)
        config.setdefault('optimize', {})['backend'] = 'pymoo' if settings['execution'] == 'cpu' else 'gpu'
        config.setdefault('pbgui', {})['execution'] = 'vast' if settings['execution'] == 'vast' else 'local'
        config['pbgui'].setdefault('optimize_runtime', {'mode': 'fresh'})
        goals = settings['goals']
        use_config = goals['direction'] == 'config'
        direction = configured_direction(config) if use_config else goals['direction']
        total = goals.get('positions')
        counts = goals.get('positions_by_side') or {}
        if counts and direction != 'both' and counts.get('short' if direction == 'long' else 'long'):
            raise ValueError('Position split conflicts with the direction of the starting config')
        if total is not None and not counts:
            counts = {'long': total, 'short': 0} if direction == 'long' else (
                     {'long': 0, 'short': total} if direction == 'short' else {'long': (total + 1) // 2, 'short': total // 2})
        for side in ('long', 'short'):
            active = direction in {'both', side}
            risk = config['bot'][side].setdefault('risk', {})
            if (not active and not use_config) or side in counts:
                capacity = counts.get(side, 0) if active else 0
                risk['n_positions'] = capacity
                config['optimize'].setdefault('bounds', {}).setdefault(side, {}).setdefault('risk', {})['n_positions'] = [capacity, capacity]
                config['optimize'].setdefault('fixed_runtime_overrides', {}).pop(f'bot.{side}.risk.n_positions', None)
            if not active and not use_config:
                risk['total_wallet_exposure_limit'] = 0
                config['optimize'].setdefault('bounds', {}).setdefault(side, {}).setdefault('risk', {})['total_wallet_exposure_limit'] = [0, 0]
            if goals.get('coins'):
                if not isinstance(config['live'].get('approved_coins'), dict):
                    config['live']['approved_coins'] = {}
                config['live'].setdefault('approved_coins', {})[side] = goals['coins'] if active else []
        normalize_suite_coins(config)
        runtime = opt.pb8_runtime_status()
        if not runtime.get('ready'):
            raise ValueError('PB8 runtime is not ready')
        if settings['execution'] == 'gpu':
            if bundle is None:
                opt._validate_optimize_backend(config, base_config_path=str(path))
            else:
                # Preflight the captured overrides, including when the source was deleted.
                from tempfile import TemporaryDirectory
                from pb8_config import save_prepared_pb8_config
                with TemporaryDirectory(prefix='pbgui-loop-preflight-') as temporary:
                    destination = Path(temporary) / 'optimize.json'
                    opt._write_override_payloads(destination.parent, overrides)
                    save_prepared_pb8_config(config, destination)
                    opt._validate_optimize_backend(config, base_config_path=str(destination))
        from scenario_windows import build_validation_plan
        plan = build_validation_plan(config)
        holdouts = copy.deepcopy((plan or {}).get('holdout_scenarios') or [])
        for holdout in holdouts:
            for key in ('exchanges', 'starting_balance'):
                holdout.setdefault(key, config['backtest'].get(key))
        comparison = copy.deepcopy(config['backtest'])
        return config, overrides, self.fingerprint(config), holdouts, comparison

    def fingerprint(self, config):
        """Track the actual PB8 checkout and relevant available market-data shards."""
        from api import optimize_v8 as opt
        runtime = opt.pb8_runtime_status()
        version = opt._runtime_commit(Path(runtime['pb8dir'])) if runtime.get('ready') else ''
        from market_data import get_market_data_root_dir
        from setup.vast_gpu_benchmark.prepare import select_shards
        try:
            shards = select_shards(config, Path(get_market_data_root_dir()), self.root / 'data/coindata')
            files = shards
        except (ValueError, OSError, KeyError) as exc:
            from logging_helpers import human_log
            human_log(SERVICE, f'Loop data fingerprint unavailable: {type(exc).__name__}', level='WARNING')
            root = Path(get_market_data_root_dir())
            files = sorted(((path, path.relative_to(root)) for path in root.rglob('*')
                            if path.is_file() and not path.is_symlink() and path.suffix in {'.npy', '.npz', '.json', '.csv'}),
                           key=lambda item: str(item[1])) if root.is_dir() else []
        signatures, metadata = [], []
        for path, relative in files:
            before = path.stat()
            with path.open('rb') as source:
                content = hashlib.file_digest(source, 'sha256').hexdigest()
            after = path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                raise ValueError('Market data changed while fingerprinting; retry before launching')
            signatures.append((str(relative), content))
            metadata.append((str(relative), before.st_size, before.st_mtime_ns))
        dataset = digest(signatures) if signatures else None
        return {'pb8': version, 'data': dataset, 'data_status': 'verified_files' if dataset else 'unavailable',
                'data_signature_version': 2, 'metadata_data': digest(metadata) if metadata else None}

    def capacity(self, record):
        """Include other local jobs and every unconfirmed Vast rental in slot use."""
        from api import optimize_v8 as opt, backtest_v8 as bt
        execution = record['settings']['execution']
        if execution == 'vast':
            from vast_queue import CloudQueue
            queue = CloudQueue()
            limits = record['settings']['vast']
            active = [row for row in queue.store.list() if row.get('kind') == 'worker'
                      and row.get('rental_state') not in {'none', 'deletion_verified'}]
            idle_owned = sum(row.get('loop_id') == record['id'] and not row.get('active_job') for row in active)
            return max(0, min(record['settings']['parallel'], limits['max_rentals'] - len(active) + idle_owned))
        import psutil
        busy = sum(row['status'] == 'running' for row in opt._load_queue()) + sum(row['status'] == 'running' for row in bt._load_queue())
        if psutil.virtual_memory().available < 512 * 1024 * 1024:
            return 0
        if execution == 'gpu':
            return 0 if busy else 1
        n_cpus = max(1, int(record['initial_config']['optimize'].get('n_cpus') or 1))
        return max(0, min(record['settings']['parallel'], (psutil.cpu_count() or 1) // n_cpus - busy))

    def authorized(self, record, observer=False):
        current = self.store.read(record['owner'], record['id'])
        allowed = {'running', 'finishing', 'completed', 'unconfirmed', 'failed'} if observer else {'running', 'finishing'}
        if current['status'] not in allowed or time.time() >= current['deadline'] - 60:
            raise ValueError('Loop is paused, stopped or its time allowance is exhausted')
        return current

    def prepare(self, record, job):
        """Create an idempotent owned job, then leave launching to the controller."""
        from api import optimize_v8 as opt, backtest_v8 as bt
        config, overrides, name = copy.deepcopy(job['config']), job['overrides'], job['name']
        self.authorized(record, observer=bool(job.get('observer_only')))
        if job['kind'] == 'optimizer':
            validate_change(record, config, overrides)
            prepared = opt._save_config_bundle(name, config, override_payloads=overrides, preserve_hsl_runtime_overrides=True)
            validate_change(record, prepared, overrides)
            if record['settings']['execution'] == 'vast':
                from vast_jobs import JobStore, digest as file_digest
                cloud = JobStore()
                existing = next((row for row in cloud.list() if row.get('loop_operation') == job['operation']), None)
                if existing:
                    if existing['status'] == 'preparing':
                        cloud.update(existing['id'], status='failed', error='Preparation was interrupted; this attempt was not launched')
                    return {'id': existing['id'], 'execution': 'vast'}
                with advisory_file_lock(cloud.root / '.queue-lock'):
                    entry = cloud.create_preparation(name, int(prepared['optimize'].get('iters') or 512),
                                                     int((prepared['optimize'].get('gpu') or {}).get('exact_workers') or prepared['optimize'].get('n_cpus') or 1), False)
                    cloud.update(entry['id'], loop_id=record['id'], loop_owner=record['owner'], loop_operation=job['operation'])
                from market_data import get_market_data_root_dir
                cloud.prepare(name, prepared, file_digest(opt._config_file(name)), Path(get_market_data_root_dir()),
                              self.root / 'data/coindata', opt._results_root(), entry['iterations'], entry['workers'], False,
                              identifier=entry['id'])
                return {'id': entry['id'], 'execution': 'vast'}
            options = opt._runtime_options_from_config(prepared)
            with opt._queue_lock():
                result = opt._create_queue_record(name, prepared, opt._validate_launch_options(options), overrides, job['operation'])
                path = opt._queue_file(result['filename'])
                data = opt._read_json(path)
                data.update(loop_id=record['id'], loop_owner=record['owner'])
                opt._write_json(path, data)
            return {'id': result['filename'], 'execution': record['settings']['execution']}
        with bt._queue_lock():
            result = bt.add_to_queue({'name': name, 'config': config, 'override_configs': overrides,
                                      'operation_id': job['operation']}, session=None)
            path = bt._queue_file(result['filename'])
            data = bt._read_json(path)
            data.update(loop_id=record['id'], loop_owner=record['owner'])
            if job.get('observer_only'):
                data['loop_observer'] = True
            bt.atomic_write_json(path, data)
        return {'id': result['filename'], 'execution': 'validation'}

    def start(self, record, job):
        """Serialize final authorization with pause/stop and the native launch."""
        with advisory_file_lock(self.store.path(record['owner'], record['id'])):
            with advisory_file_lock(self.store.root / '.execution'):
                current = self.authorized(record, observer=bool(job.get('observer_only')))
                resource_check = copy.deepcopy(current)
                resource_check['initial_config'] = copy.deepcopy(job['config'])
                if job['kind'] != 'optimizer':
                    resource_check['settings']['execution'] = 'cpu'
                    # Native backtests use their own slots, not cloud optimizer parallelism.
                    resource_check['initial_config'].setdefault('optimize', {})['n_cpus'] = 1
                if self.capacity(resource_check) <= 0:
                    return self.poll(current, job)
                return self._start_locked(current, job)

    def _start_locked(self, record, job):
        """Reconcile before every launch and preserve native checkpoint/runtime checks."""
        self.authorized(record, observer=bool(job.get('observer_only')))
        fingerprint = self.fingerprint(record['initial_config'])
        recorded = record['fingerprint']
        comparable = (fingerprint.get('pb8') == recorded.get('pb8')
                      and fingerprint.get('data_status') == recorded.get('data_status'))
        comparable = comparable and (fingerprint.get('data') == recorded.get('data')
            if recorded.get('data_signature_version') == 2 else
            fingerprint.get('metadata_data', fingerprint.get('data')) == recorded.get('data'))
        if not comparable:
            if job.get('observer_only'):
                raise ValueError('PB8 version or input data changed; user-only backtest cannot be compared')
            self.store.update(record['owner'], record['id'], lambda current: current.update(status='unconfirmed', reason='PB8 version or input data changed; comparison was stopped before a new launch', control_generation=current['control_generation'] + 1))
            raise ValueError('PB8 version or input data changed')
        if fingerprint.get('data_signature_version') == 2 and recorded.get('data_signature_version') != 2:
            # Upgrade only when the old metadata signature still proves the
            # reviewed dataset is unchanged. An already changed legacy snapshot
            # requires an independently verified recovery or a new run.
            def upgrade(current):
                if current['fingerprint'] != recorded:
                    raise ValueError('Loop fingerprint changed before launch')
                current['fingerprint'] = fingerprint
            self.store.update(record['owner'], record['id'], upgrade)
        state = self.poll(record, job)
        if state['status'] != 'queued':
            return state
        if job['backend']['execution'] == 'vast':
            self._rent(record)
            return self.poll(record, job)
        from api import optimize_v8 as opt, backtest_v8 as bt
        if job['kind'] == 'optimizer':
            opt._worker.launch(job['backend']['id'], None, False, loop_id=record['id'])
        else:
            bt._worker.launch(job['backend']['id'], loop_id=record['id'])
        return self.poll(record, job)

    def _rent(self, record):
        """Rent only for owned jobs within the frozen and current shared limits."""
        from vast_queue import CloudQueue
        from vast_pool import select_offer
        queue = CloudQueue()
        settings = record['settings']['vast']
        active = queue.workers()
        if any(row.get('loop_id') == record['id'] and not row.get('active_job') for row in active):
            return
        if len(active) >= settings['max_rentals']:
            return
        class ScopedQueue(CloudQueue):
            """Expose only this loop's prepared jobs to marketplace selection."""
            def waiting(self, loop_id=None):
                return super().waiting(record['id'])
        scoped = ScopedQueue(queue.store)
        offer = select_offer(scoped, settings)
        self.authorized(record)
        scoped.start(offer, settings['hours'], settings['budget'], settings['idle_seconds'],
                     loop_id=record['id'], loop_owner=record['owner'], loop_limits=settings)

    def poll(self, record, job):
        """Read authoritative owned runner state and reconcile persistent launch intent."""
        from api import optimize_v8 as opt, backtest_v8 as bt
        backend = job['backend']
        if backend['execution'] == 'vast':
            from vast_jobs import JobStore
            row = JobStore().read(backend['id'])
            if row.get('loop_id') != record['id'] or row.get('loop_owner') != record['owner']:
                raise ValueError('Cloud job ownership mismatch')
            status = 'queued' if row['status'] == 'ready' else row['status']
            input_path = JobStore().directory(backend['id']) / 'input/optimize.json'
            executed = opt._read_json(input_path) if input_path.is_file() and not input_path.is_symlink() else None
            proxy = None
            if record['settings'].get('run_limit_mode') == 'proxy':
                log_path = opt._safe_path(opt._log_dir() / ('vast_' + backend['id'] + '.log'), opt._log_dir())
                proxy = opt._parse_optimize_log_status(opt._read_optimize_log_excerpt(log_path)).get('proxy_evaluations')
            return {'status': status, 'started': bool(row.get('dispatch_at')), 'source': row, 'executed_config': executed,
                    'proxy_evaluations': proxy}

        api = opt if job['kind'] == 'optimizer' else bt
        data = api._read_json(api._queue_file(backend['id']))
        if data.get('loop_id') != record['id'] or data.get('loop_owner') != record['owner']:
            raise ValueError('Queue job ownership mismatch')
        status, pid = api._queue_status(data)
        executed_path = opt._launch_config_file(backend['id']) if job['kind'] == 'optimizer' else bt._snapshot_file(backend['id'])
        executed = api._read_json(executed_path) if executed_path.is_file() and not executed_path.is_symlink() else None
        proxy = None
        if job['kind'] == 'optimizer' and record['settings'].get('run_limit_mode') == 'proxy':
            log_path = opt._safe_path(opt._log_dir() / (backend['id'] + '.log'), opt._log_dir())
            proxy = opt._parse_optimize_log_status(opt._read_optimize_log_excerpt(log_path)).get('proxy_evaluations')
        runner = api._read_runner_state(backend['id']) or {}
        source = dict(data, started_at=data.get('started_at') or runner.get('started_at'), error=runner.get('error') or data.get('error'))
        return {'status': status, 'started': bool(data.get('started_at') or pid or runner), 'proxy_evaluations': proxy,

                'source': source, 'pid': pid, 'executed_config': executed}

    def _result_origin(self, source):
        """Resolve cloud provenance through the exact persisted job, never a label."""
        from api import optimize_v8 as opt
        from vast_jobs import JobStore
        from vast_provider import VastError
        directory = opt._resolve_result_path(source['path'])
        marker = directory / '.pbgui_vast.json'
        if not marker.exists() and not marker.is_symlink():
            return source['name'], None
        try:
            if marker.is_symlink() or not marker.is_file() or marker.stat().st_size > 64 * 1024:
                raise ValueError('Invalid optimizer import provenance')
            provenance = opt._read_json(marker)
            identifier = provenance.get('job_id')
            state = JobStore(self.root / 'data/vast').read(identifier)
            if state.get('id') != identifier:
                raise ValueError('Optimizer import job identity mismatch')
            return state.get('config_name'), identifier
        except (ValueError, RuntimeError, OSError, VastError) as exc:
            from logging_helpers import human_log
            human_log(SERVICE, f'Optimizer import attribution unavailable: {type(exc).__name__}', level='WARNING')
            return None, None

    def candidates(self, record, job):
        """Read evidence belonging to exactly this native optimizer job."""
        from api import optimize_v8 as opt
        backend = job.get('backend') or {}
        rows = []
        for source in opt._list_results():
            name, cloud_id = self._result_origin(source)
            matches = (bool(cloud_id) and cloud_id == backend.get('id') if backend.get('execution') == 'vast'
                       else cloud_id is None and name == job['name'])
            if matches:
                rows.append(source)
        if len(rows) != 1:
            raise ValueError('Optimizer result is not yet uniquely attributable to its loop job')
        return self._result_candidates(record, rows[0], job)

    def existing_candidates(self, record):
        """Read bounded saved evidence attributed to the chosen starting config."""
        from api import optimize_v8 as opt
        sources = []
        preferred = (record['settings'].get('source') or {}).get('result_path')
        available = opt._list_results()
        if preferred:
            sources.extend(source for source in available if source['path'] == preferred)
        for source in available:
            name, _cloud_id = self._result_origin(source)
            if name == record['settings']['config_name'] and source not in sources:
                sources.append(source)
                if len(sources) == 3:
                    break
        result = []
        for source in sources:
            job = {'operation': 'saved_' + digest([source['result'], source['modified']])[:24],
                   'config': record['initial_config'], 'overrides': record['initial_overrides']}
            try:
                result.extend(self._result_candidates(record, source, job))
            except (ValueError, RuntimeError, OSError, HTTPException) as exc:
                from logging_helpers import human_log
                human_log(SERVICE, f'Saved optimizer evidence unavailable: {type(exc).__name__}', level='WARNING')
        return sorted(result, key=lambda row: row['guidance_score'], reverse=True)[:12]

    def _result_candidates(self, record, source, job):
        """Load native Pareto metrics/configs without mutating source results."""
        from api import optimize_v8 as opt
        directory = opt._resolve_result_path(source['path'])
        _pareto_dir, files = opt._pareto_files_for_listing(directory)
        artifacts = []
        for path in files[:512]:
            try:
                artifacts.append((path.name, opt._read_json(path)))
            except (RuntimeError, ValueError, OSError) as exc:
                from logging_helpers import human_log
                human_log(SERVICE, f'Optimizer candidate unavailable: {type(exc).__name__}', level='WARNING')
        if not artifacts:
            first = opt._all_results_first(directory / 'all_results.bin')
            if first:
                artifacts.append(('all_results.bin', first))
        result = []
        for artifact, data in artifacts:
            metrics = opt._pareto_summary(data, 'mean')
            if not metrics:
                continue
            config = {key: copy.deepcopy(data.get(key, job['config'].get(key))) for key in job['config']}
            candidate_id = 'c_' + digest([job['operation'], artifact])[:24]
            result.append({'id': candidate_id, 'optimizer_job': job['operation'], 'config': config,
                           'overrides': job['overrides'], 'metrics': metrics,
                           'source': {'run': source['result'], 'modified': source['modified'], 'artifact': artifact},
                           'guidance_score': evaluate(metrics, record['rubric'])['score']})
        if not result:
            raise ValueError('Optimizer result contains no usable candidate metrics')
        return sorted(result, key=lambda row: row['guidance_score'], reverse=True)[:12]

    def validation_config(self, record, candidate, holdout=False):
        config = copy.deepcopy(candidate['config'])
        config['backtest'] = copy.deepcopy(record['comparison'])
        config.setdefault('pbgui', {}).pop('scenario_template', None)
        config['pbgui']['execution'] = 'local'
        if holdout:
            if not record['holdouts']:
                raise ValueError('No unused final holdout is available')
            config['backtest']['suite_enabled'] = True
            allowed = {'coin_sources', 'coins', 'end_date', 'exchanges', 'ignored_coins', 'label', 'overrides', 'start_date'}
            config['backtest']['scenarios'] = [{key: copy.deepcopy(value) for key, value in window.items() if key in allowed} for window in record['holdouts']]
            balances = {window.get('starting_balance', config['backtest'].get('starting_balance')) for window in record['holdouts']}
            if len(balances) > 1:
                raise ValueError('Holdout scenarios require one consistent starting balance')
            if balances and next(iter(balances)) is not None:
                config['backtest']['starting_balance'] = next(iter(balances))
            config['backtest']['start_date'] = min(row['start_date'] for row in record['holdouts'])
            config['backtest']['end_date'] = max(row['end_date'] for row in record['holdouts'])
        # Validate all requested coins/directions on the fixed reference conditions.
        config['live']['approved_coins'] = copy.deepcopy(record['initial_config']['live'].get('approved_coins'))
        normalize_suite_coins(config)
        return config

    def observer_config(self, record, candidate, kind):
        """Build fixed Holdout/Full Time Range inputs without altering the optimizer config."""
        config = self.validation_config(record, candidate)
        if kind == 'observer_holdout':
            holdouts = record.get('observer_holdouts', record.get('training_exclusions', record['holdouts']))
            if not holdouts:
                raise ValueError('No Holdout windows are configured')
            reference = {**record, 'holdouts': holdouts}
            return self.validation_config(reference, candidate, True)
        if kind != 'observer_full_range':
            raise ValueError('Unknown user-only evaluation kind')
        config['backtest']['suite_enabled'] = False
        config['backtest'].pop('scenarios', None)
        return config

    def observations(self, record, job):
        """Use full finite exact analysis values rather than a truncated GUI summary."""
        from api import backtest_v8 as bt
        paths = bt._result_analysis_paths(job['name'])
        if not paths:
            raise ValueError('Exact validation results are not yet available')
        reports = [bt._read_json(path) for path in paths]
        metrics = {}
        import math
        def collect(value, prefix=''):
            if not isinstance(value, dict):
                return
            for key, item in value.items():
                if isinstance(item, (int, float)) and not isinstance(item, bool) and math.isfinite(item):
                    metrics[prefix + key] = item
                    metrics.setdefault(key, item)
                elif isinstance(item, dict):
                    collect(item, prefix + key + '.')
        per_report = []
        for report in reports:
            metrics = {}
            collect(report)
            per_report.append(metrics)
        keys = set.intersection(*(set(item) for item in per_report))
        directions = {key: rule['direction'] for rule in record['rubric'] for key in rule['metrics']}
        metrics = {key: (max(item[key] for item in per_report) if directions.get(key) == 'min'
                         else sum(item[key] for item in per_report) / len(per_report)) for key in keys}
        assessment = evaluate(metrics, record['rubric'])
        assessment['achieved'] = all(evaluate(item, record['rubric'])['achieved'] for item in per_report)
        scenarios = job.get('config', {}).get('backtest', {}).get('scenarios') or []
        expected = len(scenarios) if job.get('config', {}).get('backtest', {}).get('suite_enabled') and scenarios else 1
        complete = len(per_report) >= expected and all(item.get('backtest_completion_ratio', 0) >= .999999 for item in per_report)
        assessment['simulation_complete'] = complete
        assessment['comparable'] = assessment['comparable'] and complete
        assessment['achieved'] = assessment['achieved'] and complete
        assessment['hard_targets_met'] = all(evaluate(item, record['rubric'])['hard_targets_met'] for item in per_report)
        assessment['qualification'] = 'incomplete' if not complete else ('target violated' if not assessment['hard_targets_met'] else 'comparable')
        return {'metrics': metrics, 'reports': len(reports), 'exact': True,
                'per_report': per_report, 'assessment': assessment}

    def failed_before_data(self, record, job):
        """Prove the specific schema failure using an owned native state and absent reports."""
        if job['kind'] != 'holdout' or not job.get('backend') or job['backend']['execution'] == 'vast':
            return False
        state = self.poll(record, job)
        error = str((state.get('source') or {}).get('error') or '')
        from api import backtest_v8 as bt
        return state['status'] in {'error', 'failed'} and 'config.backtest.scenarios' in error and 'unknown key(s)' in error and not bt._result_analysis_paths(job['name'])

    def stop(self, record, job):
        """Cancel only this loop's jobs; rental ownership remains with native supervisors."""
        from api import optimize_v8 as opt, backtest_v8 as bt
        if not job.get('backend'):
            return
        self.poll(record, job)
        if job['backend']['execution'] == 'vast':
            from vast_jobs import JobStore
            store = JobStore()
            row = store.read(job['backend']['id'])
            if row['status'] == 'ready':
                store.update(row['id'], status='cancelled')
            else:
                store.control(row['id'], 'stop')
            return
        api = opt if job['kind'] == 'optimizer' else bt
        api._terminate_verified(job['backend']['id'])
        with api._queue_lock():
            path = api._queue_file(job['backend']['id'])
            data = api._read_json(path)
            data['status_override'] = 'stopped'
            (opt._write_json if api is opt else bt.atomic_write_json)(path, data)

    def cleanup(self, record):
        """End only this loop's owned cloud rentals after securing job outputs."""
        if record['settings']['execution'] != 'vast':
            return
        from vast_queue import CloudQueue
        queue = CloudQueue()
        for worker in queue.workers():
            if worker.get('loop_id') == record['id'] and worker.get('loop_owner') == record['owner']:
                queue.store.control(worker['id'], 'cleanup')
