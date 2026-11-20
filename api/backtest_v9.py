"""PB9 backtests on the shared Run/Backtest UI with isolated native jobs and data."""
from __future__ import annotations

import asyncio
import copy
from contextlib import contextmanager
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time
import uuid

import numpy as np
import psutil
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response, FileResponse

from api.auth import SessionToken, authenticated_push_websocket, require_auth
from api.backtest_price import build_market_price_payload
from api.pb9_migration import (carry_comparison_reference, config_comparison_reference,
                               validate_comparison_reference, resolve_comparison_reference)
from api.page_templates import render_page_urls, script_json
from api.v9_instances import _NoStoreRoute, _run, _paths, _native_http_error
from backtest_result_index import load_indexed_results, invalidate_result
from backtest_autostart import BacktestQueueWorker, claim_backtest_slot, publish_backtest_process, release_backtest_slot
from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from pb9_config import PB9ConfigError, PB9CancelSignal, get_pb9_editor_metadata, validate_pb9_backtest_config
from pb9_config_helper import check_inline_config
from pb9_backtest_runtime import launch_environment, source_identity, require_cached_market_metadata
from api.backtest_v8 import _launch_backtest_runner
from pb9_runtime import inspect_pb9_runtime
from pb9_result_helpers import load_equity_arrays, summarize_native_result
from pb7_config import load_pb7_config, save_pb7_config
from pb9_config_storage import PB9FileContext
from pbgui_purefunc import PBGDIR, PBGUI_VERSION, PBGUI_SERIAL, load_ini_section, save_ini_section
from secure_files import atomic_write_private_text, ensure_private_directory

SERVICE = "BacktestV9"
router = APIRouter(route_class=_NoStoreRoute)
_LOCK = threading.RLock()
_drafts = {}
_POLICY_LOCK = threading.RLock()
_QUEUE_SETTINGS_SECTION = "backtest_v9"


def _root(kind: str) -> Path:
    """Return a separate PB9 managed root, rejecting links through any parent."""
    names = {"configs": "bt_v9", "queue": "bt_v9_queue", "results": "bt_v9_results", "trash": "bt_v9_trash"}
    root = Path(PBGDIR).absolute() / "data" / names[kind]
    for path in (root, *root.parents):
        if path.is_symlink():
            raise HTTPException(422, "PB9 storage must not contain symbolic links")
    return root


def _name(value) -> str:
    """Reject traversal, controls and unbounded names at every persisted boundary."""
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_. -]{0,119}', value) or value in {'.', '..'} or value != value.strip():
        raise HTTPException(422, "Invalid PB9 backtest name")
    return value


def _safe(root: Path, *parts: str) -> Path:
    """Keep reads and writes below a validated root without following symlinks."""
    candidate = root.joinpath(*parts)
    if not candidate.resolve().is_relative_to(root.resolve()):
        raise HTTPException(422, "Invalid PB9 managed path")
    for item in (candidate, *candidate.parents):
        if item == root.parent:
            break
        if item.is_symlink():
            raise HTTPException(422, "PB9 managed paths must not be symbolic links")
    return candidate


def _read(path: Path, limit=1024 * 1024):
    """Read a bounded regular JSON object without following its final link."""
    if not path.exists():
        raise HTTPException(404, "PB9 backtest data not found")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        import stat
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise HTTPException(422, "Expected a regular PB9 file")
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise HTTPException(413, "PB9 file exceeds the size limit")
    try:
        result = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise HTTPException(422, "Invalid PB9 JSON file") from exc
    if not isinstance(result, dict):
        raise HTTPException(422, "Expected a PB9 JSON object")
    return result


def _write(path, value):
    """Publish private JSON atomically within the caller's cross-process lock."""
    ensure_private_directory(path.parent)
    atomic_write_private_text(path, json.dumps(value, indent=4, allow_nan=False) + '\n')


def _storage_lock():
    """Serialize config, snapshot and job transactions across API processes."""
    root = _root('queue')
    ensure_private_directory(root)
    return advisory_file_lock(root / '.write')


def _runtime(cancel=None):
    """Inspect only the configured, separated PB9 slot."""
    paths = _paths()
    status = inspect_pb9_runtime(paths['pb9dir'], paths['pb9venv'],
                                {key: value for key, value in paths.items() if not key.startswith('pb9')}, cancel_event=cancel)
    if not status.get('detected'):
        raise HTTPException(503, "The separate PB9 runtime is not detected")
    if not status.get('commit'):
        raise HTTPException(409, "The PB9 source commit cannot be verified")
    try:
        evidence = source_identity(Path(paths['pb9dir']))
    except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
        raise HTTPException(409, 'PB9 source must be clean and verifiable before backtesting') from exc
    status['commit'] = evidence['commit']
    return status, paths


def _native(config, cancel):
    """Validate the authored backtest while preserving all sparse side overrides."""
    if not isinstance(config, dict):
        raise HTTPException(422, "config must be an object")
    source = copy.deepcopy(config)
    source.pop('pbgui', None)
    status, paths = _runtime(cancel)
    validate_pb9_backtest_config(source, paths['pb9dir'], paths['pb9venv'],
                                {key: value for key, value in paths.items() if not key.startswith('pb9')}, cancel_event=cancel)
    return source, status


def _context(path, cancel):
    """Select native backtest validation explicitly through the shared config gateway."""
    paths = _paths()
    return PB9FileContext(path.parent, paths['pb9dir'], paths['pb9venv'],
                         {key: value for key, value in paths.items() if not key.startswith('pb9')},
                         validation_target='backtest', cancel_event=cancel)


def _load_source(path, cancel, *, revision=False):
    """Load authored configs through pb7_config's explicit isolated PB9 branch."""
    return load_pb7_config(path.name, pb9_context=_context(path, cancel), pb9_with_revision=revision)


def _save_source(path, source, cancel, *, expected_revision=None):
    """Keep native file validation, private publication and revision checks shared."""
    ensure_private_directory(path.parent)
    context = _context(path, cancel)
    save_pb7_config(source, path.name, pb9_context=context, pb9_expected_revision=expected_revision)


def _cfg_path(name):
    """Resolve one saved authored backtest document."""
    return _safe(_root('configs'), _name(name), 'config.json')


def _job_path(job, suffix='.json'):
    """Resolve one queue artifact by canonical UUID, never a user filename."""
    try:
        if str(uuid.UUID(job)) != job:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(422, "Invalid PB9 job ID") from None
    return _safe(_root('queue'), job + suffix)


def _working_dir(job):
    """Locate legacy per-job workspaces for existing process recovery and cleanup."""
    _job_path(job)
    return _safe(_root('queue'), 'workspaces', job)


def _job_lock(job, *, blocking=True):
    """Serialize each job across processes with a bounded set of 64 lock stripes."""
    _job_path(job)
    root = _safe(_root('queue'), 'locks')
    ensure_private_directory(root)
    return advisory_file_lock(root / str(int(uuid.UUID(job)) % 64), blocking=blocking)


def _remove_job_files(job):
    """Remove only a stopped job's private artifacts, never linked market data."""
    import shutil
    if _job_status(job)[0] == 'running':
        raise HTTPException(409, 'Stop the PB9 job before deleting it')
    item = _read(_job_path(job))
    item.pop('started_at', None)
    item['status_override'] = 'stopped'
    _write(_job_path(job), item)
    workspace = _working_dir(job)
    if workspace.exists():
        shutil.rmtree(workspace)
    for suffix in ('.config.json', '.pid.json', '.state.json', '.ready'):
        _job_path(job, suffix).unlink(missing_ok=True)
    release_backtest_slot('v9', job)
    _job_path(job).unlink(missing_ok=True)


def _job_status(job):
    """Recover independent process ownership after API restarts without relaunching."""
    ownership = _job_path(job, '.pid.json')
    if ownership.exists():
        record = _read(ownership)
        try:
            process = psutil.Process(int(record['pid']))
            command = process.cmdline()
            item = _read(_job_path(job))
            expected_cwd = item.get('working_directory', str(_working_dir(job)))
            cwd_matches_source = 'working_directory' not in item or (len(command) == 11
                    and isinstance(expected_cwd, str)
                    and Path(expected_cwd).is_absolute()
                    and expected_cwd == str(Path(command[9]).resolve()))
            if (abs(process.create_time() - float(record['create_time'])) < .01
                    and command == item.get('launch_command')
                    and cwd_matches_source
                    and process.cwd() == expected_cwd
                    and process.status() != psutil.STATUS_ZOMBIE):
                return 'running', record
        except (psutil.Error, ValueError, TypeError, KeyError):
            pass
    state_path = _job_path(job, '.state.json')
    if state_path.exists():
        state = _read(state_path)
        if state.get('stopped') is True:
            return 'stopped', None
        return ('complete' if state.get('returncode') == 0 else 'error'), None
    item = _read(_job_path(job))
    if item.get('started_at'):
        return ('queued' if time.time() - item['started_at'] < 75 else 'error'), None
    return item.get('status_override', 'queued'), None


def _queue():
    """Return only validated v9 queue records; companion JSON files are not jobs."""
    items = []
    root = _root('queue')
    if root.is_dir():
        for path in sorted(root.glob('*.json')):
            if not re.fullmatch(r'[0-9a-f-]{36}', path.stem):
                continue
            item = _read(_job_path(path.stem))
            status, process = _job_status(path.stem)
            items.append({key: value for key, value in item.items() if key != 'config_snapshot'})
            items[-1].update(status=status, pid=process['pid'] if process else None,
                             created=dt.datetime.fromtimestamp(item['created_at']).isoformat())
    return sorted(items, key=lambda item: (item['created_at'], item['filename']))


def _enqueue(body, cancel, *, archive_retest=None):
    """Publish the immutable source and provenance before making a job visible."""
    name = _name(body.get('name'))
    with _LOCK, _storage_lock():
        operation = body.get('operation_id')
        if operation:
            operation = _name(operation)
            for item in _queue():
                if item.get('operation_id') == operation:
                    return {'ok': True, 'filename': item['filename'], 'idempotent': True}
        config = body.get('config')
        if config is None:
            config = _load_source(_cfg_path(name), cancel)
        source, status = _native(config, cancel)
        reference = config_comparison_reference(config)
        job = str(uuid.uuid4())
        source['backtest']['base_dir'] = str(_safe(_root('results'), name, job))
        payload = {'name': name, 'filename': job, 'created_at': time.time(),
                   'exchange': source['backtest'].get('exchanges', []), 'config_snapshot': source,
                   'operation_id': operation, 'runtime': 'pb9', 'schema': source['config_version'],
                   'commit': status['commit'], 'package_version': status.get('package_version')}
        from pb9_gui_preferences import backtest_preferences
        try:
            preferences = backtest_preferences(config)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
        if preferences:
            payload['gui_preferences'] = preferences
        if reference:
            payload['comparison_reference'] = reference
        if cancel.is_set():
            raise PB9ConfigError('helper_cancelled')
        _save_source(_job_path(job, '.config.json'), source, cancel)
        payload['snapshot_sha256'] = hashlib.sha256(_job_path(job, '.config.json').read_bytes()).hexdigest()
        if archive_retest is not None:
            from pb9_archive_retests import source_digest
            payload['archive_retest'] = {**copy.deepcopy(archive_retest), 'queue_filename': job,
                'queue_config': str(_job_path(job, '.config.json')), 'snapshot_sha256': payload['snapshot_sha256'],
                'source_content_sha256': source_digest(source), 'native_schema':source['config_version'],
                'native_commit':payload['commit']}
        _write(_job_path(job), payload)
        return {'ok': True, 'filename': job}


def _stop(job):
    """Stop only this exact job, preserving completed results and natural exits."""
    with _job_lock(job):
        status, record = _job_status(job)
        if status in {'complete', 'error', 'stopped'}:
            return {'ok': True}
        if status == 'running':
            try:
                process = psutil.Process(record['pid'])
                children = process.children(recursive=True)
                if _job_status(job)[1] != record:
                    raise HTTPException(409, "PB9 process ownership changed")
                process.terminate()
                for child in children:
                    try:
                        child.terminate()
                    except psutil.NoSuchProcess:
                        pass
                _, alive = psutil.wait_procs([process, *children], timeout=5)
                for child in alive:
                    try:
                        child.kill()
                    except psutil.NoSuchProcess:
                        pass
                _, remaining = psutil.wait_procs(alive, timeout=2)
                if remaining:
                    raise HTTPException(409, 'PB9 process has not stopped; its slot stays reserved')
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                pass
            # A natural completion between verification and signal keeps its result.
            if _job_status(job)[0] in {'complete', 'error'} and _job_path(job, '.state.json').exists():
                exit_code = _read(_job_path(job, '.state.json')).get('returncode')
                if type(exit_code) is int and exit_code >= 0:
                    release_backtest_slot('v9', job)
                    return {'ok': True}
        item = _read(_job_path(job))
        item['status_override'] = 'stopped'
        _write(_job_path(job), item)
        _write(_job_path(job, '.state.json'), {'returncode': -15, 'completed_at': time.time(), 'stopped': True})
        release_backtest_slot('v9', job)
        return {'ok': True}


def _start(job, cancel, automatic=False):
    """Start one detached native job, or adopt it if it is already running."""
    with _job_lock(job, blocking=not automatic):
        item = _read(_job_path(job))
        status, existing = _job_status(job)
        if status == 'running':
            raise HTTPException(409, "PB9 backtest is already running")
        if status != 'queued':
            raise HTTPException(409, "Only queued PB9 backtests may start")
        runtime, paths = _runtime(cancel)
        if runtime['commit'] != item['commit'] or runtime['config_schema'] != item['schema']:
            raise HTTPException(409, "PB9 source changed since enqueue; enqueue a new snapshot")
        snapshot = _job_path(job, '.config.json')
        if hashlib.sha256(snapshot.read_bytes()).hexdigest() != item['snapshot_sha256']:
            raise HTTPException(409, "PB9 queue snapshot changed")
        # Match PB8: native downloads, catalog proofs and prepared data belong
        # to the configured bot installation and are reused across both tools.
        work = Path(paths['pb9dir']).resolve()
        try:
            require_cached_market_metadata(work, _read(snapshot))
        except (OSError, ValueError) as exc:
            _log(SERVICE, 'PB9 offline metadata preflight failed', level='WARNING')
            raise HTTPException(409, str(exc)) from None
        process = None
        unit_started = False
        claimed = False
        state = _job_path(job, '.state.json')
        try:
            with _queue_policy_lock():
                policy = _queue_settings()
                if cancel.is_set() or automatic and not policy['autostart']:
                    raise HTTPException(409, "PB9 automatic queue is paused")
                runner = Path(__file__).resolve().parents[1] / 'pb9_backtest_runner.py'
                if not claim_backtest_slot('v9', job, policy['cpu'], command_markers=[str(runner), str(snapshot)]):
                    raise _QueueCapacityUnavailable(409, "All shared backtest slots are occupied")
                claimed = True
                state, pid, ready = [_job_path(job, ext) for ext in ('.state.json', '.pid.json', '.ready')]
                log_root = Path(PBGDIR) / 'data/logs/backtests_v9'
                ensure_private_directory(log_root)
                log = _safe(log_root, job + '.log')
                command = [paths['pb9venv'], '-I', '-B', str(runner), 'backtest', str(state), str(pid), str(ready),
                           str(runtime['cli_file']), paths['pb9dir'], str(snapshot)]
                item['started_at'] = time.time()
                item['status_override'] = 'running'
                item['launch_command'] = command
                item['working_directory'] = str(work)
                _write(_job_path(job), item)
                # Use PB8's launcher with native environment isolation and an
                # explicit unit owner for cancellation before publication.
                env = launch_environment(paths['pb9venv'])
                unit_name = f'pbgui-pb9-backtest-{job}-{time.time_ns()}'
                def own_unit():
                    """Own a possibly submitted unit even if its client times out."""
                    nonlocal unit_started
                    unit_started = True
                process = _launch_backtest_runner(job, command, work, log, environment=env,
                                                 unit_name=unit_name, on_unit_started=own_unit)
            deadline = time.monotonic() + 75
            while time.monotonic() < deadline:
                if cancel.is_set():
                    raise PB9ConfigError('helper_cancelled')
                if ready.is_file() and pid.is_file():
                    record = _read(pid)
                    if ready.read_text().strip() == str(record['pid']):
                        publish_backtest_process('v9', job, record['pid'], record['create_time'], [str(runner), str(snapshot)])
                        ready.unlink(missing_ok=True)
                        return {'ok': True, 'pid': record['pid']}
                if state.exists() or process is not None and process.poll() is not None:
                    raise RuntimeError('PB9 runner exited before launch readiness; inspect the job log')
                time.sleep(.05)
            raise RuntimeError('PB9 runner launch timed out')
        except Exception:
            # A not-yet-published child still belongs to this failed launch.
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait()
            cleanup_confirmed = True
            if unit_started:
                try:
                    stopped = subprocess.run(['systemctl', '--user', 'stop', unit_name],
                                   env={**env, 'XDG_RUNTIME_DIR': os.environ.get('XDG_RUNTIME_DIR') or f'/run/user/{os.getuid()}'},
                                   capture_output=True, timeout=15, check=False)
                    cleanup_confirmed = stopped.returncode == 0
                except subprocess.SubprocessError:
                    cleanup_confirmed = False
                    _log(SERVICE, 'Could not confirm cleanup of the owned PB9 backtest unit', level='ERROR')
            if claimed and cancel.is_set() and cleanup_confirmed and not state.exists():
                # An unpublished, cancelled preparation can safely be retried after
                # API restart. Never discard a real completion/error from the runner.
                for key in ('started_at', 'status_override', 'launch_command', 'working_directory'):
                    item.pop(key, None)
                _write(_job_path(job), item)
                for path in (pid, ready):
                    path.unlink(missing_ok=True)
            elif claimed and not state.exists() and _job_status(job)[0] != 'running':
                _write(state, {'returncode': 1, 'error': 'PB9 launch failed; inspect the job log', 'completed_at': time.time()})
            if claimed and cleanup_confirmed:
                release_backtest_slot('v9', job)
            raise


def _result(path):
    """Resolve a relative result directory only within PB9's managed result root."""
    if not isinstance(path, str) or not path or any(ord(c) < 32 for c in path):
        raise HTTPException(422, 'Invalid result path')
    result = _safe(_root('results'), path)
    if not result.is_dir():
        raise HTTPException(404, 'PB9 result not found')
    return result


def _read_result(path):
    """Allow native read-only results in managed local storage or validated archives."""
    try:
        return _result(path)
    except HTTPException as exc:
        if exc.status_code not in {404, 422}:
            raise
    from api.backtest_v7 import _archives_dir
    from passivbot_runtime import generation_from_schema
    root = Path(_archives_dir()).absolute()
    if not isinstance(path, str) or not path or any(ord(c) < 32 for c in path) or not Path(path).is_absolute():
        raise HTTPException(404, 'PB9 result not found')
    directory = _safe(root, path)
    if not directory.is_dir():
        raise HTTPException(404, 'PB9 archived result not found')
    config = _read(_safe(directory, 'config.json'))
    schema = config.get('config_version')
    if not isinstance(schema, str) or generation_from_schema(schema) != 'pb9':
        raise HTTPException(422, 'Archived result does not belong to PB9')
    return directory


def _equity_arrays(directory):
    """Use the shared bounded native reader for local and archived charts."""
    return load_equity_arrays(_safe(directory, 'balance_and_equity.npz'))


def _summaries(paths):
    """Translate local native metrics through the same projector as archives."""
    items = []
    root = _root('results')
    for analysis_path in paths:
        directory = _result(str(analysis_path.parent.relative_to(root)))
        analysis = _read(_safe(directory, 'analysis.json'), 8 * 1024 * 1024)
        config = _read(_safe(directory, 'config.json'))
        authored = _result_source(directory)
        arrays = _equity_arrays(directory) if _safe(directory, 'balance_and_equity.npz').is_file() else None
        items.append(summarize_native_result(directory, config, authored, analysis, arrays,
            config_name=directory.relative_to(root).parts[0], display_name=str(directory.relative_to(root)),
            modified=dt.datetime.fromtimestamp(analysis_path.stat().st_mtime).isoformat()))
    return items


def _archive_data_paths(config):
    """Reuse the existing source-root policy before importing archived data paths."""
    from api import pb8_ohlcv_tools as shared
    paths = _paths()
    def walk(value):
        """Validate regular and scenario/dotted native source/dataset selections."""
        if isinstance(value, list):
            for child in value:
                walk(child)
        elif isinstance(value, dict):
            for key, child in list(value.items()):
                if key.rsplit('.', 1)[-1] in {'ohlcv_source_dir', 'hlcvs_data_dir'} and child not in (None, ''):
                    if not isinstance(child, str):
                        raise HTTPException(422, 'Invalid archived data path')
                    try:
                        _, selected, _ = shared.resolve_pb8_ohlcv_paths(
                            {'backtest': {'ohlcv_source_dir': child}}, {'pb8dir': paths['pb9dir']})
                    except shared.PB8OhlcvError:
                        raise HTTPException(422, 'Archived data path must be below PBGui Market Data or the configured PB9 directory; choose PBGui Data or edit the source') from None
                    value[key] = str(selected)
                else:
                    walk(child)
    walk(config)
    return config


def _result_source(directory):
    """Preserve result-owned authored input instead of a normalized legacy projection."""
    for filename in ('pbgui.source.json', 'config.original.json', 'config.json'):
        path = _safe(directory, filename)
        if path.exists():
            return _read(path)
    raise HTTPException(404, 'PB9 result source is unavailable')


def _result_reference(directory):
    """Read result-owned reference metadata independently of the summary index cache."""
    path=_safe(directory,'pbgui.provenance.json')
    if not path.exists():
        return None
    return validate_comparison_reference(_read(path).get('comparison_reference'))


def _side_risk(config, side):
    """Resolve only the enabled side risk for compact shared table summaries."""
    bot = config.get('bot', {})
    if bot.get(side, {}).get('enabled') is not True:
        return {}
    return {**bot.get('global', {}).get('risk', {}), **bot.get(side, {}).get('risk', {})}


def _configured_coins_count(config):
    """Count distinct native side lists, retaining explicit empty and all policies."""
    bot = config.get('bot', {})
    shared = bot.get('global', {}).get('universe', {}).get('approved_coins', [])
    coins = set()
    for side in ('long', 'short'):
        selected = bot.get(side, {}).get('universe', {}).get('approved_coins')
        if selected is None:
            selected = shared
        if selected == 'all' or isinstance(selected, list) and 'all' in selected:
            return 'all'
        if isinstance(selected, list):
            coins.update(coin for coin in selected if isinstance(coin, str) and coin)
    return len(coins)


def store_draft(owner, config, migration_report=None):
    """Keep one short-lived owner-bound draft for the shared Backtest/Optimize handoff."""
    with _LOCK:
        for key, value in list(_drafts.items()):
            if time.monotonic() - value['created'] > 600:
                del _drafts[key]
        if len(_drafts) >= 100:
            del _drafts[next(iter(_drafts))]
        token = str(uuid.uuid4())
        _drafts[token] = {'created': time.monotonic(), 'owner': owner, 'config': config}
        if migration_report is not None:
            _drafts[token]['migration_report'] = migration_report
    return token


def _operation(operation, cancel, payload=None):
    """Use the existing bounded PB9 request owner for all native/transactional work."""
    from api.pb9 import _request_envelope
    envelope = _request_envelope(payload, {'args', 'body'}) if payload else {'args': {}, 'body': {}}
    args, body = envelope['args'], envelope['body'] or {}
    if not isinstance(args, dict) or not isinstance(body, dict):
        raise HTTPException(422, 'Invalid PB9 request')
    try:
        if cancel.is_set():
            raise PB9ConfigError('helper_cancelled')
        if operation in {'bt9_ohlcv_preflight', 'bt9_ohlcv_start'}:
            from api import pb9_ohlcv_tools as ohlcv
            config = body.get('config')
            if not isinstance(config, dict):
                raise HTTPException(422, 'config must be an object')
            if operation == 'bt9_ohlcv_preflight':
                return ohlcv.preflight(config, cancel)
            # Reject non-native/external documents before admitting a background job.
            candidate = copy.deepcopy(config); candidate.pop('pbgui', None)
            try:
                check_inline_config(candidate, allow_inline_overrides=True)
            except RuntimeError as exc:
                raise PB9ConfigError(str(exc)) from None
            try:
                return ohlcv.start(candidate, args['owner'])
            except Exception as exc:
                from api.pb8_ohlcv_tools import PB8OhlcvBusyError
                if isinstance(exc, PB8OhlcvBusyError):
                    raise HTTPException(503, 'OHLCV lookup capacity is busy') from None
                raise
        if operation == 'bt9_migrate_preview':
            from api.pb9_migration import preview
            return preview(body, args['owner'], _paths(), cancel)
        if operation == 'bt9_migrate_accept':
            from api.pb9_migration import reviewed
            status, _ = _runtime(cancel)
            review = reviewed(body, args['owner'], status)
            source, _ = _native(review['config'], cancel)
            carry_comparison_reference(source, review['config'])
            if review.get('comparison_reference'):
                source.setdefault('pbgui', {})['comparison_reference'] = review['comparison_reference']
            return {'draft_id': store_draft(args['owner'], source, review['report']), 'suggested_name': review['name']}
        if operation == 'bt9_metadata':
            paths = _paths()
            return get_pb9_editor_metadata(paths['pb9dir'], paths['pb9venv'],
                    {key: value for key, value in paths.items() if not key.startswith('pb9')}, cancel_event=cancel)
        if operation == 'bt9_new_config':
            paths = _paths()
            from pb9_config import load_pb9_template
            result = load_pb9_template(paths['pb9dir'], paths['pb9venv'],
                    {key: value for key, value in paths.items() if not key.startswith('pb9')}, cancel_event=cancel)
            result['config']['backtest']['ohlcv_source_dir'] = _pbgui_data_path()
            return {'config': result['config'], 'param_status': {}, 'override_configs': {}}
        if operation == 'bt9_prepare':
            document = body.get('config', body)
            source, _ = _native(document, cancel)
            carry_comparison_reference(source, document)
            return {'config': source, 'param_status': {}, 'override_configs': {}}
        if operation == 'bt9_draft_create':
            candidate = copy.deepcopy(body.get('config'))
            if isinstance(candidate, dict):
                backtest = candidate.setdefault('backtest', {})
                if isinstance(backtest, dict) and not backtest.get('ohlcv_source_dir'):
                    backtest['ohlcv_source_dir'] = _pbgui_data_path()
            source, _ = _native(candidate, cancel)
            carry_comparison_reference(source, candidate)
            return {'draft_id': store_draft(args['owner'], source)}
        if operation == 'bt9_draft_get':
            with _LOCK:
                value = _drafts.get(args['id'])
                if not value or value['owner'] != args['owner'] or time.monotonic() - value['created'] > 600:
                    raise HTTPException(404, 'Draft expired or not owned by this user')
                return {'config': copy.deepcopy(value['config']), 'param_status': {}, 'override_configs': {},
                        **({'migration_report': copy.deepcopy(value['migration_report'])} if 'migration_report' in value else {})}
        if operation == 'bt9_configs':
            results = []
            if _root('configs').is_dir():
                for path in sorted(_root('configs').glob('*/config.json')):
                    config = _read(_cfg_path(path.parent.name))
                    bt, bot = config.get('backtest', {}), config.get('bot', {})
                    results.append({'name': path.parent.name, 'exchanges': bt.get('exchanges', []),
                        'strategy': bot.get('portfolio', {}).get('strategy_kind', ''), 'coins': _configured_coins_count(config),
                        'twe_long': _side_risk(config, 'long').get('total_wallet_exposure_limit_ratio', 0),
                        'twe_short': _side_risk(config, 'short').get('total_wallet_exposure_limit_ratio', 0), 'start_date': bt.get('start_date'), 'end_date': bt.get('end_date'),
                        'modified': dt.datetime.fromtimestamp(path.stat().st_mtime, dt.timezone.utc).isoformat(),
                        'results': len(list(_safe(_root('results'), path.parent.name).glob('**/analysis.json')))})
            return {'configs': results}
        if operation == 'bt9_load':
            loaded = _load_source(_cfg_path(args['name']), cancel, revision=True)
            return {'name': args['name'], **loaded, 'param_status': {}, 'override_configs': {}}
        if operation == 'bt9_save':
            document = body.get('config', body)
            source, _ = _native(document, cancel)
            carry_comparison_reference(source, document)
            # base_dir is server-owned; persisted names never control arbitrary writes.
            source['backtest']['base_dir'] = str(_safe(_root('results'), _name(args['name'])))
            with _LOCK, _storage_lock():
                path = _cfg_path(args['name'])
                current = None
                if path.exists():
                    current = _load_source(path, cancel, revision=True)['revision']
                    if body.get('expected_revision') != current:
                        raise HTTPException(409, 'PB9 config exists or changed; reopen it or save with a new name')
                elif body.get('expected_revision'):
                    raise HTTPException(409, 'PB9 config was removed; reopen the editor')
                _save_source(path, source, cancel, expected_revision=current)
                revision = hashlib.sha256(path.read_bytes()).hexdigest()
            return {'ok': True, 'name': args['name'], 'revision': revision}
        if operation == 'bt9_delete_config':
            from contextlib import ExitStack
            with _LOCK, _storage_lock():
                with ExitStack() as locks:
                    jobs = sorted(item['filename'] for item in _queue() if item['name'] == args['name'])
                    # Acquire each stripe once; launches use these same locks.
                    stripes = {int(uuid.UUID(job)) % 64: job for job in jobs}
                    for stripe in sorted(stripes):
                        locks.enter_context(_job_lock(stripes[stripe]))
                    if any(_job_status(job)[0] == 'running' or
                           (_job_status(job)[0] == 'queued' and _read(_job_path(job)).get('started_at'))
                           for job in jobs):
                        raise HTTPException(409, 'Stop this config backtest before deleting it')
                    directory = _cfg_path(args['name']).parent
                    if not directory.is_dir():
                        raise HTTPException(404, 'PB9 config not found')
                    trash = _root('trash')
                    ensure_private_directory(trash)
                    moved = []
                    try:
                        targets = [directory]
                        result_root = _safe(_root('results'), _name(args['name']))
                        if args.get('remove_results') and result_root.exists():
                            targets.append(result_root)
                        for target in targets:
                            destination = _safe(trash, 'config-' + uuid.uuid4().hex)
                            os.replace(target, destination)
                            moved.append((target, destination))
                    except Exception:
                        for target, destination in reversed(moved):
                            os.replace(destination, target)
                        raise
            return {'ok': True}
        if operation in {'bt9_archive_retest', 'bt9_archive_schedule_run'}:
            from api import backtest_v7 as archives
            if operation == 'bt9_archive_retest':
                return archives._native_archive_retest(args['name'], body, cancel)
            return archives._native_schedule_run(args['name'], args['schedule_id'], cancel)
        if operation == 'bt9_archive_requeue':
            from api.backtest_v7 import _archives_dir, _load_archived_backtest_config
            archive = _safe(Path(_archives_dir()), _name(args['name']))
            paths = body.get('paths')
            overrides = body.get('overrides') or {}
            if not isinstance(paths, list) or not 1 <= len(paths) <= 100 or not isinstance(overrides, dict):
                raise HTTPException(422, 'Expected between 1 and 100 archive results')
            exchanges = overrides.get('exchanges')
            if exchanges is not None and (not isinstance(exchanges, list) or not exchanges or len(exchanges) > 20
                                           or any(not isinstance(value, str) for value in exchanges)):
                raise HTTPException(422, 'Invalid exchange selection')
            if len(paths) * len(exchanges or [None]) > 100:
                raise HTTPException(422, 'At most 100 native archive jobs per request')
            request_id = body.get('operation_id')
            if request_id is not None:
                request_id = _name(request_id)
            prepared = []
            for path in paths:
                directory = _read_result(path)
                _safe(archive, str(directory))
                config, version, _ = _load_archived_backtest_config(directory, archive)
                if version != 'v9':
                    raise HTTPException(422, 'Expected a native PB9 archive result')
                from api.archive_helpers import _config_name_from_result
                name = _name(_config_name_from_result(directory, config, archive))
                for exchange in exchanges or [None]:
                    candidate = copy.deepcopy(config)
                    bt = candidate.setdefault('backtest', {})
                    for key in ('start_date', 'end_date', 'starting_balance'):
                        if overrides.get(key) not in (None, ''):
                            bt[key] = overrides[key]
                    if exchange is not None:
                        bt['exchanges'] = [exchange]
                    if overrides.get('use_pbgui_market_data') is True:
                        bt['ohlcv_source_dir'] = _pbgui_data_path()
                    _archive_data_paths(candidate)
                    _native(candidate, cancel)
                    operation_id = hashlib.sha256(json.dumps([request_id, path, exchange]).encode()).hexdigest() if request_id else None
                    prepared.append((name, candidate, operation_id))
            queued = [_enqueue({'name': name, 'config': config, 'operation_id': operation_id}, cancel) for name, config, operation_id in prepared]
            return {'ok': True, 'queued': len(queued), 'queue_items': [{**item, 'backtest_version': 'v9'} for item in queued]}
        if operation == 'bt9_enqueue':
            return _enqueue(body, cancel)
        if operation == 'bt9_queue':
            items = _queue()
            return {'queue': items, 'items': items, 'settings': _queue_settings()}
        if operation == 'bt9_start':
            return _start(args['id'], cancel)
        if operation in {'bt9_stop', 'bt9_restart', 'bt9_delete_job'}:
            with _job_lock(args['id']):
                if operation != 'bt9_delete_job' or _job_status(args['id'])[0] == 'running':
                    _stop(args['id'])
                if operation == 'bt9_restart':
                    item = _read(_job_path(args['id']))
                    runtime, _ = _runtime(cancel)
                    if runtime['commit'] != item['commit'] or runtime['config_schema'] != item['schema']:
                        raise HTTPException(409, 'PB9 source changed; enqueue a new snapshot')
                    snapshot = _job_path(args['id'], '.config.json')
                    if hashlib.sha256(snapshot.read_bytes()).hexdigest() != item['snapshot_sha256']:
                        raise HTTPException(409, 'PB9 queue snapshot changed')
                    config = _read(snapshot)
                    config['pbgui'] = copy.deepcopy(item.get('gui_preferences', {}))
                    if item.get('comparison_reference'):
                        config['pbgui']['comparison_reference'] = item['comparison_reference']
                elif operation == 'bt9_delete_job':
                    _remove_job_files(args['id'])
                    return {'ok': True}
                else:
                    return {'ok': True}
            # New output ownership preserves the previous run and its provenance.
            queued = _enqueue({'name': item['name'], 'config': config}, cancel)
            return {**_start(queued['filename'], cancel), 'filename': queued['filename']}
        if operation == 'bt9_results':
            root = _root('results')
            paths = []
            if root.is_dir():
                for path in root.glob('**/analysis.json'):
                    _result(str(path.parent.relative_to(root)))
                    if not args.get('name') or path.relative_to(root).parts[0] == _name(args['name']):
                        paths.append(path)
            paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            offset, limit = args.get('offset', 0), args.get('limit', 100)
            if offset < 0 or not 0 <= limit <= 100:
                raise HTTPException(422, 'Invalid result page')
            selected = paths[offset:offset + (limit or 100)]
            items = load_indexed_results('v9', selected, _summaries)
            for item in items:
                try:
                    reference = _result_reference(_result(item['path']))
                except HTTPException:
                    _log(SERVICE, 'PB9 comparison metadata is invalid; manual comparison remains available', level='WARNING')
                    reference = None
                    item['comparison_reference_error'] = 'Invalid PB8 comparison reference'
                if reference:
                    item['comparison_reference'] = reference
            return {'results': items, 'pagination': {
                'total': len(paths), 'offset': offset, 'limit': limit, 'returned': len(selected),
                'has_more': offset + len(selected) < len(paths), 'next_offset': offset + len(selected)}}
        if operation == 'bt9_compare_source':
            directory = _result(args['path'])
            reference = _result_reference(directory)
            original = resolve_comparison_reference(reference)
            from api import backtest_v8 as pb8
            older = pb8._list_results([original / 'analysis.json'])
            current = _summaries([directory / 'analysis.json'])
            if len(older) != 1 or len(current) != 1:
                raise HTTPException(404, 'Comparison result is unavailable')
            return {'results': [older[0], current[0]]}
        if operation == 'bt9_analysis':
            return _read(_safe(_read_result(args['path']), 'analysis.json'), 8 * 1024 * 1024)
        if operation == 'bt9_price':
            config = _read(_safe(_read_result(args['path']), 'config.json'))
            try:
                return build_market_price_payload(config, exchange=args['exchange'], coin=args['coin'],
                                                  max_points=args['max_points'])
            except ValueError as exc:
                _log(SERVICE, 'PB9 result price request rejected', level='WARNING')
                raise HTTPException(400, str(exc)) from None
        if operation == 'bt9_result_config':
            directory = _read_result(args['path'])
            original = _safe(directory, 'pbgui.source.json')
            if not original.exists():
                original = _safe(directory, 'config.original.json')
            source = _read(original if original.exists() else _safe(directory, 'config.json'))
            reference = _result_reference(directory)
            if reference:
                source.setdefault('pbgui', {})['comparison_reference'] = reference
            return source
        if operation == 'bt9_delete_result':
            with _LOCK, _storage_lock():
                directory = _result(args['path'])
                parts = directory.relative_to(_root('results')).parts
                if len(parts) > 1:
                    try:
                        if _job_status(parts[1])[0] == 'running':
                            raise HTTPException(409, 'Stop the PB9 job before deleting its results')
                    except HTTPException as exc:
                        if exc.status_code != 404:
                            raise
                trash = _root('trash')
                ensure_private_directory(trash)
                os.replace(directory, _safe(trash, 'result-' + uuid.uuid4().hex))
                invalidate_result('v9', directory)
            return {'ok': True}
        if operation == 'bt9_clear_finished':
            count = 0
            with _LOCK, _storage_lock():
                for item in _queue():
                    with _job_lock(item['filename']):
                        if _job_status(item['filename'])[0] not in {'complete', 'error', 'stopped'}:
                            continue
                        _remove_job_files(item['filename'])
                        count += 1
            return {'ok': True, 'removed': count}
        if operation == 'bt9_files':
            directory = _read_result(args['path'])
            return {'files': [str(p.relative_to(directory)) for p in directory.rglob('*')
                              if p.is_file() and not p.is_symlink() and _safe(directory, str(p.relative_to(directory)))]}
        if operation == 'bt9_equity':
            times, balance, equity = _equity_arrays(_read_result(args['path']))
            output = io.StringIO(); writer = csv.writer(output)
            writer.writerow(['timestamp', 'usd_total_balance', 'usd_total_equity'])
            stride = max(1, math.ceil(len(times) / 10000))
            for index in sorted(set(range(0, len(times), stride)) | ({len(times) - 1} if len(times) else set())):
                writer.writerow([dt.datetime.fromtimestamp(int(times[index]) / 1000, tz=dt.timezone.utc).isoformat(),
                                 float(balance[index]) if math.isfinite(float(balance[index])) else '', float(equity[index])])
            return {'csv': output.getvalue()}
        raise HTTPException(404, 'PB9 operation is not available')
    except PB9ConfigError as exc:
        if exc.error_codes[0] in {'config_file_exists', 'config_revision_conflict'}:
            raise HTTPException(409, 'PB9 config changed; reopen its current revision') from None
        raise _native_http_error(exc) from None


async def _call(request, operation, args=None, *, body=False):
    """Keep requests under the same bounded, shutdown-owned PB9 executor."""
    allowed = frozenset(args or {}) | ({'snapshot'} if operation == 'results' else set())
    return await _run(request, 'bt9_' + operation, args or {}, with_body=body, query=allowed)


@router.get('/main_page', response_class=HTMLResponse)
def main_page(request: Request, session: SessionToken = Depends(require_auth)):
    """Render the same Backtest page used by PB8."""
    html = (Path(__file__).resolve().parents[1] / 'frontend/v7_backtest.html').read_text()
    html = render_page_urls(request, html, '/api/backtest-v9')
    nav=Path(__file__).resolve().parents[1]/'frontend/pbgui_nav.js'
    html=html.replace('%%NAV_HASH%%',str(int(nav.stat().st_mtime)))
    for key, value in {'VERSION': PBGUI_VERSION, 'SERIAL': str(PBGUI_SERIAL), 'BACKTEST_VERSION': 'v9',
                       'BACKTEST_LABEL': 'V9', 'BACKTEST_SUBTITLE': 'PBv9 BACKTEST', 'BACKTEST_NAV_CURRENT': 'v9_backtest'}.items():
        html = html.replace('"%%' + key + '%%"', script_json(value)).replace('%%' + key + '%%', value)
    return HTMLResponse(html, headers={'Cache-Control': 'no-store'})


@contextmanager
def _queue_policy_lock():
    """Serialize policy acknowledgement with final launch admission across APIs."""
    with _POLICY_LOCK, advisory_file_lock(Path(PBGDIR) / 'data/locks/backtest-v9-policy'):
        yield


def _queue_settings():
    """Load only PB9's separate queue policy, defaulting to manual execution."""
    values = load_ini_section(_QUEUE_SETTINGS_SECTION)
    maximum = max(1, os.cpu_count() or 1)
    try:
        cpu = int(values.get('cpu', '1'))
    except (TypeError, ValueError):
        cpu = 1
    return {'autostart': str(values.get('autostart', 'False')).lower() == 'true',
            'cpu': max(1, min(cpu, maximum)), 'cpu_max': maximum, 'exchange_options': [],
            'capabilities': {'local': True, 'native_schema': 'v9', 'optimizer': True,
                             'archive': True, 'autostart': True}}


@router.get('/settings')
def settings(session: SessionToken = Depends(require_auth)):
    """Expose native queue settings through the existing PB8 dialog contract."""
    return _queue_settings()


@router.post('/settings')
def update_settings(body: dict, session: SessionToken = Depends(require_auth)):
    """Save queue policy atomically without changing PB7/PB8 or running jobs."""
    if set(body) - {'cpu', 'autostart'}:
        raise HTTPException(422, 'Unsupported PB9 queue setting')
    if 'cpu' in body and type(body['cpu']) is not int:
        raise HTTPException(422, 'cpu must be an integer')
    if 'autostart' in body and type(body['autostart']) is not bool:
        raise HTTPException(422, 'autostart must be a boolean')
    values = {key: str(value) for key, value in body.items()}
    if 'cpu' in body:
        values['cpu'] = str(max(1, min(body['cpu'], max(1, os.cpu_count() or 1))))
    with _queue_policy_lock():
        save_ini_section(_QUEUE_SETTINGS_SECTION, values)
    return {'ok': True}


class _QueueCapacityUnavailable(HTTPException):
    """Distinguish shared capacity from jobs requiring individual intervention."""


class BacktestV9Worker(BacktestQueueWorker):
    """Use the PB8 controller lifecycle and shared slots with PB9's native launcher."""

    def __init__(self):
        super().__init__('backtest-v9-worker')
        self._cancel = PB9CancelSignal()
        self._deferred = {}

    def start(self):
        """Create a fresh cancellation owner only for a new controller."""
        if self._task is None or self._task.done():
            self._cancel = PB9CancelSignal()
        super().start()

    async def stop(self):
        """Cancel unpublished launch preparation, then drain its owned work."""
        self._cancel.set()
        await super().stop()
        self._deferred.clear()

    def _dispatch(self, job):
        """Native start rechecks policy under the same gate used by settings writes."""
        return _start(job, self._cancel, automatic=True)

    async def _cycle(self):
        """Inspect fresh queue state without blocking the API event loop."""
        policy = await self.run_sync(_queue_settings)
        if not policy['autostart']:
            return
        items = await self.run_sync(_queue)
        live = {item['filename'] for item in items}
        self._deferred = {key: value for key, value in self._deferred.items() if key in live}
        running = sum(item['status'] == 'running' for item in items)
        for item in items:
            if not self._running or self._cancel.is_set():
                return
            policy = await self.run_sync(_queue_settings)
            if not policy['autostart'] or running >= policy['cpu']:
                return
            if item['status'] != 'queued':
                continue
            try:
                await self.run_sync(self._dispatch, item['filename'])
                running += 1
                self._deferred.pop(item['filename'], None)
            except (_QueueCapacityUnavailable, BlockingIOError):
                return
            except HTTPException as exc:
                # Retain actionable jobs but avoid flooding the daemon log every poll.
                signature = (exc.status_code, str(exc.detail))
                if self._deferred.get(item['filename']) != signature:
                    _log(SERVICE, f"Queued PB9 {item['filename']} launch deferred: {exc.detail}", level='INFO' if exc.status_code == 409 else 'ERROR')
                    if len(self._deferred) < 1024:
                        self._deferred[item['filename']] = signature
            except PB9ConfigError:
                if not self._cancel.is_set():
                    _log(SERVICE, 'Queued PB9 runtime preparation failed', level='ERROR')
            except Exception as exc:
                _log(SERVICE, f"Queued PB9 launch failed: {exc}", level='ERROR')

    async def _loop(self):
        """Retry transient controller failures while retaining durable queue state."""
        try:
            while self._running:
                try:
                    await self._cycle()
                except Exception as exc:
                    _log(SERVICE, f"PB9 queue scan failed: {exc}", level='ERROR')
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass


_worker = BacktestV9Worker()


def startup():
    """Reconstruct only the API-owned scheduler; the default policy is disabled."""
    _worker.start()


def _pbgui_data_path():
    """Use the same shared OHLCV root as PB7/PB8, without a bot-slot dependency."""
    from market_data import get_market_data_root_dir
    return str(get_market_data_root_dir())


@router.post('/archives/{name}/results/retest-replace')
async def archive_retest(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Prepare native replacements under the bounded, cancellable PB9 request owner."""
    return await _call(request, 'archive_retest', {'name': _name(name)}, body=True)


@router.post('/archives/{name}/retest-schedules/{schedule_id}/run')
async def native_schedule_run(name: str, schedule_id: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Run native scheduled targets through their own request owner."""
    return await _call(request, 'archive_schedule_run', {'name': _name(name), 'schedule_id': _name(schedule_id)})


@router.post('/archives/{name}/results/rebacktest')
async def archive_requeue(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Queue immutable native snapshots under the existing cancellable PB9 owner."""
    return await _call(request, 'archive_requeue', {'name': _name(name)}, body=True)


@router.get('/pbgui_data_path')
def pbgui_data_path(session: SessionToken = Depends(require_auth)):
    """Expose the existing editor's PBGui Data action for native PB9 too."""
    return {'path': _pbgui_data_path()}


@router.get('/editor/metadata')
async def metadata(request: Request, session: SessionToken = Depends(require_auth)):
    """Read the installed native field/strategy registry."""
    return await _call(request, 'metadata')


@router.post('/configs/prepare')
async def prepare(request: Request, session: SessionToken = Depends(require_auth)):
    """Check an imported native PB9 document without migrating it."""
    return await _call(request, 'prepare', body=True)


@router.post('/migrate-v8/preview')
async def migrate_v8_preview(request: Request, session: SessionToken = Depends(require_auth)):
    """Review the official conversion of a saved PB8 config or local result."""
    return await _call(request, 'migrate_preview', {'owner': session.user_id}, body=True)


@router.post('/migrate-v8/accept')
async def migrate_v8_accept(request: Request, session: SessionToken = Depends(require_auth)):
    """Adopt a reviewed proposal as a private unsaved PB9 backtest draft."""
    return await _call(request, 'migrate_accept', {'owner': session.user_id}, body=True)


@router.post('/optimize-draft')
async def create_draft(request: Request, session: SessionToken = Depends(require_auth)):
    """Keep the shared UI handoff endpoint; this creates only a backtest draft."""
    return await _call(request, 'draft_create', {'owner': session.user_id}, body=True)


@router.get('/optimize-draft/{draft_id}')
async def get_draft(draft_id: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Read a short-lived draft only for its originating user."""
    return await _call(request, 'draft_get', {'id': draft_id, 'owner': session.user_id})


@router.get('/configs')
async def configs(request: Request, session: SessionToken = Depends(require_auth)):
    """List native PB9 backtest documents."""
    return await _call(request, 'configs')


@router.get('/configs/new-config')
async def new_config(request: Request, session: SessionToken = Depends(require_auth)):
    """Create a native authored PB9 template without saving it."""
    return await _call(request, 'new_config')


@router.delete('/configs/{name}')
async def delete_config(name: str, request: Request, remove_results: bool = False, session: SessionToken = Depends(require_auth)):
    """Move a stopped config into its private recoverable trash."""
    return await _call(request, 'delete_config', {'name': _name(name), 'remove_results': remove_results})


@router.get('/configs/{name}')
async def load_config(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Load the sparse source without native materialization."""
    return await _call(request, 'load', {'name': _name(name)})


@router.put('/configs/{name}')
async def save_config(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Save one native backtest under a server-selected result root."""
    return await _call(request, 'save', {'name': _name(name)}, body=True)


@router.get('/queue')
async def queue(request: Request, session: SessionToken = Depends(require_auth)):
    """Read and adopt persisted native process ownership."""
    return await _call(request, 'queue')


@router.post('/ohlcv-preflight')
async def ohlcv_preflight(request: Request, session: SessionToken = Depends(require_auth)):
    """Use the shared readiness panel with only native PB9 planning modules."""
    return await _call(request, 'ohlcv_preflight', body=True)


@router.post('/ohlcv-start-dates')
async def start_ohlcv_lookup(request: Request, session: SessionToken = Depends(require_auth)):
    """Start a bounded login-owned native inception lookup."""
    return await _call(request, 'ohlcv_start', {'owner': session.user_id}, body=True)


def _lookup_id(job_id):
    """Validate the transient controller identifier before looking it up."""
    if not isinstance(job_id, str) or not re.fullmatch(r'[a-f0-9]{12}', job_id):
        raise HTTPException(404, 'OHLCV lookup not found')
    return job_id


@router.get('/ohlcv-start-dates/{job_id}')
def get_ohlcv_lookup(job_id: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Return progress for this login and runtime only."""
    from api.pb9_ohlcv_tools import get
    from api.v9_instances import _check_query
    _check_query(request)
    result = get(_lookup_id(job_id), session.user_id)
    if result is None:
        raise HTTPException(404, 'OHLCV lookup not found')
    return result


@router.delete('/ohlcv-start-dates/{job_id}')
def stop_ohlcv_lookup(job_id: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Cancel one exact API-owned helper without controlling a native backtest."""
    from api.pb9_ohlcv_tools import stop
    from api.v9_instances import _check_query
    _check_query(request)
    result = stop(_lookup_id(job_id), session.user_id)
    if result is None:
        raise HTTPException(404, 'OHLCV lookup not found')
    return result


@router.post('/queue')
async def enqueue(request: Request, session: SessionToken = Depends(require_auth)):
    """Snapshot a saved config or the explicitly submitted current editor source."""
    return await _call(request, 'enqueue', body=True)


@router.post('/queue/clear-finished')
async def clear_finished(request: Request, session: SessionToken = Depends(require_auth)):
    """Remove finished queue records while retaining their results and logs."""
    return await _call(request, 'clear_finished')


@router.post('/queue/{job}/start')
async def start(job: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Launch through the separated native runtime."""
    _job_path(job)
    return await _call(request, 'start', {'id': job})


@router.post('/queue/{job}/stop')
async def stop(job: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Stop only the exact owned job."""
    _job_path(job)
    return await _call(request, 'stop', {'id': job})


@router.post('/queue/{job}/restart')
async def restart(job: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Stop and restart the same immutable snapshot after runtime identity checks."""
    _job_path(job)
    return await _call(request, 'restart', {'id': job})


@router.delete('/queue/{job}')
async def delete_job(job: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Remove a managed job after stopping its verified process."""
    _job_path(job)
    return await _call(request, 'delete_job', {'id': job})


@router.get('/results')
async def results(request: Request, name: str = '', offset: int = 0, limit: int = 100, session: SessionToken = Depends(require_auth)):
    """Page native results using the shared persistent result index."""
    return await _call(request, 'results', {'name': name, 'offset': offset, 'limit': limit})


@router.delete('/results')
async def delete_result(path: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Move a stopped native result into recoverable private trash."""
    return await _call(request, 'delete_result', {'path': path})


@router.get('/results/image')
def image(path: str, filename: str, session: SessionToken = Depends(require_auth)):
    """Serve a native result report image only from the selected result directory."""
    candidate = _safe(_read_result(path), filename)
    if candidate.suffix.lower() != '.png' or not candidate.is_file():
        raise HTTPException(404, 'PB9 result image not found')
    return FileResponse(candidate, media_type='image/png')


@router.get('/results/compare-source')
async def compare_source(path: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Resolve the reviewed PB8 source for the existing cross-version Compare view."""
    return await _call(request,'compare_source',{'path':path})


@router.get('/results/analysis')
async def analysis(path: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Return native metrics unchanged."""
    return await _call(request, 'analysis', {'path': path})


@router.get('/results/price')
async def result_price(path: str, request: Request, exchange: str = Query(...), coin: str = Query(...),
                       max_points: int = Query(6000, ge=2, le=10000), session: SessionToken = Depends(require_auth)):
    """Reuse PB7/PB8's bounded read-only PBGui candle overlay for native results."""
    return await _call(request, 'price', {'path': path, 'exchange': exchange, 'coin': coin, 'max_points': max_points})


@router.get('/results/config')
async def result_config(path: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Return the authored config for a deliberate rebacktest."""
    return await _call(request, 'result_config', {'path': path})


@router.get('/results/files')
async def files(path: str, request: Request, session: SessionToken = Depends(require_auth)):
    """List only managed native artifacts."""
    return await _call(request, 'files', {'path': path})


@router.get('/results/equity')
async def equity(path: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Adapt bounded native NPZ equity to the existing Plotly CSV display contract."""
    response = await _call(request, 'equity', {'path': path})
    return Response(json.loads(response.body)['csv'], media_type='text/csv')


@router.get('/results/fills')
def fills(path: str, session: SessionToken = Depends(require_auth)):
    """Reuse the shared fills chart contract for native CSV fills."""
    candidate = _safe(_read_result(path), 'fills.csv')
    if not candidate.is_file():
        raise HTTPException(404, 'PB9 fills not found')
    return FileResponse(candidate, media_type='text/csv')


@router.get('/queue/{job}/log')
def log(job: str, session: SessionToken = Depends(require_auth)):
    """Read a bounded tail; the UI displays it in the shared LogViewerPanel."""
    _job_path(job)
    root = Path(PBGDIR) / 'data/logs/backtests_v9'
    path = _safe(root, job + '.log')
    if not path.exists():
        return {'log': ''}
    with path.open('rb') as stream:
        stream.seek(max(0, path.stat().st_size - 100000))
        return {'log': stream.read(100000).decode('utf-8', errors='replace')}


@router.websocket('/ws/bt9')
@authenticated_push_websocket
async def websocket(websocket: WebSocket):
    """Push queue changes; all reads stay owned by PB9's bounded worker registry."""
    ws = websocket
    from api.backtest_v7 import _archive_ws_clients
    _archive_ws_clients.add(ws)
    previous = None
    try:
        while True:
            owner = getattr(ws.app.state, 'pb9_requests', None)
            if owner is None:
                break
            data = await owner.run('bt9_queue')
            signature = json.dumps(data, sort_keys=True)
            if signature != previous:
                await ws.send_json({'type': 'queue_update', 'items': data['queue'], 'settings': data['settings']})
                previous = signature
            await asyncio.sleep(2)
    except (WebSocketDisconnect, RuntimeError, HTTPException):
        pass
    finally:
        _archive_ws_clients.discard(ws)


async def shutdown():
    """Release only API-owned batch/controller state; native jobs remain independent."""
    await _worker.stop()
    with _LOCK:
        _drafts.clear()
    from api.pb9_migration import shutdown as clear_reviews
    clear_reviews()
    from api.pb9_ohlcv_tools import shutdown as stop_lookups
    await asyncio.to_thread(stop_lookups)
