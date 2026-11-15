"""PB9 backtests on the shared Run/Backtest UI with isolated native jobs and data."""
from __future__ import annotations

import asyncio
import copy
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
from backtest_autostart import claim_backtest_slot, publish_backtest_process, release_backtest_slot
from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from pb9_config import PB9ConfigError, PB9CancelSignal, get_pb9_editor_metadata, validate_pb9_backtest_config
from pb9_config_helper import check_inline_config
from pb9_backtest_runtime import launch_environment, source_identity, prepare_market_metadata
from pb9_runtime import inspect_pb9_runtime
from pb7_config import load_pb7_config, save_pb7_config
from pb9_config_storage import PB9FileContext
from pbgui_purefunc import PBGDIR, PBGUI_VERSION, PBGUI_SERIAL
from secure_files import atomic_write_private_text, ensure_private_directory

SERVICE = "BacktestV9"
router = APIRouter(route_class=_NoStoreRoute)
_LOCK = threading.RLock()
_drafts = {}


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
    """Keep native caches and incidental files out of the source and other runtimes."""
    _job_path(job)
    return _safe(_root('queue'), 'workspaces', job)


def _job_lock(job):
    """Serialize each job across processes with a bounded set of 64 lock stripes."""
    _job_path(job)
    root = _safe(_root('queue'), 'locks')
    ensure_private_directory(root)
    return advisory_file_lock(root / str(int(uuid.UUID(job)) % 64))


def _job_status(job):
    """Recover independent process ownership after API restarts without relaunching."""
    ownership = _job_path(job, '.pid.json')
    if ownership.exists():
        record = _read(ownership)
        try:
            process = psutil.Process(int(record['pid']))
            command = process.cmdline()
            item = _read(_job_path(job))
            if (abs(process.create_time() - float(record['create_time'])) < .01
                    and command == item.get('launch_command')
                    and process.cwd() == str(_working_dir(job))
                    and process.status() != psutil.STATUS_ZOMBIE):
                return 'running', record
        except (psutil.Error, ValueError, TypeError, KeyError):
            pass
    state_path = _job_path(job, '.state.json')
    if state_path.exists():
        state = _read(state_path)
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
    return items


def _enqueue(body, cancel):
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
        if reference:
            payload['comparison_reference'] = reference
        if cancel.is_set():
            raise PB9ConfigError('helper_cancelled')
        _save_source(_job_path(job, '.config.json'), source, cancel)
        payload['snapshot_sha256'] = hashlib.sha256(_job_path(job, '.config.json').read_bytes()).hexdigest()
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
                release_backtest_slot('v9', job)
                return {'ok': True}
        item = _read(_job_path(job))
        item['status_override'] = 'stopped'
        _write(_job_path(job), item)
        _write(_job_path(job, '.state.json'), {'returncode': -15, 'completed_at': time.time(), 'stopped': True})
        release_backtest_slot('v9', job)
        return {'ok': True}


def _start(job, cancel):
    """Start one detached native job, or adopt it if it is already running."""
    with _job_lock(job):
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
        work = _working_dir(job)
        ensure_private_directory(work)
        try:
            prepare_market_metadata(work, _read(snapshot), Path(PBGDIR) / 'data/coindata', Path(paths['pb9dir']),
                                    [paths.get('pb8dir'), paths.get('pb7dir')])
        except (OSError, ValueError) as exc:
            _log(SERVICE, 'PB9 offline metadata preflight failed', level='WARNING')
            raise HTTPException(409, str(exc)) from None
        if not claim_backtest_slot('v9', job, max(1, (os.cpu_count() or 1) // 2)):
            raise HTTPException(409, "All shared backtest slots are occupied")
        runner = Path(__file__).resolve().parents[1] / 'pb9_backtest_runner.py'
        state, pid, ready = [_job_path(job, ext) for ext in ('.state.json', '.pid.json', '.ready')]
        log_root = Path(PBGDIR) / 'data/logs/backtests_v9'
        ensure_private_directory(log_root)
        log = _safe(log_root, job + '.log')
        command = [paths['pb9venv'], '-I', '-B', str(runner), 'backtest', str(state), str(pid), str(ready),
                   str(runtime['cli_file']), paths['pb9dir'], str(snapshot)]
        item['started_at'] = time.time()
        item['status_override'] = 'running'
        item['launch_command'] = command
        _write(_job_path(job), item)
        process = None
        unit_started = False
        try:
            # A transient user unit survives an API service restart. The existing PB8
            # launcher is deliberately not used: its environment is unfiltered.
            env = launch_environment(paths['pb9venv'])
            from shutil import which
            systemd_socket = Path(f'/run/user/{os.getuid()}/systemd/private')
            if which('systemd-run') and systemd_socket.exists():
                unit_started = True  # submission can succeed even if its client times out
                result = subprocess.run([which('systemd-run'), '--user', '--quiet', '--collect',
                    '--unit=pbgui-pb9-backtest-' + job, '--property=Type=exec', '--property=UMask=0077',
                    f'--property=WorkingDirectory={work}',
                    f'--property=StandardOutput=append:{log}', f'--property=StandardError=append:{log}',
                    '--', '/usr/bin/env', '-i', *[f'{key}={value}' for key, value in env.items()], *command],
                    env={**env, 'XDG_RUNTIME_DIR': str(systemd_socket.parents[1])},
                    capture_output=True, timeout=15)
                if result.returncode:
                    raise RuntimeError('Could not launch the persistent PB9 backtest unit')
                unit_started = True
            else:
                descriptor = os.open(log, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
                with os.fdopen(descriptor, 'ab') as output:
                    process = subprocess.Popen(command, cwd=work, env=env,
                                               stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                                               start_new_session=True, close_fds=True)
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
                    stopped = subprocess.run(['systemctl', '--user', 'stop', 'pbgui-pb9-backtest-' + job],
                                   env={**env, 'XDG_RUNTIME_DIR': str(systemd_socket.parents[1])},
                                   capture_output=True, timeout=15, check=False)
                    cleanup_confirmed = stopped.returncode == 0
                except subprocess.SubprocessError:
                    cleanup_confirmed = False
                    _log(SERVICE, 'Could not confirm cleanup of the owned PB9 backtest unit', level='ERROR')
            if not state.exists() and _job_status(job)[0] != 'running':
                _write(state, {'returncode': 1, 'error': 'PB9 launch failed; inspect the job log', 'completed_at': time.time()})
            if cleanup_confirmed:
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


def _equity_arrays(directory):
    """Validate the native numeric archive once before adapting its clock/quantities."""
    import zipfile
    path = _safe(directory, 'balance_and_equity.npz')
    if not path.is_file():
        raise HTTPException(404, 'Native PB9 equity artifact not found')
    with zipfile.ZipFile(path) as archive:
        if sum(item.file_size for item in archive.infolist()) > 256 * 1024 * 1024:
            raise HTTPException(413, 'PB9 equity artifact exceeds display limit')
    with np.load(path, allow_pickle=False) as archive:
        if archive['schema_version'].shape != () or int(archive['schema_version']) != 1:
            raise HTTPException(422, 'Unsupported native PB9 equity schema')
        arrays = [archive[key] for key in ('timestamp_ms', 'usd_total_balance', 'usd_total_equity')]
    times, balance, equity = arrays
    if (any(array.ndim != 1 or len(array) != len(times) for array in arrays)
            or times.dtype.kind not in 'iu' or np.any(np.diff(times) <= 0)
            or any(array.dtype.kind != 'f' or np.isinf(array).any() for array in (balance, equity))
            or not np.isfinite(equity).all()):
        raise HTTPException(422, 'Invalid native PB9 equity shape, clock or values')
    return times, balance, equity


def _summaries(paths):
    """Translate native v9 metrics to the shared result table without a v8 fallback."""
    items = []
    root = _root('results')
    for analysis_path in paths:
        directory = _result(str(analysis_path.parent.relative_to(root)))
        analysis = _read(_safe(directory, 'analysis.json'), 8 * 1024 * 1024)
        config = _read(_safe(directory, 'config.json'))
        bt, bot = config.get('backtest', {}), config.get('bot', {})
        metrics = {key: value for key, value in analysis.items() if isinstance(value, (float, int)) and math.isfinite(value)}
        def metric(*keys, default=0):
            return next((metrics[key] for key in keys if key in metrics), default)
        source = _safe(directory, 'pbgui.source.json')
        if not source.exists():
            source = _safe(directory, 'config.original.json')
        authored = _read(source) if source.exists() else config
        coins = authored.get('bot', {}).get('global', {}).get('universe', {}).get('approved_coins', [])
        parts = directory.relative_to(root).parts
        final_balance = final_equity = adg = gain = None
        if _safe(directory, 'balance_and_equity.npz').is_file():
            times, balances, equities = _equity_arrays(directory)
            if len(times):
                final_balance = float(balances[-1]) if math.isfinite(float(balances[-1])) else None
                final_equity = float(equities[-1])
                start = float(bt.get('starting_balance') or 0)
                gain = final_equity / start if start > 0 else None
                days = (int(times[-1]) - int(times[0])) / 86400000
                if days > 0 and equities[0] > 0 and final_equity > 0:
                    exponent = math.log(final_equity / float(equities[0])) / days
                    adg = math.expm1(exponent) if exponent < 700 else None
        coins = sorted({coin for side in ('long', 'short')
                        for coin in (authored.get('bot', {}).get(side, {}).get('universe', {}).get('approved_coins')
                                     or authored.get('bot', {}).get('global', {}).get('universe', {}).get('approved_coins') or [])
                        if isinstance(coin, str)})
        item = {**metrics, 'path': str(directory), 'config_name': parts[0],
                'backtest_version': 'v9', 'config_version': config.get('config_version'),
                'strategy': bot.get('portfolio', {}).get('strategy_kind', ''),
                'exchange': ', '.join(bt.get('exchanges', [])), 'exchange_dir': ', '.join(bt.get('exchanges', [])), 'exchanges': bt.get('exchanges', []),
                'display_name': str(directory.relative_to(root)), 'result_name': directory.name, 'metrics': metrics, 'coins_text': ', '.join(coins),
                'start_date': analysis.get('effective_start_date', bt.get('start_date')), 'end_date': analysis.get('effective_end_date', bt.get('end_date')), 'coins': coins,
                'modified': dt.datetime.fromtimestamp(analysis_path.stat().st_mtime).isoformat(), 'result_time': directory.name,
                'starting_balance': metric('starting_balance_usd', 'starting_balance', default=bt.get('starting_balance', 0)),
                'final_balance': final_balance, 'final_equity': final_equity, 'gain': gain, 'adg': adg,
                'sharpe_ratio': metric('sharpe_ratio_strategy_equity', default=None),
                'drawdown_worst': metric('drawdown_max_fraction_strategy_equity', default=None),
                'liquidated': analysis.get('liquidated') is True,
                'analysis': analysis}
        for side in ('long', 'short'):
            values = {**authored.get('bot', {}).get('global', {}).get('risk', {}), **authored.get('bot', {}).get(side, {}).get('risk', {})}
            enabled = authored.get('bot', {}).get(side, {}).get('enabled') is True
            item['twe_' + side] = values.get('total_wallet_exposure_limit_ratio', 0) if enabled else 0
            item['n_positions_' + side] = values.get('max_positions', 0) if enabled else 0
            item['pos_' + side] = item['n_positions_' + side]
        items.append(item)
    return items



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
        if operation == 'bt9_migrate_preview':
            from api.pb9_migration import preview
            return preview(body, args['owner'], _paths(), cancel)
        if operation == 'bt9_migrate_accept':
            from api.pb9_migration import reviewed
            status, _ = _runtime(cancel)
            review = reviewed(body, args['owner'], status)
            source, _ = _native(review['config'], cancel)
            if review.get('comparison_reference'):
                source['pbgui'] = {'comparison_reference': review['comparison_reference']}
            with _LOCK:
                for key, value in list(_drafts.items()):
                    if time.monotonic() - value['created'] > 600:
                        del _drafts[key]
                if len(_drafts) >= 100:
                    del _drafts[next(iter(_drafts))]
                token = str(uuid.uuid4())
                _drafts[token] = {'created': time.monotonic(), 'owner': args['owner'], 'config': source,
                                  'migration_report': review['report']}
            return {'draft_id': token, 'suggested_name': review['name']}
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
            with _LOCK:
                for key, value in list(_drafts.items()):
                    if time.monotonic() - value['created'] > 600:
                        del _drafts[key]
                if len(_drafts) >= 100:
                    del _drafts[next(iter(_drafts))]
                token = str(uuid.uuid4())
                _drafts[token] = {'created': time.monotonic(), 'owner': args['owner'], 'config': source}
            return {'draft_id': token}
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
                        'strategy': bot.get('portfolio', {}).get('strategy_kind', ''), 'coins': 0,
                        'twe_long': _side_risk(config, 'long').get('total_wallet_exposure_limit_ratio', 0),
                        'twe_short': _side_risk(config, 'short').get('total_wallet_exposure_limit_ratio', 0), 'start_date': bt.get('start_date'), 'end_date': bt.get('end_date'),
                        'modified': path.stat().st_mtime, 'results': len(list(_safe(_root('results'), path.parent.name).glob('**/analysis.json')))})
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
            import shutil
            with _LOCK, _storage_lock():
                if any(item['name'] == args['name'] and item['status'] == 'running' for item in _queue()):
                    raise HTTPException(409, 'Stop this config backtest before deleting it')
                directory = _cfg_path(args['name']).parent
                if not directory.is_dir():
                    raise HTTPException(404, 'PB9 config not found')
                trash = _root('configs') / '.trash'
                ensure_private_directory(trash)
                os.replace(directory, _safe(trash, args['name'] + '-' + uuid.uuid4().hex))
            return {'ok': True}
        if operation == 'bt9_enqueue':
            return _enqueue(body, cancel)
        if operation == 'bt9_queue':
            items = _queue()
            return {'queue': items, 'items': items}
        if operation == 'bt9_start':
            return _start(args['id'], cancel)
        if operation in {'bt9_stop', 'bt9_restart', 'bt9_delete_job'}:
            with _job_lock(args['id']):
                if operation != 'bt9_delete_job' or _job_status(args['id'])[0] == 'running':
                    _stop(args['id'])
                if operation == 'bt9_restart':
                    item = _read(_job_path(args['id']))
                    item.pop('started_at', None); item.pop('status_override', None)
                    _write(_job_path(args['id']), item)
                    _job_path(args['id'], '.state.json').unlink(missing_ok=True)
                elif operation == 'bt9_delete_job':
                    for suffix in ('.json', '.config.json', '.pid.json', '.state.json', '.ready'):
                        _job_path(args['id'], suffix).unlink(missing_ok=True)
                    return {'ok': True}
                else:
                    return {'ok': True}
            return _start(args['id'], cancel)
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
            return _read(_safe(_result(args['path']), 'analysis.json'), 8 * 1024 * 1024)
        if operation == 'bt9_price':
            config = _read(_safe(_result(args['path']), 'config.json'))
            try:
                return build_market_price_payload(config, exchange=args['exchange'], coin=args['coin'],
                                                  max_points=args['max_points'])
            except ValueError as exc:
                _log(SERVICE, 'PB9 result price request rejected', level='WARNING')
                raise HTTPException(400, str(exc)) from None
        if operation == 'bt9_result_config':
            directory = _result(args['path'])
            original = _safe(directory, 'pbgui.source.json')
            if not original.exists():
                original = _safe(directory, 'config.original.json')
            source = _read(original if original.exists() else _safe(directory, 'config.json'))
            reference = _result_reference(directory)
            if reference:
                source['pbgui'] = {'comparison_reference': reference}
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
                        for suffix in ('.json', '.config.json', '.pid.json', '.state.json', '.ready'):
                            _job_path(item['filename'], suffix).unlink(missing_ok=True)
                        release_backtest_slot('v9', item['filename'])
                        count += 1
            return {'ok': True, 'removed': count}
        if operation == 'bt9_files':
            directory = _result(args['path'])
            return {'files': [str(p.relative_to(directory)) for p in directory.rglob('*')
                              if p.is_file() and not p.is_symlink() and _safe(directory, str(p.relative_to(directory)))]}
        if operation == 'bt9_equity':
            times, balance, equity = _equity_arrays(_result(args['path']))
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
    for key, value in {'VERSION': PBGUI_VERSION, 'SERIAL': str(PBGUI_SERIAL), 'BACKTEST_VERSION': 'v9',
                       'BACKTEST_LABEL': 'V9', 'BACKTEST_SUBTITLE': 'PBv9 BACKTEST', 'BACKTEST_NAV_CURRENT': 'v9_backtest'}.items():
        html = html.replace('"%%' + key + '%%"', script_json(value)).replace('%%' + key + '%%', value)
    return HTMLResponse(html, headers={'Cache-Control': 'no-store'})


@router.get('/settings')
def settings(session: SessionToken = Depends(require_auth)):
    """Expose capabilities rather than silently forwarding unsupported v8 features."""
    return {'autostart': False, 'cpu': 1, 'exchange_options': [],
            'capabilities': {'local': True, 'native_schema': 'v9', 'optimizer': False, 'archive': False, 'autostart': False}}


def _pbgui_data_path():
    """Use the same shared OHLCV root as PB7/PB8, without a bot-slot dependency."""
    from market_data import get_market_data_root_dir
    return str(get_market_data_root_dir())


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
    if remove_results:
        raise HTTPException(422, 'Delete PB9 results separately from their result view')
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
    candidate = _safe(_result(path), filename)
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
    candidate = _safe(_result(path), 'fills.csv')
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
    previous = None
    try:
        while True:
            owner = getattr(ws.app.state, 'pb9_requests', None)
            if owner is None:
                break
            data = await owner.run('bt9_queue')
            signature = json.dumps(data, sort_keys=True)
            if signature != previous:
                await ws.send_json({'type': 'queue_update', 'items': data['queue']})
                previous = signature
            await asyncio.sleep(2)
    except (WebSocketDisconnect, RuntimeError, HTTPException):
        pass


async def shutdown():
    """Release only API-owned batch/controller state; native jobs remain independent."""
    with _LOCK:
        _drafts.clear()
    from api.pb9_migration import shutdown as clear_reviews
    clear_reviews()
