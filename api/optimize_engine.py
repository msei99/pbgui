"""Shared PB8/PB9 optimize engine; generation profiles own all runtime state."""
from __future__ import annotations
from optimize_profile import OptimizeProfile
import asyncio
import copy
import datetime
import hashlib
import json
import multiprocessing
import os
import platform
import re
import secrets
import signal
import stat
import socket
import subprocess
import threading
import time
import traceback
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path, PurePath
from shutil import copy2, rmtree, which
from typing import Callable, Optional
import psutil
import httpx
import msgpack
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response
from api.auth import SessionToken, authenticate_websocket, require_auth
from api.page_templates import render_page_urls, script_json
from api.pb8_ohlcv_tools import PB8OhlcvUnavailableError, build_pb8_ohlcv_preflight, get_pb8_ohlcv_start_date_job, get_pb8_ohlcv_preload_job, safe_start_date_error, shutdown_pb8_ohlcv_start_date_jobs, start_pb8_ohlcv_start_date_job, start_pb8_ohlcv_preload_job, stop_pb8_ohlcv_start_date_job, stop_pb8_ohlcv_preload_job
from file_lock import advisory_file_lock
from optimizer_workload import estimate_snapshot
from logging_helpers import append_managed_transcript_line, human_log as _log, rotate_managed_log_before_open
from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from optimize_autostart import claim_autostart, publish_autostart_process, release_autostart
from api.v8_migration_context import apply_legacy_churn_gate, extract_legacy_churn_gate, postprocess_v7_migration, sanitize_optimize_migration_source
from pb8_config import PB8ConfigurationError, cache_prepared_pb8_config, get_pb8_optimize_metadata, get_pb8_exchange_metadata, interrupt_pb8_migration_helper, load_pb8_config, load_pb8_editor_config, preview_pb8_hsl_migration, migrate_pb7_config, prepare_pb8_migration_helper_startup, prepare_pb8_config, shutdown_pb8_migration_helper, start_pb8_migration_helper, validate_pb8_override_bundle, validate_pb8_optimizer_overrides, validate_pb8_optimize_preflight
from pbgui_purefunc import PBGDIR, PBGUI_SERIAL, PBGUI_VERSION, load_ini_section, pb7dir, pb8_runtime_status, save_ini_section
from scenario_templates import ScenarioTemplateError, generate_scenario_template, list_scenario_templates
from secure_files import atomic_write_private_text, ensure_private_directory, ensure_private_directory_tree, read_regular_file_nofollow
from scenario_windows import VALIDATION_PLAN_FILENAME, build_validation_plan, validation_holdouts
from sweep_cycles import SWEEP_METRIC_NAMES, SWEEP_PLAN_FILENAME, build_sweep_plan, evaluate_sweep_cycles, validate_sweep_plan
from optimize_convergence import convergence_complete, runner_complete, validate_config as validate_convergence_config
from pb8_local_convergence import STATE_FILENAME as CONVERGENCE_STATE_FILENAME, compatible_history as compatible_convergence_history, read_state as read_convergence_state
SERVICE = 'OptimizeV8'
_CONFIG_FILENAME = 'optimize.json'
_CONFIG_SECTIONS = ('backtest', 'bot', 'live', 'optimize', 'coin_overrides', 'pbgui')
_QUEUE_SETTINGS_SECTION = 'optimize_v7'
_PARETO_STATISTICS = ('mean', 'min', 'max', 'std', 'median')
_PARETO_METRIC_NAME_RE = re.compile('^[A-Za-z0-9_.-]{1,128}$')
_PARETO_MAX_REQUESTED_METRICS = 256
_PARETO_COMPARISON_METRICS = (('adg', ('adg_strategy_eq_w', 'adg_strategy_eq', 'adg_w_usd', 'adg_usd', 'adg')), ('gain', ('gain_usd', 'gain_strategy_eq', 'gain')), ('drawdown_worst', ('drawdown_worst_strategy_eq', 'drawdown_worst_w_usd', 'drawdown_worst_usd', 'drawdown_worst')), ('sharpe_ratio', ('sharpe_ratio_strategy_eq', 'sharpe_ratio_w_usd', 'sharpe_ratio_usd', 'sharpe_ratio')), ('loss_profit_ratio', ('loss_profit_ratio',)), ('sortino_ratio', ('sortino_ratio_strategy_eq', 'sortino_ratio_w_usd', 'sortino_ratio_usd', 'sortino_ratio')), ('omega_ratio', ('omega_ratio_strategy_eq', 'omega_ratio_w_usd', 'omega_ratio_usd', 'omega_ratio')), ('equity_balance_diff_neg_max', ('equity_balance_diff_neg_max_strategy_eq', 'equity_balance_diff_neg_max_usd', 'equity_balance_diff_neg_max')))
_LAUNCH_MODES = {'fresh', 'pareto_seed', 'checkpoint_resume'}
_RESULT_PROGRESS_CACHE_TTL_SECONDS = 15 * 60
_RESULT_PROGRESS_CACHE_MAX_ENTRIES = 64
_ACTIVE_EVAL_SCAN_BUDGET_BYTES = 32 * 1024 * 1024
_ACTIVE_EVAL_SCAN_BUDGET_SECONDS = 0.2
_RESULT_LIST_SCAN_LIMIT_BYTES = 8 * 1024 * 1024
_MAX_OVERRIDE_BYTES = 1024 * 1024
_MAX_OVERRIDE_BUNDLE_BYTES = 8 * 1024 * 1024
_BACKTEST_COUNT_CACHE_TTL_SECONDS = 30
_BACKTEST_COUNT_CACHE_MAX_ENTRIES = 512
_CONFIG_SUMMARY_CACHE_MAX_ENTRIES = 512
_BACKTEST_COUNT_BATCH_MAX_NAMES = 16
_BACKTEST_COUNT_SCAN_SECONDS = 0.1
_BACKTEST_COUNT_SCAN_MAX_ENTRIES = 10000
_BACKTEST_COUNT_SCAN_MAX_ACTIVE = 256
_BACKTEST_COUNT_SCAN_IDLE_SECONDS = 120
_BACKTEST_COUNT_SCAN_MAX_AGE_SECONDS = 600
_BACKTEST_COUNT_SCAN_MAX_PATH_BYTES = 256 * 1024
_BACKTEST_COUNT_SAFE_SCANDIR_AVAILABLE = hasattr(os, 'O_DIRECTORY') and hasattr(os, 'O_NOFOLLOW') and (os.open in os.supports_dir_fd) and (os.scandir in os.supports_fd)
_COUNT_ROOT_UNSET = object()
_PARETO_LIST_CACHE_TTL_SECONDS = 5 * 60
_PARETO_LIST_CACHE_MAX_ENTRIES = 2048
_PARETO_WARNING_TTL_SECONDS = 60
_PARETO_WARNING_MAX_ENTRIES = 64
_DASH_REQUEST_HEADERS_ALLOW = {'accept', 'accept-language', 'cache-control', 'content-type', 'pragma', 'user-agent'}
_DASH_RESPONSE_HEADERS_DROP = {'connection', 'content-encoding', 'content-length', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'set-cookie', 'te', 'trailer', 'transfer-encoding', 'upgrade'}
_DASH_MAX_ACTIVE_SESSIONS = 4
_DASH_IDLE_TTL_SECONDS = 15 * 60
_DASH_REGISTRY_VERSION = 1
_RESULT_LISTING_CACHE_TTL_SECONDS = 10 * 60
_RESULT_LISTING_CACHE_MAX_ENTRIES = 256
_EVALUATION_PROGRESS_FILENAME = '.pbgui_optimize_progress.json'
_MIGRATION_DRAFT_TTL_SECONDS = 600
_MIGRATION_DRAFT_MAX_ENTRIES = 100
_OPT_LOG_LINE_RE = re.compile('^(?P<ts>\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:?\\d{2})?)\\s+(?:(?P<level>[A-Z]+)\\s+)?(?P<msg>.*)$')
_OPT_LOG_BACKEND_RE = re.compile('Selected optimizer backend:\\s*(?P<backend>[a-z0-9_]+)', re.IGNORECASE)
_OPT_LOG_PYMOO_RE = re.compile('Using pymoo\\s+(?P<algorithm>[a-z0-9_]+)(?:\\s*\\|\\s*n_obj=(?P<n_obj>\\d+))?', re.IGNORECASE)
_OPT_LOG_ITER_RE = re.compile('Iter:\\s*(?P<iter>\\d+).*?size:(?P<size>\\d+)(?:\\s*\\|\\s*(?P<ranges>.*))?$', re.IGNORECASE)
_OPT_LOG_EVAL_RE = re.compile('(?:Pareto update\\s*\\|\\s*)?eval(?:uations)?\\s*[=:]\\s*(?P<eval>\\d+)', re.IGNORECASE)
_OPT_LOG_RANGE_RE = re.compile('(?P<name>[a-zA-Z0-9_.-]+):\\((?P<min>[^,]+),(?P<max>[^)]+)\\)')
_OPT_LOG_GPU_RE = re.compile('GPU optimize\\s*\\|\\s*gen=(?P<generation>\\d+)\\s+proxy=(?P<proxy>\\d+)\\s+\\((?P<rate>[0-9.]+)/s\\)\\s+exact=(?P<exact>\\d+)\\s+inflight=(?P<inflight>\\d+)', re.IGNORECASE)
_OPT_LOG_GPU_PROGRESS_RE = re.compile('GPU optimizer progress\\s*\\|\\s*gen=(?P<generation>\\d+)\\s+phase=(?P<stage>\\w+)\\s*\\|\\s*evolution_proxy_completed_run=(?P<proxy>\\d+)(?=\\s|$)[^\\n]*?\\bevolution_exact=(?P<exact>\\d+)/\\d+(?=\\s|$)[^\\n]*?\\bevolution_pending=(?P<pending>\\d+)(?=\\s|$)', re.IGNORECASE)
_OPT_LOG_GPU_DISPATCH_RE = re.compile('GPU proxy dispatch progress\\s*\\|\\s*strategy=(?P<strategy>\\S+)\\s+chunks=(?P<chunks_done>\\d+)/(?P<chunks_total>\\d+)\\s+candidates=(?P<candidates_done>\\d+)/(?P<candidates_total>\\d+)\\s+elapsed=(?P<elapsed>[0-9.]+)s\\s+eta=(?P<eta>[0-9.]+)s', re.IGNORECASE)
_OPT_LOG_GPU_REPLAY_RE = re.compile('GPU temporal replay\\s+(?P<state>start|progress|complete)\\s*\\|\\s*(?:suite_pass=(?P<pass_done>\\d+)/(?P<pass_total>\\d+)\\s+scenarios=(?P<scenarios>\\S+)\\s+exchange=(?P<exchange>\\S+)\\s+history=(?P<history>[0-9.]+)%\\s+)?replay=(?P<replay>\\d+)\\s+candidates=(?P<candidates>\\d+)\\s+bars=(?P<bars_done>\\d+)/(?P<bars_total>\\d+)\\s+elapsed=(?P<elapsed>[0-9.]+)s', re.IGNORECASE)
_OPT_LOG_GPU_HALVING_RE = re.compile('GPU successive halving\\s*\\|\\s*gen=(?P<generation>\\d+)\\s+rungs=(?P<rungs>\\S+)\\s+full_history=(?P<full_done>\\d+)/(?P<full_total>\\d+)', re.IGNORECASE)
_OPT_LOG_GPU_RESUME_RE = re.compile('Resumed GPU optimizer at generation\\s+(?P<generation>\\d+)\\s+with\\s+(?:(?P<seed>\\d+)\\s+seed\\s+and\\s+)?(?P<exact>\\d+)\\s+exact evaluations', re.IGNORECASE)
_OPT_LOG_GPU_COMPLETE_RE = re.compile('GPU optimization complete\\s*\\|\\s*generations=(?P<generation>\\d+)\\s+proxy=(?P<proxy>\\d+)\\s+exact=(?P<exact>\\d+)', re.IGNORECASE)
_LOG_TERMINAL_STATUS_CACHE_MAX = 1024

class OptimizeWorker:
    """Own the PB8 autostart controller while detached optimizers survive API restarts."""

    def __init__(self, engine) -> None:
        self.engine = engine
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._launch_lock = self.engine.threading.Lock()
        self._launch_executor: ThreadPoolExecutor | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._running = True
            self._task = self.engine.asyncio.create_task(self._loop(), name=self.engine.profile.text('optimize-v8-worker'))

    async def stop(self) -> None:
        self._running = False
        task = self._task
        if task is None:
            return
        if not task.done():
            task.cancel()
        await self.engine.asyncio.gather(task, return_exceptions=True)
        if self._task is task:
            self._task = None

    async def _loop(self) -> None:
        while self._running:
            delay = 10
            try:
                settings = self.engine.load_ini_section(self.engine._QUEUE_SETTINGS_SECTION)
                if str(settings.get('autostart', 'False')).lower() != 'true':
                    delay = 5
                else:
                    filename = await self._claim_next_autostart()
                    if filename and (not self._running):
                        self.engine.release_autostart(self.engine.profile.text('v8'), filename)
                        break
                    if filename:
                        await self._launch_autostart(filename)
            except self.engine.asyncio.CancelledError:
                break
            except Exception as exc:
                delay = 5
                self.engine._log(self.engine.SERVICE, f'PB8 optimize worker iteration failed: {exc}', level='ERROR', meta={'traceback': self.engine.traceback.format_exc()})
            try:
                await self.engine.asyncio.sleep(delay)
            except self.engine.asyncio.CancelledError:
                break

    async def _claim_next_autostart(self) -> str | None:
        """Claim off the event loop; a claim finishing after cancellation is released, not leaked."""
        claim = self.engine.asyncio.ensure_future(self.engine.asyncio.to_thread(self._claim_autostart_candidate))
        if not await self.engine._wait_through_cancellation(claim):
            return claim.result()
        if claim.exception() is None and claim.result():
            try:
                self.engine.release_autostart(self.engine.profile.text('v8'), claim.result())
            except Exception as exc:
                self.engine._log(self.engine.SERVICE, f'Failed to release PB8 autostart claim after stop: {exc}', level='ERROR')
        raise self.engine.asyncio.CancelledError

    async def _launch_autostart(self, filename: str) -> None:
        """Own a launch whose thread settles its claim even after the event loop closes."""
        with self._launch_lock:
            try:
                if self._launch_executor is None:
                    self._launch_executor = self.engine.ThreadPoolExecutor(max_workers=1, thread_name_prefix=self.engine.profile.text('optimize-v8-launch'))
                launch = self._launch_executor.submit(self.engine._settle_autostart_launch, filename, self.launch)
            except Exception:
                self.engine.release_autostart(self.engine.profile.text('v8'), filename)
                raise
        await self.engine.asyncio.shield(self.engine.asyncio.wrap_future(launch))

    async def drain_autostart_launches(self) -> None:
        """Join launch threads during API shutdown without terminating detached optimizer jobs."""
        with self._launch_lock:
            executor = self._launch_executor
        if executor is not None:
            closing = self.engine.asyncio.create_task(self.engine.asyncio.to_thread(executor.shutdown, wait=True), name=self.engine.profile.text('optimize-v8-launch-shutdown'))
            await self.engine._wait_through_cancellation(closing)
            closing.result()
            with self._launch_lock:
                if self._launch_executor is executor:
                    self._launch_executor = None

    def _claim_autostart_candidate(self) -> str | None:
        """Pick and claim the next autostart job off the event loop."""
        with self.engine._queue_lock():
            filename = self.engine._autostart_candidate()
            if filename and self.engine.claim_autostart(self.engine.profile.text('v8'), filename):
                return filename
        return None

    def launch(self, filename: str, launch_options: dict | None=None, automatic: bool=False, loop_id: str | None=None) -> dict:
        """Validate an immutable queue snapshot and launch one detached PB8 optimizer."""
        filename = self.engine._validate_name(filename)
        with self.engine._queue_lock():
            queue_path = self.engine._queue_file(filename)
            if not queue_path.is_file():
                raise self.engine.HTTPException(status_code=404, detail='Queue item not found')
            data = self.engine._read_json(queue_path)
            if data.get('loop_id'):
                from pb8_loop_store import authorize_native_job
                authorize_native_job(self.engine.Path(self.engine.PBGDIR), data, loop_id)
            status, _pid = self.engine._queue_status(data)
            if status != 'queued':
                raise self.engine.HTTPException(status_code=409, detail=f'Queue item is already {status}')
            if automatic:
                if self.engine._first_manual_queued_filename() != filename:
                    raise self.engine.HTTPException(status_code=409, detail='Queue order changed; automatic launch will retry')
            snapshot = self.engine._snapshot_file(filename)
            if not snapshot.is_file() or snapshot.is_symlink():
                raise self.engine.HTTPException(status_code=422, detail='Queue config snapshot is missing')
            options = self.engine._validate_launch_options(launch_options or data.get('launch_options'))
            self.engine._validate_options_for_config(options, self.engine._read_json(snapshot))
            if options['mode'] == 'pareto_seed' and options['source'] == '__self__':
                options['source'] = str(snapshot.resolve())
            elif options['mode'] == 'checkpoint_resume':
                self.engine._preflight_checkpoint_resume(self.engine._read_json(snapshot), self.engine.Path(options['source']))
            invalid_marker = self.engine.Path(self.engine.PBGDIR) / 'data' / 'locks' / self.engine.profile.text('pb8-runtime-invalid')
            if invalid_marker.exists() or invalid_marker.is_symlink():
                raise self.engine.HTTPException(status_code=409, detail='PB8 installation or update is incomplete')
            try:
                runtime_lease = self.engine.acquire_master_runtime_lock(self.engine.Path(self.engine.PBGDIR))
            except self.engine.MasterUpdateBusyError as exc:
                raise self.engine.HTTPException(status_code=409, detail=str(exc)) from exc
            try:
                settings = self.engine.load_ini_section(self.engine._QUEUE_SETTINGS_SECTION)
                pbgui_data_path = None
                if str(settings.get('use_pbgui_market_data', 'False')).lower() == 'true':
                    from market_data import get_market_data_root_dir
                    pbgui_data_path = str(get_market_data_root_dir())
                launch_config = self.engine._apply_queue_launch_settings(self.engine._read_json(snapshot), settings, automatic=automatic, pbgui_data_path=pbgui_data_path)
                sweep_plan = self.engine._validate_sweep_cycles_config(launch_config)
                prepared = self.engine.prepare_pb8_config(launch_config, base_config_path=str(snapshot))
                self.engine._validate_optimizer_overrides(prepared, base_config_path=str(snapshot))
                self.engine._validate_forager_optimize_search_space(prepared)
                self.engine._validate_optimize_backend(prepared, base_config_path=str(snapshot))
                runtime = self.engine.pb8_runtime_status()
                if not runtime.get('ready'):
                    raise self.engine.HTTPException(status_code=503, detail='PB8 runtime is not ready')
                self.engine._verify_queue_identity(data, runtime, snapshot)
                launch_dir = self.engine._launch_dir(filename)
                self.engine.rmtree(launch_dir, ignore_errors=True)
                self.engine.ensure_private_directory_tree(self.engine._queue_dir(), launch_dir)
                self.engine._write_override_payloads(launch_dir, self.engine._load_override_payloads(prepared, self.engine._snapshot_dir(filename)))
                self.engine._write_json(self.engine._launch_config_file(filename), self.engine._execution_config(prepared))
                self.engine.validate_pb8_override_bundle(self.engine._launch_config_file(filename))
                from scenario_windows import build_validation_plan
                runner_options = dict(options)
                runner_options['convergence_backend'] = self.engine._effective_optimize_value(prepared,'backend','pymoo')
                runner_options['convergence_identity'] = self.engine._convergence_identity(prepared)
                if options['mode'] == 'checkpoint_resume':
                    recovered = self.engine.prepare_pb8_config(self.engine._checkpoint_resume_readiness(self.engine.Path(options['source']))['config'])
                    runner_options['convergence_legacy_identities'] = [self.engine._legacy_convergence_identity(prepared), self.engine._legacy_convergence_identity(recovered)]
                runner_options['convergence_log'] = str(self.engine._log_dir() / f'{filename}.log')
                validation_plan = build_validation_plan(launch_config)
                if validation_plan is not None:
                    runner_options['pbgui_validation_plan'] = validation_plan
                if sweep_plan is not None:
                    runner_options['pbgui_sweep_plan'] = sweep_plan
                runner_options.update(self.engine._runner_identity(runtime, filename))
                self.engine._write_json(self.engine._launch_options_file(filename), runner_options)
                self.engine._state_file(filename).unlink(missing_ok=True)
                self.engine._state_file(filename).with_suffix('.convergence.json').unlink(missing_ok=True)
                self.engine._state_file(filename).with_suffix('.convergence.forced.json').unlink(missing_ok=True)
                self.engine._ready_file(filename).unlink(missing_ok=True)
                data.update({'started_at': self.engine.time.time(), 'launch_options': options, 'launch_message': {'fresh': 'Fresh optimizer run', 'pareto_seed': 'Continue from managed Pareto seed', 'checkpoint_resume': 'Resume exact managed checkpoint'}[options['mode']], 'automatic': bool(automatic), self.engine.profile.text('pb8_version'): runtime.get('version') or '', self.engine.profile.text('pb8_commit'): self.engine._runtime_commit(self.engine.Path(runtime[self.engine.profile.text('pb8dir')]))})
                for key in ('status_override', 'error_code', 'error_reason'):
                    data.pop(key, None)
                self.engine._write_json(queue_path, data)
                log_path = self.engine._safe_path(self.engine._log_dir() / f'{filename}.log', self.engine._log_dir())
                self.engine.ensure_private_directory(self.engine._log_dir())
                self.engine.rotate_managed_log_before_open(log_path, self.engine.profile.text('optimizes_v8'))
                command = [str(runtime[self.engine.profile.text('pb8venv')]), *self.engine._runner_python_flags(), str(self.engine.Path(self.engine.PBGDIR) / self.engine.profile.text('pb8_optimize_runner.py')), 'optimize', str(self.engine._state_file(filename)), str(self.engine._pid_file(filename)), str(self.engine._ready_file(filename)), str(runtime['cli_file']), str(runtime[self.engine.profile.text('pb8dir')]), str(self.engine._launch_config_file(filename).resolve()), str(self.engine._launch_options_file(filename).resolve())]
                process = self.engine._launch_optimizer_runner(filename, command, self.engine.Path(runtime[self.engine.profile.text('pb8dir')]), log_path)
                try:
                    record = None
                    deadline = self.engine.time.monotonic() + 120
                    while self.engine.time.monotonic() < deadline:
                        record = self.engine._read_process_record(filename)
                        if record and self.engine._ready_file(filename).is_file():
                            if int(self.engine._ready_file(filename).read_text(encoding='utf-8').strip() or 0) == record['pid']:
                                break
                        state = self.engine._read_runner_state(filename)
                        if state is not None or (process is not None and process.poll() is not None):
                            state = state or {}
                            raise RuntimeError(str(state.get('error') or 'PB8 optimize runner exited before acquiring the runtime lock'))
                        self.engine.time.sleep(0.05)
                    else:
                        raise RuntimeError('PB8 optimize runner launch handshake timed out')
                    self.engine._ready_file(filename).unlink(missing_ok=True)
                    return record
                except Exception:
                    record = record or self.engine._read_process_record(filename)
                    if record and self.engine._process_matches(filename, record):
                        self.engine._terminate_process(int(record['pid']))
                    elif process is not None:
                        self.engine._terminate_process(process.pid)
                    raise
            finally:
                runtime_lease.release()

class OptimizeEngine:
    """Shared optimizer workflows with explicitly instance-owned runtime state."""

    def __init__(self, profile: OptimizeProfile | None=None):
        """Create an optimizer engine with independent caches, workers and route ownership."""
        self.profile = profile or OptimizeProfile()
        self.version = self.profile.version
        self.label = 'PB' + self.version[1:]
        self.APIRouter = APIRouter
        self.CONVERGENCE_STATE_FILENAME = CONVERGENCE_STATE_FILENAME
        self.Callable = Callable
        self.Depends = Depends
        self.HTMLResponse = HTMLResponse
        self.HTTPException = HTTPException
        self.MasterUpdateBusyError = MasterUpdateBusyError
        self.Optional = Optional
        self.OrderedDict = OrderedDict
        self.PB8ConfigurationError = PB8ConfigurationError
        self.PB8OhlcvUnavailableError = PB8OhlcvUnavailableError
        self.PBGDIR = PBGDIR
        self.PBGUI_SERIAL = PBGUI_SERIAL
        self.PBGUI_VERSION = PBGUI_VERSION
        self.Path = Path
        self.PurePath = PurePath
        self.Query = Query
        self.Request = Request
        self.Response = Response
        self.SWEEP_METRIC_NAMES = SWEEP_METRIC_NAMES
        self.SWEEP_PLAN_FILENAME = SWEEP_PLAN_FILENAME
        self.ScenarioTemplateError = ScenarioTemplateError
        self.SessionToken = SessionToken
        self.ThreadPoolExecutor = ThreadPoolExecutor
        self.VALIDATION_PLAN_FILENAME = VALIDATION_PLAN_FILENAME
        self.WebSocket = WebSocket
        self.WebSocketDisconnect = WebSocketDisconnect
        self._log = _log
        self.acquire_master_runtime_lock = acquire_master_runtime_lock
        self.advisory_file_lock = advisory_file_lock
        self.append_managed_transcript_line = append_managed_transcript_line
        self.apply_legacy_churn_gate = apply_legacy_churn_gate
        self.asyncio = asyncio
        self.atomic_write_private_text = atomic_write_private_text
        self.authenticate_websocket = authenticate_websocket
        self.build_pb8_ohlcv_preflight = build_pb8_ohlcv_preflight
        self.build_sweep_plan = build_sweep_plan
        self.build_validation_plan = build_validation_plan
        self.cache_prepared_pb8_config = cache_prepared_pb8_config
        self.claim_autostart = claim_autostart
        self.compatible_convergence_history = compatible_convergence_history
        self.contextmanager = contextmanager
        self.convergence_complete = convergence_complete
        self.copy = copy
        self.copy2 = copy2
        self.datetime = datetime
        self.ensure_private_directory = ensure_private_directory
        self.ensure_private_directory_tree = ensure_private_directory_tree
        self.estimate_snapshot = estimate_snapshot
        self.evaluate_sweep_cycles = evaluate_sweep_cycles
        self.extract_legacy_churn_gate = extract_legacy_churn_gate
        self.generate_scenario_template = generate_scenario_template
        self.get_pb8_exchange_metadata = get_pb8_exchange_metadata
        self.get_pb8_ohlcv_preload_job = get_pb8_ohlcv_preload_job
        self.get_pb8_ohlcv_start_date_job = get_pb8_ohlcv_start_date_job
        self.get_pb8_optimize_metadata = get_pb8_optimize_metadata
        self.hashlib = hashlib
        self.httpx = httpx
        self.interrupt_pb8_migration_helper = interrupt_pb8_migration_helper
        self.json = json
        self.list_scenario_templates = list_scenario_templates
        self.load_ini_section = load_ini_section
        self.load_pb8_config = load_pb8_config
        self.load_pb8_editor_config = load_pb8_editor_config
        self.migrate_pb7_config = migrate_pb7_config
        self.msgpack = msgpack
        self.multiprocessing = multiprocessing
        self.os = os
        self.pb7dir = pb7dir
        self.pb8_runtime_status = pb8_runtime_status
        self.platform = platform
        self.postprocess_v7_migration = postprocess_v7_migration
        self.prepare_pb8_config = prepare_pb8_config
        self.prepare_pb8_migration_helper_startup = prepare_pb8_migration_helper_startup
        self.preview_pb8_hsl_migration = preview_pb8_hsl_migration
        self.psutil = psutil
        self.publish_autostart_process = publish_autostart_process
        self.re = re
        self.read_convergence_state = read_convergence_state
        self.read_regular_file_nofollow = read_regular_file_nofollow
        self.release_autostart = release_autostart
        self.render_page_urls = render_page_urls
        self.require_auth = require_auth
        self.rmtree = rmtree
        self.rotate_managed_log_before_open = rotate_managed_log_before_open
        self.runner_complete = runner_complete
        self.safe_start_date_error = safe_start_date_error
        self.sanitize_optimize_migration_source = sanitize_optimize_migration_source
        self.save_ini_section = save_ini_section
        self.script_json = script_json
        self.secrets = secrets
        self.shutdown_pb8_migration_helper = shutdown_pb8_migration_helper
        self.shutdown_pb8_ohlcv_start_date_jobs = shutdown_pb8_ohlcv_start_date_jobs
        self.signal = signal
        self.socket = socket
        self.start_pb8_migration_helper = start_pb8_migration_helper
        self.start_pb8_ohlcv_preload_job = start_pb8_ohlcv_preload_job
        self.start_pb8_ohlcv_start_date_job = start_pb8_ohlcv_start_date_job
        self.stat = stat
        self.stop_pb8_ohlcv_preload_job = stop_pb8_ohlcv_preload_job
        self.stop_pb8_ohlcv_start_date_job = stop_pb8_ohlcv_start_date_job
        self.subprocess = subprocess
        self.threading = threading
        self.time = time
        self.traceback = traceback
        self.uuid = uuid
        self.validate_convergence_config = validate_convergence_config
        self.validate_pb8_optimize_preflight = validate_pb8_optimize_preflight
        self.validate_pb8_optimizer_overrides = validate_pb8_optimizer_overrides
        self.validate_pb8_override_bundle = validate_pb8_override_bundle
        self.validate_sweep_plan = validate_sweep_plan
        self.validation_holdouts = validation_holdouts
        self.which = which
        self.SERVICE = SERVICE
        self.router = self.APIRouter()
        self._CONFIG_FILENAME = _CONFIG_FILENAME
        self._CONFIG_SECTIONS = _CONFIG_SECTIONS
        self._QUEUE_SETTINGS_SECTION = _QUEUE_SETTINGS_SECTION
        self._PARETO_STATISTICS = _PARETO_STATISTICS
        self._PARETO_METRIC_NAME_RE = _PARETO_METRIC_NAME_RE
        self._PARETO_MAX_REQUESTED_METRICS = _PARETO_MAX_REQUESTED_METRICS
        self._PARETO_COMPARISON_METRICS = _PARETO_COMPARISON_METRICS
        # PB8 gain metrics are already part of the default projection.
        self._SWEEP_GAIN_SOURCE_METRICS = ()
        self._LAUNCH_MODES = _LAUNCH_MODES
        self._RESULT_PROGRESS_CACHE_TTL_SECONDS = _RESULT_PROGRESS_CACHE_TTL_SECONDS
        self._RESULT_PROGRESS_CACHE_MAX_ENTRIES = _RESULT_PROGRESS_CACHE_MAX_ENTRIES
        self._ACTIVE_EVAL_SCAN_BUDGET_BYTES = _ACTIVE_EVAL_SCAN_BUDGET_BYTES
        self._ACTIVE_EVAL_SCAN_BUDGET_SECONDS = _ACTIVE_EVAL_SCAN_BUDGET_SECONDS
        self._RESULT_LIST_SCAN_LIMIT_BYTES = _RESULT_LIST_SCAN_LIMIT_BYTES
        self._MAX_OVERRIDE_BYTES = _MAX_OVERRIDE_BYTES
        self._MAX_OVERRIDE_BUNDLE_BYTES = _MAX_OVERRIDE_BUNDLE_BYTES
        self._BACKTEST_COUNT_CACHE_TTL_SECONDS = _BACKTEST_COUNT_CACHE_TTL_SECONDS
        self._BACKTEST_COUNT_CACHE_MAX_ENTRIES = _BACKTEST_COUNT_CACHE_MAX_ENTRIES
        self._CONFIG_SUMMARY_CACHE_MAX_ENTRIES = _CONFIG_SUMMARY_CACHE_MAX_ENTRIES
        self._BACKTEST_COUNT_BATCH_MAX_NAMES = _BACKTEST_COUNT_BATCH_MAX_NAMES
        self._BACKTEST_COUNT_SCAN_SECONDS = _BACKTEST_COUNT_SCAN_SECONDS
        self._BACKTEST_COUNT_SCAN_MAX_ENTRIES = _BACKTEST_COUNT_SCAN_MAX_ENTRIES
        self._BACKTEST_COUNT_SCAN_MAX_ACTIVE = _BACKTEST_COUNT_SCAN_MAX_ACTIVE
        self._BACKTEST_COUNT_SCAN_IDLE_SECONDS = _BACKTEST_COUNT_SCAN_IDLE_SECONDS
        self._BACKTEST_COUNT_SCAN_MAX_AGE_SECONDS = _BACKTEST_COUNT_SCAN_MAX_AGE_SECONDS
        self._BACKTEST_COUNT_SCAN_MAX_PATH_BYTES = _BACKTEST_COUNT_SCAN_MAX_PATH_BYTES
        self._BACKTEST_COUNT_SAFE_SCANDIR_AVAILABLE = _BACKTEST_COUNT_SAFE_SCANDIR_AVAILABLE
        self._COUNT_ROOT_UNSET = _COUNT_ROOT_UNSET
        self._PARETO_LIST_CACHE_TTL_SECONDS = _PARETO_LIST_CACHE_TTL_SECONDS
        self._PARETO_LIST_CACHE_MAX_ENTRIES = _PARETO_LIST_CACHE_MAX_ENTRIES
        self._PARETO_WARNING_TTL_SECONDS = _PARETO_WARNING_TTL_SECONDS
        self._PARETO_WARNING_MAX_ENTRIES = _PARETO_WARNING_MAX_ENTRIES
        self._DASH_REQUEST_HEADERS_ALLOW = _DASH_REQUEST_HEADERS_ALLOW
        self._DASH_RESPONSE_HEADERS_DROP = _DASH_RESPONSE_HEADERS_DROP
        self._DASH_MAX_ACTIVE_SESSIONS = _DASH_MAX_ACTIVE_SESSIONS
        self._DASH_IDLE_TTL_SECONDS = _DASH_IDLE_TTL_SECONDS
        self._DASH_REGISTRY_VERSION = _DASH_REGISTRY_VERSION
        self._dash_sessions = {}
        self._dash_pending_sessions = {}
        self._dash_lock = self.threading.RLock()
        self._dash_admission_open = False
        self._result_progress_cache = self.OrderedDict()
        self._result_progress_cache_lock = self.threading.RLock()
        self._result_listing_cache = self.OrderedDict()
        self._result_listing_cache_lock = self.threading.RLock()
        self._RESULT_LISTING_CACHE_TTL_SECONDS = _RESULT_LISTING_CACHE_TTL_SECONDS
        self._RESULT_LISTING_CACHE_MAX_ENTRIES = _RESULT_LISTING_CACHE_MAX_ENTRIES
        self._active_eval_count_cache = self.OrderedDict()
        self._active_eval_count_cache_lock = self.threading.RLock()
        self._active_eval_scan_threads = {}
        self._active_eval_scan_stops = {}
        self._EVALUATION_PROGRESS_FILENAME = _EVALUATION_PROGRESS_FILENAME
        self._backtest_count_cache = self.OrderedDict()
        self._backtest_count_cache_lock = self.threading.RLock()
        self._backtest_count_scans = self.OrderedDict()
        self._backtest_count_expired = self.OrderedDict()
        self._backtest_count_scans_lock = self.threading.RLock()
        self._backtest_count_scans_condition = self.threading.Condition(self._backtest_count_scans_lock)
        self._backtest_count_scans_accepting = True
        self._backtest_count_active_requests = 0
        self._backtest_count_scan_loop = None
        self._config_summary_cache = self.OrderedDict()
        self._config_summary_cache_lock = self.threading.RLock()
        self._pareto_list_cache = self.OrderedDict()
        self._pareto_list_cache_lock = self.threading.RLock()
        self._pareto_warning_cache = self.OrderedDict()
        self._migration_drafts = {}
        self._migration_draft_lock = self.threading.RLock()
        self._MIGRATION_DRAFT_TTL_SECONDS = _MIGRATION_DRAFT_TTL_SECONDS
        self._MIGRATION_DRAFT_MAX_ENTRIES = _MIGRATION_DRAFT_MAX_ENTRIES
        self._migration_helper_warmup_task = None
        self._OPT_LOG_LINE_RE = _OPT_LOG_LINE_RE
        self._OPT_LOG_BACKEND_RE = _OPT_LOG_BACKEND_RE
        self._OPT_LOG_PYMOO_RE = _OPT_LOG_PYMOO_RE
        self._OPT_LOG_ITER_RE = _OPT_LOG_ITER_RE
        self._OPT_LOG_EVAL_RE = _OPT_LOG_EVAL_RE
        self._OPT_LOG_RANGE_RE = _OPT_LOG_RANGE_RE
        self._OPT_LOG_GPU_RE = _OPT_LOG_GPU_RE
        self._OPT_LOG_GPU_PROGRESS_RE = _OPT_LOG_GPU_PROGRESS_RE
        self._OPT_LOG_GPU_DISPATCH_RE = _OPT_LOG_GPU_DISPATCH_RE
        self._OPT_LOG_GPU_REPLAY_RE = _OPT_LOG_GPU_REPLAY_RE
        self._OPT_LOG_GPU_HALVING_RE = _OPT_LOG_GPU_HALVING_RE
        self._OPT_LOG_GPU_RESUME_RE = _OPT_LOG_GPU_RESUME_RE
        self._OPT_LOG_GPU_COMPLETE_RE = _OPT_LOG_GPU_COMPLETE_RE
        self._LOG_TERMINAL_STATUS_CACHE_MAX = _LOG_TERMINAL_STATUS_CACHE_MAX
        self._log_terminal_status_cache = {}
        self._log_terminal_status_lock = self.threading.Lock()
        engine = self

        class RuntimeOptimizeWorker(OptimizeWorker):
            """Bind an ordinary worker class to exactly this optimizer engine."""

            def __init__(self):
                super().__init__(engine)
        self.OptimizeV8Worker = RuntimeOptimizeWorker
        self._worker = RuntimeOptimizeWorker()
        self._ws_clients = set()
        self._ws_snapshot_lock = self.asyncio.Lock()
        self._ws_snapshot = None
        self._ws_snapshot_until = 0.0
        self.router.websocket(self.profile.text('/ws/opt8'))(self.ws_optimize)
        self.router.get('/main_page', response_class=HTMLResponse)(self.main_page)
        self.router.get('/runtime')(self.runtime_status)
        self.router.get('/metadata')(self.get_metadata)
        self.router.get('/scenario-templates')(self.get_scenario_templates)
        self.router.post('/scenario-templates/preview')(self.preview_scenario_template)
        self.router.get('/scenario-templates/sources')(self.scenario_chart_sources)
        self.router.get('/scenario-templates/chart')(self.scenario_chart_data)
        self.router.get('/pbgui_data_path')(self.get_pbgui_data_path)
        self.router.post('/ohlcv-preflight')(self.get_ohlcv_preflight)
        self.router.post('/ohlcv-start-dates')(self.start_ohlcv_start_date_lookup)
        self.router.get('/ohlcv-start-dates/{job_id}')(self.get_ohlcv_start_date_lookup)
        self.router.delete('/ohlcv-start-dates/{job_id}')(self.stop_ohlcv_start_date_lookup)
        self.router.post('/ohlcv-preload')(self.start_ohlcv_preload)
        self.router.get('/ohlcv-preload/{job_id}')(self.get_ohlcv_preload)
        self.router.delete('/ohlcv-preload/{job_id}')(self.stop_ohlcv_preload)
        self.router.get('/settings')(self.get_settings)
        self.router.post('/settings')(self.update_settings)
        self.router.get('/bot-params')(self.get_bot_params)
        self.router.get('/configs/new-config')(self.new_config)
        self.router.post('/configs/prepare')(self.prepare_config)
        self.router.get('/configs')(self.list_configs)
        self.router.post('/configs/backtest-counts')(self.config_backtest_counts)
        self.router.get('/configs/{name}')(self.get_config)
        self.router.post('/configs/{name}/migrate-hsl')(self.preview_hsl_migration)
        self.router.put('/configs/{name}')(self.save_config)
        self.router.post('/configs/{name}/duplicate')(self.duplicate_config)
        self.router.delete('/configs/{name}')(self.delete_config)
        self.router.get('/migration-draft/{draft_id}')(self.get_migration_draft)
        self.router.post('/migrate-v7')(self.migrate_v7)
        self.router.get('/queue')(self.get_queue)
        self.router.post('/queue/reorder')(self.reorder_queue)
        self.router.post('/queue')(self.add_to_queue)
        self.router.post('/queue/{filename}/start')(self.start_queue_item)
        self.router.post('/queue/{filename}/requeue')(self.requeue_fresh)
        self.router.post('/queue/{filename}/requeue-fresh')(self.requeue_fresh)
        self.router.post('/queue/{filename}/restart')(self.requeue_fresh)
        self.router.post('/queue/{filename}/continue-pareto')(self.continue_from_pareto)
        self.router.post('/queue/{filename}/resume-checkpoint')(self.resume_checkpoint)
        self.router.post('/queue/{filename}/stop')(self.stop_queue_item)
        self.router.get('/queue/{filename}/config')(self.get_queue_config)
        self.router.post('/queue/{filename}/repair-config')(self.repair_queue_config)
        self.router.delete('/queue/{filename}')(self.remove_queue_item)
        self.router.post('/queue/delete')(self.delete_queue_items)
        self.router.post('/queue/clear-finished')(self.clear_finished)
        self.router.get('/queue/{filename}/log')(self.get_queue_log)
        self.router.get('/queue/{filename}/status')(self.get_queue_status)
        self.router.get('/results')(self.list_results)
        self.router.get('/results/config')(self.get_result_config)
        self.router.post('/results/resume')(self.queue_result_resume)
        self.router.delete('/results')(self.delete_result)
        self.router.post('/results/delete')(self.delete_results)
        self.router.post('/results/3d-plot')(self.launch_result_3d_plot)
        self.router.post('/results/pareto-dash')(self.launch_result_pareto_dash)
        self.router.api_route('/results/pareto-dash/{session_id}/', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'], name=self.profile.text('optimize_v8_pareto_dash_proxy_root'))(self.proxy_result_pareto_dash)
        self.router.api_route('/results/pareto-dash/{session_id}/{dash_path:path}', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'], name=self.profile.text('optimize_v8_pareto_dash_proxy_path'))(self.proxy_result_pareto_dash)
        self.router.delete('/results/pareto-dash/{session_id}')(self.stop_result_pareto_dash)
        self.router.get('/paretos')(self.list_paretos)
        self.router.get('/paretos/file')(self.get_pareto_file)
        self.router.post('/paretos/seed-bundle')(self.create_pareto_seed_bundle)
        if self.version == 'v9':
            self._QUEUE_SETTINGS_SECTION = 'optimize_v9'
            self.SERVICE = 'OptimizeV9'
            self._CONFIG_SECTIONS = ('config_version', *self._CONFIG_SECTIONS)

    def _data_dir(self) -> Path:
        return self.Path(self.PBGDIR) / 'data'

    def _configs_dir(self) -> Path:
        return self._data_dir() / self.profile.text('opt_v8')

    def _v7_configs_dir(self) -> Path:
        return self._data_dir() / 'opt_v7'

    def _resolve_v7_pareto_config(self, path: str) -> Path:
        """Resolve one PB7 Pareto JSON below the installed PB7 optimize result root."""
        root = (self.Path(self.pb7dir()) / 'optimize_results').resolve()
        source = self.Path(str(path or '')).resolve()
        try:
            source.relative_to(root)
        except ValueError as exc:
            raise self.HTTPException(status_code=400, detail='Invalid PB7 Pareto config path') from exc
        if source.is_symlink() or not source.is_file() or source.suffix.lower() != '.json' or (source.parent.name != 'pareto'):
            raise self.HTTPException(status_code=400, detail='Path is not a managed PB7 Pareto config')
        return source

    def _queue_dir(self) -> Path:
        return self._data_dir() / self.profile.text('opt_v8_queue')

    def _log_dir(self) -> Path:
        return self._data_dir() / 'logs' / self.profile.text('optimizes_v8')

    def _results_root(self) -> Path:
        runtime = self.pb8_runtime_status()
        pb8_dir = str(runtime.get(self.profile.text('pb8dir')) or '')
        return self.Path(pb8_dir) / 'optimize_results' if pb8_dir else self._data_dir() / self.profile.text('.pb8-unavailable')

    def _backtests_root(self) -> Path:
        runtime = self.pb8_runtime_status()
        pb8_dir = str(runtime.get(self.profile.text('pb8dir')) or '')
        return self.Path(pb8_dir) / 'backtests' / 'pbgui' if pb8_dir else self._data_dir() / self.profile.text('.pb8-unavailable')

    def _clean_migration_drafts(self, *, reserve_slot: bool=False) -> None:
        """Expire and bound unsaved V7-to-V8 Optimize previews under the draft lock."""
        now = self.time.time()
        for draft_id, (created_at, _payload) in list(self._migration_drafts.items()):
            if now - created_at > self._MIGRATION_DRAFT_TTL_SECONDS:
                self._migration_drafts.pop(draft_id, None)
        excess = len(self._migration_drafts) - self._MIGRATION_DRAFT_MAX_ENTRIES + (1 if reserve_slot else 0)
        if excess > 0:
            oldest = sorted(self._migration_drafts.items(), key=lambda item: item[1][0])[:excess]
            for draft_id, _entry in oldest:
                self._migration_drafts.pop(draft_id, None)

    def _validate_name(self, name: str) -> str:
        value = str(name or '').strip()
        if not value or value.startswith('.') or value in {'.', '..'} or any((char in value for char in ('/', '\\', '\x00'))) or any((ord(char) < 32 or ord(char) == 127 for char in value)) or (len(value.encode('utf-8')) > 128):
            raise self.HTTPException(status_code=400, detail='Invalid name')
        return value

    def _safe_path(self, path: Path, root: Path) -> Path:
        absolute_root = self.Path(self.os.path.abspath(root))
        absolute_path = self.Path(self.os.path.abspath(path))
        try:
            relative = absolute_path.relative_to(absolute_root)
        except ValueError as exc:
            raise RuntimeError(f'Managed path escaped root: {path}') from exc
        if absolute_root.is_symlink():
            raise RuntimeError(f'Managed root must not be a symlink: {absolute_root}')
        current = absolute_root
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                raise RuntimeError(f'Managed path contains a symlink: {current}')
        return absolute_path

    def _config_dir(self, name: str) -> Path:
        return self._safe_path(self._configs_dir() / self._validate_name(name), self._configs_dir())

    def _config_file(self, name: str) -> Path:
        return self._safe_path(self._config_dir(name) / self._CONFIG_FILENAME, self._configs_dir())

    def _queue_file(self, filename: str) -> Path:
        return self._safe_path(self._queue_dir() / f'{self._validate_name(filename)}.json', self._queue_dir())

    def _pid_file(self, filename: str) -> Path:
        return self._safe_path(self._queue_dir() / f'{self._validate_name(filename)}.pid', self._queue_dir())

    def _state_file(self, filename: str) -> Path:
        return self._safe_path(self._queue_dir() / 'state' / f'{self._validate_name(filename)}.json', self._queue_dir())

    def _ready_file(self, filename: str) -> Path:
        return self._safe_path(self._queue_dir() / 'state' / f'{self._validate_name(filename)}.launch-ready', self._queue_dir())

    def _reorder_file(self) -> Path:
        return self._safe_path(self._queue_dir() / 'state' / 'reorder.json', self._queue_dir())

    def _snapshot_dir(self, filename: str) -> Path:
        return self._safe_path(self._queue_dir() / 'snapshots' / self._validate_name(filename), self._queue_dir())

    def _snapshot_file(self, filename: str) -> Path:
        return self._safe_path(self._snapshot_dir(filename) / self._CONFIG_FILENAME, self._queue_dir())

    def _launch_dir(self, filename: str) -> Path:
        return self._safe_path(self._queue_dir() / 'launch' / self._validate_name(filename), self._queue_dir())

    def _launch_config_file(self, filename: str) -> Path:
        return self._safe_path(self._launch_dir(filename) / self._CONFIG_FILENAME, self._queue_dir())

    def _launch_options_file(self, filename: str) -> Path:
        return self._safe_path(self._launch_dir(filename) / 'options.json', self._queue_dir())

    def _write_json(self, path: Path, payload: dict) -> None:
        self.atomic_write_private_text(path, self.json.dumps(payload, indent=4, allow_nan=False) + '\n')

    def _read_json(self, path: Path) -> dict:
        try:
            if path.is_symlink() or not path.is_file():
                raise OSError('not a regular file')
            payload = self.json.loads(path.read_text(encoding='utf-8'))
        except (OSError, self.json.JSONDecodeError) as exc:
            raise RuntimeError(f'Failed to read {path.name}: {exc}') from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f'{path.name} must contain an object')
        return payload

    def _recover_config_transactions(self, root: Path) -> None:
        """Recover PBGui-owned bundle swaps left by an interrupted API process."""
        if not root.is_dir() or root.is_symlink():
            return
        backups: dict[str, list[Path]] = {}
        for entry in root.iterdir():
            if not entry.name.startswith('.'):
                continue
            body = entry.name[1:]
            target_name, separator, token = body.rpartition('.backup-')
            if separator and target_name and (len(token) == 32) and all((char in '0123456789abcdef' for char in token)):
                if entry.is_symlink() or not entry.is_dir():
                    raise RuntimeError(f'Unsafe PB8 optimize backup: {entry}')
                backups.setdefault(target_name, []).append(entry)
                continue
            stage_name, separator, token = body.rpartition('.stage-')
            if separator and stage_name and (len(token) == 32) and all((char in '0123456789abcdef' for char in token)):
                if entry.is_symlink() or not entry.is_dir():
                    raise RuntimeError(f'Unsafe PB8 optimize stage: {entry}')
                self.rmtree(entry, ignore_errors=True)
        for target_name, candidates in backups.items():
            target = self._safe_path(root / target_name, root)
            ordered = sorted(candidates, key=lambda path: path.stat().st_mtime_ns, reverse=True)
            if not target.exists():
                self.os.replace(ordered.pop(0), target)
                self._log(self.SERVICE, f'Recovered interrupted PB8 optimize config save for {target_name}', level='WARNING')
            for backup in ordered:
                self.rmtree(backup, ignore_errors=True)

    @contextmanager
    def _config_lock(self):
        self.ensure_private_directory(self._configs_dir())
        with self.advisory_file_lock(self._safe_path(self._configs_dir() / '.write', self._configs_dir())):
            self._recover_config_transactions(self._configs_dir())
            yield

    def _queue_lock(self):
        self.ensure_private_directory(self._queue_dir())
        return self.advisory_file_lock(self._safe_path(self._queue_dir() / '.write', self._queue_dir()))

    def _result_lock(self):
        lock_root = self.ensure_private_directory(self._data_dir() / 'locks')
        return self.advisory_file_lock(self._safe_path(lock_root / self.profile.text('optimize-v8-results'), lock_root))

    def _configuration_error(self, operation: str, exc: Exception, status_code: int=422) -> HTTPException:
        self._log(self.SERVICE, f'{operation} failed: {exc}', level='WARNING')
        return self.HTTPException(status_code=int(getattr(exc, 'status_code', status_code)), detail=str(exc))

    def _validate_optimizer_overrides(self, config: dict, *, base_config_path: str) -> None:
        """Convert PB8-native optimizer override failures into an actionable API response."""
        try:
            self.validate_pb8_optimizer_overrides(config, base_config_path=base_config_path)
        except self.PB8ConfigurationError as exc:
            raise self._configuration_error(f'Validating {self.label} optimizer overrides', exc) from exc

    def _effective_optimize_value(self,config,key,default=None):
        """Retain legacy defaults; native profiles resolve omitted values without saving them."""
        return (config.get('optimize') or {}).get(key,default)

    def _validate_optimize_backend(self, config: dict, *, base_config_path: str) -> dict:
        """Fail closed on unavailable or unsupported PB8 optimizer backends."""
        optimize = config.get('optimize') if isinstance(config.get('optimize'), dict) else {}
        backend = str(self._effective_optimize_value(config,'backend','pymoo') or 'pymoo').strip().lower()
        if backend != 'gpu':
            return {'contract_version': 1, 'backend': backend, 'valid': True, 'stage': 'not_required'}
        try:
            return self.validate_pb8_optimize_preflight(config, base_config_path=base_config_path)
        except self.PB8ConfigurationError as exc:
            raise self._configuration_error(f'Validating {self.label} optimize backend', exc) from exc

    def _normalize_config(self, config: dict, name: str, *, preserve_hsl_runtime_overrides: bool=False) -> dict:
        candidate = self.copy.deepcopy(config)
        backtest = candidate.setdefault('backtest', {})
        if not isinstance(backtest, dict):
            raise self.HTTPException(status_code=422, detail='backtest must be an object')
        backtest['base_dir'] = f'backtests/pbgui/{name}'
        optimize = candidate.get('optimize')
        overrides = optimize.get('fixed_runtime_overrides') if isinstance(optimize, dict) else None
        if isinstance(overrides, dict) and (not preserve_hsl_runtime_overrides):
            bot = candidate.setdefault('bot', {})
            if not isinstance(bot, dict):
                raise self.HTTPException(status_code=422, detail='bot must be an object')
            for side in ('long', 'short'):
                for field in ('enabled', 'no_restart_drawdown_threshold'):
                    override_key = f'bot.{side}.hsl.{field}'
                    if override_key not in overrides:
                        continue
                    side_config = bot.setdefault(side, {})
                    if not isinstance(side_config, dict):
                        raise self.HTTPException(status_code=422, detail=f'bot.{side} must be an object')
                    hsl = side_config.setdefault('hsl', {})
                    if not isinstance(hsl, dict):
                        raise self.HTTPException(status_code=422, detail=f'bot.{side}.hsl must be an object')
                    hsl[field] = overrides.pop(override_key)
        return candidate

    def _validate_forager_optimize_search_space(self, config: dict) -> None:
        """Reject candidate ranges that can produce PB8-invalid forager configs."""
        bot = config.get('bot') if isinstance(config.get('bot'), dict) else {}
        optimize = config.get('optimize') if isinstance(config.get('optimize'), dict) else {}
        bounds = optimize.get('bounds') if isinstance(optimize.get('bounds'), dict) else {}
        fixed_params = {str(value).removeprefix('bot.') for value in optimize.get('fixed_params', []) if isinstance(value, str) and value.strip()}
        errors = []
        for side in ('long', 'short'):
            side_config = bot.get(side) if isinstance(bot.get(side), dict) else {}
            side_bounds = bounds.get(side) if isinstance(bounds.get(side), dict) else {}
            risk = side_config.get('risk') if isinstance(side_config.get('risk'), dict) else {}
            risk_bounds = side_bounds.get('risk') if isinstance(side_bounds.get('risk'), dict) else {}
            forager = side_config.get('forager') if isinstance(side_config.get('forager'), dict) else {}
            forager_bounds = side_bounds.get('forager') if isinstance(side_bounds.get('forager'), dict) else {}
            score_weights = forager.get('score_weights') if isinstance(forager.get('score_weights'), dict) else {}

            def candidate_range(group: str, key: str, current: object, group_bounds: dict) -> tuple[float, float]:
                try:
                    current_number = float(current)
                except (TypeError, ValueError):
                    current_number = 0.0
                selector = f'{side}.{group}.{key}'
                if selector in fixed_params:
                    return (current_number, current_number)
                raw = group_bounds.get(key)
                if not isinstance(raw, (list, tuple)) or not raw:
                    return (current_number, current_number)
                try:
                    low = float(raw[0])
                    high = float(raw[1] if len(raw) > 1 else raw[0])
                except (TypeError, ValueError):
                    return (current_number, current_number)
                return (min(low, high), max(low, high))
            _, n_positions_max = candidate_range('risk', 'n_positions', risk.get('n_positions'), risk_bounds)
            _, exposure_max = candidate_range('risk', 'total_wallet_exposure_limit', risk.get('total_wallet_exposure_limit'), risk_bounds)
            if n_positions_max <= 0.0 or exposure_max <= 0.0:
                continue
            _, volume_weight_max = candidate_range('forager', 'score_weights_volume', score_weights.get('volume'), forager_bounds)
            _, volatility_weight_max = candidate_range('forager', 'score_weights_volatility', score_weights.get('volatility'), forager_bounds)
            _, volume_drop_max = candidate_range('forager', 'volume_drop_pct', forager.get('volume_drop_pct'), forager_bounds)
            volume_span_min, _ = candidate_range('forager', 'volume_ema_span_1m', forager.get('volume_ema_span_1m'), forager_bounds)
            volatility_span_min, _ = candidate_range('forager', 'volatility_ema_span_1m', forager.get('volatility_ema_span_1m'), forager_bounds)
            if (volume_weight_max > 0.0 or volume_drop_max > 0.0) and volume_span_min <= 0.0:
                errors.append(f'bot.{side}.forager.volume_ema_span_1m must stay > 0 because optimize bounds can enable volume ranking or pruning; use Fixed to exclude this parameter while retaining its positive bot value')
            if volatility_weight_max > 0.0 and volatility_span_min <= 0.0:
                errors.append(f'bot.{side}.forager.volatility_ema_span_1m must stay > 0 because optimize bounds can enable volatility ranking; use Fixed to exclude this parameter while retaining its positive bot value')
        if errors:
            raise self.HTTPException(status_code=422, detail='Invalid PB8 optimize search space: ' + '; '.join(errors))

    def _publish_bundle(self, stage: Path, target: Path) -> None:
        backup = target.parent / f'.{target.name}.backup-{self.uuid.uuid4().hex}'
        existed = target.exists()
        if existed:
            if target.is_symlink() or not target.is_dir():
                raise self.HTTPException(status_code=409, detail='Config target is not a safe directory')
            self.os.replace(target, backup)
        try:
            self.os.replace(stage, target)
        except Exception:
            if existed and backup.exists() and (not target.exists()):
                self.os.replace(backup, target)
            raise
        self.rmtree(backup, ignore_errors=True)

    def _override_filenames(self, config: dict) -> set[str]:
        """Return safe sparse-override filenames referenced by an Optimize config."""
        overrides = config.get('coin_overrides')
        if overrides is None:
            return set()
        if not isinstance(overrides, dict):
            raise self.HTTPException(status_code=422, detail='coin_overrides must be an object')
        filenames = set()
        for item in overrides.values():
            if not isinstance(item, dict) or not item.get('override_config_path'):
                continue
            filename = str(item['override_config_path']).strip()
            if not filename.endswith('.json') or filename.startswith('.') or '/' in filename or ('\\' in filename) or (self.Path(filename).name != filename) or (filename in {self._CONFIG_FILENAME, 'migration_report.json'}) or any((ord(char) < 32 or ord(char) == 127 for char in filename)):
                raise self.HTTPException(status_code=422, detail=f'Invalid override config path: {filename}')
            filenames.add(filename)
        return filenames

    def _load_override_payloads(self, config: dict, directory: Path) -> dict[str, dict]:
        """Load the complete referenced sparse-override set from a managed bundle."""
        result = {}
        for filename in self._override_filenames(config):
            path = self._safe_path(directory / filename, directory)
            if not path.is_file() or path.is_symlink():
                raise self.HTTPException(status_code=422, detail=f'Override config not found: {filename}')
            payload = self._read_json(path)
            if not isinstance(payload, dict):
                raise self.HTTPException(status_code=422, detail=f'Override config {filename} must be an object')
            result[filename] = payload
        return result

    def _resolve_override_payloads(self, config: dict, supplied: object, source_dir: Path | None) -> dict[str, dict]:
        """Resolve submitted and inherited overrides into one complete exact bundle."""
        if supplied is None:
            supplied = {}
        if not isinstance(supplied, dict):
            raise self.HTTPException(status_code=422, detail='override_configs must be an object')
        referenced = self._override_filenames(config)
        unknown = set((str(name) for name in supplied)) - referenced
        if unknown:
            raise self.HTTPException(status_code=422, detail=f'Unreferenced override config: {sorted(unknown)[0]}')
        result = {}
        total_bytes = 0
        for filename in referenced:
            payload = supplied.get(filename)
            if payload is None and source_dir is not None:
                path = self._safe_path(source_dir / filename, source_dir)
                if path.is_file() and (not path.is_symlink()):
                    payload = self._read_json(path)
            if not isinstance(payload, dict):
                raise self.HTTPException(status_code=422, detail=f'Override config not supplied: {filename}')
            try:
                encoded = self.json.dumps(payload, allow_nan=False).encode('utf-8')
            except (TypeError, ValueError) as exc:
                raise self.HTTPException(status_code=422, detail=f'Override config {filename} is not valid JSON') from exc
            if len(encoded) > self._MAX_OVERRIDE_BYTES:
                raise self.HTTPException(status_code=413, detail=f'Override config {filename} exceeds 1 MiB')
            total_bytes += len(encoded)
            if total_bytes > self._MAX_OVERRIDE_BUNDLE_BYTES:
                raise self.HTTPException(status_code=413, detail='Override config bundle exceeds 8 MiB')
            result[filename] = self.copy.deepcopy(payload)
        return result

    def _write_override_payloads(self, directory: Path, payloads: dict[str, dict]) -> None:
        """Write one validated sparse-override set into a private bundle directory."""
        for filename, payload in payloads.items():
            self._write_json(self._safe_path(directory / filename, directory), payload)

    def _stage_snapshot_bundle(self, filename: str, prepared: dict, override_payloads: dict[str, dict]) -> Path:
        """Build and validate an immutable queue bundle outside its final path."""
        root = self.ensure_private_directory(self._queue_dir() / 'snapshots')
        stage = self._safe_path(root / f'.{self._validate_name(filename)}.stage-{self.uuid.uuid4().hex}', root)
        self.ensure_private_directory(stage)
        try:
            self._write_override_payloads(stage, override_payloads)
            self._write_json(stage / self._CONFIG_FILENAME, prepared)
            self.validate_pb8_override_bundle(stage / self._CONFIG_FILENAME)
            return stage
        except Exception:
            self.rmtree(stage, ignore_errors=True)
            raise

    def _publish_snapshot_bundle(self, stage: Path, target: Path, *, retain_backup: bool=False) -> Path | None:
        """Atomically replace one queue snapshot and optionally retain rollback state."""
        backup = target.parent / f'.{target.name}.backup-{self.uuid.uuid4().hex}'
        existed = target.is_dir() and (not target.is_symlink())
        if target.exists() and (not existed):
            raise self.HTTPException(status_code=409, detail='Queue snapshot target is unsafe')
        if existed:
            self.os.replace(target, backup)
        try:
            self.os.replace(stage, target)
        except Exception:
            if existed and backup.exists() and (not target.exists()):
                self.os.replace(backup, target)
            raise
        if retain_backup and existed:
            return backup
        self.rmtree(backup, ignore_errors=True)
        return None

    def _save_config_bundle(self, name: str, config: dict, *, create_only: bool=False, override_payloads: object=None, source_dir: Path | None=None, migration_report: dict | None=None, preserve_hsl_runtime_overrides: bool=False) -> dict:
        root = self.ensure_private_directory(self._configs_dir())
        target = self._config_dir(name)
        if create_only and target.exists():
            raise self.HTTPException(status_code=409, detail=f"Config '{name}' already exists")
        stage = self._safe_path(root / f'.{name}.stage-{self.uuid.uuid4().hex}', root)
        self.ensure_private_directory(stage)
        try:
            if source_dir is None and target.is_dir() and (not target.is_symlink()):
                source_dir = target
            normalized = self._normalize_config(config, name, preserve_hsl_runtime_overrides=preserve_hsl_runtime_overrides)
            self._validate_sweep_cycles_config(normalized)
            resolved_overrides = self._resolve_override_payloads(normalized, override_payloads, source_dir)
            self._write_override_payloads(stage, resolved_overrides)
            prepared = self.prepare_pb8_config(normalized, base_config_path=str(stage / self._CONFIG_FILENAME))
            self._validate_optimizer_overrides(prepared, base_config_path=str(stage / self._CONFIG_FILENAME))
            if self._override_filenames(prepared) != set(resolved_overrides):
                raise self.HTTPException(status_code=422, detail='PB8 preparation changed override-file references')
            self._validate_forager_optimize_search_space(prepared)
            self._write_json(stage / self._CONFIG_FILENAME, prepared)
            if migration_report is not None:
                self._write_json(stage / 'migration_report.json', migration_report)
            self.validate_pb8_override_bundle(stage / self._CONFIG_FILENAME)
            current_report = target / 'migration_report.json'
            if migration_report is None and current_report.is_file() and (not current_report.is_symlink()):
                self.copy2(current_report, stage / current_report.name)
            self._publish_bundle(stage, target)
            self.cache_prepared_pb8_config(prepared, target / self._CONFIG_FILENAME)
            return prepared
        finally:
            self.rmtree(stage, ignore_errors=True)

    def _validate_sweep_cycles_config(self, config: dict) -> dict | None:
        """Require a complete attributable result contract for Sweep Cycles."""
        pbgui = config.get('pbgui') if isinstance(config.get('pbgui'), dict) else {}
        template = pbgui.get('scenario_template') if isinstance(pbgui.get('scenario_template'), dict) else {}
        if template.get('template') != 'sweep_cycles':
            return None
        plan = self.build_sweep_plan(config)
        if plan is None:
            raise self.HTTPException(status_code=422, detail='Sweep Cycles requires active non-overlapping Suite scenarios, inherited exchanges, and a valid policy')
        optimize = config.get('optimize') if isinstance(config.get('optimize'), dict) else {}
        if self._effective_optimize_value(config,'write_all_results') is False:
            raise self.HTTPException(status_code=422, detail='Sweep Cycles requires optimize.write_all_results=true so PBGui can bind its immutable plan to the result')
        backtest = config.get('backtest') if isinstance(config.get('backtest'), dict) else {}
        try:
            backtest_balance = float(backtest.get('starting_balance'))
        except (TypeError, ValueError):
            backtest_balance = -1.0
        sweep_balance = float(plan['policy']['starting_balance'])
        if abs(backtest_balance - sweep_balance) > max(1e-09, abs(sweep_balance) * 1e-12):
            raise self.HTTPException(status_code=422, detail='Sweep Cycles requires backtest.starting_balance to equal pbgui.scenario_template.parameters.sweep_policy.starting_balance')
        live = config.get('live') if isinstance(config.get('live'), dict) else {}
        approved = live.get('approved_coins')
        if approved == 'all':
            approved_long = approved_short = ['all']
        elif isinstance(approved, dict):
            approved_long = approved.get('long')
            approved_short = approved.get('short')
            approved_long = [approved_long] if isinstance(approved_long, str) else approved_long
            approved_short = [approved_short] if isinstance(approved_short, str) else approved_short
        else:
            approved_long = approved_short = approved
        normalized_long = sorted({str(value).strip() for value in approved_long or [] if str(value).strip()}) if isinstance(approved_long, list) else []
        normalized_short = sorted({str(value).strip() for value in approved_short or [] if str(value).strip()}) if isinstance(approved_short, list) else []
        if normalized_long != normalized_short:
            raise self.HTTPException(status_code=422, detail='Sweep Cycles requires identical live.approved_coins.long and live.approved_coins.short lists for PB8 Suite mode')
        return plan

    def _read_process_record(self, filename: str) -> dict | None:
        try:
            record = self._read_json(self._pid_file(filename))
            pid = int(record.get('pid') or 0)
            create_time = float(record.get('create_time') or 0.0)
            if pid <= 0 or create_time <= 0:
                return None
            owned_results = record.get('owned_results')
            return {'pid': pid, 'create_time': create_time, 'owned_results': list(owned_results) if isinstance(owned_results, list) else []}
        except (RuntimeError, ValueError, TypeError):
            return None

    def _systemd_user_manager_available(self) -> bool:
        """Return whether a transient user unit can isolate a persistent optimizer."""
        if self.platform.system() != 'Linux' or self.which('systemd-run') is None:
            return False
        runtime_dir = self.Path(self.os.environ.get('XDG_RUNTIME_DIR') or f'/run/user/{self.os.getuid()}')
        return (runtime_dir / 'systemd' / 'private').exists()

    def _launch_optimizer_runner(self, filename: str, command: list[str], cwd: Path, log_path: Path) -> subprocess.Popen | None:
        """Launch a runner outside the API cgroup when user systemd is available."""
        environment = self._optimizer_environment(command[0])
        launch_command = self._optimizer_command(command, environment)
        if self._systemd_user_manager_available():
            self.atomic_write_private_text(log_path, '')
            unit = f'{self.profile.text('pbgui-pb8-optimize-')}{filename}-{self.time.time_ns()}'
            runtime_dir = self.os.environ.get('XDG_RUNTIME_DIR') or f'/run/user/{self.os.getuid()}'
            result = self.subprocess.run([str(self.which('systemd-run')), '--user', '--quiet', '--collect', f'--unit={unit}', '--property=Type=exec', '--property=UMask=0077', f'--property=WorkingDirectory={cwd}', f'--property=StandardOutput=append:{log_path}', f'--property=StandardError=append:{log_path}', f'--setenv=PATH={environment["PATH"]}', '--setenv=PASSIVBOT_GPU_PROFILE=1', '--', *launch_command], check=False, capture_output=True, text=True, timeout=15, env={**environment, 'XDG_RUNTIME_DIR': runtime_dir})
            if result.returncode != 0:
                detail = ((result.stderr or '') + (result.stdout or '')).strip()
                raise RuntimeError(f'Could not start persistent PB8 optimizer unit: {detail or result.returncode}')
            return None
        descriptor = self.os.open(log_path, self.os.O_WRONLY | self.os.O_CREAT | self.os.O_TRUNC
                                  | getattr(self.os, 'O_NOFOLLOW', 0), 0o600)
        if hasattr(self.os, 'fchmod'):
            self.os.fchmod(descriptor, 0o600)
        log_file = self.os.fdopen(descriptor, 'w', encoding='utf-8')
        try:
            kwargs = {'cwd': str(cwd), 'stdout': log_file, 'stderr': log_file, 'env': environment, 'close_fds': True}
            if self.platform.system() == 'Windows':
                kwargs['creationflags'] = self.subprocess.DETACHED_PROCESS | self.subprocess.CREATE_NO_WINDOW
            else:
                kwargs['start_new_session'] = True
            return self.subprocess.Popen(command, **kwargs)
        finally:
            log_file.close()

    def _process_matches(self, filename: str, record: dict) -> bool:
        try:
            process = self.psutil.Process(int(record['pid']))
            if abs(process.create_time() - float(record['create_time'])) > 0.01:
                return False
            command = [str(part) for part in process.cmdline()]
            flags = self._runner_python_flags()
            if flags:
                if command[1:1 + len(flags)] != flags:
                    return False
                command = [command[0], *command[1 + len(flags):]]
            return len(command) == 10 and command[1] == str(self.Path(self.PBGDIR) / self.profile.text('pb8_optimize_runner.py')) and (command[2] == 'optimize') and (command[3] == str(self._state_file(filename))) and (command[4] == str(self._pid_file(filename))) and (command[5] == str(self._ready_file(filename))) and (command[8] == str(self._launch_config_file(filename).resolve())) and (command[9] == str(self._launch_options_file(filename).resolve()))
        except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess, self.psutil.AccessDenied, OSError, ValueError, TypeError):
            return False

    def _read_runner_state(self, filename: str) -> dict | None:
        try:
            forced = self.read_convergence_state(self._state_file(filename).with_suffix('.convergence.forced.json'))
            if forced:
                return forced
            state = self._read_json(self._state_file(filename))
            return state if isinstance(state.get('returncode'), int) else None
        except (RuntimeError, OSError, ValueError, TypeError):
            return None

    def _read_log_terminal_status(self, filename: str) -> str | None:
        path = self._safe_path(self._log_dir() / f'{filename}.log', self._log_dir())
        try:
            info = path.lstat()
        except OSError:
            return None
        if not self.stat.S_ISREG(info.st_mode):
            return None
        signature = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
        with self._log_terminal_status_lock:
            cached = self._log_terminal_status_cache.get(str(path))
        if cached is not None and cached[0] == signature:
            return cached[1]
        try:
            with path.open('rb') as handle:
                handle.seek(0, 2)
                handle.seek(max(0, handle.tell() - 64 * 1024))
                tail = handle.read().decode('utf-8', errors='ignore').lower()
        except OSError:
            return None
        if 'optimization complete' in tail or 'successfully processed optimize_results' in tail:
            status = 'complete'
        elif tail.strip():
            status = 'error'
        else:
            status = None
        with self._log_terminal_status_lock:
            if len(self._log_terminal_status_cache) >= self._LOG_TERMINAL_STATUS_CACHE_MAX:
                self._log_terminal_status_cache.clear()
            self._log_terminal_status_cache[str(path)] = (signature, status)
        return status

    def _clear_stale_process_record(self, filename: str, record: dict | None=None) -> None:
        path = self._pid_file(filename)
        if path.exists():
            path.unlink(missing_ok=True)
            pid = int((record or {}).get('pid') or 0)
            self._log(self.SERVICE, f'Cleared unverified PB8 optimize ownership for {filename} (PID {pid or 'unknown'})', level='WARNING')

    def _queue_status(self, data: dict) -> tuple[str, int | None]:
        filename = self._validate_name(str(data.get('filename') or ''))
        record = self._read_process_record(filename)
        if record and self._process_matches(filename, record):
            return ('running', int(record['pid']))
        if record:
            self._clear_stale_process_record(filename, record)
            record = None
        elif self._pid_file(filename).exists():
            self._clear_stale_process_record(filename)
        override = str(data.get('status_override') or '')
        if override == 'error':
            return (override, int(record['pid']) if record else None)
        state = self._read_runner_state(filename)
        if state is not None:
            return ('complete' if self.runner_complete(state) else 'error', int(record['pid']) if record else None)
        log_status = self._read_log_terminal_status(filename)
        if data.get('started_at') and log_status:
            return (log_status, None)
        return ('error' if data.get('started_at') else 'queued', int(record['pid']) if record else None)

    def _terminate_process(self, pid: int) -> None:
        try:
            process = self.psutil.Process(pid)
        except self.psutil.NoSuchProcess:
            return
        if self.platform.system() == 'Windows':
            members = [*process.children(recursive=True), process]
            for member in members:
                member.terminate()
            _gone, alive = self.psutil.wait_procs(members, timeout=5)
            for member in alive:
                member.kill()
            return
        process_group = self.os.getpgid(pid)
        if process_group == self.os.getpgrp():
            raise RuntimeError('Refusing to terminate the API process group')
        try:
            self.os.killpg(process_group, self.signal.SIGTERM)
            _gone, alive = self.psutil.wait_procs([process], timeout=5)
            if alive:
                self.os.killpg(process_group, self.signal.SIGKILL)
        except (ProcessLookupError, self.psutil.NoSuchProcess):
            pass

    def _terminate_verified(self, filename: str) -> None:
        record = self._read_process_record(filename)
        if record is None:
            if self._pid_file(filename).exists():
                self._clear_stale_process_record(filename)
            return
        pid = int(record['pid'])
        if not self.psutil.pid_exists(pid):
            self._pid_file(filename).unlink(missing_ok=True)
            return
        if not self._process_matches(filename, record):
            self._clear_stale_process_record(filename, record)
            return
        self._terminate_process(pid)
        self._pid_file(filename).unlink(missing_ok=True)

    def _launch_error_detail(self, exc: Exception) -> str:
        detail = exc.detail if isinstance(exc, self.HTTPException) else str(exc)
        if isinstance(detail, dict):
            detail = detail.get('message') or self.json.dumps(detail, sort_keys=True)
        return str(detail or exc.__class__.__name__).strip()

    def _launch_error_is_transient(self, exc: Exception) -> bool:
        if isinstance(exc, self.HTTPException) and exc.status_code in {409, 503}:
            return True
        detail = self._launch_error_detail(exc).lower()
        return any((marker in detail for marker in ('runtime lock', 'update is incomplete', 'runtime is not ready', 'launch handshake timed out', 'queue order changed')))

    def _append_queue_log(self, filename: str, message: str) -> None:
        self.ensure_private_directory(self._log_dir())
        path = self._safe_path(self._log_dir() / f'{filename}.log', self._log_dir())
        self.append_managed_transcript_line(path, f'{self.datetime.datetime.now(self.datetime.UTC).isoformat()} PBGui: {message}', self.profile.text('optimizes_v8'))

    def _record_launch_failure(self, filename: str, exc: Exception) -> bool:
        """Persist a failed launch and return whether it is safe to retry automatically."""
        filename = self._validate_name(filename)
        transient = self._launch_error_is_transient(exc)
        detail = self._launch_error_detail(exc)
        with self._queue_lock():
            path = self._queue_file(filename)
            if not path.is_file():
                return transient
            data = self._read_json(path)
            state = self._read_runner_state(filename)
            if state is not None and self.runner_complete(state):
                return transient
            if data.get('status_override') == 'error' and isinstance(exc, self.HTTPException) and (exc.status_code == 409):
                return transient
            record = self._read_process_record(filename)
            if record and self._process_matches(filename, record):
                return transient
            self._pid_file(filename).unlink(missing_ok=True)
            self._ready_file(filename).unlink(missing_ok=True)
            if transient:
                self._state_file(filename).unlink(missing_ok=True)
                data.pop('started_at', None)
                data.pop('status_override', None)
                data.pop('automatic', None)
                data.pop('error_code', None)
                data.pop('error_reason', None)
                data['launch_message'] = f'Launch deferred; PBGui will retry: {detail}'
            else:
                data['status_override'] = 'error'
                data['error_code'] = 'prelaunch_failed'
                data['error_reason'] = detail
                data['launch_message'] = f'Launch failed: {detail}. Repair or requeue this item.'
            self._write_json(path, data)
        self._append_queue_log(filename, data['launch_message'])
        return transient

    def _resolve_result_path(self, path: str, *, require_directory: bool=True, for_delete: bool=False) -> Path:
        """Resolve absolute or root-relative result paths within managed storage."""
        root = self._results_root()
        raw = str(path or '')
        if not raw or any((ord(char) < 32 for char in raw)) or '\\' in raw or ('..' in self.Path(raw).parts):
            raise self.HTTPException(status_code=400, detail='Invalid result path')
        candidate = self.Path(raw)
        if not candidate.is_absolute():
            candidate = root / candidate
        try:
            target = self._safe_path(candidate, root)
        except RuntimeError as exc:
            raise self.HTTPException(status_code=400, detail='Invalid result path') from exc
        if for_delete and (target.parent != self.Path(self.os.path.abspath(root)) or target.name.startswith('.')):
            raise self.HTTPException(status_code=400, detail='Deletion requires a top-level optimize result run directory')
        if require_directory:
            if not target.is_dir() or target.is_symlink() or target == root:
                raise self.HTTPException(status_code=404, detail='Result not found')
        elif not target.exists() or target.is_symlink():
            raise self.HTTPException(status_code=404, detail='Result artifact not found')
        return target

    def _supported_seed_file(self, path: Path) -> bool:
        name = path.name.lower()
        return path.is_file() and (not path.is_symlink()) and (name.endswith('.json') or name.endswith('_pareto.txt'))

    def _validate_pareto_seed_source(self, source: str) -> str:
        source_path = self._resolve_result_path(source, require_directory=False)
        relative = source_path.relative_to(self._results_root())
        in_pareto = 'pareto' in relative.parts
        in_bundle = '_seed_bundles' in relative.parts
        if source_path.is_file():
            if not (in_pareto or in_bundle) or not self._supported_seed_file(source_path):
                raise self.HTTPException(status_code=422, detail='Pareto seed must be a managed .json or *_pareto.txt seed file')
            return str(source_path)
        if not source_path.is_dir() or source_path.is_symlink() or (not (source_path.name == 'pareto' or in_bundle)):
            raise self.HTTPException(status_code=422, detail='Pareto seed directory is not a managed PB8 seed directory')
        try:
            seeds = [path for path in source_path.iterdir() if self._supported_seed_file(path)]
        except OSError as exc:
            raise self.HTTPException(status_code=422, detail=f'Pareto seed directory is unreadable: {exc}') from exc
        if not seeds:
            raise self.HTTPException(status_code=422, detail='Pareto seed directory contains no supported .json or *_pareto.txt seed files')
        return str(source_path)

    def _extract_result_config(self, data: dict | None) -> dict | None:
        if not isinstance(data, dict):
            return None
        config = {key: self.copy.deepcopy(data[key]) for key in self._CONFIG_SECTIONS if isinstance(data.get(key), dict)}
        if data.get('config_version') is not None:
            config['config_version'] = self.copy.deepcopy(data['config_version'])
        return config if all((isinstance(config.get(key), dict) for key in ('backtest', 'optimize'))) else None

    def _recover_result_config(self, result_dir: Path) -> dict | None:
        for name in ('config.json', 'optimize.json'):
            path = result_dir / name
            if path.is_file() and (not path.is_symlink()):
                try:
                    config = self._extract_result_config(self._read_json(path))
                    if config is not None:
                        return config
                except RuntimeError:
                    pass
        pareto = self._first_pareto_file(result_dir)
        if pareto is not None:
            try:
                config = self._extract_result_config(self._read_json(pareto))
                if config is not None:
                    return config
            except RuntimeError:
                pass
        config = self._extract_result_config(self._all_results_first(result_dir / 'all_results.bin'))
        if config is not None:
            return config
        return None

    def _result_override_payloads(self, result_dir: Path, config: dict) -> dict[str, dict]:
        """Load sparse overrides colocated with a managed result config."""
        if not self._override_filenames(config):
            return {}
        return self._load_override_payloads(config, result_dir)

    def _restore_result_window_plan(self, result_dir: Path, config: dict) -> dict:
        """Reattach result-owned holdouts only after matching the native training suite."""
        path = result_dir / self.VALIDATION_PLAN_FILENAME
        if not path.exists() and (not path.is_symlink()):
            return config
        try:
            if path.stat().st_size > 256 * 1024:
                raise ValueError('Validation windows exceed the size limit')
            template = self.json.loads(self.read_regular_file_nofollow(path, result_dir))
            restored = self.copy.deepcopy(config)
            backtest = restored.get('backtest') or {}
            if not backtest.get('suite_enabled'):
                raise ValueError('Saved validation windows require an enabled training suite')
            for scenario in [backtest] + list(backtest.get('scenarios') or []):
                for key in ('start_date', 'end_date'):
                    value = scenario.get(key)
                    if isinstance(value, str) and self.re.fullmatch('\\d{4}-\\d{2}-\\d{2}T00:00:00(?:\\.0+)?', value):
                        scenario[key] = value[:10]
            pbgui = restored.setdefault('pbgui', {})
            pbgui['scenario_template'] = template
            plan = self.build_validation_plan(restored)
            if plan is None:
                raise ValueError('Unsupported validation window contract')
            pbgui['scenario_template'] = plan
            parameters = plan['parameters']
            pbgui['scenario_generator'] = {**self.copy.deepcopy(parameters), **self.copy.deepcopy(parameters.get('sweep_policy') or {}), 'template': plan['template']}
            return restored
        except (OSError, RuntimeError, ValueError, TypeError, AttributeError, KeyError) as exc:
            self._log(self.SERVICE, 'Cannot restore result-owned scenario windows', level='WARNING', meta={'error_type': type(exc).__name__})
            raise self.HTTPException(status_code=422, detail='Cannot restore saved Training/Holdout windows: validation metadata is invalid or does not match this run.') from exc

    def _checkpoint_resume_readiness(self, result_dir: Path, *, for_listing: bool=False, progress_hint: dict | None=None, config_hint: dict | None=None, results_root: Path | None=None) -> dict:
        root = results_root if results_root is not None else self._results_root()
        checkpoint = self._safe_path(result_dir / 'checkpoint.pkl', root)
        all_results = self._safe_path(result_dir / 'all_results.bin', root)
        reasons = []
        try:
            if checkpoint.is_symlink() or not checkpoint.is_file() or checkpoint.stat().st_size <= 0:
                reasons.append('checkpoint.pkl is missing or empty')
            else:
                with checkpoint.open('rb') as handle:
                    if not handle.read(1):
                        reasons.append('checkpoint.pkl is unreadable or empty')
        except OSError as exc:
            reasons.append(f'checkpoint.pkl is unreadable: {exc}')
        progress = progress_hint if for_listing and isinstance(progress_hint, dict) else self._all_results_progress_for_listing(all_results) if for_listing else self._all_results_progress(all_results)
        if not all_results.is_file() or all_results.is_symlink() or progress.get('bytes', 0) <= 0:
            reasons.append('all_results.bin is missing or empty')
        elif progress.get('error'):
            reasons.append(f'all_results.bin is malformed: {progress['error']}')
        elif not progress.get('scan_deferred') and (progress.get('trailing_partial_entry') or progress.get('evaluations', 0) <= 0):
            reasons.append('all_results.bin does not strictly decode to complete result records')
        config = self._extract_result_config(config_hint)
        if config is None:
            config = self._recover_result_config(result_dir)
        if config is None:
            reasons.append('base result config could not be recovered from native result artifacts')
        elif self._effective_optimize_value(config,'write_all_results') is not True:
            reasons.append('base result config does not enable optimize.write_all_results')
        return {'ready': not reasons, 'reasons': reasons, 'config': config, 'evaluations': progress.get('evaluations', 0)}

    def _resume_compatibility_fields(self, config: dict) -> dict:
        optimize = config.get('optimize') if isinstance(config.get('optimize'), dict) else {}
        backtest = config.get('backtest') if isinstance(config.get('backtest'), dict) else {}
        live = config.get('live') if isinstance(config.get('live'), dict) else {}
        return {'config_version': config.get('config_version'), 'optimize.backend': optimize.get('backend'), 'optimize.gpu': optimize.get('gpu'), 'optimize.pymoo': optimize.get('pymoo'), 'optimize.population_size': optimize.get('population_size'), 'optimize.offspring_multiplier': optimize.get('offspring_multiplier'), 'optimize.crossover_probability': optimize.get('crossover_probability'), 'optimize.mutation_probability': optimize.get('mutation_probability'), 'optimize.mutation_indpb': optimize.get('mutation_indpb'), 'optimize.crossover_eta': optimize.get('crossover_eta'), 'optimize.mutation_eta': optimize.get('mutation_eta'), 'optimize.compress_results_file': optimize.get('compress_results_file'), 'optimize.round_to_n_significant_digits': optimize.get('round_to_n_significant_digits'), 'optimize.objective_scenario': optimize.get('objective_scenario'), 'optimize.scoring': optimize.get('scoring'), 'optimize.limits': optimize.get('limits'), 'optimize.bounds': optimize.get('bounds'), 'optimize.fixed_params': optimize.get('fixed_params'), 'optimize.fixed_runtime_overrides': optimize.get('fixed_runtime_overrides'), 'optimize.enable_overrides': optimize.get('enable_overrides'), 'live.strategy_kind': live.get('strategy_kind'), 'backtest.exchanges': backtest.get('exchanges'), 'backtest.coins': backtest.get('coins'), 'backtest.start_date': backtest.get('start_date'), 'backtest.end_date': backtest.get('end_date'), 'backtest.reducer': backtest.get('reducer'), 'backtest.balance_sample_divider': backtest.get('balance_sample_divider'), 'backtest.btc_collateral_cap': backtest.get('btc_collateral_cap'), 'backtest.btc_collateral_ltv_cap': backtest.get('btc_collateral_ltv_cap'), 'backtest.candle_interval_minutes': backtest.get('candle_interval_minutes'), 'backtest.dynamic_wel_by_tradability': backtest.get('dynamic_wel_by_tradability'), 'backtest.filter_by_min_effective_cost': backtest.get('filter_by_min_effective_cost'), 'backtest.liquidation_threshold': backtest.get('liquidation_threshold'), 'backtest.maker_fee_override': backtest.get('maker_fee_override'), 'backtest.market_order_slippage_pct': backtest.get('market_order_slippage_pct'), 'backtest.starting_balance': backtest.get('starting_balance'), 'backtest.taker_fee_override': backtest.get('taker_fee_override'), 'backtest.volume_normalization': backtest.get('volume_normalization'), 'backtest.suite_enabled': backtest.get('suite_enabled'), 'backtest.scenarios': backtest.get('scenarios'), 'backtest.aggregate': backtest.get('aggregate'), 'backtest.suite': backtest.get('suite'), 'bot.long.enabled': ((config.get('bot') or {}).get('long') or {}).get('enabled', True), 'bot.short.enabled': ((config.get('bot') or {}).get('short') or {}).get('enabled', True), 'live.approved_coins': live.get('approved_coins'), 'live.ignored_coins': live.get('ignored_coins')}

    def _convergence_identity(self, config: dict) -> str:
        """Use the checkpoint evaluation contract, excluding names, budgets and resources."""
        return self.hashlib.sha256(self.json.dumps(self._resume_compatibility_fields(config), sort_keys=True, allow_nan=False).encode()).hexdigest()

    def _legacy_convergence_identity(self, config: dict) -> str:
        """Allow legacy history only when its old complete-config hash can be reproduced."""
        return self.hashlib.sha256(self.json.dumps({key: value for key, value in config.items() if key not in ('pbgui', 'logging')}, sort_keys=True, allow_nan=False).encode()).hexdigest()

    def _preflight_checkpoint_resume(self, snapshot: dict, result_dir: Path) -> None:
        readiness = self._checkpoint_resume_readiness(result_dir)
        if not readiness['ready']:
            raise self.HTTPException(status_code=422, detail='Checkpoint cannot be resumed: ' + '; '.join(readiness['reasons']))
        try:
            queued = self.prepare_pb8_config(snapshot)
            recovered = self.prepare_pb8_config(readiness['config'])
        except self.PB8ConfigurationError as exc:
            raise self.HTTPException(status_code=422, detail=f'PB8 resume preflight rejected the config: {exc}') from exc
        queued_fields = self._resume_compatibility_fields(queued)
        recovered_fields = self._resume_compatibility_fields(recovered)
        mismatches = [key for key in queued_fields if queued_fields[key] != recovered_fields[key]]
        if mismatches:
            raise self.HTTPException(status_code=422, detail='Queued config is incompatible with the checkpoint: ' + ', '.join(mismatches[:8]))

    def _validate_launch_options(self, raw: dict | None) -> dict:
        payload = self.copy.deepcopy(raw or {})
        try:
            convergence_config = self.validate_convergence_config(payload.get('convergence_config', payload))
        except ValueError as exc:
            raise self.HTTPException(status_code=422, detail=str(exc)) from exc
        mode = str(payload.get('mode') or 'fresh').strip().lower()
        if mode not in self._LAUNCH_MODES:
            raise self.HTTPException(status_code=422, detail='Invalid optimize launch mode')
        source = str(payload.get('source') or '').strip()
        if mode == 'checkpoint_resume':
            result_dir = self._resolve_result_path(source)
            readiness = self._checkpoint_resume_readiness(result_dir)
            if not readiness['ready']:
                raise self.HTTPException(status_code=422, detail='Checkpoint cannot be resumed: ' + '; '.join(readiness['reasons']))
            source = str(result_dir)
        elif mode == 'pareto_seed':
            if source == '__self__':
                source = '__self__'
            else:
                source = self._validate_pareto_seed_source(source)
        else:
            source = ''
        fine_tune = payload.get('fine_tune_params', [])
        if isinstance(fine_tune, str):
            fine_tune = [item.strip() for item in fine_tune.split(',') if item.strip()]
        if not isinstance(fine_tune, list) or not all((isinstance(item, str) and item.strip() for item in fine_tune)):
            raise self.HTTPException(status_code=422, detail='fine_tune_params must be a list of paths')
        polish = payload.get('polish_percentage')
        if polish is not None:
            if isinstance(polish, bool) or not isinstance(polish, (int, float)) or polish < 0:
                raise self.HTTPException(status_code=422, detail='polish_percentage must be a non-negative number')
            polish = float(polish)
        bounds_mode = str(payload.get('polish_bounds_mode') or 'clamp')
        if bounds_mode not in {'clamp', 'override-tunable', 'override-all'}:
            raise self.HTTPException(status_code=422, detail='Invalid polish_bounds_mode')
        if polish is None and bounds_mode != 'clamp':
            raise self.HTTPException(status_code=422, detail='polish_bounds_mode requires polish_percentage')
        if polish is not None and polish > 1.0 and (bounds_mode == 'clamp'):
            raise self.HTTPException(status_code=422, detail='polish_percentage above 1.0 (100%) requires an intentional override bounds mode')
        return {'mode': mode, 'source': source, 'fine_tune_params': [item.strip() for item in fine_tune], 'polish_percentage': polish, 'polish_bounds_mode': bounds_mode, 'convergence_config': convergence_config}

    def _runtime_options_from_config(self, config: dict) -> dict:
        pbgui = config.get('pbgui') if isinstance(config.get('pbgui'), dict) else {}
        options = self.copy.deepcopy(pbgui.get('optimize_runtime')) if isinstance(pbgui.get('optimize_runtime'), dict) else {}
        seed_mode = str(pbgui.get('optimize_seed_mode') or '').strip().lower()
        if str(options.get('mode') or 'fresh').strip().lower() == 'fresh':
            if seed_mode == 'path' and str(pbgui.get('optimize_seed_path') or '').strip():
                options.update(mode='pareto_seed', source=str(pbgui['optimize_seed_path']).strip())
            elif seed_mode == 'self':
                options.update(mode='pareto_seed', source='__self__')
        return self._validate_launch_options(options)

    def _validate_options_for_config(self, options: dict, config: dict) -> None:
        """Generation hook for CLI options that depend on the frozen config."""

    def _result_launch_options(self, result_dir, config):
        """Retain the existing PB8 checkpoint launch options."""
        return {}

    def _config_seed_metadata(self, config: dict) -> tuple[str, str]:
        pbgui = config.get('pbgui') if isinstance(config.get('pbgui'), dict) else {}
        runtime = pbgui.get('optimize_runtime') if isinstance(pbgui.get('optimize_runtime'), dict) else {}
        mode = str(runtime.get('mode') or '').strip().lower()
        source = str(runtime.get('source') or '').strip()
        if mode == 'pareto_seed':
            return ('self', '__self__') if source == '__self__' else ('path', source)
        if mode == 'checkpoint_resume':
            return ('none', '')
        legacy_mode = str(pbgui.get('optimize_seed_mode') or '').strip().lower()
        legacy_path = str(pbgui.get('optimize_seed_path') or '').strip()
        if legacy_mode == 'self' or pbgui.get('starting_config') is True:
            return ('self', '__self__')
        if legacy_mode == 'path' and legacy_path:
            return ('path', legacy_path)
        return ('none', '')

    def _file_identity(self, path: Path) -> tuple:
        """Detect replacement and in-place edits, including preserved mtimes."""
        stat = path.stat()
        return (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size)

    def _config_summary(self, name: str, path: Path) -> tuple[dict, dict, tuple]:
        """Cache lightweight metadata only while the validated file identity matches."""
        identity = self._file_identity(path)
        key = str(path)
        with self._config_summary_cache_lock:
            cached = self._config_summary_cache.get(key)
            if cached and cached['identity'] == identity:
                self._config_summary_cache.move_to_end(key)
                return (self.copy.deepcopy(cached['summary']), self.copy.deepcopy(cached['count_config']), identity)
        config = self._read_json(path)
        backtest = config.get('backtest') if isinstance(config.get('backtest'), dict) else {}
        seed_mode, seed_source = self._config_seed_metadata(config)
        live = config.get('live') if isinstance(config.get('live'), dict) else {}
        summary = {'name': name, 'exchange': backtest.get('exchanges') or [], 'start_date': backtest.get('start_date') or '', 'end_date': backtest.get('end_date') or '', 'strategy': self._strategy_kind(config), 'seed_mode': seed_mode, 'seed_source': seed_source, 'modified': self.datetime.datetime.fromtimestamp(path.stat().st_mtime).isoformat()}
        count_config = {'backtest': {'base_dir': backtest.get('base_dir')}}
        if self._file_identity(path) == identity:
            with self._config_summary_cache_lock:
                self._config_summary_cache[key] = {'identity': identity, 'summary': self.copy.deepcopy(summary), 'count_config': count_config}
                self._config_summary_cache.move_to_end(key)
                while len(self._config_summary_cache) > self._CONFIG_SUMMARY_CACHE_MAX_ENTRIES:
                    self._config_summary_cache.popitem(last=False)
        return (summary, self.copy.deepcopy(count_config), identity)

    def _close_backtest_scan(self, scan: dict) -> None:
        """Release a scan's directory iterator while holding its lock."""
        iterator = scan.get('iterator')
        scan['iterator'] = None
        try:
            if iterator is not None:
                iterator.close()
        finally:
            descriptor = scan.pop('directory_fd', None)
            if descriptor is not None:
                self.os.close(descriptor)

    def _open_backtest_scan_directory(self, root: Path, directory: Path) -> int:
        """Open each managed directory component without following symbolic links."""
        if not self._BACKTEST_COUNT_SAFE_SCANDIR_AVAILABLE:
            raise OSError('Safe directory-descriptor scans are unavailable on this platform')
        relative = directory.relative_to(root)
        flags = self.os.O_RDONLY | self.os.O_DIRECTORY | self.os.O_NOFOLLOW
        descriptor = self.os.open(root, flags)
        try:
            for part in relative.parts:
                child = self.os.open(part, flags, dir_fd=descriptor)
                self.os.close(descriptor)
                descriptor = child
            return descriptor
        except BaseException:
            self.os.close(descriptor)
            raise

    def _descriptor_identity(self, descriptor: int) -> tuple:
        """Read a directory identity from the opened descriptor."""
        info = self.os.fstat(descriptor)
        return (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns, info.st_size)

    def _expire_backtest_scan(self, key: str, scan: dict) -> None:
        """Expire an idle scan without blocking the API event loop on filesystem work."""
        with self._backtest_count_scans_lock:
            if self._backtest_count_scans.get(key) is not scan:
                return
            remaining = min(scan['touched'] + self._BACKTEST_COUNT_SCAN_IDLE_SECONDS, scan['created'] + self._BACKTEST_COUNT_SCAN_MAX_AGE_SECONDS) - self.time.monotonic()
            if remaining > 0:
                scan['timer'] = self._backtest_count_scan_loop.call_later(remaining, self._expire_backtest_scan, key, scan)
                return
            self._backtest_count_scans.pop(key, None)
            scan['cancelled'] = True
            if self.time.monotonic() >= scan['created'] + self._BACKTEST_COUNT_SCAN_MAX_AGE_SECONDS:
                self._backtest_count_expired[key] = scan['identity']
                self._backtest_count_expired.move_to_end(key)
                while len(self._backtest_count_expired) > self._BACKTEST_COUNT_SCAN_MAX_ACTIVE:
                    self._backtest_count_expired.popitem(last=False)
        if scan['lock'].acquire(blocking=False):
            try:
                try:
                    self._close_backtest_scan(scan)
                except OSError as exc:
                    self._log(self.SERVICE, f'Unable to close expired backtest scan: {exc}', level='WARNING')
            finally:
                scan['lock'].release()

    def _schedule_backtest_scan_expiry(self, key: str, scan: dict) -> None:
        """Schedule cleanup on the API-owned event loop."""
        with self._backtest_count_scans_lock:
            if self._backtest_count_scans.get(key) is scan and self._backtest_count_scan_loop is not None:
                scan['timer'] = self._backtest_count_scan_loop.call_later(self._BACKTEST_COUNT_SCAN_IDLE_SECONDS, self._expire_backtest_scan, key, scan)

    def _discard_backtest_scan(self, key: str, scan: dict) -> None:
        """Remove a scan and close its iterator; caller holds its lock."""
        with self._backtest_count_scans_lock:
            if self._backtest_count_scans.get(key) is scan:
                self._backtest_count_scans.pop(key, None)
            scan['cancelled'] = True
            timer = scan.pop('timer', None)
            if timer is not None:
                timer.cancel()
        self._close_backtest_scan(scan)

    def _shutdown_backtest_scans(self) -> None:
        """Close all API-owned scan iterators, including in-flight scans."""
        with self._backtest_count_scans_condition:
            self._backtest_count_scans_accepting = False
            self._backtest_count_scans_condition.notify_all()
            self._backtest_count_scan_loop = None
            scans = list(self._backtest_count_scans.items())
            self._backtest_count_scans.clear()
            self._backtest_count_expired.clear()
            for _key, scan in scans:
                scan['cancelled'] = True
                timer = scan.pop('timer', None)
                if timer is not None:
                    timer.cancel()
        for key, scan in scans:
            try:
                with scan['lock']:
                    self._discard_backtest_scan(key, scan)
            except Exception as exc:
                self._log(self.SERVICE, f'Unable to close backtest scan during shutdown: {exc}', level='WARNING')
        with self._backtest_count_scans_condition:
            while self._backtest_count_active_requests:
                self._backtest_count_scans_condition.wait()

    def _managed_backtest_count_status(self, name: str, config: dict, root=_COUNT_ROOT_UNSET, *, config_identity: tuple=(), complete: bool=False) -> tuple[str, int | None]:
        """Track request ownership until all filesystem work has finished."""
        with self._backtest_count_scans_condition:
            if not self._backtest_count_scans_accepting:
                return ('unknown', None)
            self._backtest_count_active_requests += 1
        try:
            complete_until = self.time.monotonic() + self._BACKTEST_COUNT_SCAN_MAX_AGE_SECONDS if complete else None
            while True:
                status, count = self._managed_backtest_count_status_inner(name, config, root, config_identity=config_identity, complete_until=complete_until)
                if not complete or status != 'pending':
                    return (status, count)
                with self._backtest_count_scans_lock:
                    if not self._backtest_count_scans_accepting or self.time.monotonic() >= complete_until:
                        return ('unknown', None)
        finally:
            with self._backtest_count_scans_condition:
                self._backtest_count_active_requests -= 1
                self._backtest_count_scans_condition.notify_all()

    def _managed_backtest_count_status_inner(self, name: str, config: dict, root=_COUNT_ROOT_UNSET, *, config_identity: tuple=(), complete_until: float | None=None) -> tuple[str, int | None]:
        """Continue a bounded scan, publishing only a fully verified count."""
        name = self._validate_name(name)
        backtest = config.get('backtest') if isinstance(config.get('backtest'), dict) else {}
        base_dir = str(backtest.get('base_dir') or '').strip()
        if self.PurePath(base_dir).parts != ('backtests', 'pbgui', name):
            return ('unknown', None)
        if root is self._COUNT_ROOT_UNSET:
            root = self._backtests_root()
        if root is None:
            return ('unknown', None)
        root = self.Path(self.os.path.abspath(root))
        try:
            target = self._safe_path(root / name, root)
            if not root.is_dir() or not target.is_dir():
                return ('unknown', None)
            identity = (self._file_identity(root), self._file_identity(target), config_identity, base_dir)
            key = str(target)
            now = self.time.monotonic()
            with self._backtest_count_cache_lock:
                cached = self._backtest_count_cache.get(key)
                if cached and cached['identity'] == identity and (now - cached['loaded_at'] < self._BACKTEST_COUNT_CACHE_TTL_SECONDS):
                    self._backtest_count_cache.move_to_end(key)
                    return ('known', cached['count'])
            with self._backtest_count_scans_lock:
                if not self._backtest_count_scans_accepting:
                    return ('unknown', None)
                expired_identity = self._backtest_count_expired.pop(key, None)
                if expired_identity == identity:
                    return ('unknown', None)
                scan = self._backtest_count_scans.get(key)
                if scan is not None and scan['identity'] != identity:
                    self._backtest_count_scans.pop(key, None)
                    scan['cancelled'] = True
                    timer = scan.pop('timer', None)
                    if timer is not None:
                        timer.cancel()
                    if scan['lock'].acquire(blocking=False):
                        try:
                            self._close_backtest_scan(scan)
                        finally:
                            scan['lock'].release()
                    scan = None
                if scan is None:
                    if len(self._backtest_count_scans) >= self._BACKTEST_COUNT_SCAN_MAX_ACTIVE:
                        return ('unknown', None)
                    scan = {'identity': identity, 'lock': self.threading.Lock(), 'cancelled': False, 'touched': now, 'created': now, 'stack': [target], 'directories': [], 'count': 0, 'visited': 0, 'bytes': len(self.os.fsencode(target)), 'iterator': None, 'verify_index': None, 'verify_started': None, 'timer': None}
                    self._backtest_count_scans[key] = scan
                    loop = self._backtest_count_scan_loop
                    if loop is not None:
                        loop.call_soon_threadsafe(self._schedule_backtest_scan_expiry, key, scan)
                else:
                    scan['touched'] = now
                    self._backtest_count_scans.move_to_end(key)
            acquired = scan['lock'].acquire(blocking=False) if complete_until is None else scan['lock'].acquire(timeout=max(0.0, complete_until - self.time.monotonic()))
            if not acquired:
                return ('pending' if complete_until is None else 'unknown', None)
            try:
                if scan['cancelled']:
                    return ('unknown' if complete_until is None else 'pending', None)
                if now - scan['created'] >= self._BACKTEST_COUNT_SCAN_MAX_AGE_SECONDS:
                    self._discard_backtest_scan(key, scan)
                    return ('unknown', None)
                if config_identity and self._file_identity(self._config_file(name)) != config_identity:
                    self._discard_backtest_scan(key, scan)
                    return ('unknown', None)
                deadline = now + self._BACKTEST_COUNT_SCAN_SECONDS
                while not scan['cancelled']:
                    if complete_until is not None and self.time.monotonic() >= min(complete_until, scan['created'] + self._BACKTEST_COUNT_SCAN_MAX_AGE_SECONDS):
                        self._discard_backtest_scan(key, scan)
                        return ('unknown', None)
                    if scan['verify_index'] is not None:
                        if self.time.monotonic() - scan['verify_started'] >= self._BACKTEST_COUNT_CACHE_TTL_SECONDS:
                            self._discard_backtest_scan(key, scan)
                            return ('unknown', None)
                        index = scan['verify_index']
                        if index >= len(scan['directories']):
                            if identity != (self._file_identity(root), self._file_identity(target), config_identity, base_dir) or (config_identity and self._file_identity(self._config_file(name)) != config_identity):
                                self._discard_backtest_scan(key, scan)
                                return ('unknown', None)
                            if self.time.monotonic() - scan['verify_started'] >= self._BACKTEST_COUNT_CACHE_TTL_SECONDS:
                                self._discard_backtest_scan(key, scan)
                                return ('unknown', None)
                            count = scan['count']
                            self._discard_backtest_scan(key, scan)
                            with self._backtest_count_cache_lock:
                                self._backtest_count_cache[key] = {'identity': identity, 'loaded_at': scan['verify_started'], 'count': count}
                                self._backtest_count_cache.move_to_end(key)
                                while len(self._backtest_count_cache) > self._BACKTEST_COUNT_CACHE_MAX_ENTRIES:
                                    self._backtest_count_cache.popitem(last=False)
                            return ('known', count)
                        directory, expected = scan['directories'][index]
                        descriptor = self._open_backtest_scan_directory(root, self._safe_path(directory, root))
                        try:
                            actual = self._descriptor_identity(descriptor)
                        finally:
                            self.os.close(descriptor)
                        if actual != expected:
                            self._discard_backtest_scan(key, scan)
                            return ('unknown', None)
                        scan['verify_index'] += 1
                    elif scan['iterator'] is None:
                        if not scan['stack']:
                            scan['verify_started'] = self.time.monotonic()
                            scan['verify_index'] = 0
                            continue
                        directory = self._safe_path(scan['stack'].pop(), root)
                        descriptor = self._open_backtest_scan_directory(root, directory)
                        scan['directory_fd'] = descriptor
                        scan['directories'].append((directory, self._descriptor_identity(descriptor)))
                        scan['directory'] = directory
                        scan['iterator'] = self.os.scandir(descriptor)
                    else:
                        try:
                            entry = next(scan['iterator'])
                        except StopIteration:
                            self._close_backtest_scan(scan)
                            continue
                        scan['visited'] += 1
                        if scan['visited'] > self._BACKTEST_COUNT_SCAN_MAX_ENTRIES:
                            self._discard_backtest_scan(key, scan)
                            return ('unknown', None)
                        if not entry.is_symlink():
                            if entry.is_dir(follow_symlinks=False):
                                path = scan['directory'] / entry.name
                                scan['bytes'] += len(self.os.fsencode(path))
                                if scan['bytes'] > self._BACKTEST_COUNT_SCAN_MAX_PATH_BYTES:
                                    self._discard_backtest_scan(key, scan)
                                    return ('unknown', None)
                                scan['stack'].append(path)
                            elif entry.name == 'analysis.json' and entry.is_file(follow_symlinks=False):
                                scan['count'] += 1
                    if self.time.monotonic() >= deadline:
                        if complete_until is not None:
                            with self._backtest_count_scans_lock:
                                scan['touched'] = self.time.monotonic()
                            deadline = scan['touched'] + self._BACKTEST_COUNT_SCAN_SECONDS
                            continue
                        if self._backtest_count_scan_loop is None:
                            self._discard_backtest_scan(key, scan)
                            return ('unknown', None)
                        return ('pending', None)
                self._discard_backtest_scan(key, scan)
                return ('unknown', None)
            except (OSError, RuntimeError):
                self._discard_backtest_scan(key, scan)
                raise
            finally:
                try:
                    if scan['cancelled']:
                        self._close_backtest_scan(scan)
                finally:
                    scan['lock'].release()
        except (OSError, RuntimeError) as exc:
            self._log(self.SERVICE, f'Unable to count managed backtests for {name}: {exc}', level='WARNING')
            if 'scan' in locals() and scan['lock'].acquire(blocking=False):
                try:
                    self._discard_backtest_scan(key, scan)
                finally:
                    scan['lock'].release()
            return ('unknown', None)

    def _managed_backtest_count(self, name: str, config: dict, root=_COUNT_ROOT_UNSET, *, config_identity: tuple=()) -> int | None:
        """Keep synchronous callers waiting for a complete count, within safety limits."""
        return self._managed_backtest_count_status(name, config, root, config_identity=config_identity, complete=True)[1]

    def _normalize_autostart_cpu(self, value) -> int:
        """Clamp an Optimize autostart CPU setting to the current host."""
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = 1
        return max(1, min(parsed, self.multiprocessing.cpu_count()))

    def _apply_queue_launch_settings(self, config: dict, settings: dict, *, automatic: bool, pbgui_data_path: str | None=None) -> dict:
        """Apply shared queue settings to a launch copy without mutating its snapshot."""
        prepared = self.copy.deepcopy(config)
        use_pbgui_data = str(settings.get('use_pbgui_market_data', 'False')).lower() == 'true'
        if use_pbgui_data:
            if not pbgui_data_path:
                raise RuntimeError('PBGui market data path is unavailable')
            prepared.setdefault('backtest', {})['ohlcv_source_dir'] = str(pbgui_data_path)
        cpu_override = str(settings.get('cpu_override', 'True')).lower() == 'true'
        if automatic and cpu_override:
            prepared.setdefault('optimize', {})['n_cpus'] = self._normalize_autostart_cpu(settings.get('cpu', '1'))
        return prepared

    def _read_queue_record(self, path: Path) -> tuple[dict, str]:
        """Return one persisted queue entry with its validated filename and creation stamp."""
        data = self._read_json(self._safe_path(path, self._queue_dir()))
        filename = str(data.get('filename') or path.stem)
        if filename != path.stem:
            raise RuntimeError('Persisted queue filename does not match its file')
        created = self.datetime.datetime.fromtimestamp(path.stat().st_mtime).isoformat()
        return ({**data, 'filename': filename}, created)

    def _queue_records(self) -> list[tuple[Path, dict, str]]:
        """Read each queue file once and return (path, data, created) in queue order."""
        self.ensure_private_directory(self._queue_dir())
        records = []
        for path in self._queue_dir().glob('*.json'):
            try:
                records.append((path, *self._read_queue_record(path)))
            except Exception as exc:
                self._log(self.SERVICE, f'Failed to load PB8 optimize queue item {path.name}: {exc}', level='WARNING')
        pending_order = self._pending_reorder_filenames()
        pending_rank = {filename: index for index, filename in enumerate(pending_order)}
        return sorted(records, key=lambda record: (pending_rank.get(record[1]['filename'], 10 ** 18) if pending_rank else int(record[1]['order']) if isinstance(record[1].get('order'), int) else 10 ** 18, record[2], record[1]['filename']))

    def _queue_record_status(self, path: Path, data: dict) -> tuple[str, int | None] | None:
        try:
            return self._queue_status(data)
        except Exception as exc:
            self._log(self.SERVICE, f'Failed to load PB8 optimize queue item {path.name}: {exc}', level='WARNING')
            return None

    def _queue_item(self, path: Path) -> dict:
        data, created = self._read_queue_record(path)
        return self._queue_record_item(data, created, *self._queue_status(data))

    def _queue_record_item(self, data: dict, created: str, status: str, pid: int | None) -> dict:
        filename = data['filename']
        options = data.get('launch_options') if isinstance(data.get('launch_options'), dict) else {}
        return {'filename': filename, 'loop_id': data.get('loop_id'), 'estimated_coin_candles': self.estimate_snapshot(self._snapshot_file(filename), self._queue_dir()), 'name': str(data.get('name') or filename), 'exchange': data.get('exchange') or [], 'status': status, 'completion_reason': 'convergence' if status == 'complete' and self.convergence_complete(self._read_runner_state(filename)) else None, 'pid': pid, 'created': created, 'started_at': data.get('started_at'), 'order': data.get('order'), 'launch_mode': options.get('mode') or 'fresh', 'launch_message': data.get('launch_message') or '', 'error_code': data.get('error_code') or '', 'error_reason': data.get('error_reason') or '', self.profile.text('pb8_version'): data.get(self.profile.text('pb8_version')) or '', self.profile.text('pb8_commit'): data.get(self.profile.text('pb8_commit')) or '', 'automatic': bool(data.get('automatic', False))}

    def _load_queue(self) -> list[dict]:
        items = []
        for path, data, created in self._queue_records():
            result = self._queue_record_status(path, data)
            if result is None:
                continue
            try:
                items.append(self._queue_record_item(data, created, *result))
            except Exception as exc:
                self._log(self.SERVICE, f'Failed to load PB8 optimize queue item {path.name}: {exc}', level='WARNING')
        return items

    def _first_manual_queued_filename(self) -> str | None:
        """Return the queue head eligible for autostart, evaluating only jobs ahead of it."""
        for path, data, _created in self._queue_records():
            if data.get('loop_id'):
                continue
            result = self._queue_record_status(path, data)
            if result is not None and result[0] == 'queued':
                return data['filename']
        return None

    def _autostart_candidate(self) -> str | None:
        """Return the next autostart job unless an automatic job is already running."""
        candidate = None
        for path, data, _created in self._queue_records():
            result = self._queue_record_status(path, data)
            if result is None:
                continue
            if result[0] == 'running' and data.get('automatic'):
                return None
            if candidate is None and result[0] == 'queued' and (not data.get('loop_id')):
                candidate = data['filename']
        return candidate

    def _pending_reorder_filenames(self) -> list[str]:
        try:
            payload = self._read_json(self._reorder_file())
            filenames = payload.get('filenames')
            if isinstance(filenames, list):
                normalized = [self._validate_name(str(item)) for item in filenames]
                if len(normalized) == len(set(normalized)):
                    return normalized
        except (RuntimeError, self.HTTPException):
            pass
        return []

    def _recover_pending_reorder_unlocked(self) -> None:
        path = self._reorder_file()
        if not path.exists():
            return
        filenames = self._pending_reorder_filenames()
        existing = {queue_path.stem for queue_path in self._queue_dir().glob('*.json')}
        if filenames and set(filenames) == existing:
            for index, filename in enumerate(filenames):
                data = self._read_json(self._queue_file(filename))
                data['order'] = index
                self._write_json(self._queue_file(filename), data)
            self._log(self.SERVICE, 'Recovered interrupted PB8 optimize queue reorder', level='WARNING')
        else:
            self._log(self.SERVICE, 'Discarded invalid interrupted PB8 optimize queue reorder', level='WARNING')
        path.unlink(missing_ok=True)

    def _reconcile_queue_artifacts(self) -> None:
        """Repair queue metadata and remove only non-live, queue-less runtime artifacts."""
        with self._queue_lock():
            self.ensure_private_directory_tree(self._queue_dir(), self._queue_dir() / 'state')
            self.ensure_private_directory_tree(self._queue_dir(), self._queue_dir() / 'snapshots')
            self.ensure_private_directory_tree(self._queue_dir(), self._queue_dir() / 'launch')
            self.ensure_private_directory_tree(self._queue_dir(), self._queue_dir() / 'repair')
            for journal in sorted((self._queue_dir() / 'repair').glob('*.json')):
                try:
                    filename = self._validate_name(journal.stem)
                    repair = self._read_json(journal)
                    backup_name = str(repair.get('backup') or '')
                    expected_prefix = f'.{filename}.repair-backup-'
                    if self.Path(backup_name).name != backup_name or not backup_name.startswith(expected_prefix):
                        raise RuntimeError('Invalid queue repair backup name')
                    backup = self._safe_path(self._queue_dir() / 'snapshots' / backup_name, self._queue_dir())
                    target = self._snapshot_dir(filename)
                    if target.exists():
                        self.rmtree(target, ignore_errors=True)
                    if backup.is_dir() and (not backup.is_symlink()):
                        self.os.replace(backup, target)
                    journal.unlink(missing_ok=True)
                except Exception as exc:
                    self._log(self.SERVICE, f'Could not recover PB8 Optimize queue repair {journal.name}: {exc}', level='ERROR')
            self._recover_pending_reorder_unlocked()
            queue_ids: set[str] = set()
            next_order = 0
            for path in sorted(self._queue_dir().glob('*.json')):
                try:
                    filename = self._validate_name(path.stem)
                    data = self._read_json(path)
                except (RuntimeError, self.HTTPException) as exc:
                    self._log(self.SERVICE, f'Could not reconcile PB8 optimize queue record {path.name}: {exc}', level='ERROR')
                    continue
                queue_ids.add(filename)
                changed = False
                if data.get('filename') != filename:
                    data['filename'] = filename
                    changed = True
                if not str(data.get('name') or '').strip():
                    data['name'] = filename
                    changed = True
                if not isinstance(data.get('order'), int):
                    data['order'] = next_order
                    changed = True
                next_order = max(next_order, int(data['order']) + 1)
                expected_snapshot = str(self._snapshot_file(filename))
                if data.get('snapshot_path') != expected_snapshot:
                    data['snapshot_path'] = expected_snapshot
                    changed = True
                record = self._read_process_record(filename)
                live = bool(record and self._process_matches(filename, record))
                if record and (not live):
                    self._clear_stale_process_record(filename, record)
                if not live:
                    self._ready_file(filename).unlink(missing_ok=True)
                state = self._read_runner_state(filename)
                if not live and (not self._snapshot_file(filename).is_file() or self._snapshot_file(filename).is_symlink()):
                    reason = 'Queue config snapshot is missing; select a managed config and use Repair Config.'
                    if data.get('error_code') != 'snapshot_missing' or data.get('error_reason') != reason:
                        data.update(status_override='error', error_code='snapshot_missing', error_reason=reason, launch_message=reason)
                        self._append_queue_log(filename, reason)
                        changed = True
                elif not live and data.get('started_at') and (state is None) and (self._read_log_terminal_status(filename) != 'complete') and (data.get('status_override') != 'error'):
                    reason = 'Optimizer ownership was stale and no durable runner state was found; requeue to retry.'
                    data.update(status_override='error', error_code='stale_ownership', error_reason=reason, launch_message=reason)
                    self._append_queue_log(filename, reason)
                    changed = True
                elif state is not None and (not self.runner_complete(state)) and (not data.get('error_reason')):
                    reason = str(state.get('error') or f'Optimizer exited with code {state['returncode']}; inspect the retained log.')
                    data.update(error_code='runner_failed', error_reason=reason)
                    changed = True
                if changed:
                    self._write_json(path, data)
            artifact_ids: set[str] = set()
            for path in (self._queue_dir() / 'snapshots').iterdir():
                if path.is_dir() and (not path.is_symlink()):
                    artifact_ids.add(path.name)
            for path in (self._queue_dir() / 'launch').iterdir():
                if path.is_dir() and (not path.is_symlink()):
                    artifact_ids.add(path.name)
            for path in self._queue_dir().glob('*.pid'):
                artifact_ids.add(path.name.removesuffix('.pid'))
            for path in (self._queue_dir() / 'state').glob('*.json'):
                if path != self._reorder_file():
                    suffix = next((suffix for suffix in ('.convergence.forced.json', '.convergence.json') if path.name.endswith(suffix)), '.json')
                    artifact_ids.add(path.name.removesuffix(suffix))
            for path in (self._queue_dir() / 'state').glob('*.launch-ready'):
                artifact_ids.add(path.name.removesuffix('.launch-ready'))
            for raw_filename in sorted(artifact_ids - queue_ids):
                try:
                    filename = self._validate_name(raw_filename)
                except self.HTTPException:
                    continue
                record = self._read_process_record(filename)
                if record and self._process_matches(filename, record):
                    self._log(self.SERVICE, f'Preserved live orphan PB8 optimizer artifacts for {filename}', level='WARNING')
                    continue
                for artifact in (self._pid_file(filename), self._state_file(filename), self._ready_file(filename), self._state_file(filename).with_suffix('.convergence.json'), self._state_file(filename).with_suffix('.convergence.forced.json')):
                    artifact.unlink(missing_ok=True)
                self.rmtree(self._snapshot_dir(filename), ignore_errors=True)
                self.rmtree(self._launch_dir(filename), ignore_errors=True)
                self._log(self.SERVICE, f'Removed stale orphan PB8 optimize artifacts for {filename}', level='WARNING')

    def _runtime_commit(self, pb8_dir: Path) -> str:
        try:
            result = self.subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=pb8_dir, capture_output=True, text=True, timeout=5, check=False)
            return result.stdout.strip() if result.returncode == 0 else ''
        except (OSError, self.subprocess.TimeoutExpired):
            return ''

    def _result_dirs(self, root: Path | None=None):
        root = root if root is not None else self._results_root()
        if not root.is_dir() or root.is_symlink():
            return
        for directory in sorted(root.iterdir()):
            if directory.name.startswith('.') or not directory.is_dir() or directory.is_symlink():
                continue
            if (directory / 'all_results.bin').is_file() or (directory / 'checkpoint.pkl').is_file() or (directory / 'pareto').is_dir():
                yield directory

    def _first_pareto_file(self, result_dir: Path) -> Path | None:
        pareto_dir = result_dir / 'pareto'
        if not pareto_dir.is_dir() or pareto_dir.is_symlink():
            return None
        return next((path for path in sorted(pareto_dir.glob('*.json')) if path.is_file() and (not path.is_symlink())), None)

    def _pareto_files_for_listing(self, result_dir: Path) -> tuple[Path, list[Path]]:
        """Enumerate regular Pareto JSON files once without per-file pathlib stat calls."""
        pareto_dir = result_dir / 'pareto'
        if not pareto_dir.is_dir() or pareto_dir.is_symlink():
            return (pareto_dir, [])
        try:
            with self.os.scandir(pareto_dir) as entries:
                names = sorted((entry.name for entry in entries if entry.name.endswith('.json') and entry.is_file(follow_symlinks=False)))
        except OSError:
            return (pareto_dir, [])
        return (pareto_dir, [pareto_dir / name for name in names])

    def _apply_result_diff(self, base: dict, diff: dict) -> dict:
        result = {}
        for key in base.keys() | diff.keys():
            if key not in diff:
                result[key] = base[key]
                continue
            value = diff[key]
            if isinstance(value, dict) and value == {'__passivbot_diff_delete__': True}:
                continue
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                result[key] = self._apply_result_diff(base[key], value)
            else:
                result[key] = value
        return result

    def _all_results_progress(self, path: Path) -> dict:
        """Decode only appended PB8 MessagePack bytes while retaining diff state."""
        state = self._scan_all_results(path)
        payload = {'evaluations': state['evaluations'], 'bytes': state['size'], 'trailing_partial_entry': state['offset'] < state['size'] and (not state.get('error')), 'latest': self._pareto_summary(state['current'], 'mean')}
        if state.get('error'):
            payload['error'] = state['error']
        return payload

    def _all_results_evaluation_count(self, path: Path) -> dict:
        """Count appended PB8 MessagePack records without reconstructing result configs."""
        key = str(path.resolve(strict=False))
        now = self.time.monotonic()
        with self._active_eval_count_cache_lock:
            for stale_key, cached in list(self._active_eval_count_cache.items()):
                if now - float(cached.get('accessed_at') or 0) > self._RESULT_PROGRESS_CACHE_TTL_SECONDS:
                    self._active_eval_count_cache.pop(stale_key, None)
            try:
                stat_result = path.stat()
                if path.is_symlink() or not path.is_file():
                    raise OSError('not a regular file')
            except OSError:
                self._active_eval_count_cache.pop(key, None)
                return {'evaluations': 0, 'trailing_partial_entry': False, 'error': 'result file unavailable'}
            cached = self._active_eval_count_cache.get(key)
            same_file = bool(cached and cached.get('device') == stat_result.st_dev and (cached.get('inode') == stat_result.st_ino) and (stat_result.st_size >= int(cached.get('size') or 0)) and (not (stat_result.st_size == int(cached.get('size') or 0) and stat_result.st_mtime_ns != cached.get('mtime_ns'))))
            if same_file and stat_result.st_size == cached['size'] and (int(cached['offset']) >= int(cached['size'])):
                cached['accessed_at'] = now
                self._active_eval_count_cache.move_to_end(key)
                return {'evaluations': int(cached['evaluations']), 'trailing_partial_entry': False, 'scan_complete': True, 'bytes_scanned': int(cached['offset']), 'total_bytes': int(cached['size']), 'scan_percent': 100.0}
            offset = int(cached['offset']) if same_file else 0
            evaluations = int(cached['evaluations']) if same_file else 0
            last_complete = offset
            error = None
            scan_reaches_eof = False
            scan_deadline = self.time.monotonic() + self._ACTIVE_EVAL_SCAN_BUDGET_SECONDS
            scan_budget_exhausted = False
            try:
                with path.open('rb') as handle:
                    handle.seek(offset)
                    unpacker = self.msgpack.Unpacker(raw=False, strict_map_key=False, max_buffer_size=64 * 1024 * 1024)
                    remaining = min(stat_result.st_size - offset, self._ACTIVE_EVAL_SCAN_BUDGET_BYTES)
                    scan_reaches_eof = offset + remaining >= stat_result.st_size
                    while remaining > 0:
                        chunk = handle.read(min(1024 * 1024, remaining))
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        unpacker.feed(chunk)
                        for entry in unpacker:
                            if not isinstance(entry, dict):
                                raise ValueError('all_results.bin contains a non-object record')
                            evaluations += 1
                            last_complete = offset + unpacker.tell()
                            if self.time.monotonic() >= scan_deadline:
                                scan_budget_exhausted = True
                                break
                        if scan_budget_exhausted:
                            break
            except (OSError, ValueError, self.msgpack.UnpackException) as exc:
                error = str(exc) or exc.__class__.__name__
            state = {'device': stat_result.st_dev, 'inode': stat_result.st_ino, 'size': stat_result.st_size, 'mtime_ns': stat_result.st_mtime_ns, 'offset': last_complete, 'evaluations': evaluations, 'accessed_at': now}
            self._active_eval_count_cache[key] = state
            self._active_eval_count_cache.move_to_end(key)
            while len(self._active_eval_count_cache) > self._RESULT_PROGRESS_CACHE_MAX_ENTRIES:
                self._active_eval_count_cache.popitem(last=False)
            result = {'evaluations': evaluations, 'trailing_partial_entry': bool(scan_reaches_eof and (not scan_budget_exhausted) and (last_complete < stat_result.st_size) and (not error)), 'scan_complete': last_complete >= stat_result.st_size, 'bytes_scanned': last_complete, 'total_bytes': stat_result.st_size, 'scan_percent': round(last_complete / stat_result.st_size * 100.0, 1) if stat_result.st_size else 100.0}
            if error:
                result['error'] = error
            return result

    def _runner_evaluation_progress(self, path: Path) -> dict | None:
        """Read the exact count published by PBGui's detached runner for new runs."""
        sidecar = path.parent / self._EVALUATION_PROGRESS_FILENAME
        try:
            stat_result = sidecar.stat()
            if sidecar.is_symlink() or not sidecar.is_file() or stat_result.st_size > 16 * 1024:
                return None
            payload = self.json.loads(sidecar.read_text(encoding='utf-8'))
            evaluations = int(payload.get('evaluations'))
            published_size = int(payload.get('all_results_size') or 0)
            current_size = path.stat().st_size
        except (OSError, self.json.JSONDecodeError, TypeError, ValueError):
            return None
        if evaluations < 0 or published_size < 0 or published_size > current_size:
            return None
        return {'evaluations': evaluations, 'trailing_partial_entry': False, 'scan_complete': True, 'bytes_scanned': published_size, 'total_bytes': current_size, 'scan_percent': 100.0, 'source': 'runner'}

    def _cached_evaluation_scan_progress(self, path: Path) -> dict:
        """Return the current non-blocking fallback scan snapshot."""
        key = str(path.resolve(strict=False))
        try:
            total_bytes = path.stat().st_size
        except OSError:
            total_bytes = 0
        with self._active_eval_count_cache_lock:
            cached = self.copy.deepcopy(self._active_eval_count_cache.get(key) or {})
        offset = min(int(cached.get('offset') or 0), total_bytes)
        return {'evaluations': int(cached.get('evaluations') or 0), 'trailing_partial_entry': False, 'scan_complete': bool(total_bytes == 0 or offset >= total_bytes), 'bytes_scanned': offset, 'total_bytes': total_bytes, 'scan_percent': round(offset / total_bytes * 100.0, 1) if total_bytes else 100.0, 'source': 'fallback_scan'}

    def _run_active_evaluation_scan(self, path: Path, key: str, stop_event: threading.Event) -> None:
        """Continuously drain a legacy result history without blocking status requests."""
        last_offset = -1
        try:
            while not stop_event.is_set():
                progress = self._all_results_evaluation_count(path)
                offset = int(progress.get('bytes_scanned') or 0)
                if progress.get('error') or progress.get('scan_complete') or progress.get('trailing_partial_entry') or (offset <= last_offset):
                    break
                last_offset = offset
        finally:
            with self._active_eval_count_cache_lock:
                self._active_eval_scan_threads.pop(key, None)
                self._active_eval_scan_stops.pop(key, None)

    def _ensure_active_evaluation_scan(self, path: Path) -> None:
        """Start at most one bounded fallback scanner for one legacy result file."""
        key = str(path.resolve(strict=False))
        with self._active_eval_count_cache_lock:
            thread = self._active_eval_scan_threads.get(key)
            if thread is not None and thread.is_alive():
                return
            stop_event = self.threading.Event()
            thread = self.threading.Thread(target=self._run_active_evaluation_scan, args=(path, key, stop_event), name=f'{self.profile.text('pb8-eval-scan-')}{path.parent.name[:32]}', daemon=True)
            self._active_eval_scan_stops[key] = stop_event
            self._active_eval_scan_threads[key] = thread
            thread.start()

    def _shutdown_active_evaluation_scans(self) -> None:
        """Stop and join every API-owned legacy evaluation scanner."""
        with self._active_eval_count_cache_lock:
            stops = list(self._active_eval_scan_stops.values())
            threads = list(self._active_eval_scan_threads.values())
        for stop_event in stops:
            stop_event.set()
        for thread in threads:
            if thread is not self.threading.current_thread():
                thread.join(timeout=2)

    def _all_results_progress_for_listing(self, path: Path) -> dict:
        """Return bounded list metadata without cold-decoding a large result stream."""
        try:
            stat_result = path.stat()
            if path.is_symlink() or not path.is_file():
                raise OSError('not a regular file')
        except OSError:
            return {'evaluations': 0, 'bytes': 0, 'trailing_partial_entry': False, 'latest': {}}
        key = str(path.resolve(strict=False))
        with self._result_progress_cache_lock:
            cached = self._result_progress_cache.get(key)
            cache_current = bool(cached and cached.get('device') == stat_result.st_dev and (cached.get('inode') == stat_result.st_ino) and (cached.get('size') == stat_result.st_size) and (cached.get('mtime_ns') == stat_result.st_mtime_ns))
        if cache_current:
            return self._all_results_progress(path)
        return {'evaluations': 0, 'bytes': stat_result.st_size, 'trailing_partial_entry': False, 'latest': {}, 'scan_deferred': True}

    def _scan_all_results(self, path: Path) -> dict:
        key = str(path.resolve(strict=False))
        now = self.time.monotonic()
        with self._result_progress_cache_lock:
            for stale_key, cached in list(self._result_progress_cache.items()):
                if now - float(cached.get('accessed_at') or 0) > self._RESULT_PROGRESS_CACHE_TTL_SECONDS:
                    self._result_progress_cache.pop(stale_key, None)
            try:
                stat_result = path.stat()
                if path.is_symlink() or not path.is_file():
                    raise OSError('not a regular file')
            except OSError:
                self._result_progress_cache.pop(key, None)
                return {'device': 0, 'inode': 0, 'size': 0, 'mtime_ns': 0, 'offset': 0, 'evaluations': 0, 'current': {}, 'first': None}
            cached = self._result_progress_cache.get(key)
            prefix_length = min(int(cached.get('size') or 0), 64) if cached else 0
            suffix_length = prefix_length
            current_prefix = b''
            current_suffix = b''
            if prefix_length:
                try:
                    with path.open('rb') as prefix_handle:
                        current_prefix = prefix_handle.read(prefix_length)
                        prefix_handle.seek(int(cached['size']) - suffix_length)
                        current_suffix = prefix_handle.read(suffix_length)
                except OSError:
                    current_prefix = b''
                    current_suffix = b''
            same_file = bool(cached and cached.get('device') == stat_result.st_dev and (cached.get('inode') == stat_result.st_ino) and (stat_result.st_size >= int(cached.get('size') or 0)) and (current_prefix == bytes(cached.get('prefix') or b'')[:prefix_length]) and (current_suffix == bytes(cached.get('suffix') or b'')[-suffix_length:]) and (not (stat_result.st_size == int(cached.get('size') or 0) and stat_result.st_mtime_ns != cached.get('mtime_ns'))))
            if same_file:
                state = cached
                if stat_result.st_size == state['size']:
                    state['accessed_at'] = now
                    self._result_progress_cache.move_to_end(key)
                    return self.copy.deepcopy(state)
            else:
                state = {'device': stat_result.st_dev, 'inode': stat_result.st_ino, 'size': 0, 'mtime_ns': 0, 'offset': 0, 'evaluations': 0, 'current': {}, 'first': None}
            start_offset = int(state['offset'])
            last_complete = start_offset
            state.pop('error', None)
            try:
                with path.open('rb') as handle:
                    handle.seek(start_offset)
                    unpacker = self.msgpack.Unpacker(raw=False, strict_map_key=False, max_buffer_size=64 * 1024 * 1024)
                    remaining = stat_result.st_size - start_offset
                    while remaining > 0:
                        chunk = handle.read(min(1024 * 1024, remaining))
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        unpacker.feed(chunk)
                        for entry in unpacker:
                            last_complete = start_offset + unpacker.tell()
                            if not isinstance(entry, dict):
                                raise ValueError('all_results.bin contains a non-object record')
                            if state['first'] is None:
                                state['first'] = self.copy.deepcopy(entry)
                            if state['evaluations'] % 100 == 0:
                                state['current'] = self.copy.deepcopy(entry)
                            else:
                                state['current'] = self._apply_result_diff(state['current'], entry)
                            state['evaluations'] += 1
                            state['offset'] = last_complete
            except (OSError, ValueError, self.msgpack.UnpackException) as exc:
                state['error'] = str(exc)
            if not state.get('error') and state['offset'] < stat_result.st_size:
                tail_size = stat_result.st_size - state['offset']
                if tail_size <= 64 * 1024 * 1024:
                    try:
                        with path.open('rb') as tail_handle:
                            tail_handle.seek(state['offset'])
                            self.msgpack.unpackb(tail_handle.read(tail_size), raw=False, strict_map_key=False)
                    except (self.msgpack.FormatError, self.msgpack.StackError) as exc:
                        state['error'] = str(exc) or exc.__class__.__name__
                    except ValueError:
                        pass
                else:
                    state['error'] = 'Trailing MessagePack record exceeds the safe decode limit'
            state.update(device=stat_result.st_dev, inode=stat_result.st_ino, size=stat_result.st_size, mtime_ns=stat_result.st_mtime_ns, accessed_at=now)
            try:
                with path.open('rb') as prefix_handle:
                    state['prefix'] = prefix_handle.read(min(stat_result.st_size, 64))
                    prefix_handle.seek(max(0, stat_result.st_size - 64))
                    state['suffix'] = prefix_handle.read(min(stat_result.st_size, 64))
            except OSError:
                state['prefix'] = b''
                state['suffix'] = b''
            self._result_progress_cache[key] = state
            self._result_progress_cache.move_to_end(key)
            while len(self._result_progress_cache) > self._RESULT_PROGRESS_CACHE_MAX_ENTRIES:
                self._result_progress_cache.popitem(last=False)
            return self.copy.deepcopy(state)

    def _all_results_first(self, path: Path) -> dict | None:
        """Decode only the first bounded MessagePack record needed for result metadata."""
        try:
            stat_result = path.stat()
            if path.is_symlink() or not path.is_file() or stat_result.st_size <= 0:
                return None
        except OSError:
            return None
        key = str(path.resolve(strict=False))
        with self._result_progress_cache_lock:
            cached = self._result_progress_cache.get(key)
            if cached and cached.get('device') == stat_result.st_dev and (cached.get('inode') == stat_result.st_ino) and (cached.get('size') == stat_result.st_size) and (cached.get('mtime_ns') == stat_result.st_mtime_ns) and isinstance(cached.get('first'), dict):
                return self.copy.deepcopy(cached['first'])
        unpacker = self.msgpack.Unpacker(raw=False, strict_map_key=False, max_buffer_size=64 * 1024 * 1024)
        remaining = min(stat_result.st_size, self._RESULT_LIST_SCAN_LIMIT_BYTES)
        try:
            with path.open('rb') as handle:
                while remaining > 0:
                    chunk = handle.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    unpacker.feed(chunk)
                    for entry in unpacker:
                        return self.copy.deepcopy(entry) if isinstance(entry, dict) else None
        except (OSError, ValueError, self.msgpack.UnpackException):
            return None
        return None

    def _result_name(self, result_dir: Path, data_hint: dict | None=None) -> str:
        data = data_hint if isinstance(data_hint, dict) else None
        if data is None:
            first = self._first_pareto_file(result_dir)
            if first:
                try:
                    data = self._read_json(first)
                except (RuntimeError, AttributeError):
                    pass
            if data is None:
                data = self._all_results_first(result_dir / 'all_results.bin')
        if isinstance(data, dict):
            base_dir = str((data.get('backtest') or {}).get('base_dir') or '')
            if base_dir:
                return self.PurePath(base_dir).name
        return result_dir.name

    def _latest_existing_mtime(self, paths: list[Path]) -> float | None:
        """Return the latest mtime while tolerating active result-file replacement."""
        mtimes = []
        for path in paths:
            try:
                mtimes.append(path.stat().st_mtime)
            except OSError:
                continue
        return max(mtimes) if mtimes else None

    def _result_listing_signature(self, directory: Path, pareto_dir: Path, paretos: list[Path]) -> tuple:
        """Track the files that can change a result row without decoding their contents."""
        paths = [directory, pareto_dir, paretos[0] if paretos else None, directory / 'all_results.bin', directory / 'checkpoint.pkl', directory / 'config.json', directory / 'optimize.json']
        signature = [paretos[0].name if paretos else None, len(paretos)]
        for path in paths:
            if path is None:
                signature.append(None)
                continue
            try:
                info = path.stat(follow_symlinks=False)
                signature.append((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
            except OSError:
                signature.append(None)
        return tuple(signature)

    def _list_results(self) -> list[dict]:
        results = []
        results_root = self._results_root()
        for directory in self._result_dirs(results_root) or []:
            pareto_dir, paretos = self._pareto_files_for_listing(directory)
            signature = self._result_listing_signature(directory, pareto_dir, paretos)
            cache_key = str(directory)
            now = self.time.monotonic()
            with self._result_listing_cache_lock:
                cached = self._result_listing_cache.get(cache_key)
                if cached and cached['signature'] == signature and (now - cached['created_at'] < self._RESULT_LISTING_CACHE_TTL_SECONDS):
                    self._result_listing_cache.move_to_end(cache_key)
                    results.append(self.copy.deepcopy(cached['row']))
                    continue
            artifacts = [path for path in (directory / 'all_results.bin', directory / 'checkpoint.pkl') if path.is_file()]
            modified_paths = [pareto_dir, *artifacts] if paretos else [*artifacts, directory]
            progress = self._all_results_progress_for_listing(directory / 'all_results.bin')
            first_data = None
            if paretos:
                try:
                    first_data = self._read_json(paretos[0])
                except RuntimeError:
                    pass
            if first_data is None:
                first_data = self._all_results_first(directory / 'all_results.bin')
            contract = self._pareto_contract(first_data or {})
            readiness = self._checkpoint_resume_readiness(directory, for_listing=True, progress_hint=progress, config_hint=first_data, results_root=results_root)
            result_config = first_data if isinstance(first_data, dict) else {}
            recovered_config = readiness.get('config') if isinstance(readiness.get('config'), dict) else {}
            strategy = str(self._listed_strategy_kind(result_config) or self._listed_strategy_kind(recovered_config) or '').strip()
            has_pareto = bool(paretos)
            summary = self._pareto_summary(first_data or {}, 'mean')
            objective_names = [spec['metric'] for spec in contract['objectives'] if spec['metric'] in summary]
            if not objective_names:
                objective_names = list(summary)
            checkpoint_present = (directory / 'checkpoint.pkl').is_file() and (not (directory / 'checkpoint.pkl').is_symlink())
            modified = self._latest_existing_mtime(modified_paths)
            if modified is None:
                modified = self._latest_existing_mtime([directory])
            if modified is None:
                continue
            row = {'path': str(directory), 'result': directory.name, 'name': self._result_name(directory, first_data), 'pareto_count': len(paretos), 'has_pareto': has_pareto, 'checkpoint': checkpoint_present, 'checkpoint_present': checkpoint_present, 'resumable': readiness['ready'], 'has_config': readiness['config'] is not None, 'supports_3d': has_pareto and len(objective_names) == 3, 'supports_dash': has_pareto, 'resume_reasons': readiness['reasons'], 'evaluations': progress['evaluations'], 'progress': progress, 'strategy': strategy, 'mode': contract['mode'], 'scenario_count': contract['scenario_count'], 'scenario_labels': contract['scenario_labels'], 'modified': self.datetime.datetime.fromtimestamp(modified).isoformat()}
            with self._result_listing_cache_lock:
                self._result_listing_cache[cache_key] = {'signature': signature, 'created_at': now, 'row': self.copy.deepcopy(row)}
                self._result_listing_cache.move_to_end(cache_key)
                while len(self._result_listing_cache) > self._RESULT_LISTING_CACHE_MAX_ENTRIES:
                    self._result_listing_cache.popitem(last=False)
            results.append(row)
        return sorted(results, key=lambda item: item['modified'], reverse=True)

    def _metric_value(self, value, statistic: str, scenario: str='Aggregated') -> float | None:
        if isinstance(value, (int, float)) and (not isinstance(value, bool)):
            return float(value)
        if not isinstance(value, dict):
            return None
        if scenario != 'Aggregated':
            scenario_value = (value.get('scenarios') or {}).get(scenario) if isinstance(value.get('scenarios'), dict) else None
            if isinstance(scenario_value, (int, float)) and (not isinstance(scenario_value, bool)):
                return float(scenario_value)
        for candidate in (value.get('aggregated') if statistic == 'mean' else None, (value.get('stats') or {}).get(statistic), value.get(statistic)):
            if isinstance(candidate, (int, float)) and (not isinstance(candidate, bool)):
                return float(candidate)
        return None

    def _load_sweep_plan(self, result_dir: Path) -> dict | None:
        """Load one bounded PBGui sweep sidecar without following links."""
        path = result_dir / self.SWEEP_PLAN_FILENAME
        try:
            stat_result = path.stat()
            if path.is_symlink() or not path.is_file() or stat_result.st_size > 256 * 1024:
                return None
            payload = self.json.loads(path.read_text(encoding='utf-8'))
        except (OSError, self.json.JSONDecodeError, TypeError):
            return None
        return self.validate_sweep_plan(payload)

    def _suite_metric_payload(self, data: dict) -> tuple[dict, list[str]]:
        suite = data.get('suite_metrics') if isinstance(data.get('suite_metrics'), dict) else {}
        labels = suite.get('scenario_labels') or suite.get('scenarios') or []
        labels = [str(label).strip() for label in labels if str(label).strip()] if isinstance(labels, list) else []
        metrics = suite.get('metrics')
        if not isinstance(metrics, dict):
            aggregate = suite.get('aggregate') if isinstance(suite.get('aggregate'), dict) else {}
            stats = aggregate.get('stats') if isinstance(aggregate.get('stats'), dict) else {}
            aggregated = aggregate.get('aggregated') if isinstance(aggregate.get('aggregated'), dict) else {}
            metrics = {str(name): {'stats': value if isinstance(value, dict) else {}, 'aggregated': aggregated.get(name, value.get('mean') if isinstance(value, dict) else None), 'scenarios': {}} for name, value in {**aggregated, **stats}.items()}
        for value in metrics.values() if isinstance(metrics, dict) else []:
            scenarios = value.get('scenarios') if isinstance(value, dict) and isinstance(value.get('scenarios'), dict) else {}
            for label in scenarios:
                normalized = str(label).strip()
                if normalized and normalized not in labels:
                    labels.append(normalized)
        return (metrics if isinstance(metrics, dict) else {}, labels)

    def _suite_metric_sources(self, data: dict) -> dict:
        """Return the per-scenario native evaluation clocks needed to invert CAGR."""
        suite = data.get('suite_metrics') if isinstance(data.get('suite_metrics'), dict) else {}
        sources = suite.get('sources') if isinstance(suite.get('sources'), dict) else {}
        result = {}
        for label, per_label in sources.items():
            if not isinstance(per_label, dict):
                continue
            result[str(label)] = {str(source): {key: metadata[key] for key in ('start_timestamp_ms', 'actual_end_timestamp_ms', 'year_days') if key in metadata} for source, metadata in per_label.items() if isinstance(metadata, dict)}
        return result

    def _pareto_objective_specs(self, data: dict) -> list[dict]:
        optimize = data.get('optimize') if isinstance(data.get('optimize'), dict) else {}
        scoring = optimize.get('scoring') if isinstance(optimize.get('scoring'), list) else []
        specs = []
        for item in scoring:
            if isinstance(item, dict):
                name = str(item.get('metric') or '').strip()
                goal = 'min' if str(item.get('goal') or '').strip().lower() == 'min' else 'max'
            else:
                name = str(item or '').strip()
                goal = 'max'
            if name and name not in {entry['metric'] for entry in specs}:
                specs.append({'metric': name, 'goal': goal})
        return specs

    def _pareto_contract(self, data: dict) -> dict:
        suite_metrics, labels = self._suite_metric_payload(data)
        if suite_metrics:
            mode = 'suite'
        elif isinstance(data.get('metrics'), dict) and data['metrics']:
            mode = 'stats'
        elif 'analyses_combined' in data or 'analyses' in data:
            mode = 'legacy'
        else:
            mode = 'unknown'
        return {'mode': mode, 'scenario_count': len(labels), 'scenario_labels': labels, 'objectives': self._pareto_objective_specs(data)}

    def _pareto_summary(self, data: dict, statistic: str, scenario: str='Aggregated') -> dict:
        suite_metrics, _labels = self._suite_metric_payload(data)
        if suite_metrics:
            return {str(key): numeric for key, value in suite_metrics.items() if (numeric := self._metric_value(value, statistic, scenario)) is not None}
        metrics = data.get('metrics') if isinstance(data.get('metrics'), dict) else {}
        roots = [data.get('objectives'), (data.get('result') or {}).get('objectives') if isinstance(data.get('result'), dict) else None, metrics.get('objectives'), metrics.get('stats'), metrics]
        result = {}
        for root in roots:
            if not isinstance(root, dict):
                continue
            for key, value in root.items():
                numeric = self._metric_value(value, statistic, scenario)
                if numeric is not None:
                    result[str(key)] = numeric
            if result:
                break
        return result

    def _compact_metric_value(self, value):
        """Retain only numeric projection fields needed by the Pareto list."""
        if isinstance(value, (int, float)) and (not isinstance(value, bool)):
            return float(value)
        if not isinstance(value, dict):
            return None
        compact = {}
        for key in ('aggregated', *self._PARETO_STATISTICS):
            candidate = value.get(key)
            if isinstance(candidate, (int, float)) and (not isinstance(candidate, bool)):
                compact[key] = float(candidate)
        stats = value.get('stats') if isinstance(value.get('stats'), dict) else {}
        compact_stats = {key: float(stats[key]) for key in self._PARETO_STATISTICS if isinstance(stats.get(key), (int, float)) and (not isinstance(stats.get(key), bool))}
        if compact_stats:
            compact['stats'] = compact_stats
        scenarios = value.get('scenarios') if isinstance(value.get('scenarios'), dict) else {}
        compact_scenarios = {str(key): float(candidate) for key, candidate in scenarios.items() if isinstance(candidate, (int, float)) and (not isinstance(candidate, bool))}
        if compact_scenarios:
            compact['scenarios'] = compact_scenarios
        return compact or None

    def _requested_pareto_metrics(self, value) -> list[str]:
        """Return one bounded, validated list of metric projections requested by the browser."""
        if not isinstance(value, str) or not value.strip():
            return []
        result = []
        for item in value.split(','):
            metric = item.strip()
            if not metric or metric in result:
                continue
            if not self._PARETO_METRIC_NAME_RE.fullmatch(metric):
                raise self.HTTPException(status_code=422, detail=f'Invalid Pareto metric: {metric[:128]}')
            result.append(metric)
            if len(result) > self._PARETO_MAX_REQUESTED_METRICS:
                raise self.HTTPException(status_code=422, detail='Too many Pareto metrics requested')
        return result

    def _pareto_metric_catalog(self, data: dict) -> list[str]:
        """Return all numeric metric names advertised by one result's stable metric schema."""
        suite_metrics, _labels = self._suite_metric_payload(data)
        metrics = data.get('metrics') if isinstance(data.get('metrics'), dict) else {}
        stats_root = metrics.get('stats') if isinstance(metrics.get('stats'), dict) else {}
        roots = [suite_metrics, stats_root]
        roots.extend((root for root in (data.get('objectives'), (data.get('result') or {}).get('objectives') if isinstance(data.get('result'), dict) else None, metrics.get('objectives')) if isinstance(root, dict)))
        raw_names = []
        for root in roots:
            if not isinstance(root, dict):
                continue
            for name, value in root.items():
                normalized = str(name).strip()
                if normalized and normalized not in raw_names and (self._compact_metric_value(value) is not None):
                    raw_names.append(normalized)
        raw_set = set(raw_names)
        canonical = [summary_name for summary_name, aliases in self._PARETO_COMPARISON_METRICS if any((alias in raw_set for alias in aliases))]
        return list(dict.fromkeys([*canonical, *sorted(raw_names)]))

    def _pareto_list_objective_specs(self, data: dict) -> list[dict]:
        specs = self._pareto_objective_specs(data)
        if specs:
            return specs
        metrics = data.get('metrics') if isinstance(data.get('metrics'), dict) else {}
        roots = [data.get('objectives'), (data.get('result') or {}).get('objectives') if isinstance(data.get('result'), dict) else None, metrics.get('objectives')]
        names = []
        for root in roots:
            if not isinstance(root, dict):
                continue
            for name in root:
                normalized = str(name).strip()
                if normalized and normalized not in names:
                    names.append(normalized)
        return [{'metric': name, 'goal': 'max'} for name in names]

    def _compact_pareto_data(self, data: dict, requested_metrics: tuple[str, ...]=(), *, include_catalog: bool=False) -> dict:
        contract = self._pareto_contract(data)
        specs = self._pareto_list_objective_specs(data)
        names = [spec['metric'] for spec in specs]
        default_projection_names = list(dict.fromkeys(['gain', *names, 'drawdown_worst']))
        projection_names = list(dict.fromkeys([*default_projection_names, *requested_metrics]))
        comparison_by_name = dict(self._PARETO_COMPARISON_METRICS)
        metric_names = []
        for name in projection_names:
            for source_name in comparison_by_name.get(name, (name,)):
                if source_name not in metric_names:
                    metric_names.append(source_name)
        suite_metrics, labels = self._suite_metric_payload(data)
        metrics = data.get('metrics') if isinstance(data.get('metrics'), dict) else {}
        stats_root = metrics.get('stats') if isinstance(metrics.get('stats'), dict) else {}
        objective_roots = [data.get('objectives'), (data.get('result') or {}).get('objectives') if isinstance(data.get('result'), dict) else None, metrics.get('objectives')]
        objective_values = {}
        for name in metric_names:
            for root in objective_roots:
                if not isinstance(root, dict) or name not in root:
                    continue
                compact = self._compact_metric_value(root[name])
                if compact is not None:
                    objective_values[name] = compact
                    break
        return {'mode': contract['mode'], 'scenario_count': len(labels) if suite_metrics else contract['scenario_count'], 'scenario_labels': labels if suite_metrics else contract['scenario_labels'], 'objectives': specs, 'objective_names': names, 'default_projection_names': default_projection_names, 'available_metric_names': self._pareto_metric_catalog(data) if include_catalog else [], 'suite_values': {name: compact for name in metric_names if name in suite_metrics and (compact := self._compact_metric_value(suite_metrics[name])) is not None}, 'stats_values': {name: compact for name in metric_names if name in stats_root and (compact := self._compact_metric_value(stats_root[name])) is not None}, 'objective_values': objective_values, 'suite_sources': self._suite_metric_sources(data) if self._SWEEP_GAIN_SOURCE_METRICS else {}}

    def _project_compact_pareto(self, compact: dict, statistic: str, scenario: str, requested_metrics: tuple[str, ...]=()) -> dict:
        suite_values = compact['suite_values']
        stats_values = compact['stats_values']
        objective_values = compact['objective_values']

        def requested_stat(value) -> float | None:
            if isinstance(value, (int, float)) and (not isinstance(value, bool)):
                return float(value)
            if not isinstance(value, dict):
                return None
            nested_stats = value.get('stats') if isinstance(value.get('stats'), dict) else {}
            for candidate in (value.get(statistic), nested_stats.get(statistic)):
                if isinstance(candidate, (int, float)) and (not isinstance(candidate, bool)):
                    return float(candidate)
            return None

        def value_for(name: str) -> float | None:
            if compact['mode'] == 'suite':
                return self._metric_value(suite_values.get(name), statistic, scenario)
            value = requested_stat(stats_values.get(name))
            if value is not None:
                return value
            return self._metric_value(objective_values.get(name), statistic, scenario)
        summary = {}
        comparison_by_name = dict(self._PARETO_COMPARISON_METRICS)
        projection_names = list(dict.fromkeys([*compact['default_projection_names'], *requested_metrics]))
        for name in projection_names:
            aliases = comparison_by_name.get(name)
            if aliases is None:
                value = value_for(name)
                if value is not None:
                    summary[name] = value
                continue
            for alias in aliases:
                value = value_for(alias)
                if value is not None:
                    summary[name] = value
                    break
        return summary

    def _pareto_file_signature(self, path: Path) -> tuple[tuple[str, int | None, int, int], os.stat_result]:
        if path.is_symlink():
            raise OSError('symlinked Pareto candidate')
        stat_result = path.stat()
        if not path.is_file():
            raise OSError('Pareto candidate is not a regular file')
        resolved = str(path.resolve())
        inode = int(stat_result.st_ino) if getattr(stat_result, 'st_ino', 0) else None
        return ((resolved, inode, int(stat_result.st_mtime_ns), int(stat_result.st_size)), stat_result)

    def _prune_pareto_list_cache(self, now: float | None=None) -> None:
        """Expire compact projections once per list request instead of once per candidate."""
        cutoff = self.time.monotonic() if now is None else now
        with self._pareto_list_cache_lock:
            for key, entry in list(self._pareto_list_cache.items()):
                if cutoff - entry['loaded_at'] > self._PARETO_LIST_CACHE_TTL_SECONDS:
                    self._pareto_list_cache.pop(key, None)

    def _load_compact_pareto(self, path: Path, requested_metrics: tuple[str, ...]=(), *, include_catalog: bool=False) -> tuple[dict, os.stat_result]:
        signature, stat_result = self._pareto_file_signature(path)
        now = self.time.monotonic()
        requested_set = set(requested_metrics)
        cached = None
        with self._pareto_list_cache_lock:
            cached = self._pareto_list_cache.get(signature)
            if cached is not None and now - cached['loaded_at'] > self._PARETO_LIST_CACHE_TTL_SECONDS:
                self._pareto_list_cache.pop(signature, None)
                cached = None
            if cached is not None and requested_set.issubset(cached.get('loaded_metrics') or set()) and (not include_catalog or bool(cached['compact'].get('available_metric_names'))):
                self._pareto_list_cache.move_to_end(signature)
                return (cached['compact'], stat_result)
        loaded_metrics = set(cached.get('loaded_metrics') or set()) if cached is not None else set()
        loaded_metrics.update(requested_set)
        keep_catalog = include_catalog or bool((cached or {}).get('compact', {}).get('available_metric_names'))
        data = self._read_json(path)
        final_signature, final_stat = self._pareto_file_signature(path)
        if final_signature != signature:
            raise RuntimeError(f'{path.name} changed while being read')
        compact = self._compact_pareto_data(data, tuple(sorted(loaded_metrics)), include_catalog=keep_catalog)
        with self._pareto_list_cache_lock:
            resolved = signature[0]
            for key in list(self._pareto_list_cache):
                if key[0] == resolved and key != signature:
                    self._pareto_list_cache.pop(key, None)
            self._pareto_list_cache[signature] = {'loaded_at': now, 'compact': compact, 'loaded_metrics': loaded_metrics}
            self._pareto_list_cache.move_to_end(signature)
            while len(self._pareto_list_cache) > self._PARETO_LIST_CACHE_MAX_ENTRIES:
                self._pareto_list_cache.popitem(last=False)
            return (compact, final_stat)

    def _log_pareto_skips(self, result_dir: Path, count: int, example: str) -> None:
        key = str(result_dir)
        now = self.time.monotonic()
        with self._pareto_list_cache_lock:
            last = self._pareto_warning_cache.get(key)
            if last is not None and now - last < self._PARETO_WARNING_TTL_SECONDS:
                return
            self._pareto_warning_cache[key] = now
            self._pareto_warning_cache.move_to_end(key)
            while len(self._pareto_warning_cache) > self._PARETO_WARNING_MAX_ENTRIES:
                self._pareto_warning_cache.popitem(last=False)
        self._log(self.SERVICE, f'Skipped {count} unreadable PB8 Pareto candidate(s) in {result_dir.name}; first error: {example}', level='WARNING')

    def _create_seed_bundle(self, result_dir: Path, paths: list[Path]) -> Path:
        root = self._safe_path(result_dir / '_seed_bundles', self._results_root())
        root.mkdir(mode=448, exist_ok=True)
        name = f'{self.datetime.datetime.now(self.datetime.UTC).strftime('%Y%m%d_%H%M%S')}_{self.uuid.uuid4().hex[:8]}'
        stage = self._safe_path(root / f'.{name}.tmp', root)
        target = self._safe_path(root / name, root)
        stage.mkdir(mode=448)
        try:
            for index, source in enumerate(paths, start=1):
                destination = stage / source.name
                if destination.exists():
                    destination = stage / f'{index:03d}_{source.name}'
                self.copy2(source, destination)
                self.os.chmod(destination, 384)
            self.os.replace(stage, target)
        finally:
            self.rmtree(stage, ignore_errors=True)
        return target

    def _find_free_local_port(self) -> int:
        with self.socket.socket(self.socket.AF_INET, self.socket.SOCK_STREAM) as handle:
            handle.bind(('127.0.0.1', 0))
            handle.setsockopt(self.socket.SOL_SOCKET, self.socket.SO_REUSEADDR, 1)
            return int(handle.getsockname()[1])

    def _dash_cache_root(self) -> Path:
        return self._data_dir() / 'cache' / self.profile.text('pareto_dash_v8')

    def _dash_registry_root(self) -> Path:
        return self._data_dir() / 'locks' / self.profile.text('pareto_dash_v8')

    def _dash_registry_path(self) -> Path:
        return self._dash_registry_root() / 'sessions.json'

    def _dash_registry_lock(self):
        self.ensure_private_directory(self._data_dir() / 'locks')
        self.ensure_private_directory(self._dash_registry_root())
        return self.advisory_file_lock(self._dash_registry_root() / 'sessions')

    def _dash_log_path(self, session_id: str) -> Path:
        return self._log_dir() / f'pareto_dash_{session_id}.log'

    def _valid_dash_session_id(self, session_id: str) -> bool:
        return len(session_id) == 12 and all((char in '0123456789abcdef' for char in session_id))

    def _dash_registry_record(self, session_id: str, raw: dict) -> dict | None:
        """Return the non-secret, process-verifiable registry fields for one session."""
        if not self._valid_dash_session_id(session_id) or not isinstance(raw, dict):
            return None
        if not isinstance(raw.get('command'), list) or not isinstance(raw.get('command_markers'), list):
            return None
        try:
            command = [str(part) for part in raw['command']]
            command_markers = [str(part) for part in raw['command_markers']]
            record = {'session_id': session_id, 'pid': int(raw['pid']), 'create_time': float(raw['create_time']), 'owner_pid': int(raw['owner_pid']), 'owner_create_time': float(raw['owner_create_time']), 'command': command, 'command_markers': command_markers, 'result_dir': str(raw['result_dir']), 'stage_root': str(raw['stage_root']), 'port': int(raw['port']), 'created_at': float(raw['created_at']), 'last_access': float(raw['last_access'])}
        except (KeyError, TypeError, ValueError):
            return None
        if record['pid'] <= 0 or record['owner_pid'] <= 0 or (not 0 < record['port'] <= 65535):
            return None
        if not command or len(command) > 32 or len(command_markers) > 31:
            return None
        return record

    def _read_dash_registry_unlocked(self) -> dict[str, dict]:
        path = self._dash_registry_path()
        if not path.exists():
            return {}
        try:
            payload = self._read_json(path)
        except RuntimeError as exc:
            self._log(self.SERVICE, f'Failed to read PB8 Pareto Dash registry: {exc}', level='WARNING')
            return {}
        if payload.get('version') != self._DASH_REGISTRY_VERSION or not isinstance(payload.get('sessions'), dict):
            self._log(self.SERVICE, 'Ignoring invalid PB8 Pareto Dash registry', level='WARNING')
            return {}
        records = {}
        for session_id, raw in payload['sessions'].items():
            record = self._dash_registry_record(str(session_id), raw)
            if record is not None:
                records[record['session_id']] = record
        return records

    def _write_dash_registry_unlocked(self, records: dict[str, dict]) -> None:
        sessions = {}
        for session_id, raw in records.items():
            record = self._dash_registry_record(str(session_id), raw)
            if record is not None:
                sessions[session_id] = record
        self._write_json(self._dash_registry_path(), {'version': self._DASH_REGISTRY_VERSION, 'sessions': sessions})

    def _read_dash_registry(self) -> dict[str, dict]:
        with self._dash_registry_lock():
            return self._read_dash_registry_unlocked()

    def _persist_dash_session(self, session: dict) -> None:
        session_id = str(session.get('session_id') or '')
        record = self._dash_registry_record(session_id, session)
        if record is None:
            raise RuntimeError('Invalid PB8 Pareto Dash session record')
        with self._dash_registry_lock():
            records = self._read_dash_registry_unlocked()
            active_records = [item for item in records.values() if self._dash_owner_matches(item)]
            if session_id not in records and len(active_records) >= self._DASH_MAX_ACTIVE_SESSIONS:
                raise RuntimeError('PB8 Pareto Dash session limit reached')
            records[session_id] = record
            self._write_dash_registry_unlocked(records)

    def _remove_dash_registry_session(self, session_id: str) -> None:
        if not self._valid_dash_session_id(session_id):
            return
        with self._dash_registry_lock():
            records = self._read_dash_registry_unlocked()
            if records.pop(session_id, None) is not None or self._dash_registry_path().exists():
                self._write_dash_registry_unlocked(records)

    def _dash_process_matches(self, record: dict) -> bool:
        """Verify PID reuse protection and the complete command before process control."""
        try:
            session_id = str(record['session_id'])
            command = [str(part) for part in record['command']]
            expected_stage = self.Path(self.os.path.abspath(self._dash_cache_root() / session_id))
            expected_markers = ['-u', command[2], '--data-root', str(expected_stage / 'runs'), '--host', '127.0.0.1', '--port', str(int(record['port']))]
            if len(command) != 9 or self.Path(command[2]).name != 'pareto_dash.py' or command != [command[0], *expected_markers] or (record.get('command_markers') != expected_markers) or (self.Path(self.os.path.abspath(str(record['stage_root']))) != expected_stage):
                return False
            process = self.psutil.Process(int(record['pid']))
            return abs(process.create_time() - float(record['create_time'])) <= 0.01 and [str(part) for part in process.cmdline()] == command
        except (IndexError, KeyError, self.psutil.Error, OSError, TypeError, ValueError):
            return False

    def _dash_owner_matches(self, record: dict) -> bool:
        try:
            process = self.psutil.Process(int(record['owner_pid']))
            return abs(process.create_time() - float(record['owner_create_time'])) <= 0.01
        except (KeyError, self.psutil.Error, OSError, TypeError, ValueError):
            return False

    def _terminate_dash_record(self, record: dict) -> None:
        if not self._dash_process_matches(record):
            return
        pid = int(record['pid'])
        process = self.psutil.Process(pid)
        if self.platform.system() == 'Windows':
            try:
                process.terminate()
                process.wait(timeout=2)
            except self.psutil.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
            except self.psutil.NoSuchProcess:
                pass
            return
        process_group = self.os.getpgid(pid)
        if process_group != pid or process_group == self.os.getpgrp():
            raise RuntimeError('Refusing to terminate an unverified PB8 Pareto Dash process group')
        try:
            self.os.killpg(process_group, self.signal.SIGTERM)
            process.wait(timeout=2)
        except self.psutil.TimeoutExpired:
            if self._dash_process_matches(record):
                self.os.killpg(process_group, self.signal.SIGKILL)
                try:
                    process.wait(timeout=2)
                except (self.psutil.NoSuchProcess, self.psutil.TimeoutExpired):
                    pass
        except (ProcessLookupError, self.psutil.NoSuchProcess):
            pass

    def _remove_dash_artifacts(self, session_id: str, record: dict | None=None) -> None:
        if not self._valid_dash_session_id(session_id):
            return
        expected_stage = self.Path(self.os.path.abspath(self._dash_cache_root() / session_id))
        if record is not None and self.Path(self.os.path.abspath(str(record.get('stage_root') or ''))) != expected_stage:
            self._log(self.SERVICE, f'Ignored mismatched PB8 Pareto Dash stage path for {session_id}', level='WARNING')
        if expected_stage.is_symlink():
            expected_stage.unlink(missing_ok=True)
        elif expected_stage.is_dir():
            self.rmtree(expected_stage)
        log_path = self.Path(self.os.path.abspath(self._dash_log_path(session_id)))
        expected_log_root = self.Path(self.os.path.abspath(self._log_dir()))
        try:
            log_path.relative_to(expected_log_root)
        except ValueError:
            return
        log_path.unlink(missing_ok=True)
        for rotated in log_path.parent.glob(f'{log_path.name}.*'):
            if rotated.name.removeprefix(f'{log_path.name}.').isdigit():
                rotated.unlink(missing_ok=True)

    def _cleanup_dash_record(self, session_id: str, record: dict | None, *, remove_registry: bool=True) -> None:
        if record is not None:
            try:
                if self._dash_process_matches(record):
                    self._terminate_dash_record(record)
                elif self.psutil.pid_exists(int(record.get('pid') or 0)):
                    self._log(self.SERVICE, f'Refused to signal unverified PB8 Pareto Dash PID for {session_id}', level='WARNING')
            except Exception as exc:
                self._log(self.SERVICE, f'Failed to stop PB8 Pareto Dash session {session_id}: {exc}', level='ERROR')
        try:
            self._remove_dash_artifacts(session_id, record)
        except Exception as exc:
            self._log(self.SERVICE, f'Failed to clean PB8 Pareto Dash artifacts for {session_id}: {exc}', level='WARNING')
        if remove_registry:
            try:
                self._remove_dash_registry_session(session_id)
            except Exception as exc:
                self._log(self.SERVICE, f'Failed to remove PB8 Pareto Dash registry entry {session_id}: {exc}', level='WARNING')

    def _stop_dash_session(self, session_id: str) -> None:
        if not self._valid_dash_session_id(session_id):
            return
        with self._dash_lock:
            session = self._dash_sessions.pop(session_id, None)
            pending = self._dash_pending_sessions.pop(session_id, None)
        record = session or pending
        if record is None:
            try:
                record = self._read_dash_registry().get(session_id)
            except Exception as exc:
                self._log(self.SERVICE, f'Failed to inspect PB8 Pareto Dash registry for {session_id}: {exc}', level='WARNING')
        self._cleanup_dash_record(session_id, record)

    def _stop_all_dash_sessions(self) -> None:
        with self._dash_lock:
            self._dash_admission_open = False
            session_ids = list(dict.fromkeys([*self._dash_sessions, *self._dash_pending_sessions]))
        for session_id in session_ids:
            try:
                self._stop_dash_session(session_id)
            except Exception as exc:
                self._log(self.SERVICE, f'Failed to clean PB8 Pareto Dash session {session_id}: {exc}', level='ERROR')

    def _recover_dash_registry(self) -> None:
        """Terminate only exactly verified Dash orphans and discard all stale ownership."""
        with self._dash_lock:
            local_records = {**self._dash_pending_sessions, **self._dash_sessions}
            self._dash_pending_sessions.clear()
            self._dash_sessions.clear()
        try:
            records = self._read_dash_registry()
        except Exception as exc:
            self._log(self.SERVICE, f'Failed to load PB8 Pareto Dash recovery registry: {exc}', level='ERROR')
            records = {}
        local_records = {key: value for key, value in local_records.items() if value}
        survivors = {session_id: record for session_id, record in records.items() if session_id not in local_records and self._dash_owner_matches(record)}
        for session_id, record in {**records, **local_records}.items():
            if session_id in survivors:
                continue
            try:
                self._cleanup_dash_record(session_id, record, remove_registry=False)
            except Exception as exc:
                self._log(self.SERVICE, f'Failed to recover PB8 Pareto Dash session {session_id}: {exc}', level='ERROR')
        cache_root = self._dash_cache_root()
        if cache_root.is_dir() and (not cache_root.is_symlink()):
            for stage_root in cache_root.iterdir():
                if self._valid_dash_session_id(stage_root.name) and stage_root.name not in survivors:
                    try:
                        self._remove_dash_artifacts(stage_root.name)
                    except Exception as exc:
                        self._log(self.SERVICE, f'Failed to remove stale PB8 Pareto Dash stage {stage_root.name}: {exc}', level='WARNING')
        try:
            with self._dash_registry_lock():
                self._write_dash_registry_unlocked(survivors)
        except Exception as exc:
            self._log(self.SERVICE, f'Failed to clear PB8 Pareto Dash recovery registry: {exc}', level='ERROR')

    def _reap_dash_sessions(self) -> None:
        now = self.time.time()
        with self._dash_lock:
            stale = [session_id for session_id, launched in self._dash_sessions.items() if launched['process'].poll() is not None or now - float(launched.get('last_access') or launched.get('created_at') or 0) > self._DASH_IDLE_TTL_SECONDS]
            known = {*self._dash_sessions, *self._dash_pending_sessions}
        try:
            stale.extend((session_id for session_id, record in self._read_dash_registry().items() if session_id not in known and (not self._dash_owner_matches(record))))
        except Exception as exc:
            self._log(self.SERVICE, f'Failed to inspect PB8 Pareto Dash registry while reaping: {exc}', level='WARNING')
        for session_id in dict.fromkeys(stale):
            try:
                self._stop_dash_session(session_id)
            except Exception as exc:
                self._log(self.SERVICE, f'Failed to reap PB8 Pareto Dash session {session_id}: {exc}', level='ERROR')

    def _reserve_dash_launch(self, session_id: str) -> None:
        if not self._valid_dash_session_id(session_id):
            raise self.HTTPException(status_code=400, detail='Invalid PB8 Pareto Dash session ID')
        self._reap_dash_sessions()
        try:
            foreign_sessions = sum((1 for foreign_id, record in self._read_dash_registry().items() if foreign_id not in self._dash_sessions and self._dash_owner_matches(record)))
        except Exception as exc:
            self._log(self.SERVICE, f'Failed to inspect PB8 Pareto Dash capacity: {exc}', level='WARNING')
            foreign_sessions = self._DASH_MAX_ACTIVE_SESSIONS
        with self._dash_lock:
            if not self._dash_admission_open:
                raise self.HTTPException(status_code=503, detail='PB8 Pareto Dash is shutting down')
            if len(self._dash_sessions) + len(self._dash_pending_sessions) + foreign_sessions >= self._DASH_MAX_ACTIVE_SESSIONS:
                raise self.HTTPException(status_code=429, detail='PB8 Pareto Dash session limit reached')
            self._dash_pending_sessions[session_id] = None

    def _wait_for_dash(self, process: subprocess.Popen, port: int, timeout: float=10.0) -> None:
        deadline = self.time.monotonic() + timeout
        with self.httpx.Client(timeout=1.0, follow_redirects=False) as client:
            while self.time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError('PB8 Pareto Dash exited before it became ready')
                try:
                    if client.get(f'http://127.0.0.1:{port}/').status_code < 500:
                        return
                except self.httpx.HTTPError:
                    pass
                self.time.sleep(0.2)
        raise RuntimeError('PB8 Pareto Dash did not become ready')

    def _launch_dash_session(self, session_id: str, result_dir: Path, proxy_root: str) -> dict:
        self._reserve_dash_launch(session_id)
        stage_root = self._safe_path(self._dash_cache_root() / session_id, self._dash_cache_root())
        process = None
        session = None
        runtime_lease = None
        registered = False
        try:
            runtime = self.pb8_runtime_status()
            if not runtime.get('ready'):
                raise self.HTTPException(status_code=503, detail='PB8 runtime is not ready')
            data_root = stage_root / 'runs'
            self.ensure_private_directory_tree(self._dash_cache_root(), data_root)
            (data_root / result_dir.name).symlink_to(result_dir, target_is_directory=True)
            port = self._find_free_local_port()
            script = self.Path(runtime[self.profile.text('pb8dir')]) / 'src' / 'tools' / 'pareto_dash.py'
            if not script.is_file() or script.is_symlink():
                raise self.HTTPException(status_code=503, detail='PB8 Pareto Dash tool is unavailable')
            try:
                runtime_lease = self.acquire_master_runtime_lock(self.Path(self.PBGDIR))
            except self.MasterUpdateBusyError as exc:
                raise self.HTTPException(status_code=409, detail=str(exc)) from exc
            log_path = self._dash_log_path(session_id)
            self.ensure_private_directory(log_path.parent)
            self.rotate_managed_log_before_open(log_path, self.profile.text('optimizes_v8'))
            command = [str(runtime[self.profile.text('pb8venv')]), '-u', str(script), '--data-root', str(data_root), '--host', '127.0.0.1', '--port', str(port)]
            with open(log_path, 'w', encoding='utf-8') as log_file:
                kwargs = {'cwd': str(runtime[self.profile.text('pb8dir')]), 'stdout': log_file, 'stderr': log_file, 'env': self._optimizer_environment(str(runtime[self.profile.text('pb8venv')])), 'close_fds': True}
                if self.platform.system() == 'Windows':
                    kwargs['creationflags'] = self.subprocess.DETACHED_PROCESS | self.subprocess.CREATE_NO_WINDOW
                else:
                    kwargs['start_new_session'] = True
                with self._dash_lock:
                    if not self._dash_admission_open or session_id not in self._dash_pending_sessions:
                        raise self.HTTPException(status_code=503, detail='PB8 Pareto Dash is shutting down')
                process = self.subprocess.Popen(command, **kwargs)
            now = self.time.time()
            session = {'session_id': session_id, 'pid': process.pid, 'create_time': self.psutil.Process(process.pid).create_time(), 'owner_pid': self.os.getpid(), 'owner_create_time': self.psutil.Process(self.os.getpid()).create_time(), 'command': command, 'command_markers': command[1:], 'result_dir': str(result_dir), 'proxy_root': proxy_root, 'port': port, 'process': process, 'stage_root': str(stage_root), 'created_at': now, 'last_access': now}
            with self._dash_lock:
                if not self._dash_admission_open or session_id not in self._dash_pending_sessions:
                    raise self.HTTPException(status_code=503, detail='PB8 Pareto Dash is shutting down')
                self._dash_pending_sessions[session_id] = session
            try:
                self._wait_for_dash(process, port)
            except Exception as exc:
                raise self.HTTPException(status_code=500, detail=str(exc)) from exc
            runtime_lease.release()
            runtime_lease = None
            with self._dash_lock:
                if not self._dash_admission_open or self._dash_pending_sessions.get(session_id) is not session:
                    raise self.HTTPException(status_code=503, detail='PB8 Pareto Dash is shutting down')
                try:
                    self._persist_dash_session(session)
                except Exception as exc:
                    raise self.HTTPException(status_code=500, detail='Failed to register PB8 Pareto Dash session') from exc
                self._dash_pending_sessions.pop(session_id, None)
                self._dash_sessions[session_id] = session
                registered = True
            return session
        finally:
            if runtime_lease is not None:
                runtime_lease.release()
            if not registered:
                if session is not None:
                    self._cleanup_dash_record(session_id, session)
                else:
                    if process is not None and process.poll() is None:
                        try:
                            process.terminate()
                            process.wait(timeout=2)
                        except (self.subprocess.TimeoutExpired, self.psutil.TimeoutExpired):
                            process.kill()
                    try:
                        self._remove_dash_artifacts(session_id)
                    except Exception as exc:
                        self._log(self.SERVICE, f'Failed to clean failed PB8 Pareto Dash launch {session_id}: {exc}', level='WARNING')
                with self._dash_lock:
                    self._dash_pending_sessions.pop(session_id, None)

    def _proxy_path(self, proxy_root: str, path: str) -> str:
        suffix = str(path or '/')
        return f'{proxy_root.rstrip('/')}/{suffix.lstrip('/')}'

    def _rewrite_dash_content(self, content: bytes, content_type: str, proxy_root: str) -> bytes:
        if not content or not any((value in content_type.lower() for value in ('text/html', 'javascript', 'application/json'))):
            return content
        try:
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            return content
        root = proxy_root.rstrip('/')
        root_slash = f'{root}/'
        for key in ('requests_pathname_prefix', 'url_base_pathname', 'routes_pathname_prefix'):
            text = self.re.sub(f'("{key}"\\s*:\\s*)"(?:/|\\\\/|\\\\u002[fF])"', f'\\1"{root_slash}"', text)
            text = self.re.sub(f'("{key}"\\s*:\\s*)null', f'\\1"{root_slash}"', text)
        for old, new in {'src="/': f'src="{root_slash}', 'href="/': f'href="{root_slash}', 'action="/': f'action="{root_slash}', "src='/": f"src='{root_slash}", "href='/": f"href='{root_slash}", "action='/": f"action='{root_slash}", '"/_dash-': f'"{root}/_dash-', "'/_dash-": f"'{root}/_dash-", '"/assets/': f'"{root}/assets/', "'/assets/": f"'{root}/assets/", '"/favicon.ico"': f'"{root}/favicon.ico"', "'/favicon.ico'": f"'{root}/favicon.ico'"}.items():
            text = text.replace(old, new)
        return text.encode('utf-8')

    def _dash_request_headers(self, headers) -> dict[str, str]:
        """Forward only content-negotiation metadata, never browser credentials."""
        return {key: value for key, value in headers.items() if key.lower() in self._DASH_REQUEST_HEADERS_ALLOW}

    def _dash_response_headers(self, headers) -> dict[str, str]:
        connection_headers = {token.strip().lower() for token in str(headers.get('connection') or '').split(',') if token.strip()}
        blocked = self._DASH_RESPONSE_HEADERS_DROP | connection_headers
        return {key: value for key, value in headers.items() if key.lower() not in blocked}

    async def _wait_through_cancellation(self, future: asyncio.Future) -> bool:
        """Wait until a thread-backed future finishes, even across repeated cancellation; return if cancelled."""
        cancelled = False
        while not future.done():
            try:
                await self.asyncio.wait({future})
            except self.asyncio.CancelledError:
                cancelled = True
        return cancelled

    def _settle_autostart_launch(self, filename: str, launcher: Callable[[str, dict | None, bool], dict]) -> None:
        """Launch and persist claim ownership in one thread, independently of asyncio cancellation."""
        try:
            try:
                record = launcher(filename, None, True)
                self.publish_autostart_process(self.profile.text('v8'), filename, record['pid'], record['create_time'], [str(self.Path(self.PBGDIR) / self.profile.text('pb8_optimize_runner.py')), str(self._launch_config_file(filename).resolve())])
            except Exception as exc:
                self.release_autostart(self.profile.text('v8'), filename)
                transient = self._record_launch_failure(filename, exc)
                self._log(self.SERVICE, f'PB8 automatic optimize launch failed: {exc}', level='WARNING' if transient else 'ERROR')
        except Exception as exc:
            self._log(self.SERVICE, f'Failed to settle PB8 autostart claim for {filename}: {exc}', level='ERROR', meta={'traceback': self.traceback.format_exc()})

    async def _queue_ws_snapshot(self) -> dict:
        """Share one bounded queue scan across all Optimize WebSocket clients."""
        async with self._ws_snapshot_lock:
            if self._ws_snapshot is not None and self.time.monotonic() < self._ws_snapshot_until:
                return self._ws_snapshot
            scan = self.asyncio.create_task(self.asyncio.to_thread(lambda: {'type': 'queue_update', 'items': [row for row in self._load_queue() if not row.get('loop_id')], 'settings': self.load_ini_section(self._QUEUE_SETTINGS_SECTION)}))
            try:
                payload = await self.asyncio.shield(scan)
            except self.asyncio.CancelledError:
                await self.asyncio.gather(scan, return_exceptions=True)
                raise
            self._ws_snapshot = payload
            active = any((item.get('status') in ('running', 'optimizing') for item in payload['items']))
            self._ws_snapshot_until = self.time.monotonic() + (3.0 if active else 8.0)
            return payload

    def startup(self) -> None:
        """Start only the API-owned PB8 optimize controller."""
        with self._dash_lock:
            self._dash_admission_open = False
        self._recover_dash_registry()
        with self._dash_lock:
            self._dash_admission_open = True
        self._reconcile_queue_artifacts()
        self._worker.start()
        self.prepare_pb8_migration_helper_startup()

        async def warm_migration_helper() -> None:
            try:
                await self.asyncio.to_thread(self.start_pb8_migration_helper)
            except self.PB8ConfigurationError as exc:
                self._log(self.SERVICE, f'PB8 migration helper prewarm unavailable: {exc}', level='WARNING')
        if self._migration_helper_warmup_task is None or self._migration_helper_warmup_task.done():
            self._migration_helper_warmup_task = self.asyncio.create_task(warm_migration_helper(), name=self.profile.text('pb8-migration-helper-warmup'))
        with self._backtest_count_scans_lock:
            self._backtest_count_scan_loop = self.asyncio.get_running_loop()
            self._backtest_count_scans_accepting = True

    async def shutdown(self) -> None:
        """Stop the controller without terminating detached optimize jobs."""
        with self._dash_lock:
            self._dash_admission_open = False
        await self.asyncio.to_thread(self._shutdown_backtest_scans)
        await self._worker.stop()
        await self._worker.drain_autostart_launches()
        await self.asyncio.to_thread(self._stop_all_dash_sessions)
        await self.asyncio.to_thread(self.shutdown_pb8_ohlcv_start_date_jobs)
        await self.asyncio.to_thread(self._shutdown_active_evaluation_scans)
        await self.asyncio.to_thread(self.interrupt_pb8_migration_helper)
        if self._migration_helper_warmup_task is not None:
            await self.asyncio.gather(self._migration_helper_warmup_task, return_exceptions=True)
            self._migration_helper_warmup_task = None
        await self.asyncio.to_thread(self.shutdown_pb8_migration_helper)

    async def ws_optimize(self, websocket: WebSocket) -> None:
        """Push changed state only, with an idle backoff and a shared scan cache."""
        if await self.authenticate_websocket(websocket) is None:
            return
        self._ws_clients.add(websocket)
        last_payload = None
        try:
            while True:
                payload = await self._queue_ws_snapshot()
                if payload != last_payload:
                    await websocket.send_json(payload)
                    last_payload = payload
                active = any((item.get('status') in ('running', 'optimizing') for item in payload['items']))
                try:
                    await self.asyncio.wait_for(websocket.receive_text(), timeout=3.0 if active else 8.0)
                except self.asyncio.TimeoutError:
                    pass
        except (self.WebSocketDisconnect, RuntimeError):
            pass
        finally:
            self._ws_clients.discard(websocket)
            if not self._ws_clients:
                self._ws_snapshot = None
                self._ws_snapshot_until = 0.0

    def main_page(self, request: Request, session: SessionToken=Depends(require_auth)) -> HTMLResponse:
        """Render PB8 through the shared optimize page without exposing the session cookie."""
        html_path = self.Path(self.PBGDIR) / 'frontend' / 'v7_optimize.html'
        if not html_path.is_file():
            raise self.HTTPException(status_code=404, detail='v7_optimize.html not found')
        html = html_path.read_text(encoding='utf-8')
        try:
            limits = (self.get_pb8_optimize_metadata(include_backend_contract=False).get('limits') or {}) if self.version == 'v8' else {}
        except self.PB8ConfigurationError as exc:
            self._log(self.SERVICE, f'Rendering PB8 Optimize without unavailable runtime metadata: {exc}', level='WARNING')
            limits = {}
        html = self.render_page_urls(request, html, self.profile.text('/api/optimize-v8'))
        replacements = {'%%LIMITS_META%%': self.json.dumps(limits), '"%%VERSION%%"': self.script_json(self.PBGUI_VERSION), '%%VERSION%%': self.PBGUI_VERSION, '"%%SERIAL%%"': self.script_json(self.PBGUI_SERIAL), '%%SERIAL%%': self.PBGUI_SERIAL, '%%OPTIMIZE_VERSION%%': self.profile.text('v8'), '%%OPTIMIZE_LABEL%%': self.profile.text('V8'), '%%OPTIMIZE_NAV_TITLE%%': self.profile.text('PBv8 OPTIMIZE'), '%%OPTIMIZE_NAV_CURRENT%%': self.profile.text('v8_optimize'), '%%BACKTEST_VERSION%%': self.profile.text('v8')}
        for old, new in replacements.items():
            html = html.replace(old, new)
        nav_js = self.Path(self.PBGDIR) / 'frontend' / 'pbgui_nav.js'
        html = html.replace('%%NAV_HASH%%', str(int(nav_js.stat().st_mtime)) if nav_js.is_file() else self.PBGUI_VERSION)
        return self.HTMLResponse(content=html, headers={'Cache-Control': 'no-store'})

    def runtime_status(self, session: SessionToken=Depends(require_auth)) -> dict:
        return self.pb8_runtime_status()

    def _load_optimizer_metadata(self, *, include_backend_contract: bool=True) -> dict:
        """Load metadata with consistent runtime diagnostics for optimizer routes."""
        try:
            return self.get_pb8_optimize_metadata(include_backend_contract=include_backend_contract)
        except self.PB8ConfigurationError as exc:
            detail = str(exc)
            if 'optimize bound ' in detail and ' does not map to bot.' in detail:
                detail = f'{self.label} optimizer metadata is incompatible with the installed Rust extension. Run Update {self.label} to rebuild the {self.label} runtime.'
                self._log(self.SERVICE, f'Loading {self.label} optimize metadata failed: {exc}', level='WARNING')
                raise self.HTTPException(status_code=503, detail=detail) from exc
            raise self._configuration_error(f'Loading {self.label} optimize metadata', exc, 503) from exc

    def get_metadata(self, session: SessionToken=Depends(require_auth)) -> dict:
        return self._load_optimizer_metadata()

    def get_scenario_templates(self, session: SessionToken=Depends(require_auth)) -> dict:
        """Return deterministic PB8 scenario template descriptors."""
        return {'templates': self.list_scenario_templates()}

    def preview_scenario_template(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        """Validate and preview a deterministic PB8 scenario template."""
        try:
            return self.generate_scenario_template(body)
        except self.ScenarioTemplateError as exc:
            self._log(self.SERVICE, f'Rejected {self.label} scenario template preview: {exc}', level='WARNING')
            raise self.HTTPException(status_code=422, detail=str(exc)) from exc

    def scenario_chart_sources(self, exchange: str, session: SessionToken=Depends(require_auth)) -> dict:
        """List local reference candle datasets without fetching exchange data."""
        from scenario_chart import sources
        try:
            return {'sources': sources(exchange)}
        except (ValueError, OSError) as exc:
            self._log(self.SERVICE, 'Scenario chart source lookup failed', level='WARNING')
            raise self.HTTPException(status_code=422, detail='Local candle sources unavailable') from exc

    def scenario_chart_data(self, exchange: str, dataset: str, coin: str, start: str, end: str, session: SessionToken=Depends(require_auth)) -> dict:
        """Return bounded daily local candles for a scenario reference chart."""
        from scenario_chart import daily_chart
        try:
            return daily_chart(exchange, dataset, coin, start, end)
        except (ValueError, OSError) as exc:
            self._log(self.SERVICE, 'Scenario chart request rejected', level='WARNING')
            raise self.HTTPException(status_code=422, detail='Invalid or unavailable local candle range') from exc

    def get_pbgui_data_path(self, session: SessionToken=Depends(require_auth)) -> dict:
        from market_data import get_market_data_root_dir
        return {'path': str(get_market_data_root_dir())}

    async def get_ohlcv_preflight(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        config = body.get('config') if isinstance(body, dict) else None
        if not isinstance(config, dict):
            raise self.HTTPException(status_code=400, detail='config must be an object')
        try:
            return await self.build_pb8_ohlcv_preflight(config)
        except self.HTTPException:
            raise
        except Exception as exc:
            detail = str(exc).strip() or exc.__class__.__name__
            self._log(self.SERVICE, f'PB8 optimize OHLCV readiness failed: {detail}', level='WARNING', meta={'traceback': self.traceback.format_exc()})
            status_code = 503 if isinstance(exc, self.PB8OhlcvUnavailableError) else 422
            raise self.HTTPException(status_code=status_code, detail=detail) from exc

    def start_ohlcv_start_date_lookup(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        """Start a bounded PB8 OHLCV inception lookup job."""
        config = body.get('config') if isinstance(body, dict) else None
        if not isinstance(config, dict):
            raise self.HTTPException(status_code=400, detail='config must be an object')
        try:
            return self.start_pb8_ohlcv_start_date_job(config)
        except self.HTTPException:
            raise
        except Exception as exc:
            detail = self.safe_start_date_error(exc)
            self._log(self.SERVICE, f'PB8 optimize OHLCV start-date lookup failed: {detail}', level='WARNING')
            status_code = 503 if isinstance(exc, self.PB8OhlcvUnavailableError) else 422
            raise self.HTTPException(status_code=status_code, detail=detail) from exc

    def get_ohlcv_start_date_lookup(self, job_id: str, session: SessionToken=Depends(require_auth)) -> dict:
        """Return progress or the result for one PB8 OHLCV inception lookup."""
        payload = self.get_pb8_ohlcv_start_date_job(job_id)
        if payload is None:
            raise self.HTTPException(status_code=404, detail='OHLCV start-date lookup job not found')
        return payload

    def stop_ohlcv_start_date_lookup(self, job_id: str, session: SessionToken=Depends(require_auth)) -> dict:
        """Stop one API-owned PB8 OHLCV inception lookup."""
        payload = self.stop_pb8_ohlcv_start_date_job(job_id)
        if payload is None:
            raise self.HTTPException(status_code=404, detail='OHLCV start-date lookup job not found')
        return payload

    def start_ohlcv_preload(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        config = body.get('config') if isinstance(body, dict) else None
        if not isinstance(config, dict):
            raise self.HTTPException(status_code=400, detail='config must be an object')
        try:
            return self.start_pb8_ohlcv_preload_job(config)
        except self.HTTPException:
            raise
        except Exception as exc:
            detail = str(exc).strip() or exc.__class__.__name__
            self._log(self.SERVICE, f'PB8 optimize OHLCV preload failed: {detail}', level='WARNING', meta={'traceback': self.traceback.format_exc()})
            status_code = 503 if isinstance(exc, self.PB8OhlcvUnavailableError) else 422
            raise self.HTTPException(status_code=status_code, detail=detail) from exc

    def get_ohlcv_preload(self, job_id: str, session: SessionToken=Depends(require_auth)) -> dict:
        payload = self.get_pb8_ohlcv_preload_job(job_id)
        if payload is None:
            raise self.HTTPException(status_code=404, detail='OHLCV preload job not found')
        return payload

    def stop_ohlcv_preload(self, job_id: str, session: SessionToken=Depends(require_auth)) -> dict:
        payload = self.stop_pb8_ohlcv_preload_job(job_id)
        if payload is None:
            raise self.HTTPException(status_code=404, detail='OHLCV preload job not found')
        return payload

    def get_settings(self, session: SessionToken=Depends(require_auth)) -> dict:
        settings = self.load_ini_section(self._QUEUE_SETTINGS_SECTION)
        metadata = self._load_optimizer_metadata(include_backend_contract=False)
        return {'autostart': str(settings.get('autostart', 'False')).lower() == 'true', 'cpu': self._normalize_autostart_cpu(settings.get('cpu', '1')), 'cpu_override': str(settings.get('cpu_override', 'True')).lower() == 'true', 'use_pbgui_market_data': str(settings.get('use_pbgui_market_data', 'False')).lower() == 'true', 'cpu_max': self.multiprocessing.cpu_count(), 'host_cpu_count': self.multiprocessing.cpu_count(), 'optimize_backend_options': metadata.get('backends') or [], 'optimize_backend_default': (metadata.get('optimize_defaults') or {}).get('backend', 'pymoo'), 'pymoo_algorithm_options': (metadata.get('pymoo') or {}).get('algorithms') or [], 'pymoo_ref_dir_method_options': (metadata.get('pymoo') or {}).get('ref_dir_methods') or [], 'optimize_defaults': metadata.get('optimize_defaults') or {}, 'strategies': metadata.get('strategies') or [], 'hsl_signal_modes': ['coin', 'pside', 'unified'], 'exchange_options': self.get_pb8_exchange_metadata()['optimize']}

    def update_settings(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        updates = {}
        for key in ('autostart', 'cpu_override', 'use_pbgui_market_data'):
            if key not in body:
                continue
            if type(body[key]) is not bool:
                raise self.HTTPException(status_code=422, detail=f'{key} must be a boolean')
            updates[key] = str(body[key])
        if 'cpu' in body:
            if type(body['cpu']) is not int:
                raise self.HTTPException(status_code=422, detail='cpu must be an integer')
            updates['cpu'] = str(self._normalize_autostart_cpu(body['cpu']))
        if updates:
            self.save_ini_section(self._QUEUE_SETTINGS_SECTION, updates)
        return {'ok': True}

    def get_bot_params(self, session: SessionToken=Depends(require_auth)) -> dict:
        metadata = self._load_optimizer_metadata(include_backend_contract=False)
        return {'params': [{'key': path.removeprefix('bot.long.')} for path in metadata.get('bot_parameter_paths') or [] if path.startswith('bot.long.')]}

    def new_config(self, session: SessionToken=Depends(require_auth)) -> dict:
        return {'config': self._load_optimizer_metadata(include_backend_contract=False)['template'], 'param_status': {}}

    def prepare_config(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        config = body.get('config') if isinstance(body, dict) else None
        if not isinstance(config, dict):
            raise self.HTTPException(status_code=400, detail='config must be an object')
        try:
            return {'config': self.prepare_pb8_config(config), 'param_status': {}}
        except self.PB8ConfigurationError as exc:
            raise self._configuration_error(f'Preparing {self.label} optimize config', exc) from exc

    def list_configs(self, session: SessionToken=Depends(require_auth), include_result_summary: bool=True, include_backtest_counts: bool=True) -> dict:
        """Preserve default callers; the UI explicitly requests scan-free summaries."""
        configs = []
        result_summaries = []
        if include_result_summary:
            with self._result_lock():
                result_summaries = self._list_results()
        latest_result_by_name = {}
        for result in result_summaries:
            latest_result_by_name.setdefault(result['name'], result)
        root = self._configs_dir()
        count_root = self._COUNT_ROOT_UNSET
        if root.is_dir() and (not root.is_symlink()):
            for directory in sorted(root.iterdir()):
                if directory.name.startswith('.') or not directory.is_dir() or directory.is_symlink():
                    continue
                try:
                    name = self._validate_name(directory.name)
                    path = self._safe_path(directory / self._CONFIG_FILENAME, root)
                    if not directory.is_dir() or not path.is_file():
                        continue
                    summary, config, identity = self._config_summary(name, path)
                    count = None
                    if include_backtest_counts:
                        managed = self.PurePath(str(config['backtest'].get('base_dir') or '').strip()).parts == ('backtests', 'pbgui', name)
                        if managed and count_root is self._COUNT_ROOT_UNSET:
                            try:
                                count_root = self._backtests_root()
                            except Exception as exc:
                                count_root = None
                                self._log(self.SERVICE, f'Unable to resolve PB8 backtest root: {exc}', level='WARNING')
                        count = self._managed_backtest_count(name, config, count_root, config_identity=identity)
                    result_summary = latest_result_by_name.get(name) or {}
                    summary.update(backtest_count=count, backtest_count_status=('known' if count is not None else 'unknown') if include_backtest_counts else 'pending', result_mode=result_summary.get('mode', 'unknown'), scenario_count=result_summary.get('scenario_count', 0))
                    configs.append(summary)
                except Exception as exc:
                    self._log(self.SERVICE, f'Failed to list {self.label} optimize config {directory.name}: {exc}', level='WARNING')
        return {'configs': configs}

    def config_backtest_counts(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        """Resolve at most sixteen validated names in a synchronous worker request."""
        names = body.get('names')
        if not isinstance(names, list) or not 1 <= len(names) <= self._BACKTEST_COUNT_BATCH_MAX_NAMES or (not all((isinstance(name, str) for name in names))) or (set(body) != {'names'}):
            raise self.HTTPException(status_code=422, detail='Supply only 1 to 16 config names')
        names = list(dict.fromkeys((self._validate_name(name) for name in names)))
        try:
            root = self._backtests_root()
        except Exception as exc:
            self._log(self.SERVICE, f'Unable to resolve PB8 backtest root: {exc}', level='WARNING')
            root = None
        rows = []
        for name in names:
            count = None
            status = 'unknown'
            try:
                path = self._config_file(name)
                if path.is_file() and (not path.is_symlink()):
                    _summary, config, identity = self._config_summary(name, path)
                    status, count = self._managed_backtest_count_status(name, config, root, config_identity=identity)
            except (OSError, RuntimeError, ValueError) as exc:
                self._log(self.SERVICE, f'Unable to load count config {name}: {exc}', level='WARNING')
            rows.append({'name': name, 'backtest_count': count, 'backtest_count_status': status})
        return {'configs': rows}

    def get_config(self, name: str, session: SessionToken=Depends(require_auth)) -> dict:
        path = self._config_file(name)
        try:
            with self._config_lock():
                if not path.is_file() or path.is_symlink():
                    raise self.HTTPException(status_code=404, detail=f"Config '{name}' not found")
                editor_payload = self.load_pb8_editor_config(path, loader=self.load_pb8_config, report_changes=True)
                config = editor_payload['config']
                return {**editor_payload, 'name': name, 'config': config, 'param_status': editor_payload.get('param_status', {}), 'override_configs': self._load_override_payloads(config, path.parent)}
        except self.PB8ConfigurationError as exc:
            raise self._configuration_error(f'Loading {self.label} optimize config {name}', exc) from exc

    def preview_hsl_migration(self, name: str, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        """Preview the native HSL migration; the existing config remains untouched."""
        path = self._config_file(self._validate_name(name))
        with self._config_lock():
            if not path.is_file() or path.is_symlink() or path.parent.is_symlink():
                raise self.HTTPException(status_code=404, detail='Config not found')
            try:
                editor_payload = self.load_pb8_editor_config(path, loader=self.load_pb8_config)
                self._load_override_payloads(editor_payload['config'], path.parent)
                return self.preview_pb8_hsl_migration(path, body)
            except self.PB8ConfigurationError as exc:
                raise self._configuration_error(f'Previewing {self.label} HSL migration', exc) from exc

    def save_config(self, name: str, body: dict, create_only: bool=False, session: SessionToken=Depends(require_auth)) -> dict:
        name = self._validate_name(name)
        config = body.get('config') if isinstance(body, dict) and 'config' in body else body
        override_payloads = body.get('override_configs') if isinstance(body, dict) and 'config' in body else None
        migration_report = body.get('migration_report') if isinstance(body, dict) and 'config' in body else None
        if not isinstance(config, dict):
            raise self.HTTPException(status_code=400, detail='config must be an object')
        if migration_report is not None and (not isinstance(migration_report, dict)):
            raise self.HTTPException(status_code=422, detail='migration_report must be an object')
        try:
            with self._config_lock():
                prepared = self._save_config_bundle(name, config, create_only=create_only, override_payloads=override_payloads, migration_report=migration_report, preserve_hsl_runtime_overrides=migration_report is not None)
            return {'ok': True, 'name': name, 'config': prepared, 'override_configs': self._load_override_payloads(prepared, self._config_dir(name))}
        except self.PB8ConfigurationError as exc:
            raise self._configuration_error(f'Saving {self.label} optimize config {name}', exc) from exc

    def duplicate_config(self, name: str, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        source = self._config_file(name)
        new_name = self._validate_name(str((body or {}).get('new_name') or ''))
        with self._config_lock():
            if not source.is_file() or source.is_symlink():
                raise self.HTTPException(status_code=404, detail=f"Config '{name}' not found")
            prepared = self._save_config_bundle(new_name, self.load_pb8_config(source), create_only=True, source_dir=source.parent)
        return {'ok': True, 'name': new_name, 'config': prepared}

    def delete_config(self, name: str, session: SessionToken=Depends(require_auth)) -> dict:
        target = self._config_dir(name)
        with self._config_lock():
            if not target.is_dir() or target.is_symlink():
                raise self.HTTPException(status_code=404, detail=f"Config '{name}' not found")
            self.rmtree(target)
        return {'ok': True}

    def get_migration_draft(self, draft_id: str, session: SessionToken=Depends(require_auth)) -> dict:
        """Return one unexpired unsaved V7-to-V8 Optimize preview."""
        with self._migration_draft_lock:
            self._clean_migration_drafts()
            entry = self._migration_drafts.get(str(draft_id or ''))
            if entry is None:
                raise self.HTTPException(status_code=404, detail='Migration draft not found or expired')
            return self.copy.deepcopy(entry[1])

    def _optimize_migration_review_report(self, report: dict) -> dict:
        """Keep only migration findings that can affect an Optimize evaluation."""
        contextual = self.copy.deepcopy(report)
        official_findings = {'status': report.get('status'), 'manual_review_fields': list(report.get('manual_review_fields') or []), 'dropped_unsupported_fields': list(report.get('dropped_unsupported_fields') or [])}
        for key in ('manual_review_fields', 'dropped_unsupported_fields'):
            contextual[key] = [item for item in report.get(key) or [] if not str(item or '').strip().startswith('live.')]
        unresolved = bool(contextual.get('manual_review_fields') or contextual.get('dropped_unsupported_fields'))
        contextual['manual_review_required'] = unresolved
        if unresolved:
            contextual['status'] = 'manual_review_required'
        elif contextual.get('pbgui_post_migration_adjustments'):
            contextual['status'] = 'ok_with_adjustments'
        elif contextual.get('status') in {'manual_review_required', 'unsafe_manual_review_output_written'}:
            contextual['status'] = 'ok'
        if contextual.get('manual_review_fields') != official_findings['manual_review_fields'] or contextual.get('dropped_unsupported_fields') != official_findings['dropped_unsupported_fields']:
            contextual['official_review'] = official_findings
        return contextual

    def migrate_v7(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        source_path = str((body or {}).get('source_path') or '').strip()
        if source_path:
            source = self._resolve_v7_pareto_config(source_path)
            source_name = source.stem
        else:
            source_name = self._validate_name(str((body or {}).get('source_name') or ''))
            source = self._safe_path(self._v7_configs_dir() / f'{source_name}.json', self._v7_configs_dir())
            if not source.is_file() or source.is_symlink():
                raise self.HTTPException(status_code=404, detail=f"V7 optimize config '{source_name}' not found")
        target_name = self._validate_name(str((body or {}).get('target_name') or f'{source_name}{self.profile.text('_v8')}'))
        with self._config_lock():
            stage = self._safe_path(self._configs_dir() / f'.migrate-{self.uuid.uuid4().hex}', self._configs_dir())
            self.ensure_private_directory(stage)
            try:
                try:
                    migration_payload, source_adjustments = self.sanitize_optimize_migration_source(self._read_json(source))
                    migration_payload, legacy_churn_plan, churn_adjustments = self.extract_legacy_churn_gate(migration_payload)
                    source_adjustments.extend(churn_adjustments)
                except ValueError as exc:
                    raise self.HTTPException(status_code=422, detail=str(exc)) from exc
                migration_source = stage / '.source-v7.json'
                self._write_json(migration_source, migration_payload)
                result = self.migrate_pb7_config(migration_source, stage / self._CONFIG_FILENAME, allow_manual_review_output=True)
                official_report = result.get('report') if isinstance(result.get('report'), dict) else {}
                optimize_metadata = result.get('optimize_metadata') if isinstance(result.get('optimize_metadata'), dict) else None
                official_report = self.copy.deepcopy(official_report)
                if source_adjustments:
                    official_report['pbgui_source_adjustments'] = source_adjustments
                report = self._optimize_migration_review_report(official_report)
                config = result.get('config')
                if isinstance(config, dict) and legacy_churn_plan is not None:
                    config = self.prepare_pb8_config(self.apply_legacy_churn_gate(config, legacy_churn_plan))
                if isinstance(config, dict):
                    config, official_report = self.postprocess_v7_migration(migration_payload, config, official_report)
                    report = self._optimize_migration_review_report(official_report)
                unresolved = report.get('manual_review_fields') or report.get('dropped_unsupported_fields')
                if not official_report.get('output_written') or not isinstance(config, dict) or unresolved:
                    fields = report.get('manual_review_fields') or report.get('dropped_unsupported_fields') or []
                    message = 'Migration requires manual review'
                    if fields:
                        message += ': ' + '; '.join((str(item) for item in fields[:5]))
                    raise self.HTTPException(status_code=422, detail={'code': 'migration_manual_review', 'message': message, 'report': report})
                normalized = self._normalize_config(config, target_name, preserve_hsl_runtime_overrides=True)
                enable_overrides = (normalized.get('optimize') or {}).get('enable_overrides')
                if enable_overrides:
                    self._validate_optimizer_overrides(normalized, base_config_path=str(stage / self._CONFIG_FILENAME))
                self._validate_forager_optimize_search_space(normalized)
                with self._migration_draft_lock:
                    self._clean_migration_drafts(reserve_slot=True)
                    draft_id = self.secrets.token_urlsafe(16)
                    self._migration_drafts[draft_id] = (self.time.time(), {'config': self.copy.deepcopy(normalized), 'override_configs': {}, 'migration_report': self.copy.deepcopy(report), 'optimize_metadata': self.copy.deepcopy(optimize_metadata)})
                return {'ok': True, 'name': target_name, 'draft_id': draft_id, 'editor': 'optimize', 'report': report}
            finally:
                self.rmtree(stage, ignore_errors=True)

    def get_queue(self, session: SessionToken=Depends(require_auth)) -> dict:
        return {'items': [row for row in self._load_queue() if not row.get('loop_id')]}

    def reorder_queue(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        filenames = (body or {}).get('filenames')
        if not isinstance(filenames, list):
            raise self.HTTPException(status_code=400, detail='filenames must be a list')
        normalized = [self._validate_name(str(item)) for item in filenames]
        if len(set(normalized)) != len(normalized):
            raise self.HTTPException(status_code=400, detail='filenames must not contain duplicates')
        with self._queue_lock():
            self._recover_pending_reorder_unlocked()
            existing = {item['filename'] for item in self._load_queue()}
            if set(normalized) != existing:
                raise self.HTTPException(status_code=400, detail='filenames must include all queue items exactly once')
            self.ensure_private_directory_tree(self._queue_dir(), self._queue_dir() / 'state')
            self._write_json(self._reorder_file(), {'filenames': normalized, 'created_at': self.time.time()})
            for index, filename in enumerate(normalized):
                data = self._read_json(self._queue_file(filename))
                data['order'] = index
                self._write_json(self._queue_file(filename), data)
            self._reorder_file().unlink(missing_ok=True)
        return {'ok': True, 'count': len(normalized)}

    def add_to_queue(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        name = self._validate_name(str((body or {}).get('name') or ''))
        config = (body or {}).get('config')
        override_payloads = (body or {}).get('override_configs') if isinstance(config, dict) else None
        try:
            with self._config_lock():
                if isinstance(config, dict):
                    prepared = self._save_config_bundle(name, config, override_payloads=override_payloads)
                else:
                    path = self._config_file(name)
                    if not path.is_file() or path.is_symlink():
                        raise self.HTTPException(status_code=404, detail=f"Config '{name}' not found")
                    prepared = self.load_pb8_config(path)
                cloud = (prepared.get('pbgui') or {}).get('execution') == 'vast'
                if cloud:
                    from vast_jobs import digest
                    source_hash = digest(self._config_file(name))
                else:
                    self._validate_optimizer_overrides(prepared, base_config_path=str(self._config_file(name)))
                    self._validate_optimize_backend(prepared, base_config_path=str(self._config_file(name)))
                    overrides = self._load_override_payloads(prepared, self._config_dir(name))
            if cloud:
                from vast_jobs import JobStore
                from vast_provider import VastError
                try:
                    opt = prepared.get('optimize') or {}
                    workers = (opt.get('gpu') or {}).get('exact_workers') or opt.get('n_cpus') or 4
                    job = JobStore().prepare(name, prepared, source_hash, self.Path(self.PBGDIR) / 'data/ohlcv', self.Path(self.PBGDIR) / 'data/coindata', self._results_root(), opt.get('iters', 512), workers, False)
                    return {'ok': True, 'filename': job['id'], 'execution': 'vast'}
                except VastError as exc:
                    self._log(self.SERVICE, str(exc), level='WARNING')
                    raise self.HTTPException(status_code=exc.status, detail=str(exc)) from None
            options = self._validate_launch_options((body or {}).get('launch_options') or self._runtime_options_from_config(prepared))
        except self.PB8ConfigurationError as exc:
            raise self._configuration_error(f'Queueing {self.label} optimize config {name}', exc) from exc
        return self._create_queue_record(name, prepared, options, overrides)

    def _queue_record_for_operation_unlocked(self, operation_id: str) -> dict | None:
        """Return an existing queue result for one persistent idempotency key."""
        if not operation_id:
            return None
        for path in self._queue_dir().glob('*.json'):
            try:
                data = self._read_json(path)
            except RuntimeError:
                continue
            if data.get('operation_id') == operation_id:
                return {'ok': True, 'filename': str(data.get('filename') or path.stem), 'idempotent': True}
        return None

    def _queue_identity(self, prepared: dict, filename: str) -> dict:
        """Let native profiles bind provenance before a queue record becomes visible."""
        return {}

    def _strategy_kind(self, config: dict) -> str:
        """Read the legacy strategy identity through a generation-specific seam."""
        return (config.get('live') or {}).get('strategy_kind', '')

    def _listed_strategy_kind(self, config) -> str:
        """Read a result's strategy for listings; malformed records show none."""
        if not isinstance(config, dict):
            return ''
        try:
            value = self._strategy_kind(config)
        except (AttributeError, TypeError):
            return ''
        return value if isinstance(value, str) else ''

    def _verify_queue_identity(self, record: dict, runtime: dict, snapshot: Path) -> None:
        """Retain legacy launch behavior; native profiles verify immutable provenance."""

    def _execution_config(self, config: dict) -> dict:
        """Allow native profiles to remove GUI-only metadata from execution copies."""
        return config

    def _runner_identity(self, runtime: dict, filename: str) -> dict:
        """Allow native profiles to bind independent runners to their launch snapshot."""
        return {}

    def _runner_python_flags(self) -> list[str]:
        """Retain legacy invocation while allowing isolated native Python launchers."""
        return []

    def _optimizer_environment(self, interpreter: str) -> dict:
        """Retain the existing environment for legacy optimizer launches."""
        return {**self.os.environ, 'PASSIVBOT_GPU_PROFILE':'1',
                'PATH':str(self.Path(interpreter).parent) + self.os.pathsep + self.os.environ.get('PATH', '')}

    def _optimizer_command(self, command: list[str], environment: dict) -> list[str]:
        """Allow native profiles to clear inherited systemd-manager environment variables."""
        return command

    def _create_queue_record(self, name: str, prepared: dict, options: dict, override_payloads: dict[str, dict] | None=None, operation_id: str | None=None) -> dict:
        """Publish one queue record and remove partial artifacts if persistence fails."""
        self._validate_options_for_config(options, prepared)
        if operation_id is not None:
            operation_id = self._validate_name(operation_id)
            with self._queue_lock():
                existing = self._queue_record_for_operation_unlocked(operation_id)
                if existing is not None:
                    return existing
        filename = str(self.uuid.uuid4())
        stage = None
        try:
            if override_payloads is None:
                with self._config_lock():
                    override_payloads = self._load_override_payloads(prepared, self._config_dir(name))
            stage = self._stage_snapshot_bundle(filename, prepared, override_payloads)
            with self._queue_lock():
                if operation_id:
                    existing = self._queue_record_for_operation_unlocked(operation_id)
                    if existing is not None:
                        self.rmtree(stage, ignore_errors=True)
                        stage = None
                        return existing
                self._publish_snapshot_bundle(stage, self._snapshot_dir(filename))
                stage = None
                current = self._load_queue()
                order = min((item['order'] for item in current if isinstance(item.get('order'), int)), default=0) - 1
                backtest = prepared.get('backtest') if isinstance(prepared.get('backtest'), dict) else {}
                data = {'filename': filename, 'name': name, 'config_path': str(self._config_file(name)), 'snapshot_path': str(self._snapshot_file(filename)), 'exchange': backtest.get('exchanges') or [], 'launch_options': options, 'order': order}
                if operation_id:
                    data['operation_id'] = operation_id
                data.update(self._queue_identity(prepared, filename))
                self._write_json(self._queue_file(filename), data)
        except Exception:
            if stage is not None:
                self.rmtree(stage, ignore_errors=True)
            with self._queue_lock():
                self._queue_file(filename).unlink(missing_ok=True)
                self.rmtree(self._snapshot_dir(filename), ignore_errors=True)
                self.rmtree(self._launch_dir(filename), ignore_errors=True)
            raise
        return {'ok': True, 'filename': filename}

    def start_queue_item(self, filename: str, body: dict | None=None, session: SessionToken=Depends(require_auth)) -> dict:
        options = (body or {}).get('launch_options') if isinstance(body, dict) else None
        try:
            record = self._worker.launch(filename, options, False)
        except Exception as exc:
            self._record_launch_failure(filename, exc)
            raise
        return {'ok': True, 'pid': record['pid']}

    def _reset_queue_item(self, filename: str, options: dict) -> dict:
        filename = self._validate_name(filename)
        with self._queue_lock():
            path = self._queue_file(filename)
            if not path.is_file():
                raise self.HTTPException(status_code=404, detail='Queue item not found')
            data = self._read_json(path)
            options = {**options, 'convergence_config': options.get('convergence_config', (data.get('launch_options') or {}).get('convergence_config', {}))}
            validated_options = self._validate_launch_options(options)
            snapshot = self._snapshot_file(filename)
            if not snapshot.is_file() or snapshot.is_symlink():
                raise self.HTTPException(status_code=422, detail='Queue config snapshot is missing')
            self._validate_options_for_config(validated_options, self._read_json(snapshot))
            if validated_options['mode'] == 'checkpoint_resume':
                self._preflight_checkpoint_resume(self._read_json(snapshot), self.Path(validated_options['source']))
            self._terminate_verified(filename)
            data['launch_options'] = validated_options
            for artifact in (self._pid_file(filename), self._state_file(filename), self._ready_file(filename), self._state_file(filename).with_suffix('.convergence.json'), self._state_file(filename).with_suffix('.convergence.forced.json')):
                artifact.unlink(missing_ok=True)
            self.rmtree(self._launch_dir(filename), ignore_errors=True)
            data.pop('started_at', None)
            data.pop('status_override', None)
            data.pop('automatic', None)
            data.pop('error_code', None)
            data.pop('error_reason', None)
            data['launch_message'] = {'fresh': 'Fresh requeue requested', 'pareto_seed': 'Pareto seed selected for next launch', 'checkpoint_resume': 'Checkpoint resume selected for next launch'}[data['launch_options']['mode']]
            self._write_json(path, data)
        return {'ok': True, 'mode': data['launch_options']['mode']}

    def requeue_fresh(self, filename: str, session: SessionToken=Depends(require_auth)) -> dict:
        return self._reset_queue_item(filename, {'mode': 'fresh'})

    def continue_from_pareto(self, filename: str, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        return self._reset_queue_item(filename, {**(body or {}), 'mode': 'pareto_seed', 'source': (body or {}).get('source')})

    def resume_checkpoint(self, filename: str, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        return self._reset_queue_item(filename, {**(body or {}), 'mode': 'checkpoint_resume', 'source': (body or {}).get('source')})

    def stop_queue_item(self, filename: str, session: SessionToken=Depends(require_auth)) -> dict:
        with self._queue_lock():
            path = self._queue_file(filename)
            if not path.is_file():
                raise self.HTTPException(status_code=404, detail='Queue item not found')
            self._terminate_verified(filename)
            data = self._read_json(path)
            reason = 'Stopped by user; requeue this item to run it again.'
            data['status_override'] = 'error'
            data['error_code'] = 'stopped'
            data['error_reason'] = reason
            data['launch_message'] = reason
            self._write_json(path, data)
            self._append_queue_log(filename, reason)
        return {'ok': True}

    def get_queue_config(self, filename: str, session: SessionToken=Depends(require_auth)) -> dict:
        path = self._snapshot_file(filename)
        if not path.is_file() or path.is_symlink():
            raise self.HTTPException(status_code=404, detail='Queue config snapshot not found')
        config = self._read_json(path)
        return {'name': self._read_json(self._queue_file(filename)).get('name', filename), 'config': config, 'param_status': {}, 'override_configs': self._load_override_payloads(config, path.parent)}

    def repair_queue_config(self, filename: str, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        name = self._validate_name(str((body or {}).get('name') or ''))
        with self._config_lock():
            config_path = self._config_file(name)
            if not config_path.is_file() or config_path.is_symlink():
                raise self.HTTPException(status_code=404, detail=f"Config '{name}' not found")
            try:
                prepared = self.load_pb8_config(config_path)
                overrides = self._load_override_payloads(prepared, self._config_dir(name))
            except self.PB8ConfigurationError as exc:
                raise self._configuration_error(f'Repairing {self.label} optimize queue config {name}', exc) from exc
        stage = self._stage_snapshot_bundle(filename, prepared, overrides)
        with self._queue_lock():
            backup = None
            target = self._snapshot_dir(filename)
            published = False
            journal = None
            try:
                queue_path = self._queue_file(filename)
                if not queue_path.is_file():
                    raise self.HTTPException(status_code=404, detail='Queue item not found')
                data = self._read_json(queue_path)
                repair_root = self.ensure_private_directory(self._queue_dir() / 'repair')
                backup = self._safe_path(self._queue_dir() / 'snapshots' / f'.{filename}.repair-backup-{self.uuid.uuid4().hex}', self._queue_dir())
                journal = self._safe_path(repair_root / f'{filename}.json', repair_root)
                self._write_json(journal, {'filename': filename, 'backup': backup.name})
                if target.exists():
                    if not target.is_dir() or target.is_symlink():
                        raise self.HTTPException(status_code=409, detail='Queue snapshot target is unsafe')
                    self.os.replace(target, backup)
                else:
                    backup = None
                self.os.replace(stage, target)
                published = True
                stage = None
                data['name'] = name
                data['config_path'] = str(config_path)
                data['snapshot_path'] = str(self._snapshot_file(filename))
                backtest = prepared.get('backtest') if isinstance(prepared.get('backtest'), dict) else {}
                data['exchange'] = backtest.get('exchanges') or []
                self._write_json(queue_path, data)
                if backup is not None:
                    self.rmtree(backup, ignore_errors=True)
                journal.unlink(missing_ok=True)
            except Exception:
                if published:
                    if target.exists():
                        self.rmtree(target, ignore_errors=True)
                    if backup is not None and backup.exists():
                        self.os.replace(backup, target)
                if journal is not None:
                    journal.unlink(missing_ok=True)
                raise
            finally:
                if stage is not None:
                    self.rmtree(stage, ignore_errors=True)
        return {'ok': True, 'filename': filename, 'name': name, 'json': data['config_path']}

    def _remove_queue_item(self, filename: str, *, require_exists: bool=True) -> bool:
        path = self._queue_file(filename)
        if not path.is_file():
            if require_exists:
                raise self.HTTPException(status_code=404, detail='Queue item not found')
            return False
        self._terminate_verified(filename)
        path.unlink(missing_ok=True)
        self._pid_file(filename).unlink(missing_ok=True)
        self._state_file(filename).unlink(missing_ok=True)
        self._state_file(filename).with_suffix('.convergence.json').unlink(missing_ok=True)
        self._state_file(filename).with_suffix('.convergence.forced.json').unlink(missing_ok=True)
        self._ready_file(filename).unlink(missing_ok=True)
        (self._log_dir() / f'{filename}.log').unlink(missing_ok=True)
        self.rmtree(self._snapshot_dir(filename), ignore_errors=True)
        self.rmtree(self._launch_dir(filename), ignore_errors=True)
        return True

    def remove_queue_item(self, filename: str, session: SessionToken=Depends(require_auth)) -> dict:
        with self._queue_lock():
            self._remove_queue_item(filename)
        return {'ok': True}

    def delete_queue_items(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        filenames = (body or {}).get('filenames')
        if not isinstance(filenames, list):
            raise self.HTTPException(status_code=400, detail='filenames must be a list')
        removed = 0
        missing = []
        with self._queue_lock():
            for raw in filenames:
                filename = self._validate_name(str(raw))
                if self._remove_queue_item(filename, require_exists=False):
                    removed += 1
                else:
                    missing.append(filename)
        return {'ok': True, 'removed': removed, 'missing': missing}

    def clear_finished(self, session: SessionToken=Depends(require_auth)) -> dict:
        removed = 0
        with self._queue_lock():
            for item in self._load_queue():
                if item.get('loop_id'):
                    continue
                if item['status'] == 'complete' and self._remove_queue_item(item['filename'], require_exists=False):
                    removed += 1
        return {'ok': True, 'removed': removed}

    def get_queue_log(self, filename: str, lines: int=100, session: SessionToken=Depends(require_auth)) -> dict:
        path = self._safe_path(self._log_dir() / f'{self._validate_name(filename)}.log', self._log_dir())
        if not path.is_file() or path.is_symlink():
            return {'log': '', 'exists': False}
        with path.open('rb') as handle:
            handle.seek(0, 2)
            handle.seek(max(0, handle.tell() - max(10, min(lines, 5000)) * 1024))
            return {'log': handle.read().decode('utf-8', errors='ignore'), 'exists': True}

    def _read_optimize_log_excerpt(self, path: Path, head_bytes: int=32 * 1024, tail_bytes: int=512 * 1024) -> str:
        if not path.is_file() or path.is_symlink():
            return ''
        try:
            with path.open('rb') as handle:
                handle.seek(0, 2)
                size = handle.tell()
                if size <= head_bytes + tail_bytes:
                    handle.seek(0)
                    return handle.read().decode('utf-8', errors='ignore')
                handle.seek(0)
                head = handle.read(head_bytes)
                handle.seek(size - tail_bytes)
                return head.decode('utf-8', errors='ignore') + '\n...\n' + handle.read().decode('utf-8', errors='ignore')
        except OSError:
            return ''

    def _estimate_log_evaluations(self, path: Path, max_bytes: int=64 * 1024 * 1024) -> int | None:
        """Return a fast lower bound from the latest Iter line plus visible duplicate drops."""
        if not path.is_file() or path.is_symlink():
            return None
        try:
            with path.open('rb') as handle:
                handle.seek(0, 2)
                size = handle.tell()
                handle.seek(max(0, size - max_bytes))
                text = handle.read(max_bytes).decode('utf-8', errors='ignore')
        except OSError:
            return None
        matches = list(self.re.finditer('\\bIter:\\s*(\\d+)', text))
        if not matches:
            return None
        latest = matches[-1]
        drops = text[latest.end():].count('Dropping candidate whose obj score is already present')
        return int(latest.group(1)) + drops

    def _parse_log_number(self, value) -> float | None:
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    def _parse_optimize_log_status(self, text: str) -> dict:
        summary = {'phase': 'queued', 'backend': None, 'algorithm': None, 'objective_count': None, 'iter': None, 'evaluations': None, 'front': None, 'ranges': {}, 'objectives': {}, 'last_log_at': None, 'last_line': '', 'last_error': None, 'stage': None, 'generation': None, 'proxy_evaluations': None, 'proxy_rate': None, 'exact_evaluations': None, 'exact_inflight': None, 'dispatch': {}, 'replay': {}, 'halving': {}}
        proxy_generation = 0
        proxy_total = 0
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line == '...':
                continue
            message = line
            match = self._OPT_LOG_LINE_RE.match(line)
            if match:
                message = match.group('msg').strip()
                summary['last_log_at'] = match.group('ts')
                if str(match.group('level') or '').upper() in {'ERROR', 'CRITICAL'}:
                    summary['last_error'] = message
                    summary['phase'] = 'error'
            summary['last_line'] = message
            backend = self._OPT_LOG_BACKEND_RE.search(message)
            if backend:
                summary['backend'] = backend.group('backend').lower()
            pymoo = self._OPT_LOG_PYMOO_RE.search(message)
            if pymoo:
                summary['algorithm'] = pymoo.group('algorithm').lower()
                if pymoo.group('n_obj'):
                    summary['objective_count'] = int(pymoo.group('n_obj'))
                summary['phase'] = 'optimizing'
            iteration = self._OPT_LOG_ITER_RE.search(message)
            if iteration:
                summary['iter'] = int(iteration.group('iter'))
                summary['front'] = int(iteration.group('size'))
                ranges = iteration.group('ranges') or ''
                for found in self._OPT_LOG_RANGE_RE.finditer(ranges):
                    summary['ranges'][found.group('name')] = {'min': self._parse_log_number(found.group('min')), 'max': self._parse_log_number(found.group('max'))}
                summary['phase'] = 'optimizing'
            evaluation = self._OPT_LOG_EVAL_RE.search(message)
            if evaluation:
                summary['evaluations'] = int(evaluation.group('eval'))
                summary['phase'] = 'optimizing'
            gpu = self._OPT_LOG_GPU_RE.search(message)
            if gpu:
                summary.update(backend='gpu', algorithm='nsga2', phase='optimizing', stage='exact_validation', generation=int(gpu.group('generation')), proxy_evaluations=int(gpu.group('proxy')), proxy_rate=float(gpu.group('rate')), exact_evaluations=int(gpu.group('exact')), exact_inflight=int(gpu.group('inflight')), evaluations=int(gpu.group('exact')))
                proxy_generation = summary['generation']
                proxy_total = summary['proxy_evaluations']
            progress = self._OPT_LOG_GPU_PROGRESS_RE.search(message)
            if progress:
                summary.update(backend='gpu', algorithm='nsga2', phase='optimizing', stage=progress.group('stage'), generation=int(progress.group('generation')), proxy_evaluations=int(progress.group('proxy')), proxy_rate=None, exact_evaluations=int(progress.group('exact')), evaluations=int(progress.group('exact')), exact_inflight=int(progress.group('pending')))
                front = self.re.search('\\bfront=(\\d+)(?=\\s|$)', message)
                if front:
                    summary['front'] = int(front.group(1))
                proxy_generation, proxy_total = (summary['generation'], summary['proxy_evaluations'])
            if '[gpu-profile]' in message:
                try:
                    profile = self.json.loads(message.split('[gpu-profile]', 1)[1].strip())
                except (ValueError, TypeError):
                    profile = None
                if isinstance(profile, dict):
                    event = profile.get('event')
                    exact = profile.get('exact_completed')
                    if type(exact) is int and exact >= 0:
                        summary.update(exact_evaluations=exact, evaluations=exact)
                    if event == 'generation':
                        generation = profile.get('generation')
                        population = profile.get('population_size')
                        reused = profile.get('seed_proxy_reused', 0)
                        screening = profile.get('screening') or []
                        count = None
                        if isinstance(screening, list) and screening:
                            counts = [item.get('candidate_count') if isinstance(item, dict) else None for item in screening]
                            if all((type(value) is int and value >= 0 for value in counts)):
                                count = sum(counts)
                        elif screening == [] and type(population) is int and (population > 0) and (type(reused) is int) and (0 <= reused <= population):
                            count = population - reused
                        if type(generation) is int and generation == proxy_generation + 1 and (count is not None):
                            proxy_generation = generation
                            proxy_total += count
                            summary.update(backend='gpu', algorithm='nsga2', phase='optimizing', generation=generation, proxy_evaluations=proxy_total)
                    elif event == 'complete':
                        proxy = profile.get('proxy_evaluations')
                        if type(proxy) is int and proxy >= 0:
                            proxy_total = proxy
                            summary.update(proxy_evaluations=proxy, phase='complete')
                    inflight = profile.get('exact_inflight')
                    if type(inflight) is int and inflight >= 0:
                        summary['exact_inflight'] = inflight
            dispatch = self._OPT_LOG_GPU_DISPATCH_RE.search(message)
            if dispatch:
                summary.update(backend='gpu', algorithm='nsga2', phase='optimizing', stage='proxy_dispatch')
                summary['dispatch'] = {'strategy': dispatch.group('strategy'), 'chunks_completed': int(dispatch.group('chunks_done')), 'chunks_total': int(dispatch.group('chunks_total')), 'candidates_completed': int(dispatch.group('candidates_done')), 'candidates_total': int(dispatch.group('candidates_total')), 'elapsed_seconds': float(dispatch.group('elapsed')), 'eta_seconds': float(dispatch.group('eta'))}
            replay = self._OPT_LOG_GPU_REPLAY_RE.search(message)
            if replay:
                elapsed = float(replay.group('elapsed'))
                bars_completed = int(replay.group('bars_done'))
                bars_total = int(replay.group('bars_total'))
                eta = elapsed * max(0, bars_total - bars_completed) / bars_completed if bars_completed > 0 and bars_completed < bars_total else 0.0 if bars_completed >= bars_total else None
                summary.update(backend='gpu', algorithm='nsga2', phase='optimizing', stage='proxy_replay')
                summary['replay'] = {'state': replay.group('state').lower(), 'id': int(replay.group('replay')), 'candidates': int(replay.group('candidates')), 'bars_completed': bars_completed, 'bars_total': bars_total, 'elapsed_seconds': elapsed, 'eta_seconds': eta, 'suite_pass': int(replay.group('pass_done')) if replay.group('pass_done') else None, 'suite_passes': int(replay.group('pass_total')) if replay.group('pass_total') else None, 'scenarios': replay.group('scenarios'), 'exchange': replay.group('exchange'), 'history_percent': float(replay.group('history')) if replay.group('history') else None}
            halving = self._OPT_LOG_GPU_HALVING_RE.search(message)
            if halving:
                rungs = []
                for item in halving.group('rungs').split(','):
                    fraction, separator, count = item.partition(':')
                    if separator and count.isdigit():
                        rungs.append({'history_fraction': fraction, 'candidates': int(count)})
                summary.update(backend='gpu', algorithm='nsga2', phase='optimizing', stage='successive_halving', generation=int(halving.group('generation')))
                summary['halving'] = {'rungs': rungs, 'full_history': int(halving.group('full_done')), 'population': int(halving.group('full_total'))}
            resumed = self._OPT_LOG_GPU_RESUME_RE.search(message)
            if resumed:
                exact = int(resumed.group('exact'))
                proxy_generation = int(resumed.group('generation'))
                proxy_total = 0
                summary.update(backend='gpu', algorithm='nsga2', phase='optimizing', stage='resumed', proxy_evaluations=0, generation=int(resumed.group('generation')), exact_evaluations=exact, evaluations=exact)
            completed = self._OPT_LOG_GPU_COMPLETE_RE.search(message)
            if completed:
                exact = int(completed.group('exact'))
                summary.update(backend='gpu', algorithm='nsga2', phase='complete', stage='complete', generation=int(completed.group('generation')), proxy_evaluations=int(completed.group('proxy')), exact_evaluations=exact, exact_inflight=0, evaluations=exact)
            objective_block = self.re.search('objectives=\\[([^\\]]*)\\]', message, self.re.IGNORECASE)
            if objective_block:
                for raw_objective in objective_block.group(1).split(','):
                    name, separator, value = raw_objective.partition('=')
                    numeric = self._parse_log_number(value)
                    if separator and name.strip() and (numeric is not None):
                        summary['objectives'][name.strip()] = numeric
            lower = message.lower()
            if 'optimization complete' in lower or 'successfully processed optimize_results' in lower:
                summary['phase'] = 'complete'
            elif 'starting optimize' in lower and summary['phase'] == 'queued':
                summary['phase'] = 'optimizing'
            elif 'initializ' in lower and summary['phase'] == 'queued':
                summary['phase'] = 'initializing'
        return summary

    def _verified_process_stats(self, filename: str) -> dict:
        record = self._read_process_record(filename)
        stats = {'running': False, 'pid': int(record['pid']) if record else None, 'status': None, 'rss_bytes': None, 'memory_percent': None, 'cpu_percent': None, 'cpu_cores_est': None, 'threads': None, 'children': 0, 'create_time': float(record['create_time']) if record else None, 'started_at': None}
        if not record or not self._process_matches(filename, record):
            return stats
        try:
            parent = self.psutil.Process(int(record['pid']))
            processes = [parent, *parent.children(recursive=True)]
            rss = threads = 0
            memory_percent = cpu_percent = 0.0
            for process in processes:
                try:
                    rss += int(process.memory_info().rss)
                    threads += int(process.num_threads())
                    memory_percent += float(process.memory_percent())
                    cpu_percent += float(process.cpu_percent(interval=None) or 0.0)
                except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess, self.psutil.AccessDenied):
                    continue
            stats.update(running=True, status=parent.status(), rss_bytes=rss, memory_percent=round(memory_percent, 2), cpu_percent=round(cpu_percent, 1), cpu_cores_est=round(cpu_percent / 100.0, 2), threads=threads, children=max(0, len(processes) - 1), started_at=self.datetime.datetime.fromtimestamp(float(record['create_time']), self.datetime.UTC).isoformat())
        except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess, self.psutil.AccessDenied, OSError):
            pass
        return stats

    def _system_stats(self) -> dict:
        memory = self.psutil.virtual_memory()
        swap = self.psutil.swap_memory()
        try:
            per_core = [round(float(value), 1) for value in self.psutil.cpu_percent(interval=0.05, percpu=True)]
        except Exception:
            per_core = []
        try:
            load_avg = tuple((round(value, 2) for value in self.os.getloadavg())) if hasattr(self.os, 'getloadavg') else None
        except OSError:
            load_avg = None
        overall = round(sum(per_core) / len(per_core), 1) if per_core else round(float(self.psutil.cpu_percent(interval=0.05)), 1)
        return {'cpu_percent': overall, 'cpu_per_core': per_core, 'cpu_core_count': len(per_core), 'memory_percent': round(float(memory.percent), 1), 'memory_used_bytes': int(memory.used), 'memory_total_bytes': int(memory.total), 'swap_percent': round(float(swap.percent), 1), 'swap_used_bytes': int(swap.used), 'swap_total_bytes': int(swap.total), 'load_avg': load_avg}

    def _active_all_results_progress(self, filename: str) -> dict | None:
        """Return durable evaluation progress for the verified queue process, if available."""
        record = self._read_process_record(filename)
        if not record or not self._process_matches(filename, record):
            return None
        results_root = self.Path(self.os.path.abspath(self._results_root()))
        try:
            parent = self.psutil.Process(int(record['pid']))
            processes = [parent, *parent.children(recursive=True)]
        except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess, self.psutil.AccessDenied, OSError, ValueError, TypeError):
            return None
        for process in processes:
            try:
                open_files = process.open_files()
            except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess, self.psutil.AccessDenied, OSError):
                continue
            for opened in open_files:
                try:
                    path = self.Path(self.os.path.abspath(str(opened.path)))
                    relative = path.relative_to(results_root)
                except (AttributeError, OSError, ValueError):
                    continue
                if len(relative.parts) != 2 or relative.parts[-1] != 'all_results.bin':
                    continue
                if not path.is_file() or path.is_symlink():
                    continue
                progress = self._runner_evaluation_progress(path)
                if progress is None:
                    self._ensure_active_evaluation_scan(path)
                    progress = self._cached_evaluation_scan_progress(path)
                return {'evaluations': int(progress.get('evaluations') or 0), 'result': relative.parts[0], 'trailing_partial_entry': bool(progress.get('trailing_partial_entry')), 'scan_complete': bool(progress.get('scan_complete')), 'bytes_scanned': int(progress.get('bytes_scanned') or 0), 'total_bytes': int(progress.get('total_bytes') or 0), 'scan_percent': float(progress.get('scan_percent') or 0.0), 'source': str(progress.get('source') or 'fallback_scan')}
        return None

    def get_queue_status(self, filename: str, session: SessionToken=Depends(require_auth)) -> dict:
        queue_items = self._load_queue()
        item = next((item for item in queue_items if item['filename'] == filename), None)
        if item is None:
            raise self.HTTPException(status_code=404, detail='Queue item not found')
        state = self._read_runner_state(filename) or {}
        try:
            convergence = self.read_convergence_state(self._state_file(filename).with_suffix('.convergence.json'))
        except (OSError, ValueError, TypeError):
            convergence = {}
        log_path = self._safe_path(self._log_dir() / f'{filename}.log', self._log_dir())
        log_summary = self._parse_optimize_log_status(self._read_optimize_log_excerpt(log_path))
        config = {}
        for path in (self._launch_config_file(filename), self._snapshot_file(filename)):
            try:
                if path.is_file() and (not path.is_symlink()):
                    config = self._read_json(path)
                    break
            except RuntimeError:
                pass
        optimize = config.get('optimize') if isinstance(config.get('optimize'), dict) else {}
        pymoo = optimize.get('pymoo') if isinstance(optimize.get('pymoo'), dict) else {}
        specs = self._pareto_objective_specs(config)
        target = None
        for key in ('iters', 'max_evaluations', 'n_evaluations'):
            try:
                if optimize.get(key) not in (None, ''):
                    target = int(optimize[key])
                    break
            except (TypeError, ValueError):
                continue
        evaluations = log_summary['evaluations']
        evaluation_source = 'log' if evaluations is not None else None
        log_estimate = self._estimate_log_evaluations(log_path) if item['status'] == 'running' else None
        if log_estimate is not None and (evaluations is None or log_estimate > evaluations):
            evaluations = log_estimate
            evaluation_source = 'log_lower_bound'
        durable_progress = self._active_all_results_progress(filename) if item['status'] == 'running' else None
        if durable_progress is not None:
            durable_evaluations = int(durable_progress['evaluations'])
            if evaluations is None or durable_evaluations >= evaluations:
                evaluations = durable_evaluations
                evaluation_source = durable_progress.get('source') or 'fallback_scan'
        if state.get('evaluations') is not None:
            evaluations = state['evaluations']
            evaluation_source = 'runner'
        if evaluations is None and item['status'] == 'complete' and (target is not None) and (not self.convergence_complete(state)):
            evaluations = target
            evaluation_source = 'complete'
        percent = None
        if evaluations is not None and target and (target > 0):
            percent = max(0.0, min(100.0, evaluations / target * 100.0))
        queue_totals = {status: sum((1 for queued in queue_items if queued['status'] == status)) for status in ('queued', 'running', 'complete', 'error')}
        log_meta = {'exists': False, 'size_bytes': 0, 'updated_at': None, 'activity_seconds': None, 'last_log_at': log_summary['last_log_at'], 'last_line': log_summary['last_line'], 'last_error': log_summary['last_error'] or item.get('error_reason') or None}
        try:
            if log_path.is_file() and (not log_path.is_symlink()):
                log_stat = log_path.stat()
                log_meta.update(exists=True, size_bytes=log_stat.st_size, updated_at=self.datetime.datetime.fromtimestamp(log_stat.st_mtime, self.datetime.UTC).isoformat(), activity_seconds=max(0.0, self.time.time() - log_stat.st_mtime))
        except OSError:
            pass
        phase = log_summary['phase']
        if item['status'] in {'complete', 'error'}:
            phase = item['status']
        elif item['status'] == 'running' and phase == 'queued':
            phase = 'running'
        backend = log_summary['backend'] or optimize.get('backend')
        return {**item, 'convergence': convergence.get('convergence') or state.get('convergence'), 'convergence_config': convergence.get('config'), 'phase': phase, 'progress': {'eval': evaluations, 'evaluations': evaluations, 'iter': log_summary['iter'], 'target_iters': target, 'target_evaluations': target, 'evaluation_source': evaluation_source, 'estimated': evaluation_source in {'log_lower_bound', 'fallback_scan'}, 'result': durable_progress.get('result') if durable_progress else None, 'evaluation_scan': {'complete': bool(durable_progress.get('scan_complete')), 'bytes_scanned': int(durable_progress.get('bytes_scanned') or 0), 'total_bytes': int(durable_progress.get('total_bytes') or 0), 'percent': float(durable_progress.get('scan_percent') or 0.0)} if durable_progress else None, 'percent': percent, 'front': log_summary['front'], 'generation': log_summary['generation'], 'proxy_evaluations': log_summary['proxy_evaluations'], 'proxy_rate': log_summary['proxy_rate'], 'exact_evaluations': evaluations if backend == 'gpu' and evaluation_source == 'runner' else log_summary['exact_evaluations'], 'target_exact_evaluations': target if backend == 'gpu' else None, 'exact_inflight': log_summary['exact_inflight'], 'dispatch': log_summary['dispatch'], 'replay': log_summary['replay'], 'halving': log_summary['halving']}, 'runtime': {'launch_mode': item['launch_mode'], 'backend': backend, 'algorithm': log_summary['algorithm'] or ('nsga2' if backend == 'gpu' else pymoo.get('algorithm')), 'stage': log_summary['stage'], 'objective_count': log_summary['objective_count'] or (len(specs) if specs else None), 'objectives': specs, 'config_n_cpus': optimize.get('n_cpus'), 'seed_mode': item['launch_mode'], 'exchange': item.get('exchange')}, 'metrics': {'objectives': log_summary['objectives'], 'ranges': log_summary['ranges']}, 'process': self._verified_process_stats(filename), 'system': self._system_stats(), 'queue': queue_totals, 'log': log_meta, 'runner': state}

    def list_results(self, session: SessionToken=Depends(require_auth)) -> dict:
        with self._result_lock():
            return {'results': self._list_results()}

    def get_result_config(self, path: str, session: SessionToken=Depends(require_auth)) -> dict:
        with self._result_lock():
            result_dir = self._resolve_result_path(path)
            config = self._recover_result_config(result_dir)
            if config is not None:
                config = self._restore_result_window_plan(result_dir, config)
        if config is None:
            raise self.HTTPException(status_code=404, detail='No recoverable PB8 config found for result')
        try:
            prepared = self.prepare_pb8_config(config, base_config_path=str(result_dir / self._CONFIG_FILENAME))
            return {'config': prepared, 'param_status': {}, 'override_configs': self._result_override_payloads(result_dir, prepared)}
        except self.PB8ConfigurationError as exc:
            raise self._configuration_error(f'Loading {self.label} result config', exc) from exc

    def _result_convergence_config(self, result_dir: Path, fallback: dict) -> dict:
        """Recover reviewed local settings from bounded, result-owned history, never native defaults."""
        path = result_dir / self.CONVERGENCE_STATE_FILENAME
        if not path.exists() and (not path.is_symlink()):
            return self.validate_convergence_config(fallback)
        try:
            state = self.read_convergence_state(path)
            signature = state.get('signature')
            if not isinstance(signature, str) or not self.re.fullmatch('[0-9a-f]{64}', signature) or (not isinstance(state.get('config'), dict)) or (not self.compatible_convergence_history(state, signature, result_dir)):
                raise ValueError('Invalid or mismatched result-owned convergence history')
            return self.validate_convergence_config(state['config'])
        except (OSError, ValueError, TypeError) as exc:
            self._log(self.SERVICE, f'Cannot restore local convergence settings for checkpoint resume: {exc}', level='WARNING')
            raise self.HTTPException(status_code=422, detail='Cannot restore local convergence settings for checkpoint resume: ' + str(exc)) from exc

    def queue_result_resume(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        """Validate and atomically create a managed config plus checkpoint queue item."""
        name = self._validate_name(str((body or {}).get('name') or ''))
        created_config = False
        with self._result_lock():
            result_dir = self._resolve_result_path(str((body or {}).get('path') or ''))
            readiness = self._checkpoint_resume_readiness(result_dir)
            if not readiness['ready']:
                raise self.HTTPException(status_code=422, detail='Checkpoint cannot be resumed: ' + '; '.join(readiness['reasons']))
            config = self._restore_result_window_plan(result_dir, self.copy.deepcopy(readiness['config']))
            runtime_options = (config.get('pbgui') or {}).get('optimize_runtime') or {}
            convergence_config = self._result_convergence_config(result_dir, runtime_options.get('convergence_config', runtime_options))
            config.setdefault('pbgui', {}).setdefault('optimize_runtime', {})['convergence_config'] = convergence_config
            result_overrides = self._result_override_payloads(result_dir, config)
            try:
                candidate = self.prepare_pb8_config(self._normalize_config(config, name))
            except self.PB8ConfigurationError as exc:
                raise self._configuration_error(f'Preparing {self.label} checkpoint resume', exc) from exc
            self._preflight_checkpoint_resume(candidate, result_dir)
            options = self._validate_launch_options({**self._result_launch_options(result_dir, config), 'mode': 'checkpoint_resume', 'source': str(result_dir), 'convergence_config': convergence_config})
            try:
                with self._config_lock():
                    prepared = self._save_config_bundle(name, candidate, create_only=True, override_payloads=result_overrides)
                    created_config = True
                    saved_overrides = self._load_override_payloads(prepared, self._config_dir(name))
                return self._create_queue_record(name, prepared, options, saved_overrides)
            except Exception:
                if created_config:
                    try:
                        with self._config_lock():
                            target = self._config_dir(name)
                            if target.is_dir() and (not target.is_symlink()):
                                self.rmtree(target)
                    except Exception as cleanup_exc:
                        self._log(self.SERVICE, f'Failed to roll back PB8 checkpoint resume config {name}: {cleanup_exc}', level='ERROR')
                raise

    def _source_references_result(self, source: str, result_dir: Path) -> bool:
        raw = str(source or '').strip()
        if not raw or raw == '__self__':
            return False
        try:
            source_path = self.Path(self.os.path.abspath(raw))
            source_path.relative_to(result_dir)
            return True
        except (OSError, ValueError):
            return False

    def _assert_result_deletable(self, result_dir: Path) -> None:
        """Accept only canonical run directories, never their nested artifacts or parents."""
        results_root = self.Path(self.os.path.abspath(self._results_root()))
        result_dir = self.Path(self.os.path.abspath(result_dir))
        if result_dir.parent != results_root or result_dir.name.startswith('.'):
            raise self.HTTPException(status_code=400, detail='Deletion requires a top-level optimize result run directory')
        try:
            pid_paths = list(self._queue_dir().glob('*.pid'))
        except OSError as exc:
            raise self.HTTPException(status_code=409, detail='Active PB8 optimizer ownership could not be verified safely') from exc
        for pid_path in pid_paths:
            try:
                filename = self._validate_name(pid_path.stem)
                record = self._read_process_record(filename)
                if record and self._process_matches(filename, record):
                    owned_results = set()
                    for persisted in record.get('owned_results') or []:
                        try:
                            relative = self.Path(self.os.path.abspath(str(persisted))).relative_to(results_root)
                        except (OSError, ValueError):
                            continue
                        if relative.parts:
                            owned_results.add(results_root / relative.parts[0])
                    observed_results = set()
                    inspection_complete = True
                    try:
                        process = self.psutil.Process(int(record['pid']))
                    except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess, self.psutil.AccessDenied, OSError, ValueError, TypeError):
                        processes = []
                        inspection_complete = False
                    else:
                        processes = [process]
                        try:
                            processes.extend(process.children(recursive=True))
                        except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess, self.psutil.AccessDenied, OSError):
                            inspection_complete = False
                    for candidate in processes:
                        try:
                            open_files = candidate.open_files()
                        except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess, self.psutil.AccessDenied, OSError):
                            inspection_complete = False
                            continue
                        for opened in open_files:
                            try:
                                relative = self.Path(self.os.path.abspath(str(opened.path))).relative_to(results_root)
                            except (AttributeError, OSError, ValueError):
                                continue
                            if relative.parts:
                                observed_results.add(results_root / relative.parts[0])
                    if observed_results.difference(owned_results):
                        owned_results.update(observed_results)
                        self._write_json(self._pid_file(filename), {'pid': record['pid'], 'create_time': record['create_time'], 'owned_results': sorted((str(path) for path in owned_results))})
                    if result_dir in owned_results:
                        raise self.HTTPException(status_code=409, detail=f"PB8 optimizer queue item '{filename}' owns result '{result_dir.name}'")
                    if not owned_results:
                        raise self.HTTPException(status_code=409, detail=f"Could not verify result ownership for active PB8 optimizer queue item '{filename}'; deletion of '{result_dir.name}' is blocked")
            except self.HTTPException:
                raise
            except Exception as exc:
                self._log(self.SERVICE, f'Could not inspect PB8 optimizer result ownership: {exc}', level='WARNING')
                raise self.HTTPException(status_code=409, detail='Active PB8 optimizer ownership could not be verified safely') from exc
        try:
            queue_paths = list(self._queue_dir().glob('*.json'))
        except OSError as exc:
            raise self.HTTPException(status_code=409, detail='PB8 queue references could not be verified safely') from exc
        for queue_path in queue_paths:
            try:
                filename = self._validate_name(queue_path.stem)
                data = self._read_json(queue_path)
                sources = []
                options = data.get('launch_options') if isinstance(data.get('launch_options'), dict) else {}
                sources.append(options.get('source'))
                launch_options_path = self._launch_options_file(filename)
                if launch_options_path.is_file() and (not launch_options_path.is_symlink()):
                    sources.append(self._read_json(launch_options_path).get('source'))
                if any((self._source_references_result(source, result_dir) for source in sources)):
                    raise self.HTTPException(status_code=409, detail=f"Queue item '{filename}' references this result as a continuation source")
            except self.HTTPException:
                raise
            except Exception as exc:
                self._log(self.SERVICE, f'Could not verify PB8 queue source before result deletion: {exc}', level='WARNING')
                raise self.HTTPException(status_code=409, detail='PB8 queue references could not be verified safely') from exc
        with self._dash_lock:
            dash_records = [record for record in [*self._dash_sessions.values(), *self._dash_pending_sessions.values()] if record]
        try:
            dash_records.extend(self._read_dash_registry().values())
        except Exception as exc:
            raise self.HTTPException(status_code=409, detail='Pareto Dash ownership could not be verified safely') from exc
        if any((self.Path(self.os.path.abspath(str(record.get('result_dir') or ''))) == result_dir for record in dash_records)):
            raise self.HTTPException(status_code=409, detail='A live or persisted Pareto Dash session references this result')

    def _forget_result_progress(self, result_dir: Path) -> None:
        prefix = str(result_dir) + self.os.sep
        with self._result_progress_cache_lock:
            for key in list(self._result_progress_cache):
                if key == str(result_dir) or key.startswith(prefix):
                    self._result_progress_cache.pop(key, None)

    def _forget_result_caches(self, result_dir: Path) -> None:
        self._forget_result_progress(result_dir)
        with self._result_listing_cache_lock:
            self._result_listing_cache.pop(str(result_dir), None)
        prefix = str(result_dir.resolve()) + self.os.sep
        with self._pareto_list_cache_lock:
            for key in list(self._pareto_list_cache):
                if key[0].startswith(prefix):
                    self._pareto_list_cache.pop(key, None)
            self._pareto_warning_cache.pop(str(result_dir), None)

    def _stage_delete_result(self, result_dir: Path) -> Path:
        staged = self._safe_path(result_dir.parent / f'.{result_dir.name}.delete-{self.uuid.uuid4().hex}', self._results_root())
        self.os.replace(result_dir, staged)
        self._forget_result_caches(result_dir)
        return staged

    def _rollback_staged_results(self, staged: list[tuple[Path, Path]]) -> None:
        for original, temporary in reversed(staged):
            try:
                if temporary.exists() and (not original.exists()):
                    self.os.replace(temporary, original)
            except OSError as exc:
                self._log(self.SERVICE, f'Failed to roll back staged PB8 result {original.name}: {exc}', level='ERROR')

    def delete_result(self, path: str, session: SessionToken=Depends(require_auth)) -> dict:
        with self._result_lock():
            result_dir = self._resolve_result_path(path, for_delete=True)
            with self._queue_lock():
                self._assert_result_deletable(result_dir)
                try:
                    staged = self._stage_delete_result(result_dir)
                except FileNotFoundError:
                    self._forget_result_caches(result_dir)
                    return {'ok': True, 'removed': 0, 'missing': [path]}
            try:
                self.rmtree(staged)
            except FileNotFoundError:
                pass
        return {'ok': True, 'removed': 1, 'missing': []}

    def delete_results(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        paths = (body or {}).get('paths')
        if not isinstance(paths, list):
            raise self.HTTPException(status_code=400, detail='paths must be a list')
        missing = []
        resolved = []
        with self._result_lock():
            for raw in paths:
                try:
                    result_dir = self._resolve_result_path(str(raw), for_delete=True)
                except self.HTTPException as exc:
                    if exc.status_code == 404:
                        missing.append(str(raw))
                        continue
                    raise
                if result_dir not in resolved:
                    resolved.append(result_dir)
            staged = []
            with self._queue_lock():
                for result_dir in resolved:
                    self._assert_result_deletable(result_dir)
                try:
                    for result_dir in resolved:
                        try:
                            staged.append((result_dir, self._stage_delete_result(result_dir)))
                        except FileNotFoundError:
                            self._forget_result_caches(result_dir)
                            missing.append(str(result_dir))
                except Exception:
                    self._rollback_staged_results(staged)
                    raise
            for _original, temporary in staged:
                try:
                    self.rmtree(temporary)
                except FileNotFoundError:
                    pass
        return {'ok': True, 'removed': len(staged), 'missing': list(dict.fromkeys(missing))}

    def launch_result_3d_plot(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        """Normalize nested PB8 metrics in a temporary managed stage for the shared plotter."""
        from api.optimize_v7 import _build_pareto_3d_plot_payload
        with self._result_lock():
            result_dir = self._resolve_result_path(str((body or {}).get('path') or ''))
            pareto_dir = result_dir / 'pareto'
            paths = sorted(pareto_dir.glob('*.json')) if pareto_dir.is_dir() and (not pareto_dir.is_symlink()) else []
            if not paths:
                raise self.HTTPException(status_code=404, detail='No pareto config found for result')
            rows = []
            first_data = None
            for path in paths:
                try:
                    data = self._read_json(path)
                except RuntimeError:
                    continue
                first_data = first_data or data
                rows.append((path, data))
            if first_data is None:
                raise self.HTTPException(status_code=422, detail='No readable PB8 Pareto candidates were found')
            contract = self._pareto_contract(first_data)
            statistic = str((body or {}).get('statistic') or 'mean').strip().lower()
            statistic = statistic if statistic in self._PARETO_STATISTICS else 'mean'
            scenario = str((body or {}).get('scenario') or 'Aggregated').strip() or 'Aggregated'
            if scenario not in {'Aggregated', *contract['scenario_labels']}:
                scenario = 'Aggregated'
            summaries = [(path, data, self._pareto_summary(data, statistic, scenario)) for path, data in rows]
            specs = contract['objectives']
            if specs:
                usable_specs = [spec for spec in specs if any((spec['metric'] in summary for _path, _data, summary in summaries))]
            else:
                names = list(summaries[0][2]) if summaries else []
                usable_specs = [{'metric': name, 'goal': 'max'} for name in names]
            if len(usable_specs) != 3:
                names = [spec['metric'] for spec in usable_specs]
                return {'ok': False, 'message': 'PB8 3D plot requires exactly 3 usable objectives for the selected statistic and scenario.', 'output': self.json.dumps({'objectives': names, 'statistic': statistic, 'scenario': scenario}, indent=2)}
            complete = [(path, data, summary) for path, data, summary in summaries if all((spec['metric'] in summary for spec in usable_specs))]
            if not complete:
                return {'ok': False, 'message': 'PB8 reported no complete 3D Pareto points for the selected statistic and scenario.', 'output': self.json.dumps({'objectives': [spec['metric'] for spec in usable_specs], 'points': 0}, indent=2)}
            stage_root = self.ensure_private_directory(self._data_dir() / 'cache' / self.profile.text('pareto_plot_v8'))
            stage = self._safe_path(stage_root / self.uuid.uuid4().hex, stage_root)
            stage_pareto = stage / 'pareto'
            self.ensure_private_directory_tree(stage_root, stage_pareto)
            try:
                for path, data, summary in complete:
                    normalized = {'backtest': self.copy.deepcopy(data.get('backtest') or {}), 'optimize': {'scoring': self.copy.deepcopy(usable_specs)}, 'result': {'objectives': {spec['metric']: summary[spec['metric']] for spec in usable_specs}}}
                    self._write_json(stage_pareto / path.name, normalized)
                payload = _build_pareto_3d_plot_payload(stage)
            finally:
                self.rmtree(stage, ignore_errors=True)
        if isinstance(payload.get('message'), str):
            payload['message'] = payload['message'].replace('PB7', 'PB8')
        return payload

    def launch_result_pareto_dash(self, body: dict, request: Request, session: SessionToken=Depends(require_auth)) -> dict:
        with self._result_lock():
            result_dir = self._resolve_result_path(str((body or {}).get('path') or ''))
            if self._first_pareto_file(result_dir) is None:
                raise self.HTTPException(status_code=400, detail='Result has no pareto data')
            session_id = self.uuid.uuid4().hex[:12]
            proxy_root = str(request.app.url_path_for(self.profile.text('optimize_v8_pareto_dash_proxy_root'), session_id=session_id))
            launched = self._launch_dash_session(session_id, result_dir, proxy_root)
        return {'ok': True, 'session_id': session_id, 'url': proxy_root, 'result': self.Path(launched['result_dir']).name}

    async def proxy_result_pareto_dash(self, session_id: str, request: Request, dash_path: str='', session: SessionToken=Depends(require_auth)) -> Response:
        self._reap_dash_sessions()
        with self._dash_lock:
            launched = self._dash_sessions.get(session_id)
            if launched is not None:
                launched['last_access'] = self.time.time()
                try:
                    self._persist_dash_session(launched)
                except Exception as exc:
                    self._log(self.SERVICE, f'Failed to refresh PB8 Pareto Dash session {session_id}: {exc}', level='WARNING')
        if not launched:
            raise self.HTTPException(status_code=404, detail='PB8 Pareto Dash session not found')
        target_path = '/' if not dash_path else f'/{dash_path.lstrip('/')}'
        target_url = f'http://127.0.0.1:{launched['port']}{target_path}'
        if request.url.query:
            target_url = f'{target_url}?{request.url.query}'
        headers = self._dash_request_headers(request.headers)
        try:
            async with self.httpx.AsyncClient(timeout=60, follow_redirects=False) as client:
                upstream = await client.request(request.method, target_url, headers=headers, content=await request.body())
        except self.httpx.HTTPError as exc:
            raise self.HTTPException(status_code=502, detail=f'PB8 Pareto Dash proxy failed: {exc}') from exc
        response_headers = self._dash_response_headers(upstream.headers)
        location = response_headers.get('location')
        if location:
            upstream_base = f'http://127.0.0.1:{launched['port']}'
            if location.startswith(upstream_base):
                location = location[len(upstream_base):] or '/'
            if location.startswith('/'):
                response_headers['location'] = self._proxy_path(launched['proxy_root'], location)
        content = self._rewrite_dash_content(upstream.content, response_headers.get('content-type', ''), launched['proxy_root'])
        return self.Response(content=content, status_code=upstream.status_code, headers=response_headers)

    def stop_result_pareto_dash(self, session_id: str, session: SessionToken=Depends(require_auth)) -> dict:
        self._stop_dash_session(session_id)
        return {'ok': True}

    def list_paretos(self, result_path: str, scenario: str=Query('Aggregated'), statistic: str=Query('mean'), session: SessionToken=Depends(require_auth), metrics: str=Query('')) -> dict:
        with self._result_lock():
            result_dir = self._resolve_result_path(result_path)
            selected_statistic = statistic if statistic in self._PARETO_STATISTICS else 'mean'
            requested_metrics = tuple(self._requested_pareto_metrics(metrics))
            pareto_dir = result_dir / 'pareto'
            try:
                paths = sorted(pareto_dir.glob('*.json')) if pareto_dir.is_dir() and (not pareto_dir.is_symlink()) else []
            except OSError as exc:
                paths = []
                self._log_pareto_skips(result_dir, 1, str(exc))
            sweep_plan = self._load_sweep_plan(result_dir)
            if sweep_plan is not None:
                # Sweep evaluation needs per-window growth even when it is not displayed.
                requested_metrics = tuple(dict.fromkeys((*requested_metrics, *self._SWEEP_GAIN_SOURCE_METRICS)))
            candidates = []
            errors = []
            metric_catalog = []
            catalog_loaded = False
            self._prune_pareto_list_cache()
            for path in paths:
                try:
                    compact, stat_result = self._load_compact_pareto(path, requested_metrics, include_catalog=not catalog_loaded)
                    candidates.append((path, compact, stat_result))
                    if not catalog_loaded:
                        metric_catalog = list(compact.get('available_metric_names') or [])
                        catalog_loaded = True
                except (OSError, RuntimeError) as exc:
                    errors.append(f'{path.name}: {exc}')
                    continue
            if errors:
                self._log_pareto_skips(result_dir, len(errors), errors[0])
            contract = candidates[0][1] if candidates else {'mode': 'unknown', 'scenario_count': 0, 'scenario_labels': [], 'objectives': []}
            selected_scenario = scenario if scenario in {'Aggregated', *contract['scenario_labels']} else 'Aggregated'
            paretos = []
            for path, compact, stat_result in candidates:
                summary = self._project_compact_pareto(compact, selected_statistic, selected_scenario, requested_metrics)
                sweep_evaluation = None
                if sweep_plan is not None and selected_scenario == 'Aggregated':
                    sweep_evaluation = self.evaluate_sweep_cycles(sweep_plan, compact.get('suite_values') or {}, compact.get('suite_sources') or {})
                    if sweep_evaluation.get('available'):
                        summary.update(sweep_evaluation['summary'])
                item = {'path': str(path), 'name': path.stem, 'modified': self.datetime.datetime.fromtimestamp(stat_result.st_mtime).isoformat(), 'summary': summary}
                if sweep_evaluation is not None:
                    item['sweep_cycles'] = {'available': bool(sweep_evaluation.get('available')), 'reason': sweep_evaluation.get('reason'), 'gain_metric': sweep_evaluation.get('gain_metric'), 'holdout_status': sweep_evaluation.get('holdout_status')}
                paretos.append(item)
            available_set = {str(metric) for metric in metric_catalog}
            available_set.update((str(metric) for item in paretos for metric in item.get('summary') or {}))
            if sweep_plan is not None:
                available_set.update(self.SWEEP_METRIC_NAMES)
            comparison_order = [name for name, _aliases in self._PARETO_COMPARISON_METRICS]
            available_metrics = [name for name in comparison_order if name in available_set]
            available_metrics.extend(sorted(available_set - set(available_metrics)))
            objective_names = [str(item.get('metric') or '').strip() for item in contract['objectives']]
            default_metrics = []
            default_candidates = ['gain']
            if sweep_plan is not None:
                default_candidates.extend(['sweep_net_cashflow', 'sweep_cycles_completed'])
            default_candidates.extend([*objective_names, 'drawdown_worst'])
            for name in default_candidates:
                if name in available_set and name not in default_metrics:
                    default_metrics.append(name)
        return {'paretos': paretos, 'meta': {'mode': contract['mode'], 'has_suite_metrics': contract['mode'] == 'suite', 'scenario_count': contract['scenario_count'], 'scenario_labels': contract['scenario_labels'], 'objectives': contract['objectives'], 'available_metrics': available_metrics, 'default_metrics': default_metrics or available_metrics[:1], 'available_statistics': list(self._PARETO_STATISTICS), 'selected_scenario': selected_scenario, 'selected_statistic': selected_statistic, 'statistic_enabled': contract['mode'] != 'suite' or selected_scenario == 'Aggregated', 'validation_holdout_count': len(self.validation_holdouts(result_dir)), 'sweep_cycles': {'enabled': sweep_plan is not None, 'policy': self.copy.deepcopy(sweep_plan.get('policy')) if sweep_plan else None, 'holdout_count': int(sweep_plan.get('holdout_count') or 0) if sweep_plan else 0}}}

    def get_pareto_file(self, path: str, session: SessionToken=Depends(require_auth)) -> dict:
        with self._result_lock():
            pareto = self._resolve_result_path(path, require_directory=False)
            if not pareto.is_file() or pareto.parent.name != 'pareto':
                raise self.HTTPException(status_code=400, detail='Path is not a managed PB8 pareto file')
            config = self._restore_result_window_plan(pareto.parent.parent, self._read_json(pareto))
            response = {'config': config, 'override_configs': self._result_override_payloads(pareto.parent.parent, config)}
            response['validation_holdouts'] = self.validation_holdouts(pareto.parent.parent)
            sweep_plan = self._load_sweep_plan(pareto.parent.parent)
            if sweep_plan is not None:
                suite_metrics, _labels = self._suite_metric_payload(config)
                response['sweep_cycles'] = self.evaluate_sweep_cycles(sweep_plan, suite_metrics, self._suite_metric_sources(config))
            return response

    def create_pareto_seed_bundle(self, body: dict, session: SessionToken=Depends(require_auth)) -> dict:
        with self._result_lock():
            result_dir = self._resolve_result_path(str((body or {}).get('result_path') or ''))
            raw_paths = (body or {}).get('paths')
            if not isinstance(raw_paths, list):
                raise self.HTTPException(status_code=400, detail='paths must be a list')
            pareto_dir = self._safe_path(result_dir / 'pareto', self._results_root())
            paths = []
            seen = set()
            for raw in raw_paths:
                path = self._resolve_result_path(str(raw), require_directory=False)
                if path.parent != pareto_dir or not self._supported_seed_file(path):
                    raise self.HTTPException(status_code=422, detail='All Pareto seeds must be supported files from the selected result')
                if str(path) not in seen:
                    paths.append(path)
                    seen.add(str(path))
            if not paths:
                raise self.HTTPException(status_code=422, detail='No Pareto seed files selected')
            if len(paths) == 1:
                return {'path': str(paths[0]), 'count': 1}
            bundle = self._create_seed_bundle(result_dir, paths)
            return {'path': str(bundle), 'count': len(paths)}
