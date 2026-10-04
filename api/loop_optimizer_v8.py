"""Authenticated owner-scoped PB8 autonomous loop optimizer routes."""
from __future__ import annotations

import asyncio
import copy
import math
import time
from pathlib import Path
import traceback
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator, ValidationError

from api.auth import SessionToken, require_auth
from ai_chat import AIChatError, get_ai_chat_service, owner_key
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR
from pb8_loop_controller import LoopController
from pb8_loop_backend import migrate_loop_bundle
from pb8_loop_store import TERMINAL, LoopDeletionConflict, config_defaults, config_changes, digest, run_deletable, completed_round_observations

SERVICE = 'PB8Loop'
router = APIRouter()
_controller = LoopController(Path(PBGDIR))
PRESETS = {'gain', 'drawdown', 'uptrend', 'consistency', 'trades', 'robustness'}


def configure_restart_gate(gate):
    """Prevent new decisions after the API coordinator reserves its restart."""
    _controller.restart_pending = gate


def restart_block_reason():
    """Expose in-flight Loop AI requests to the shared API restart guard."""
    return _controller.restart_block_reason()


class LoopGoals(BaseModel):
    """Simple explicit goals plus optional user-written requirements."""
    model_config = ConfigDict(extra='forbid')
    coins: list[str] = Field(default_factory=list, max_length=200)
    direction: Literal['config', 'long', 'short', 'both'] = 'config'
    positions: int | None = Field(default=None, ge=1, le=1000)
    positions_by_side: dict[str, int] = Field(default_factory=dict)
    presets: list[str] = Field(default_factory=lambda: ['gain', 'drawdown', 'uptrend'], max_length=6)
    targets: dict[str, float] = Field(default_factory=dict)
    text: str = Field(default='', max_length=8000)

    @model_validator(mode='after')
    def validate_goals(self):
        """Reject ambiguous side capacities and nonfinite goal targets."""
        import re
        if not set(self.presets) <= PRESETS or len(set(self.presets)) != len(self.presets):
            raise ValueError('Choose known unique goal presets')
        if not self.presets and not self.text.strip():
            raise ValueError('Choose a goal preset or describe your own goal')
        if any(not re.fullmatch(r'[A-Za-z0-9._-]{1,40}', coin) for coin in self.coins):
            raise ValueError('Invalid coin identifier')
        self.coins = sorted(set(self.coins))
        if not set(self.targets) <= set(self.presets) or any(not math.isfinite(value) for value in self.targets.values()):
            raise ValueError('Targets must be finite values for chosen goals')
        if self.positions_by_side:
            if set(self.positions_by_side) != {'long', 'short'} or any(type(value) is not int or value < 0 for value in self.positions_by_side.values()):
                raise ValueError('Specify nonnegative Long and Short capacities')
            if self.positions is None or sum(self.positions_by_side.values()) != self.positions:
                raise ValueError('Long plus Short positions must equal the requested total')
            if self.direction in {'long', 'short'} and self.positions_by_side.get('short' if self.direction == 'long' else 'long'):
                raise ValueError('Disabled direction cannot have positions')
        return self


class LoopStart(BaseModel):
    """One bounded authorization for automatic optimizer and validation jobs."""
    model_config = ConfigDict(extra='forbid')
    config_name: str = Field(min_length=1, max_length=120)
    provider: Literal['chatgpt', 'opencode-go', 'opencode-zen']
    model: str = Field(min_length=1, max_length=128)
    profile: str = Field(default='default', max_length=32)
    effort: str = Field(default='', max_length=64)
    service_tier: str = Field(default='', max_length=64)
    execution: Literal['cpu', 'gpu', 'vast'] = 'cpu'
    scenario_enabled: bool = False
    strategy_enabled: bool = False
    goals: LoopGoals = Field(default_factory=LoopGoals)
    run_limit_mode: Literal['config', 'iters', 'proxy', 'hours'] = 'config'
    run_iters: int | None = Field(default=None, ge=1, le=10_000_000)
    run_proxy: int | None = Field(default=None, ge=1, le=1_000_000_000)
    run_hours: float | None = Field(default=None, ge=.001, le=168, allow_inf_nan=False)
    max_runs: int = Field(default=10, ge=1, le=200)
    hours: float = Field(default=24, ge=.1, le=168, allow_inf_nan=False)
    parallel: int = Field(default=1, ge=1, le=16)
    max_validations: int = Field(default=30, ge=1, le=1000)
    candidates: int = Field(default=2, ge=1, le=12)
    patience: int = Field(default=3, ge=1, le=100)
    jev_enabled: bool = True
    authorization: bool = False

    @model_validator(mode='after')
    def validate_run_limit(self):
        """Validate one explicit per-optimizer limit independently of the total loop budget."""
        field = {'iters': self.run_iters, 'proxy': self.run_proxy, 'hours': self.run_hours}
        if self.run_limit_mode != 'config' and field[self.run_limit_mode] is None:
            raise ValueError('Specify the chosen per-run limit')
        if self.run_limit_mode == 'proxy' and self.execution == 'cpu':
            raise ValueError('Proxy evaluation limits require Local GPU or Vast.ai')
        if self.run_limit_mode == 'iters' and self.execution == 'vast' and self.run_iters < 256:
            raise ValueError('Vast.ai requires at least 256 iterations per run')
        if self.run_limit_mode == 'hours' and self.run_hours > self.hours:
            raise ValueError('Per-run hours cannot exceed total loop hours')
        return self


class LoopAction(BaseModel):
    """Explicit routine pause/resume/stop intent, without confirmation dialogs."""
    model_config = ConfigDict(extra='forbid')
    action: Literal['start', 'pause', 'resume', 'stop']
    selection: dict | None = None


def startup():
    """Start only the replaceable API-owned loop controller."""
    _controller.start()


async def shutdown():
    """Join controller tasks without killing detached PB8 jobs."""
    await _controller.close()


def _owner(session):
    return owner_key(session.user_id)


def _error(exc):
    """Preserve native HTTP statuses and log bounded nonsecret failure details."""
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, ValidationError):
        _log(SERVICE, 'Loop request failed: invalid configuration fields', level='WARNING')
        return HTTPException(422, '; '.join(item['msg'] for item in exc.errors(include_input=False, include_context=False))[:1000])
    _log(SERVICE, f'Loop request failed: {type(exc).__name__}', level='WARNING', meta={'traceback': traceback.format_exc()})
    if isinstance(exc, FileNotFoundError):
        return HTTPException(404, 'Loop not found')
    if isinstance(exc, LoopDeletionConflict):
        return HTTPException(409, str(exc))
    if isinstance(exc, (ValueError, AIChatError)):
        return HTTPException(422, str(exc)[:1000])
    return HTTPException(500, 'Loop operation failed; see PBGui log')


def _job_changes(before, after, before_overrides, after_overrides, limit=80):
    """Expose bounded leaf changes, including individual scoring/limit list entries."""
    def flattened(value):
        result = {}
        def visit(item, prefix=''):
            if isinstance(item, dict) and item:
                for key, child in item.items():
                    visit(child, prefix + ('.' if prefix else '') + key)
            elif isinstance(item, list) and item:
                for index, child in enumerate(item):
                    visit(child, prefix + '.' + str(index))
            elif prefix:
                result[prefix] = item
        visit(value)
        return result
    changes = config_changes(flattened(before), flattened(after), limit=limit)
    if len(changes) < limit:
        changes.extend({**item, 'path': 'overrides.' + item['path']} for item in
                       config_changes(flattened(before_overrides), flattened(after_overrides), limit=limit-len(changes)))
    return changes


def _cycle_reports(row):
    """Project real per-cycle exact evidence and timing; unknown legacy data stays unknown."""
    reports = []
    rounds = sorted({job['round'] for job in row['jobs'] if job['round'] >= 0} | {item['round'] for item in row['history']})
    previous_score = None
    previous_goals = {}
    for number in rounds:
        jobs = [job for job in row['jobs'] if job['round'] == number]
        history = next((item for item in row['history'] if item['round'] == number), {})
        assessment = history.get('assessment')
        evidence = history.get('observations') or []
        if not assessment and number == row['round'] and row['phase'] == 'evaluate':
            completed = {job['operation'] for job in jobs if job['kind'] == 'validation' and job['status'] in {'complete', 'completed'}}
            observations = [item for item in row.get('observations', []) if item.get('operation') in completed
                            and item.get('exact') and item['assessment'].get('comparable', True)]
            if not observations:
                observations = [item for item in row.get('observations', []) if item.get('operation') in completed and item.get('exact')]
            if observations:
                assessment = max(observations, key=lambda item: (item['assessment'].get('hard_targets_met', True), item['assessment']['score']))['assessment']
                goal_metrics = {metric for rule in row['rubric'] for metric in rule['metrics']}
                evidence = [{'operation':item['operation'], 'candidate_id':item['candidate']['id'],
                             'optimizer_job':item['candidate']['optimizer_job'], 'assessment':item['assessment'],
                             'metrics':{key:value for key,value in item['metrics'].items() if key in goal_metrics}}
                            for item in observations]
        if assessment and 'hard_targets_met' not in assessment:
            assessment = {**assessment, 'hard_targets_met': all(goal.get('confirmed', False) for goal in assessment.get('goals', []) if goal.get('target') is not None)}
        score = assessment.get('score') if assessment else None
        goals = []
        for index, rule in enumerate(row['rubric']):
            result = (assessment.get('goals') or [])[index] if assessment and index < len(assessment.get('goals') or []) else {}
            key = (rule['goal'], tuple(rule['metrics']))
            value = result.get('value')
            previous = previous_goals.get(key)
            delta = value - previous if value is not None and previous is not None else None
            goals.append({'goal':rule['goal'], 'metrics':rule['metrics'], 'direction':rule['direction'],
                          'target':rule.get('target'), 'value':value, 'previous':previous, 'delta':delta,
                          'improved': None if delta is None else delta > 0 if rule['direction']=='max' else delta < 0,
                          'confirmed':bool(result.get('confirmed'))})
            if value is not None:
                previous_goals[key] = value
        starts = [job.get('created_at') or job.get('run_started_at') for job in jobs]
        starts = [value for value in starts if isinstance(value, (int, float))]
        active = any(job['status'] not in {'completed','complete','cancelled','failed','error','stopped'} for job in jobs) or (number==row['round'] and row['status'] not in TERMINAL and not history.get('evaluated_at'))
        end = None if active else history.get('evaluated_at') or max([job.get('ended_at') or job.get('last_observed_at') or 0 for job in jobs], default=0) or None
        start = min(starts) if starts else None
        duration = max(0, (time.time() if active else end) - start) if start and (active or end) else None
        checks = [job for job in jobs if job['kind']=='validation']
        passed = sum(job['status'] in {'completed','complete'} for job in checks)
        status = ('incomplete' if assessment.get('simulation_complete') is False else 'target violated' if assessment.get('hard_targets_met') is False else 'validated') if assessment else row['phase'] if number==row['round'] and not row['status'] in TERMINAL else 'not validated'
        if not assessment and any(job['status'] in {'failed','error'} for job in jobs):
            status = 'failed'
        reports.append({'round':number, 'status':status, 'started_at':start, 'ended_at':end,
                        'duration_seconds':duration, 'duration_estimated':any(not job.get('created_at') for job in jobs),
                        'active':active, 'backtests':len(checks), 'completed_backtests':passed,
                        'score':score, 'score_delta':score-previous_score if score is not None and previous_score is not None else None,
                        'goals':goals, 'reason':history.get('reason',''), 'knowledge':history.get('knowledge',''),
                        'observations':evidence})
        if score is not None:
            previous_score = score
    return reports


def _observer_reports(row):
    """Expose user-only metrics/deltas without mixing them into decision history."""
    from pb8_loop_observer import comparison_deltas, observer_selections
    jobs = row.get('observer_jobs', [])
    selections = {item['round']: item for item in observer_selections(row)}
    def reference(number, kind):
        """Compare against the displayed round winner, never the first candidate."""
        selection = selections.get(number) or {}
        candidates = [item for item in jobs if item['kind'] == kind and item['round'] == number]
        if selection.get('status') == 'selected':
            return next((item for item in candidates if item.get('candidate_id') == selection['candidate_id']), None)
        if not row.get('observer_enabled'):
            return next(iter(candidates), None)
        return None
    reports = []
    for job in jobs:
        backtest = job.get('config', {}).get('backtest', {})
        windows = backtest.get('scenarios') if backtest.get('suite_enabled') else None
        baseline = reference(-1, job['kind'])
        previous = reference(job['round'] - 1, job['kind'])
        selection = selections.get(job['round']) or {}
        checks = [item for item in row.get('jobs', []) if item['kind'] == 'validation' and item['round'] == job['round']]
        index = next((number + 1 for number, item in enumerate(checks)
                      if (item.get('candidate') or {}).get('id') == job.get('candidate_id')), None)
        score = (job.get('assessment') or {}).get('score')
        reports.append({key:job.get(key) for key in ('operation','name','kind','round','status','started','candidate_id',
                       'created_at','ended_at','run_started_at','error','metrics','reports','simulation_complete','report_done')} | {
                       'observer_only':True, 'execution':'cpu', 'log_available':bool((job.get('backend') or {}).get('id')),
                       'comparison_label': 'Comparison backtest ' + str(index) if index else job.get('candidate_id'),
                       'evaluation_score':score if type(score) in (int,float) and math.isfinite(score) else None,
                       'evaluation_targets_met':(job.get('assessment') or {}).get('hard_targets_met') is True,
                       'holdout_score':score if job['kind']=='observer_holdout' and type(score) in (int,float) and math.isfinite(score) else None,
                       'selected_for_full_range': selection.get('status') == 'selected' and job.get('candidate_id') == selection.get('candidate_id'),
                       'previous_selection': job['kind']=='observer_full_range' and selection.get('status') == 'selected' and job.get('candidate_id') != selection.get('candidate_id'),
                       'windows':[{key:window.get(key, backtest.get(key)) for key in ('label','start_date','end_date','exchanges')}
                                  for window in windows or [backtest]],
                       'delta_start':comparison_deltas(job, baseline) if job['round'] >= 0 else {},
                       'delta_previous':comparison_deltas(job, previous) if job['round'] >= 0 else {},
                       'display_status':('incomplete' if job.get('simulation_complete') is False else 'report error')
                       if job.get('report_done') and job['status'] in {'complete','completed'} and not job.get('simulation_complete') else job['status']})
    return reports


def projection(row):
    """Keep polling lightweight and avoid exposing internal paths or config copies."""
    best = row.get('best')
    from pb8_loop_observer import observer_selections
    return {key: row.get(key) for key in ('id', 'status', 'phase', 'round', 'created_at', 'started_at', 'updated_at', 'reason',
            'last_error', 'failure', 'holdout_retryable', 'stop_reason', 'continuation', 'interpretation', 'rubric', 'history', 'bootstrap', 'ai_calls', 'ai_tokens_reserved',
            'ai_usd_reserved', 'jev_usd_reserved', 'validation_count', 'holdout_used', 'deadline')} | {
        'name': row['settings'].get('loop_name') or row['settings']['config_name'],
        'definition_name': row['settings'].get('definition_name'),
        'settings': row['settings'], 'fingerprint': row['fingerprint'], 'cycles': _cycle_reports(row),
        'observer_enabled':bool(row.get('observer_enabled')), 'observer_jobs':_observer_reports(row),
        'observer_selections':observer_selections(row),
        'comparison': {key: row['comparison'].get(key) for key in ('start_date','end_date','exchanges','suite_enabled')},
        'baseline': {key: row['baseline'][key] for key in ('assessment', 'metrics') if key in row['baseline']} if row.get('baseline') else None,
        'final_validation': [{key:item[key] for key in ('assessment', 'metrics', 'reports') if key in item} for item in row.get('final_validation', [])],
        'holdout_available': bool(row['holdouts']),
        'best': {'assessment': best['assessment'], 'metrics': best['metrics'],
                 'candidate_id': best['candidate']['id'], 'round': next((job['round'] for job in row['jobs'] if job['operation'] == best.get('operation')), None)} if best else None,
        'jobs': [{key: job.get(key) for key in ('operation', 'name', 'kind', 'round', 'status', 'started', 'reason', 'error', 'runner', 'executed_digest', 'run_started_at', 'limit_reached', 'results_consumed', 'result_count', 'result_error', 'created_at', 'ended_at')} | {
                    'changes': _job_changes(row['initial_config'], job['config'], row['initial_overrides'], job['overrides']) if job['kind'] == 'optimizer' else [],
                    'native_iters': job['config'].get('optimize', {}).get('iters') if job['kind'] == 'optimizer' else None,
                    'candidate_optimizer': (job.get('candidate') or {}).get('optimizer_job'),
                    'log_available': bool((job.get('backend') or {}).get('id')),
                    'execution': (job.get('backend') or {}).get('execution') or (row['settings']['execution'] if job['kind'] == 'optimizer' else 'cpu'),
                    'display_status': 'Proxy limit reached' if job.get('limit_reached') == 'proxy' else 'Time limit reached' if job.get('limit_reached') else job['status'],
                    'proxy_limit': row['settings'].get('run_proxy') if job['kind'] == 'optimizer' else None,
                 } for job in row['jobs']],
        'jev_answers': (row.get('bootstrap_jev_answers') or []) + (row.get('jev_answers') or []),
        'jev_status': 'disabled' if not row['settings'].get('jev_enabled') else 'consulted' if row.get('jev_usd_reserved', 0) else 'failed' if any('failed' in str(answer).lower() for answer in (row.get('bootstrap_jev_answers') or []) + (row.get('jev_answers') or [])) else 'not requested',
        'usd_status': 'reserved_estimate' if row['settings']['provider'] == 'opencode-zen' else 'subscription',
        'ended': row['status'] in TERMINAL,
        'deletable': run_deletable(row),
    }


@router.get('/options')
async def options(config_name: str = Query(default='', max_length=120), session: SessionToken = Depends(require_auth), source_kind: str = '', source_id: str = '', list_only: bool = False):
    """Offer only compatible local GPU execution and the saved Vast limits."""
    try:
        from api import optimize_v8 as opt
        from api.vast import RentalPreferences
        from vast_queue import CloudQueue
        from vast_credentials import VastCredentialStore
        configs = await asyncio.to_thread(lambda: [row['name'] for row in opt.list_configs(session, include_result_summary=False)['configs']])
        if config_name:
            opt._validate_name(config_name)
        selected_name = config_name if config_name in configs else (configs[0] if configs else '')
        defaults, bundle = None, None
        missing = config_name if config_name and config_name not in configs and not source_kind else ''
        if source_kind and not list_only:
            bundle, selected_name, _source, _settings = await asyncio.to_thread(_source_bundle, LoopSource(kind=source_kind, id=source_id), session)
            defaults = config_defaults(bundle['config'])
        elif selected_name and not list_only:
            bundle = await asyncio.to_thread(opt.get_config, selected_name, session)
            defaults = config_defaults(bundle['config'])
        gpu_available, gpu_reason = False, 'Choose a configuration to check GPU compatibility'
        if selected_name and not list_only:
            settings = LoopStart(config_name=selected_name, provider='chatgpt', model='probe', execution='gpu').model_dump()

            try:
                args = (selected_name, settings, bundle) if source_kind else (selected_name, settings)
                await asyncio.to_thread(_controller.backend.initial, *args)
                gpu_available, gpu_reason = True, ''
            except Exception as exc:
                gpu_reason = str(exc)[:500]
        queue = CloudQueue()
        vast = RentalPreferences.model_validate(queue.read().get('gpu_preferences') or {}).model_dump()
        return JSONResponse({'configs': configs, 'config_name': selected_name, 'config_defaults': defaults, 'missing_config': missing,
                             'bundle_digest': digest({'config': bundle['config'], 'override_configs': bundle.get('override_configs') or {}}) if bundle else None,
                             'gpu_available': gpu_available, 'gpu_reason': gpu_reason,
                             'vast': vast, 'vast_connected': VastCredentialStore(queue.root).metadata().get('configured', False),
                             'presets': sorted(PRESETS)}, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/models')
async def models(provider: str, profile: str = 'default', session: SessionToken = Depends(require_auth)):
    """List explicitly selected provider models for the owning account."""
    try:
        if provider not in {'chatgpt', 'opencode-go', 'opencode-zen'}:
            raise ValueError('Unsupported loop provider')
        value = await get_ai_chat_service().models(_owner(session), provider, profile=profile)
        return JSONResponse({'models': [row for row in value if not row.get('decision')]}, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/knowledge')
async def knowledge(session: SessionToken = Depends(require_auth), limit: int = Query(20, ge=1, le=200)):
    """Read recent owner-scoped knowledge and its evidence provenance."""
    try:
        value = await asyncio.to_thread(_controller.store.knowledge, _owner(session))
        return JSONResponse({'entries': value['entries'][-limit:], 'used_holdouts': value['used_holdouts']}, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.get('')
async def list_loops(session: SessionToken = Depends(require_auth)):
    try:
        rows = await asyncio.to_thread(_controller.store.list, _owner(session))
        return JSONResponse({'loops': [projection(row) for row in rows]}, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


async def _start_loop(body: LoopStart, session: SessionToken = Depends(require_auth), definition=None, *, owner=None, queue_proposal=None):
    """Authorize one full bounded loop at start, without subsequent approval prompts."""
    owner = owner if owner is not None else _owner(session)
    try:
        if not body.authorization:
            raise HTTPException(422, 'Authorize automatic configuration changes, job starts and AI usage when starting the loop')
        settings = body.model_dump(exclude={'authorization'})
        if queue_proposal is not None:
            settings['ai_queue_proposal'] = queue_proposal
        ai_preferences = get_ai_chat_service().get_preferences(owner)
        settings['jev_max_cost_usd'] = ai_preferences['jev_max_cost_usd']
        settings['jev_budget_usd'] = settings['jev_max_cost_usd'] * body.max_runs
        if body.execution == 'vast':
            from api.vast import RentalPreferences
            from vast_queue import CloudQueue
            from vast_credentials import VastCredentialStore
            if not VastCredentialStore(CloudQueue().root).metadata()['configured']:
                raise ValueError('Connect Vast.ai before starting a cloud loop')
            settings['vast'] = RentalPreferences.model_validate(CloudQueue().read().get('gpu_preferences') or {}).model_dump()
            rental_hours = settings['vast']['hours']
            if 'hours' not in body.model_fields_set:
                settings['hours'] = rental_hours
            if settings['hours'] > rental_hours:
                raise ValueError(f'Loop duration cannot exceed Rental & Automation duration ({rental_hours:g} hours) for Vast.ai')
            settings['parallel'] = min(settings['parallel'], settings['vast']['max_rentals'])
        if settings['run_limit_mode'] == 'hours' and settings['run_hours'] > settings['hours']:
            raise ValueError('Per-run hours cannot exceed total loop hours')
        if definition is not None:
            settings.update(loop_name=definition['name'], definition_name=definition['name'], source=definition['source'])
            row = await _controller.create(owner, settings, definition['bundle'], queued=True)
        else:
            row = await _controller.create(owner, settings)
        return JSONResponse(projection(row), status_code=201, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.post('', status_code=201)
async def start_loop(body: LoopStart, session: SessionToken = Depends(require_auth)):
    return await _start_loop(body, session)


class LoopSource(BaseModel):
    """Reference an accessible PB8 configuration, native job or result snapshot."""
    model_config = ConfigDict(extra='forbid')
    kind: Literal['config', 'queue', 'result', 'cloud', 'run', 'definition']
    id: str = Field(min_length=1, max_length=1024)


class LoopDefinitionSave(BaseModel):
    """Save settings using a server-resolved snapshot, never an arbitrary path."""
    model_config = ConfigDict(extra='forbid')
    settings: dict
    source: LoopSource
    revision: int | None = Field(default=None, ge=1)
    queue_id: str | None = Field(default=None, pattern=r'^[a-f0-9]{32}$')
    source_digest: str | None = Field(default=None, pattern=r'^[a-f0-9]{64}$')


def _source_bundle(source, session):
    """Resolve source provenance and overrides under the existing PB8 boundaries."""
    from api import optimize_v8 as opt
    owner = _owner(session)
    settings = None
    provenance = {'kind': source.kind, 'id': source.id}
    if source.kind == 'config':
        with opt._config_lock():
            bundle = opt.get_config(source.id, session)
        name = source.id
    elif source.kind == 'queue':
        with opt._queue_lock():
            data = opt._read_json(opt._queue_file(source.id))
            if data.get('loop_id') and data.get('loop_owner') != owner:
                raise FileNotFoundError('Queue entry not found')
            bundle = opt.get_queue_config(source.id, session)
        name = bundle['name']
    elif source.kind == 'result':
        with opt._result_lock():
            bundle = opt.get_result_config(source.id, session)
            directory = opt._resolve_result_path(source.id)
            name, _ = _controller.backend._result_origin(next((row for row in opt._list_results() if row['path'] == source.id), {'path': source.id, 'name': directory.name, 'result': directory.name}))
        name = name or directory.name
        provenance['result_path'] = source.id
    elif source.kind == 'cloud':
        from vast_jobs import JobStore
        from pb8_config import load_pb8_config
        store = JobStore()
        data = store.read(source.id)
        if data.get('kind') in {'worker', 'calibration'} or data.get('loop_id') and data.get('loop_owner') != owner:
            raise ValueError('Select an optimizer job')
        path = store.directory(source.id) / 'input/optimize.json'
        if not path.is_file() or path.is_symlink():
            raise ValueError('The cloud optimizer snapshot is not prepared yet')
        config = load_pb8_config(path)
        bundle = {'config': config, 'override_configs': opt._load_override_payloads(config, path.parent)}
        name = data['config_name']
    elif source.kind == 'run':
        row = _controller.store.read(owner, source.id)
        bundle = {'config': row['initial_config'], 'override_configs': row['initial_overrides']}
        settings = row['settings']
        name = settings['config_name']
        provenance = settings.get('source') or provenance
    else:
        row = _controller.store.definition(owner, source.id)
        bundle, settings = row['bundle'], row['settings']
        name = settings['config_name']
        provenance = row['source']
    if bundle.get('migration_unresolved'):
        raise ValueError('Starting config requires an explicit HSL choice: '
                         + ', '.join(bundle['migration_unresolved'])
                         + '. Open the PB8 starting config, select its HSL policy and Save.')
    return {'config': copy.deepcopy(bundle['config']), 'override_configs': copy.deepcopy(bundle.get('override_configs') or {})}, name, provenance, settings


def _definition_projection(row):
    """Return editor settings, source identity and defaults without raw configs."""
    return {key: row[key] for key in ('name', 'revision', 'created_at', 'updated_at', 'settings', 'source')} | {
        'config_defaults': config_defaults(row['bundle']['config']), 'bundle_digest': digest(row['bundle'])}


@router.post('/sources')
async def loop_source(body: LoopSource, session: SessionToken = Depends(require_auth)):
    """Open an unsaved loop draft from the selected native snapshot."""
    try:
        bundle, name, source, settings = await asyncio.to_thread(_source_bundle, body, session)
        return JSONResponse({'config_name': name, 'source': source, 'settings': settings,
                             'config_defaults': config_defaults(bundle['config']),
                             'bundle_digest': digest(bundle),
                             'execution': 'vast' if bundle['config'].get('pbgui', {}).get('execution') == 'vast' else 'gpu' if bundle['config'].get('optimize', {}).get('backend') == 'gpu' else 'cpu'}, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/configs')
async def loop_configs(session: SessionToken = Depends(require_auth)):
    """List named loop definitions belonging to the signed-in owner."""
    try:
        rows = await asyncio.to_thread(_controller.store.definitions, _owner(session))
        return JSONResponse({'configs': [_definition_projection(row) for row in rows]}, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.put('/configs/{name}')
async def save_loop_config(name: str, body: LoopDefinitionSave, session: SessionToken = Depends(require_auth)):
    """Save a normal named config; refresh only its explicitly edited waiting run."""
    try:
        bundle, config_name, source, _ = await asyncio.to_thread(_source_bundle, body.source, session)
        if body.source_digest and body.source_digest != digest(bundle):
            raise HTTPException(409, 'The starting snapshot changed. Open Edit or Create AI Loop again before saving.')
        bundle = await asyncio.to_thread(migrate_loop_bundle, bundle)
        raw = dict(body.settings, config_name=config_name, provider='chatgpt', model='shared-selection')
        validated = LoopStart.model_validate(raw).model_dump(exclude={'authorization','provider','model','profile','effort','service_tier'})
        # Rental settings constrain the editor as well as Queue/Start.
        if validated['execution'] == 'vast':
            from api.vast import RentalPreferences
            from vast_queue import CloudQueue
            rental = RentalPreferences.model_validate(CloudQueue().read().get('gpu_preferences') or {})
            if validated['hours'] > rental.hours or validated['parallel'] > rental.max_rentals:
                raise ValueError('Loop duration and concurrency must fit Rental & Automation')
        owner = _owner(session)
        prepared = None
        if body.queue_id:
            waiting = await asyncio.to_thread(_controller.store.read, owner, body.queue_id)
            if waiting['status'] != 'queued' or waiting['jobs']:
                raise ValueError('Only a waiting loop can be edited; this run has already started')
            prepared = await asyncio.to_thread(_controller.backend.initial, config_name, validated, bundle)
        row = await asyncio.to_thread(_controller.store.save_definition, owner, name, validated, bundle, source, body.revision)
        if body.queue_id:
            def refresh(current):
                if current['status'] != 'queued' or current['jobs'] or current['control_generation'] != waiting['control_generation']:
                    raise ValueError('Only a waiting loop can be edited; this run has already started')
                current['settings'] = dict(current['settings'], **validated, loop_name=name, definition_name=name, source=source)
                config, overrides, fingerprint, holdouts, comparison = prepared
                current.update(initial_config=config, initial_overrides=overrides, fingerprint=fingerprint, holdouts=holdouts, comparison=comparison)
                current['control_generation'] += 1
            await asyncio.to_thread(_controller.store.update, owner, body.queue_id, refresh)
        return JSONResponse(_definition_projection(row), headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.delete('/configs/{name}')
async def delete_loop_config(name: str, session: SessionToken = Depends(require_auth)):
    """Delete a saved definition without deleting history."""
    try:
        await asyncio.to_thread(_controller.store.delete_definition, _owner(session), name)
        return {'deleted': True}
    except Exception as exc:
        raise _error(exc) from exc


@router.post('/configs/{name}/queue', status_code=201)
async def queue_loop_config(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Queue an immutable run snapshot with the current shared AI selection."""
    try:
        if not set(body) <= {'provider','model','profile','effort','service_tier','authorization'}:
            raise ValueError('Queue uses the saved loop settings and shared AI selection')
        definition = await asyncio.to_thread(_controller.store.definition, _owner(session), name)
        start = LoopStart.model_validate(dict(definition['settings'], **body))
        # Reuse the same resource and authorization validation as direct starts.
        response = await _start_loop(start, session, definition)
        return response
    except Exception as exc:
        raise _error(exc) from exc


class LoopInstructionSave(BaseModel):
    """Save a new named version without replacing an existing prompt."""
    model_config = ConfigDict(extra='forbid')
    name: str = Field(min_length=1, max_length=80)
    text: str = Field(min_length=1, max_length=64000)
    revision: int = Field(ge=0, strict=True)


class LoopInstructionActivate(BaseModel):
    """Select an owned instruction version for future runs."""
    model_config = ConfigDict(extra='forbid')
    version_id: str = Field(pattern=r'^(default|[0-9a-f]{32})$')
    revision: int = Field(ge=0, strict=True)


class LoopInstructionDelete(BaseModel):
    """Delete an owned version only against the displayed catalog revision."""
    model_config = ConfigDict(extra='forbid')
    revision: int = Field(ge=0, strict=True)


@router.get('/instructions')
async def loop_instructions(session: SessionToken = Depends(require_auth)):
    """List instruction metadata without returning every prompt on each poll."""
    try:
        catalog = await asyncio.to_thread(_controller.store.instructions, _owner(session))
        return JSONResponse({'revision': catalog['revision'], 'active': catalog['active'],
            'versions': [{key: item[key] for key in ('id', 'name', 'digest', 'created_at')}
                         for item in catalog['versions']]}, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.post('/instructions', status_code=201)
async def save_loop_instructions(body: LoopInstructionSave, session: SessionToken = Depends(require_auth)):
    """Append and activate a private immutable instruction version."""
    from pb8_loop_store import LoopInstructionConflict
    try:
        version = await asyncio.to_thread(_controller.store.save_instructions, _owner(session), body.name, body.text, body.revision)
        return JSONResponse(version, status_code=201, headers={'Cache-Control': 'no-store'})
    except LoopInstructionConflict as exc:
        _log(SERVICE, 'Instruction save rejected: stale revision', level='WARNING')
        raise HTTPException(409, str(exc)) from exc
    except Exception as exc:
        raise _error(exc) from exc


@router.post('/instructions/active')
async def activate_loop_instructions(body: LoopInstructionActivate, session: SessionToken = Depends(require_auth)):
    """Change the future-run default without mutating queued or running snapshots."""
    from pb8_loop_store import LoopInstructionConflict
    try:
        version = await asyncio.to_thread(_controller.store.activate_instructions, _owner(session), body.version_id, body.revision)
        return JSONResponse(version, headers={'Cache-Control': 'no-store'})
    except LoopInstructionConflict as exc:
        _log(SERVICE, 'Instruction selection rejected: stale revision', level='WARNING')
        raise HTTPException(409, str(exc)) from exc
    except Exception as exc:
        raise _error(exc) from exc


@router.delete('/instructions/{version_id}')
async def delete_loop_instructions(version_id: str, body: LoopInstructionDelete, session: SessionToken = Depends(require_auth)):
    """Remove a personal version without changing historical run snapshots."""
    from pb8_loop_store import LoopInstructionConflict
    try:
        result = await asyncio.to_thread(_controller.store.delete_instructions, _owner(session), version_id, body.revision)
        return JSONResponse(result, headers={'Cache-Control': 'no-store'})
    except LoopInstructionConflict as exc:
        _log(SERVICE, 'Instruction deletion rejected: stale revision', level='WARNING')
        raise HTTPException(409, str(exc)) from exc
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/instructions/{version_id}')
async def loop_instruction_version(version_id: str, session: SessionToken = Depends(require_auth)):
    """Read only versions belonging to the authenticated owner."""
    try:
        version = await asyncio.to_thread(_controller.store.instruction_version, _owner(session), version_id)
        return JSONResponse(version, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/{loop_id}/instructions')
async def loop_run_instructions(loop_id: str, session: SessionToken = Depends(require_auth)):
    """Show the exact instruction snapshot used by a run, or label legacy fallback."""
    from pb8_loop_ai import instructions_for_run
    try:
        row = await asyncio.to_thread(_controller.store.read, _owner(session), loop_id)
        return JSONResponse(instructions_for_run(row), headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/{loop_id}')
async def get_loop(loop_id: str, session: SessionToken = Depends(require_auth)):
    try:
        row = await asyncio.to_thread(_controller.store.read, _owner(session), loop_id)
        return JSONResponse(projection(row), headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.delete('/{loop_id}')
async def delete_loop(loop_id: str, session: SessionToken = Depends(require_auth)):
    """Delete an inactive owned run and its round history without deleting its config."""
    try:
        await asyncio.to_thread(_controller.delete, _owner(session), loop_id)
        return {'deleted': True}
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/{loop_id}/diff')
async def config_diff(loop_id: str, operation: str = Query(pattern=r'^[0-9a-f]{32}$'),
                      baseline: str = Query(default='initial', pattern=r'^(initial|[0-9a-f]{32})$'),
                      session: SessionToken = Depends(require_auth)):
    """Compare two snapshots within this owner's loop without exposing full configs."""
    try:
        row = await asyncio.to_thread(_controller.store.read, _owner(session), loop_id)
        job = next((item for item in row['jobs'] if item['operation']==operation and item['kind']=='optimizer'), None)
        reference = next((item for item in row['jobs'] if item['operation']==baseline and item['kind']=='optimizer'), None)
        if job is None or (baseline!='initial' and reference is None):
            raise HTTPException(404, 'Optimizer comparison snapshot not found in this loop')
        before = reference['config'] if reference else row['initial_config']
        overrides = reference['overrides'] if reference else row['initial_overrides']
        fields = _job_changes(before, job['config'], overrides, job['overrides'], limit=4096)
        return JSONResponse({'fields':fields, 'truncated':len(fields)>=4096}, headers={'Cache-Control':'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/{loop_id}/log-target')
async def log_target(loop_id: str, operation: str = Query(pattern=r'^[0-9a-f]{32}$'),
                     session: SessionToken = Depends(require_auth)):
    """Resolve an owned optimizer operation to its verified native queue log window."""
    try:
        row = await asyncio.to_thread(_controller.store.read, _owner(session), loop_id)
        job = next((item for item in row['jobs'] + row.get('observer_jobs', []) if item['operation'] == operation), None)
        if job is None or not (job.get('backend') or {}).get('id'):
            raise HTTPException(404, 'The native optimizer job has not been created or is no longer available.')
        # The native queue/job record must still belong to this loop and owner.
        await asyncio.to_thread(_controller.backend.poll, row, job)
        return JSONResponse({'id': job['backend']['id'], 'cloud': job['backend']['execution'] == 'vast',
                             'name': job['name'], 'kind': job['kind']}, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.get('/{loop_id}/best-config')
async def best_config(loop_id: str, session: SessionToken = Depends(require_auth)):
    """Return the best complete candidate bundle with no automatic bot deployment."""
    try:
        row = await asyncio.to_thread(_controller.store.read, _owner(session), loop_id)
        if not row.get('best'):
            raise HTTPException(409, 'No exact comparison candidate is available yet')
        candidate = row['best']['candidate']
        return JSONResponse({'config': candidate['config'], 'override_configs': candidate['overrides'],
                             'assessment': row['best']['assessment'], 'final_validation': row.get('final_validation'),
                             'status': row['status'], 'provisional': row['status'] != 'completed', 'baseline': row.get('baseline')},
                            headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.post('/{loop_id}/action')
async def loop_action(loop_id: str, body: LoopAction, session: SessionToken = Depends(require_auth)):
    return await _loop_action_owned(_owner(session), loop_id, body)


async def _loop_action_owned(owner, loop_id, body):
    """Run native owner-scoped actions for authenticated routes and reviewed AI tools."""
    try:
        if body.action == 'start':
            waiting = await asyncio.to_thread(_controller.store.read, owner, loop_id)
            if waiting['status'] != 'queued':
                raise ValueError('Only a queued loop can be started')
            allowed = set(LoopStart.model_fields) - {'authorization'}
            raw = {key: value for key, value in waiting['settings'].items() if key in allowed}
            if body.selection:
                if not set(body.selection) <= {'provider','model','profile','effort','service_tier'}:
                    raise ValueError('Invalid shared AI selection')
                raw.update(body.selection)
            settings = LoopStart.model_validate(raw).model_dump(exclude={'authorization'})
            if settings['execution'] == 'vast':
                from api.vast import RentalPreferences
                from vast_queue import CloudQueue
                from vast_credentials import VastCredentialStore
                queue = CloudQueue()
                if not VastCredentialStore(queue.root).metadata()['configured']:
                    raise ValueError('Connect Vast.ai before starting a cloud loop')
                settings['vast'] = RentalPreferences.model_validate(queue.read().get('gpu_preferences') or {}).model_dump()
                if settings['hours'] > settings['vast']['hours'] or settings['parallel'] > settings['vast']['max_rentals']:
                    raise ValueError('Edit this waiting loop to fit the current Rental & Automation settings')
            preferences = get_ai_chat_service().get_preferences(owner)
            settings.update(jev_max_cost_usd=preferences['jev_max_cost_usd'], jev_budget_usd=preferences['jev_max_cost_usd'] * settings['max_runs'])
            await _controller.ai.preflight(owner, settings)
            bundle = {'config': waiting['initial_config'], 'override_configs': waiting['initial_overrides']}
            config, overrides, fingerprint, holdouts, comparison = await asyncio.to_thread(_controller.backend.initial, settings['config_name'], settings, bundle)
            exclusions = copy.deepcopy(holdouts)
            used = _controller.store.knowledge(owner)['used_holdouts']
            holdouts = [window for window in holdouts if not any(item.get('start_date', '') <= window['end_date'] and window['start_date'] <= item.get('end_date', '') for item in used)]
            def refresh(current):
                if current['status'] != 'queued' or current['control_generation'] != waiting['control_generation']:
                    raise ValueError('This queue entry changed while starting')
                current['settings'].update(settings)
                current.update(initial_config=config, initial_overrides=overrides, fingerprint=fingerprint, holdouts=holdouts, comparison=comparison,
                               training_exclusions=current.get('training_exclusions', []) + exclusions, status='running', started_at=time.time(), deadline=time.time() + settings['hours'] * 3600, control_generation=current['control_generation'] + 1)
            row = await asyncio.to_thread(_controller.store.update, owner, loop_id, refresh)
        else:
            row = await asyncio.to_thread(_controller.action, owner, loop_id, body.action)
        return JSONResponse(projection(row), headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


class LoopContinue(BaseModel):
    """Continue an exact checkpoint with a new bounded authorization and provenance."""
    model_config = ConfigDict(extra='forbid')
    rounds: int = Field(default=5, ge=1, le=200)
    round: int | None = Field(default=None, ge=0)
    selection: dict = Field(default_factory=dict)
    authorization: bool = False


@router.post('/{loop_id}/continue')
async def continue_loop(loop_id: str, body: LoopContinue, session: SessionToken = Depends(require_auth)):
    """Create a new queue entry from the retained best or selected exact round winner."""
    try:
        if not body.authorization:
            raise ValueError('Authorize the additional autonomous rounds')
        if not set(body.selection) <= {'provider', 'model', 'profile', 'effort', 'service_tier'}:
            raise ValueError('Invalid shared AI selection')
        row = await asyncio.to_thread(_controller.store.read, _owner(session), loop_id)
        checkpoint = row.get('best')
        if body.round is not None:
            candidates = completed_round_observations(row, body.round)
            if not candidates:
                raise ValueError('This round has no complete exact checkpoint')
            verified = []
            for item in candidates:
                job = next((job for job in row['jobs'] if job['operation'] == item['operation']), None)
                if not job or job['status'] not in {'completed', 'complete'}:
                    continue
                exact = await asyncio.to_thread(_controller.backend.observations, row, job)
                if exact['assessment'].get('comparable') and exact['assessment'].get('simulation_complete', True):
                    verified.append({**item, 'assessment': exact['assessment']})
            if not verified:
                raise ValueError('This round has no complete exact checkpoint')
            candidates = verified
            chosen = max(candidates, key=lambda item: (item['assessment'].get('hard_targets_met', True), item['assessment']['score']))
            job = next((job for job in row['jobs'] if job['operation'] == chosen['operation']), None)
            checkpoint = {'candidate': job['candidate'], 'metrics': chosen.get('metrics', {})} if job else None
        if not checkpoint or not checkpoint['candidate']:
            raise ValueError('No complete exact checkpoint is available')
        if body.round is None:
            job = next((item for item in row['jobs'] if item['operation'] == checkpoint.get('operation')), None)
            if not job or job['status'] not in {'completed', 'complete'}:
                raise ValueError('The best checkpoint has no successful exact backtest')
            exact = await asyncio.to_thread(_controller.backend.observations, row, job)
            if not exact['assessment'].get('comparable') or exact['assessment'].get('simulation_complete') is False:
                raise ValueError('The best checkpoint simulation is incomplete')
            checkpoint = {**checkpoint, 'assessment': exact['assessment'], 'metrics': exact['metrics']}
        if checkpoint.get('assessment', {}).get('comparable') is False or checkpoint.get('assessment', {}).get('simulation_complete') is False:
            raise ValueError('The checkpoint simulation is incomplete')
        candidate = copy.deepcopy(checkpoint['candidate'])
        candidate['metrics'] = checkpoint.get('metrics') or candidate.get('metrics', {})
        candidate['source'] = {'run': row['id'], 'round': body.round, 'kind': 'exact_comparison'}
        optimizer = next((job for job in row['jobs'] if job['operation'] == candidate.get('optimizer_job')), None)
        config = copy.deepcopy(optimizer['config'] if optimizer else row['initial_config'])
        config['bot'] = copy.deepcopy(candidate['config']['bot'])
        config['live'] = copy.deepcopy(candidate['config']['live'])
        config['backtest'] = copy.deepcopy(row['comparison'])
        raw = {key: value for key, value in row['settings'].items() if key in LoopStart.model_fields and key != 'authorization'}
        raw.update(body.selection)
        raw.update(max_runs=body.rounds, patience=min(100, max(row['settings']['patience'], body.rounds)),
                   max_validations=min(1000, body.rounds * row['settings']['candidates'] + 2), authorization=True)
        if raw['execution'] == 'vast':
            from api.vast import RentalPreferences
            from vast_queue import CloudQueue
            rental = RentalPreferences.model_validate(CloudQueue().read().get('gpu_preferences') or {})
            raw.update(hours=min(raw['hours'], rental.hours), parallel=min(raw['parallel'], rental.max_rentals))
            if raw['run_limit_mode'] == 'hours':
                raw['run_hours'] = min(raw['run_hours'], raw['hours'])
        name = (row['settings'].get('loop_name') or row['settings']['config_name'])[:85] + '_continue'
        definition = {'name': name, 'source': {'kind': 'run', 'id': row['id']},
                      'bundle': {'config': config, 'override_configs': candidate['overrides']}}
        response = await _start_loop(LoopStart.model_validate(raw), session, definition)
        import json
        result = json.loads(response.body)
        await asyncio.to_thread(_controller.store.update, _owner(session), result['id'],
            lambda current: current.update(training_exclusions=current.get('training_exclusions', []) + row.get('training_exclusions', []) + row['holdouts'], continuation={'parent': row['id'], 'round': body.round, 'candidate_id': candidate['id'], 'additional_runs': body.rounds}))
        def inherited(current):
            current.update(required_rounds=body.rounds, rubric=copy.deepcopy(row['rubric']), phase='bootstrap', interpretation=row.get('interpretation', ''),
                           bootstrap_candidates=[copy.deepcopy(candidate)],
                           bootstrap={'status': 'evaluating', 'sources': [row['id']], 'candidate_count': 1,
                                      'candidates': [{'id': candidate['id'], 'metrics': checkpoint.get('metrics') or candidate.get('metrics', {})}]})
        await asyncio.to_thread(_controller.store.update, _owner(session), result['id'], inherited)
        await loop_action(result['id'], LoopAction(action='start', selection=body.selection), session)
        current = await asyncio.to_thread(_controller.store.read, _owner(session), result['id'])
        return JSONResponse(projection(current), status_code=201, headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc


@router.post('/{loop_id}/retry-holdout')
async def retry_holdout(loop_id: str, session: SessionToken = Depends(require_auth)):
    """Retry only an unchanged winner after a proven pre-data configuration failure."""
    try:
        row = await asyncio.to_thread(_controller.store.read, _owner(session), loop_id)
        if row['status'] != 'unconfirmed' or not row.get('holdout_retryable') or row.get('holdout_retry_count', 0) >= 1:
            raise ValueError('This holdout is not eligible for an unchanged technical retry')
        if row['validation_count'] >= row['settings']['max_validations'] or time.time() >= row['deadline'] - 60:
            raise ValueError('The original validation/time allowance is exhausted')
        candidate = row['best']['candidate']
        config = _controller.backend.validation_config(row, candidate, True)
        job = _controller.job(row, 'holdout', config, candidate['overrides'], 'Unchanged technical holdout retry', candidate=candidate)
        def retry(current):
            if current['status'] != row['status'] or current['control_generation'] != row['control_generation'] or not current.get('holdout_retryable'):
                raise ValueError('This loop changed while retrying')
            if not _controller.store.claim_holdouts(current):
                raise ValueError('The holdout is reserved or already evaluated')
            current.update(status='finishing', phase='holdout', holdout_used=True, holdout_retryable=False, holdout_retry_count=1,
                           jobs=current['jobs'] + [job], validation_count=current['validation_count'] + 1,
                           control_generation=current['control_generation'] + 1,
                           cleanup_done=False, errors=0, last_error=None)
        row = await asyncio.to_thread(_controller.store.update, row['owner'], row['id'], retry)
        return JSONResponse(projection(row), headers={'Cache-Control': 'no-store'})
    except Exception as exc:
        raise _error(exc) from exc
