"""Native PB9 profile on the existing instance-owned Optimize engine and page."""

import copy
from functools import wraps
import inspect
import json
import re
from pathlib import Path

from fastapi import Depends, HTTPException, Request
from fastapi.routing import APIRoute

from api.auth import SessionToken, require_auth
from api.optimize_engine import OptimizeEngine
from optimize_profile import OptimizeProfile
from pb9_config import PB9CancelSignal, PB9ConfigError
from pb9_optimize_adapter import NativeOptimizeAdapter, REQUEST_CANCEL, source_hash

SERVICE = 'OptimizeV9'


class NativeOptimizeEngine(OptimizeEngine):
    """Change native schema/config hooks while sharing queue, results and controls."""

    def _config_compatibility_error(self, config):
        """Keep retired configs visible and deletable, without invoking native code."""
        from pb9_config_helper import incompatible_pb9_layout, INCOMPATIBLE_PB9_MESSAGE
        return INCOMPATIBLE_PB9_MESSAGE if incompatible_pb9_layout(config) else ''

    def __init__(self):
        """Construct native hooks after the shared engine's own isolated state."""
        super().__init__(OptimizeProfile('v9'))
        self.native = NativeOptimizeAdapter(self)
        self.native.bind()
        self._CONFIG_SECTIONS=(*self._CONFIG_SECTIONS,'logging','monitor')
        self._PARETO_COMPARISON_METRICS=(
            ('cagr',('cagr_fraction_strategy_equity_ew','cagr_fraction_strategy_equity')),
            ('drawdown_max',('drawdown_max_fraction_strategy_equity',)),
            ('sharpe_ratio',('sharpe_ratio_strategy_equity',)),
            ('loss_profit_ratio',('loss_profit_ratio',)),
            ('sortino_ratio',('sortino_ratio_strategy_equity',)),
            ('omega_ratio',('omega_ratio_strategy_equity',)))
        # PB9 has no gain metric; Sweep derives window growth from native CAGR.
        from sweep_cycles import PB9_CAGR_METRIC
        self._SWEEP_GAIN_SOURCE_METRICS=(PB9_CAGR_METRIC,)

    def _normalize_config(self, config, name, **_kwargs):
        """Retain sparse native policy and manage only the optimizer's output namespace."""
        candidate = copy.deepcopy(config)
        if not isinstance(candidate, dict) or not re.fullmatch(r'v9\.\d+\.\d+',str(candidate.get('config_version', ''))):
            raise HTTPException(422, 'Expected a native PB9 optimizer config')
        candidate.setdefault('backtest', {})['base_dir'] = f'backtests/pbgui/{self._validate_name(name)}'
        self._validate_options_for_config(self._runtime_options_from_config(candidate), candidate)
        return candidate

    def _validate_launch_options(self, raw):
        """Keep PB8 validation and add the native CLI contract without path inputs."""
        from pb9_optimize_options import validate_options
        result = super()._validate_launch_options(raw)
        try:
            from pb9_optimize_manifests import resolve_manifest
            payload = {**(raw or {}), 'mode':result['mode']}
            payload['preparation_manifest'] = resolve_manifest(self._manifest_root(), payload.get('preparation_manifest'))
            result.update(validate_options(payload))
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        return result

    def _manifest_root(self):
        """Keep user attachments in PBGui data, independent of the bot checkout."""
        return Path(self.PBGDIR)/'data/opt_v9_manifests'

    def _validate_options_for_config(self, options, config):
        """Reject stale scenario selections before queue publication or launch."""
        from pb9_optimize_options import validate_options
        try:
            validate_options(options, config)
            active = [key for key in ('scenarios', 'filter_starting_configs', 'compress_starting_configs', 'preparation_manifest') if options.get(key)]
            capabilities = self.native.metadata().get('native_cli_options', []) if active else []
            for key in active:
                if key not in capabilities:
                    raise ValueError('The configured PB9 source does not advertise --' + key.replace('_', '-'))
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    def _result_launch_options(self, result_dir, config):
        """Resume the original subset and imported prepared-input report."""
        from pb9_optimize_options import MAX_OPTIONS_BYTES, OPTIONS_FILENAME, validate_options
        path = Path(result_dir) / OPTIONS_FILENAME
        if not path.exists() and not path.is_symlink():
            return {}
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_OPTIONS_BYTES:
                raise ValueError('Invalid native result launch options file')
            result = json.loads(self.read_regular_file_nofollow(path, Path(result_dir)))
            if not isinstance(result, dict):
                raise ValueError('Native result launch options must be an object')
            # Seed selection affects initialization only, never checkpoint resume.
            result.update(mode='checkpoint_resume', filter_starting_configs=False, compress_starting_configs=None)
            return {'mode': 'checkpoint_resume', **validate_options(result, config)}
        except (OSError, RuntimeError, ValueError, TypeError, RecursionError) as exc:
            raise HTTPException(422, 'Invalid native result launch options') from exc

    def _reset_queue_item(self, filename, options):
        """Keep frozen native selections when changing a queued job's launch mode."""
        with self._queue_lock():
            filename = self._validate_name(filename)
            path = self._queue_file(filename)
            if not path.is_file():
                raise HTTPException(404, 'Queue item not found')
            previous = self._read_json(path).get('launch_options') or {}
            additions = {key: copy.deepcopy(previous[key]) for key in
                ('scenarios', 'preparation_manifest', 'filter_starting_configs', 'compress_starting_configs') if key in previous}
            if options.get('mode') != 'pareto_seed':
                additions.update(filter_starting_configs=False, compress_starting_configs=None)
            return super()._reset_queue_item(filename, {**additions, **options})

    def get_pareto_file(self, path: str, session=Depends(require_auth)):
        """Convert a managed native result record before handing it to config consumers."""
        result = super().get_pareto_file(path, session)
        result['config'] = self.native.result_config(result['config'])
        return result

    def migrate_v7(self, body, session=Depends(require_auth)):
        """Never expose the inherited PB7-to-PB8 converter through a native PB9 route."""
        raise HTTPException(422,'Import a native PB9 optimizer config; this legacy migration targets PB8')

    def preview_hsl_migration(self, name, body, session=Depends(require_auth)):
        """PB9 configs already use native bot.*.hsl policy; the PB8 HSL converter does not apply."""
        raise HTTPException(422,'HSL migration converts legacy PB8 settings; PB9 configs use native bot HSL policy')

    def cloud_validate(self, body, session=None):
        """Validate native cloud drafts inside the existing cancellable PB9 request owner."""
        from api.vast_v9 import _profile
        from vast_pb9 import validate
        config=body.get('config') if isinstance(body,dict) else None
        if not isinstance(config,dict):raise HTTPException(422,'Expected a config object')
        errors=validate(config)
        profile=_profile()
        config_valid=not errors
        if not profile['published']:
            errors.append({'path':'worker','message':profile['reason']})
        return {'config_valid':config_valid,'valid':not errors,'errors':errors,'warnings':[],
                'metrics':self.native.metadata()['scoring']['metrics'],**profile}

    def _effective_optimize_value(self,config,key,default=None):
        """Use native metadata defaults only when an authored value is omitted."""
        options=config.get('optimize') or {}
        if key in options:return options[key]
        return self.native.metadata()['optimize_defaults'].get(key,default)

    def _strict_runtime(self):
        """Require a clean native source tree; report local changes as a conflict."""
        try:
            from pb9_backtest_runtime import require_runtime_available
            require_runtime_available(self.PBGDIR)
            return self.native.runtime(strict=True)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from None

    def _runtime_commit(self,repo):
        """Record the clean native source identity, independent of inherited Git variables."""
        status=self._strict_runtime()
        if Path(status['pb9dir']).resolve()!=Path(repo).resolve():
            raise HTTPException(409,'Native optimizer source path changed')
        return status['commit']

    def _strategy_kind(self, config):
        """Read native shared strategy policy without adding a retired live field."""
        return ((config.get('bot') or {}).get('portfolio') or {}).get('strategy_kind', '')

    def _write_json(self, path, payload):
        """Use the common config gateway for configs and ordinary atomic JSON for state."""
        if isinstance(payload, dict) and str(payload.get('config_version', '')).startswith('v9.'):
            self.native.write(path, payload)
        else:
            super()._write_json(path, payload)

    def _validate_sweep_cycles_config(self, config):
        """Use the shared Sweep contract with native inherited universe selection."""
        candidate=copy.deepcopy(config)
        bot=candidate.get('bot') or {}
        shared=((bot.get('global') or {}).get('universe') or {}).get('approved_coins',[])
        approved={}
        for side in ('long','short'):
            value=((bot.get(side) or {}).get('universe') or {}).get('approved_coins')
            approved[side]=shared if value is None else value
        candidate.setdefault('live',{})['approved_coins']=approved
        return super()._validate_sweep_cycles_config(candidate)

    def _validate_forager_optimize_search_space(self, config):
        """Use native PB9's inherited policy/search validator instead of PB8 path checks."""
        self.native.prepare(config)

    def get_settings(self, session=Depends(require_auth)):
        """Preserve settings parity while reporting native HSL scope choices."""
        result = super().get_settings(session)
        result['use_pbgui_market_data']=str(self.load_ini_section(self._QUEUE_SETTINGS_SECTION).get('use_pbgui_market_data','True')).lower()=='true'
        result['hsl_signal_modes'] = ['coin', 'position_side', 'portfolio']
        return result

    def new_config(self, session=Depends(require_auth)):
        """Show the configured launch source in new drafts without changing metadata."""
        result = copy.deepcopy(super().new_config(session))
        settings = self.load_ini_section(self._QUEUE_SETTINGS_SECTION)
        if str(settings.get('use_pbgui_market_data', 'True')).lower() == 'true':
            result['config'].setdefault('backtest', {})['ohlcv_source_dir'] = self.get_pbgui_data_path(session)['path']
        return result

    def _queue_identity(self, prepared, filename):
        """Publish native provenance together with the shared immutable queue record."""
        # New and resumed jobs require a clean source tree for exact provenance.
        runtime = self._strict_runtime()
        return {'runtime':'pb9', 'schema':prepared['config_version'], 'commit':runtime['commit'],
                'snapshot_sha256':self.hashlib.sha256(self._snapshot_file(filename).read_bytes()).hexdigest(),
                'source_sha256':source_hash(prepared)}

    def _verify_queue_identity(self, record, runtime, snapshot):
        """Reject source changes and snapshot edits before any native launch artifacts."""
        if not runtime.get('source_clean'):
            raise HTTPException(409, 'PB9 source tree has local changes; commit or revert them before starting an optimizer')
        if (record.get('runtime') != 'pb9' or record.get('commit') != runtime['commit']
                or record.get('schema') != runtime['config_schema']
                or self.hashlib.sha256(snapshot.read_bytes()).hexdigest() != record.get('snapshot_sha256')
                or source_hash(self._read_json(snapshot))!=record.get('source_sha256')):
            raise HTTPException(409, 'PB9 optimizer source or snapshot changed; enqueue a fresh job')

    def _execution_config(self, config):
        """Never pass GUI preferences into the native optimizer input."""
        result = copy.deepcopy(config);result.pop('pbgui', None)
        backtest=result.get('backtest')
        if not isinstance(backtest,dict) or not isinstance(backtest.get('base_dir'),str):
            raise HTTPException(422,'Missing native optimizer backtest namespace')
        base=Path(backtest['base_dir'])
        if base.is_absolute() or base.parts[:2]!=('backtests','pbgui') or len(base.parts)!=3:
            raise HTTPException(422,'Invalid native optimizer backtest namespace')
        result['backtest']['base_dir']=str(self._backtests_root()/self._validate_name(base.parts[-1]))
        return result

    def _runner_identity(self, runtime, filename):
        """Require independent runners to reverify the exact launch config and source."""
        return {'runtime_identity':{'runtime':'pb9','schema':runtime['config_schema'],'commit':runtime['commit'],
            'launch_sha256':self.hashlib.sha256(self._launch_config_file(filename).read_bytes()).hexdigest()}}

    def _runner_python_flags(self):
        """Run native Python with interpreter and user-path isolation enabled."""
        return ['-I', '-B']

    def _optimizer_environment(self, interpreter):
        """Use the same credential-free environment boundary as native PB9 backtests."""
        from pb9_backtest_runtime import launch_environment
        return {**launch_environment(interpreter), 'PASSIVBOT_GPU_PROFILE':'1'}

    def _optimizer_command(self, command, environment):
        """Clear the systemd user manager's inherited environment before native execution."""
        return ['/usr/bin/env','-i', *(key + '=' + value for key,value in sorted(environment.items())), *command]

    def _resume_compatibility_fields(self, config):
        """Compare native effective evaluation policy, including inline coin overrides."""
        candidate = copy.deepcopy(config)
        selected = ((candidate.get('pbgui') or {}).get('optimize_runtime') or {}).get('scenarios') or []
        if selected:
            candidate['backtest']['suite_enabled'] = True
            candidate['backtest']['scenarios'] = [item for item in candidate['backtest'].get('scenarios', []) if isinstance(item, dict) and item.get('label') in selected]
        return self.native.effective(candidate)

    def _convergence_identity(self,config):
        """Use the same native effective policy for convergence history and checkpoint resume."""
        return source_hash(self._resume_compatibility_fields(config))


engine = NativeOptimizeEngine()
router = engine.router


@router.post('/preparation-manifests')
def import_preparation_manifest(body: dict, session=Depends(require_auth)):
    """Import one bounded user attachment without interpreting a filesystem path."""
    from logging_helpers import human_log
    from pb9_optimize_manifests import store_manifest
    try:
        return {'manifest':store_manifest(engine._manifest_root(), body)}
    except (ValueError, OSError, RuntimeError) as exc:
        human_log(SERVICE, 'Preparation manifest import rejected', level='WARNING',
                  meta={'error_type':type(exc).__name__})
        raise HTTPException(422, str(exc) if isinstance(exc, ValueError) else 'Cannot store preparation manifest') from exc

# Work that may start the native helper/optimizer stays in the existing bounded
# PB9 request owner. Other shared readers retain their original FastAPI lifecycle.
OWNED_OPERATIONS = frozenset({'cloud_validate','get_metadata','new_config','prepare_config','get_config','save_config',
    'duplicate_config','get_settings','add_to_queue','start_queue_item','requeue_fresh','continue_from_pareto',
    'resume_checkpoint','get_queue_config','repair_queue_config','get_result_config','queue_result_resume'})


def _owned_endpoint(endpoint):
    """Preserve the shared endpoint's FastAPI validation and auth dependencies."""
    signature = inspect.signature(endpoint, eval_str=True)
    with_body = 'body' in signature.parameters

    @wraps(endpoint)
    async def owned(**kwargs):
        """Forward native work with validated route/query arguments and owned cancellation."""
        from api.v9_instances import _run
        request = kwargs.pop('_pb9_request')
        kwargs.pop('session', None)
        kwargs.pop('body', None)
        return await _run(request, 'opt9_' + endpoint.__name__, kwargs, with_body=with_body,
                          query=frozenset(signature.parameters) - {'session','body'})

    owned.__signature__ = signature.replace(parameters=[*signature.parameters.values(),
        inspect.Parameter('_pb9_request', inspect.Parameter.KEYWORD_ONLY, annotation=Request)])
    return owned


for route in list(router.routes):
    if isinstance(route, APIRoute) and route.endpoint.__name__ in OWNED_OPERATIONS:
        router.routes.remove(route)
        router.add_api_route(route.path, _owned_endpoint(route.endpoint), methods=route.methods,
                             name=route.name, response_class=route.response_class)

# Both native editors use the same user-owned readiness/date-lookup endpoints.
from api.backtest_v9 import router as _backtest_router
router.routes[:] = [route for route in router.routes if not route.path.startswith('/ohlcv-')]
router.routes.extend(route for route in _backtest_router.routes if route.path.startswith('/ohlcv-'))


@router.post('/migrate-v8/preview')
async def migrate_v8_preview(request: Request, session: SessionToken = Depends(require_auth)):
    """Review the official conversion of a saved PB8 Optimize or Pareto config."""
    from api.v9_instances import _run
    return await _run(request, 'opt9_migrate_preview', {'owner': session.user_id}, with_body=True)


@router.post('/migrate-v8/accept')
async def migrate_v8_accept(request: Request, session: SessionToken = Depends(require_auth)):
    """Adopt a reviewed proposal as a private unsaved PB9 Optimize draft."""
    from api.v9_instances import _run
    return await _run(request, 'opt9_migrate_accept', {'owner': session.user_id}, with_body=True)


MIGRATION_OPERATIONS = frozenset({'migrate_preview', 'migrate_accept'})


def _migration(name, cancel, envelope):
    """Run the official PB8 -> PB9 converter for Optimize sources; never save or queue."""
    from api.backtest_v9 import _paths, _runtime, store_draft
    from api.pb9_migration import preview, reviewed
    args, body = envelope.get('args') or {}, envelope.get('body')
    owner = args.get('owner') if isinstance(args, dict) else None
    if not isinstance(owner, str) or not owner or not isinstance(body, dict):
        raise HTTPException(422, 'Invalid PB9 migration request')
    if name == 'migrate_preview':
        return preview(body, owner, _paths(), cancel, target='optimize')
    status, _ = _runtime(cancel)
    review = reviewed(body, owner, status, target='optimize')
    token = REQUEST_CANCEL.set(cancel)
    try:
        source = engine.native.prepare(review['config'])
    finally:
        REQUEST_CANCEL.reset(token)
    return {'draft_id': store_draft(owner, source, review['report']), 'suggested_name': review['name']}


def _operation(operation, cancel, payload):
    """Execute one allowlisted native operation inside its registered request worker."""
    from api.pb9 import _request_envelope
    from api.v9_instances import _native_http_error
    name = operation.removeprefix('opt9_')
    if name in MIGRATION_OPERATIONS:
        try:
            return _migration(name, cancel, _request_envelope(payload, {'args', 'body'}))
        except PB9ConfigError as exc:
            raise _native_http_error(exc) from None
    if name not in OWNED_OPERATIONS:
        raise HTTPException(404, 'Unknown PB9 optimizer operation')
    envelope = _request_envelope(payload, {'args','body'})
    args = envelope.get('args') or {}
    signature = inspect.signature(getattr(engine, name))
    if not isinstance(args, dict) or set(args) - (set(signature.parameters) - {'body','session'}):
        raise HTTPException(422, 'Invalid native optimizer arguments')
    args = {**args, 'session':None}
    if 'body' in signature.parameters:
        args['body'] = envelope.get('body')
    token = REQUEST_CANCEL.set(cancel)
    try:
        return getattr(engine, name)(**args)
    except PB9ConfigError as exc:
        raise _native_http_error(exc) from None
    finally:
        REQUEST_CANCEL.reset(token)


def startup():
    """Start the shared native controller with a new preparation cancellation owner."""
    engine.native.cancel = PB9CancelSignal()
    engine.startup()


async def shutdown():
    """Join native preparation while already published independent optimizers continue."""
    engine.native.cancel.set()
    await engine.shutdown()
