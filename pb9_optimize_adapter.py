"""Native PB9 configuration hooks for the existing shared optimizer engine."""

import copy
from contextvars import ContextVar
import hashlib
import json
from pathlib import Path
import threading
import time

from fastapi import HTTPException
from logging_helpers import human_log as _log

from pb7_config import load_pb7_config, save_pb7_config
from pb9_config import (PB9CancelSignal, PB9ConfigError, get_pb9_optimize_metadata,
                        validate_pb9_document, get_pb9_optimize_effective,
                        get_pb9_optimize_cloud_plan)
from pb9_config_storage import PB9FileContext
from pb9_runtime import inspect_pb9_runtime
from passivbot_runtime import classify_passivbot_source
from pb9_backtest_runtime import source_identity

SERVICE = 'OptimizeV9'
REQUEST_CANCEL = ContextVar('pb9_optimize_cancel', default=None)


class NativeOptimizeAdapter:
    """Own native helper/cache state; share queue/results/UI infrastructure only."""

    def __init__(self, engine):
        """Bind configuration work to this engine and its cancellable controller."""
        self.engine = engine
        self.cancel = PB9CancelSignal()
        self._metadata = None
        self._lock = threading.RLock()
        self._effective_cache={}

    def paths(self):
        """Read only the existing allowlisted runtime-slot settings."""
        from api.backtest_v9 import _paths
        return _paths()

    def options(self):
        """Select the exact native environment and current operation's stop flag."""
        paths = self.paths()
        return (paths['pb9dir'], paths['pb9venv'],
                {key:value for key,value in paths.items() if not key.startswith('pb9')})

    def stop_flag(self):
        """Keep request cancellation separate from the persistent controller lifetime."""
        return REQUEST_CANCEL.get() or self.cancel

    def runtime(self, *, strict=False):
        """Return native generation and source identity.

        Reading saved results, metadata and paths works with local source
        changes. ``strict`` requires a clean tree, so a commit identifies all
        code used by a new launch, resume or queued job.
        """
        args = self.options()
        status = inspect_pb9_runtime(*args, cancel_event=self.stop_flag())
        if status['detected']:
            try:
                identity = source_identity(Path(args[0]))
                clean, error = True, ''
            except RuntimeError as exc:
                if strict:
                    raise
                identity = classify_passivbot_source(str(args[0]))
                clean, error = False, str(exc)
            status.update(commit=identity.get('commit') or '', config_schema=identity.get('config_schema') or '',
                          source_clean=clean, source_error=error)
        return {**status, 'ready':status['detected'], 'version':status.get('package_version', '')}

    def metadata(self, *, include_backend_contract=True):
        """Cache one native metadata document briefly, bound to its inspected commit."""
        status = self.runtime()
        if not status['ready']:
            raise PB9ConfigError('runtime_not_detected')
        key = (status['pb9dir'], status['pb9venv'], status['commit'], status['config_schema'], status['source_clean'])
        with self._lock:
            if self._metadata and self._metadata[0] == key and time.monotonic() - self._metadata[1] < 30:
                return copy.deepcopy(self._metadata[2])
            payload = get_pb9_optimize_metadata(*self.options(), cancel_event=self.stop_flag())
            from pb9_parameter_help import populate_pb9_field_help
            try:
                populate_pb9_field_help(status['pb9dir'], payload.get('fields', []))
            except (OSError, UnicodeError, ValueError) as exc:
                populate_pb9_field_help(status['pb9dir'], payload.get('fields', []), catalog={'entries': {}})
                _log(SERVICE, 'PB9 parameter documentation is unavailable', level='WARNING',
                     meta={'error_type': type(exc).__name__})
            self._metadata = (key, time.monotonic(), copy.deepcopy(payload))
            return payload

    def prepare(self, config, **_kwargs):
        """Validate authored values without expanding or rewriting native sparse pins."""
        checked = validate_pb9_document(config, *self.options(), validation_target='optimize', cancel_event=self.stop_flag())
        source = checked['config']
        if checked['pbgui'] is not None:
            source['pbgui'] = checked['pbgui']
        return source

    def result_config(self, record):
        """Use the selected native result reader, retaining sparse policy and GUI provenance."""
        checked = validate_pb9_document(record, *self.options(), validation_target='result',
                                        cancel_event=self.stop_flag())
        source = checked['config']
        if checked['pbgui'] is not None:
            source['pbgui'] = checked['pbgui']
        return source

    def _workload_context(self):
        """Invalidate display estimates when the selected native defaults/source change."""
        status = self.runtime()
        if not status['ready']:
            raise PB9ConfigError('runtime_not_detected')
        return (status['pb9dir'], status['pb9venv'], status['commit'], status['config_schema'], status['source_clean'])

    def _estimate_workload_snapshot(self, path):
        """Resolve inherited universes and scenarios natively, then share candle counting."""
        from optimizer_workload import estimate_coin_candles
        config = self.load(path)
        config.pop('pbgui', None)
        # Dynamic selectors cannot supply a conservative config-only count.
        def dynamic(value):
            """Recognize unresolved selectors before requesting a concrete data plan."""
            if isinstance(value, dict):
                return any((key in {'approved_coins', 'ignored_coins'}
                            and item is not None and not isinstance(item, list))
                           or dynamic(item) for key, item in value.items())
            return isinstance(value, list) and any(dynamic(item) for item in value)
        if dynamic(config):
            return None
        plans = get_pb9_optimize_cloud_plan(config, *self.options(),
                                            cancel_event=self.stop_flag())['data_plans']
        # The native export includes the base policy followed by training scenarios.
        # Count the training windows once, excluding the base envelope and holdouts.
        selected = plans[1:] if plans[0]['backtest'].get('suite_enabled') else plans[:1]
        if not selected:
            return None
        total = 0
        for plan in selected:
            projected = {'live': plan['live'],
                         'backtest': {**plan['backtest'], 'suite_enabled': False}}
            count = estimate_coin_candles(projected)
            if count is None:
                return None
            total += count
        return total

    def estimate_snapshot(self, path, root):
        """Share containment, bounded cache and failure handling without calling PB8."""
        from optimizer_workload import estimate_snapshot
        return estimate_snapshot(path, root, estimator=self._estimate_workload_snapshot,
                                 context=self._workload_context)

    def effective(self,config):
        """Cache at most 64 source-bound native resume projections, never saved defaults."""
        source=copy.deepcopy(config);source.pop('pbgui',None)
        status=self.runtime()
        if not status['ready']:raise PB9ConfigError('runtime_not_detected')
        key=(status['commit'],source_hash(source))
        # A changed tree can differ at the same commit; never reuse its projection.
        cacheable=bool(status.get('source_clean'))
        with self._lock:
            if cacheable and key in self._effective_cache:return copy.deepcopy(self._effective_cache[key])
        result=get_pb9_optimize_effective(source,*self.options(),cancel_event=self.stop_flag())['effective']
        if not cacheable:return result
        with self._lock:
            if len(self._effective_cache)>=64:self._effective_cache.pop(next(iter(self._effective_cache)))
            self._effective_cache[key]=copy.deepcopy(result)
        return result

    def context(self, path):
        """Use the existing shared config gateway with an explicit native optimizer target."""
        return PB9FileContext(Path(path).parent, *self.options(), validation_target='optimize', cancel_event=self.stop_flag())

    def load(self, path, **_kwargs):
        """Load only a managed optimizer document through pb7_config's PB9 boundary."""
        path = Path(path)
        return load_pb7_config(path.name, pb9_context=self.context(path))

    def editor(self, path, **_kwargs):
        """Keep the common editor envelope and authored native config intact."""
        config = self.load(path)
        return {'config':config, 'param_status':{}, 'raw_config':copy.deepcopy(config)}

    def write(self, path, config):
        """Persist native configs through the same checked atomic config gateway."""
        path = Path(path)
        save_pb7_config(config, path.name, pb9_context=self.context(path))

    def override_bundle(self, path, **_kwargs):
        """Native PB9 validates inline overrides without invoking legacy override loaders."""
        self.load(path)

    def optimizer_overrides(self, config, **_kwargs):
        """Ask the native search-space validator to accept all authored fixed values."""
        self.prepare(config)

    def preflight(self, config, **_kwargs):
        """Verify native configuration and local GPU availability before launch."""
        self.prepare(config)
        runtime = self.metadata()['backend_contract']['runtime']
        if not runtime['device_available']:
            raise HTTPException(503, runtime['reason'])
        return {'valid':True, 'backend':'gpu', 'runtime':runtime}

    def exchanges(self):
        """Use native capabilities rather than the template's selected exchanges."""
        return copy.deepcopy(self.metadata()['exchange_options'])

    def bind(self):
        """Supply version-specific hooks at engine construction, without global mutation."""
        engine = self.engine
        original_settings=engine.load_ini_section
        def settings(section):
            """Default the native slot to PBGui candles while retaining explicit user choices."""
            values=original_settings(section)
            return {'use_pbgui_market_data':'True',**values} if section==engine._QUEUE_SETTINGS_SECTION else values
        engine.load_ini_section=settings
        from api.pb9_ohlcv_tools import shutdown as stop_native_lookups
        hooks = {'PB8ConfigurationError':PB9ConfigError, 'pb8_runtime_status':self.runtime,
            'estimate_snapshot':self.estimate_snapshot,
            'prepare_pb8_config':self.prepare, 'load_pb8_config':self.load, 'load_pb8_editor_config':self.editor,
            'get_pb8_optimize_metadata':self.metadata, 'get_pb8_exchange_metadata':self.exchanges,
            'validate_pb8_optimizer_overrides':self.optimizer_overrides, 'validate_pb8_override_bundle':self.override_bundle,
            'validate_pb8_optimize_preflight':self.preflight, 'cache_prepared_pb8_config':lambda *_args:None,
            'prepare_pb8_migration_helper_startup':lambda:None, 'start_pb8_migration_helper':lambda:None,
            'interrupt_pb8_migration_helper':lambda:self.cancel.set(), 'shutdown_pb8_migration_helper':lambda:None,
            'shutdown_pb8_ohlcv_start_date_jobs':stop_native_lookups}
        for name, value in hooks.items():
            setattr(engine, name, value)


def source_hash(config):
    """Fingerprint an immutable native execution document without GUI-only metadata."""
    value = copy.deepcopy(config);value.pop('pbgui', None)
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
