"""Explicit generation boundary for shared autonomous optimizer workflows."""
from __future__ import annotations

from dataclasses import dataclass
import copy
import importlib

SERVICE = 'AILoop'


def native(config):
    """Identify native PB9 from its schema, never its development package version."""
    return isinstance(config, dict) and str(config.get('config_version', '')).startswith('v9.')


@dataclass(frozen=True)
class LoopRuntime:
    """Keep APIs, durable state and source proofs separate while sharing loop code."""
    version: str = 'v8'

    def __post_init__(self):
        """Reject unsupported generations rather than falling back to PB8."""
        if self.version not in {'v8', 'v9'}:
            raise ValueError('Unsupported AI Loop runtime')

    @property
    def native(self):
        """Whether the policy uses native Global/Portfolio inheritance."""
        return self.version == 'v9'

    @property
    def label(self):
        """Return the user-visible generation label."""
        return 'PB' + self.version[1:]

    def api(self, kind='optimize'):
        """Resolve only known, explicit runtime APIs; never accept arbitrary imports."""
        if kind not in {'optimize', 'backtest'}:
            raise ValueError('Unsupported AI Loop API')
        if self.native and kind == 'backtest':
            from pb9_loop_backtest import NativeBacktestLoopAPI
            return NativeBacktestLoopAPI()
        module = importlib.import_module('api.' + kind + '_' + self.version)
        return module.engine if self.native else module

    def directory(self, root):
        """Preserve existing PB8 storage and isolate PB9 authorization and evidence."""
        return root / 'data' / ('loop_optimizer_v9' if self.native else 'loop_optimizer')

    def prepare(self, config):
        """Validate through the selected native gateway without translating execution JSON."""
        engine = self.api()
        return engine.prepare_pb8_config(config)


def for_config(config):
    """Use a config-bound generation for pure validation and presentation helpers."""
    return LoopRuntime('v9' if native(config) else 'v8')


def for_record(record):
    """Derive historical generation from immutable input when no explicit marker exists."""
    profile = for_config(record.get('initial_config') or record.get('config') or {})
    if record.get('runtime', profile.version) != profile.version:
        raise ValueError('AI Loop runtime differs from its immutable source config')
    return profile


def side_policy(config, side):
    """Resolve authored native inheritance without changing saved sparse values."""
    if side not in {'long', 'short'}:
        raise ValueError('Invalid trading side')
    bot = config.get('bot') or {}
    def merge(base, patch):
        result = copy.deepcopy(base)
        for key, value in patch.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result
    return merge(bot.get('global') or {}, bot.get(side) or {}) if native(config) else copy.deepcopy(bot.get(side) or {})


def store_for_job(root, job):
    """Select durable authorization by an explicit runtime; never share PB8/PB9 state."""
    from pathlib import Path
    from pb8_loop_store import LoopStore
    runtime = job.get('loop_runtime', 'v8')
    expected = {'pb8': 'v8', 'pb9': 'v9'}.get(job.get('runtime'), runtime)
    if runtime != expected:
        raise ValueError('Loop job runtime mismatch')
    profile = LoopRuntime(runtime)
    return LoopStore(profile.directory(Path(root)))


def queue_identity(identity, runtime, existing=None):
    """Bind ownership in the first queue write and reject idempotent cross-owner reuse."""
    from pb8_loop_store import identifier
    LoopRuntime(runtime)
    result = {} if identity is None else {
        'loop_id': identifier(identity['loop_id']),
        'loop_owner': identifier(identity['loop_owner']),
        'loop_runtime': runtime,
        'loop_observer': bool(identity.get('loop_observer')),
    }
    if existing is not None:
        actual = {key: existing.get(key, False if key == 'loop_observer' else 'v8' if key == 'loop_runtime' else None)
                  for key in ('loop_id', 'loop_owner', 'loop_runtime', 'loop_observer')}
        if result and actual != result or not result and existing.get('loop_id'):
            raise ValueError('Queue operation belongs to a different AI Loop')
    return result
