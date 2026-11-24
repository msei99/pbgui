"""V8 facade for the shared, instance-owned autonomous Loop engine."""
import sys
from types import ModuleType
from api import loop_optimizer_engine as shared
from api.loop_optimizer_engine import LoopOptimizerEngine

engine = LoopOptimizerEngine(runtime="v8")
router = engine.router


class _EngineModule(ModuleType):
    """Keep compatibility callers and test overrides tied to this exact runtime."""
    def __getattr__(self, name):
        """Resolve shared models and instance-owned controls without global mutation."""
        if hasattr(engine, name):
            return getattr(engine, name)
        return getattr(shared, name)

    def __setattr__(self, name, value):
        """Retain existing explicit overrides on their owning engine."""
        if name not in {'engine', 'router', 'shared'} and hasattr(engine, name):
            setattr(engine, name, value)
        else:
            super().__setattr__(name, value)


sys.modules[__name__].__class__ = _EngineModule
