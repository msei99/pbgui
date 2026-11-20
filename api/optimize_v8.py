"""PB8 compatibility entry point for the shared optimizer engine."""
import sys
from types import ModuleType
from api.optimize_engine import OptimizeEngine

engine = OptimizeEngine()
router = engine.router

class _EngineModule(ModuleType):
    """Keep existing Python callers and overrides bound to this exact engine."""

    def __getattr__(self, name):
        """Resolve legacy helpers on their PB8 engine instance."""
        return getattr(engine, name)

    def __setattr__(self, name, value):
        """Preserve explicit legacy overrides without touching another engine."""
        if name not in {"engine", "router"} and hasattr(engine, name):
            setattr(engine, name, value)
        else:
            super().__setattr__(name, value)

sys.modules[__name__].__class__ = _EngineModule
