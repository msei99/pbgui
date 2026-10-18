"""Browser-safe failures owned by the PB7 Strategy Explorer."""

import re

from pb7_config import PB7ConfigurationError
from strategy_explorer_types import BotParams

SERVICE = "StrategyExplorer"

_PUBLIC_FIELDS = frozenset(
    f"bot.{side}.{name}"
    for side in ("long", "short")
    for name in BotParams.__dataclass_fields__
)


def safe_failure_reason(error: Exception) -> str:
    """Describe failures using trusted labels, never upstream input values.

    Exception strings can contain entire configs, quoted passwords with spaces,
    or bare values from numeric parsers. Pattern-based credential masking cannot
    make that free text safe. Only fixed categories, known field identifiers and
    a fixed migration command are allowed into browser/log/polling messages.
    """
    if isinstance(error, StrategyExplorerConfigurationError):
        return str(error)
    text = str(error)[:16384]
    if isinstance(error, PB7ConfigurationError):
        reason = "PB7 configuration runtime unavailable. Check the configured PB7 path and environment."
    elif "legacy flat strategy field" in text:
        reason = "Legacy flat strategy fields are incompatible with the configured strategy."
    elif "failed to format config: unknown flavor" in text:
        reason = "failed to format config: unknown flavor."
    elif "could not convert string to float" in text or "invalid literal for int()" in text:
        reason = "Invalid numeric value in the configuration."
    else:
        reason = "Configuration rejected by the PB7 pipeline."
    fields = sorted(set(re.findall(r"\bbot\.(?:long|short)\.[a-zA-Z0-9_]+", text)) & _PUBLIC_FIELDS)
    if fields:
        reason += " Fields: " + ", ".join(fields[:24]) + "."
    if "passivbot tool migrate-config-v7" in text:
        reason += " For v7 configs, run `passivbot tool migrate-config-v7`."
    return reason


class StrategyExplorerConfigurationError(ValueError):
    """An expected rejection by the explorer's canonical config preparation."""

    def __init__(self, error: Exception) -> None:
        """Retain only a safe public reason, never the input configuration."""
        super().__init__(safe_failure_reason(error))


def is_configuration_rejection(error: Exception) -> bool:
    """Recognize upstream rejection without changing the shared PB7 wrapper.

    PB7 has no dedicated validation exception. For the real wrapper, require
    a loader/metadata-strip call in the traceback, excluding its own bookkeeping.
    A substituted preparation callable is itself the boundary (offline tests).
    Only the two allowlisted callable locals are inspected, never config values.
    """
    if not isinstance(error, ValueError) and not (
        type(error) in (Exception, RuntimeError)
        and str(error).startswith("failed to format config: unknown flavor")
    ):
        return False
    frames = []
    trace = error.__traceback__
    while trace is not None:
        frames.append(trace.tb_frame)
        trace = trace.tb_next
    wrapper_frames = [frame for frame in frames if frame.f_globals.get("__name__") == "pb7_config"]
    if not wrapper_frames:
        return True
    upstream_codes = {
        code
        for frame in wrapper_frames
        for name in ("load_prepared_config", "strip_config_metadata")
        if (code := getattr(frame.f_locals.get(name), "__code__", None)) is not None
    }
    return any(frame.f_code in upstream_codes for frame in frames)
