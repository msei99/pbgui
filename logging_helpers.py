from contextlib import contextmanager
from contextvars import ContextVar, Token
from datetime import datetime
from itertools import islice
import json
import os
from pathlib import Path
import re
import sys
from typing import Optional

from file_lock import advisory_file_lock
from pbgui_purefunc import load_ini_snapshot, update_ini


PBGDIR = Path(__file__).resolve().parent
PBGUI_INI = PBGDIR / "pbgui.ini"
LOG_ROOT = PBGDIR / "data" / "logs"

# Per-service minimum levels. If a service has an entry here, messages
# whose severity is lower than the configured value are suppressed.
# Values are numeric similar to logging module: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50
_LEVEL_MAP = {
    'DEBUG': 10,
    'INFO': 20,
    'WARNING': 30,
    'ERROR': 40,
    'CRITICAL': 50,
}
_service_min_levels = {}
_global_min_level = 0

# Service-to-log-group mapping.
# Services listed here share a common log file (group name = log stem)
# instead of getting their own individual {service}.log file.
LOG_GROUPS: dict[str, str] = {
    'ApiLogging':      'PBGui',
    'ApiKeys':         'PBGui',
    'BalanceCalc':     'PBGui',
    'CoinDataUI':      'PBGui',
    'Dashboard':       'PBGui',
    'Services':        'PBGui',
    'V7Instances':     'PBGui',
    'MarketDataAPI':   'PBGui',
    'PB7OhlcvAPI':     'PBGui',
    'PBV7UI':          'PBGui',
    'VPSManager':      'PBGui',
    'VPSManagerApi':   'PBGui',
    'Config':          'PBGui',
    'ParetoDataLoader':'PBGui',
    'Status':          'PBGui',
    'HyperliquidAWS':  'PBGui',
    'BacktestQueueAPI': 'PBGui',
    'Cluster':         'PBGui',
    'DbTools':         'PBGui',
    'Auth':            'PBGui',
    'LiveSession':     'PBGui',
    'ApiKeyState':     'PBGui',
    'User':            'PBGui',
}


DEFAULT_ROTATE_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_ROTATE_BACKUP_COUNT = 1
MANAGED_LOG_SCOPES: dict[str, dict[str, object]] = {
    "api_console": {"label": "API console", "description": "PBApiServer.console.log", "paths": ("PBApiServer.console.log",)},
    "jobs": {"label": "Jobs", "description": "jobs/*.log", "paths": ("jobs",)},
    "backtests": {"label": "Backtests", "description": "backtests/*.log", "paths": ("backtests",)},
    "backtests_v8": {"label": "PB8 backtests", "description": "backtests_v8/*.log", "paths": ("backtests_v8",)},
    "optimizes": {"label": "Optimizes", "description": "optimizes/*.log", "paths": ("optimizes",)},
    "optimizes_v8": {"label": "PB8 optimizes", "description": "optimizes_v8/*.log", "paths": ("optimizes_v8",)},
    "pareto_sessions": {"label": "Pareto sessions", "description": "pareto*.log", "paths": ("pareto_dash.log", "pareto_sessions")},
    "api_handoff": {"label": "API handoff", "description": "api-systemd-handoff.log", "paths": ("api-systemd-handoff.log",)},
    "vps_manager_runs": {"label": "VPS Manager runs", "description": "vps-manager/**/*.log", "paths": ("vps-manager",)},
    "ohlcv_preloads": {"label": "OHLCV preloads", "description": "ohlcv-preloads/*.log", "paths": ("ohlcv-preloads",)},
    "monitor_agent_live": {"label": "Monitor agent live data", "description": "monitor-agent/live_metrics*.ndjson", "paths": ("monitor-agent",)},
}
REDACTED = "[REDACTED]"
_MAX_REDACT_DEPTH = 8
_MAX_REDACT_ITEMS = 100
_MAX_REDACT_TEXT = 16_384
_MAX_CONTEXT_ITEMS = 20
_SENSITIVE_KEYS = {
    "password", "passwd", "api_key", "apikey", "api_secret", "secret", "token",
    "session", "cookie", "authorization", "bearer", "private_key", "passphrase",
    "access_key", "access_key_id", "access_token", "refresh_token", "session_token",
    "client_secret", "secret_key", "secret_access_key", "aws_access_key_id",
    "aws_secret_access_key", "proxy_authorization", "set_cookie", "x_api_key",
    "csrf_token", "totp_secret",
}
_SENSITIVE_KEYS_COMPACT = {key.replace("_", "") for key in _SENSITIVE_KEYS}
_PEM_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----.*?-----END(?: [A-Z0-9]+)? PRIVATE KEY-----",
    re.IGNORECASE | re.DOTALL,
)
_AUTH_RE = re.compile(r"(?i)(\b(?:proxy[-_ ]?authorization|authorization)\s*[:=]\s*)(?:(?:bearer|basic)\s+)?(?!\[REDACTED\])[^\s,;\]\}]+")
_BEARER_ASSIGN_RE = re.compile(r"(?i)(\bbearer\s*[:=]\s*)(?!\[REDACTED\])[^\s,;\]\}]+")
_COOKIE_RE = re.compile(r"(?i)(\b(?:set[-_ ]?cookie|cookie|session(?:id|_token)?|pbgui_session)\s*[:=]\s*)[^\s,;\]\}]+")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)(?<![?&])(?P<prefix>(?:[\"']?(?:password|passwd|api[_-]?key|apikey|api[_-]?secret|secret|token|access[_-]?key(?:[_-]?id)?|access[_-]?token|refresh[_-]?token|session[_-]?token|client[_-]?secret|secret[_-]?(?:key|access[_-]?key)|aws[_-]?(?:access[_-]?key[_-]?id|secret[_-]?access[_-]?key)|x[_-]?api[_-]?key|csrf[_-]?token|totp[_-]?secret|private[_-]?key|passphrase)[\"']?\s*[:=]\s*))"
    r"(?!\[REDACTED\])(?P<quote>[\"']?)(?P<value>[^\s,;\}\]]+)(?P=quote)"
)
_URL_QUERY_RE = re.compile(
    r"(?i)([?&](?:password|passwd|api[_-]?key|apikey|api[_-]?secret|secret|token|access[_-]?key(?:[_-]?id)?|access[_-]?token|refresh[_-]?token|session[_-]?token|client[_-]?secret|secret[_-]?(?:key|access[_-]?key)|aws[_-]?(?:access[_-]?key[_-]?id|secret[_-]?access[_-]?key)|x[_-]?api[_-]?key|csrf[_-]?token|totp[_-]?secret|session|cookie|authorization|bearer|private[_-]?key|passphrase)=)[^&#\s]+"
)
_logging_context: ContextVar[dict] = ContextVar("logging_context", default={})


def bind_logging_context(**context) -> Token:
    """Bind bounded metadata to the current execution context."""
    current = dict(_logging_context.get())
    for key, value in islice(context.items(), _MAX_CONTEXT_ITEMS):
        current[str(key)[:100]] = value
    return _logging_context.set(dict(islice(current.items(), _MAX_CONTEXT_ITEMS)))


def reset_logging_context(token: Token) -> None:
    """Restore the logging context represented by ``token``."""
    _logging_context.reset(token)


@contextmanager
def logging_context(**context):
    """Temporarily bind metadata, supporting nested request operations."""
    token = bind_logging_context(**context)
    try:
        yield
    finally:
        reset_logging_context(token)


def _normalize_rotate_key(value: str) -> str:
    try:
        key = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower()).strip("_")
        return key or "default"
    except Exception:
        return "default"


def _is_sensitive_key(value) -> bool:
    try:
        normalized = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
        compact = normalized.replace("_", "")
        return normalized in _SENSITIVE_KEYS or compact in _SENSITIVE_KEYS_COMPACT
    except Exception:
        return True


def _safe_text(value) -> str:
    try:
        text = str(value)
    except Exception:
        return f"<{type(value).__name__}>"
    return text[:_MAX_REDACT_TEXT] + ("...[TRUNCATED]" if len(text) > _MAX_REDACT_TEXT else "")


def _redact_text(value) -> str:
    """Return bounded text with common credential forms removed."""
    try:
        text = _safe_text(value)
        text = _PEM_PRIVATE_KEY_RE.sub(REDACTED, text)
        text = _AUTH_RE.sub(r"\1" + REDACTED, text)
        text = _BEARER_ASSIGN_RE.sub(r"\1" + REDACTED, text)
        text = _COOKIE_RE.sub(r"\1" + REDACTED, text)
        text = _URL_QUERY_RE.sub(r"\1" + REDACTED, text)
        text = _SECRET_ASSIGNMENT_RE.sub(lambda match: match.group("prefix") + REDACTED, text)
        return text
    except Exception:
        return REDACTED


def _redact_value(value, *, _depth=0, _seen=None):
    """Recursively sanitize data into a deterministic JSON-safe value."""
    if _seen is None:
        _seen = set()
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, bytes):
        return _redact_text(value.decode("utf-8", errors="replace"))
    if _depth >= _MAX_REDACT_DEPTH:
        return "[MAX_DEPTH]"

    track = isinstance(value, (dict, list, tuple, set, frozenset))
    value_id = id(value)
    if track and value_id in _seen:
        return "[RECURSIVE]"
    if track:
        _seen.add(value_id)
    try:
        if isinstance(value, dict):
            result = {}
            for key, item in islice(value.items(), _MAX_REDACT_ITEMS):
                safe_key = _redact_text(key)
                result[safe_key] = REDACTED if _is_sensitive_key(key) else _redact_value(
                    item, _depth=_depth + 1, _seen=_seen
                )
            if len(value) > _MAX_REDACT_ITEMS:
                result["[TRUNCATED]"] = len(value) - _MAX_REDACT_ITEMS
            return result
        if isinstance(value, (list, tuple)):
            result = [_redact_value(item, _depth=_depth + 1, _seen=_seen) for item in value[:_MAX_REDACT_ITEMS]]
            if len(value) > _MAX_REDACT_ITEMS:
                result.append("[TRUNCATED]")
            return result
        if isinstance(value, (set, frozenset)):
            if len(value) > _MAX_REDACT_ITEMS:
                return [f"[TRUNCATED_SET:{len(value)}]"]
            rendered = [_redact_value(item, _depth=_depth + 1, _seen=_seen) for item in value]
            return sorted(rendered, key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False))
        return f"<{type(value).__name__}>"
    except Exception:
        return REDACTED
    finally:
        if track:
            _seen.discard(value_id)


def _read_rotate_ini():
    try:
        cfg = load_ini_snapshot(PBGUI_INI).parser
    except Exception:
        return None
    if not cfg.has_section('logging'):
        cfg.add_section('logging')
    return cfg


def _update_rotate_ini(values: dict[str, str]) -> None:
    def mutate(cfg) -> None:
        if not cfg.has_section('logging'):
            cfg.add_section('logging')
        for key, value in values.items():
            cfg.set('logging', key, value)

    update_ini(mutate, PBGUI_INI)


def _physical_log_stem(service: str = None, logfile: str = None) -> str | None:
    if logfile:
        try:
            return Path(str(logfile)).stem
        except Exception:
            return None
    if service:
        return LOG_GROUPS.get(service, service)
    return None


def resolve_managed_log_scope(logfile: str | Path | None) -> str | None:
    """Resolve a physical path to one fixed managed scope."""
    if not logfile:
        return None
    try:
        relative = Path(logfile).resolve(strict=False).relative_to(LOG_ROOT.resolve(strict=False))
    except (OSError, ValueError):
        try:
            relative_root = Path(logfile).resolve(strict=False).relative_to(PBGDIR.resolve(strict=False)).as_posix()
        except (OSError, ValueError):
            return None
        if relative_root.startswith("data/vpsmanager/") and relative_root.endswith(".log"):
            return "vps_manager_runs"
        if relative_root.startswith("data/ohlcv_preload/logs/") and relative_root.endswith(".log"):
            return "ohlcv_preloads"
        if relative_root == "data/monitor_agent/live_metrics.ndjson":
            return "monitor_agent_live"
        return None
    if not relative.parts:
        return None
    relative_text = relative.as_posix()
    for scope_id, definition in MANAGED_LOG_SCOPES.items():
        for managed_path in definition["paths"]:
            if relative_text == managed_path or relative_text.startswith(f"{managed_path}/"):
                return scope_id
    if relative.name.startswith("pareto") and relative.suffix == ".log":
        return "pareto_sessions"
    return None


def get_managed_scope_settings(scope_id: str) -> tuple[int, int]:
    """Return configured settings for one declared managed scope."""
    if scope_id not in MANAGED_LOG_SCOPES:
        raise ValueError("Unknown managed log scope")
    default_max_bytes, default_backup_count = get_rotate_defaults()
    with advisory_file_lock(PBGUI_INI):
        cfg = _read_rotate_ini()
    if cfg is None:
        return DEFAULT_ROTATE_MAX_BYTES, DEFAULT_ROTATE_BACKUP_COUNT
    key = _normalize_rotate_key(scope_id)
    return (
        _parse_positive_int(cfg.get("logging", f"managed_{key}_max_bytes", fallback=str(default_max_bytes)), default_max_bytes),
        _parse_nonnegative_int(cfg.get("logging", f"managed_{key}_backup_count", fallback=str(default_backup_count)), default_backup_count),
    )


def set_managed_scope_settings(scope_id: str, max_bytes: int, backup_count: int) -> None:
    """Persist settings for one declared managed scope."""
    if scope_id not in MANAGED_LOG_SCOPES:
        raise ValueError("Unknown managed log scope")
    key = _normalize_rotate_key(scope_id)
    _update_rotate_ini({
        f"managed_{key}_max_bytes": str(_parse_positive_int(max_bytes, DEFAULT_ROTATE_MAX_BYTES)),
        f"managed_{key}_backup_count": str(_parse_nonnegative_int(backup_count, DEFAULT_ROTATE_BACKUP_COUNT)),
    })


def _parse_positive_int(value, default_value: int) -> int:
    try:
        parsed = int(value)
        if parsed > 0:
            return parsed
    except Exception:
        pass
    return int(default_value)


def _parse_nonnegative_int(value, default_value: int) -> int:
    try:
        parsed = int(value)
        if parsed >= 0:
            return parsed
    except Exception:
        pass
    return int(default_value)


def get_rotate_defaults() -> tuple[int, int]:
    """Return default rotation settings (max_bytes, backup_count)."""
    try:
        with advisory_file_lock(PBGUI_INI):
            cfg = _read_rotate_ini()
        if cfg is None:
            return DEFAULT_ROTATE_MAX_BYTES, DEFAULT_ROTATE_BACKUP_COUNT
        max_bytes = _parse_positive_int(
            cfg.get('logging', 'rotate_default_max_bytes', fallback=str(DEFAULT_ROTATE_MAX_BYTES)),
            DEFAULT_ROTATE_MAX_BYTES,
        )
        backup_count = _parse_nonnegative_int(
            cfg.get('logging', 'rotate_default_backup_count', fallback=str(DEFAULT_ROTATE_BACKUP_COUNT)),
            DEFAULT_ROTATE_BACKUP_COUNT,
        )
        return max_bytes, backup_count
    except Exception:
        return DEFAULT_ROTATE_MAX_BYTES, DEFAULT_ROTATE_BACKUP_COUNT


def set_rotate_defaults(max_bytes: int, backup_count: int):
    """Persist default rotation settings in pbgui.ini under [logging]."""
    _update_rotate_ini({
        'rotate_default_max_bytes': str(_parse_positive_int(max_bytes, DEFAULT_ROTATE_MAX_BYTES)),
        'rotate_default_backup_count': str(_parse_nonnegative_int(backup_count, DEFAULT_ROTATE_BACKUP_COUNT)),
    })


def get_rotate_settings(service: str = None, logfile: str = None) -> tuple[int, int]:
    """Return effective rotation settings for a specific service/logfile.

    Lookup order:
    1) [logging] rotate_<service_key>_max_bytes / rotate_<service_key>_backup_count
    2) [logging] rotate_default_max_bytes / rotate_default_backup_count
    """
    default_max_bytes, default_backup_count = get_rotate_defaults()
    key_src = _physical_log_stem(service, logfile)
    if not key_src:
        return default_max_bytes, default_backup_count

    try:
        with advisory_file_lock(PBGUI_INI):
            cfg = _read_rotate_ini()
        key = _normalize_rotate_key(key_src)
        max_option = f'rotate_{key}_max_bytes'
        backup_option = f'rotate_{key}_backup_count'
        scope_id = resolve_managed_log_scope(logfile)
        scope_settings = get_managed_scope_settings(scope_id) if scope_id else (default_max_bytes, default_backup_count)
        max_bytes = _parse_positive_int(cfg.get('logging', max_option, fallback=str(scope_settings[0])), scope_settings[0])
        backup_count = _parse_nonnegative_int(cfg.get('logging', backup_option, fallback=str(scope_settings[1])), scope_settings[1])
        return max_bytes, backup_count
    except Exception:
        return default_max_bytes, default_backup_count


def set_rotate_settings(service: str, max_bytes: int, backup_count: int):
    """Persist per-service rotation settings in pbgui.ini under [logging]."""
    key = _normalize_rotate_key(_physical_log_stem(service=service))
    _update_rotate_ini({
        f'rotate_{key}_max_bytes': str(_parse_positive_int(max_bytes, DEFAULT_ROTATE_MAX_BYTES)),
        f'rotate_{key}_backup_count': str(_parse_nonnegative_int(backup_count, DEFAULT_ROTATE_BACKUP_COUNT)),
    })


def trim_logfile_to_max_bytes(path: str, max_bytes: int = DEFAULT_ROTATE_MAX_BYTES):
    """Trim `path` in place to the last `max_bytes` bytes, aligned to a newline."""
    try:
        with advisory_file_lock(Path(path)):
            _trim_logfile_to_max_bytes_unlocked(Path(path), max_bytes)
    except Exception as exc:
        _write_fallback_error("trim logfile", exc)


def _trim_logfile_to_max_bytes_unlocked(path: Path, max_bytes: int) -> None:
    if not path.exists():
        return
    max_bytes = _parse_positive_int(max_bytes, DEFAULT_ROTATE_MAX_BYTES)
    if path.stat().st_size <= max_bytes:
        return
    data = path.read_bytes()
    tail = data[-max_bytes:]
    newline = tail.find(b'\n')
    if newline != -1 and newline + 1 < len(tail):
        tail = tail[newline + 1:]
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_bytes(tail)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def set_service_min_level(service: str, level: Optional[str]):
    """Set a minimum level for `service`.

    `level` may be a string like 'DEBUG'/'INFO'/... or None to remove the override.
    """
    try:
        if level is None or str(level).strip() == '':
            _service_min_levels.pop(service, None)
            return
        lev = str(level).upper()
        if lev in _LEVEL_MAP:
            _service_min_levels[service] = _LEVEL_MAP[lev]
        else:
            # Unknown level: ignore
            _service_min_levels.pop(service, None)
    except Exception:
        pass


def set_global_min_level(level: Optional[str]):
    """Set global minimum level applied when no per-service override exists."""
    global _global_min_level
    try:
        if level is None or str(level).strip() == '':
            _global_min_level = 0
            return
        lev = str(level).upper()
        _global_min_level = _LEVEL_MAP.get(lev, 0)
    except Exception:
        pass


def is_debug_enabled(service: str) -> bool:
    """Return True when the effective minimum level for `service` is DEBUG or lower.

    This is a convenience helper used by other modules to enable debug-only
    behavior (e.g. verbose payload printing) when the service's log level is
    set to DEBUG.
    """
    try:
        min_lvl = _service_min_levels.get(service, _global_min_level)
        return min_lvl <= _LEVEL_MAP.get('DEBUG', 10)
    except Exception:
        return False


def _now_isoz():
    # Local time ISO with milliseconds (consistent with other services)
    return datetime.now().isoformat(timespec='milliseconds')


def _extract_leading_brackets(s: str):
    """Extract consecutive leading bracketed tokens from the start of string.
    Returns (tags_list, rest_of_string).
    """
    tags = []
    pos = 0
    L = len(s)
    # skip leading spaces
    while pos < L and s[pos].isspace():
        pos += 1
    while pos < L and s[pos] == '[':
        end = s.find(']', pos+1)
        if end == -1:
            break
        content = s[pos+1:end].strip()
        tags.append(content)
        pos = end + 1
        # skip spaces after bracket
        while pos < L and s[pos].isspace():
            pos += 1
    rest = s[pos:].lstrip()
    return tags, rest


def _sanitize_tag(t: str) -> str:
    # remove troublesome characters and limit length
    t2 = re.sub(r"[\n\r\t]", ' ', _redact_text(t))
    t2 = t2.replace(',', '').replace('"', '').replace("'", '')
    if len(t2) > 60:
        t2 = t2[:60]
    return t2


def rotate_logfile_if_oversize(path: str, max_bytes: int = DEFAULT_ROTATE_MAX_BYTES, backup_count: int = DEFAULT_ROTATE_BACKUP_COUNT):
    """Rotate `path` when it exceeds `max_bytes`.

    Keep current plus `backup_count` rotated generations named
    `<path>.1`, `<path>.2`, ... `<path>.<backup_count>`.
    This function is intentionally simple and safe to call before writes.
    """
    try:
        with advisory_file_lock(Path(path)):
            _rotate_logfile_if_oversize_unlocked(Path(path), max_bytes, backup_count)
    except Exception as exc:
        _write_fallback_error("rotate logfile", exc)


def _rotate_logfile_if_oversize_unlocked(path: Path, max_bytes: int, backup_count: int) -> None:
    backup_count = _parse_nonnegative_int(backup_count, DEFAULT_ROTATE_BACKUP_COUNT)
    _prune_rotated_generations_unlocked(path, backup_count)
    if not path.exists() or path.stat().st_size <= int(max_bytes):
        return
    if backup_count <= 0:
        _trim_logfile_to_max_bytes_unlocked(path, max_bytes)
        return
    oldest = Path(f"{path}.{backup_count}")
    oldest.unlink(missing_ok=True)
    for idx in range(backup_count - 1, 0, -1):
        src = Path(f"{path}.{idx}")
        if src.exists():
            os.replace(src, Path(f"{path}.{idx + 1}"))
    os.replace(path, Path(f"{path}.1"))


def _prune_rotated_generations_unlocked(path: Path, backup_count: int) -> None:
    """Remove numeric generations outside the configured retention count."""
    backup_count = _parse_nonnegative_int(backup_count, DEFAULT_ROTATE_BACKUP_COUNT)
    prefix = f"{path.name}."
    for candidate in path.parent.glob(f"{path.name}.*"):
        suffix = candidate.name.removeprefix(prefix)
        if suffix.isdigit() and int(suffix) > backup_count:
            candidate.unlink(missing_ok=True)


def rotate_managed_log_before_open(path: str | Path, scope_id: str | None = None) -> Path:
    """Rotate a closed managed transcript under its physical lock."""
    log_path = Path(path)
    resolved_scope = scope_id or resolve_managed_log_scope(log_path)
    if resolved_scope not in MANAGED_LOG_SCOPES:
        raise ValueError("Path is not in a managed log scope")
    max_bytes, backup_count = get_managed_scope_settings(resolved_scope)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with advisory_file_lock(log_path):
        _rotate_logfile_if_oversize_unlocked(log_path, max_bytes, backup_count)
    return log_path


def append_managed_transcript_line(path: str | Path, text: str, scope_id: str | None = None) -> None:
    """Sanitize, rotate, and append one complete transcript line atomically."""
    log_path = Path(path)
    resolved_scope = scope_id or resolve_managed_log_scope(log_path)
    if resolved_scope not in MANAGED_LOG_SCOPES:
        raise ValueError("Path is not in a managed log scope")
    max_bytes, backup_count = get_managed_scope_settings(resolved_scope)
    line = _redact_text(text).rstrip("\r\n") + "\n"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with advisory_file_lock(log_path):
        _rotate_logfile_if_oversize_unlocked(log_path, max_bytes, backup_count)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()


def purge_log_to_rotated(
    path: str,
    max_bytes: int = DEFAULT_ROTATE_MAX_BYTES,
    backup_count: int = DEFAULT_ROTATE_BACKUP_COUNT,
):
    """Force-rotate `path` according to its configured retention and empty it.

    Returns (success: bool, message: str).
    """
    try:
        with advisory_file_lock(Path(path)):
            return _purge_log_to_rotated_unlocked(Path(path), max_bytes, backup_count)
    except Exception as exc:
        return False, f"Failed to purge logfile: {_redact_text(exc)}"


def _purge_log_to_rotated_unlocked(p: Path, max_bytes: int, backup_count: int):
    if not p.exists():
        return False, "Logfile does not exist"

    max_bytes = _parse_positive_int(max_bytes, DEFAULT_ROTATE_MAX_BYTES)
    backup_count = _parse_nonnegative_int(backup_count, DEFAULT_ROTATE_BACKUP_COUNT)
    if backup_count <= 0:
        with p.open("r+b") as handle:
            handle.truncate(0)
        _prune_rotated_generations_unlocked(p, backup_count)
        return True, f"Truncated {p.name}; rotated backups are disabled"

    with p.open("rb") as handle:
        handle.seek(max(0, p.stat().st_size - max_bytes))
        content = handle.read()
    temp = p.with_name(f".{p.name}.purge.{os.getpid()}.tmp")
    try:
        temp.write_bytes(content)
        os.chmod(temp, p.stat().st_mode & 0o777)
        _prune_rotated_generations_unlocked(p, backup_count)
        oldest = Path(f"{p}.{backup_count}")
        oldest.unlink(missing_ok=True)
        for idx in range(backup_count - 1, 0, -1):
            source = Path(f"{p}.{idx}")
            if source.exists():
                os.replace(source, Path(f"{p}.{idx + 1}"))
        os.replace(temp, Path(f"{p}.1"))
        with p.open("r+b") as handle:
            handle.truncate(0)
    finally:
        temp.unlink(missing_ok=True)
    return True, f"Rotated {p.name} with {backup_count} backup generation(s) and truncated the current log"


def _write_fallback_error(operation: str, exc: Exception) -> None:
    """Report helper failures to sanitized stderr without recursing into logging."""
    try:
        sys.stderr.write(f"{_now_isoz()} [LoggingHelpers] {_redact_text(operation)} failed: {_redact_text(exc)}\n")
    except Exception:
        pass


def human_log(service: str, msg: str, user: str = None, tags=None, level: str = None, code: str = None, meta: dict = None, logfile: str = None):
    """Write a canonical human-readable log line.

    Format:
    2025-11-20T12:55:50.123Z [SERVICE] [tag1] [tag2] [User:mani] message... {json_meta}

    Leading bracket tokens inside `msg` are treated as tags if they appear
    at the very start of `msg`. If one of those tokens matches `User:...`,
    it is extracted as the `user` field.
    """
    try:
        if tags is None:
            tags = []
        # Extract leading brackets from msg (if any)
        safe_msg = _redact_text(msg or '')
        safe_service = _redact_text(service)
        leading, rest = _extract_leading_brackets(safe_msg)
        # Recognize explicit level tokens among leading brackets
        recognized_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        level_from_msg = None
        for lt in leading:
            up = lt.upper()
            if up in recognized_levels and level_from_msg is None and not level:
                level_from_msg = up
                # do not add this token to tags
            elif lt.lower().startswith('user:') and not user:
                user = lt.split(':', 1)[1].strip()
            else:
                tags.append(lt)

        # sanitize tags
        tags = [_sanitize_tag(t) for t in tags if t]

        # Simple normalization: prefer `user.name` for User objects, otherwise
        # accept a string user or fall back to str(user).
        try:
            if isinstance(user, str):
                user_name = _redact_text(user)
            elif user is None:
                user_name = None
            else:
                user_name = getattr(user, 'name', None)
                if user_name is None:
                    user_name = _safe_text(user)
                user_name = _redact_text(user_name)
        except Exception:
            try:
                user_name = _redact_text(user)
            except Exception:
                user_name = None

        parts = []
        parts.append(_now_isoz())
        parts.append(f'[{safe_service}]')
        # Determine final level (explicit param > message token > heuristic > default INFO)
        if level:
            lev = str(level).upper()
        elif level_from_msg:
            lev = level_from_msg
        else:
            # Heuristic: if caller didn't pass a level, infer from message content
            try:
                low = (rest or '').lower()
                warn_tokens = ('warn', 'demot', 'backoff', 'timeout', 'could not', "couldn't", 'cannot', 'requesttimeout', 'rate limit')
                if any(k in low for k in ('error', 'failed', 'exception', 'traceback')):
                    lev = 'ERROR'
                elif any(k in low for k in warn_tokens) or re.search(r"\b429\b", low):
                    lev = 'WARNING'
                elif any(k in low for k in ('debug', 'payload', 'preview')):
                    lev = 'DEBUG'
                else:
                    lev = 'INFO'
            except Exception:
                lev = 'INFO'
        parts.append(f'[{lev}]')

        # Respect per-service/global minimum levels: if the message level is lower
        # than configured minimum, skip writing the log.
        try:
            msg_level_num = _LEVEL_MAP.get(lev, 20)
            min_lvl = _service_min_levels.get(service, _global_min_level)
            if msg_level_num < min_lvl:
                return
        except Exception:
            pass
        for t in tags:
            parts.append(f'[{t}]')
        if user_name:
            # sanitize user_name for safety and length
            try:
                u = _sanitize_tag(str(user_name))
            except Exception:
                u = str(user_name)
            parts.append(f'[User:{u}]')
        # main message
        line = ' '.join(parts) + ' ' + (rest or '')
        if code:
            line = line + ' ' + _redact_text(code)
        # Explicit call metadata overrides values inherited from request/task context.
        context_meta = dict(_logging_context.get())
        if isinstance(meta, dict):
            context_meta.update(meta)
            effective_meta = context_meta
        elif meta is None:
            effective_meta = context_meta or None
        else:
            # Preserve compatibility for callers that supplied non-dict JSON metadata.
            effective_meta = meta
        if effective_meta is not None:
            try:
                j = json.dumps(_redact_value(effective_meta), ensure_ascii=False, sort_keys=True)
                line = line + ' ' + j
            except Exception:
                line = line + ' ' + json.dumps(REDACTED)

        # Determine logfile path
        if not logfile:
            p = LOG_ROOT
            p.mkdir(parents=True, exist_ok=True)
            log_stem = LOG_GROUPS.get(service, service)
            logfile = str(p / f'{log_stem}.log')
        else:
            Path(logfile).parent.mkdir(parents=True, exist_ok=True)

        rotate_max_bytes, rotate_backup_count = get_rotate_settings(service=service, logfile=logfile)
        with advisory_file_lock(Path(logfile)):
            _rotate_logfile_if_oversize_unlocked(Path(logfile), rotate_max_bytes, rotate_backup_count)
            with open(logfile, 'a', encoding='utf-8') as f:
                f.write(line.rstrip() + '\n')
                f.flush()
    except Exception:
        # Best-effort sanitized fallback; never expose the original payload.
        try:
            sys.stderr.write(f"{_now_isoz()} [{_redact_text(service)}] {_redact_text(msg)}\n")
        except Exception:
            pass
