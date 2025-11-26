import json
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Optional

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
    # UTC ISO with milliseconds
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


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
    t2 = re.sub(r"[\n\r\t]", ' ', t)
    t2 = t2.replace(',', '').replace('"', '').replace("'", '')
    if len(t2) > 60:
        t2 = t2[:60]
    return t2


def rotate_logfile_if_oversize(path: str, max_bytes: int = 10 * 1024 * 1024):
    """Rotate `path` when it exceeds `max_bytes`.

    Keep only two generations: the current file and a single rotated
    backup named `<path>.1`. If `<path>.1` exists it will be overwritten.
    This function is intentionally simple and safe to call before writes.
    """
    try:
        p = Path(path)
        if not p.exists():
            return
        try:
            size = p.stat().st_size
        except Exception:
            return
        if size <= int(max_bytes):
            return
        # Remove existing .1 if present
        backup = Path(str(path) + '.1')
        try:
            if backup.exists():
                backup.unlink()
        except Exception:
            pass
        # Rename current to .1 (atomic on same filesystem)
        try:
            p.rename(backup)
        except Exception:
            # Best-effort: try copy-then-truncate
            try:
                data = p.read_bytes()
                backup.write_bytes(data)
                p.unlink()
            except Exception:
                pass
    except Exception:
        # Never raise from logging helpers
        pass


def purge_log_to_rotated(path: str, max_bytes: int = 10 * 1024 * 1024):
    """Purge `path` but keep the file. If `<path>.1` exists and has room,
    append the content of `path` to it up to `max_bytes`. Otherwise, move
    the current file to `<path>.1` (if none exists) or truncate `path`.

    Returns (success: bool, message: str).
    """
    try:
        p = Path(path)
        if not p.exists():
            return False, 'Logfile does not exist'
        rotated = Path(str(p) + '.1')
        orig_size = p.stat().st_size

        # If no rotated file exists, move current to .1 and recreate empty logfile
        if not rotated.exists():
            try:
                p.replace(rotated)
                p.open('w').close()
                return True, f'Moved logfile to {rotated.name} and recreated {p.name}'
            except Exception as e:
                return False, f'Failed to move logfile to rotated: {e}'

        rot_size = rotated.stat().st_size
        # If rotated has room for the whole original, append it
        if rot_size < max_bytes and (rot_size + orig_size) <= max_bytes:
            try:
                with rotated.open('ab') as rf, p.open('rb') as of:
                    rf.write(of.read())
                with p.open('r+') as of:
                    of.truncate(0)
                return True, f'Appended logfile to {rotated.name} and truncated {p.name}'
            except Exception as e:
                return False, f'Failed to append to rotated logfile: {e}'

        # If appending would overflow the rotated file, prefer replacing the
        # rotated file with the full current logfile so we don't lose the
        # current content. If the current logfile itself is larger than
        # max_bytes, write the tail of the current logfile (last max_bytes)
        # into the rotated file instead.
        try:
            if orig_size <= max_bytes:
                # Remove existing rotated and move current to rotated
                try:
                    try:
                        rotated.unlink()
                    except Exception:
                        pass
                    p.replace(rotated)
                    # recreate empty original file
                    p.open('w').close()
                    return True, f'Replaced {rotated.name} with full logfile and recreated {p.name}'
                except Exception as e:
                    return False, f'Failed to replace rotated logfile: {e}'
            else:
                # Current logfile larger than max_bytes: write only the tail
                try:
                    with p.open('rb') as of:
                        of.seek(max(0, orig_size - max_bytes))
                        chunk = of.read()
                    with rotated.open('wb') as rf:
                        rf.write(chunk)
                    with p.open('r+') as of:
                        of.truncate(0)
                    return True, f'Wrote last {max_bytes} bytes of logfile to {rotated.name} and truncated {p.name}'
                except Exception as e:
                    return False, f'Failed to write tail to rotated logfile: {e}'
        except Exception as e:
            return False, f'Failed to manage rotated logfile: {e}'
    except Exception as e:
        return False, f'Failed to purge logfile: {e}'


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
        leading, rest = _extract_leading_brackets(msg or '')
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
                user_name = user
            elif user is None:
                user_name = None
            else:
                user_name = getattr(user, 'name', None)
                if user_name is None:
                    user_name = str(user)
        except Exception:
            try:
                user_name = str(user)
            except Exception:
                user_name = None

        parts = []
        parts.append(_now_isoz())
        parts.append(f'[{service}]')
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
            line = line + ' ' + str(code)
        # append meta as JSON if present
        if meta is not None:
            try:
                j = json.dumps(meta, ensure_ascii=False)
                line = line + ' ' + j
            except Exception:
                # ignore meta serialization errors
                pass

        # Determine logfile path
        if not logfile:
            p = Path.cwd() / 'data' / 'logs'
            p.mkdir(parents=True, exist_ok=True)
            logfile = str(p / f'{service}.log')

        # Rotate if oversize (keep only current + one rotated generation)
        try:
            rotate_logfile_if_oversize(logfile, 10 * 1024 * 1024)
        except Exception:
            pass

        # Append line atomically (no fsync)
        with open(logfile, 'a', encoding='utf-8') as f:
            f.write(line.rstrip() + '\n')
            f.flush()
    except Exception:
        # Best-effort; do not raise from logging
        try:
            print(f"{_now_isoz()} [{service}] {msg}")
        except Exception:
            pass
