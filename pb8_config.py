"""PBGui client for isolated Passivbot V8 config operations."""

from __future__ import annotations

from collections import OrderedDict
import copy
import json
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from pbgui_purefunc import pb8_runtime_status
from pbgui_purefunc import PBGDIR


class PB8ConfigurationError(RuntimeError):
    """Raised when PB8 cannot validate or migrate a config."""


class PB8RuntimeBusyError(PB8ConfigurationError):
    """Raised when a retryable PB8 update blocks config runtime access."""

    retryable = True
    status_code = 503


_CACHE_TTL_SECONDS = 30.0
_CACHE_MAX_CONFIGS = 64
_cache_lock = threading.RLock()
_template_cache: tuple[float, tuple, dict] | None = None
_result_metrics_cache: tuple[float, tuple, list[str]] | None = None
_optimize_metadata_cache: tuple[float, tuple, dict] | None = None
_config_cache: OrderedDict[str, tuple[float, tuple[int, int], tuple, dict]] = OrderedDict()


def _file_signature(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_mtime_ns, stat.st_size


def _runtime_fingerprint(status: dict | None = None) -> tuple:
    """Identify the exact PB8 source/helper runtime used by cached values."""
    current = status or pb8_runtime_status()
    pb8_dir = Path(str(current.get("pb8dir") or "")).resolve(strict=False)
    helper = Path(__file__).resolve().with_name("pb8_config_helper.py")

    def signature(value: str | Path | None) -> tuple[int, int]:
        if not value:
            return 0, 0
        path = Path(value)
        try:
            stat = path.stat()
            return stat.st_mtime_ns, stat.st_size
        except OSError:
            return 0, 0

    git_head = ""
    head_path = pb8_dir / ".git" / "HEAD"
    try:
        head_value = head_path.read_text(encoding="utf-8").strip()
        if head_value.startswith("ref: "):
            ref_path = pb8_dir / ".git" / head_value[5:].strip()
            git_head = ref_path.read_text(encoding="utf-8").strip()
        else:
            git_head = head_value
    except OSError:
        pass
    return (
        str(pb8_dir),
        str(current.get("pb8venv") or ""),
        str(current.get("version") or ""),
        str(current.get("config_schema") or ""),
        git_head,
        signature(current.get("version_file")),
        signature(current.get("config_schema_file")),
        signature(helper),
    )


def _cache_config(path: Path, config: dict, fingerprint: tuple | None = None) -> None:
    key = str(path.resolve())
    signature = _file_signature(path)
    runtime_fingerprint = fingerprint or _runtime_fingerprint()
    _config_cache[key] = (
        time.monotonic() + _CACHE_TTL_SECONDS,
        signature,
        runtime_fingerprint,
        copy.deepcopy(config),
    )
    _config_cache.move_to_end(key)
    while len(_config_cache) > _CACHE_MAX_CONFIGS:
        _config_cache.popitem(last=False)


def _write_prepared_config(config: dict, destination: Path) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(config, handle, indent=4)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, destination)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise
    with _cache_lock:
        _cache_config(destination, config)
    return copy.deepcopy(config)


def _runtime() -> dict:
    """Return a ready PB8 runtime or raise a concise configuration error."""
    status = pb8_runtime_status()
    if not status.get("ready"):
        detail = "; ".join(status.get("errors") or []) or "PB8 runtime is not ready"
        raise PB8ConfigurationError(detail)
    return status


def _call_helper(operation: str, **payload) -> dict:
    """Execute one helper request in PB8's Python environment."""
    runtime_lease = None
    try:
        runtime_lease = acquire_master_runtime_lock(Path(PBGDIR))
        status = _runtime()
        helper = Path(__file__).resolve().with_name("pb8_config_helper.py")
        request = {
            "operation": operation,
            "pb8_dir": status["pb8dir"],
            **payload,
        }
        proc = subprocess.run(
            [status["pb8venv"], str(helper)],
            cwd=status["pb8dir"],
            input=json.dumps(request),
            text=True,
            capture_output=True,
            check=False,
            timeout=120,
        )
    except MasterUpdateBusyError as exc:
        raise PB8RuntimeBusyError(
            "PB8 is being installed or updated. Retry this configuration operation when the update finishes."
        ) from exc
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PB8ConfigurationError(f"PB8 config helper failed: {exc}") from exc
    finally:
        if runtime_lease is not None:
            runtime_lease.release()
    try:
        response = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        detail = (proc.stderr or proc.stdout or "empty helper response").strip()[-2000:]
        raise PB8ConfigurationError(f"Invalid PB8 config helper response: {detail}") from exc
    if proc.returncode != 0 or not response.get("ok"):
        detail = str(response.get("detail") or proc.stderr or "PB8 config operation failed").strip()
        raise PB8ConfigurationError(detail[-2000:])
    result = response.get("result")
    if not isinstance(result, dict):
        raise PB8ConfigurationError("PB8 config helper returned no result")
    return result


def pb8_config_status() -> dict:
    """Return versions reported by the executable PB8 config runtime."""
    return _call_helper("status")


def get_pb8_template_config() -> dict:
    """Return the current installed PB8 template as a canonical config."""
    global _template_cache
    with _cache_lock:
        now = time.monotonic()
        fingerprint = _runtime_fingerprint()
        if _template_cache and _template_cache[0] > now and _template_cache[1] == fingerprint:
            return copy.deepcopy(_template_cache[2])
        config = _call_helper("default")["config"]
        _template_cache = (now + _CACHE_TTL_SECONDS, fingerprint, copy.deepcopy(config))
        return copy.deepcopy(config)


def get_pb8_result_metrics() -> list[str]:
    """Return metric names accepted by the installed PB8 visibility config."""
    global _result_metrics_cache
    with _cache_lock:
        now = time.monotonic()
        fingerprint = _runtime_fingerprint()
        if _result_metrics_cache and _result_metrics_cache[0] > now and _result_metrics_cache[1] == fingerprint:
            return list(_result_metrics_cache[2])
        metrics = _call_helper("result_metrics").get("metrics")
        if not isinstance(metrics, list) or not all(isinstance(item, str) for item in metrics):
            raise PB8ConfigurationError("PB8 config helper returned invalid result metrics")
        normalized = sorted(set(metrics))
        _result_metrics_cache = (now + _CACHE_TTL_SECONDS, fingerprint, normalized)
        return list(normalized)


def get_pb8_optimize_metadata() -> dict:
    """Return a cached optimizer model reported by the installed PB8 runtime."""
    global _optimize_metadata_cache
    with _cache_lock:
        now = time.monotonic()
        fingerprint = _runtime_fingerprint()
        if _optimize_metadata_cache and _optimize_metadata_cache[0] > now and _optimize_metadata_cache[1] == fingerprint:
            return copy.deepcopy(_optimize_metadata_cache[2])
        metadata = _call_helper("optimize_metadata")
        if not isinstance(metadata.get("template"), dict) or not isinstance(metadata.get("strategies"), list):
            raise PB8ConfigurationError("PB8 config helper returned invalid optimize metadata")
        _optimize_metadata_cache = (now + _CACHE_TTL_SECONDS, fingerprint, copy.deepcopy(metadata))
        return copy.deepcopy(metadata)


def prepare_pb8_config(config: dict, *, base_config_path: str = "") -> dict:
    """Validate and canonicalize an in-memory PB8 config."""
    return _call_helper(
        "prepare",
        config=config,
        base_config_path=base_config_path,
    )["config"]


def load_pb8_config(path: Path | str) -> dict:
    """Load and canonicalize a PB8 config through the installed PB8 loader."""
    source = Path(path).resolve()
    key = str(source)
    with _cache_lock:
        signature = _file_signature(source)
        fingerprint = _runtime_fingerprint()
        cached = _config_cache.get(key)
        if (
            cached
            and cached[0] > time.monotonic()
            and cached[1] == signature
            and cached[2] == fingerprint
        ):
            _config_cache.move_to_end(key)
            return copy.deepcopy(cached[3])
        config = _call_helper("load", config_path=key)["config"]
        _cache_config(source, config, fingerprint)
        return copy.deepcopy(config)


def save_pb8_config(config: dict, path: Path | str) -> dict:
    """Validate and atomically persist a canonical PB8 config."""
    destination = Path(path)
    prepared = prepare_pb8_config(config, base_config_path=str(destination.resolve()))
    return _write_prepared_config(prepared, destination)


def save_prepared_pb8_config(config: dict, path: Path | str) -> dict:
    """Atomically persist a config already canonicalized by the PB8 helper."""
    if not isinstance(config, dict):
        raise PB8ConfigurationError("Prepared PB8 config must be an object")
    return _write_prepared_config(config, Path(path))


def cache_prepared_pb8_config(config: dict, path: Path | str) -> None:
    """Cache a prepared config after its containing directory was atomically moved."""
    source = Path(path)
    if not source.is_file():
        return
    with _cache_lock:
        _cache_config(source, config)


def migrate_pb7_config(
    source_path: Path | str,
    output_path: Path | str,
    *,
    allow_manual_review_output: bool = False,
) -> dict:
    """Run PB8's official V7 migration and return config plus report."""
    return _call_helper(
        "migrate_v7",
        source_path=str(Path(source_path).resolve()),
        output_path=str(Path(output_path).resolve()),
        allow_manual_review_output=allow_manual_review_output,
    )
