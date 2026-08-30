"""PBGui client for isolated Passivbot V8 config operations."""

from __future__ import annotations

from collections import OrderedDict
import copy
import json
import os
import queue
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from file_lock import advisory_file_lock
from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from pbgui_purefunc import pb8_runtime_status
from pbgui_purefunc import PBGDIR


class PB8ConfigurationError(RuntimeError):
    """Raised when PB8 cannot validate or migrate a config."""


class PB8RuntimeBusyError(PB8ConfigurationError):
    """Raised when a retryable PB8 update blocks config runtime access."""

    retryable = True
    status_code = 503


class PB8MarketDataUnavailableError(PB8ConfigurationError):
    """Raised when PB8 cannot provide a complete collision-safe market catalog."""

    retryable = True
    status_code = 503


class PB8MarketRequestError(PB8ConfigurationError):
    """Raised when a caller submits an invalid bounded market request."""

    status_code = 422


_CACHE_TTL_SECONDS = 30.0
_CACHE_MAX_CONFIGS = 64
_cache_lock = threading.RLock()
_template_cache: tuple[float, tuple, dict] | None = None
_result_metrics_cache: tuple[float, tuple, list[str]] | None = None
_optimize_metadata_cache: tuple[float, tuple, dict] | None = None
_coin_override_metadata_cache: OrderedDict[tuple, tuple[float, tuple, dict]] = OrderedDict()
_exchange_metadata_cache: tuple[float, tuple, dict] | None = None
_market_catalog_cache: OrderedDict[tuple, tuple[float, tuple, dict]] = OrderedDict()
_config_cache: OrderedDict[str, tuple[float, tuple[int, int], tuple, dict]] = OrderedDict()
_migration_helper_lock = threading.RLock()
_migration_helper_state_lock = threading.RLock()
_migration_helper_shutdown = threading.Event()
_migration_helper_process: subprocess.Popen[str] | None = None
_migration_helper_fingerprint: tuple | None = None
_migration_helper_responses: queue.Queue[str | None] | None = None
_migration_helper_reader_thread: threading.Thread | None = None


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


def _stop_migration_helper_locked() -> None:
    """Stop and reap the persistent migration helper while holding its lock."""
    global _migration_helper_process, _migration_helper_fingerprint
    global _migration_helper_responses, _migration_helper_reader_thread
    with _migration_helper_state_lock:
        proc = _migration_helper_process
        reader = _migration_helper_reader_thread
        _migration_helper_process = None
        _migration_helper_fingerprint = None
        _migration_helper_responses = None
        _migration_helper_reader_thread = None
    if proc is not None:
        try:
            if proc.stdin is not None:
                proc.stdin.close()
        except OSError:
            pass
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
        if proc.stdout is not None:
            proc.stdout.close()
    if reader is not None and reader.is_alive():
        reader.join(timeout=2)


def shutdown_pb8_migration_helper() -> None:
    """Idempotently stop the API-owned persistent PB8 migration helper."""
    with _migration_helper_lock:
        _stop_migration_helper_locked()


def prepare_pb8_migration_helper_startup() -> None:
    """Allow helper creation for a new API lifespan before scheduling prewarm."""
    _migration_helper_shutdown.clear()


def interrupt_pb8_migration_helper() -> None:
    """Wake a blocked helper request during API shutdown without waiting for its I/O lock."""
    _migration_helper_shutdown.set()
    with _migration_helper_state_lock:
        proc = _migration_helper_process
        if proc is not None and proc.poll() is None:
            proc.terminate()


def _migration_helper_reader(proc: subprocess.Popen[str], responses: queue.Queue[str | None]) -> None:
    """Forward helper response lines to the bounded synchronous request path."""
    try:
        if proc.stdout is not None:
            for line in proc.stdout:
                responses.put(line)
    finally:
        responses.put(None)


def _ensure_migration_helper_locked(status: dict) -> None:
    """Start or replace the persistent helper for the current PB8 runtime fingerprint."""
    global _migration_helper_process, _migration_helper_fingerprint
    global _migration_helper_responses, _migration_helper_reader_thread
    fingerprint = _runtime_fingerprint(status)
    if (
        _migration_helper_process is not None
        and _migration_helper_process.poll() is None
        and _migration_helper_fingerprint == fingerprint
    ):
        return
    _stop_migration_helper_locked()
    helper = Path(__file__).resolve().with_name("pb8_config_helper.py")
    with _migration_helper_state_lock:
        if _migration_helper_shutdown.is_set():
            raise PB8ConfigurationError("PB8 migration helper is shutting down")
        proc = subprocess.Popen(
            [status["pb8venv"], str(helper), "--serve"],
            cwd=status["pb8dir"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
            close_fds=True,
        )
        responses: queue.Queue[str | None] = queue.Queue()
        reader = threading.Thread(
            target=_migration_helper_reader,
            args=(proc, responses),
            name="pb8-migration-helper-reader",
            daemon=True,
        )
        reader.start()
        _migration_helper_process = proc
        _migration_helper_fingerprint = fingerprint
        _migration_helper_responses = responses
        _migration_helper_reader_thread = reader


def _call_migration_helper(operation: str, **payload) -> dict:
    """Call the persistent PB8 helper with one serialized bounded request."""
    runtime_lease = None
    try:
        runtime_lease = acquire_master_runtime_lock(Path(PBGDIR))
        status = _runtime()
        with _migration_helper_lock:
            _ensure_migration_helper_locked(status)
            proc = _migration_helper_process
            responses = _migration_helper_responses
            if proc is None or proc.stdin is None or responses is None:
                raise PB8ConfigurationError("PB8 migration helper is unavailable")
            request = {"operation": operation, "pb8_dir": status["pb8dir"], **payload}
            proc.stdin.write(json.dumps(request, separators=(",", ":"), allow_nan=False) + "\n")
            proc.stdin.flush()
            try:
                line = responses.get(timeout=120)
            except queue.Empty as exc:
                _stop_migration_helper_locked()
                raise PB8ConfigurationError("PB8 migration helper timed out") from exc
            if line is None:
                _stop_migration_helper_locked()
                raise PB8ConfigurationError("PB8 migration helper exited unexpectedly")
            try:
                response = json.loads(line)
            except json.JSONDecodeError as exc:
                _stop_migration_helper_locked()
                raise PB8ConfigurationError("PB8 migration helper returned invalid JSON") from exc
            if not response.get("ok"):
                detail = str(response.get("detail") or "PB8 migration operation failed").strip()
                raise PB8ConfigurationError(detail[-2000:])
            result = response.get("result")
            if not isinstance(result, dict):
                raise PB8ConfigurationError("PB8 migration helper returned no result")
    except MasterUpdateBusyError as exc:
        raise PB8RuntimeBusyError(
            "PB8 is being installed or updated. Retry this configuration operation when the update finishes."
        ) from exc
    except (OSError, BrokenPipeError) as exc:
        with _migration_helper_lock:
            _stop_migration_helper_locked()
        raise PB8ConfigurationError(f"PB8 migration helper failed: {exc}") from exc
    finally:
        if runtime_lease is not None:
            runtime_lease.release()
    return result


def start_pb8_migration_helper() -> None:
    """Prewarm the persistent helper so UI-triggered migrations avoid cold imports."""
    _call_migration_helper("optimize_metadata")


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


def get_pb8_coin_override_metadata(hsl_signal_mode: str, strategy_kind: str) -> dict:
    """Return typed PB8 coin-override metadata for one effective config context."""
    fingerprint = _runtime_fingerprint()
    cache_key = (str(hsl_signal_mode), str(strategy_kind))
    with _cache_lock:
        cached = _coin_override_metadata_cache.get(cache_key)
        if cached and cached[0] > time.monotonic() and cached[1] == fingerprint:
            _coin_override_metadata_cache.move_to_end(cache_key)
            return copy.deepcopy(cached[2])
        metadata = _call_helper(
            "coin_override_metadata",
            hsl_signal_mode=hsl_signal_mode,
            strategy_kind=strategy_kind,
        )
        if metadata.get("contract_version") != 1 or not isinstance(metadata.get("params"), dict):
            raise PB8ConfigurationError("PB8 config helper returned invalid coin override metadata")
        _coin_override_metadata_cache[cache_key] = (
            time.monotonic() + _CACHE_TTL_SECONDS,
            fingerprint,
            copy.deepcopy(metadata),
        )
        _coin_override_metadata_cache.move_to_end(cache_key)
        while len(_coin_override_metadata_cache) > 16:
            _coin_override_metadata_cache.popitem(last=False)
        return copy.deepcopy(metadata)


def validate_pb8_override_bundle(config_path: Path | str) -> None:
    """Validate staged inline and file coin overrides through PB8's runtime parser."""
    result = _call_helper("validate_overrides", config_path=str(Path(config_path).resolve()))
    if result.get("valid") is not True:
        raise PB8ConfigurationError("PB8 config helper did not validate coin overrides")


def validate_pb8_optimizer_overrides(config: dict, *, base_config_path: str = "") -> None:
    """Validate strategy-dependent optimizer overrides through PB8's native runtime."""
    result = _call_helper(
        "validate_optimizer_overrides",
        config=config,
        base_config_path=base_config_path,
    )
    if result.get("valid") is not True:
        raise PB8ConfigurationError("PB8 config helper did not validate optimizer overrides")


def validate_pb8_optimize_preflight(config: dict, *, base_config_path: str = "") -> dict:
    """Run the installed PB8 backend's static preflight before queue/start."""
    result = _call_helper(
        "optimize_preflight",
        config=config,
        base_config_path=base_config_path,
    )
    if result.get("contract_version") != 1 or result.get("valid") is not True:
        raise PB8ConfigurationError("PB8 optimize preflight returned an invalid result")
    return result


def get_pb8_exchange_metadata() -> dict:
    """Return PB8's vetted live and historical exchange capabilities."""
    global _exchange_metadata_cache
    with _cache_lock:
        now = time.monotonic()
        fingerprint = _runtime_fingerprint()
        if _exchange_metadata_cache and _exchange_metadata_cache[0] > now and _exchange_metadata_cache[1] == fingerprint:
            return copy.deepcopy(_exchange_metadata_cache[2])
        metadata = _call_helper("exchange_metadata")
        required = ("live", "backtest", "optimize", "suite")
        if metadata.get("contract_version") != 1 or any(
            not isinstance(metadata.get(key), list)
            or not all(isinstance(item, str) and item for item in metadata[key])
            for key in required
        ):
            raise PB8ConfigurationError("PB8 config helper returned invalid exchange metadata")
        normalized = {
            "contract_version": 1,
            **{key: sorted(set(metadata[key])) for key in required},
        }
        _exchange_metadata_cache = (now + _CACHE_TTL_SECONDS, fingerprint, copy.deepcopy(normalized))
        return copy.deepcopy(normalized)


def get_pb8_market_identifiers(
    exchanges: list[str],
    identifiers: list[str] | None = None,
    *,
    quote: str | None = None,
) -> dict:
    """Return PB8.1's official collision-aware market catalog and statuses."""
    if not isinstance(exchanges, list) or not exchanges or len(exchanges) > 16:
        raise PB8MarketRequestError("Select between one and 16 exchanges")
    if identifiers is not None and not isinstance(identifiers, list):
        raise PB8MarketRequestError("Market identifiers must be an array")
    if identifiers is not None and len(identifiers) > 1000:
        raise PB8MarketRequestError("Market identifiers exceed the maximum of 1000 items")
    normalized_exchanges = []
    for exchange in exchanges:
        if not isinstance(exchange, str) or exchange != exchange.strip() or not exchange:
            raise PB8MarketRequestError("Exchange names must be non-empty trimmed strings")
        if len(exchange.encode("utf-8")) > 64 or any(ord(char) < 32 or ord(char) == 127 for char in exchange):
            raise PB8MarketRequestError("Invalid exchange name")
        normalized_exchanges.append(exchange)
    normalized_identifiers = []
    for identifier in identifiers or []:
        if not isinstance(identifier, str) or identifier != identifier.strip() or not identifier:
            raise PB8MarketRequestError("Market identifiers must be non-empty trimmed strings")
        if len(identifier.encode("utf-8")) > 256 or any(ord(char) < 32 or ord(char) == 127 for char in identifier):
            raise PB8MarketRequestError("Invalid market identifier")
        normalized_identifiers.append(identifier)
    if quote is not None and (
        not isinstance(quote, str)
        or quote != quote.strip()
        or not quote
        or len(quote) > 16
        or not quote.isalnum()
    ):
        raise PB8MarketRequestError("Quote must contain at most 16 letters or digits")
    fingerprint = _runtime_fingerprint()
    cache_key = (tuple(normalized_exchanges), quote or "")
    with _cache_lock:
        cached = _market_catalog_cache.get(cache_key)
        if not normalized_identifiers and cached and cached[0] > time.monotonic() and cached[1] == fingerprint:
            _market_catalog_cache.move_to_end(cache_key)
            return copy.deepcopy(cached[2])
    try:
        with advisory_file_lock(Path(PBGDIR) / "data" / ".pb8-market-helper"):
            if not normalized_identifiers:
                with _cache_lock:
                    cached = _market_catalog_cache.get(cache_key)
                    if cached and cached[0] > time.monotonic() and cached[1] == fingerprint:
                        _market_catalog_cache.move_to_end(cache_key)
                        return copy.deepcopy(cached[2])
            result = _call_helper(
                "market_identifiers",
                exchanges=normalized_exchanges,
                identifiers=normalized_identifiers,
                quote=quote,
            )
    except PB8RuntimeBusyError:
        raise
    except PB8ConfigurationError as exc:
        raise PB8MarketDataUnavailableError(str(exc)) from exc
    if (
        result.get("contract_version") != 1
        or not isinstance(result.get("symbols"), list)
        or not isinstance(result.get("catalog"), list)
        or not isinstance(result.get("statuses"), dict)
    ):
        raise PB8ConfigurationError("PB8 config helper returned invalid market identifiers")
    if not normalized_identifiers:
        with _cache_lock:
            _market_catalog_cache[cache_key] = (
                time.monotonic() + _CACHE_TTL_SECONDS,
                fingerprint,
                copy.deepcopy(result),
            )
            _market_catalog_cache.move_to_end(cache_key)
            while len(_market_catalog_cache) > 32:
                _market_catalog_cache.popitem(last=False)
    return copy.deepcopy(result)


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
    return _call_migration_helper(
        "migrate_v7",
        source_path=str(Path(source_path).resolve()),
        output_path=str(Path(output_path).resolve()),
        allow_manual_review_output=allow_manual_review_output,
    )
