"""Isolated one-request client for native PB9 templates and inline validation."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import threading
import time

from logging_helpers import human_log as _log
from pb9_runtime import inspect_pb9_runtime
from pb9_config_helper import MAX_VALIDATION_BYTES, _HelperError, _require_json_value, check_inline_config, check_migration_input


SERVICE = "PB9Config"
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_DOCUMENT_BYTES = 1024 * 1024
_HELPER_PATH = Path(__file__).with_name("pb9_config_helper.py")
_SUPPORTED_PLATFORM = sys.platform.startswith("linux") and hasattr(os, "waitid")
_ENV_KEYS = {"PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "SYSTEMROOT", "WINDIR",
             "TMPDIR", "TMP", "TEMP", "TZ"}
_HELPER_CODES = {"invalid_request", "runtime_identity_mismatch", "native_template_failed",
                 "helper_output_limit", "invalid_template", "invalid_config", "config_version_mismatch",
                 "external_reference_rejected", "native_config_rejected", "native_validation_failed", "native_metadata_failed"}


class PB9ConfigError(RuntimeError):
    """Report fixed config failure codes without exposing subprocess data."""

    def __init__(self, code: str):
        """Keep codes available for later authenticated API error mapping."""
        self.error_codes = (code,)
        super().__init__("PB9 config request failed: " + code)


class PB9CancelSignal:
    """A poll-only stop flag whose setter never waits on an Event condition lock.

    The API's process signal handler only stores this boolean. Worker threads
    poll it, so cancellation does not depend on a responsive ASGI event loop.
    """

    def __init__(self):
        """Start with admission and native execution allowed."""
        self._requested = False

    def set(self) -> None:
        """Request cancellation without blocking or allocating synchronization state."""
        self._requested = True

    def is_set(self) -> bool:
        """Return the current cancellation request for bounded transport polling."""
        return self._requested


def _run_helper(status: dict, timeout: float, *, operation: str = "template", config: dict | None = None,
                 cancel_event: threading.Event | PB9CancelSignal | None = None) -> bytes:
    """Own one process group and nonblocking pipes under a single deadline.

    Requires normal SIGCHLD handling: no SIG_IGN or external child reaper.
    Cleanup covers descendants remaining in this group, not detached sessions.
    """
    payload = {
        "operation": operation, "pb9_dir": status["pb9dir"],
        "config_schema": status["config_schema"], "package_version": status["package_version"],
    }
    if operation in {"validate", "validate_live", "validate_backtest", "migrate_v8"}:
        payload["config"] = config
    request = json.dumps(payload, allow_nan=False).encode("utf-8")
    if len(request) > (MAX_VALIDATION_BYTES if operation in {"validate", "validate_live", "validate_backtest", "migrate_v8"} else 16384):
        raise PB9ConfigError("invalid_request")
    env = {key: value for key, value in os.environ.items() if key.upper() in _ENV_KEYS}
    env["PATH"] = os.pathsep.join((str(Path(status["pb9venv"]).parent), os.defpath))
    output = bytearray()
    if cancel_event is not None and cancel_event.is_set():
        raise PB9ConfigError("helper_cancelled")
    proc = subprocess.Popen(
        [status["pb9venv"], "-I", "-B", str(_HELPER_PATH)], cwd=status["pb9dir"], env=env,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
        start_new_session=True,
    )
    selector = None
    deadline = time.monotonic() + timeout

    def stop_group() -> None:
        """Signal the owned group while its leader is still unreaped, preventing PID reuse."""
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass  # The group can disappear naturally before cleanup.

    def exited_without_reaping() -> bool:
        """Observe exit without releasing the leader PID before group cleanup."""
        return os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None

    try:
        selector = selectors.DefaultSelector()
        os.set_blocking(proc.stdin.fileno(), False)
        os.set_blocking(proc.stdout.fileno(), False)
        selector.register(proc.stdin, selectors.EVENT_WRITE, "input")
        selector.register(proc.stdout, selectors.EVENT_READ, "output")
        remaining_input = memoryview(request)
        group_stopped = False
        while selector.get_map():
            if cancel_event is not None and cancel_event.is_set():
                raise PB9ConfigError("helper_cancelled")
            remaining_time = deadline - time.monotonic()
            if remaining_time <= 0:
                raise PB9ConfigError("helper_timeout")
            if not group_stopped and exited_without_reaping():
                # A successful leader may leave descendants holding its pipe.
                stop_group()
                group_stopped = True
            for key, _events in selector.select(min(remaining_time, 0.05)):
                try:
                    if key.data == "input":
                        count = os.write(key.fd, remaining_input)
                        remaining_input = remaining_input[count:]
                        if not remaining_input:
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                    else:
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                        elif len(output) + len(chunk) > MAX_RESPONSE_BYTES:
                            raise PB9ConfigError("helper_output_limit")
                        else:
                            output.extend(chunk)
                except BlockingIOError:
                    continue
        while not exited_without_reaping():
            if cancel_event is not None and cancel_event.is_set():
                raise PB9ConfigError("helper_cancelled")
            remaining_time = deadline - time.monotonic()
            if remaining_time <= 0:
                raise PB9ConfigError("helper_timeout")
            time.sleep(min(remaining_time, 0.01))
    finally:
        try:
            # Never poll/wait before this: an unreaped leader reserves the PGID.
            stop_group()
        finally:
            if selector is not None:
                selector.close()
            proc.stdin.close()
            proc.stdout.close()
            proc.wait()
    if proc.returncode != 0:
        raise PB9ConfigError("helper_failed")
    return bytes(output)


def _checked_response(raw: bytes, status: dict, *, operation: str = "template") -> dict:
    """Bind the native result to the selected source and static schema identity."""
    try:
        def reject_constant(_value):
            """The protocol permits strict JSON only, even on unexpected responses."""
            raise ValueError("nonfinite JSON constant")

        response = json.loads(raw, parse_constant=reject_constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise PB9ConfigError("invalid_response") from exc
    if not isinstance(response, dict) or not isinstance(response.get("ok"), bool):
        raise PB9ConfigError("invalid_response")
    if not response["ok"]:
        code = response.get("error_code")
        raise PB9ConfigError(code if isinstance(code, str) and code in _HELPER_CODES else "helper_failed")
    result = response.get("result")
    if not isinstance(result, dict):
        raise PB9ConfigError("invalid_response")
    if operation == "template" and not isinstance(result.get("config"), dict):
        raise PB9ConfigError("invalid_response")
    if operation in {"validate", "validate_live", "validate_backtest"} and result.get("valid") is not True:
        raise PB9ConfigError("invalid_response")
    src = (Path(status["pb9dir"]) / "src").resolve()
    module_keys = ("schema_module_file", "version_module_file")
    if operation in {"validate", "validate_live", "validate_backtest"}:
        module_keys += ("load_module_file",)
    elif operation == "migrate_v8":
        module_keys += ("converter_module_file",)
    elif operation == "metadata":
        module_keys += ("registry_module_file", "strategy_module_file")
    for key in module_keys:
        value = result.get(key)
        if not isinstance(value, str) or not Path(value).is_absolute() or not Path(value).resolve().is_relative_to(src):
            raise PB9ConfigError("runtime_identity_mismatch")
    if (result.get("config_schema") != status["config_schema"]
            or result.get("package_version") != status["package_version"]
            or operation == "template" and result["config"].get("config_version") != status["config_schema"]):
        raise PB9ConfigError("runtime_identity_mismatch")
    source = {
        "repo_dir": status["pb9dir"], "config_schema": status["config_schema"],
        "package_version": status["package_version"], "commit": status["commit"],
        "commit_available": status["commit_available"],
        "schema_module_file": result["schema_module_file"],
        "version_module_file": result["version_module_file"],
    }
    if operation in {"validate", "validate_live", "validate_backtest"}:
        source["load_module_file"] = result["load_module_file"]
        return {"valid": True, "source": source}
    if operation == "migrate_v8":
        migration = result.get('migration')
        if not isinstance(migration, dict) or migration.get('status') not in {'ready', 'blocked'}:
            raise PB9ConfigError('invalid_response')
        if migration['status'] == 'ready':
            check_inline_config(migration.get('config'), status['config_schema'], allow_inline_overrides=True)
            if not isinstance(migration.get('report'), dict):
                raise PB9ConfigError('invalid_response')
        elif not isinstance(migration.get('message'), str):
            raise PB9ConfigError('invalid_response')
        source['converter_module_file'] = result['converter_module_file']
        return {'migration': migration, 'source': source}
    if operation == "metadata":
        metadata = result.get("metadata")
        if (not isinstance(metadata, dict) or metadata.get("contract_version") != 1
                or not isinstance(metadata.get("template"), dict)
                or metadata["template"].get("config_version") != status["config_schema"]
                or not isinstance(metadata.get("prototype"), dict)
                or metadata["prototype"].get("config_version") != status["config_schema"]
                or not isinstance(metadata.get("fields"), list)
                or len(metadata["fields"]) > 10000):
            raise PB9ConfigError("invalid_response")
        kinds = metadata.get("strategies")
        if (not isinstance(kinds, list) or not kinds
                or any(not isinstance(kind, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", kind) for kind in kinds)
                or len(kinds) != len(set(kinds))
                or not isinstance(metadata.get("strategy_specs"), dict)
                or set(metadata["strategy_specs"]) != set(kinds)
                or not isinstance(metadata.get("strategy_defaults"), dict)):
            raise PB9ConfigError("invalid_response")
        for field in metadata["fields"]:
            if (not isinstance(field, dict) or not isinstance(field.get("path"), list) or not field["path"]
                    or any(not isinstance(part, str) or not part for part in field["path"])
                    or not isinstance(field.get("types"), list) or not field["types"]
                    or any(not isinstance(kind, str) or kind not in {"boolean", "integer", "float", "string", "array", "object"} for kind in field["types"])
                    or type(field.get("nullable")) is not bool):
                raise PB9ConfigError("invalid_response")
        rust_file = result.get("rust_module_file")
        environment = Path(status["pb9venv"]).absolute().parent.parent.resolve()
        if not isinstance(rust_file, str) or not Path(rust_file).is_absolute() or not Path(rust_file).resolve().is_relative_to(environment):
            raise PB9ConfigError("runtime_identity_mismatch")
        source.update({key: result[key] for key in ("registry_module_file", "strategy_module_file", "rust_module_file")})
        source["rust_source_agreement"] = "not_checked"
        _require_json_value(metadata)
        return {key: metadata[key] for key in ("contract_version", "template", "prototype", "fields", "strategies", "strategy_specs", "strategy_defaults")} | {"source": source}
    return {"config": result["config"], "source": source}


def load_pb9_template(pb9dir: str | Path | None, pb9venv: str | Path | None,
                      other_slots: dict | None = None, *, timeout: float = 60,
                      cancel_event: threading.Event | PB9CancelSignal | None = None) -> dict:
    """Load one native authored template, without preparation, validation or saving.

    No INI, PB8 client, persistent process or cache is used. The synchronous call
    owns its Linux process group and pipes until cleanup. A future API binding must
    offload this call and register in-flight ownership with shutdown handling.
    Commit metadata describes the inspected declarations, not full-tree cleanliness.
    """
    return _config_operation("template", pb9dir, pb9venv, other_slots, timeout=timeout, cancel_event=cancel_event)


def get_pb9_editor_metadata(pb9dir, pb9venv, other_slots=None, *, timeout=60, cancel_event=None) -> dict:
    """Read the selected native field registry, strategy contracts and defaults."""
    return _config_operation("metadata", pb9dir, pb9venv, other_slots, timeout=timeout, cancel_event=cancel_event)


def validate_pb9_live_config(config, pb9dir, pb9venv, other_slots=None, *, timeout=60, cancel_event=None) -> dict:
    """Validate native live source with inline overrides, never compile or launch it."""
    return _config_operation("validate_live", pb9dir, pb9venv, other_slots, timeout=timeout,
                             config=config, cancel_event=cancel_event)


def validate_pb9_backtest_config(config, pb9dir, pb9venv, other_slots=None, *, timeout=60, cancel_event=None):
    """Check native backtest source with inline overrides without changing its sparse form."""
    return _config_operation("validate_backtest", pb9dir, pb9venv, other_slots, timeout=timeout,
                             config=config, cancel_event=cancel_event)


def validate_pb9_config(config: dict, pb9dir: str | Path | None, pb9venv: str | Path | None,
                        other_slots: dict | None = None, *, timeout: float = 60,
                        cancel_event: threading.Event | PB9CancelSignal | None = None) -> dict:
    """Validate supported inline native source, retaining the caller's original dict.

    GUI/internal metadata, file-backed coins/overrides and symbolmap-dependent
    coin_sources are intentionally unsupported. Native preparation is used only
    to check acceptance; its expanded result is discarded. No file load/save,
    version migration or runtime compilation is requested.
    """
    return _config_operation("validate", pb9dir, pb9venv, other_slots, timeout=timeout, config=config,
                             cancel_event=cancel_event)


def validate_pb9_document(document: dict, pb9dir: str | Path | None, pb9venv: str | Path | None,
                          other_slots: dict | None = None, *, timeout: float = 60,
                          cancel_event: threading.Event | PB9CancelSignal | None = None,
                          validation_target: str = "config") -> dict:
    """Validate an authored snapshot, keeping root GUI metadata out of native code.

    Return separate ``config``, ``pbgui`` and inspected ``source`` fields. Missing
    root ``pbgui`` remains None; a present value must be a strict JSON object.
    The strict JSON snapshot is independent of caller-owned nested objects, and
    native preparation never replaces it with hydrated defaults. This is an
    in-memory boundary only: no loading, saving, migration or metadata rewriting.
    Source identity is reported separately, never trusted from GUI metadata.
    The document limit counts ASCII-escaped JSON bytes. The separate helper
    request cap also includes runtime identity, so near-limit native payloads can
    still fail with ``invalid_request`` after runtime inspection, before launch.
    """
    try:
        if not isinstance(document, dict):
            raise PB9ConfigError("invalid_document")
        _require_json_value(document)
        encoded = json.dumps(document, allow_nan=False).encode("utf-8")
        if len(encoded) > MAX_DOCUMENT_BYTES:
            raise PB9ConfigError("invalid_document")
        snapshot = json.loads(encoded)
        if "pbgui" in snapshot and not isinstance(snapshot["pbgui"], dict):
            raise PB9ConfigError("invalid_document")
    except (ValueError, TypeError, RuntimeError) as exc:
        error = exc if isinstance(exc, PB9ConfigError) else PB9ConfigError("invalid_document")
        _log(SERVICE, "PB9 config document rejected", level="ERROR",
             meta={"operation": "document", "error_codes": list(error.error_codes)})
        if error is exc:
            raise
        raise error from exc
    metadata = snapshot.pop("pbgui", None)
    options = {"cancel_event": cancel_event} if cancel_event is not None else {}
    if validation_target not in {"config", "backtest"}:
        raise PB9ConfigError("invalid_document")
    validator = validate_pb9_backtest_config if validation_target == "backtest" else validate_pb9_config
    validation = validator(snapshot, pb9dir, pb9venv, other_slots, timeout=timeout, **options)
    return {"config": snapshot, "pbgui": metadata, "source": validation["source"]}


def _config_operation(operation: str, pb9dir, pb9venv, other_slots, *, timeout: float, config=None,
                      cancel_event: threading.Event | PB9CancelSignal | None = None) -> dict:
    """Apply shared preflight, isolation and sanitized failure mapping."""
    try:
        if cancel_event is not None:
            if not isinstance(cancel_event, (threading.Event, PB9CancelSignal)):
                raise PB9ConfigError("invalid_request")
            if cancel_event.is_set():
                raise PB9ConfigError("helper_cancelled")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 120 or not math.isfinite(timeout):
            raise PB9ConfigError("invalid_timeout")
        if not _SUPPORTED_PLATFORM:
            raise PB9ConfigError("unsupported_platform")
        validating = operation in {"validate", "validate_live", "validate_backtest"}
        if operation == "migrate_v8":
            check_migration_input(config)
        if validating:
            check_inline_config(config, allow_inline_overrides=operation in {"validate_live", "validate_backtest"})
        status = (inspect_pb9_runtime(pb9dir, pb9venv, other_slots, cancel_event=cancel_event)
                  if cancel_event is not None else inspect_pb9_runtime(pb9dir, pb9venv, other_slots))
        if cancel_event is not None and cancel_event.is_set():
            raise PB9ConfigError("helper_cancelled")
        if not status["detected"]:
            raise PB9ConfigError("runtime_not_detected")
        if operation == "migrate_v8":
            raw = _run_helper(status, timeout, operation=operation, config=config, cancel_event=cancel_event)
        elif validating:
            check_inline_config(config, status["config_schema"], allow_inline_overrides=operation in {"validate_live", "validate_backtest"})
            options = {"cancel_event": cancel_event} if cancel_event is not None else {}
            raw = _run_helper(status, timeout, operation=operation, config=config, **options)
        elif operation == "metadata":
            options = {"cancel_event": cancel_event} if cancel_event is not None else {}
            raw = _run_helper(status, timeout, operation=operation, **options)
        elif cancel_event is not None:
            raw = _run_helper(status, timeout, cancel_event=cancel_event)
        else:
            raw = _run_helper(status, timeout)
        return _checked_response(raw, status, operation=operation)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        error = (exc if isinstance(exc, PB9ConfigError) else
                 PB9ConfigError(str(exc) if isinstance(exc, _HelperError) and str(exc) in _HELPER_CODES else "helper_failed"))
        _log(SERVICE, "PB9 config request failed", level="ERROR",
             meta={"operation": operation, "error_codes": list(error.error_codes)})
        if error is exc:
            raise
        raise error from exc


def migrate_pb8_config_to_pb9(bundle, pb9dir, pb9venv, other_slots=None, *, timeout=90, cancel_event=None):
    """Preview an official PB8 migration using the existing isolated PB9 process owner."""
    return _config_operation('migrate_v8', pb9dir, pb9venv, other_slots, timeout=timeout,
                             config=bundle, cancel_event=cancel_event)
