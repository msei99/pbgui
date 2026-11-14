"""Isolated one-request client for native PB9 templates and inline validation."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time

from logging_helpers import human_log as _log
from pb9_runtime import inspect_pb9_runtime
from pb9_config_helper import MAX_VALIDATION_BYTES, _HelperError, check_inline_config


SERVICE = "PB9Config"
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_HELPER_PATH = Path(__file__).with_name("pb9_config_helper.py")
_SUPPORTED_PLATFORM = sys.platform.startswith("linux") and hasattr(os, "waitid")
_ENV_KEYS = {"PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "SYSTEMROOT", "WINDIR",
             "TMPDIR", "TMP", "TEMP", "TZ"}
_HELPER_CODES = {"invalid_request", "runtime_identity_mismatch", "native_template_failed",
                 "helper_output_limit", "invalid_template", "invalid_config", "config_version_mismatch",
                 "external_reference_rejected", "native_config_rejected", "native_validation_failed"}


class PB9ConfigError(RuntimeError):
    """Report fixed config failure codes without exposing subprocess data."""

    def __init__(self, code: str):
        """Keep codes available for later authenticated API error mapping."""
        self.error_codes = (code,)
        super().__init__("PB9 config request failed: " + code)


def _run_helper(status: dict, timeout: float, *, operation: str = "template", config: dict | None = None) -> bytes:
    """Own one process group and nonblocking pipes under a single deadline.

    Requires normal SIGCHLD handling: no SIG_IGN or external child reaper.
    Cleanup covers descendants remaining in this group, not detached sessions.
    """
    payload = {
        "operation": operation, "pb9_dir": status["pb9dir"],
        "config_schema": status["config_schema"], "package_version": status["package_version"],
    }
    if operation == "validate":
        payload["config"] = config
    request = json.dumps(payload, allow_nan=False).encode("utf-8")
    if len(request) > (MAX_VALIDATION_BYTES if operation == "validate" else 16384):
        raise PB9ConfigError("invalid_request")
    env = {key: value for key, value in os.environ.items() if key.upper() in _ENV_KEYS}
    env["PATH"] = os.pathsep.join((str(Path(status["pb9venv"]).parent), os.defpath))
    output = bytearray()
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
    if operation == "validate" and result.get("valid") is not True:
        raise PB9ConfigError("invalid_response")
    src = (Path(status["pb9dir"]) / "src").resolve()
    module_keys = ("schema_module_file", "version_module_file") + (("load_module_file",) if operation == "validate" else ())
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
    if operation == "validate":
        source["load_module_file"] = result["load_module_file"]
        return {"valid": True, "source": source}
    return {"config": result["config"], "source": source}


def load_pb9_template(pb9dir: str | Path | None, pb9venv: str | Path | None,
                      other_slots: dict | None = None, *, timeout: float = 60) -> dict:
    """Load one native authored template, without preparation, validation or saving.

    No INI, PB8 client, persistent process or cache is used. The synchronous call
    owns its Linux process group and pipes until cleanup. A future API binding must
    offload this call and register in-flight ownership with shutdown handling.
    Commit metadata describes the inspected declarations, not full-tree cleanliness.
    """
    return _config_operation("template", pb9dir, pb9venv, other_slots, timeout=timeout)


def validate_pb9_config(config: dict, pb9dir: str | Path | None, pb9venv: str | Path | None,
                        other_slots: dict | None = None, *, timeout: float = 60) -> dict:
    """Validate supported inline native source, retaining the caller's original dict.

    GUI/internal metadata, file-backed coins/overrides and symbolmap-dependent
    coin_sources are intentionally unsupported. Native preparation is used only
    to check acceptance; its expanded result is discarded. No file load/save,
    version migration or runtime compilation is requested.
    """
    return _config_operation("validate", pb9dir, pb9venv, other_slots, timeout=timeout, config=config)


def _config_operation(operation: str, pb9dir, pb9venv, other_slots, *, timeout: float, config=None) -> dict:
    """Apply shared preflight, isolation and sanitized failure mapping."""
    try:
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 120 or not math.isfinite(timeout):
            raise PB9ConfigError("invalid_timeout")
        if not _SUPPORTED_PLATFORM:
            raise PB9ConfigError("unsupported_platform")
        if operation == "validate":
            check_inline_config(config)
        status = inspect_pb9_runtime(pb9dir, pb9venv, other_slots)
        if not status["detected"]:
            raise PB9ConfigError("runtime_not_detected")
        if operation == "validate":
            check_inline_config(config, status["config_schema"])
            raw = _run_helper(status, timeout, operation=operation, config=config)
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
