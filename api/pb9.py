"""Authenticated, read-only PB9 inspection with explicitly owned request workers."""

import asyncio
import configparser
from concurrent.futures import Future
from dataclasses import dataclass
import json
import math
import re
import signal
import threading
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.routing import APIRoute

from api.auth import SessionToken, require_auth
from logging_helpers import human_log as _log
from pb9_config import (MAX_DOCUMENT_BYTES, PB9CancelSignal, PB9ConfigError,
                        load_pb9_template, validate_pb9_document)
from pb9_runtime import PB9RuntimePathError, inspect_pb9_runtime, save_pb9_runtime_paths
from pbgui_purefunc import PBGDIR, load_ini_snapshot
from pb7_config import load_pb7_config, save_pb7_config
from pb9_config_library import config_library, config_name, list_configs, require_config
from pb9_config_storage import inspect_pb9_config_revision, reconcile_pb9_config_file


SERVICE = "PB9Config"
MAX_ACTIVE_REQUESTS = 2
MAX_ACTIVE_RUN_REQUESTS = 8
_NATIVE_OPERATIONS = frozenset({
    "bt9_migrate_preview", "bt9_migrate_accept", "bt9_metadata", "bt9_new_config", "bt9_load", "bt9_prepare", "bt9_draft_create", "bt9_save", "bt9_enqueue", "bt9_archive_requeue", "bt9_archive_retest", "bt9_archive_schedule_run", "bt9_start",
    "template", "validate", "config_load", "config_save", "run_new_config", "run_metadata",
    "run_prepare", "run_draft_create", "run_save", "run_forced_mode", "run_copy", "run_restore",
})
MAX_VALIDATION_BODY_BYTES = MAX_DOCUMENT_BYTES
MAX_JSON_DEPTH = 64
_VALIDATION_ERRORS = frozenset({"invalid_request", "invalid_document", "invalid_config",
                                "config_version_mismatch", "external_reference_rejected",
                                "native_config_rejected", "invalid_config_name", "invalid_revision", "runtime_paths_invalid"})
_PATH_KEYS = ("pb9dir", "pb9venv", "pb7dir", "pb7venv", "pb8dir", "pb8venv")


def _error(status: int, code: str, detail: str, *, busy: bool = False) -> HTTPException:
    """Build a fixed, uncached API error without native diagnostics or paths."""
    headers = {"Cache-Control": "no-store", "X-PBGUI-Error-Codes": code}
    if busy:
        headers["Retry-After"] = "1"
    return HTTPException(status_code=status, detail=detail, headers=headers)


class _NoStoreRoute(APIRoute):
    """Keep authenticated responses and dependency errors out of browser caches."""

    def get_route_handler(self):
        """Wrap the shared authentication handler without changing its statuses."""
        original = super().get_route_handler()

        async def uncached(request):
            """Apply no-store to success and expected HTTP errors alike."""
            try:
                response = await original(request)
            except HTTPException as exc:
                exc.headers = {**(exc.headers or {}), "Cache-Control": "no-store"}
                raise
            response.headers["Cache-Control"] = "no-store"
            return response

        return uncached


router = APIRouter(route_class=_NoStoreRoute)


def _read_paths() -> dict:
    """Read exactly six non-secret keys from one locked settings snapshot."""
    try:
        snapshot = load_ini_snapshot()
        return {key: snapshot.get("main", key) if snapshot.has_option("main", key) else ""
                for key in _PATH_KEYS}
    except (OSError, configparser.Error, ValueError, RuntimeError):
        pass
    raise PB9ConfigError("runtime_settings_unavailable") from None


def _request_envelope(payload: bytes, keys: set[str]) -> dict:
    """Decode one explicit config envelope without coercion or duplicate members."""
    def unique_object(pairs):
        """Reject duplicate fields at every depth rather than choosing a value."""
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate member")
            result[key] = value
        return result

    def reject_constant(value):
        """Exclude nonstandard NaN/Infinity literals."""
        raise ValueError("Nonfinite constant")

    def finite_float(value):
        """Exponent overflow is invalid even when JSON syntax itself is valid."""
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("Nonfinite number")
        return number

    try:
        envelope = json.loads(payload.decode("utf-8"), object_pairs_hook=unique_object,
                              parse_constant=reject_constant, parse_float=finite_float)
        if isinstance(envelope, dict) and set(envelope) == keys:
            pending = [(envelope, 0)]
            while pending:
                value, depth = pending.pop()
                if depth > MAX_JSON_DEPTH:
                    raise ValueError("JSON nesting limit exceeded")
                if isinstance(value, dict):
                    pending.extend((item, depth + 1) for item in value.values())
                elif isinstance(value, list):
                    pending.extend((item, depth + 1) for item in value)
            # Reject lone surrogate escapes before a successful JSONResponse
            # would attempt strict UTF-8 encoding on the event loop.
            json.dumps(envelope, ensure_ascii=False, allow_nan=False).encode("utf-8")
            return envelope
    except (ValueError, RecursionError):
        pass
    # JSONDecodeError retains the submitted document: keep it out of error chains.
    raise PB9ConfigError("invalid_document") from None


def _validation_document(payload: bytes) -> dict:
    """Require an object document inside the explicit validation envelope."""
    envelope = _request_envelope(payload, {"config"})
    if not isinstance(envelope["config"], dict):
        raise PB9ConfigError("invalid_document")
    return envelope["config"]


def _operation(operation: str, cancel_event: PB9CancelSignal, payload: bytes | None = None) -> dict:
    """Inspect, load or validate using only settings selected on the server."""
    if cancel_event.is_set():
        raise PB9ConfigError("helper_cancelled")
    if operation.startswith("bt9_"):
        from api.backtest_v9 import _operation as backtest_operation
        return backtest_operation(operation, cancel_event, payload)
    if operation.startswith("run_"):
        from api.v9_instances import _operation as run_operation
        return run_operation(operation, cancel_event, payload)
    if operation.startswith("market_"):
        from api.v9_market import _operation as market_operation
        return market_operation(operation[7:], cancel_event, payload)
    document = _validation_document(payload) if operation == "validate" else None
    if operation == "runtime_paths":
        values = _request_envelope(payload, {"pb9dir", "pb9venv"})
        if any(not isinstance(value, str) for value in values.values()):
            raise PB9ConfigError("runtime_paths_invalid")
        try:
            return save_pb9_runtime_paths(values["pb9dir"], values["pb9venv"], cancel_event=cancel_event)
        except PB9RuntimePathError:
            raise PB9ConfigError("runtime_paths_invalid") from None
        except (ValueError, configparser.Error):
            raise PB9ConfigError("runtime_paths_invalid") from None
        except RuntimeError:
            if cancel_event.is_set():
                raise PB9ConfigError("helper_cancelled") from None
            raise
    envelope = None
    if operation in {"config_load", "config_revision"}:
        envelope = _request_envelope(payload, {"name"})
    elif operation == "config_reconcile":
        envelope = _request_envelope(payload, {"name", "config"})
        if not isinstance(envelope["config"], dict):
            raise PB9ConfigError("invalid_document")
    elif operation == "config_save":
        envelope = _request_envelope(payload, {"name", "config", "revision"})
        if not isinstance(envelope["config"], dict):
            raise PB9ConfigError("invalid_document")
    if envelope is not None:
        envelope["name"] = config_name(envelope["name"])
        if operation == "config_save" and envelope["revision"] is not None:
            if not isinstance(envelope["revision"], str) or not re.fullmatch(r"[0-9a-f]{64}", envelope["revision"]):
                raise PB9ConfigError("invalid_revision")
    paths = _read_paths()
    if cancel_event.is_set():
        raise PB9ConfigError("helper_cancelled")
    other_slots = {key: paths[key] for key in _PATH_KEYS[2:]}
    if operation in {"config_list", "config_load", "config_revision", "config_reconcile", "config_save"}:
        try:
            create = operation == "config_save" and envelope["revision"] is None
            with config_library(PBGDIR, paths, cancel_event, create=create) as context:
                if operation == "config_list":
                    return {"configs": list_configs(context)}
                if operation == "config_revision":
                    return inspect_pb9_config_revision(envelope["name"], context)
                if operation == "config_reconcile":
                    return reconcile_pb9_config_file(envelope["name"], envelope["config"], context)
                if operation == "config_load":
                    name = require_config(context, envelope["name"])
                    loaded = load_pb7_config(name, pb9_context=context, pb9_with_revision=True)
                    loaded["config_text"] = json.dumps(loaded["config"], ensure_ascii=False, indent=4, allow_nan=False) + "\n"
                    return loaded
                save_pb7_config(envelope["config"], envelope["name"], pb9_context=context,
                                pb9_expected_revision=envelope["revision"])
                return {"saved": True, "name": envelope["name"]}
        except PB9ConfigError as exc:
            if operation == "config_list" and exc.error_codes == ("config_file_not_found",):
                return {"configs": []}
            if operation == "config_save" and exc.error_codes == ("config_file_not_found",):
                raise PB9ConfigError("config_revision_conflict") from None
            raise
    if operation == "runtime":
        result = inspect_pb9_runtime(paths["pb9dir"], paths["pb9venv"], other_slots, cancel_event=cancel_event)
        if cancel_event.is_set():
            raise PB9ConfigError("helper_cancelled")
        return result
    if operation == "template":
        result = load_pb9_template(paths["pb9dir"], paths["pb9venv"], other_slots, cancel_event=cancel_event)
        result["config_text"] = json.dumps(result["config"], ensure_ascii=False, indent=4, allow_nan=False) + "\n"
        return result
    if operation == "validate":
        result = validate_pb9_document(document, paths["pb9dir"], paths["pb9venv"], other_slots,
                                       cancel_event=cancel_event)
        return {"valid": True, **result}
    raise PB9ConfigError("invalid_request")


@dataclass
class _Job:
    """Own a worker thread and a completion signal independent of asyncio cancellation."""

    future: Future
    cancel_event: PB9CancelSignal
    thread: threading.Thread | None = None
    operation: str = ""


def _worker(operation: str, future: Future, cancel_event: PB9CancelSignal, payload: bytes | None = None) -> None:
    """Publish completion only after synchronous/native work has cleaned up."""
    try:
        result = (_operation(operation, cancel_event, payload) if payload is not None
                  else _operation(operation, cancel_event))
    except HTTPException as exc:
        if operation.startswith(("run_", "market_", "bt9_")):
            future.set_exception(exc)
        else:
            _log(SERVICE, "PB9 API operation failed", level="ERROR",
                 meta={"operation": operation, "error_codes": ["api_operation_failed"]})
            future.set_exception(PB9ConfigError("api_operation_failed"))
    except BaseException as exc:
        code = exc.error_codes[0] if isinstance(exc, PB9ConfigError) else "api_operation_failed"
        expected = _VALIDATION_ERRORS | {"config_file_not_found", "config_file_exists", "config_revision_conflict"}
        level = "WARNING" if operation in {"validate", "config_save", "config_load", "config_revision", "config_reconcile", "runtime_paths"} and code in expected else "ERROR"
        _log(SERVICE, "PB9 API operation failed", level=level,
             meta={"operation": operation, "error_codes": [code]})
        future.set_exception(PB9ConfigError(code))
    else:
        future.set_result(result)


async def _drain(job: _Job) -> bool:
    """Wait through caller cancellation, then join only an already completed worker."""
    wrapped = asyncio.wrap_future(job.future)

    def consume(done):
        """Retrieve exceptions even when an abandoned HTTP request has no consumer."""
        if not done.cancelled():
            done.exception()

    wrapped.add_done_callback(consume)
    cancelled = False
    while not job.future.done():
        try:
            await asyncio.shield(wrapped)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    # Worker completion is published after all subprocess/file work has ended.
    job.thread.join()
    return cancelled


class PB9RequestOwner:
    """Bound per-app workers and retain ownership until cancellation/shutdown drains them."""

    def __init__(self, restart_pending=lambda: False):
        """Create no threads or filesystem work during startup."""
        self._restart_pending = restart_pending
        self._lock = threading.RLock()
        self._jobs: dict[int, _Job] = {}
        self._accepting = True
        self.cancel_signal = PB9CancelSignal()
        self._signal_handlers = {}

    @property
    def accepting(self) -> bool:
        """Report admission state without touching runtime files."""
        with self._lock:
            return self._accepting and not self.cancel_signal.is_set()

    @property
    def active(self) -> int:
        """Count registered workers, including requests cancelled by their callers."""
        with self._lock:
            return len(self._jobs)

    async def run(self, operation: str, payload: bytes | None = None) -> dict:
        """Bound UI reads separately while retaining at most two native operations."""
        start_failure = None
        with self._lock:
            if not self._accepting or self.cancel_signal.is_set():
                raise _error(503, "pb9_stopping", "PB9 inspection is stopping")
            if self._restart_pending():
                raise _error(409, "api_restart_pending", "API restart is pending")
            run_request = operation.startswith(("run_", "market_", "bt9_"))
            run_count = sum(job.operation.startswith(("run_", "market_", "bt9_")) for job in self._jobs.values())
            native_count = sum(job.operation in _NATIVE_OPERATIONS for job in self._jobs.values())
            group_full = (run_count >= MAX_ACTIVE_RUN_REQUESTS if run_request
                          else len(self._jobs) - run_count >= MAX_ACTIVE_REQUESTS)
            if group_full or operation in _NATIVE_OPERATIONS and native_count >= MAX_ACTIVE_REQUESTS:
                raise _error(503, "pb9_busy", "PB9 inspection capacity is busy", busy=True)
            job = _Job(Future(), self.cancel_signal, operation=operation)
            arguments = (operation, job.future, job.cancel_event)
            if payload is not None:
                arguments += (payload,)
            job.thread = threading.Thread(target=_worker, args=arguments,
                                          name=f"pb9-api-{operation}")
            self._jobs[id(job)] = job
            try:
                job.thread.start()
            except BaseException as exc:
                start_failure = exc
                if job.thread.ident is None:
                    self._jobs.pop(id(job), None)
                    job.future.cancel()
        if start_failure is not None:
            _log(SERVICE, "PB9 API worker could not start", level="ERROR",
                 meta={"error_codes": ["api_worker_start_failed"]})
            if job.thread.ident is None:
                if not isinstance(start_failure, Exception):
                    raise start_failure
                raise _error(503, "api_worker_start_failed", "PB9 inspection worker is unavailable") from None
        try:
            if await _drain(job):
                raise asyncio.CancelledError
            if start_failure is not None:
                if not isinstance(start_failure, Exception):
                    raise start_failure
                raise _error(503, "api_worker_start_failed", "PB9 inspection worker is unavailable")
            return job.future.result()
        finally:
            with self._lock:
                self._jobs.pop(id(job), None)

    def request_stop(self) -> None:
        """Close admission and interrupt native helpers before server request draining."""
        self.cancel_signal.set()
        with self._lock:
            self._accepting = False

    async def shutdown(self) -> None:
        """Cancel native work and join all workers, idempotently through caller cancellation."""
        self.request_stop()
        with self._lock:
            jobs = tuple(self._jobs.values())
        cancelled = False
        for job in jobs:
            cancelled = await _drain(job) or cancelled
            with self._lock:
                self._jobs.pop(id(job), None)
        if cancelled:
            raise asyncio.CancelledError


def _install_stop_signals(owner: PB9RequestOwner) -> None:
    """Chain the ASGI server's handlers so native cancellation precedes request draining.

    Uvicorn installs callable handlers before lifespan startup. A default/ignored
    handler is not replaced. The handler only sets a poll flag, without locks
    or event-loop callbacks, before forwarding the signal. The official server
    opts in; even a temporarily blocked ASGI loop cannot delay helper abortion.
    """
    if threading.current_thread() is not threading.main_thread():
        return
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous = signal.getsignal(signum)
        if not callable(previous):
            continue

        def stop(received, frame, prior=previous):
            """Request cancellation before preserving the server's normal signal handling."""
            owner.cancel_signal.set()
            prior(received, frame)

        owner._signal_handlers[signum] = (previous, stop)
        signal.signal(signum, stop)


def _restore_stop_signals(owner: PB9RequestOwner) -> None:
    """Restore only handlers still owned by this lifespan."""
    for signum, (previous, installed) in owner._signal_handlers.items():
        if signal.getsignal(signum) is installed:
            signal.signal(signum, previous)
    owner._signal_handlers.clear()


def startup(app, *, restart_pending=lambda: False, install_signal_hooks: bool = False) -> None:
    """Attach one owner per app/lifespan without resetting live requests."""
    previous = getattr(app.state, "pb9_requests", None)
    if previous is not None:
        if previous.accepting:
            return
        if previous.active or previous._signal_handlers:
            raise RuntimeError("Cannot replace an active PB9 request owner")
    app.state.pb9_requests = PB9RequestOwner(restart_pending)
    if install_signal_hooks:
        try:
            _install_stop_signals(app.state.pb9_requests)
        except BaseException:
            _restore_stop_signals(app.state.pb9_requests)
            app.state.pb9_requests.request_stop()
            raise


async def shutdown(app) -> None:
    """Drain this app's owner, including partial startup and repeated shutdown."""
    owner = getattr(app.state, "pb9_requests", None)
    if owner is not None:
        try:
            await owner.shutdown()
        finally:
            _restore_stop_signals(owner)
            from api.v9_instances import clear_drafts
            clear_drafts()


def restart_block_reason(app) -> str:
    """Keep coordinator restarts from abandoning owned native helper groups."""
    owner = getattr(app.state, "pb9_requests", None)
    if owner is not None and owner.active:
        return "PB9 inspection is in progress; wait for completion (native helper timeout: 60 seconds)"
    return ""


async def _execute(request: Request, operation: str, payload: bytes | None = None) -> JSONResponse:
    """Map only sanitized operation failures while preserving expected HTTP statuses."""
    owner = getattr(request.app.state, "pb9_requests", None)
    if owner is None:
        raise _error(503, "pb9_unavailable", "PB9 inspection is not initialized")
    try:
        result = await owner.run(operation, payload)
        return JSONResponse(result)
    except HTTPException:
        raise
    except PB9ConfigError as exc:
        if operation in {"validate", "config_save", "config_load", "config_revision", "config_reconcile", "runtime_paths"} and exc.error_codes[0] in _VALIDATION_ERRORS:
            raise _error(422, exc.error_codes[0], "PB9 config was rejected") from None
        if exc.error_codes[0] in {"config_file_exists", "config_revision_conflict"}:
            raise _error(409, exc.error_codes[0], "PB9 config changed; load its current revision") from None
        if exc.error_codes[0] == "config_file_not_found":
            raise _error(404, exc.error_codes[0], "PB9 config was not found") from None
        raise _error(503, exc.error_codes[0], "PB9 request failed",
                     busy=exc.error_codes[0] == "config_storage_busy") from None
    except Exception:
        _log(SERVICE, "PB9 API response failed", level="ERROR",
             meta={"operation": operation, "error_codes": ["api_response_failed"]})
        raise _error(503, "api_response_failed", "PB9 response could not be produced") from None


def _no_query(request: Request) -> None:
    """Refuse parameter overrides rather than interpreting any request paths or tokens."""
    if request.query_params:
        raise _error(422, "invalid_request", "PB9 inspection accepts no query parameters")


@router.get("/runtime")
async def runtime(request: Request, session: SessionToken = Depends(require_auth)):
    """Return current static evidence, deliberately not a runtime-readiness promise."""
    _no_query(request)
    return await _execute(request, "runtime")


@router.post("/template")
async def template(request: Request, session: SessionToken = Depends(require_auth)):
    """Load one authored template; request bodies cannot select paths or launch jobs."""
    _no_query(request)
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > 1024:
            raise _error(413, "invalid_request", "PB9 template request is too large")
        body.extend(chunk)
    if body.strip():
        try:
            empty = json.loads(body) == {}
        except (ValueError, UnicodeError, RecursionError):
            empty = False
        if not empty:
            raise _error(422, "invalid_request", "PB9 template accepts no parameters")
    return await _execute(request, "template")


@router.post("/validate")
async def validate(request: Request, session: SessionToken = Depends(require_auth)):
    """Validate an inline document without saving, migration or path overrides."""
    _no_query(request)
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_VALIDATION_BODY_BYTES:
            raise _error(413, "invalid_document", "PB9 validation request is too large")
        body.extend(chunk)
    return await _execute(request, "validate", bytes(body))


async def _file_payload(request: Request, operation: str, limit: int) -> JSONResponse:
    """Bound each explicit file request before decoding it in its owned worker."""
    _no_query(request)
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > limit:
            raise _error(413, "invalid_document", "PB9 config request is too large")
        body.extend(chunk)
    return await _execute(request, operation, bytes(body))


@router.get("/configs")
async def configs(request: Request, session: SessionToken = Depends(require_auth)):
    """List only the server-owned PB9 config library."""
    _no_query(request)
    return await _execute(request, "config_list")


@router.post("/configs/load")
async def config_load(request: Request, session: SessionToken = Depends(require_auth)):
    """Load one JSON basename together with the exact-byte update revision."""
    return await _file_payload(request, "config_load", 4096)


@router.post("/configs/save")
async def config_save(request: Request, session: SessionToken = Depends(require_auth)):
    """Create exclusively or update an exact revision; never migrate or overwrite implicitly."""
    return await _file_payload(request, "config_save", MAX_VALIDATION_BODY_BYTES)


@router.post("/configs/revision")
async def config_revision(request: Request, session: SessionToken = Depends(require_auth)):
    """Check exact-byte changes without launching native code on editor polling."""
    return await _file_payload(request, "config_revision", 4096)


@router.post("/configs/reconcile")
async def config_reconcile(request: Request, session: SessionToken = Depends(require_auth)):
    """Compare exact authored values after an acknowledged or uncertain Save."""
    return await _file_payload(request, "config_reconcile", MAX_VALIDATION_BODY_BYTES)


@router.post("/runtime/paths")
async def runtime_paths(request: Request, session: SessionToken = Depends(require_auth)):
    """Explicitly configure the separated PB9 slot through its atomic INI writer."""
    return await _file_payload(request, "runtime_paths", 4096)


@router.get("/main_page", response_class=HTMLResponse)
def editor_page(request: Request, session: SessionToken = Depends(require_auth)):
    """Render the productive PB9 editor through the trusted application mount."""
    from api.page_templates import render_page_urls

    frontend = Path(__file__).resolve().parent.parent / "frontend"
    html = (frontend / "pb9_editor.html").read_text(encoding="utf-8")
    html = render_page_urls(request, html, "/api/pb9")
    html = html.replace("%%NAV_HASH%%", str((frontend / "pbgui_nav.js").stat().st_mtime_ns))
    return HTMLResponse(html, headers={"Cache-Control": "no-store"})
