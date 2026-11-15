"""PB9 read-only API auth and ownership tests without production runtime access."""

import asyncio
import configparser
import copy
from contextlib import asynccontextmanager
import signal
import threading
from types import SimpleNamespace

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
import pytest

from api import auth
from api import pb9
from pb9_config import PB9ConfigError


@pytest.fixture
def endpoint(tmp_path, monkeypatch):
    """Serve only the new routes with in-memory settings and operation doubles."""
    parser = configparser.ConfigParser()
    values = {key: str(tmp_path / key) for key in pb9._PATH_KEYS}
    parser.read_dict({"main": {**values, "password": "fixture-private-ini-value"}})
    calls, snapshots = [], []
    gate = {"pending": False}

    def snapshot():
        """Count settings reads without opening a real INI file."""
        snapshots.append(threading.get_ident())
        return parser

    def record(operation, source, python, other_slots):
        """Assert synchronous inspection never executes on the ASGI event loop."""
        with pytest.raises(RuntimeError):
            asyncio.get_running_loop()
        calls.append((operation, source, python, other_slots))

    def inspect(source, python, other_slots, *, cancel_event=None):
        """Return static evidence without promising runtime readiness."""
        record("runtime", source, python, other_slots)
        return {"configured": True, "detected": True, "config_schema": "v9.0.0"}

    def template(source, python, other_slots, *, cancel_event=None):
        """Return a sparse template without executing upstream code."""
        record("template", source, python, other_slots)
        return {"config": {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}}},
                "source": {"repo_dir": source}}

    def validate(document, source, python, other_slots, *, cancel_event=None):
        """Model the authored-document boundary with a detached snapshot."""
        record("validate", source, python, other_slots)
        config = copy.deepcopy(document)
        return {"config": config, "pbgui": config.pop("pbgui", None), "source": {"repo_dir": source}}

    monkeypatch.setattr(pb9, "validate_pb9_document", validate)
    monkeypatch.setattr(pb9, "load_ini_snapshot", snapshot)
    monkeypatch.setattr(pb9, "inspect_pb9_runtime", inspect)
    monkeypatch.setattr(pb9, "load_pb9_template", template)
    monkeypatch.setattr(auth, "validate_token", lambda token: SimpleNamespace(user_id="fixture-user")
                        if token == "fixture-auth" else None)

    @asynccontextmanager
    async def lifespan(app):
        """Own exactly this tiny app's workers and always drain them on teardown."""
        pb9.startup(app, restart_pending=lambda: gate["pending"])
        try:
            yield
        finally:
            await pb9.shutdown(app)

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(auth.BrowserOriginMiddleware)
    app.include_router(pb9.router, prefix="/api/pb9")
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, app=app, values=values, parser=parser,
                              calls=calls, snapshots=snapshots, gate=gate)


@pytest.mark.parametrize("method,path", [("get", "/runtime"), ("post", "/template"), ("post", "/validate")])
def test_routes_require_shared_auth_and_never_accept_query_tokens(endpoint, method, path):
    """Missing, invalid or query credentials cannot start a worker."""
    request = getattr(endpoint.client, method)
    for suffix, headers in (("", {}), ("?token=fixture-auth", {}), ("", {"Authorization": "Bearer invalid"})):
        response = request("/api/pb9" + path + suffix, headers=headers)
        assert response.status_code == 401
        assert response.headers["cache-control"] == "no-store"
    assert not endpoint.calls
    assert not endpoint.snapshots


def test_each_request_uses_one_current_allowlisted_snapshot(endpoint):
    """Authored templates and static status use current server-selected slots only."""
    headers = {"Authorization": "Bearer fixture-auth"}
    response = endpoint.client.get("/api/pb9/runtime", headers=headers)
    assert response.status_code == 200
    assert response.json()["detected"] is True
    assert "ready" not in response.json()
    assert response.headers["cache-control"] == "no-store"
    response = endpoint.client.post("/api/pb9/template", json={}, headers=headers)
    assert response.status_code == 200
    assert response.json()["config"]["bot"]["long"] == {}
    assert response.headers["cache-control"] == "no-store"
    expected_other = {key: endpoint.values[key] for key in pb9._PATH_KEYS[2:]}
    assert endpoint.calls == [(operation, endpoint.values["pb9dir"], endpoint.values["pb9venv"], expected_other)
                              for operation in ("runtime", "template")]
    assert len(endpoint.snapshots) == 2
    endpoint.parser.set("main", "pb9dir", "new-source-selection")
    response = endpoint.client.get("/api/pb9/runtime", headers=headers)
    assert response.status_code == 200
    assert endpoint.calls[-1][1] == "new-source-selection"
    assert len(endpoint.snapshots) == 3
    assert "fixture-private-ini-value" not in response.text
    assert endpoint.app.state.pb9_requests.active == 0


@pytest.mark.parametrize("origin", [None, "https://other.example", "http://testserver"])
def test_cookie_template_requests_use_shared_origin_protection(endpoint, origin):
    """Ambient cookies need origin evidence; an explicit Bearer remains usable separately."""
    endpoint.client.cookies.set(auth.SESSION_COOKIE_NAME, "fixture-auth")
    headers = {"Origin": origin} if origin else {}
    response = endpoint.client.post("/api/pb9/template", json={}, headers=headers)
    assert response.status_code == (200 if origin == "http://testserver" else 403)
    assert response.headers["cache-control"] == "no-store"
    assert len(endpoint.calls) == (1 if response.status_code == 200 else 0)


@pytest.mark.parametrize("body,status", [(b'{"pb9dir":"/unapproved"}', 422), (b"[]", 422),
                                        (b"null", 422), (b"not json", 422), (b"x" * 1025, 413)])
def test_template_rejects_parameters_without_echoing_input(endpoint, body, status):
    """No runtime path, config or oversized body reaches the operation worker."""
    response = endpoint.client.post("/api/pb9/template", content=body,
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == status
    assert response.headers["cache-control"] == "no-store"
    assert "unapproved" not in response.text
    assert not endpoint.calls
    assert not endpoint.snapshots


def test_query_overrides_are_rejected(endpoint):
    """The request cannot substitute server-selected paths through query parameters."""
    response = endpoint.client.get("/api/pb9/runtime?pb9dir=/unapproved",
                                   headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 422
    assert not endpoint.calls


@pytest.mark.parametrize("failure,code", [("ini", "runtime_settings_unavailable"),
                                         ("native", "helper_failed"), ("unexpected", "api_operation_failed")])
def test_api_errors_are_fixed_uncached_and_do_not_leak_settings(endpoint, monkeypatch, failure, code):
    """Only safe operation codes cross the boundary, never original exception payloads."""
    events = []
    monkeypatch.setattr(pb9, "_log", lambda *args, **kwargs: events.append((args, kwargs)))
    if failure == "ini":
        def fail_snapshot():
            """Represent an invalid INI including private diagnostic data."""
            raise configparser.Error("fixture-private-ini-value /private/settings")

        monkeypatch.setattr(pb9, "load_ini_snapshot", fail_snapshot)
    else:
        def fail_template(*args, **kwargs):
            """Represent known native refusal or an unforeseen adapter failure."""
            if failure == "native":
                raise PB9ConfigError("helper_failed")
            raise ValueError("fixture-private-ini-value /private/settings")

        monkeypatch.setattr(pb9, "load_pb9_template", fail_template)
    response = endpoint.client.post("/api/pb9/template", json={},
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 503
    assert response.headers["x-pbgui-error-codes"] == code
    assert response.headers["cache-control"] == "no-store"
    assert "fixture-private-ini-value" not in response.text + repr(events)
    assert "/private/settings" not in response.text + repr(events)
    assert endpoint.app.state.pb9_requests.active == 0


def test_unexpected_worker_http_details_are_not_exposed(endpoint, monkeypatch):
    """The native-domain worker cannot smuggle arbitrary HTTP detail or headers out."""
    def refuse(*args, **kwargs):
        """Represent a future shared restart guard without private diagnostics."""
        raise HTTPException(status_code=409, detail="fixture-private-http-detail",
                            headers={"X-Private": "fixture-private-header"})

    monkeypatch.setattr(pb9, "load_pb9_template", refuse)
    response = endpoint.client.post("/api/pb9/template", json={},
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 503
    assert response.headers["x-pbgui-error-codes"] == "api_operation_failed"
    assert "fixture-private" not in response.text
    assert "x-private" not in response.headers
    assert response.headers["cache-control"] == "no-store"


def test_known_restart_admission_http_status_is_preserved(endpoint):
    """Expected admission errors retain their 409 without entering a worker."""
    endpoint.gate["pending"] = True
    response = endpoint.client.post("/api/pb9/template", json={},
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 409
    assert response.headers["x-pbgui-error-codes"] == "api_restart_pending"
    assert response.headers["cache-control"] == "no-store"
    assert not endpoint.calls
    assert not endpoint.snapshots


async def _until(predicate):
    """Yield while a fixture worker starts, with a finite assertion deadline."""
    deadline = asyncio.get_running_loop().time() + 3
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "fixture worker did not reach expected state"
        await asyncio.sleep(0.01)


def test_owner_capacity_has_no_unbounded_waiting_queue(monkeypatch):
    """A third request fails immediately while exactly two real workers remain owned."""
    release = threading.Event()
    entered = []

    def hold(operation, cancel_event=None):
        """Keep fixture work live until the parent releases both workers."""
        entered.append(threading.get_ident())
        assert release.wait(5)
        return {"operation": operation}

    monkeypatch.setattr(pb9, "_operation", hold)

    async def exercise():
        owner = pb9.PB9RequestOwner()
        requests = [asyncio.create_task(owner.run("runtime")) for _ in range(2)]
        try:
            await _until(lambda: len(entered) == 2)
            with pytest.raises(HTTPException) as caught:
                await owner.run("runtime")
            assert caught.value.status_code == 503
            assert caught.value.headers["Retry-After"] == "1"
            assert owner.active == 2
        finally:
            release.set()
            await asyncio.gather(*requests, return_exceptions=True)
            await owner.shutdown()
        assert owner.active == 0

    asyncio.run(exercise())


@pytest.mark.parametrize("fail_worker", [False, True])
def test_cancelled_request_retains_ownership_until_shutdown_drains(monkeypatch, fail_worker):
    """Cancellation never relabels a running worker as completed or hides its failure."""
    release, entered = threading.Event(), threading.Event()

    def hold(operation, cancel_event=None):
        """Release work only after cancellation and shutdown have inspected ownership."""
        entered.set()
        assert release.wait(5)
        if fail_worker:
            raise ValueError("fixture-private-worker-error")
        return {}

    monkeypatch.setattr(pb9, "_operation", hold)

    async def exercise():
        owner = pb9.PB9RequestOwner()
        app = FastAPI()
        app.state.pb9_requests = owner
        unhandled = []
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: unhandled.append(context))
        request = asyncio.create_task(owner.run("template"))
        closing = None
        try:
            await _until(entered.is_set)
            request.cancel()
            await asyncio.sleep(0.02)
            assert not request.done()
            assert owner.active == 1
            assert "PB9" in pb9.restart_block_reason(app)
            closing = asyncio.create_task(pb9.shutdown(app))
            await _until(lambda: not owner.accepting)
            assert not closing.done()
            with pytest.raises(RuntimeError):
                pb9.startup(app)
            assert app.state.pb9_requests is owner
            with pytest.raises(HTTPException) as caught:
                await owner.run("runtime")
            assert caught.value.headers["X-PBGUI-Error-Codes"] == "pb9_stopping"
        finally:
            release.set()
            results = await asyncio.gather(request, *([closing] if closing else []), return_exceptions=True)
            await pb9.shutdown(app)
        assert isinstance(results[0], asyncio.CancelledError)
        assert owner.active == 0
        assert pb9.restart_block_reason(app) == ""
        await asyncio.sleep(0.01)
        assert not unhandled

    asyncio.run(exercise())


def test_cancelled_shutdown_finishes_before_rethrowing(monkeypatch):
    """Shutdown cancellation cannot leave a worker behind or reopen admission."""
    release, entered = threading.Event(), threading.Event()

    def hold(operation, cancel_event=None):
        """Model a still-running native helper while shutdown is cancelled."""
        entered.set()
        assert release.wait(5)
        return {}

    monkeypatch.setattr(pb9, "_operation", hold)

    async def exercise():
        owner = pb9.PB9RequestOwner()
        request = asyncio.create_task(owner.run("runtime"))
        await _until(entered.is_set)
        closing = asyncio.create_task(owner.shutdown())
        try:
            await _until(lambda: not owner.accepting)
            closing.cancel()
            await asyncio.sleep(0.02)
            assert not closing.done()
            assert owner.active == 1
        finally:
            release.set()
            results = await asyncio.gather(request, closing, return_exceptions=True)
            await owner.shutdown()
        assert results[0] == {}
        assert isinstance(results[1], asyncio.CancelledError)
        assert owner.active == 0

    asyncio.run(exercise())


def test_restart_gate_and_idempotent_lifespan_admission(monkeypatch):
    """A pending coordinator restart closes admission before a worker can start."""
    monkeypatch.setattr(pb9, "_operation", lambda operation, cancel_event=None: {})

    async def exercise():
        app = FastAPI()
        gate = {"pending": True}
        pb9.startup(app, restart_pending=lambda: gate["pending"])
        owner = app.state.pb9_requests
        pb9.startup(app)
        assert app.state.pb9_requests is owner
        with pytest.raises(HTTPException) as caught:
            await owner.run("runtime")
        assert caught.value.status_code == 409
        assert owner.active == 0
        gate["pending"] = False
        assert await owner.run("runtime") == {}
        await pb9.shutdown(app)
        await pb9.shutdown(app)
        pb9.startup(app)
        assert app.state.pb9_requests is not owner
        await pb9.shutdown(app)

    asyncio.run(exercise())


@pytest.mark.parametrize("started", [False, True])
@pytest.mark.parametrize("abort", [False, True])
def test_thread_start_failures_leave_no_unowned_work(monkeypatch, started, abort):
    """A failure before or after actual start either removes or drains that worker."""
    release, entered = threading.Event(), threading.Event()
    original_start = threading.Thread.start

    class StartupAbort(BaseException):
        """Represent a non-Exception interruption during worker startup."""

    def hold(operation, cancel_event=None):
        """Allow the test to inspect ownership after an uncertain start outcome."""
        entered.set()
        assert release.wait(5)
        return {}

    def fail_start(thread):
        """Represent no start or an error reported after the real worker started."""
        if started:
            original_start(thread)
        raise (StartupAbort if abort else RuntimeError)("fixture-private-thread-start-error")

    monkeypatch.setattr(pb9, "_operation", hold)
    monkeypatch.setattr(threading.Thread, "start", fail_start)

    async def exercise():
        owner = pb9.PB9RequestOwner()
        request = asyncio.create_task(owner.run("runtime"))
        try:
            if started:
                await _until(entered.is_set)
                assert owner.active == 1
                assert not request.done()
        finally:
            release.set()
            with pytest.raises(StartupAbort if abort else HTTPException) as caught:
                await request
            await owner.shutdown()
        if not abort:
            assert caught.value.headers["X-PBGUI-Error-Codes"] == "api_worker_start_failed"
        assert owner.active == 0

    asyncio.run(exercise())


def test_keyboard_interrupt_during_start_cleans_registration(monkeypatch):
    """A real BaseException before thread start cannot leave a pending shutdown future."""
    def interrupt(thread):
        """Interrupt before any OS thread or operation exists."""
        raise KeyboardInterrupt

    monkeypatch.setattr(threading.Thread, "start", interrupt)

    async def exercise():
        owner = pb9.PB9RequestOwner()
        with pytest.raises(KeyboardInterrupt):
            await owner.run("runtime")
        assert owner.active == 0
        await owner.shutdown()

    asyncio.run(exercise())


@pytest.mark.parametrize("partial_failure", [False, True])
def test_signal_hooks_chain_restore_and_clean_partial_startup(monkeypatch, partial_failure):
    """Only owned handlers are restored, including a failure midway through installation."""
    calls = []

    def prior(received, frame):
        """Represent Uvicorn's preexisting signal handler."""
        calls.append(received)

    handlers = {signal.SIGTERM: prior, signal.SIGINT: prior}

    def install(received, handler):
        """Replace a fixture handler without touching process-wide real signal state."""
        if partial_failure and received == signal.SIGINT and handler is not prior:
            raise RuntimeError("fixture signal installation failed")
        old = handlers[received]
        handlers[received] = handler
        return old

    monkeypatch.setattr(pb9.signal, "getsignal", lambda received: handlers[received])
    monkeypatch.setattr(pb9.signal, "signal", install)

    async def exercise():
        app = FastAPI()
        if partial_failure:
            with pytest.raises(RuntimeError):
                pb9.startup(app, install_signal_hooks=True)
            assert not app.state.pb9_requests.accepting
        else:
            pb9.startup(app, install_signal_hooks=True)
            owner = app.state.pb9_requests
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            assert owner.cancel_signal.is_set()
            assert not owner.accepting
            assert calls == [signal.SIGTERM]
            with pytest.raises(RuntimeError):
                pb9.startup(app)
        await pb9.shutdown(app)
        assert handlers == {signal.SIGTERM: prior, signal.SIGINT: prior}

    asyncio.run(exercise())


def test_validate_uses_owned_worker_server_paths_and_detached_document(endpoint):
    """Only authored input and GUI metadata are echoed; source is server-derived."""
    document = {"config_version": "v9.0.0", "bot": {"long": {}},
                "pbgui": {"name": "Example", "source": "authored-note"}}
    response = endpoint.client.post("/api/pb9/validate", json={"config": document},
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 200
    result = response.json()
    assert result["valid"] is True
    assert result["config"] == {key: value for key, value in document.items() if key != "pbgui"}
    assert result["pbgui"] == document["pbgui"]
    assert result["source"] == {"repo_dir": endpoint.values["pb9dir"]}
    assert response.headers["cache-control"] == "no-store"
    assert len(endpoint.snapshots) == 1
    assert endpoint.calls == [("validate", endpoint.values["pb9dir"], endpoint.values["pb9venv"],
                               {key: endpoint.values[key] for key in pb9._PATH_KEYS[2:]})]
    assert endpoint.app.state.pb9_requests.active == 0


@pytest.mark.parametrize("body", [
    b"", b"null", b"[]", b"{}", b'{"config":null}', b'{"config":[]}',
    b'{"config":{},"pb9dir":"/private/override"}', b'{"config":{},"config":{}}',
    b'{"config":{"bot":{"x":1,"x":2}}}', b'{"config":{"x":NaN}}',
    b'{"config":{"x":Infinity}}', b'{"config":{"x":1e400}}',
    b'{"config":{"x":"\xff"}}', b'{"config":{"pbgui":{"note":"\\ud800"}}}',
    b'{"config":{"\\udfff":0}}', b'{"config":' + b'[' * 1200 + b'0' + b']' * 1200 + b'}',
])
def test_validate_rejects_ambiguous_json_and_path_overrides_before_settings(endpoint, body):
    """Strict UTF-8 JSON rejects duplicates and nonfinite values without echoing inputs."""
    response = endpoint.client.post("/api/pb9/validate", content=body,
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 422
    assert response.headers["x-pbgui-error-codes"] == "invalid_document"
    assert response.headers["cache-control"] == "no-store"
    assert "/private/override" not in response.text
    assert not endpoint.snapshots
    assert not endpoint.calls
    assert endpoint.app.state.pb9_requests.active == 0


def test_validate_bounds_streamed_body_and_rejects_query_parameters(endpoint):
    """Streaming cannot bypass the byte budget or select another runtime."""
    def chunks():
        """Deliver a body without depending on Content-Length."""
        yield b" " * pb9.MAX_VALIDATION_BODY_BYTES
        yield b"x"

    headers = {"Authorization": "Bearer fixture-auth"}
    response = endpoint.client.post("/api/pb9/validate", content=chunks(), headers=headers)
    assert response.status_code == 413
    assert response.headers["cache-control"] == "no-store"
    response = endpoint.client.post("/api/pb9/validate?pb9dir=/private/override", json={"config": {}}, headers=headers)
    assert response.status_code == 422
    assert not endpoint.snapshots
    assert not endpoint.calls


@pytest.mark.parametrize("origin", [None, "https://other.example", "http://testserver"])
def test_validate_cookie_auth_requires_origin(endpoint, origin):
    """Validation consumes native resources and uses the shared POST origin guard."""
    endpoint.client.cookies.set(auth.SESSION_COOKIE_NAME, "fixture-auth")
    response = endpoint.client.post("/api/pb9/validate", json={"config": {"config_version": "v9.0.0"}},
                                    headers={"Origin": origin} if origin else {})
    assert response.status_code == (200 if origin == "http://testserver" else 403)
    assert response.headers["cache-control"] == "no-store"
    assert len(endpoint.calls) == (1 if response.status_code == 200 else 0)


@pytest.mark.parametrize("code,status", [("invalid_document", 422), ("invalid_config", 422),
    ("config_version_mismatch", 422), ("external_reference_rejected", 422),
    ("native_config_rejected", 422), ("invalid_request", 422), ("runtime_not_detected", 503),
    ("helper_cancelled", 503), ("helper_timeout", 503), ("native_validation_failed", 503)])
def test_validation_error_mapping_separates_input_and_runtime_failures(endpoint, monkeypatch, code, status):
    """Known input rejections are 422, operational failures stay fixed 503 errors."""
    def reject(*args, **kwargs):
        """Return one sanitized native-domain result without private diagnostics."""
        raise PB9ConfigError(code)

    monkeypatch.setattr(pb9, "validate_pb9_document", reject)
    response = endpoint.client.post("/api/pb9/validate", json={"config": {"config_version": "v9.0.0"}},
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == status
    assert response.headers["x-pbgui-error-codes"] == code
    assert response.headers["cache-control"] == "no-store"
    assert "config_version" not in response.text
    assert endpoint.app.state.pb9_requests.active == 0


def test_validate_rejects_excessive_but_parseable_nesting(endpoint):
    """A bounded depth remains safe to re-encode on the deeper ASGI call stack."""
    body = b'{"config":{"pbgui":{"nested":' + b'[' * 65 + b'0' + b']' * 65 + b'}}}'
    response = endpoint.client.post("/api/pb9/validate", content=body,
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 422
    assert response.headers["x-pbgui-error-codes"] == "invalid_document"
    assert not endpoint.snapshots


def test_response_render_failure_is_fixed_uncached_error(endpoint, monkeypatch):
    """Unexpected adapter output cannot escape as an unguarded render traceback."""
    monkeypatch.setattr(pb9, "load_pb9_template", lambda *args, **kwargs: {"private": object()})
    response = endpoint.client.post("/api/pb9/template", json={},
                                    headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 503
    assert response.headers["x-pbgui-error-codes"] == "api_response_failed"
    assert response.headers["cache-control"] == "no-store"
    assert "private" not in response.text
    assert endpoint.app.state.pb9_requests.active == 0
