"""Offline Chromium checks driven directly from Python over the DevTools protocol.

No JavaScript automation server, package manager or driver process is involved.
Each browser owns a temporary profile and blocks all unhandled network traffic.
The small synchronous API keeps existing UI assertions readable while callbacks
and polling run on the same Python thread, including deliberately delayed routes.
"""
from __future__ import annotations

import atexit
import base64
import contextlib
import fnmatch
import json
import mimetypes
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
from types import SimpleNamespace
from urllib.parse import urlparse

from websockets.sync.client import connect


class Error(RuntimeError):
    """A browser protocol or JavaScript evaluation failed."""


class TimeoutError(Error):
    """A bounded browser condition did not become true."""


def _matches(pattern, value):
    """Match explicit strings, browser URL globs or compiled regular expressions."""
    if hasattr(pattern, "search"):
        return bool(pattern.search(value))
    return fnmatch.fnmatchcase(value, pattern) if "*" in pattern else value == pattern


def _function_expression(source):
    """Distinguish a callable browser expression from an already invoked closure."""
    if re.match(r"^\(\s*(?:\(|async\s*\(|(?:async\s+)?function\b)", source, re.S):
        return False
    return bool(re.match(r"^(?:async\s*)?(?:function\b|\([^)]*\)\s*=>|[\w$]+\s*=>)", source, re.S))


def _install_route(routes, pattern, handler, times):
    """Consume bounded route registrations before invoking their callbacks."""
    if times is None:
        routes.append((pattern, handler))
        return
    if not isinstance(times, int) or times < 1:
        raise ValueError("Route times must be a positive integer")
    remaining = times

    def bounded(route):
        """Remove the final registration before a nested request can reuse it."""
        nonlocal remaining
        remaining -= 1
        if not remaining:
            routes.remove((pattern, bounded))
        handler(route)

    bounded.__wrapped__ = handler
    routes.append((pattern, bounded))


class _Connection:
    """Own one CDP connection and dispatch events while awaiting command replies."""

    def __init__(self, url):
        """Connect only to the browser-owned loopback debugging socket."""
        self.socket = connect(url, proxy=None, max_size=64 * 1024 * 1024, open_timeout=10, close_timeout=1)
        self.sequence = 0
        self.replies = {}
        self.handlers = {}
        self.timeout = 10

    def send(self, method, params=None, timeout=None):
        """Send a command; route callbacks can safely issue nested commands."""
        self.sequence += 1
        identifier = self.sequence
        self.socket.send(json.dumps({"id": identifier, "method": method, "params": params or {}}))
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while identifier not in self.replies:
            self.pump(max(0.001, deadline - time.monotonic()))
            if time.monotonic() >= deadline and identifier not in self.replies:
                raise TimeoutError(f"Browser command timed out: {method}")
        response = self.replies.pop(identifier)
        if "error" in response:
            raise Error(f"{method}: {response['error'].get('message', 'protocol failure')}")
        return response.get("result", {})

    def pump(self, timeout=0.01):
        """Dispatch one event without a reader thread or concurrent Python callbacks."""
        try:
            message = json.loads(self.socket.recv(timeout=timeout))
        except __import__('builtins').TimeoutError:
            return False
        if "id" in message:
            self.replies[message["id"]] = message
        else:
            for handler in list(self.handlers.get(message.get("method"), [])):
                handler(message.get("params", {}))
        return True

    def on(self, method, handler):
        """Subscribe a lifecycle-owned callback."""
        self.handlers.setdefault(method, []).append(handler)

    def close(self):
        """Close the socket and release callbacks deterministically."""
        self.handlers.clear()
        self.socket.close()


class Browser:
    """An isolated native Chromium process and its owned contexts."""

    def __init__(self, **options):
        """Launch a locally installed browser; never download executables implicitly."""
        executable = options.get("executable_path") or os.environ.get("PBGUI_TEST_BROWSER")
        if not executable:
            executable = next((shutil.which(name) for name in ("brave-browser", "google-chrome", "chromium")
                               if shutil.which(name)), None)
        if not executable:
            raise Error("Install a native Chromium browser or set PBGUI_TEST_BROWSER")
        self.profile = tempfile.TemporaryDirectory(prefix="pbgui-browser-test-")
        self.contexts = []
        self.closed = False
        self.log = tempfile.TemporaryFile()
        argv = [str(executable), "--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
                "--no-first-run", "--no-default-browser-check",
                "--disable-background-networking", "--disable-component-update", "--disable-sync",
                "--disable-background-timer-throttling", "--disable-renderer-backgrounding",
                "--disable-backgrounding-occluded-windows",
                "--disable-default-apps", "--disable-extensions", "--disable-client-side-phishing-detection",
                "--metrics-recording-only", "--password-store=basic", "--proxy-server=direct://",
                "--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE localhost, EXCLUDE 127.0.0.1, EXCLUDE [::1]",
                "--remote-debugging-port=0", "--remote-debugging-address=127.0.0.1",
                "--user-data-dir=" + self.profile.name, "about:blank"]
        if options.get("software_rendering", True):
            argv.insert(1, "--disable-gpu")
        # Test launch arguments may change layout/features, never profile ownership.
        argv[1:1] = [arg for arg in options.get("args", []) if not arg.startswith((
            "--user-data-dir", "--remote-debugging", "--proxy", "--host-resolver"))]
        self.process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log,
                                        start_new_session=True)
        try:
            endpoint = Path(self.profile.name) / "DevToolsActivePort"
            deadline = time.monotonic() + 15
            while not endpoint.is_file():
                if self.process.poll() is not None or time.monotonic() > deadline:
                    raise Error("Native Chromium could not start its isolated test profile")
                time.sleep(0.02)
            port, path, *_ = endpoint.read_text().splitlines()
            self.port = int(port)
            self.cdp = _Connection(f"ws://127.0.0.1:{self.port}{path}")
            self.downloads = {}
            self.download_directory = Path(self.profile.name) / "downloads"
            self.download_directory.mkdir(mode=0o700)
            self.cdp.on("Browser.downloadWillBegin", self._download)
            self.cdp.on("Browser.downloadProgress", self._download_progress)
        except BaseException:
            self.close()
            raise

    def new_context(self, **options):
        """Create a separate incognito context with no shared application storage."""
        context = BrowserContext(self, options)
        self.contexts.append(context)
        return context

    def _download(self, payload):
        """Attribute a native download to its owning page, never the user's profile."""
        for context in self.contexts:
            for page in context.pages:
                if payload.get("frameId") in page.frames:
                    download = Download(page, payload, self.download_directory)
                    self.downloads[payload["guid"]] = download
                    page._emit("download", download)
                    return

    def _download_progress(self, payload):
        """Retain completion/cancellation proof for bounded download waits."""
        download = self.downloads.get(payload["guid"])
        if download:
            download.state = payload["state"]

    def new_page(self, **options):
        """Create a page in its own context."""
        return self.new_context(**options).new_page()

    def close(self):
        """Close contexts, browser, pipes and temporary profile, even after failures."""
        if self.closed:
            return
        self.closed = True
        for context in self.contexts:
            with contextlib.suppress(Exception):
                context.close()
        if hasattr(self, "cdp"):
            with contextlib.suppress(Exception):
                self.cdp.send("Browser.close", timeout=2)
            with contextlib.suppress(Exception):
                self.cdp.close()
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, signal.SIGTERM)
        if self.process.poll() is None:
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        # Native browser helpers share this newly created process group. Do not
        # let a surviving crash/utility helper race profile removal or linger.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, signal.SIGKILL)
        self.log.close()
        for attempt in range(10):
            try:
                self.profile.cleanup()
                break
            except OSError:
                if attempt == 9:
                    raise
                time.sleep(0.02)


class BrowserContext:
    """Own synthetic routes, pages and one incognito browser context."""

    def __init__(self, browser, options):
        """Initialize context defaults before creating any network-capable page."""
        self.browser, self.options = browser, options
        self.identifier = browser.cdp.send("Target.createBrowserContext")["browserContextId"]
        browser.cdp.send("Browser.setDownloadBehavior", {"behavior": "allowAndName",
            "browserContextId": self.identifier, "downloadPath": str(browser.download_directory), "eventsEnabled": True})
        self.pages, self.routes, self.ws_routes, self.init_scripts = [], [], [], []
        self.timeout = 30000
        self.navigation_timeout = 30000
        self.closed = False

    def new_page(self):
        """Create a blank target and attach request interception before navigation."""
        target = self.browser.cdp.send("Target.createTarget", {
            "url": "about:blank", "browserContextId": self.identifier})["targetId"]
        page = Page(self, target)
        self.pages.append(page)
        return page

    def route(self, pattern, handler, *, times=None, **_options):
        """Install a context route shared by current and future pages."""
        _install_route(self.routes, pattern, handler, times)

    def route_web_socket(self, pattern, handler):
        """Install an entirely local WebSocket test double."""
        self.ws_routes.append((pattern, handler))
        for page in self.pages:
            page._install_sockets()

    def add_init_script(self, script=None, *, path=None):
        """Register browser-only setup for current and future documents."""
        script = Path(path).read_text() if path else script
        self.init_scripts.append(script)
        for page in self.pages:
            page.add_init_script(script)

    def set_default_timeout(self, value):
        """Set the bounded assertion/action timeout in milliseconds."""
        self.timeout = value

    def set_default_navigation_timeout(self, value):
        """Keep navigation and action deadlines independent."""
        self.navigation_timeout = value

    def new_cdp_session(self, page):
        """Expose the already owned Python connection for protocol-specific checks."""
        return page.cdp

    def close(self):
        """Dispose all pages and their browser storage exactly once."""
        if self.closed:
            return
        self.closed = True
        for page in self.pages:
            page.close()
        with contextlib.suppress(Error):
            self.browser.cdp.send("Target.disposeBrowserContext", {"browserContextId": self.identifier})


class Request:
    """A synthetic or intercepted browser request, without production credentials."""

    def __init__(self, payload):
        """Retain only the request fields exercised by isolated tests."""
        self.url, self.method = payload["url"], payload.get("method", "GET")
        self.headers = payload.get("headers", {})
        self.post_data = payload.get("postData")
        self.post_data_buffer = self.post_data.encode() if self.post_data is not None else None

    @property
    def post_data_json(self):
        """Decode the test request body on demand."""
        return json.loads(self.post_data) if self.post_data else None


class Route:
    """A paused request that can remain pending until the test explicitly releases it."""

    def __init__(self, page, payload):
        """Bind the route to its owning page and request ID."""
        self.page, self.identifier = page, payload["requestId"]
        self.network_id = payload.get("networkId")
        self.request = Request(payload["request"])
        self.done = False

    def fulfill(self, *, status=200, body="", content_type=None, headers=None, json=None, path=None, **_options):
        """Fulfil through CDP; headers and binary bodies retain their original types."""
        if self.done or self.page.closed:
            return
        if path is not None:
            source = Path(path).resolve()
            root = Path(__file__).resolve().parents[1]
            if not any(source.is_relative_to(directory) for directory in (
                    root / "frontend", root / "docs", Path(tempfile.gettempdir()).resolve())):
                raise Error("Browser response files must be public source assets or temporary fixtures")
            body = source.read_bytes()
            content_type = content_type or mimetypes.guess_type(source)[0] or "application/octet-stream"
        if json is not None:
            body = __import__('json').dumps(json)
            content_type = content_type or "application/json"
        raw = body if isinstance(body, bytes) else str(body).encode()
        response_headers = {**(headers or {})}
        if content_type:
            response_headers["Content-Type"] = content_type
        try:
            self.page.cdp.send("Fetch.fulfillRequest", {"requestId": self.identifier,
                "responseCode": status, "responseHeaders": [{"name": k, "value": str(v)}
                for k, v in response_headers.items()], "body": base64.b64encode(raw).decode()})
        except Error as exc:
            if "Invalid InterceptionId" not in str(exc):
                raise
            self.done = True
            return
        self.done = True
        response = SimpleNamespace(url=self.request.url, status=status, request=self.request)
        self.page._emit("response", response)
        if not self.page.route_depth:
            if self.network_id:
                self.page._wait(lambda: self.network_id in self.page.completed_requests, 5000)
                self.page.completed_requests.discard(self.network_id)
            self.page._drain()

    def abort(self, *_args, **_options):
        """Fail a request without accessing the network."""
        if not self.done and not self.page.closed:
            try:
                self.page.cdp.send("Fetch.failRequest", {"requestId": self.identifier, "errorReason": "Failed"})
            except Error as exc:
                if "Invalid InterceptionId" not in str(exc):
                    raise
            self.done = True

    def continue_(self, **_options):
        """Permit only an explicitly routed loopback test server."""
        parsed = urlparse(self.request.url)
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} or parsed.port == 8000:
            raise Error("Browser tests may continue only isolated loopback requests")
        self.page.cdp.send("Fetch.continueRequest", {"requestId": self.identifier})
        self.done = True


class _EventWait:
    """Collect an event without missing it during the action's nested protocol calls."""

    def __init__(self, page, name, predicate=None, timeout=None):
        """Register lazily at context entry."""
        self.page, self.name, self.predicate = page, name, predicate
        self.timeout = timeout or page.timeout
        self.value = None

    def __enter__(self):
        """Subscribe before the triggering user action."""
        self.page.on(self.name, self._receive)
        return self

    def _receive(self, value):
        """Retain the first matching event."""
        predicate = self.predicate
        if self.value is None and (predicate is None or (
                predicate(value) if callable(predicate) else _matches(predicate, value.url))):
            self.value = value

    def __exit__(self, kind, _value, _traceback):
        """Wait boundedly, then unregister even on failure."""
        try:
            if kind is None:
                self.page._wait(lambda: self.value is not None, self.timeout)
        finally:
            self.page.events[self.name].remove(self._receive)


class Download:
    """A bounded native download stored exclusively in the test browser profile."""

    def __init__(self, page, payload, directory):
        """Keep the browser-assigned GUID separate from untrusted suggested names."""
        self.page, self.url = page, payload["url"]
        self.suggested_filename = payload["suggestedFilename"]
        self.file = directory / payload["guid"]
        self.state = "inProgress"

    def path(self):
        """Wait for browser completion before exposing the test-owned file."""
        self.page._wait(lambda: self.state in {"completed", "canceled"}, 10000)
        if self.state != "completed" or not self.file.is_file():
            raise Error("Isolated test download did not complete")
        return str(self.file)

    def save_as(self, destination):
        """Copy only into an explicitly temporary test directory."""
        path = Path(destination).resolve()
        if not path.is_relative_to(Path(tempfile.gettempdir()).resolve()):
            raise Error("Browser test downloads must remain in the temporary directory")
        shutil.copyfile(self.path(), path)


class Page:
    """One browser page controlled entirely by its Python owner."""

    def __init__(self, context, target):
        """Enable scripts and interception before allowing test navigation."""
        self.context, self.target = context, target
        self.cdp = _Connection(f"ws://127.0.0.1:{context.browser.port}/devtools/page/{target}")
        self.timeout = context.timeout
        self.navigation_timeout = context.navigation_timeout
        self.events, self.routes, self.ws_routes = {}, [], []
        self.route_depth = 0
        self.completed_requests = set()
        self.frames = set()
        self.loader = None
        self.main_frame = target
        self.loading = False
        self.loading_frames = set()
        self.test_origins = set()
        self.init_scripts = []
        self.closed = False
        self.cdp.on("Fetch.requestPaused", self._request)
        self.cdp.on("Runtime.exceptionThrown", self._exception)
        self.cdp.on("Runtime.consoleAPICalled", self._console)
        self.cdp.on("Runtime.bindingCalled", self._binding)
        self.cdp.on("Runtime.executionContextDestroyed", self._context_destroyed)
        self.cdp.on("Runtime.executionContextsCleared", lambda _payload: self._sockets.clear())
        self.cdp.on("Network.loadingFinished", self._request_completed)
        self.cdp.on("Network.loadingFailed", self._request_completed)
        self.cdp.on("Page.frameNavigated", self._frame_navigated)
        self.cdp.on("Page.frameStartedLoading", lambda payload: self._loading(payload, True))
        self.cdp.on("Page.frameStoppedLoading", lambda payload: self._loading(payload, False))
        self.cdp.on("Page.frameDetached", lambda payload: self.loading_frames.discard(payload["frameId"]))
        self.cdp.send("Page.enable")
        self.cdp.send("Page.bringToFront")
        self.cdp.send("Runtime.enable")
        self.cdp.send("Network.enable")
        self.cdp.send("Emulation.setLocaleOverride", {"locale": context.options.get("locale", "en-US")})
        self.cdp.send("Emulation.setTimezoneOverride", {"timezoneId": context.options.get("timezone_id", "UTC")})
        if context.options.get("has_touch"):
            self.cdp.send("Emulation.setTouchEmulationEnabled", {"enabled": True, "maxTouchPoints": 1})
        self.cdp.send("Network.setCacheDisabled", {"cacheDisabled": True})
        self.cdp.send("Fetch.enable", {"patterns": [{"urlPattern": "*"}]})
        self.cdp.send("Runtime.addBinding", {"name": "__pbguiTestSocket"})
        self.mouse, self.keyboard, self.clock = Mouse(self), Keyboard(self), Clock(self)
        self._sockets = {}
        self.asset_sequence = 0
        query_helpers = (Path(__file__).parent / "browser_queries.js").read_text()
        self.add_init_script(query_helpers)
        self.evaluate(query_helpers)
        self._install_sockets()
        self.set_viewport_size(context.options.get("viewport") or {"width": 1280, "height": 720})
        for script in context.init_scripts:
            self.add_init_script(script)

    def _exception(self, payload):
        """Forward genuine page errors without swallowing test callbacks."""
        details = payload.get("exceptionDetails", {})
        self._emit("pageerror", Error(details.get("exception", {}).get("description") or details.get("text", "JavaScript error")))

    def _request_completed(self, payload):
        """Retain bounded completion proof before advancing a released response's clock."""
        if len(self.completed_requests) >= 4096:
            self.completed_requests.clear()
        self.completed_requests.add(payload["requestId"])

    def _console(self, payload):
        """Expose genuine browser console diagnostics to regression assertions."""
        kind = "warning" if payload["type"] == "warning" else payload["type"]
        values = [str(arg.get("value", arg.get("description", ""))) for arg in payload.get("args", [])]
        self._emit("console", SimpleNamespace(type=kind, text=" ".join(values)))

    def _frame_navigated(self, payload):
        """Track document commits separately from old-document readiness."""
        frame = payload["frame"]
        self.frames.add(frame["id"])
        if not frame.get("parentId"):
            self.loader = frame.get("loaderId")
            self.main_frame = frame["id"]

    def _loading(self, payload, value):
        """Track native document loading independently of element visibility."""
        if value:
            self.loading_frames.add(payload['frameId'])
        else:
            self.loading_frames.discard(payload['frameId'])
        if payload["frameId"] == self.main_frame:
            self.loading = value

    def _emit(self, name, value):
        """Call event observers on the controlling Python thread."""
        for handler in list(self.events.get(name, [])):
            handler(value)

    def on(self, name, handler):
        """Subscribe a callback owned by this page."""
        self.events.setdefault(name, []).append(handler)

    def _request(self, payload):
        """All requests are intercepted; unmatched traffic never leaves the browser."""
        route = Route(self, payload)
        if urlparse(route.request.url).path == "/favicon.ico":
            route.fulfill(status=204, body="")
            return
        self._emit("request", route.request)
        for pattern, handler in reversed(self.context.routes + self.routes):
            if _matches(pattern, route.request.url):
                self.route_depth += 1
                try:
                    handler(route)
                finally:
                    self.route_depth -= 1
                return
        parsed = urlparse(route.request.url)
        if (parsed.scheme, parsed.hostname, parsed.port) in self.test_origins:
            route.continue_()
            return
        route.abort()

    def _binding(self, payload):
        """Route local WebSocket messages to the explicit Python test double."""
        if payload.get("name") != "__pbguiTestSocket":
            return
        value = json.loads(payload["payload"])
        context_id = payload["executionContextId"]
        if value["type"] == "open":
            socket = _WebSocket(self, value["id"], value["url"], context_id)
            self._sockets[(context_id, socket.identifier)] = socket
            for pattern, handler in reversed(self.context.ws_routes + self.ws_routes):
                if _matches(pattern, socket.url):
                    handler(socket)
                    return
            socket.close()
        elif value["type"] == "message":
            socket = self._sockets.get((context_id, value["id"]))
            if socket and socket.handler:
                socket.handler(value["data"])

    def _context_destroyed(self, payload):
        """Release socket doubles owned by an iframe/document that went away."""
        identifier = payload["executionContextId"]
        for key in list(self._sockets):
            if key[0] == identifier:
                self._sockets.pop(key, None)

    def _install_sockets(self):
        """Block native WebSocket access and expose message-compatible local doubles."""
        self.add_init_script("""(() => {
          window.__pbguiSockets = new Map(); let sequence=0;
          class TestSocket extends EventTarget {
            static CONNECTING=0; static OPEN=1; static CLOSING=2; static CLOSED=3;
            constructor(url){super();this.url=String(url);this.readyState=0;this.id=++sequence;
              window.__pbguiSockets.set(this.id,this);
              queueMicrotask(()=>{this.readyState=1;
                __pbguiTestSocket(JSON.stringify({type:'open',id:this.id,url:this.url}));
                this._event('open',new Event('open'));});}
            _event(name,event){this.dispatchEvent(event);this['on'+name]?.(event);}
            send(data){__pbguiTestSocket(JSON.stringify({type:'message',id:this.id,data}));}
            close(code=1000,reason=''){this.readyState=3;this._event('close',new CloseEvent('close',{code,reason}));}
          }
          Object.assign(TestSocket.prototype,{CONNECTING:0,OPEN:1,CLOSING:2,CLOSED:3});
          window.WebSocket=TestSocket;
        })();""")

    def route(self, pattern, handler, *, times=None, **_options):
        """Register a page-local request handler, most recent first."""
        _install_route(self.routes, pattern, handler, times)

    def unroute(self, pattern, handler=None):
        """Remove only the requested route."""
        self.routes = [(p, h) for p, h in self.routes if p != pattern or handler is not None and getattr(h, '__wrapped__', h) != handler]

    def unroute_all(self, **_options):
        """Release page route callbacks."""
        self.routes.clear()

    def route_web_socket(self, pattern, handler):
        """Register a Python-only synthetic socket handler."""
        self.ws_routes.append((pattern, handler))

    def add_init_script(self, script=None, *, path=None):
        """Install a browser script for each navigation, without server-side JS."""
        script = Path(path).read_text() if path else script
        self.init_scripts.append(script)
        self.cdp.send("Page.addScriptToEvaluateOnNewDocument", {"source": script})

    def _wait(self, predicate, timeout=None):
        """Poll while dispatching network/events, retaining the final useful failure."""
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout) / 1000
        last_error = None
        while True:
            try:
                result = predicate()
                if result:
                    return result
            except (Error, AssertionError) as exc:
                last_error = exc
            if time.monotonic() >= deadline:
                raise TimeoutError(str(last_error) if last_error else "Browser condition timed out")
            self.cdp.pump(0.01)
            self.context.browser.cdp.pump(0.001)

    def evaluate(self, expression, arg=None, *, _context=None):
        """Evaluate Vanilla JS in the actual browser and await returned promises."""
        source = expression.strip()
        function = _function_expression(source)
        if function:
            source = f"({source.rstrip(';')})({json.dumps(arg)})"
        options = {"expression": source, "awaitPromise": True, "returnByValue": False, "userGesture": True}
        if _context is not None:
            options["contextId"] = _context
        result = self.cdp.send("Runtime.evaluate", options, timeout=max(self.timeout / 1000, 15))
        if "exceptionDetails" in result:
            details = result["exceptionDetails"]
            raise Error(details.get("exception", {}).get("description") or details.get("text", "JavaScript failed"))
        value = result.get("result", {})
        identifier = value.get("objectId")
        if identifier:
            try:
                if value.get("type") == "function" or value.get("subtype") == "node" or value.get("className") == "Window":
                    return None
                serialized = self.cdp.send("Runtime.callFunctionOn", {"objectId": identifier,
                    "functionDeclaration": "function(){return this;}", "returnByValue": True, "awaitPromise": True})
                if "exceptionDetails" in serialized:
                    raise Error(serialized["exceptionDetails"].get("text", "Browser value could not be serialized"))
                value = serialized.get("result", {})
            finally:
                self.cdp.send("Runtime.releaseObject", {"objectId": identifier})
        if value.get("unserializableValue") == "NaN":
            return float("nan")
        return value.get("value")

    def goto(self, url, **_options):
        """Navigate through intercepted requests and wait for the loaded document."""
        parsed = urlparse(url)
        if parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"} and parsed.port and parsed.port >= 1024 and parsed.port != 8000:
            self.test_origins.add((parsed.scheme, parsed.hostname, parsed.port))
        navigation = self.cdp.send("Page.navigate", {"url": url})
        if navigation.get("errorText"):
            raise Error("Isolated browser navigation failed: " + navigation["errorText"])
        expected = navigation.get("loaderId")
        self._wait(lambda: (not expected or self.loader == expected) and self.evaluate("document.readyState") == "complete", self.navigation_timeout)
        self._drain()

    def _drain(self):
        """Let immediate route replies/microtasks settle without fixed long sleeps."""
        deadline = time.monotonic() + 0.1
        while time.monotonic() < deadline:
            active = self.cdp.pump(0.01)
            active = self.context.browser.cdp.pump(0.001) or active
            if not active:
                break

    @property
    def url(self):
        """Read the active browser location."""
        return self.evaluate("location.href")

    def reload(self, **_options):
        """Reload while preserving the same page and storage context."""
        old_loader = self.loader
        self.cdp.send("Page.reload", {"ignoreCache": True})
        self._wait(lambda: self.loader != old_loader and self.evaluate("document.readyState") == "complete", self.navigation_timeout)
        self._drain()

    def set_content(self, html, **_options):
        """Write an isolated document; scripts execute only in Chromium."""
        self.evaluate("html=>{document.open();document.write(html);document.close();}", html)
        self._drain()

    def add_script_tag(self, *, content=None, path=None, url=None, **_options):
        """Load local JavaScript as a real browser script tag."""
        self.asset_sequence += 1
        marker = "script-" + str(self.asset_sequence)
        if url is not None:
            self.evaluate("arg=>new Promise((resolve,reject)=>{const s=document.createElement('script');s.dataset.pbguiAsset=arg.marker;s.src=arg.url;s.onload=()=>resolve(true);s.onerror=()=>reject(Error('Fixture script failed to load'));document.head.appendChild(s);})", {"url": url, "marker": marker})
        else:
            self.evaluate("arg=>{const s=document.createElement('script');s.dataset.pbguiAsset=arg.marker;s.textContent=arg.code;document.head.appendChild(s);}",
                          {"code": Path(path).read_text() if path else content, "marker": marker})
        return self.locator('script[data-pbgui-asset="' + marker + '"]')

    def add_style_tag(self, *, content=None, path=None, url=None, **_options):
        """Load a local stylesheet without network dependencies."""
        self.asset_sequence += 1
        marker = "style-" + str(self.asset_sequence)
        if url is not None:
            self.evaluate("arg=>new Promise((resolve,reject)=>{const s=document.createElement('link');s.dataset.pbguiAsset=arg.marker;s.rel='stylesheet';s.href=arg.url;s.onload=()=>resolve(true);s.onerror=()=>reject(Error('Fixture stylesheet failed to load'));document.head.appendChild(s);})", {"url": url, "marker": marker})
        else:
            self.evaluate("arg=>{const s=document.createElement('style');s.dataset.pbguiAsset=arg.marker;s.textContent=arg.css;document.head.appendChild(s);}",
                          {"css": Path(path).read_text() if path else content, "marker": marker})
        return self.locator('[data-pbgui-asset="' + marker + '"]')

    def set_viewport_size(self, value):
        """Set deterministic layout dimensions."""
        self.cdp.send("Emulation.setDeviceMetricsOverride", {**value, "deviceScaleFactor": 1, "mobile": False})

    def set_default_timeout(self, value):
        """Set bounded page action/assertion deadlines."""
        self.timeout = value

    def set_default_navigation_timeout(self, value):
        """Set navigation deadlines without changing control action deadlines."""
        self.navigation_timeout = value

    def locator(self, selector, **options):
        """Build a lazy DOM query evaluated afresh after each automatic update."""
        return Locator(self, f"window.__pbguiQuery(document,{json.dumps(selector)})").filter(**options)

    def get_by_role(self, role, *, name=None, exact=False, include_hidden=False):
        """Query explicit and native ARIA roles with accessible names."""
        return Locator(self, _role_query("document", role, name, exact, include_hidden))

    def select_option(self, selector, value=None, **options):
        """Select options through the page's live DOM locator."""
        return self.locator(selector).select_option(value, **options)

    def dispatch_event(self, selector, name, event_init=None, **options):
        """Dispatch a typed browser event on the selected live control."""
        return self.locator(selector).dispatch_event(name, event_init, **options)

    def get_by_text(self, text, *, exact=False):
        """Find the smallest DOM elements containing the requested visible text."""
        return Locator(self, _text_query("document", text, exact))

    def get_by_label(self, text, *, exact=False):
        """Resolve explicit labels and ARIA labels to their actual control."""
        return Locator(self, f"window.__pbguiLabel(document,{_js_pattern(text)},{json.dumps(exact)})")

    def get_by_alt_text(self, text, *, exact=False):
        """Resolve image alternatives."""
        return self.locator('[alt]').filter(_attribute=("alt", text, exact))

    def get_by_title(self, text, *, exact=False):
        """Resolve title-labelled controls."""
        return self.locator('[title]').filter(_attribute=("title", text, exact))

    def frame_locator(self, selector):
        """Build queries rooted in an explicitly selected same-origin frame."""
        return Frame(self.locator(selector))

    def wait_for_function(self, expression, arg=None, timeout=None, **_options):
        """Wait for a real browser condition while servicing synthetic traffic."""
        source = expression.strip()
        function = _function_expression(source)
        predicate = f"async arg=>Boolean(await ({source})(arg))" if function else f"()=>Boolean({source})"
        result = self._wait(lambda: self.evaluate(predicate, arg), timeout)
        self._drain()
        return result

    def wait_for_selector(self, selector, **options):
        """Wait for a DOM locator using the same assertion semantics."""
        self.locator(selector).wait_for(**options)

    def wait_for_timeout(self, milliseconds):
        """Service events during an explicit test timing interval."""
        deadline = time.monotonic() + milliseconds / 1000
        while time.monotonic() < deadline:
            self.cdp.pump(min(0.01, max(0.001, deadline - time.monotonic())))

    def wait_for_url(self, pattern, **options):
        """Wait for the actual destination, including redirects and user navigation."""
        try:
            self._wait(lambda: _matches(pattern, self.url), options.get("timeout"))
        except TimeoutError as exc:
            raise TimeoutError(f"Expected browser URL {pattern!r}, got {self.url!r}") from exc

    def expect_request(self, predicate=None, **options):
        """Observe a matching request across a triggering action."""
        return _EventWait(self, "request", predicate, options.get("timeout"))

    def expect_response(self, predicate=None, **options):
        """Observe a fulfilled synthetic response."""
        return _EventWait(self, "response", predicate, options.get("timeout"))

    def expect_download(self, **options):
        """Observe an isolated download without using the user's downloads directory."""
        return _EventWait(self, "download", timeout=options.get("timeout"))

    def close(self):
        """Detach CDP callbacks before disposing the owning browser context."""
        if self.closed:
            return
        self.closed = True
        with contextlib.suppress(Exception):
            self.cdp.close()
        self.events.clear()
        self.routes.clear()
        self._sockets.clear()
        self.ws_routes.clear()
        self.init_scripts.clear()
        self.completed_requests.clear()
        self.frames.clear()
        self.loading_frames.clear()
        with contextlib.suppress(Exception):
            self.context.browser.cdp.send("Target.closeTarget", {"targetId": self.target}, timeout=2)


def _role_query(root, role, name, exact, include_hidden=False):
    """Build browser-native role and accessible-name filtering."""
    return f"window.__pbguiRole({root},{json.dumps(role)},{_js_pattern(name)},{json.dumps(exact)},{json.dumps(include_hidden)})"


def _text_query(root, text, exact):
    """Build text matching without interpolating unescaped selector input."""
    return f"window.__pbguiText({root},{_js_pattern(text)},{json.dumps(exact)})"


def _js_pattern(value):
    """Encode a Python regular expression or literal safely for browser evaluation."""
    if hasattr(value, "pattern"):
        return f"new RegExp({json.dumps(value.pattern)}, {'\"i\"' if value.flags & re.I else '\"\"'})"
    return json.dumps(value)


class Locator:
    """Lazy DOM queries with browser input events and retrying assertions."""

    def __init__(self, page, query):
        """Retain a query, never a stale element handle."""
        self.page, self.query = page, query

    def _elements(self):
        """Query expressions always evaluate to a finite array."""
        return f"({self.query})"

    def evaluate(self, expression, arg=None):
        """Evaluate against exactly one live element."""
        return self.page.evaluate(f"arg=>{{const es={self._elements()};if(es.length!==1)throw Error('Expected one element, got '+es.length);return es[0].ownerDocument.defaultView.eval({json.dumps('(' + expression + ')')})(es[0],arg);}}", arg)

    def evaluate_all(self, expression, arg=None):
        """Evaluate against the current matching collection."""
        return self.page.evaluate(f"arg=>{{const es={self._elements()};return (es[0]?.ownerDocument.defaultView||window).eval({json.dumps('(' + expression + ')')})(es,arg);}}", arg)

    def count(self):
        """Count current matches without stale references."""
        return self.page.evaluate(f"{self._elements()}.length")

    def nth(self, index):
        """Select an indexed live match."""
        return Locator(self.page, f"{self._elements()}.slice({index},{index + 1})")

    @property
    def first(self):
        """Select the first current match."""
        return self.nth(0)

    @property
    def last(self):
        """Select the last current match."""
        return Locator(self.page, f"{self._elements()}.slice(-1)")

    def all(self):
        """Return lazy indexed locators for all current matches."""
        return [self.nth(i) for i in range(self.count())]

    def locator(self, selector, **options):
        """Query descendants of the current matching roots."""
        return Locator(self.page, f"{self._elements()}.flatMap(e=>window.__pbguiQuery(e,{json.dumps(selector)}))").filter(**options)

    def filter(self, *, has_text=None, has=None, has_not=None, _attribute=None, **_options):
        """Filter by text or descendant predicates without changing DOM state."""
        query = self._elements()
        if has_text is not None:
            query += f".filter(e=>window.__pbguiMatch(e.textContent,{_js_pattern(has_text)},false))"
        if has is not None:
            query += f".filter(e=>({has.query}).some(child=>e.contains(child)))"
        if has_not is not None:
            query += f".filter(e=>!({has_not.query}).some(child=>e.contains(child)))"
        if _attribute is not None:
            name, value, exact = _attribute
            query += f".filter(e=>window.__pbguiMatch(e.getAttribute({json.dumps(name)}),{_js_pattern(value)},{json.dumps(exact)}))"
        return Locator(self.page, query)

    def get_by_role(self, role, *, name=None, exact=False, include_hidden=False):
        """Query semantic controls inside the current roots."""
        return Locator(self.page, f"{self._elements()}.flatMap(e=>{_role_query('e', role, name, exact, include_hidden)})")

    def get_by_text(self, text, *, exact=False):
        """Query descendant text matches."""
        return Locator(self.page, f"{self._elements()}.flatMap(e=>{_text_query('e', text, exact)})")

    def get_attribute(self, name, **_options):
        """Read the raw attribute, preserving missing versus empty values."""
        return self.evaluate("(e,name)=>e.getAttribute(name)", name)

    def inner_text(self, **_options):
        """Read rendered text."""
        return self.evaluate("e=>e.innerText")

    def text_content(self, **_options):
        """Read DOM text content."""
        return self.evaluate("e=>e.textContent")

    def all_text_contents(self):
        """Read every current match's text."""
        return self.evaluate_all("es=>es.map(e=>e.textContent)")

    def input_value(self, **_options):
        """Read the real control value."""
        return self.evaluate("e=>e.value")

    def is_visible(self, **_options):
        """Match CSS visibility and nonempty bounding geometry."""
        return self.page.evaluate(f"{self._elements()}.some(e=>window.__pbguiVisible(e))")

    def is_hidden(self, **_options):
        """Missing or CSS-hidden elements are hidden."""
        return not self.is_visible()

    def is_disabled(self, **_options):
        """Include native and ARIA disabled state."""
        return self.evaluate("e=>e.matches(':disabled')||e.getAttribute('aria-disabled')==='true'")

    def is_enabled(self, **_options):
        """Read the control's enabled state."""
        return not self.is_disabled()

    def is_checked(self, **_options):
        """Read native checked state."""
        return self.evaluate("e=>e.checked===true")

    def bounding_box(self, **_options):
        """Read actual layout dimensions."""
        return self.evaluate("""e=>{const r=e.getBoundingClientRect();let x=r.x,y=r.y,w=e.ownerDocument.defaultView;
          while(w.frameElement){const f=w.frameElement,fr=f.getBoundingClientRect();x+=fr.x+f.clientLeft;y+=fr.y+f.clientTop;w=w.parent;}
          return {x,y,width:r.width,height:r.height};}""")

    def scroll_into_view_if_needed(self, **_options):
        """Bring a control into the actual scrolling viewport."""
        self.evaluate("e=>e.scrollIntoView({block:'nearest',inline:'nearest'})")

    def click(self, *, position=None, modifiers=None, button="left", click_count=1, timeout=None, force=False, **_options):
        """Dispatch real Chromium mouse input after waiting for actionability."""
        old_loader = self.page.loader
        old_url = self.page.url
        if not force:
            try:
                self.page._wait(lambda: self.is_visible() and self.is_enabled(), timeout)
            except TimeoutError as exc:
                raise TimeoutError(f"Control is not visible/enabled: {self.query}; matches={self.count()}") from exc
        self.scroll_into_view_if_needed()
        previous = None

        def actionable():
            """Wait for stable layout and a hit-tested point, including CSS transitions."""
            nonlocal previous
            current = self.bounding_box()
            pos = position or {"x": current["width"] / 2, "y": current["height"] / 2}
            hit = self.evaluate("""(e,pos)=>{const r=e.getBoundingClientRect();
              const hit=e.ownerDocument.elementFromPoint(r.x+pos.x,r.y+pos.y);return e===hit||e.contains(hit);}""", pos)
            stable = previous is not None and all(abs(current[key] - previous[key]) < 0.25 for key in current)
            previous = current
            if not hit:
                self.evaluate("e=>e.scrollIntoView({block:'center',inline:'center'})")
            return (current, pos) if hit and stable else None

        if force:
            box = self.bounding_box()
            point = position or {"x": box["width"] / 2, "y": box["height"] / 2}
        else:
            try:
                box, point = self.page._wait(actionable, timeout)
            except TimeoutError as exc:
                target = self.evaluate("""(e,pos)=>{const r=e.getBoundingClientRect();
                  const h=e.ownerDocument.elementFromPoint(r.x+(pos?.x??r.width/2),r.y+(pos?.y??r.height/2));
                  return {box:r.toJSON(),hit:h?{tag:h.tagName,id:h.id,classes:String(h.className)}:null};}""", position)
                raise TimeoutError(f"Control did not become clickable: {self.query}; {target}") from exc
        bits = self.page.keyboard.modifiers | sum({"Alt": 1, "Control": 2, "Meta": 4, "Shift": 8}.get(value, 0) for value in modifiers or [])
        self.page.mouse.click(box["x"] + point["x"], box["y"] + point["y"], button=button,
                              click_count=click_count, modifiers=bits)
        new_url = self.page._wait(lambda: self.page.url, self.page.navigation_timeout)
        if self.page.loader != old_loader or self.page.loading or new_url.split('#')[0] != old_url.split('#')[0]:
            self.page._wait(lambda: not self.page.loading and self.page.evaluate("document.readyState") == "complete", self.page.navigation_timeout)
        if self.page.loading_frames:
            self.page._wait(lambda: not self.page.loading_frames, self.page.navigation_timeout)
        self.page._drain()

    def dblclick(self, **options):
        """Dispatch a genuine double click."""
        self.click(click_count=2, **options)

    def tap(self, **_options):
        """Dispatch genuine touch input without preceding mouse-hover events."""
        self.page._wait(lambda: self.is_visible() and self.is_enabled())
        self.scroll_into_view_if_needed()
        box = self.bounding_box()
        point = {"x": box["x"] + box["width"] / 2, "y": box["y"] + box["height"] / 2, "id": 1}
        self.page.cdp.send("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [point]})
        self.page.cdp.send("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})
        self.page._drain()

    def hover(self, **_options):
        """Move the pointer to the live control."""
        self.scroll_into_view_if_needed()
        box = self.bounding_box()
        self.page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)

    def focus(self, **_options):
        """Focus the current DOM control."""
        self.evaluate("e=>e.focus()")

    def blur(self, **_options):
        """Blur the current control."""
        self.evaluate("e=>e.blur()")

    def fill(self, value, **_options):
        """Use the native control setter and dispatch its bubbling input event."""
        try:
            self.page._wait(lambda: self.count() == 1)
        except TimeoutError as exc:
            raise TimeoutError(f"Expected one fill target for {self.query}, got {self.count()}") from exc
        self.evaluate("""(e,value)=>{if(e.ownerDocument.activeElement!==e)e.focus();
          const proto=e instanceof HTMLTextAreaElement?HTMLTextAreaElement.prototype:HTMLInputElement.prototype;
          if(e.isContentEditable)e.textContent=value;else Object.getOwnPropertyDescriptor(proto,'value').set.call(e,value);
          e.dispatchEvent(new Event('input',{bubbles:true}));
          if(['date','datetime-local','time','month','week'].includes(e.type))e.dispatchEvent(new Event('change',{bubbles:true}));} """, str(value))
        self.page._drain()

    def press(self, key, **_options):
        """Focus before sending real keyboard input."""
        self.focus()
        self.page.keyboard.press(key)

    def press_sequentially(self, value, **_options):
        """Insert text through Chromium's input method."""
        self.focus()
        self.page.keyboard.type(value)

    type = press_sequentially

    def select_text(self, **_options):
        """Select the control's current content."""
        self.evaluate("e=>{e.focus();e.select();}")

    def select_option(self, value=None, *, label=None, **_options):
        """Select exact option values and dispatch standard input/change events."""
        values = value if isinstance(value, list) else [value]
        selected = self.evaluate("""(e,args)=>{const selected=[];
          for(const o of e.options){o.selected=args.label!==null?o.label===args.label:args.values.includes(o.value);
            if(o.selected)selected.push(o.value);}
          e.dispatchEvent(new Event('input',{bubbles:true}));e.dispatchEvent(new Event('change',{bubbles:true}));return selected;}""",
          {"values": values, "label": label})
        self.page._drain()
        return selected

    def check(self, **options):
        """Check only if a real click is needed."""
        if not self.is_checked():
            self.click(**options)

    def uncheck(self, **options):
        """Uncheck only if a real click is needed."""
        if self.is_checked():
            self.click(**options)

    def dispatch_event(self, name, event_init=None, **_options):
        """Dispatch a browser event with explicitly provided fixture data."""
        self.evaluate("""(e,arg)=>{const w=e.ownerDocument.defaultView;
          const Type=arg.name.startsWith('pointer')?w.PointerEvent:
            /^(mouse|click|dblclick|contextmenu)/.test(arg.name)?w.MouseEvent:
            arg.name==='wheel'?w.WheelEvent:arg.name.startsWith('key')?w.KeyboardEvent:
            /^(focus|blur)/.test(arg.name)?w.FocusEvent:arg.name==='input'?w.InputEvent:w.Event;
          return e.dispatchEvent(new Type(arg.name,{bubbles:true,cancelable:true,...arg.init}));}""",
                      {"name": name, "init": event_init or {}})
        self.page._drain()

    def wait_for(self, *, state="visible", timeout=None, **_options):
        """Wait for attached/detached or visible/hidden state."""
        predicate = {"visible": self.is_visible, "hidden": self.is_hidden,
                     "attached": lambda: self.count() > 0, "detached": lambda: self.count() == 0}[state]
        self.page._wait(predicate, timeout)

    def element_handle(self):
        """The lazy locator is sufficient for same-origin test frame access."""
        return self

    def content_frame(self):
        """Return a same-origin frame query root."""
        return Frame(self)

    def set_input_files(self, files, **_options):
        """Assign synthetic in-memory files through the browser's File/DataTransfer APIs."""
        entries = files if isinstance(files, list) else [files]
        payload = []
        for entry in entries:
            if isinstance(entry, (str, Path)):
                path = Path(entry)
                entry = {"name": path.name, "mimeType": "application/octet-stream", "buffer": path.read_bytes()}
            payload.append({"name": entry["name"], "type": entry.get("mimeType", ""),
                            "data": base64.b64encode(entry["buffer"]).decode()})
        self.evaluate("""(e,files)=>{const dt=new DataTransfer();for(const f of files)
          dt.items.add(new File([Uint8Array.from(atob(f.data),c=>c.charCodeAt(0))],f.name,{type:f.type}));
          e.files=dt.files;e.dispatchEvent(new Event('input',{bubbles:true}));e.dispatchEvent(new Event('change',{bubbles:true}));}""", payload)


class Frame:
    """Same-origin iframe locators without a second automation runtime."""

    def __init__(self, locator):
        """Keep the frame root live across DOM updates."""
        self.root = locator

    def locator(self, selector, **options):
        """Query inside the current frame document."""
        return Locator(self.root.page, f"({self.root.query}).flatMap(f=>window.__pbguiQuery(f.contentDocument,{json.dumps(selector)}))").filter(**options)

    def get_by_role(self, role, *, name=None, exact=False, include_hidden=False):
        """Resolve roles in the actual frame document."""
        return Locator(self.root.page, f"({self.root.query}).flatMap(f=>{_role_query('f.contentDocument', role, name, exact, include_hidden)})")

    def evaluate(self, expression, arg=None):
        """Execute a fixture in the frame's own browser realm."""
        function = _function_expression(expression.strip())
        source = f"({expression})({json.dumps(arg)})" if function else expression
        return self.root.evaluate("(frame,code)=>frame.contentWindow.eval(code)", source)

    def wait_for_function(self, expression, arg=None, timeout=None, **_options):
        """Wait using JavaScript truthiness inside the frame's own realm."""
        source = expression.strip()
        function = _function_expression(source)
        predicate = f"async arg=>Boolean(await ({source})(arg))" if function else f"()=>Boolean({source})"
        result = self.root.page._wait(lambda: self.evaluate(predicate, arg), timeout)
        self.root.page._drain()
        return result

    def wait_for_timeout(self, milliseconds):
        """Service page protocol events while a frame's explicit timer elapses."""
        self.root.page.wait_for_timeout(milliseconds)

    def wait_for_selector(self, selector, **options):
        """Wait for a frame-local selector."""
        self.locator(selector).wait_for(**options)

    @property
    def url(self):
        """Read the actual frame destination."""
        return self.root.evaluate("frame=>frame.contentWindow.location.href")


class Mouse:
    """Genuine Chromium pointer input, including range-selection drags."""

    def __init__(self, page):
        """Initialize pointer position and pressed-button state."""
        self.page, self.x, self.y, self.button = page, 0, 0, None

    def move(self, x, y, *, steps=1):
        """Send interpolated pointer movement with the correct held button."""
        start_x, start_y = self.x, self.y
        for i in range(1, steps + 1):
            self.x, self.y = start_x + (x - start_x) * i / steps, start_y + (y - start_y) * i / steps
            self.page.cdp.send("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": self.x, "y": self.y,
                "button": self.button or "none", "buttons": {"left": 1, "right": 2, "middle": 4}.get(self.button, 0),
                "modifiers": self.page.keyboard.modifiers})

    def down(self, *, button="left", click_count=1, modifiers=None):
        """Press a button at the current pointer location."""
        self.button = button
        self.page.cdp.send("Input.dispatchMouseEvent", {"type": "mousePressed", "x": self.x, "y": self.y,
            "button": button, "buttons": {"left": 1, "right": 2, "middle": 4}.get(button, 0),
            "clickCount": click_count, "modifiers": self.page.keyboard.modifiers if modifiers is None else modifiers})

    def up(self, *, button="left", click_count=1, modifiers=None):
        """Release a pointer button."""
        self.page.cdp.send("Input.dispatchMouseEvent", {"type": "mouseReleased", "x": self.x, "y": self.y,
            "button": button, "buttons": 0, "clickCount": click_count,
            "modifiers": self.page.keyboard.modifiers if modifiers is None else modifiers})
        self.button = None
        self.page._drain()

    def click(self, x, y, *, button="left", click_count=1, modifiers=0):
        """Move, press and release without synthetic DOM click shortcuts."""
        self.move(x, y)
        self.down(button=button, click_count=click_count, modifiers=modifiers)
        self.up(button=button, click_count=click_count, modifiers=modifiers)

    def wheel(self, delta_x, delta_y):
        """Send wheel input at the current pointer location."""
        self.page.cdp.send("Input.dispatchMouseEvent", {"type": "mouseWheel", "x": self.x,
            "y": self.y, "deltaX": delta_x, "deltaY": delta_y, "modifiers": self.page.keyboard.modifiers})

    def dblclick(self, x, y, **options):
        """Send both native clicks with the second click's correct event count."""
        self.click(x, y, **options)
        self.click(x, y, click_count=2, **options)


class Keyboard:
    """Native text and keyboard input through CDP."""

    def __init__(self, page):
        """Own keyboard state for one page."""
        self.page, self.modifiers = page, 0

    def press(self, chord):
        """Send a key chord with explicit modifier bits."""
        parts = chord.split("+")
        mods = self.modifiers | sum({"Alt": 1, "Control": 2, "Meta": 4, "Shift": 8,
                                     "ControlOrMeta": 2}.get(part, 0) for part in parts[:-1])
        key = parts[-1]
        codes = {"Enter": 13, "Tab": 9, "Escape": 27, "Backspace": 8, "Delete": 46,
                 "ArrowLeft": 37, "ArrowUp": 38, "ArrowRight": 39, "ArrowDown": 40,
                 "Home": 36, "End": 35, "PageUp": 33, "PageDown": 34, "Space": 32}
        actual = " " if key == "Space" else key
        params = {"key": actual, "windowsVirtualKeyCode": codes.get(key, ord(key.upper()) if len(key) == 1 else 0), "modifiers": mods}
        if (len(actual) == 1 or key == "Enter") and not mods & (1 | 2 | 4):
            params["text"] = "\r" if key == "Enter" else actual
        self.page.cdp.send("Input.dispatchKeyEvent", {"type": "keyDown", **params})
        self.page.cdp.send("Input.dispatchKeyEvent", {"type": "keyUp", **params})
        self.page._drain()

    def type(self, value, **options):
        """Type real key events so overwrite, validation and shortcut handlers run."""
        for character in value:
            self.press(character)
            if options.get("delay"):
                self.page.wait_for_timeout(options["delay"])

    def down(self, key):
        """Hold an explicit modifier for pointer-selection tests."""
        self.modifiers |= {"Alt": 1, "Control": 2, "Meta": 4, "Shift": 8}.get(key, 0)
        self.page.cdp.send("Input.dispatchKeyEvent", {"type": "keyDown", "key": key,
            "windowsVirtualKeyCode": {"Alt": 18, "Control": 17, "Meta": 91, "Shift": 16}.get(key, 0), "modifiers": self.modifiers})

    def up(self, key):
        """Release an explicit modifier."""
        self.modifiers &= ~{"Alt": 1, "Control": 2, "Meta": 4, "Shift": 8}.get(key, 0)
        self.page.cdp.send("Input.dispatchKeyEvent", {"type": "keyUp", "key": key,
            "windowsVirtualKeyCode": {"Alt": 18, "Control": 17, "Meta": 91, "Shift": 16}.get(key, 0), "modifiers": self.modifiers})


class _WebSocket:
    """A Python-owned local socket double; it can never open a network connection."""

    def __init__(self, page, identifier, url, context_id):
        """Remember its page-local browser socket ID."""
        self.page, self.identifier, self.url, self.handler = page, identifier, url, None
        self.context_id = context_id

    def on_message(self, handler):
        """Subscribe the test's client-to-server message callback."""
        self.handler = handler

    def send(self, value):
        """Deliver a server message through a genuine browser MessageEvent."""
        self._evaluate("args=>{const s=window.__pbguiSockets.get(args.id);if(s&&s.readyState===1)s._event('message',new MessageEvent('message',{data:args.data}));}",
                       {"id": self.identifier, "data": value})

    def close(self, *, code=1000, reason=""):
        """Close the synthetic socket with the tested status code."""
        self._evaluate("args=>window.__pbguiSockets.get(args.id)?.close(args.code,args.reason)",
                       {"id": self.identifier, "code": code, "reason": reason})

    def _evaluate(self, expression, value):
        """Discard messages belonging to a document that navigation destroyed."""
        try:
            self.page.evaluate(expression, value, _context=self.context_id)
        except Error as exc:
            if "Cannot find context with specified id" not in str(exc):
                raise
            self.page._sockets.pop((self.context_id, self.identifier), None)


class Clock:
    """Deterministic browser timers installed only for explicitly clocked tests."""

    def __init__(self, page):
        """Retain ownership of the page's synthetic timer queue."""
        self.page = page

    def install(self, *, time=None):
        """Install the project's locally served timer fixture for future documents."""
        source = (Path(__file__).parent / "browser_clock.js").read_text()
        self.page.add_init_script(source)
        self.page.evaluate(source)
        if time is not None:
            value = time.isoformat() if hasattr(time, "isoformat") else time
            initial = "window.__pbguiClock.setTime(" + json.dumps(value) + ");"
            self.page.add_init_script(initial)
            self.page.evaluate(initial)

    def run_for(self, value):
        """Run scheduled callbacks at their original deadlines."""
        remaining = _milliseconds(value)
        step = max(100, remaining / 500)
        while remaining > 0:
            elapsed = min(step, remaining)
            self.page.evaluate("ms=>window.__pbguiClock.advance(ms,false)", elapsed)
            self.page._drain()
            remaining -= elapsed

    def fast_forward(self, value):
        """Skip elapsed time while firing each pending periodic timer once."""
        self.page.evaluate("ms=>window.__pbguiClock.advance(ms,true)", _milliseconds(value))
        self.page._drain()

    def set_fixed_time(self, value):
        """Set the wall clock without executing pending timers."""
        value = value.isoformat() if hasattr(value, "isoformat") else value
        source = """(() => {const value=VALUE;
          if(window.__pbguiClock){window.__pbguiClock.setFixed(value);return;}
          const RealDate=window.__pbguiNativeDate||Date;window.__pbguiNativeDate=RealDate;
          const fixed=typeof value==='number'?value:RealDate.parse(value);
          window.Date=class extends RealDate {constructor(...args){super(...(args.length?args:[fixed]));}static now(){return fixed;}};
        })();""".replace("VALUE", json.dumps(value))
        self.page.add_init_script(source)
        self.page.evaluate(source)

    def pause_at(self, value):
        """Advance to an explicit wall-clock instant."""
        value = value.isoformat() if hasattr(value, "isoformat") else value
        source = "window.__pbguiClock.pause(" + json.dumps(value) + ");"
        self.page.add_init_script(source)
        self.page.evaluate(source)


def _milliseconds(value):
    """Accept test durations in milliseconds or hh:mm:ss form."""
    if isinstance(value, str):
        result = 0
        for part in value.split(":"):
            result = result * 60 + float(part)
        return result * 1000
    return value


class Expect:
    """Retry observable browser assertions without replacing application logic."""

    def __init__(self, target):
        """Remember the live locator/page and expectation polarity."""
        self.target = target

    def _check(self, predicate, message, timeout=None):
        """Fail with a bounded, useful assertion rather than a false passing fallback."""
        page = self.target if isinstance(self.target, Page) else self.target.page
        try:
            page._wait(predicate, 5000 if timeout is None else timeout)
        except TimeoutError as exc:
            raise AssertionError(message) from exc
        page._drain()

    def to_be_visible(self, *, timeout=None):
        """Require a current visible match."""
        self._check(self.target.is_visible, "Expected visible element: " + self.target.query, timeout)

    def to_be_hidden(self, *, timeout=None):
        """Require no visible matches."""
        self._check(self.target.is_hidden, "Expected hidden element: " + self.target.query, timeout)

    not_to_be_visible = to_be_hidden

    def to_be_enabled(self, *, timeout=None):
        """Require an enabled native control."""
        self._check(self.target.is_enabled, "Expected enabled control", timeout)

    def to_be_disabled(self, *, timeout=None):
        """Require a disabled native control."""
        self._check(self.target.is_disabled, "Expected disabled control", timeout)

    def to_be_checked(self, *, timeout=None):
        """Require a checked control."""
        self._check(self.target.is_checked, "Expected checked control", timeout)

    def not_to_be_checked(self, *, timeout=None):
        """Require an unchecked control."""
        self._check(lambda: not self.target.is_checked(), "Expected unchecked control", timeout)

    def to_be_focused(self, *, timeout=None):
        """Require the control to own browser focus."""
        self._check(lambda: self.target.evaluate("e=>e===e.ownerDocument.activeElement"), "Expected focused control", timeout)

    def to_have_count(self, expected, *, timeout=None):
        """Require an exact live DOM match count."""
        self._check(lambda: self.target.count() == expected, f"Expected {expected} DOM matches", timeout)

    def not_to_have_count(self, expected, *, timeout=None):
        """Reject an exact match count."""
        self._check(lambda: self.target.count() != expected, f"Unexpected {expected} DOM matches", timeout)

    def to_have_value(self, expected, *, timeout=None):
        """Require the exact control value or regex match."""
        self._check(lambda: _matches(expected, self.target.input_value()), f"Expected value {expected!r}", timeout)

    def _text(self, expected, exact, negative=False, timeout=None):
        """Compare normalized browser text, preserving list count and order."""
        def matches():
            """Read current text for each assertion attempt."""
            actual = self.target.all_text_contents()
            values = expected if isinstance(expected, list) else [expected]
            if len(actual) != len(values):
                return negative
            result = all(_matches(want, got) if hasattr(want, "search") else
                (" ".join(got.split()) == " ".join(str(want).split()) if exact else
                 " ".join(str(want).split()) in " ".join(got.split())) for want, got in zip(values, actual))
            return not result if negative else result
        self._check(matches, f"Expected text {expected!r} (negative={negative})", timeout)

    def to_have_text(self, expected, *, timeout=None, **_options):
        """Require normalized exact text."""
        self._text(expected, True, timeout=timeout)

    def to_contain_text(self, expected, *, timeout=None, **_options):
        """Require a normalized substring or regex."""
        self._text(expected, False, timeout=timeout)

    def not_to_have_text(self, expected, *, timeout=None, **_options):
        """Reject normalized exact text."""
        self._text(expected, True, True, timeout)

    def not_to_contain_text(self, expected, *, timeout=None, **_options):
        """Reject a substring or regex."""
        self._text(expected, False, True, timeout)

    def to_have_class(self, expected, *, timeout=None):
        """Require exact class text or a compiled regular expression."""
        self._check(lambda: _matches(expected, self.target.get_attribute("class") or ""), f"Expected class {expected!r}", timeout)

    def not_to_have_class(self, expected, *, timeout=None):
        """Reject exact class text or a compiled regular expression."""
        self._check(lambda: not _matches(expected, self.target.get_attribute("class") or ""), f"Unexpected class {expected!r}", timeout)

    def to_have_attribute(self, name, expected=None, *, timeout=None):
        """Require attribute presence, exact text or regex match."""
        self._check(lambda: self.target.get_attribute(name) is not None if expected is None else
                    _matches(expected, self.target.get_attribute(name) or ""), f"Expected attribute {name}={expected!r}", timeout)

    def to_have_url(self, expected, *, timeout=None):
        """Require the actual current navigation URL."""
        self._check(lambda: _matches(expected, self.target.url), f"Expected URL {expected!r}", timeout)

    def to_have_js_property(self, name, expected, *, timeout=None):
        """Require an actual browser element property value."""
        self._check(lambda: self.target.evaluate("(e,name)=>e[name]", name) == expected, f"Expected property {name}={expected!r}", timeout)

    def to_have_css(self, name, expected, *, timeout=None):
        """Read computed CSS, rather than inline declaration text."""
        self._check(lambda: _matches(expected, self.target.evaluate("(e,name)=>getComputedStyle(e).getPropertyValue(name)", name)),
                    f"Expected CSS {name}={expected!r}", timeout)

    def to_be_empty(self, *, timeout=None):
        """Require an empty input or empty rendered content."""
        self._check(lambda: self.target.evaluate("e=>'value' in e?!e.value:!e.textContent"), "Expected empty control", timeout)

    def to_be_in_viewport(self, *, timeout=None, **_options):
        """Require nonempty intersection with the actual browser viewport."""
        self._check(lambda: self.target.evaluate("e=>{const r=e.getBoundingClientRect();return r.bottom>0&&r.right>0&&r.top<innerHeight&&r.left<innerWidth;}"),
                    "Expected element in viewport", timeout)


def expect(target):
    """Create a retrying assertion for a live browser target."""
    return Expect(target)


class _Runner:
    """Context manager owning every browser launched within one test scope."""

    def __init__(self):
        """Expose one Chromium launcher without any package-install side effects."""
        self.chromium = self
        self.browsers = []

    def __enter__(self):
        """Enter an explicitly owned automation scope."""
        return self

    def __exit__(self, *_args):
        """Close all launched processes on success and on test exceptions."""
        self.stop()

    def start(self):
        """Support fixtures that register cleanup separately."""
        return self

    def launch(self, **options):
        """Launch only the locally installed native browser."""
        browser = Browser(**options)
        self.browsers.append(browser)
        return browser

    def stop(self):
        """Release every owned browser, even if one teardown fails."""
        for browser in self.browsers:
            browser.close()
        self.browsers.clear()


def sync_browser():
    """Open a Python-only browser automation scope."""
    return _Runner()
