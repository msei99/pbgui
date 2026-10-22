"""Offline regressions for fingerprint-bound PB8 metadata and helper ownership."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import threading
from types import ModuleType, SimpleNamespace

import pytest

import pb8_config as client
import pb8_config_helper as helper
from master_update_lock import acquire_master_update_lock

_REAL_FINGERPRINT = client._runtime_fingerprint


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    """Isolate all cache/process state and never inspect the user's installation."""
    for name in ("_template_cache", "_result_metrics_cache", "_optimize_metadata_cache",
                 "_exchange_metadata_cache", "_gpu_metadata_cache"):
        monkeypatch.setattr(client, name, None)
    for name in ("_coin_override_metadata_cache", "_market_catalog_cache", "_config_cache",
                 "_fingerprint_scans"):
        monkeypatch.setattr(client, name, client.OrderedDict())
    monkeypatch.setattr(client, "_metadata_flights", {})
    monkeypatch.setattr(client, "_migration_helper_shutdown", threading.Event())
    monkeypatch.setattr(client, "PBGDIR", tmp_path)
    monkeypatch.setattr(client, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(client, "_runtime_fingerprint", lambda *_args: ("runtime",))
    yield monkeypatch
    assert not client._metadata_flights


@pytest.mark.parametrize("name", ["template", "metrics", "optimize", "override", "exchange"])
def test_static_metadata_remains_warm_after_31_seconds(isolated, name):
    """Only GPU capabilities expire; immutable metadata never starts cold again."""
    clock = [0.0]
    isolated.setattr(client.time, "monotonic", lambda: clock[0])
    calls = []
    results = {
        "default": {"config": {"value": []}}, "result_metrics": {"metrics": ["adg"]},
        "optimize_metadata_static": {"template": {}, "strategies": []},
        "optimizer_backend_contract": {"contract_version": 1, "items": {}},
        "coin_override_metadata": {"contract_version": 1, "params": {}},
        "exchange_metadata": {"contract_version": 1, **{key: ["binance"] for key in
                                ("live", "backtest", "optimize", "suite")}},
    }
    isolated.setattr(client, "_call_helper", lambda operation, **kwargs:
                     calls.append(operation) or results[operation])
    getters = {
        "template": client.get_pb8_template_config, "metrics": client.get_pb8_result_metrics,
        "optimize": client.get_pb8_optimize_metadata,
        "override": lambda: client.get_pb8_coin_override_metadata("coin", "grid"),
        "exchange": client.get_pb8_exchange_metadata,
    }
    first = getters[name]()
    original = json.loads(json.dumps(first))
    first.clear()
    clock[0] = 31.0
    assert getters[name]() == original
    assert len(calls) == (2 if name == "optimize" else 1)
    if name == "optimize":
        clock[0] = 61.0
        assert getters[name]() == original
        assert calls == ["optimize_metadata_static", "optimizer_backend_contract",
                         "optimizer_backend_contract"]


@pytest.mark.parametrize("include_backend_contract", [False, True])
def test_optimize_warm_reads_use_two_fingerprints(isolated, include_backend_contract):
    """Both paths reuse the entry snapshot and still verify the final runtime."""
    fingerprints, calls = [], []
    isolated.setattr(client, "_runtime_fingerprint", lambda:
                     fingerprints.append(True) or ("runtime",))
    def load(operation):
        """Return isolated static data or host capabilities."""
        calls.append(operation)
        if operation == "optimize_metadata_static":
            return {"template": {"bot": []}, "strategies": ["grid"]}
        return {"contract_version": 1, "items": {"gpu": {"available": True}}}
    isolated.setattr(client, "_call_helper", load)
    original = client.get_pb8_optimize_metadata(include_backend_contract=include_backend_contract)
    expected = json.loads(json.dumps(original))
    original["template"]["bot"].append("mutation")
    if include_backend_contract:
        original["backend_contract"]["items"].clear()
    fingerprints.clear()
    calls.clear()
    assert client.get_pb8_optimize_metadata(
        include_backend_contract=include_backend_contract) == expected
    assert len(fingerprints) == 2
    assert calls == []


def test_static_optimize_skips_expired_gpu_probe(isolated):
    """Static consumers never touch capabilities, even after the full path expires."""
    clock, calls = [0.0], []
    isolated.setattr(client.time, "monotonic", lambda: clock[0])
    def load(operation):
        """Record probes independently from the immutable contract."""
        calls.append(operation)
        if operation == "optimize_metadata_static":
            return {"template": {}, "strategies": []}
        return {"contract_version": 1, "items": {}}
    isolated.setattr(client, "_call_helper", load)
    full = client.get_pb8_optimize_metadata()
    clock[0] = client._GPU_CACHE_TTL_SECONDS + 1
    calls.clear()
    static = client.get_pb8_optimize_metadata(include_backend_contract=False)
    assert static == {key: value for key, value in full.items() if key != "backend_contract"}
    assert calls == []
    assert client.get_pb8_optimize_metadata() == full
    assert calls == ["optimizer_backend_contract"]


def test_fingerprint_hint_is_consumed_before_singleflight_wait(isolated):
    """A waiter rechecks generation on its second lookup instead of reusing a hint."""
    checks = []
    isolated.setattr(client, "_runtime_fingerprint", lambda:
                     checks.append(True) or ("new",))
    class Flight:
        """Complete another owner's work while this caller waits."""
        def wait(self, timeout):
            """Publish only a new-generation cache and release its flight."""
            assert timeout == 0.1
            assert checks == []
            client._template_cache = (float("inf"), ("new",), {"generation": "new"})
            client._metadata_flights.clear()
    client._metadata_flights[("_template_cache", None)] = Flight()
    assert client._cached_call("_template_cache", lambda: pytest.fail("unexpected load"),
                               fingerprint=("old",)) == {"generation": "new"}
    assert len(checks) == 1


def test_empty_fingerprint_hint_is_valid(isolated):
    """Only None means absent; a supplied tuple is used without a fresh probe."""
    client._template_cache = (float("inf"), (), {"ok": True})
    isolated.setattr(client, "_runtime_fingerprint", lambda: pytest.fail("unused hint"))
    assert client._cached_call("_template_cache", lambda: pytest.fail("warm cache"),
                               fingerprint=()) == {"ok": True}


def test_cache_miss_rechecks_fingerprint_before_loading(isolated):
    """A stale hint cannot label newly loaded data with an obsolete generation."""
    checks = []
    isolated.setattr(client, "_runtime_fingerprint", lambda:
                     checks.append(True) or ("new",))
    assert client._cached_call("_template_cache", lambda: {"generation": "new"},
                               fingerprint=("old",)) == {"generation": "new"}
    assert len(checks) == 2
    assert client._template_cache[1] == ("new",)


@pytest.mark.parametrize("include_backend_contract", [False, True])
@pytest.mark.parametrize("repeat", [False, True])
def test_optimize_runtime_change_after_static_read(isolated, include_backend_contract, repeat):
    """An outer retry catches swaps between component reads or before static return."""
    generation, calls, changes = [0], [], []
    isolated.setattr(client, "_runtime_fingerprint", lambda: tuple(generation))
    def load(operation):
        """Include a generation marker to detect mixed component results."""
        calls.append((operation, generation[0]))
        if operation == "optimize_metadata_static":
            return {"template": {"generation": generation[0]}, "strategies": []}
        return {"contract_version": 1, "items": {"generation": generation[0]}}
    isolated.setattr(client, "_call_helper", load)
    original = client._cached_call
    def read(cache_name, loader, **kwargs):
        """Swap immediately after the static component passes its own checks."""
        result = original(cache_name, loader, **kwargs)
        if cache_name == "_optimize_metadata_cache" and (repeat or not changes):
            generation[0] += 1
            changes.append(True)
        return result
    isolated.setattr(client, "_cached_call", read)
    if repeat:
        with pytest.raises(client.PB8RuntimeBusyError, match="changed repeatedly"):
            client.get_pb8_optimize_metadata(include_backend_contract=include_backend_contract)
    else:
        result = client.get_pb8_optimize_metadata(include_backend_contract=include_backend_contract)
        assert result["template"] == {"generation": 1}
        if include_backend_contract:
            assert result["backend_contract"]["items"] == {"generation": 1}
    assert [value for op, value in calls if op == "optimize_metadata_static"] == [0, 1]


@pytest.mark.parametrize("include_backend_contract", [False, True])
@pytest.mark.parametrize("repeat", [False, True])
def test_optimize_runtime_change_during_static_load(isolated, include_backend_contract, repeat):
    """A hint never bypasses load-time swap detection on either optimizer path."""
    generation, loads = [0], []
    isolated.setattr(client, "_runtime_fingerprint", lambda: tuple(generation))
    def load(operation):
        """Change runtime inside the helper call, before cache publication."""
        if operation == "optimizer_backend_contract":
            return {"contract_version": 1, "items": {}}
        value = generation[0]
        loads.append(value)
        if repeat or len(loads) == 1:
            generation[0] += 1
        return {"template": {"generation": value}, "strategies": []}
    isolated.setattr(client, "_call_helper", load)
    if repeat:
        with pytest.raises(client.PB8RuntimeBusyError, match="changed repeatedly"):
            client.get_pb8_optimize_metadata(include_backend_contract=include_backend_contract)
        assert client._optimize_metadata_cache is None
    else:
        assert client.get_pb8_optimize_metadata(
            include_backend_contract=include_backend_contract)["template"] == {"generation": 1}


@pytest.mark.parametrize("failure", ["timeout", "eof", "invalid_json"])
def test_helper_failures_reap_resources_and_release_flight(isolated, failure):
    """Failure paths retain the timeout and terminate/kill/wait/join cleanup order."""
    import io
    import queue
    import subprocess

    events = []
    class Process:
        """Process double that needs escalation after terminate."""
        stdin = io.StringIO()
        stdout = io.StringIO()
        def poll(self):
            """Remain alive until cleanup explicitly waits."""
            return None
        def terminate(self):
            """Record the graceful stop."""
            events.append("terminate")
        def wait(self, timeout):
            """Force the first wait to time out and require reaping after kill."""
            assert timeout == 2
            events.append("wait")
            if events.count("wait") == 1:
                raise subprocess.TimeoutExpired("isolated-helper", timeout)
        def kill(self):
            """Record escalation."""
            events.append("kill")
    class Reader:
        """Reader double recording a bounded join."""
        def is_alive(self):
            """Require cleanup to join the reader."""
            return True
        def join(self, timeout):
            """Record deterministic reader cleanup."""
            assert timeout == 2
            events.append("join")
    class Responses:
        """Return the selected transport failure without waiting in real time."""
        def get(self, timeout):
            """Preserve the production response timeout."""
            assert timeout == 120
            if failure == "timeout":
                raise queue.Empty
            return None if failure == "eof" else "{invalid"
    proc = Process()
    isolated.setattr(client, "_runtime", lambda: {"pb8dir": str(client.PBGDIR)})
    isolated.setattr(client, "_ensure_migration_helper_locked", lambda *_args: None)
    isolated.setattr(client, "_migration_helper_process", proc)
    isolated.setattr(client, "_migration_helper_reader_thread", Reader())
    isolated.setattr(client, "_migration_helper_fingerprint", ("runtime",))
    isolated.setattr(client, "_migration_helper_responses", Responses())
    with pytest.raises(client.PB8ConfigurationError):
        client.get_pb8_optimize_metadata(include_backend_contract=False)
    assert events == ["terminate", "wait", "kill", "wait", "join"]
    assert proc.stdin.closed and proc.stdout.closed
    assert client._optimize_metadata_cache is None
    assert not client._metadata_flights
    for name in ("_migration_helper_process", "_migration_helper_reader_thread",
                 "_migration_helper_fingerprint", "_migration_helper_responses"):
        assert getattr(client, name) is None
    client.shutdown_pb8_migration_helper()
    assert events == ["terminate", "wait", "kill", "wait", "join"]


def test_single_flight_and_warm_reads_during_blocked_helper(isolated):
    """Identical misses load once and a warm unrelated cache bypasses helper I/O."""
    entered, release = threading.Event(), threading.Event()
    calls = []
    def load(operation, **kwargs):
        calls.append(operation)
        if operation == "default":
            entered.set()
            assert release.wait(3)
            return {"config": {"ok": True}}
        return {"metrics": ["adg"]}
    isolated.setattr(client, "_call_helper", load)
    client.get_pb8_result_metrics()
    with ThreadPoolExecutor(max_workers=4) as pool:
        first = pool.submit(client.get_pb8_template_config)
        assert entered.wait(2)
        second = pool.submit(client.get_pb8_template_config)
        warm = pool.submit(client.get_pb8_result_metrics)
        try:
            assert warm.result(timeout=2) == ["adg"]
        finally:
            release.set()
        assert first.result(timeout=2) == second.result(timeout=2) == {"ok": True}
    assert calls.count("default") == 1


@pytest.mark.parametrize("repeat", [False, True])
def test_runtime_change_during_load_retries_once_or_fails(isolated, repeat):
    """Never publish a load spanning a detected runtime generation change."""
    generation = [0]
    calls = []
    isolated.setattr(client, "_runtime_fingerprint", lambda *_args: tuple(generation))
    def load(operation, **kwargs):
        calls.append(operation)
        value = generation[0]
        if repeat or len(calls) == 1:
            generation[0] += 1
        return {"config": {"generation": value}}
    isolated.setattr(client, "_call_helper", load)
    if repeat:
        with pytest.raises(client.PB8RuntimeBusyError, match="changed repeatedly"):
            client.get_pb8_template_config()
        assert client._template_cache is None
    else:
        assert client.get_pb8_template_config() == {"generation": 1}
    assert len(calls) == 2


def test_single_flight_failure_releases_waiters(isolated):
    """Failed results are not cached and waiters can retry without a leaked flight."""
    entered, release = threading.Event(), threading.Event()
    calls = []
    def load(operation, **kwargs):
        calls.append(operation)
        if len(calls) == 1:
            entered.set()
            assert release.wait(3)
            raise client.PB8ConfigurationError("failed")
        return {"config": {"ok": True}}
    isolated.setattr(client, "_call_helper", load)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(client.get_pb8_template_config)
        assert entered.wait(2)
        second = pool.submit(client.get_pb8_template_config)
        release.set()
        with pytest.raises(client.PB8ConfigurationError, match="failed"):
            first.result(timeout=2)
        assert second.result(timeout=2) == {"ok": True}


def test_flight_registry_bounded_and_context_cache_lru(isolated):
    """Adversarial context diversity cannot retain unlimited locks or metadata."""
    isolated.setattr(client, "_METADATA_MAX_FLIGHTS", 1)
    client._metadata_flights[("other", None)] = threading.Event()
    try:
        with pytest.raises(client.PB8RuntimeBusyError, match="busy"):
            client.get_pb8_template_config()
    finally:
        client._metadata_flights.clear()
    isolated.setattr(client, "_call_helper", lambda *_args, **kwargs:
                     {"contract_version": 1, "params": kwargs})
    for index in range(20):
        client.get_pb8_coin_override_metadata("coin", str(index))
    assert len(client._coin_override_metadata_cache) == 16
    assert ("coin", "0") not in client._coin_override_metadata_cache


@pytest.mark.parametrize("layout", ["loose", "packed", "detached", "worktree", "submodule"])
def test_git_revision_layouts(isolated, tmp_path, layout):
    """Resolve Git metadata without invoking Git or depending on this checkout."""
    repo = tmp_path / "repo"
    repo.mkdir()
    common = tmp_path / "common"
    common.mkdir()
    gitdir = repo / ".git"
    if layout in {"worktree", "submodule"}:
        gitdir = tmp_path / "linked"
        gitdir.mkdir()
        (repo / ".git").write_text("gitdir: ../linked\n")
        if layout == "worktree":
            (gitdir / "commondir").write_text("../common\n")
        else:
            common = gitdir
    else:
        gitdir.mkdir()
        common = gitdir
    (gitdir / "HEAD").write_text("abc123\n" if layout == "detached" else "ref: refs/heads/main\n")
    if layout in {"packed", "worktree"}:
        (common / "packed-refs").write_text("# refs\nabc123 refs/heads/main\n")
    else:
        (common / "refs" / "heads").mkdir(parents=True)
        (common / "refs" / "heads" / "main").write_text("abc123\n")
    assert client._git_revision(repo) == "abc123"


def test_source_and_environment_signature_debounce(isolated, tmp_path):
    """Recognize source additions/deletions/edits and PB8 dependency changes."""
    source = tmp_path / "src"
    source.mkdir()
    path = source / "utils.py"
    path.write_text("one")
    ignored = source / "__pycache__"
    ignored.mkdir()
    (ignored / "ignore.py").write_text("ignored")
    environment = tmp_path / "venv"
    packages = environment / "lib" / "python3.12" / "site-packages"
    package = packages / "ccxt" / "async_support"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("exchanges=[]")
    dist = packages / "ccxt-1.0.dist-info"
    dist.mkdir()
    (dist / "METADATA").write_text("Version: 1.0")
    clock = [0.0]
    isolated.setattr(client.time, "monotonic", lambda: clock[0])
    def signature():
        return client._runtime_source_signature(tmp_path, str(environment / "bin" / "python"), (0, 0))
    previous = signature()
    assert not any("ignore.py" in entry[0] for entry in previous)
    path.write_text("two different")
    assert signature() == previous
    mutations = [lambda: None, lambda: path.write_text("third edit larger"),
                 lambda: (source / "extra.py").write_text("extra"),
                 lambda: (source / "extra.py").unlink(),
                 lambda: (package / "__init__.py").write_text("exchanges=['new']"),
                 lambda: dist.rename(packages / "ccxt-2.0.dist-info")]
    for mutate in mutations:
        mutate()
        clock[0] += 2
        current = signature()
        assert current != previous
        previous = current


def test_update_writer_signal_bypasses_scan_debounce(isolated, tmp_path):
    """Known update acquisition invalidates the cached scan immediately."""
    (tmp_path / "src").mkdir()
    path = tmp_path / "src" / "schema.py"
    path.write_text("before")
    first = client._runtime_source_signature(tmp_path, "", (1, 1))
    path.write_text("after update")
    assert client._runtime_source_signature(tmp_path, "", (2, 1)) != first


@pytest.mark.parametrize("operation", ["default", "result_metrics", "optimize_metadata",
                                      "optimize_preflight", "optimize_metadata_static",
                                      "optimizer_backend_contract", "coin_override_metadata",
                                      "exchange_metadata"])
def test_metadata_operations_use_persistent_helper(isolated, operation):
    """Metadata must not fall back to a cold subprocess, including on errors."""
    calls = []
    isolated.setattr(client, "_call_migration_helper", lambda name, **kwargs:
                     calls.append(name) or {"ok": True})
    isolated.setattr(client.subprocess, "run", lambda *_args, **_kwargs:
                     pytest.fail("cold interpreter"))
    assert client._call_helper(operation) == {"ok": True}
    assert calls == [operation]


def test_market_cache_still_expires_and_uses_cold_operation(isolated):
    """Dynamic symbols retain the 30-second TTL and the isolated market helper."""
    clock, calls = [0.0], []
    isolated.setattr(client.time, "monotonic", lambda: clock[0])
    isolated.setattr(client, "_call_helper", lambda operation, **kwargs:
                     calls.append(operation) or {"contract_version": 1, "symbols": [],
                                                "catalog": [], "statuses": {}})
    client.get_pb8_market_identifiers(["binance"])
    clock[0] = 29
    client.get_pb8_market_identifiers(["binance"])
    clock[0] = 31
    client.get_pb8_market_identifiers(["binance"])
    assert calls == ["market_identifiers", "market_identifiers"]


def test_update_writer_still_blocks_persistent_metadata(isolated):
    """A real exclusive lock must produce the existing retryable runtime error."""
    isolated.setattr(client.subprocess, "Popen", lambda *_args, **_kwargs:
                     pytest.fail("must not spawn under writer"))
    with acquire_master_update_lock(Path(client.PBGDIR)):
        with pytest.raises(client.PB8RuntimeBusyError):
            client.get_pb8_template_config()
    assert client._template_cache is None


def test_helper_static_cache_does_not_freeze_gpu_contract(isolated, tmp_path):
    """Persistent helper keeps static data but recomputes its dynamic contract."""
    isolated.setattr(helper, "_OPTIMIZE_METADATA_CACHE", {})
    builds, probes = [], []
    modules = {"backends": ["gpu"]}
    isolated.setattr(helper, "_load_pb8_modules", lambda *_args: modules)
    def static(_modules, **kwargs):
        assert kwargs == {"include_backend": False}
        builds.append(True)
        return {"template": {}, "strategies": [], "scoring": {"metrics": ["adg"]}}
    isolated.setattr(helper, "_optimize_metadata", static)
    isolated.setattr(helper, "_optimizer_backend_contract", lambda *_args:
                     probes.append(True) or {"generation": len(probes)})
    payload = {"pb8_dir": str(tmp_path), "operation": "optimize_metadata_static"}
    assert "backend_contract" not in helper.handle(payload)
    payload["operation"] = "optimizer_backend_contract"
    assert helper.handle(payload) == {"generation": 1}
    assert helper.handle(payload) == {"generation": 2}
    payload["operation"] = "optimize_metadata"
    assert helper.handle(payload)["backend_contract"] == {"generation": 3}
    assert len(builds) == 1


def test_real_helper_replaced_and_returns_new_source_metadata(isolated, tmp_path):
    """Replacing the owned process must discard helper-local cached source data."""
    source = tmp_path / "src"
    source.mkdir()
    path = source / "schema.py"
    path.write_text("old")
    program = tmp_path / "pb8_config_helper.py"
    program.write_text('''import json, sys
from pathlib import Path
value = Path("src/schema.py").read_text()
for line in sys.stdin:
    print(json.dumps({"ok": True, "result": {"config": {"source": value}}}), flush=True)
''')
    isolated.setattr(client, "__file__", str(tmp_path / "pb8_config.py"))
    for name in ("_migration_helper_process", "_migration_helper_fingerprint",
                 "_migration_helper_responses", "_migration_helper_reader_thread"):
        isolated.setattr(client, name, None)
    status = {"pb8dir": str(tmp_path), "pb8venv": sys.executable, "ready": True}
    isolated.setattr(client, "_runtime_fingerprint", _REAL_FINGERPRINT)
    isolated.setattr(client, "_FINGERPRINT_SCAN_SECONDS", 0)
    isolated.setattr(client, "pb8_runtime_status", lambda: status)
    isolated.setattr(client, "_runtime", lambda: status)
    try:
        assert client.get_pb8_template_config() == {"source": "old"}
        old = client._migration_helper_process
        path.write_text("new")
        assert client.get_pb8_template_config() == {"source": "new"}
        assert old.poll() is not None
        assert client._migration_helper_process.pid != old.pid
    finally:
        client.shutdown_pb8_migration_helper()
    assert client._migration_helper_process is None
    assert client._migration_helper_reader_thread is None


def test_interrupt_wakes_real_helper_and_metadata_waiters(isolated, tmp_path):
    """Interrupt terminates actual blocked I/O and releases all single-flight owners."""
    program = tmp_path / "pb8_config_helper.py"
    program.write_text('''import json, sys, time
from pathlib import Path
for line in sys.stdin:
    Path("entered").touch()
    time.sleep(120)
''')
    isolated.setattr(client, "__file__", str(tmp_path / "pb8_config.py"))
    for name in ("_migration_helper_process", "_migration_helper_fingerprint",
                 "_migration_helper_responses", "_migration_helper_reader_thread"):
        isolated.setattr(client, name, None)
    isolated.setattr(client, "_runtime", lambda: {"pb8dir": str(tmp_path), "pb8venv": sys.executable})
    entered = threading.Event()
    original = client._ensure_migration_helper_locked
    def ensure(status):
        original(status)
        entered.set()
    isolated.setattr(client, "_ensure_migration_helper_locked", ensure)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(client.get_pb8_template_config)
            assert entered.wait(2)
            second = pool.submit(client.get_pb8_template_config)
            client.interrupt_pb8_migration_helper()
            for request in (first, second):
                with pytest.raises(client.PB8ConfigurationError):
                    request.result(timeout=3)
    finally:
        client.shutdown_pb8_migration_helper()
    assert client._template_cache is None


def test_helper_timeout_reaps_process_and_releases_flight(isolated):
    """An expired persistent response wait must reset owned state and permit retry."""
    import io
    import queue
    stopped = []
    isolated.setattr(client, "_runtime", lambda: {"pb8dir": str(client.PBGDIR)})
    isolated.setattr(client, "_ensure_migration_helper_locked", lambda *_args: None)
    isolated.setattr(client, "_migration_helper_process", SimpleNamespace(stdin=io.StringIO()))
    class Expired:
        """Queue stand-in that expires immediately without a 120-second test wait."""
        def get(self, timeout):
            """Require the production timeout, then simulate its expiry."""
            assert timeout == 120
            raise queue.Empty
    isolated.setattr(client, "_migration_helper_responses", Expired())
    isolated.setattr(client, "_stop_migration_helper_locked", lambda: stopped.append(True))
    with pytest.raises(client.PB8ConfigurationError, match="timed out"):
        client.get_pb8_template_config()
    assert stopped == [True]
    assert client._template_cache is None


def test_warm_cache_bypasses_shared_helper_request_lock(isolated):
    """A cold miss waits behind prepare while an already warm cache stays readable."""
    blocked = threading.Event()
    client._result_metrics_cache = (float("inf"), ("runtime",), ["adg"])
    def persistent(operation, **kwargs):
        blocked.set()
        with client._migration_helper_lock:
            return {"config": {"ok": True}}
    isolated.setattr(client, "_call_migration_helper", persistent)
    pool = ThreadPoolExecutor(max_workers=2)
    try:
        with client._migration_helper_lock:
            cold = pool.submit(client.get_pb8_template_config)
            assert blocked.wait(2)
            warm = pool.submit(client.get_pb8_result_metrics)
            assert warm.result(timeout=2) == ["adg"]
            assert not cold.done()
        assert cold.result(timeout=2) == {"ok": True}
    finally:
        pool.shutdown(wait=True)


def test_file_load_does_not_hold_metadata_cache_lock(isolated, tmp_path):
    """Prepared config file-cache policy stays bounded without blocking warm metadata."""
    path = tmp_path / "config.json"
    path.write_text("{}")
    blocked, release = threading.Event(), threading.Event()
    client._result_metrics_cache = (float("inf"), ("runtime",), ["adg"])
    def load(operation, **kwargs):
        assert operation == "load"
        blocked.set()
        assert release.wait(3)
        return {"config": {"ok": True}}
    isolated.setattr(client, "_call_helper", load)
    with ThreadPoolExecutor(max_workers=2) as pool:
        cold = pool.submit(client.load_pb8_config, path)
        assert blocked.wait(2)
        warm = pool.submit(client.get_pb8_result_metrics)
        try:
            assert warm.result(timeout=2) == ["adg"]
        finally:
            release.set()
        assert cold.result(timeout=2) == {"ok": True}


@pytest.mark.parametrize("operation", ["market_identifiers", "optimizer_warmup", "status"])
def test_network_and_warmup_operations_remain_cold(isolated, operation):
    """Unrelated Vast warmups and network catalog requests never enter the shared helper."""
    isolated.setattr(client, "_runtime", lambda:
                     {"pb8dir": str(client.PBGDIR), "pb8venv": "/isolated/python"})
    isolated.setattr(client, "_call_migration_helper", lambda *_args, **_kwargs:
                     pytest.fail("must remain isolated"))
    requests = []
    def run(argv, **kwargs):
        requests.append(json.loads(kwargs["input"])["operation"])
        return SimpleNamespace(returncode=0, stdout='{"ok":true,"result":{"ok":true}}', stderr="")
    isolated.setattr(client.subprocess, "run", run)
    assert client._call_helper(operation) == {"ok": True}
    assert requests == [operation]


@pytest.mark.parametrize("operation,payload", [
    ("optimize_metadata", {}),
    ("optimize_preflight", {"config": {"optimize": {"backend": "gpu"}},
                            "base_config_path": "/isolated/config.json"}),
])
def test_optimizer_operations_preserve_persistent_payload(isolated, operation, payload):
    """Forward optimizer requests unchanged without spawning a cold interpreter."""
    calls = []
    isolated.setattr(client, "_call_migration_helper", lambda name, **kwargs:
                     calls.append((name, kwargs)) or {"valid": True})
    isolated.setattr(client.subprocess, "run", lambda *_args, **_kwargs:
                     pytest.fail("cold interpreter"))
    assert client._call_helper(operation, **payload) == {"valid": True}
    assert calls == [(operation, payload)]
    if "config" in payload:
        assert calls[0][1]["config"] is payload["config"]


def test_preflight_validation_error_has_no_cold_retry(isolated):
    """The public preflight client preserves persistent validation failures."""
    error = client.PB8ConfigurationError("Unsupported GPU metric")

    def reject(operation, **payload):
        """Reject the request through the persistent helper boundary."""
        assert operation == "optimize_preflight"
        raise error

    isolated.setattr(client, "_call_migration_helper", reject)
    isolated.setattr(client.subprocess, "run", lambda *_args, **_kwargs:
                     pytest.fail("cold retry"))
    with pytest.raises(client.PB8ConfigurationError) as caught:
        client.validate_pb8_optimize_preflight({"optimize": {"backend": "gpu"}})
    assert caught.value is error


def test_persistent_preflights_revalidate_after_gpu_failure(isolated, tmp_path):
    """One helper handles success, GPU failure and recovery without caching requests."""
    root = ModuleType("optimization")
    root.__path__ = []
    backend = ModuleType("optimization.backends.gpu_backend")
    package = ModuleType("optimization.backends")
    package.__path__ = []
    package.gpu_backend = backend
    gpu = ModuleType("optimization.gpu")
    gpu.__path__ = []
    metrics = ModuleType("optimization.gpu.metrics")
    gpu.metrics = metrics
    root.backends = package
    root.gpu = gpu
    suite = ModuleType("suite_runner")
    for module in (root, package, backend, gpu, metrics, suite):
        isolated.setitem(sys.modules, module.__name__, module)
    validated, materialized, requests, probes = [], [], [], []
    availability = iter([True, False, True])

    def probe(backends):
        """Return fresh device availability for each request in the same helper."""
        assert backends == ["gpu"]
        probes.append(True)
        return {"device_available": next(availability), "reason": "GPU unavailable",
                "probe": len(probes)}

    def materialize(config):
        """Exercise native mutation of the helper's private config copy."""
        materialized.append(config["optimize"]["scoring"][0]["metric"])
        config["native_prepared"] = True
        return config

    backend.materialize_gpu_preparation_config = materialize
    backend.validate_gpu_preparation_scope = lambda config, _suite: validated.append(config)
    metrics.validate_gpu_metric_names = lambda names: validated.append(list(names))
    suite.extract_suite_config = lambda config, _path: {}
    isolated.setattr(helper, "_load_pb8_modules", lambda _path: {"backends": ["gpu"]})
    isolated.setattr(helper, "_gpu_runtime_contract", probe)

    def persistent(operation, **payload):
        """Keep the real dispatcher and native validation in one fake transport."""
        requests.append((operation, json.loads(json.dumps(payload))))
        response = helper._response({"operation": operation, "pb8_dir": str(tmp_path), **payload})
        if not response["ok"]:
            raise client.PB8ConfigurationError(response["detail"])
        return response["result"]

    isolated.setattr(client, "_call_migration_helper", persistent)
    isolated.setattr(client.subprocess, "run", lambda *_args, **_kwargs:
                     pytest.fail("cold interpreter"))
    configs = [{"optimize": {"backend": "gpu", "scoring": [{"metric": metric}]}}
               for metric in ("adg", "drawdown_worst", "sharpe_ratio")]
    first = client.validate_pb8_optimize_preflight(configs[0], base_config_path="/isolated/first.json")
    assert first["valid"] is True
    first["runtime"]["device_available"] = False
    with pytest.raises(client.PB8ConfigurationError, match="GPU unavailable"):
        client.validate_pb8_optimize_preflight(configs[1], base_config_path="/isolated/second.json")
    last = client.validate_pb8_optimize_preflight(configs[2], base_config_path="/isolated/last.json")
    assert last["valid"] is True
    assert last["runtime"]["device_available"] is True
    assert last["runtime"]["probe"] == 3
    assert len(probes) == 3
    assert materialized == ["adg", "sharpe_ratio"]
    assert validated == [["adg"], {**configs[0], "native_prepared": True},
                         ["sharpe_ratio"], {**configs[2], "native_prepared": True}]
    assert requests == [("optimize_preflight", {"config": config, "base_config_path": path})
                        for config, path in zip(configs, ("/isolated/first.json", "/isolated/second.json",
                                                         "/isolated/last.json"))]
    assert all("native_prepared" not in config for config in configs)
