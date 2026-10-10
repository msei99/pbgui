"""Offline regressions for fingerprint-bound PB8 metadata and helper ownership."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

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


@pytest.mark.parametrize("operation", ["default", "result_metrics", "optimize_metadata_static",
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
