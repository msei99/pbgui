"""Offline regression coverage for revision-aware PB8 list snapshots (#413)."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.websockets import WebSocketDisconnect

import pb8_config
from api import api_keys, auth, v7_instances, v8_instances as v8
from file_lock import advisory_file_lock


@pytest.fixture
def listing(tmp_path, monkeypatch):
    """Use only temporary bundles and a simulated helper behind the real loader."""
    clock = SimpleNamespace(now=100.0)
    runtime = ["runtime-a"]
    calls = []
    monkeypatch.setattr(v8, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(pb8_config, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v8, "_monitor", None)
    monkeypatch.setattr(v8, "_runtime_fingerprint", lambda: tuple(runtime))
    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda: tuple(runtime))
    monkeypatch.setattr(v8, "time", SimpleNamespace(monotonic=lambda: clock.now, time=lambda: clock.now,
                                                 strftime=time.strftime, localtime=time.localtime))
    monkeypatch.setattr(pb8_config, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(v8.pbgui_purefunc, "pb8_runtime_status", lambda: {"ready": False})
    monkeypatch.setattr(v8, "_desired_pb8_state", lambda: ({}, {}, {}))
    monkeypatch.setattr(v8, "_master_hostname", lambda: "master-test")
    monkeypatch.setattr(v8, "_user_exchange_map", lambda: {"alice": "test-exchange"})
    monkeypatch.setattr(v8, "load_pb8_config", pb8_config.load_pb8_config)
    monkeypatch.setattr(pb8_config, "_config_cache", pb8_config.OrderedDict())
    root = tmp_path / "data/run_v8"
    bundle = root / "alice"
    bundle.mkdir(parents=True)
    path = bundle / "config.json"
    path.write_text(json.dumps({"live": {"user": "alice"}, "pbgui": {"note": "first", "enabled_on": "disabled", "version": 1}}))

    def helper(operation, **kwargs):
        """Read a synthetic bundle without launching PB8 or any process."""
        assert operation == "load"
        calls.append(kwargs["config_path"])
        config = json.loads(Path(kwargs["config_path"]).read_text())
        if "coin_overrides" in config:
            override = json.loads((bundle / "BTC.json").read_text())
            config["pbgui"]["note"] = override["note"]
        return {"config": config}

    monkeypatch.setattr(pb8_config, "_call_helper", helper)
    v8._invalidate_instance_list()
    yield SimpleNamespace(path=path, root=root, clock=clock, runtime=runtime, calls=calls, helper=helper)
    v8._invalidate_instance_list()


def test_atomic_inode_replacement_bypasses_warm_inner_cache(listing):
    """Same mtime/size replacement cannot poison the outer revision cache."""
    assert pb8_config.load_pb8_config(listing.path)["pbgui"]["note"] == "first"
    inner = copy.deepcopy(pb8_config._config_cache)
    assert v8._list_instances()[0]["note"] == "first"
    original = listing.path.stat()
    replacement = listing.path.with_name("replacement.json")
    replacement.write_text(listing.path.read_text().replace('"first"', '"other"'))
    os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
    os.replace(replacement, listing.path)
    assert listing.path.stat().st_ino != original.st_ino
    assert listing.path.stat().st_size == original.st_size
    # Existing callers retain the exact default TTL behavior.
    assert pb8_config.load_pb8_config(listing.path)["pbgui"]["note"] == "first"
    listing.clock.now += 2
    assert v8._list_instances()[0]["note"] == "other"
    assert pb8_config._config_cache == inner
    count = len(listing.calls)
    listing.clock.now += 31
    assert v8._list_instances()[0]["note"] == "other"
    assert len(listing.calls) == count


@pytest.mark.parametrize("instance_count", [256, 257, 513])
def test_current_instances_survive_former_cache_limit(listing, instance_count):
    """Every current bundle stays warm across scans, TTL expiry and a single edit."""
    for index in range(instance_count - 1):
        path = listing.root / f"instance-{index:04}" / "config.json"
        path.parent.mkdir()
        path.write_text(listing.path.read_text())
    assert len(v8._list_instances()) == instance_count
    assert len(listing.calls) == instance_count

    for elapsed in (2, 31):
        listing.clock.now += elapsed
        assert len(v8._list_instances()) == instance_count
        assert len(listing.calls) == instance_count

    config = json.loads(path.read_text())
    config["pbgui"]["note"] = "only this instance changed"
    path.write_text(json.dumps(config))
    listing.clock.now += 2
    rows = v8._list_instances()
    assert sum(row["note"] == "only this instance changed" for row in rows) == 1
    assert len(listing.calls) == instance_count + 1

    path.unlink()
    path.parent.rmdir()
    listing.clock.now += 2
    assert len(v8._list_instances()) == instance_count - 1
    assert len(listing.calls) == instance_count + 1
    assert set(v8._list_configs) == {str(source) for source in listing.root.glob("*/config.json")}


@pytest.mark.parametrize("root_change", ["removed", "switched", "switched-populated"])
def test_config_cache_is_bounded_to_current_root(listing, monkeypatch, tmp_path, root_change):
    """Missing roots and root changes discard projections from the previous scope."""
    v8._list_instances()
    assert len(v8._list_configs) == 1
    if root_change == "removed":
        shutil.rmtree(listing.root)
    else:
        monkeypatch.setattr(v8, "PBGDIR", str(tmp_path / "another-root"))
        if root_change == "switched-populated":
            path = v8._run_root() / "bob" / "config.json"
            path.parent.mkdir(parents=True)
            path.write_text(listing.path.read_text())
    listing.clock.now += 2
    rows = v8._list_instances()
    if root_change == "switched-populated":
        assert [row["name"] for row in rows] == ["bob"]
        assert set(v8._list_configs) == {str(path)}
        assert len(listing.calls) == 2
    else:
        assert rows == []
        assert v8._list_configs == {}


def test_overrides_runtime_and_dynamic_state_invalidate_independently(listing, monkeypatch):
    """Override/runtime changes reload config; monitor changes only enrich cached values."""
    config = json.loads(listing.path.read_text())
    config["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}
    listing.path.write_text(json.dumps(config))
    override = listing.path.with_name("BTC.json")
    override.write_text('{"note":"override-a"}')
    assert v8._list_instances()[0]["note"] == "override-a"
    override.write_text('{"note":"override-b-longer"}')
    listing.clock.now += 2
    assert v8._list_instances()[0]["note"] == "override-b-longer"
    assert len(listing.calls) == 2
    listing.runtime[0] = "runtime-b"
    listing.clock.now += 2
    v8._list_instances()
    assert len(listing.calls) == 3
    monkeypatch.setattr(v8, "_monitor", SimpleNamespace(store=SimpleNamespace(v8_instances={"worker": [{"name": "alice", "running": True, "rv": 1}]})))
    assert v8._list_instances()[0]["running_on"] == ["worker"]
    assert len(listing.calls) == 3
    listing.path.unlink()
    listing.clock.now += 2
    assert v8._list_instances() == []
    assert str(listing.path) not in v8._list_configs


@pytest.mark.parametrize("migration", [False, True])
def test_error_cache_recovers_but_hsl_classification_is_stable(listing, monkeypatch, migration):
    """Busy errors expire without file edits; the known migration fallback stays cached."""
    count = []

    def helper(*args, **kwargs):
        """Fail only the first request using the selected existing error contract."""
        count.append(1)
        if migration:
            raise pb8_config.PB8ConfigurationError("pre-v8.6 HSL configuration requires explicit migration")
        if len(count) == 1:
            raise pb8_config.PB8RuntimeBusyError("busy")
        return listing.helper(*args, **kwargs)

    monkeypatch.setattr(pb8_config, "_call_helper", helper)
    first = v8._list_instances()[0]
    assert bool(first["load_error"]) is not migration
    listing.clock.now += 1.1
    v8._list_instances()
    assert len(count) == 1
    listing.clock.now += 2
    assert v8._list_instances()[0]["load_error"] == ""
    assert len(count) == (1 if migration else 2)


def test_single_flight_copies_and_invalidated_generation(listing, monkeypatch):
    """Concurrent sync callers share work and cannot publish an invalidated generation."""
    entered, release, claimed = threading.Event(), threading.Event(), threading.Event()
    scans = []
    original_scan = v8._scan_instances
    original_claim = v8._claim_instance_list
    claims = []

    def claim():
        """Signal when all concurrent readers have joined the flight."""
        result = original_claim()
        with v8._list_guard:
            claims.append(1)
            if len(claims) >= 6:
                claimed.set()
        return result

    def scan(*args):
        """Hold the first computed snapshot until a simulated mutation finishes."""
        rows = original_scan(*args)
        scans.append(1)
        if len(scans) == 1:
            entered.set()
            assert release.wait(5)
        return rows

    monkeypatch.setattr(v8, "_claim_instance_list", claim)
    monkeypatch.setattr(v8, "_scan_instances", scan)
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(v8._list_instances) for _ in range(6)]
        assert entered.wait(5) and claimed.wait(5)
        config = json.loads(listing.path.read_text())
        config["pbgui"]["note"] = "updated"
        listing.path.write_text(json.dumps(config))
        v8._invalidate_instance_list("alice")
        release.set()
        results = [future.result(timeout=5) for future in futures]
    assert len(scans) == 2
    assert all(rows[0]["note"] == "updated" for rows in results)
    results[0][0]["running_on"].append("fake-host")
    results[1][0]["note"] = "edited-return-value"
    assert v8.get_v8_instances(session=None)["instances"][0]["note"] == "updated"
    assert v8._list_instances()[0]["running_on"] == []


def test_many_async_readers_and_cancelled_owner_share_one_worker(listing, monkeypatch):
    """A departing socket neither cancels shared work nor occupies waiting threads."""
    entered, release = threading.Event(), threading.Event()
    original_scan = v8._scan_instances
    scans = []

    def scan(*args):
        """Block only the shared scan while browser coroutines join it."""
        scans.append(1)
        entered.set()
        assert release.wait(5)
        return original_scan(*args)

    monkeypatch.setattr(v8, "_scan_instances", scan)

    async def exercise():
        """Cancel the initiating consumer and await all surviving readers."""
        loop = asyncio.get_running_loop()
        original_executor = loop.run_in_executor
        submissions = []

        def submit(executor, function, *args):
            """Count worker submissions without replacing executor semantics."""
            submissions.append(function)
            return original_executor(executor, function, *args)

        monkeypatch.setattr(loop, "run_in_executor", submit)
        owner = asyncio.create_task(v8._list_instances_async())
        while not entered.is_set():
            await asyncio.sleep(0)
        waiters = [asyncio.create_task(v8._list_instances_async()) for _ in range(50)]
        await asyncio.sleep(0)
        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        release.set()
        rows = await asyncio.wait_for(asyncio.gather(*waiters), 5)
        assert all(row[0]["name"] == "alice" for row in rows)
        assert submissions.count(v8._finish_instance_list) == 1

    asyncio.run(exercise())
    assert len(scans) == 1 and v8._list_flight is None


def test_reader_keeps_stages_and_rejects_nested_lock_wait(listing):
    """Read locks never clean stages, and nested list calls fail before joining a flight."""
    stage = listing.root / ".pbgui-v8-stage-test"
    stage.mkdir()
    v8._list_instances()
    assert stage.is_dir()
    with advisory_file_lock(listing.root / ".write"):
        with pytest.raises(RuntimeError, match="outside the live transaction lock"):
            v8._list_instances()
    with v8._run_lock():
        assert not stage.exists()


def test_reader_waits_for_complete_double_rename(listing):
    """A reader cannot observe the interval between the two writer renames."""
    entered, release = threading.Event(), threading.Event()
    old = listing.root / ".old-alice"

    def writer():
        """Pause a realistic publication with the instance temporarily absent."""
        with advisory_file_lock(listing.root / ".write"):
            os.replace(listing.path.parent, old)
            entered.set()
            assert release.wait(5)
            os.replace(old, listing.path.parent)

    with ThreadPoolExecutor(max_workers=2) as pool:
        writing = pool.submit(writer)
        assert entered.wait(5)
        reading = pool.submit(v8._list_instances)
        release.set()
        writing.result(timeout=5)
        assert reading.result(timeout=5)[0]["name"] == "alice"


def test_backup_and_credential_projections_do_not_mutate_snapshot(listing, monkeypatch):
    """Real auxiliary callers consume isolated snapshots without polluting list state."""
    backup = listing.root.parent / "backup/v8/alice/1"
    backup.mkdir(parents=True)
    (backup / "config.json").write_text(listing.path.read_text())
    monkeypatch.setattr(v7_instances, "_load_local_instances", lambda: [])
    monkeypatch.setattr(v7_instances, "_enrich_with_vps_data", lambda rows: rows)
    projected = api_keys._get_bot_runtime()
    projected["alice"][0]["running_on"].append("fake")
    backups = v8.list_v8_backups(session=None)
    backups["backups"][0]["running_on"].append("also-fake")
    assert v8.get_v8_instances(session=None)["instances"][0]["running_on"] == []
    assert len(listing.calls) == 1


def test_websocket_initial_changes_and_fixed_cadence(listing, monkeypatch):
    """Push only changed data, keeping the cadence independent from serialization time."""
    frames, sleeps = [], []
    values = [[{"name": "alice", "version": version}] for version in [1, 1, 2, 2]]

    async def rows():
        """Return controlled snapshots for successive ticks."""
        return copy.deepcopy(values[len(sleeps)])

    async def sleep(delay):
        """Advance the deterministic clock and stop after four ticks."""
        sleeps.append(delay)
        listing.clock.now += delay
        if len(sleeps) == 4:
            raise asyncio.CancelledError()

    class Socket:
        """Collect application frames without a real transport."""
        async def send_json(self, message):
            frames.append(message)

        async def close(self):
            pass

    monkeypatch.setattr(v8, "_list_instances_async", rows)
    monkeypatch.setattr(v8, "asyncio", SimpleNamespace(wait_for=asyncio.wait_for, sleep=sleep, CancelledError=asyncio.CancelledError))
    asyncio.run(v8.ws_v8.__wrapped__(Socket()))
    assert [frame["data"][0]["version"] for frame in frames] == [1, 2]
    assert sleeps == [2.0] * 4


def test_websocket_send_timeout_closes_socket(listing, monkeypatch):
    """A stalled peer cannot retain a sender forever."""
    closed = []

    class Socket:
        """Simulate a blocked browser transport."""
        async def send_json(self, message):
            await asyncio.Event().wait()

        async def close(self):
            closed.append(True)

    monkeypatch.setattr(v8, "_WS_SEND_TIMEOUT_SECONDS", 0.01)
    asyncio.run(v8.ws_v8.__wrapped__(Socket()))
    assert closed == [True]


def test_authenticated_socket_flood_disconnect_and_reconnect(listing, monkeypatch):
    """The real wrapper consumes texts without triggering scans and cleans auth ownership."""
    checks = []
    original_list = v8._list_instances_async

    async def checked_list():
        """Count all list requests, even those satisfied by a warm snapshot."""
        checks.append(1)
        return await original_list()

    monkeypatch.setattr(v8, "_list_instances_async", checked_list)
    async def exercise():
        """Run two connections, including an immediate reconnect with an initial frame."""
        frames = []

        class Socket:
            """Disconnect after draining incoming text flood following the initial frame."""
            def __init__(self):
                self.sent = asyncio.Event()
                self.received = 0

            async def receive(self):
                await self.sent.wait()
                self.received += 1
                if self.received > 100:
                    return {"type": "websocket.disconnect"}
                await asyncio.sleep(0)
                return {"type": "websocket.receive", "text": "ping"}

            async def send_json(self, message):
                frames.append(message)
                self.sent.set()

            async def close(self):
                pass

        async def authenticate(socket):
            """Install a synthetic watchdog in the real wrapper registry."""
            auth._websocket_sessions.setdefault("test-session", set()).add(socket)
            auth._websocket_watchdogs[socket] = asyncio.create_task(asyncio.Event().wait())
            return SimpleNamespace(token="test-session")

        monkeypatch.setattr(auth, "authenticate_websocket", authenticate)
        for _ in range(2):
            socket = Socket()
            await asyncio.wait_for(v8.ws_v8(socket), 5)
            assert socket.received == 101
            assert socket not in auth._websocket_watchdogs
        assert len(frames) == 2
        assert "test-session" not in auth._websocket_sessions

    asyncio.run(exercise())
    assert len(listing.calls) == 1
    assert len(checks) == 2


def test_runtime_change_during_scan_discards_old_revision(listing, monkeypatch):
    """A helper evaluated across a runtime update cannot seed a lasting old entry."""
    def helper(*args, **kwargs):
        """Simulate a concurrent runtime switch during the first helper invocation."""
        if not listing.calls:
            listing.runtime[0] = "runtime-b"
        return listing.helper(*args, **kwargs)

    monkeypatch.setattr(pb8_config, "_call_helper", helper)
    assert v8._list_instances()[0]["note"] == "first"
    assert len(listing.calls) == 2
    assert v8._list_configs[str(listing.path)][0][1] == ("runtime-b",)


def test_real_auth_watchdog_stops_v8_socket_on_revocation(listing, monkeypatch):
    """Exercise the actual authenticator/watchdog with an isolated synthetic session."""
    from starlette.websockets import WebSocketState

    revoked = []
    monkeypatch.setattr(auth, "require_same_origin", lambda _socket: None)
    monkeypatch.setattr(auth, "_request_session_cookie_name", lambda _socket: auth.SESSION_COOKIE_NAME)
    monkeypatch.setattr(auth, "validate_token", lambda _token: None if revoked else SimpleNamespace(token="isolated-session"))
    monkeypatch.setattr(auth, "_websocket_sessions", {})
    monkeypatch.setattr(auth, "_websocket_watchdogs", {})

    async def exercise():
        """Revoke after the first frame and wait for code 4001 and deterministic cleanup."""
        class Socket:
            """Minimal transport retaining the real auth lifecycle contract."""
            cookies = {auth.SESSION_COOKIE_NAME: "isolated-session"}
            client_state = WebSocketState.CONNECTING

            def __init__(self):
                self.sent = asyncio.Event()
                self.codes = []

            async def accept(self):
                self.client_state = WebSocketState.CONNECTED

            async def send_json(self, message):
                self.sent.set()

            async def receive(self):
                await asyncio.Event().wait()

            async def close(self, code=1000, reason=""):
                if self.client_state != WebSocketState.CONNECTED:
                    raise RuntimeError("already closed")
                self.codes.append(code)
                self.client_state = WebSocketState.DISCONNECTED

        socket = Socket()
        task = asyncio.create_task(v8.ws_v8(socket))
        await asyncio.wait_for(socket.sent.wait(), 5)
        revoked.append(True)
        await asyncio.wait_for(task, 3)
        assert socket.codes == [4001]
        assert auth._websocket_sessions == auth._websocket_watchdogs == {}

    asyncio.run(exercise())


def test_executor_rejection_releases_flight(listing, monkeypatch):
    """An executor closing during a request must not strand later synchronous readers."""
    async def exercise():
        """Reject one worker submission without changing shared executor state."""
        loop = asyncio.get_running_loop()

        def reject(*_args):
            """Simulate an already-closing event-loop executor."""
            raise RuntimeError("executor stopped")

        monkeypatch.setattr(loop, "run_in_executor", reject)
        with pytest.raises(RuntimeError, match="executor stopped"):
            await v8._list_instances_async()
        assert v8._list_flight is None

    asyncio.run(exercise())
    assert v8._list_instances()[0]["name"] == "alice"


def test_nested_reader_fails_before_waiting_on_writer_blocked_flight(listing, monkeypatch):
    """A lock-owning caller cannot deadlock against an already waiting shared scan."""
    entered = threading.Event()
    original_lock = v8._list_read_lock
    from contextlib import contextmanager

    @contextmanager
    def read_lock():
        """Signal before attempting to acquire the currently held write lock."""
        entered.set()
        with original_lock():
            yield

    monkeypatch.setattr(v8, "_list_read_lock", read_lock)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with advisory_file_lock(listing.root / ".write"):
            waiting = pool.submit(v8._list_instances)
            assert entered.wait(5)
            with pytest.raises(RuntimeError, match="outside the live transaction lock"):
                v8._list_instances()
        assert waiting.result(timeout=5)[0]["name"] == "alice"
