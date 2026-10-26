"""Offline regressions for scoped Cluster workers and their actual lifetime."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import threading
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api import cluster


@pytest.fixture
def isolated_cluster(monkeypatch, tmp_path):
    """Keep every local file and lifecycle registry isolated from runtime state."""
    monkeypatch.setattr(cluster, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(cluster, "_get_master_pbname", lambda: "test-master")
    monkeypatch.setattr(cluster, "_LOCAL_IO_WORKERS", {})
    monkeypatch.setattr(cluster, "_BACKGROUND_JOBS", {})
    monkeypatch.setattr(cluster, "_VPS_ONBOARD_LOCK", asyncio.Lock())
    monkeypatch.setattr(cluster, "_log", lambda *args, **kwargs: None)
    return {
        "identity": {"node_id": "local", "cluster_id": "test-cluster"},
        "cluster_nodes": {"nodes": {}},
        "desired_state": {"instances": {}},
    }


async def wait_event(event: threading.Event) -> None:
    """Bound waits without consuming another executor thread."""
    async with asyncio.timeout(4):
        while not event.is_set():
            await asyncio.sleep(0.001)


def _assert_ten_retention_requests_leave_heartbeat_running(
    monkeypatch, isolated_cluster, *, executor=None, blocked_workers=1,
):
    """Prove ten in-flight requests stay responsive even when some workers queue."""
    release = threading.Event()
    entered = threading.Event()
    counter_lock = threading.Lock()
    count = 0
    loop_thread = threading.get_ident()

    def snapshot():
        """Hold available workers without waiting for queued requests to start."""
        nonlocal count
        assert threading.get_ident() != loop_thread
        with counter_lock:
            count += 1
            if count == blocked_workers:
                entered.set()
        assert release.wait(5), "snapshot worker was not released"
        return isolated_cluster

    def local_result(result):
        """Assert every reached local status/checkpoint read uses a worker."""
        assert threading.get_ident() != loop_thread
        return result

    monkeypatch.setattr(cluster, "_load_cluster_snapshot", snapshot)
    monkeypatch.setattr(cluster, "build_shadow_checkpoint", lambda root: local_result({"migration_seal": {}}))
    monkeypatch.setattr(cluster, "retention_preview", lambda *args, **kwargs: local_result({"items": []}))
    monkeypatch.setattr(cluster, "retention_cleanup_status", lambda root: local_result({}))
    monkeypatch.setattr(cluster, "checkpoint_status", lambda root: local_result({"checkpoint_id": "test"}))
    monkeypatch.setattr(cluster, "retention_policy", lambda snapshot: {})

    async def run():
        """Drive concurrent requests and an independent event-loop heartbeat."""
        if executor is not None:
            asyncio.get_running_loop().set_default_executor(executor)
        tasks = [asyncio.create_task(cluster.get_retention_report(session=None)) for _ in range(10)]
        ticker = None
        ticks = []

        async def heartbeat():
            """Make progress while all ten requests are pending in local I/O."""
            for tick in range(10):
                await asyncio.sleep(0)
                ticks.append(tick)

        try:
            await wait_event(entered)
            # _cluster_local_io registers at submission, before a thread starts.
            async with asyncio.timeout(4):
                while len(cluster._LOCAL_IO_WORKERS) != 10:
                    await asyncio.sleep(0)
            ticker = asyncio.create_task(heartbeat())
            await asyncio.wait_for(ticker, 4)
            assert ticks == list(range(10))
            assert all(not task.done() for task in tasks)
            assert len(cluster._LOCAL_IO_WORKERS) == 10
            assert "get_retention_report" in cluster.restart_block_reason()
            if executor is not None:
                with counter_lock:
                    assert count == blocked_workers < len(tasks)
        finally:
            release.set()
            if ticker is not None:
                await asyncio.gather(ticker, return_exceptions=True)
            results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)
        assert len(results) == 10
        assert all(isinstance(item, dict) and item["read_only"] for item in results)
        assert count == 10
        assert not cluster._LOCAL_IO_WORKERS
        assert cluster.restart_block_reason() == ""

    asyncio.run(run())


def test_ten_retention_snapshots_leave_heartbeat_running(monkeypatch, isolated_cluster):
    """Ten concurrent requests preserve heartbeat and cleanup at any default capacity."""
    _assert_ten_retention_requests_leave_heartbeat_running(monkeypatch, isolated_cluster)


def test_ten_retention_snapshots_leave_heartbeat_running_with_small_pool(monkeypatch, isolated_cluster):
    """Four test-owned threads serve ten concurrent requests without blocking heartbeat."""
    executor = ThreadPoolExecutor(max_workers=4)
    try:
        _assert_ten_retention_requests_leave_heartbeat_running(
            monkeypatch, isolated_cluster, executor=executor, blocked_workers=4,
        )
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


@pytest.mark.parametrize("target", ["local", "remote"])
def test_v7_local_and_remote_file_steps_use_workers(monkeypatch, isolated_cluster, target):
    """Local materialization and remote push preparation leave SSH on the event loop."""
    loop_thread = threading.get_ident()
    isolated_cluster["desired_state"]["instances"]["demo"] = {"version": "3", "assigned_host": target}
    node = {"node_id": "remote", "pbname": "test-runner", "sync_mode": "reachable", "ssh_host": "192.0.2.1"}
    isolated_cluster["cluster_nodes"]["nodes"]["remote"] = node
    calls = []

    def local(name, result):
        """Record file operations only when invoked outside the event loop."""
        assert threading.get_ident() != loop_thread
        calls.append(name)
        return result

    async def remote_read(*args, **kwargs):
        """Emulate SSH state-vector reads without any network access."""
        assert threading.get_ident() == loop_thread
        return {"state_vector": {}}

    async def materialize(*args, **kwargs):
        """Preserve the existing remote materialization timeout."""
        assert threading.get_ident() == loop_thread
        assert kwargs["timeout"] == 120
        return {"ok": True, "counts": {"error": 0}}

    async def upload(*args, **kwargs):
        """Emulate successful bulk operation upload."""
        assert threading.get_ident() == loop_thread
        return SimpleNamespace(exit_status=0, stdout='{"operations": []}', stderr="")

    async def pool_run(*args, **kwargs):
        """Emulate the final remote rebuild."""
        assert threading.get_ident() == loop_thread
        assert kwargs["timeout"] == 120
        return SimpleNamespace(exit_status=0, stdout='{}', stderr="")

    monkeypatch.setattr(cluster, "_load_cluster_snapshot", lambda: local("snapshot", isolated_cluster))
    monkeypatch.setattr(cluster, "_materialize_v7_configs", lambda *a, **kw: local("materialize", {"ok": True}))
    monkeypatch.setattr(cluster, "load_operations", lambda *a, **kw: local("operations", [{"actor": "local", "seq": 1, "op": "TEST"}]))
    monkeypatch.setattr(cluster, "rebuild_materialized_state", lambda *a, **kw: local("rebuild", {"state_vector": {"local": 1}, "desired_state": {}}))
    monkeypatch.setattr(cluster, "_collect_current_config_blobs", lambda *a: local("config_blobs", ([], [])))
    monkeypatch.setattr(cluster, "_collect_current_api_key_blobs", lambda *a: local("api_blobs", ([], [], [])))
    monkeypatch.setattr(cluster, "_collect_current_sealed_blobs", lambda *a: local("sealed_blobs", ([], [])))
    monkeypatch.setattr(cluster, "_run_remote_read_command", remote_read)
    monkeypatch.setattr(cluster, "_run_remote_materialize_command", materialize)
    monkeypatch.setattr(cluster, "_run_cluster_payload_command", upload)
    monkeypatch.setattr(cluster, "_pushed_operation_summary", lambda *args: {})
    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=SimpleNamespace(run=pool_run)))
    result = asyncio.run(cluster.sync_and_materialize_v7_instance("demo", expected_version=3))
    assert result["local"] is (target == "local")
    assert result["version"] == "3"
    if target == "local":
        assert calls == ["snapshot", "materialize"]
    else:
        assert {"operations", "rebuild", "config_blobs", "api_blobs", "sealed_blobs"} <= set(calls)
    assert not cluster._LOCAL_IO_WORKERS


class JsonRequest:
    """Minimal in-memory request whose body must be consumed on the event loop."""

    def __init__(self, loop_thread, operation_id="same-operation"):
        """Retain only harmless operation metadata."""
        self.loop_thread = loop_thread
        self.operation_id = operation_id

    async def json(self):
        """Validate body ownership before returning synthetic input."""
        assert threading.get_ident() == self.loop_thread
        return {"operation_id": self.operation_id}


def test_rewrap_serializes_retry_and_entire_transaction_in_one_thread(monkeypatch, isolated_cluster):
    """A simultaneous duplicate returns the stored result without another publication."""
    loop_thread = threading.get_ident()
    release = threading.Event()
    entered = threading.Event()
    lock = cluster.credential_mutation_lock
    local = threading.local()
    calls = []
    threads = []

    @contextmanager
    def checked_lock(root):
        """Use the real reentrant cross-process lock and verify thread ownership."""
        assert threading.get_ident() != loop_thread
        with lock(root):
            owner = threading.get_ident()
            local.held = True
            try:
                yield
            finally:
                assert owner == threading.get_ident()
                local.held = False

    def stage(name, result):
        """Ensure transaction stages remain under the worker's lock."""
        assert getattr(local, "held", False)
        calls.append(name)
        threads.append(threading.get_ident())
        return result

    def publish(credential_id):
        """Pause publication so a second request overlaps the first one."""
        stage("publish", None)
        entered.set()
        assert release.wait(5)
        return {"status": "current"}

    begin = cluster._begin_credential_operation

    def begin_checked(*args):
        """Check journal acquisition as well as durable completion under the lock."""
        stage("begin", None)
        operations, completed = begin(*args)
        complete = operations.complete

        def complete_checked(*args):
            """Observe durable completion before the mutation lock is released."""
            stage("complete", None)
            return complete(*args)

        operations.complete = complete_checked
        return operations, completed

    monkeypatch.setattr(cluster, "credential_mutation_lock", checked_lock)
    monkeypatch.setattr(cluster, "_begin_credential_operation", begin_checked)
    monkeypatch.setattr(cluster, "ClusterCredentialPublisher", lambda *args: SimpleNamespace(rewrap=publish))
    monkeypatch.setattr(cluster, "_materialize_credentials", lambda *a, **kw: stage("materialize", {}))
    monkeypatch.setattr(cluster, "_load_cluster_snapshot", lambda: stage("snapshot", isolated_cluster))
    monkeypatch.setattr(cluster, "_credential_status", lambda snapshot: stage("status", {"test": True}))
    monkeypatch.setattr(cluster, "_request_pbcluster_sync", lambda root: stage("sync", None))

    async def run():
        """Release and join both workers even if an assertion fails."""
        first = asyncio.create_task(cluster.rewrap_cluster_credentials(JsonRequest(loop_thread), session=None))
        second = None
        try:
            await wait_event(entered)
            second = asyncio.create_task(cluster.rewrap_cluster_credentials(JsonRequest(loop_thread), session=None))
            async with asyncio.timeout(4):
                while len(cluster._LOCAL_IO_WORKERS) != 2:
                    await asyncio.sleep(0.001)
            assert calls == ["begin", "publish"]
        finally:
            release.set()
            results = await asyncio.wait_for(asyncio.gather(*[t for t in (first, second) if t], return_exceptions=True), 5)
        assert len(results) == 2 and results[0] == results[1]
        assert isinstance(results[0], dict) and results[0]["ok"]
        assert calls == ["begin", "publish", "materialize", "snapshot", "sync", "status", "complete", "begin"]
        assert len(set(threads[:-1])) == 1
        assert not cluster._LOCAL_IO_WORKERS

    asyncio.run(run())


@pytest.mark.parametrize("error, status", [(cluster.CredentialPublicationError("blocked"), 409), (HTTPException(422, "invalid"), 422), (RuntimeError("failure"), 500)])
def test_rewrap_worker_preserves_errors(monkeypatch, isolated_cluster, error, status):
    """Thread exceptions retain the established public HTTP error mapping."""
    def fail(*args):
        """Raise a synthetic failure before accessing any real credentials."""
        raise error

    monkeypatch.setattr(cluster, "ClusterCredentialPublisher", fail)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(cluster.rewrap_cluster_credentials(JsonRequest(threading.get_ident()), session=None))
    assert caught.value.status_code == status
    assert not cluster._LOCAL_IO_WORKERS


@pytest.mark.parametrize("fails", [False, True])
def test_cancelled_onboard_keeps_lock_restart_guard_and_shutdown_until_worker_end(monkeypatch, isolated_cluster, fails):
    """Repeated cancellation cannot release onboarding or hide a live worker."""
    entered = threading.Event()
    release = threading.Event()
    registrations = []
    loop_thread = threading.get_ident()

    def bootstrap(hostname, session=None):
        """Block a worker while a second onboarding waits for its asyncio lock."""
        assert threading.get_ident() != loop_thread
        registrations.append(hostname)
        entered.set()
        assert release.wait(5)
        if fails:
            raise RuntimeError("synthetic worker failure")
        return {"ok": True}

    monkeypatch.setattr(cluster, "apply_bootstrap_node", bootstrap)
    monkeypatch.setattr(cluster, "_load_cluster_snapshot", lambda: isolated_cluster)

    async def run():
        """Exercise request cancellation and API shutdown without remote actions."""
        first = asyncio.create_task(cluster.onboard_vps_cluster_node("test-runner"))
        second = stopping = None
        try:
            await wait_event(entered)
            first.cancel()
            await asyncio.sleep(0)
            first.cancel()
            await asyncio.sleep(0)
            second = asyncio.create_task(cluster.onboard_vps_cluster_node("other-runner"))
            await asyncio.sleep(0)
            assert not first.done()
            assert cluster._VPS_ONBOARD_LOCK.locked()
            assert "onboard_vps_cluster_node" in cluster.restart_block_reason()
            assert registrations == ["test-runner"]
            stopping = asyncio.create_task(cluster.shutdown())
            await asyncio.sleep(0)
            assert not stopping.done()
            assert cluster._LOCAL_IO_WORKERS
            second.cancel()
            await asyncio.gather(second, return_exceptions=True)
        finally:
            release.set()
            for task in (second,):
                if task and not task.done():
                    task.cancel()
            tasks = [task for task in (first, second, stopping) if task]
            results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 5)
        assert isinstance(results[0], asyncio.CancelledError)
        assert not cluster._VPS_ONBOARD_LOCK.locked()
        assert not cluster._LOCAL_IO_WORKERS
        assert cluster.restart_block_reason() == ""
        assert registrations == ["test-runner"]

    asyncio.run(run())


@pytest.mark.parametrize("configure_ssh, new_join, protocol, checkpoint", [
    (True, False, 2, False),
    (False, False, 1, False),
    (False, True, 1, False),
    (False, True, 2, True),
])
def test_onboard_reaches_local_repair_push_pull_and_join_workers(
    monkeypatch, isolated_cluster, configure_ssh, new_join, protocol, checkpoint,
):
    """Exercise real onboarding branches while mocking only local primitives and SSH."""
    loop_thread = threading.get_ident()
    calls = []
    phases = []
    local_id = "pbgui-node-00000000-0000-4000-8000-000000000001"
    remote_id = "pbgui-node-00000000-0000-4000-8000-000000000002"
    isolated_cluster["identity"]["node_id"] = local_id
    node = {
        "node_id": remote_id, "pbname": "test-runner", "role": "vps",
        "sync_mode": "outbound_only" if configure_ssh else "reachable",
        "ssh_host": "192.0.2.1", "ssh_user": "test", "remote_pbgui_dir": "/tmp/test-pbgui",
        "state_replica": False if configure_ssh else True, "sync_peers": [local_id],
    }
    isolated_cluster["cluster_nodes"]["nodes"] = {
        remote_id: node, local_id: {"node_id": local_id},
    }
    isolated_cluster["state_vector"] = {}

    def local(name, result):
        """Every reached file/key/materialization primitive must run in a worker."""
        assert threading.get_ident() != loop_thread, name
        calls.append(name)
        return result

    def append(root, op, payload):
        """Apply only synthetic in-memory node updates."""
        local("append", None)
        node.update(payload)
        return {}

    def update(node_id, current, updates):
        """Publish fake discovered SSH metadata."""
        local("ssh_metadata", None)
        current.update(updates)
        return configure_ssh

    def progress(update):
        """UI callbacks remain on the event loop throughout every branch."""
        assert threading.get_ident() == loop_thread
        phases.append(update["phase"])

    async def probe(*args):
        """Return an uninitialized first probe only for a new join."""
        assert threading.get_ident() == loop_thread
        if new_join and "probe" not in calls:
            calls.append("probe")
            return {"status": "not_initialized"}
        calls.append("probe")
        return {"status": "ok", "remote_cluster_id": "test-cluster", "remote_node_id": remote_id,
                "credential_protocol_version": protocol}

    async def ssh_setup(*args, **kwargs):
        """Emulate restricted SSH-key installation without touching a host."""
        assert threading.get_ident() == loop_thread
        return {"public_key": "ssh-ed25519 d29ybGQ= test", "fingerprint": "test", "changed": True}

    async def remote_json(*args, **kwargs):
        """Return an empty synchronized remote history, with optional checkpoint."""
        assert threading.get_ident() == loop_thread
        return {"cluster_id": "test-cluster", "node_id": remote_id, "state_vector": {},
                "active": checkpoint, "checkpoint_id": "test-checkpoint"}

    async def remote_read(*args, **kwargs):
        """Read the remote vector before pushing the empty local delta."""
        assert threading.get_ident() == loop_thread
        return {"state_vector": {}}

    async def pool_run(*args, **kwargs):
        """Emulate either a legacy join command or remote rebuild."""
        assert threading.get_ident() == loop_thread
        return SimpleNamespace(exit_status=0, stdout='{"cluster_id":"test-cluster","node_id":"' + remote_id + '"}', stderr="")

    async def payload_command(*args, **kwargs):
        """Checkpoint joins retain their 60-second SSH timeout."""
        assert threading.get_ident() == loop_thread
        assert args[4] == "join-checkpoint"
        assert kwargs["timeout"] == 60
        calls.append("join_checkpoint")
        return await pool_run()

    async def materialize(*args, **kwargs):
        """Emulate previews and successful writes without invoking any real bot."""
        assert threading.get_ident() == loop_thread
        return {"ok": True, "can_apply": True, "counts": {"error": 0}}

    async def pbrun(*args, **kwargs):
        """All PBRun actions are mocked; no process is started or stopped."""
        assert threading.get_ident() == loop_thread
        return {"ok": True}

    monkeypatch.setattr(cluster, "apply_bootstrap_node", lambda *a, **kw: local("bootstrap", {"ok": True}))
    monkeypatch.setattr(cluster, "_load_cluster_snapshot", lambda: local("snapshot", isolated_cluster))
    monkeypatch.setattr(cluster, "append_operation", append)
    monkeypatch.setattr(cluster, "rebuild_materialized_state", lambda *a, **kw: local("rebuild", isolated_cluster))
    monkeypatch.setattr(cluster, "ensure_local_cluster_ssh_material", lambda *a, **kw: local("ssh_material", {"public_key": "test"}))
    monkeypatch.setattr(cluster, "_append_node_update_if_changed", update)
    monkeypatch.setattr(cluster, "install_authorized_cluster_key", lambda **kw: local("authorized_key", {"changed": True}))
    monkeypatch.setattr(cluster, "_run_remote_cluster_ssh_setup", ssh_setup)
    monkeypatch.setattr(cluster, "_probe_cluster_node", probe)
    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=SimpleNamespace(run=pool_run)))
    monkeypatch.setattr(cluster, "_run_cluster_json_command_on_host", remote_json)
    monkeypatch.setattr(cluster, "_run_remote_read_command", remote_read)
    monkeypatch.setattr(cluster, "load_operations", lambda *a, **kw: local("operations", []))
    monkeypatch.setattr(cluster, "_run_remote_materialize_command", materialize)
    monkeypatch.setattr(cluster, "_run_remote_pbrun_service", pbrun)
    monkeypatch.setattr(cluster, "_rewrap_credentials_for_current_membership", lambda: local("rewrap", {}))
    monkeypatch.setattr(cluster, "credential_lifecycle_status", lambda state: {"nodes": {remote_id: {"materialization_ack": {"current": True}}}})
    monkeypatch.setattr(cluster, "_request_pbcluster_sync", lambda root: local("sync", None))
    monkeypatch.setattr(cluster, "active_checkpoint_bundle", lambda root: local("checkpoint_bundle", {
        "commit_proof": {"coordinator_id": local_id},
        "checkpoint": {"blob_refs": {}},
    } if checkpoint else None))
    monkeypatch.setattr(cluster, "checkpoint_status", lambda root: local("checkpoint_status", {"active": True, "checkpoint_id": "test-checkpoint"}))
    monkeypatch.setattr(cluster, "_cluster_join_command", lambda *a: local("join_command", "test-join"))
    monkeypatch.setattr(cluster, "create_join_authorization", lambda *a: local("join_authorization", {}))
    monkeypatch.setattr(cluster, "ensure_node_key_material", lambda *a: local("node_keys", SimpleNamespace(public_bundle=lambda *a: {"signing_public_key": "test-key"})))
    monkeypatch.setattr(cluster, "_run_cluster_payload_command", payload_command)

    result = asyncio.run(cluster.onboard_vps_cluster_node("test-runner", progress_callback=progress))
    assert result["ok"]
    assert result["join"]["completion"]["pbrun_start"]["started"]
    assert phases[0] == "registering" and phases[-1] == "complete"
    assert {"ssh_material", "ssh_metadata", "authorized_key", "operations", "rebuild", "sync"} <= set(calls)
    if configure_ssh:
        assert "configuring_ssh" in phases and node["state_replica"] is True
        assert calls.count("snapshot") == 4  # Registration, SSH settings, repair, replica restore.
    if new_join:
        assert "joining" in phases
    if checkpoint:
        assert {"checkpoint_status", "join_authorization", "node_keys", "join_checkpoint"} <= set(calls)
    if protocol == 2:
        assert "rewrap" in calls
    assert not cluster._LOCAL_IO_WORKERS


def test_shutdown_waits_other_workers_after_one_failure(monkeypatch, isolated_cluster):
    """One failed worker cannot skip a still-running worker during API shutdown."""
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]

    def work(index):
        """Provide independently controlled workers, one of which raises."""
        entered[index].set()
        assert release[index].wait(5)
        if index == 0:
            raise RuntimeError("test failure")
        return "done"

    @cluster._cluster_worker_operation
    async def operation(index):
        """Give both workers a retained operation owner."""
        return await cluster._cluster_local_io(work, index)

    async def run():
        """Ensure finally releases and joins every synthetic worker."""
        tasks = [asyncio.create_task(operation(index)) for index in range(2)]
        stopping = None
        try:
            for event in entered:
                await wait_event(event)
            stopping = asyncio.create_task(cluster.shutdown())
            release[0].set()
            result = await asyncio.wait_for(asyncio.gather(tasks[0], return_exceptions=True), 4)
            assert isinstance(result[0], RuntimeError)
            assert not stopping.done()
            assert cluster.restart_block_reason()
        finally:
            for event in release:
                event.set()
            await asyncio.wait_for(asyncio.gather(*tasks, *([stopping] if stopping else []), return_exceptions=True), 5)
        assert not cluster._LOCAL_IO_WORKERS
        assert cluster.restart_block_reason() == ""
        await cluster.shutdown()  # Idempotent after complete cleanup.

    asyncio.run(run())


def test_scoped_pull_batches_writes_and_blob_files_off_loop(monkeypatch, isolated_cluster):
    """Nonempty pulls preserve deferred credential ordering and offload real blob I/O."""
    import base64
    import hashlib
    import json

    loop_thread = threading.get_ident()
    raw_file = b'{"test": true}'
    file_digest = hashlib.sha256(raw_file).hexdigest()
    manifest = json.dumps({"files": {"config.json": {"sha256": file_digest}}}).encode()
    manifest_hash = "sha256:" + hashlib.sha256(manifest).hexdigest()
    blobs = {manifest_hash: manifest, "sha256:" + file_digest: raw_file}
    operations = [
        {"actor": "remote", "seq": 1, "op": "TEST"},
        {"actor": "remote", "seq": 2, "op": sorted(cluster.V2_CREDENTIAL_OPS)[0]},
    ]
    materialized = {"state_vector": {}, "desired_state": {"instances": {"demo": {"config_manifest_hash": manifest_hash}}}}
    written = []
    read_blob, write_blob = cluster._read_cluster_blob, cluster._write_cluster_blob
    writes = []

    def check():
        """Reject local I/O on the event-loop thread."""
        assert threading.get_ident() != loop_thread

    def read(*args):
        """Use real temporary blob files while checking their execution thread."""
        check()
        return read_blob(*args)

    def write(*args, **kwargs):
        """Write only below the fixture's temporary cluster root."""
        check()
        writes.append(args[1])
        return write_blob(*args, **kwargs)

    def rebuild(*args, **kwargs):
        """Return a synthetic desired state referencing real temporary blobs."""
        check()
        return materialized

    def stage(*args, **kwargs):
        """Model membership staging before credential operations are validated."""
        check()
        return {}

    def validate(operation, **kwargs):
        """Observe validation after staging without involving key stores."""
        check()
        if operation["seq"] == 2:
            assert written == [1]

    def persist(root, operation, **kwargs):
        """Observe oplog persistence without adding unrelated signed fixtures."""
        check()
        written.append(operation["seq"])

    async def remote_json(pool, hostname, remote_dir, local_id, command, **kwargs):
        """Serve synthetic vector, operations and blobs from memory."""
        assert threading.get_ident() == loop_thread
        if command == "get-state-vector":
            return {"cluster_id": "test-cluster", "state_vector": {"remote": 2}}
        if command.startswith("get-ops "):
            return {"operations": operations}
        blob_hash = command.split()[-1]
        return {"hash": blob_hash, "content_b64": base64.b64encode(blobs[blob_hash]).decode()}

    monkeypatch.setattr(cluster, "_read_cluster_blob", read)
    monkeypatch.setattr(cluster, "_write_cluster_blob", write)
    monkeypatch.setattr(cluster, "rebuild_materialized_state", rebuild)
    monkeypatch.setattr(cluster, "stage_membership_operations", stage)
    monkeypatch.setattr(cluster, "validate_operation", validate)
    monkeypatch.setattr(cluster, "write_operation", persist)
    monkeypatch.setattr(cluster, "_run_cluster_json_command_on_host", remote_json)

    @cluster._cluster_worker_operation
    async def operation():
        """Use the same inherited worker ownership as the onboarding entry point."""
        return await cluster._pull_missing_operations_from_host(object(), "test-runner", "/tmp/test", "local", "test-cluster")

    result = asyncio.run(operation())
    assert written == [1, 2]
    assert result["pulled_ops"] == 2
    assert result["pulled_config_blobs"] == 2
    assert set(writes) == set(blobs)
    assert not cluster._LOCAL_IO_WORKERS


def test_scoped_checkpoint_blob_collection_uses_worker(monkeypatch, isolated_cluster):
    """Checkpoint manifests, child blobs and secret envelopes are read off-loop."""
    loop_thread = threading.get_ident()
    reads = []
    uploads = []
    hashes = {kind: "sha256:" + char * 64 for kind, char in zip(("manifest", "child", "secret", "sealed"), "abcd")}

    def read(base, blob_hash):
        """Model local blob reads and a manifest with one child file."""
        assert threading.get_ident() != loop_thread
        reads.append(blob_hash)
        if blob_hash == hashes["manifest"]:
            return ('{"files":{"config.json":{"sha256":"' + "b" * 64 + '"}}}').encode()
        return b'{}'

    async def upload(*args):
        """Check every actual remote upload stays on the event loop."""
        assert threading.get_ident() == loop_thread
        uploads.extend(item["hash"] for item in args[4])
        return {"pushed": len(args[4])}

    monkeypatch.setattr(cluster, "get_monitor", lambda: SimpleNamespace(pool=object()))
    monkeypatch.setattr(cluster, "_read_cluster_blob", read)
    for name in ("_push_config_blobs_to_remote", "_push_secret_blobs_to_remote", "_push_sealed_blobs_to_remote"):
        monkeypatch.setattr(cluster, name, upload)

    @cluster._cluster_worker_operation
    async def operation():
        """Exercise checkpoint collection with onboarding's inherited ownership."""
        return await cluster._push_active_checkpoint_blobs_to_remote(
            {"pbname": "test-runner"}, isolated_cluster["identity"],
            {"blob_refs": {"config": [hashes["manifest"]], "secret": [hashes["secret"]], "sealed": [hashes["sealed"]]}},
        )

    result = asyncio.run(operation())
    assert result == {"config": 2, "secret": 1, "sealed": 1}
    assert set(reads) == set(hashes.values())
    assert set(uploads) == set(hashes.values())
    assert not cluster._LOCAL_IO_WORKERS


def test_worker_and_unscoped_pull_can_publish_same_blob(monkeypatch, isolated_cluster, tmp_path):
    """Overlapping worker and event-loop pulls cannot consume each other's temp file."""
    import base64
    import hashlib
    import stat

    base = tmp_path / "config_blobs"
    raw = b'{"test": 492}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    loop_thread = threading.get_ident()
    entered = threading.Event()
    release = threading.Event()
    temporary_paths = []
    original_write = type(base).write_bytes

    def paused_write(path, data):
        """Pause the worker after writing while the existing entrance publishes."""
        result = original_write(path, data)
        if path.name.endswith(".tmp"):
            temporary_paths.append(path)
            if threading.get_ident() != loop_thread:
                entered.set()
                assert release.wait(5), "blob writer was not released"
        return result

    async def remote(*args, **kwargs):
        """Return only synthetic content without contacting a remote host."""
        assert threading.get_ident() == loop_thread
        return {"hash": blob_hash, "content_b64": base64.b64encode(raw).decode()}

    async def pull():
        """Reach the real local read and write helpers via a mocked remote read."""
        return await cluster._ensure_remote_blob_on_host(
            None, "test-runner", "/tmp/test", "local", base, blob_hash, secret=False,
        )

    worker_pull = cluster._cluster_worker_operation(pull)
    monkeypatch.setattr(type(base), "write_bytes", paused_write)
    monkeypatch.setattr(cluster, "_run_cluster_json_command_on_host", remote)

    async def run():
        """Join the worker on every path and preserve its lifecycle protection."""
        worker = asyncio.create_task(worker_pull())
        try:
            await wait_event(entered)
            existing_result = await pull()
            assert cluster._LOCAL_IO_WORKERS
            assert cluster.restart_block_reason()
        finally:
            release.set()
            results = await asyncio.wait_for(asyncio.gather(worker, return_exceptions=True), 5)
        assert existing_result == (raw, True)
        assert results == [(raw, True)]
        assert not cluster._LOCAL_IO_WORKERS
        assert not cluster.restart_block_reason()

    asyncio.run(run())
    target = cluster._cluster_blob_path(base, blob_hash)
    assert target.read_bytes() == raw
    assert stat.S_IMODE(target.stat().st_mode) == 0o644
    assert len(temporary_paths) == len(set(temporary_paths)) == 2
    assert list(base.rglob("*.tmp")) == []


@pytest.mark.parametrize("failure_stage", ["write", "chmod", "replace"])
def test_config_blob_write_failure_preserves_target_and_removes_temp(monkeypatch, tmp_path, failure_stage):
    """Failed publication removes partial temp files and preserves committed content."""
    import hashlib

    base = tmp_path / "config_blobs"
    raw = b'{"test": "previously committed"}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    cluster._write_cluster_blob(base, blob_hash, raw, secret=False)
    target = cluster._cluster_blob_path(base, blob_hash)
    original_write = type(base).write_bytes

    def partial_write(path, data):
        """Simulate a write failing after partially filling its temporary file."""
        original_write(path, data[:1])
        raise OSError("synthetic blob publication failure")

    def fail(*args, **kwargs):
        """Fail the selected publication stage without accessing runtime data."""
        raise OSError("synthetic blob publication failure")

    if failure_stage == "write":
        monkeypatch.setattr(type(base), "write_bytes", partial_write)
    elif failure_stage == "chmod":
        monkeypatch.setattr(cluster.os, "chmod", fail)
    else:
        monkeypatch.setattr(cluster.os, "replace", fail)
    with pytest.raises(OSError, match="synthetic blob publication failure"):
        cluster._write_cluster_blob(base, blob_hash, raw, secret=False)
    assert target.read_bytes() == raw
    assert list(base.rglob("*.tmp")) == []
