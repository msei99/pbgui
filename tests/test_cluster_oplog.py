"""Offline HTTP, error and history-transaction regressions for the Oplog endpoint."""

import fcntl
import multiprocessing
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import cluster
from file_lock import advisory_file_lock
from master import cluster_checkpoint as checkpoints
from master.cluster_state import append_operation, ensure_local_identity, load_operations


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
NODE_ID = "pbgui-node-00000000-0000-4000-8000-00000000000a"


def _initialize(root):
    """Create only test-owned identity files, without touching the configured install."""
    ensure_local_identity(root, cluster_id=CLUSTER_ID, node_id=NODE_ID, pbname="test", created_at=100)


def _history_writer(root, ready, release):
    """Hold the real history lock in a separate process, then publish one operation."""
    with advisory_file_lock(root / ".append_sequence"):
        ready.set()
        if not release.wait(15):
            raise RuntimeError("test writer release timed out")
        append_operation(root, "STOP_INSTANCE", {"instance": "process-write"}, created_at=1000)


@pytest.fixture
def oplog_client(tmp_path, monkeypatch):
    """Serve the real route without production authentication, startup or runtime data."""
    root = tmp_path / "data" / "cluster"
    logs = []
    monkeypatch.setattr(cluster, "PBGDIR", tmp_path)
    monkeypatch.setattr(cluster, "_log", lambda service, message, **kwargs: logs.append((message, kwargs)))
    monkeypatch.setattr(cluster, "_load_cluster_snapshot", lambda: pytest.fail("unexpected snapshot rebuild"))
    app = FastAPI()
    app.include_router(cluster.router, prefix="/api/cluster")
    app.dependency_overrides[cluster.require_auth] = lambda: object()
    with TestClient(app) as client:
        yield client, root, logs


@pytest.mark.parametrize("existing_empty_directory", [False, True])
def test_unconfigured_oplog_does_not_create_state(oplog_client, existing_empty_directory):
    """Only a missing or genuinely empty root yields empty history without writes."""
    client, root, logs = oplog_client
    if existing_empty_directory:
        root.mkdir(parents=True)
    response = client.get("/api/cluster/oplog")
    assert response.status_code == 200
    assert response.json() == {"count": 0, "operations": []}
    assert root.exists() is existing_empty_directory
    assert not root.exists() or list(root.iterdir()) == []
    assert logs == []


@pytest.mark.parametrize("residue", [
    "oplog/actor/00000001.json", "checkpoints/commit.json", "node_identity.json",
    "cluster_id", "node_id",
])
def test_missing_identity_with_retained_state_is_an_error(oplog_client, residue):
    """Retained state or half an identity must not be mislabeled as an empty cluster."""
    client, root, logs = oplog_client
    path = root / residue
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="utf-8")
    response = client.get("/api/cluster/oplog")
    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to load cluster oplog"}
    assert logs[-1][1]["level"] == "ERROR"
    assert path.read_text(encoding="utf-8") == "{}"


@pytest.mark.parametrize("name", ["cluster_id", "node_id", "node_identity.json", ".append_sequence", ".append_sequence.lock"])
@pytest.mark.parametrize("dangling", [False, True])
def test_identity_and_lock_symlinks_are_not_followed(oplog_client, name, dangling):
    """Reject both valid and dangling identity/lock links without touching their target."""
    client, root, logs = oplog_client
    _initialize(root)
    target = root.parent / "outside"
    if not dangling:
        target.write_text("unchanged", encoding="utf-8")
    path = root / name
    if path.exists():
        path.unlink()
    path.symlink_to(target)
    response = client.get("/api/cluster/oplog")
    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to load cluster oplog"}
    assert path.is_symlink()
    assert logs[-1][1]["level"] == "ERROR"
    assert not target.exists() if dangling else target.read_text(encoding="utf-8") == "unchanged"


def test_dangling_root_is_not_an_unconfigured_cluster(oplog_client):
    """A dangling root symlink is an error, not an invitation to create its target."""
    client, root, logs = oplog_client
    root.parent.mkdir(parents=True)
    target = root.parent / "missing"
    root.symlink_to(target)
    response = client.get("/api/cluster/oplog")
    assert response.status_code == 500
    assert logs[-1][1]["level"] == "ERROR"
    assert not target.exists()


@pytest.mark.parametrize("path", ["node_identity.json", "checkpoints/commit.json", "oplog/actor/00000001.json"])
def test_malformed_persisted_json_returns_logged_json_error(oplog_client, path):
    """Actual identity, checkpoint and operation parser failures share the HTTP contract."""
    client, root, logs = oplog_client
    _initialize(root)
    broken = root / path
    broken.parent.mkdir(parents=True, exist_ok=True)
    broken.write_text("{", encoding="utf-8")
    response = client.get("/api/cluster/oplog")
    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to load cluster oplog"}
    assert logs[-1][1]["level"] == "ERROR"
    assert broken.read_text(encoding="utf-8") == "{"


@pytest.mark.parametrize("error", [PermissionError("denied"), OSError("unreadable"), checkpoints.ClusterCheckpointError("invalid checkpoint")])
def test_loader_errors_are_controlled(oplog_client, monkeypatch, error):
    """I/O and checkpoint failures must be logged, not escape as an unstructured 500."""
    client, root, logs = oplog_client
    _initialize(root)

    def fail(*args, **kwargs):
        """Inject an actual loader exception type without accessing production data."""
        raise error

    monkeypatch.setattr(cluster, "load_operations", fail)
    response = client.get("/api/cluster/oplog")
    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to load cluster oplog"}
    assert logs[-1][1]["level"] == "ERROR"


def test_http_exception_status_is_preserved(oplog_client, monkeypatch):
    """Expected HTTP failures retain their status and detail through the error guard."""
    client, root, _ = oplog_client
    _initialize(root)

    def conflict(*args, **kwargs):
        """Represent a deliberate admission rejection."""
        raise HTTPException(409, "busy")

    monkeypatch.setattr(cluster, "load_operations", conflict)
    response = client.get("/api/cluster/oplog")
    assert response.status_code == 409
    assert response.json() == {"detail": "busy"}


@pytest.mark.parametrize("limit", [0, 501, "invalid"])
def test_oplog_limit_is_validated_before_read(oplog_client, limit):
    """The route keeps its existing bounded query contract."""
    client, root, _ = oplog_client
    assert client.get("/api/cluster/oplog", params={"limit": limit}).status_code == 422
    assert not root.exists()


def test_oplog_keeps_authentication_dependency(oplog_client):
    """An authentication rejection cannot be bypassed by the empty-state fast path."""
    client, root, _ = oplog_client

    def reject():
        """Reject the request without reading the production credential store."""
        raise HTTPException(401, "not authenticated")

    client.app.dependency_overrides[cluster.require_auth] = reject
    assert client.get("/api/cluster/oplog").status_code == 401
    assert not root.exists()


def test_oplog_waits_for_history_writer_before_identity_or_load(oplog_client, monkeypatch):
    """A concurrent reader waits for the complete write and owns the real OS lease."""
    _, root, _ = oplog_client
    _initialize(root)
    append_operation(root, "STOP_INSTANCE", {"instance": "first"}, created_at=1000)
    attempted = threading.Event()
    identity_read = threading.Event()
    real_identity = cluster.read_local_identity
    calls = []

    @contextmanager
    def observed_lock(path):
        """Signal the reader reaching the real lock without bypassing it."""
        attempted.set()
        with advisory_file_lock(path):
            yield

    def read_identity(path):
        """Observe that even identity reads remain inside admission."""
        identity_read.set()
        return real_identity(path)

    def read_operations(*args, **kwargs):
        """Verify the OS lock, not just the in-process mutex, protects the real load."""
        calls.append(1)
        with (root / ".append_sequence.lock").open("rb") as handle:
            with pytest.raises(BlockingIOError):
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return load_operations(*args, **kwargs)

    monkeypatch.setattr(cluster, "advisory_file_lock", observed_lock)
    monkeypatch.setattr(cluster, "read_local_identity", read_identity)
    monkeypatch.setattr(cluster, "load_operations", read_operations)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with advisory_file_lock(root / ".append_sequence"):
            future = executor.submit(cluster.get_oplog, limit=50, session=None)
            assert attempted.wait(5)
            assert not identity_read.is_set()
            assert not future.done()
            append_operation(root, "START_INSTANCE", {"instance": "second"}, created_at=1000)
        payload = future.result(timeout=5)
    assert calls == [1]
    assert [op["seq"] for op in payload["operations"]] == [2, 1]


def test_oplog_reads_validated_checkpoint_tail(oplog_client, monkeypatch):
    """A real signed checkpoint and its validated tail retain reverse replay ordering."""
    client, root, _ = oplog_client
    _initialize(root)
    now = 2_000_000_000
    append_operation(root, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "test"}, created_at=100)
    # Migration policy is unrelated to reading a cryptographically verified tail.
    monkeypatch.setattr(checkpoints, "build_migration_seal", lambda materialized: {
        "schema_version": 1, "status": "sealed", "cluster_id": CLUSTER_ID,
        "active_node_ids": [NODE_ID], "blockers": [],
    })
    checkpoint = checkpoints.build_shadow_checkpoint(root, created_at=now)
    proposal = checkpoints.create_checkpoint_proposal(root, checkpoint, created_at=now, expires_at=now + 60)
    ack = checkpoints.create_checkpoint_ack(root, checkpoint, proposal, created_at=now + 1)
    proof = checkpoints.create_checkpoint_commit_proof(root, checkpoint, proposal, [ack], created_at=now + 2)
    checkpoints.activate_checkpoint(root, checkpoint, commit_proof=proof, activated_at=now + 2)
    append_operation(root, "STOP_INSTANCE", {"instance": "first"}, created_at=now + 3)
    append_operation(root, "START_INSTANCE", {"instance": "second"}, created_at=now + 3)
    expected = list(reversed(load_operations(root, expected_cluster_id=CLUSTER_ID)))
    response = client.get("/api/cluster/oplog?limit=1")
    assert response.status_code == 200
    assert response.json() == {"count": 2, "operations": expected[:1]}


def test_oplog_waits_for_other_process_writer(oplog_client, monkeypatch):
    """A separate process cannot expose an intermediate history to the endpoint."""
    _, root, _ = oplog_client
    _initialize(root)
    context = multiprocessing.get_context("spawn")
    ready, release = context.Event(), context.Event()
    attempted = threading.Event()

    @contextmanager
    def observed_lock(path):
        """Record the point at which the reader blocks on the cross-process lease."""
        attempted.set()
        with advisory_file_lock(path):
            yield

    monkeypatch.setattr(cluster, "advisory_file_lock", observed_lock)
    process = context.Process(target=_history_writer, args=(root, ready, release))
    process.start()
    try:
        assert ready.wait(10)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(cluster.get_oplog, limit=50, session=None)
            try:
                assert attempted.wait(5)
                assert not future.done()
            finally:
                release.set()
            payload = future.result(timeout=10)
        assert payload["count"] == 1
        assert payload["operations"][0]["instance"] == "process-write"
    finally:
        release.set()
        process.join(timeout=10)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
    assert process.exitcode == 0
