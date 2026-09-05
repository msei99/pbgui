"""Offline regressions for cluster snapshot transactions and publication recovery."""

from __future__ import annotations

import multiprocessing
import os
import queue
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from file_lock import advisory_file_lock
from master import cluster_state as state


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
NODE_ID = "pbgui-node-00000000-0000-4000-8000-00000000000a"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """Create signed, synthetic membership wholly below the pytest directory."""

    root = tmp_path / "cluster"
    state.ensure_local_identity(root, cluster_id=CLUSTER_ID, node_id=NODE_ID, created_at=100)
    state.append_operation(root, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "old"}, created_at=101)
    return root


def _paused_rebuild(root, captured, release, results, write=True, pause_file=None):
    """Pause a real replay/publication while it owns the cross-process lock."""

    owner = threading.get_ident()
    original_load = state.load_operations
    original_write = state._atomic_write_json

    def pause():
        """Signal the exact transaction boundary, then wait for the test."""
        captured.set()
        assert release.wait(15), "rebuild barrier timed out"

    def load(*args, **kwargs):
        """Pause after reading the old operations, not before acquiring the lock."""
        operations = original_load(*args, **kwargs)
        if pause_file is None and threading.get_ident() == owner:
            pause()
        return operations

    def publish(path, value):
        """Pause between atomic snapshot file replacements."""
        original_write(path, value)
        if Path(path).name == pause_file and threading.get_ident() == owner:
            pause()

    try:
        with patch.object(state, "load_operations", load), patch.object(state, "_atomic_write_json", publish):
            state.rebuild_materialized_state(root, write=write)
        results.put("rebuilt")
    except BaseException as exc:
        results.put(type(exc).__name__ + ": " + str(exc))


def _contender(root, attempted, finished, results, mutate=True):
    """Signal lock acquisition attempts before appending or reading a snapshot."""

    owner = threading.get_ident()

    @contextmanager
    def observed_lock(path):
        """Observe the real lock without replacing its synchronization."""
        if Path(path).name == ".append_sequence" and threading.get_ident() == owner:
            attempted.set()
        with advisory_file_lock(path):
            yield

    try:
        with patch.object(state, "advisory_file_lock", observed_lock):
            if mutate:
                state.append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "new"}, created_at=102)
                materialized = state.rebuild_materialized_state(root)
            else:
                materialized = state.read_materialized_state(root)
        results.put(materialized["state_vector"][NODE_ID])
    except BaseException as exc:
        results.put(type(exc).__name__ + ": " + str(exc))
    finally:
        finished.set()


@pytest.mark.parametrize("worker_kind", ["thread", "process"])
@pytest.mark.parametrize("write", [False, True])
def test_rebuild_serializes_history_read_through_publish(root: Path, worker_kind: str, write: bool) -> None:
    """Neither another thread nor process can append inside a replay transaction."""

    context = multiprocessing.get_context("spawn") if worker_kind == "process" else threading
    captured, release, attempted, finished = [context.Event() for _ in range(4)]
    results = context.Queue() if worker_kind == "process" else queue.Queue()
    worker = context.Process if worker_kind == "process" else context.Thread
    old = worker(target=_paused_rebuild, args=(root, captured, release, results, write))
    new = worker(target=_contender, args=(root, attempted, finished, results))
    old.start()
    try:
        assert captured.wait(15)
        new.start()
        assert attempted.wait(15)
        assert not finished.wait(0.1), "append overtook the locked replay"
    finally:
        release.set()
        old.join(15)
        if new.ident is not None:
            new.join(15)
    assert not old.is_alive() and not new.is_alive()
    if worker_kind == "process":
        assert old.exitcode == new.exitcode == 0
    assert {results.get(timeout=5), results.get(timeout=5)} == {"rebuilt", 2}
    assert len(state.load_operations(root)) == 2
    assert state.read_materialized_state(root) == state.rebuild_materialized_state(root, write=False)
    if worker_kind == "process":
        results.close()
        results.join_thread()


@pytest.mark.parametrize("pause_file", ["cluster_nodes.json", "desired_state.json", "state_vector.json"])
def test_reader_waits_for_complete_publication(root: Path, pause_file: str) -> None:
    """A reader cannot accept a stable midpoint between snapshot replacements."""

    state.rebuild_materialized_state(root)
    state.append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "new"}, created_at=102)
    captured, release, attempted, finished = [threading.Event() for _ in range(4)]
    results = queue.Queue()
    writer = threading.Thread(target=_paused_rebuild, args=(root, captured, release, results, True, pause_file))
    reader = threading.Thread(target=_contender, args=(root, attempted, finished, results, False))
    writer.start()
    try:
        assert captured.wait(15)
        reader.start()
        assert attempted.wait(15)
        assert not finished.wait(0.1), "reader accepted an uncommitted snapshot"
    finally:
        release.set()
        writer.join(15)
        if reader.ident is not None:
            reader.join(15)
    assert not writer.is_alive() and not reader.is_alive()
    assert {results.get(timeout=5), results.get(timeout=5)} == {"rebuilt", 2}


@pytest.mark.parametrize("failure_file", ["desired_state.json", "state_vector.json", "snapshot_commit.json"])
def test_snapshot_recovers_after_interrupted_publication(root: Path, monkeypatch, failure_file: str) -> None:
    """A failed writer leaves no valid commit for partial new snapshot files."""

    state.rebuild_materialized_state(root)
    state.append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "new"}, created_at=102)
    original = state._atomic_write_json

    def fail(path, value):
        """Inject failure at a specific boundary without touching the oplog."""
        if Path(path).name == failure_file:
            raise OSError("simulated interrupted publication")
        original(path, value)

    with monkeypatch.context() as scoped:
        scoped.setattr(state, "_atomic_write_json", fail)
        with pytest.raises(OSError, match="interrupted"):
            state.rebuild_materialized_state(root)
    assert state.read_materialized_state(root) == state.rebuild_materialized_state(root, write=False)
    assert state.read_materialized_state(root)["state_vector"] == {NODE_ID: 2}


@pytest.mark.parametrize("legacy", [False, True])
def test_freshly_written_stale_snapshot_is_not_current(root: Path, legacy: bool) -> None:
    """Fresh mtimes cannot validate an old snapshot, with or without a commit."""

    old = state.rebuild_materialized_state(root)
    state.append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "new"}, created_at=102)
    current = state.rebuild_materialized_state(root)
    state._atomic_write_json(root / "cluster_nodes.json", old["cluster_nodes"])
    if legacy:
        (root / "snapshot_commit.json").unlink()
    assert state.read_materialized_state(root) == current


def test_snapshot_identity_includes_operations_beyond_vector_gaps(root: Path) -> None:
    """New out-of-order operations invalidate a snapshot even with an equal vector."""

    before = state.rebuild_materialized_state(root)
    actor_dir = root / "oplog" / NODE_ID
    old_stat = actor_dir.stat()
    operation = {
        "schema_version": 1, "cluster_id": CLUSTER_ID,
        "op_id": f"{NODE_ID}:00000003", "actor": NODE_ID, "seq": 3,
        "op": "STOP_INSTANCE", "created_at": 103, "instance": "bot-a",
    }
    state.write_operation(root, operation)
    os.utime(actor_dir, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
    after = state.read_materialized_state(root)
    assert after["state_vector"] == before["state_vector"] == {NODE_ID: 1}
    assert after["cluster_nodes"]["generation"] == 2
    state.write_operation(root, {**operation, "op_id": f"{NODE_ID}:00000002", "seq": 2, "created_at": 102})
    assert state.read_materialized_state(root)["state_vector"] == {NODE_ID: 3}


def test_nested_snapshot_reads_and_replay_are_reentrant(root: Path) -> None:
    """Existing history transactions can nest readers, rebuilds and append calls."""

    with advisory_file_lock(root / ".append_sequence"):
        preview = state.rebuild_materialized_state(root, write=False)
        assert not (root / "snapshot_commit.json").exists()
        assert not (root / "cluster_nodes.json").exists()
        assert state.read_materialized_state(root) == preview
        state.append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "new"}, created_at=102)
        assert state.read_materialized_state(root)["state_vector"] == {NODE_ID: 2}


@pytest.mark.parametrize("read_only", [False, True])
def test_unconfigured_snapshot_reads_do_not_create_cluster_tree(tmp_path: Path, read_only: bool) -> None:
    """A standalone node probe must not initialize a cluster as a read side effect."""

    root = tmp_path / "absent-cluster"
    with pytest.raises(state.ClusterStateError, match="not initialized"):
        if read_only:
            state.rebuild_materialized_state(root, write=False)
        else:
            state.read_materialized_state(root)
    assert not root.exists()


def _checkpoint_bundle(root: Path, monkeypatch):
    """Build a real signed checkpoint proof with a synthetic completed migration."""

    from master import cluster_checkpoint as checkpoint

    def sealed(materialized):
        """Keep the fixture focused on snapshots rather than credential migration."""
        return {
            "schema_version": 1, "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID], "blockers": [],
        }

    monkeypatch.setattr(checkpoint, "build_migration_seal", sealed)
    monkeypatch.setattr(checkpoint, "time", SimpleNamespace(time=lambda: 113))
    payload = checkpoint.build_shadow_checkpoint(root, created_at=110)
    proposal = checkpoint.create_checkpoint_proposal(root, payload, created_at=110, expires_at=200)
    ack = checkpoint.create_checkpoint_ack(root, payload, proposal, created_at=111)
    proof = checkpoint.create_checkpoint_commit_proof(root, payload, proposal, [ack], created_at=112)
    return payload, proof


def test_checkpoint_identity_invalidates_equal_vector_and_survives_pruning(root: Path, monkeypatch) -> None:
    """Checkpoint activation/pruning invalidate the cache even without vector changes."""

    from master import cluster_checkpoint as checkpoint

    initial = state.rebuild_materialized_state(root)
    marker = root / "snapshot_commit.json"
    before = marker.read_bytes()
    payload, proof = _checkpoint_bundle(root, monkeypatch)
    checkpoint.activate_checkpoint(root, payload, commit_proof=proof, activated_at=113)
    assert state.read_materialized_state(root) == initial
    assert marker.read_bytes() != before
    checkpoint.checkpoint_status(root)  # Permission repair must not invalidate content.
    with monkeypatch.context() as scoped:
        scoped.setattr(state, "rebuild_materialized_state", lambda *_a, **_kw: pytest.fail("unchanged checkpoint replayed"))
        assert state.read_materialized_state(root) == initial

    before = marker.read_bytes()
    with advisory_file_lock(root / ".append_sequence"):
        (root / "oplog" / NODE_ID / "00000001.json").unlink()
    assert state.read_materialized_state(root) == initial
    assert marker.read_bytes() != before
    state.append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "tail"}, created_at=114)
    after = state.read_materialized_state(root)
    assert after["state_vector"] == {NODE_ID: 2}
    assert after["cluster_nodes"]["nodes"][NODE_ID]["pbname"] == "tail"


def test_checkpoint_corruption_is_not_hidden_by_current_snapshot(root: Path, monkeypatch) -> None:
    """Changes to checkpoint content with preserved mtimes cannot reuse a snapshot."""

    from master import cluster_checkpoint as checkpoint

    payload, proof = _checkpoint_bundle(root, monkeypatch)
    checkpoint.activate_checkpoint(root, payload, commit_proof=proof, activated_at=113)
    state.rebuild_materialized_state(root)
    digest = payload["checkpoint_id"].removeprefix("sha256:")
    with advisory_file_lock(root / ".append_sequence"):
        for suffix in (".json", ".backup.json"):
            path = root / "checkpoints" / "objects" / (digest + suffix)
            old_stat = path.stat()
            path.write_text("{}", encoding="utf-8")
            os.utime(path, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns))
    with pytest.raises(checkpoint.ClusterCheckpointError):
        state.read_materialized_state(root)


def test_checkpoint_activation_waits_for_readonly_replay(root: Path, monkeypatch) -> None:
    """A checkpoint branch cannot switch during an in-progress write=False replay."""

    from master import cluster_checkpoint as checkpoint

    payload, proof = _checkpoint_bundle(root, monkeypatch)
    captured, release, attempted, finished = [threading.Event() for _ in range(4)]
    results = queue.Queue()

    @contextmanager
    def observed_lock(path):
        """Report the checkpoint activation reaching its history lock."""
        if threading.current_thread().name == "activate" and Path(path).name == ".append_sequence":
            attempted.set()
        with advisory_file_lock(path):
            yield

    def activate():
        """Activate the prepared checkpoint without network or runtime access."""
        try:
            checkpoint.activate_checkpoint(root, payload, commit_proof=proof, activated_at=113)
            results.put("activated")
        except BaseException as exc:
            results.put(type(exc).__name__ + ": " + str(exc))
        finally:
            finished.set()

    monkeypatch.setattr(checkpoint, "advisory_file_lock", observed_lock)
    reader = threading.Thread(target=_paused_rebuild, args=(root, captured, release, results, False))
    writer = threading.Thread(target=activate, name="activate")
    reader.start()
    try:
        assert captured.wait(15)
        writer.start()
        assert attempted.wait(15)
        assert not finished.wait(0.1)
    finally:
        release.set()
        reader.join(15)
        if writer.ident is not None:
            writer.join(15)
    assert not reader.is_alive() and not writer.is_alive()
    assert {results.get(timeout=5), results.get(timeout=5)} == {"rebuilt", "activated"}
    assert state.read_materialized_state(root) == payload["materialized"]


def _exit_during_publication(root: Path) -> None:
    """Exit a disposable subprocess after the first snapshot replacement."""

    original = state._atomic_write_json

    def publish(path, value):
        """Simulate abrupt process death, not a Python exception cleanup."""
        original(path, value)
        if Path(path).name == "cluster_nodes.json":
            os._exit(73)

    with patch.object(state, "_atomic_write_json", publish):
        state.rebuild_materialized_state(root)


def test_snapshot_recovers_after_publisher_process_exit(root: Path) -> None:
    """A process crash releases the lock but never commits a mixed snapshot."""

    state.rebuild_materialized_state(root)
    state.append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "new"}, created_at=102)
    worker = multiprocessing.get_context("spawn").Process(target=_exit_during_publication, args=(root,))
    worker.start()
    worker.join(15)
    assert not worker.is_alive() and worker.exitcode == 73
    assert state.read_materialized_state(root) == state.rebuild_materialized_state(root, write=False)
    assert state.read_materialized_state(root)["state_vector"] == {NODE_ID: 2}


def test_monitor_metadata_uses_coherent_snapshot_reader() -> None:
    """The embedded host probe must not independently read multiple snapshot files."""

    from master.async_monitor import HOST_META_SCRIPT

    assert "materialized = read_materialized_state(cluster_root)" in HOST_META_SCRIPT
    assert "cluster_nodes.json" not in HOST_META_SCRIPT
    assert "desired_state.json" not in HOST_META_SCRIPT
