"""Compact credential bases retain dependencies needed by full and future replay."""

from copy import deepcopy
import random

import pytest

from master import cluster_checkpoint as checkpoint
from master.cluster_state import _materialize_v2_credentials


ACTOR = "pbgui-node-00000000-0000-4000-8000-00000000000a"
SECRET = "secret_" + "a" * 32


def _op(kind, seq, **fields):
    """Build value-free synthetic operation metadata for the pure reducer."""
    return {"op": kind, "seq": seq, "op_id": f"{ACTOR}:{seq:08d}",
            "actor": ACTOR, "created_at": 1000 + seq, **fields}


def _secret(seq=1, generation=1):
    """Describe a synthetic sealed reference without storing a credential value."""
    return _op("UPSERT_SECRET", seq, secret_id=SECRET, secret_kind="exchange_api_key",
               audience="masters", generation=generation, parent_generation=generation - 1,
               sealed_blob_hash="sha256:" + "a" * 64, recipient_generation=1,
               parent_recipient_generation=0, membership_generation=1,
               recipient_ids=[ACTOR], recipient_key_ids={ACTOR: "synthetic-public-key"})


def _recipient(seq, generation, parent, provider=1, digest="b"):
    """Describe one parent-linked envelope-recipient generation."""
    return _op("UPDATE_SECRET_RECIPIENTS", seq, secret_id=SECRET,
               provider_generation=provider, recipient_generation=generation,
               parent_recipient_generation=parent, membership_generation=generation,
               sealed_blob_hash="sha256:" + digest * 64, recipient_ids=[ACTOR],
               recipient_key_ids={ACTOR: "synthetic-public-key"})


def _freeze(seq, generation):
    """Declare one value-blind migration generation."""
    return _op("WRITER_FREEZE", seq, freeze_generation=generation, frozen=True)


def _ack(kind, seq, generation):
    """Create a migration ACK whose materialized projection can omit its op_id."""
    return _op(kind, seq, node_id=ACTOR, freeze_generation=generation,
               credential_generations={}, process_readiness=[],
               source_generations={}, source_fingerprints={})


def _basis(operations):
    """Verify compaction and return the selected public credential metadata."""
    full = _materialize_v2_credentials(sorted(operations, key=checkpoint._operation_order))
    reduced = checkpoint._build_reducer_state(operations, {"desired_state": full})
    checkpoint._verify_reducer_state(reduced, {"desired_state": full})
    return reduced["v2_basis_operations"]


def _assert_tail_equivalence(operations, tail):
    """Compare an independent full replay to a checkpoint basis plus the same tail."""
    before = deepcopy(operations)
    basis = _basis(operations)
    assert _materialize_v2_credentials(sorted(basis + tail, key=checkpoint._operation_order)) == \
        _materialize_v2_credentials(sorted(operations + tail, key=checkpoint._operation_order))
    assert operations == before
    assert _basis(basis) == basis


def test_recipient_chain_reproduces_the_current_envelope_reference():
    """A recipient winner is unreachable without the intermediate parent updates."""
    operations = [_secret(), _recipient(2, 2, 1), _recipient(3, 3, 2), _recipient(4, 4, 3)]
    full = _materialize_v2_credentials(operations)
    assert full["secrets"][SECRET]["recipient_generation"] == 4
    assert full["secrets"][SECRET]["recipient_op_id"] == operations[-1]["op_id"]
    _assert_tail_equivalence(operations, [_recipient(5, 5, 4)])


@pytest.mark.parametrize("kind", ["WRITER_FREEZE_ACK", "CREDENTIAL_MATERIALIZATION_ACK", "CREDENTIAL_INVENTORY_ACK"])
def test_migration_ack_dependencies_remain_replayable(kind):
    """Freeze and legacy materialization ACK projections need not contain op_id."""
    _assert_tail_equivalence([_freeze(1, 1), _ack(kind, 2, 1)], [])


@pytest.mark.parametrize("kind", ["WRITER_FREEZE_ACK", "CREDENTIAL_MATERIALIZATION_ACK", "CREDENTIAL_INVENTORY_ACK"])
def test_future_generation_ack_survives_until_its_freeze_arrives(kind):
    """A valid early ACK must not disappear before its future freeze is replayed."""
    _assert_tail_equivalence([_freeze(1, 1), _ack(kind, 2, 2)], [_freeze(3, 2)])


def test_recipient_siblings_and_late_branch_keep_the_same_conflict():
    """A later parent update can select a previously unreachable recipient branch."""
    operations = [_secret(), _recipient(2, 2, 1), _recipient(3, 3, 2),
                  _recipient(4, 3, 2, digest="c"), _recipient(5, 5, 4)]
    _assert_tail_equivalence(operations, [_recipient(6, 4, 3)])
    _assert_tail_equivalence(operations, [_recipient(7, 2, 1, digest="d")])


def test_future_provider_recipient_update_is_preserved():
    """A future secret upsert can activate a previously received recipient update."""
    _assert_tail_equivalence([_secret(), _recipient(2, 2, 1, provider=2)], [_secret(3, generation=2)])


def test_obsolete_ack_and_recipient_generations_do_not_accumulate():
    """Superseded freeze generations and provider lifetimes are not a history fallback."""
    operations = [_freeze(i * 2, i) for i in range(1, 201)]
    operations += [_ack("WRITER_FREEZE_ACK", i * 2 + 1, i) for i in range(1, 201)]
    operations += [_recipient(500 + i, i + 2, i + 1) for i in range(100)]
    operations += [_secret(700, generation=2)]
    basis = _basis(operations)
    assert len(basis) == 3
    assert {op["op"] for op in basis} == {"WRITER_FREEZE", "WRITER_FREEZE_ACK", "UPSERT_SECRET"}


def test_deterministic_basis_with_many_repeated_acknowledgements():
    """Unordered inputs and repeated acknowledgements have one stable compact basis."""
    operations = [_freeze(1, 1), _secret(2)]
    for seq in range(3, 70):
        operations.append(_ack("WRITER_FREEZE_ACK", seq, 1))
    operations += [_recipient(70, 2, 1), _recipient(71, 3, 2)]
    expected = _basis(operations)
    assert len(expected) == 5
    random.Random(43).shuffle(operations)
    assert _basis(operations) == expected


@pytest.mark.parametrize("seed", range(16))
def test_generated_dependency_histories_preserve_future_replay(seed):
    """Varied recipient branches and early migration ACKs survive arbitrary arrival order."""
    rng = random.Random(seed)
    operations = [_secret(), _freeze(2, 1)]
    for seq in range(3, 65):
        if rng.randrange(2):
            parent = rng.randrange(1, 8)
            operation = _recipient(seq, parent + 1, parent, provider=rng.choice([1, 2]),
                                   digest=rng.choice(["b", "c"]))
        else:
            kind = rng.choice(["WRITER_FREEZE_ACK", "CREDENTIAL_INVENTORY_ACK", "CREDENTIAL_MATERIALIZATION_ACK"])
            operation = _ack(kind, seq, rng.randrange(1, 5))
            if kind == "CREDENTIAL_MATERIALIZATION_ACK" and rng.randrange(2):
                operation.update(recipient_generations={}, membership_generation=1)
        operation["created_at"] = rng.randrange(1000, 1100)
        operations.append(operation)
    tail = [_freeze(65, 3), _recipient(66, 2, 1), _recipient(67, 3, 2), _secret(68, generation=2)]
    _assert_tail_equivalence(operations, tail)
    expected = _basis(operations)
    rng.shuffle(operations)
    assert _basis(operations) == expected


def test_recipient_and_legacy_materialization_acks_share_the_same_freeze_projection():
    """The reducer filters the global projection by variant, but not the freeze projection."""
    recipient = _ack("CREDENTIAL_MATERIALIZATION_ACK", 3, 1)
    recipient.update(recipient_generations={}, membership_generation=1)
    operations = [_freeze(1, 1), _ack("CREDENTIAL_MATERIALIZATION_ACK", 2, 1), recipient]
    basis = _basis(operations)
    assert {op["seq"] for op in basis} == {1, 3}
    _assert_tail_equivalence(operations, [_ack("CREDENTIAL_MATERIALIZATION_ACK", 4, 1)])


def test_same_generation_freezes_do_not_partition_acknowledgements():
    """Freeze ACKs bind to a generation, without a separate freeze-operation selector."""
    operations = [_freeze(1, 1), _freeze(2, 1), _ack("WRITER_FREEZE_ACK", 3, 1),
                  _ack("WRITER_FREEZE_ACK", 4, 1)]
    basis = _basis(operations)
    assert {op["seq"] for op in basis} == {2, 4}
    _assert_tail_equivalence(operations, [_ack("WRITER_FREEZE_ACK", 5, 1)])


def test_signed_checkpoint_rebuild_after_pruned_recipient_history(tmp_path, monkeypatch):
    """A real signed checkpoint retains the envelope chain after isolated history removal."""
    from cluster_credentials import ensure_node_key_material
    from master.cluster_state import append_operation, ensure_local_identity, load_operations, rebuild_materialized_state

    root = tmp_path / "cluster"
    now = 2_000_000_000
    cluster_id = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
    ensure_local_identity(root, role="master", pbname="test-master", node_id=ACTOR,
                          cluster_id=cluster_id, created_at=100)
    public = ensure_node_key_material(root).public_bundle(ACTOR, "master")
    append_operation(root, "ADD_NODE", {**public, "pbname": "test-master"}, created_at=now - 100)
    secret_id = "cmc_" + "a" * 32
    initial = _secret()
    initial.update(secret_id=secret_id, secret_kind="cmc_api_key", audience="cluster",
                   recipient_key_ids={ACTOR: public["encryption_key_id"]})
    excluded = {"op", "seq", "op_id", "actor", "created_at"}
    append_operation(root, "UPSERT_SECRET", {k:v for k,v in initial.items() if k not in excluded},
                     created_at=now - 90)
    for generation in (2, 3):
        update = _recipient(generation, generation, generation - 1)
        update.update(secret_id=secret_id, recipient_key_ids={ACTOR: public["encryption_key_id"]})
        append_operation(root, "UPDATE_SECRET_RECIPIENTS", {k:v for k,v in update.items() if k not in excluded},
                         created_at=now - 80 + generation)
    full_before = load_operations(root)
    # Only the migration seal is synthetic; all signatures, checkpoint hashes,
    # membership, activation proof and post-baseline validation remain real.
    monkeypatch.setattr(checkpoint, "build_migration_seal", lambda state: {
        "schema_version": 1, "status": "sealed", "cluster_id": cluster_id,
        "active_node_ids": [ACTOR], "blockers": [],
    })
    first = checkpoint.build_shadow_checkpoint(root, created_at=now)
    proposal = checkpoint.create_checkpoint_proposal(root, first, created_at=now, expires_at=now + 60)
    ack = checkpoint.create_checkpoint_ack(root, first, proposal, created_at=now + 1)
    proof = checkpoint.create_checkpoint_commit_proof(root, first, proposal, [ack], created_at=now + 2)
    checkpoint.activate_checkpoint(root, first, commit_proof=proof, activated_at=now + 2)
    for path in (root / "oplog").glob("*/*.json"):
        path.unlink()  # Only this temporary fixture; model an already-pruned baseline.
    update = _recipient(5, 4, 3)
    update.update(secret_id=secret_id, recipient_key_ids={ACTOR: public["encryption_key_id"]})
    tail = append_operation(root, "UPDATE_SECRET_RECIPIENTS", {k:v for k,v in update.items() if k not in excluded},
                            created_at=now + 3)
    expected = _materialize_v2_credentials(sorted(full_before + [tail], key=checkpoint._operation_order))
    actual = rebuild_materialized_state(root, write=False)["desired_state"]
    assert {key:actual[key] for key in expected} == expected
    second = checkpoint.build_shadow_checkpoint(root, created_at=now + 4)
    assert second["materialized"]["desired_state"]["secrets"][secret_id]["recipient_generation"] == 4
    assert second["checkpoint_id"] != first["checkpoint_id"]
