# PBCluster Checkpoint And Seven-Day History Retention

## Goal

Bound PBCluster disk usage without retaining an archive of every historical
state when an administrator explicitly enables cleanup. Keep the configured
operation-history window, the current verified checkpoint,
one previous verified checkpoint for crash recovery, and only blobs reachable
from those states or the retained operation tail.

## Non-Goals

- Reconstructing arbitrary historical cluster states.
- Keeping offline replicas synchronizable from genesis forever.
- Deleting data before every active replica has verified the same checkpoint.
- Treating materialized UI JSON alone as a sufficient signing trust anchor.

## Retention Policy

- Default mode: `report_only`; checkpoints and reports run, but nothing is deleted.
- Optional modes: `oplog` and `oplog_and_blobs`.
- Default history window: seven days; configurable from 1 to 3650 days.
- Checkpoints retained: current and previous.
- Soft compaction trigger: 5000 operations or 10 MiB of oplog data.
- Periodic evaluation: at most once per day, when policy changes, or when a
  soft checkpoint trigger is reached. The daily age transition still runs when
  the oplog is idle.
- Operations are eligible only when their actor sequence is at or below the
  committed checkpoint baseline and both their signed `created_at` and local
  durable file age are older than the history cutoff.
- No active checkpoint means no deletion.
- A destructive policy must be active for 24 hours before its first deletion.

## Checkpoint Contents

The checkpoint identity is deterministic across replicas and covers:

- Cluster ID and checkpoint schema version.
- Baseline state vector for every actor.
- Hash of the complete secret-free materialized reducer state.
- Hash of the membership trust anchor.
- Hash of the compact reducer frontier and credential migration seal.
- Direct Config, Secret, and Sealed blob references.

The checkpoint payload additionally carries:

- Current cluster nodes and desired state.
- Current state vector.
- Removed node IDs.
- Historical public signing-key validity ranges.
- Historical role epochs needed to validate later signed operations.
- Validated membership operation IDs.
- Creation time, seven-day cutoff, and operation count.
- The signed cluster-wide retention policy.
- Compact V2 credential, CMC, TradFi, ACK, candidate, and policy reducer bases
  that must reproduce full replay exactly.

Checkpoint files and independent backup copies are owner-only, written atomically, and rejected on symlink,
cluster-ID, shape, state-hash, membership-hash, or checkpoint-ID mismatch.

## Safety Invariants

1. Never compact an actor with a sequence gap.
2. Never accept a checkpoint derived from a foreign cluster.
3. Never trust a coordinator-provided state without independently deriving the
   same checkpoint ID from the local validated oplog.
4. Never delete an operation newer than the seven-day cutoff.
5. Never delete an operation above the committed baseline vector.
6. Preserve tombstones, removed node IDs, signing keys, and role epochs in the
   checkpoint so stale data cannot resurrect.
7. Persist and verify the checkpoint before deleting the first operation.
8. Treat interrupted deletion as idempotent; the committed baseline makes
   already-pruned operation files optional.
9. Never garbage-collect a blob referenced by the current checkpoint, previous
   checkpoint, retained operation tail, or live mailbox message.
10. Nodes behind a pruned baseline must rebootstrap and may not relay their old
    tail into the cluster.

## Sync Protocol

Handshake metadata includes:

```json
{
    "checkpoint_id": "sha256:...",
    "checkpoint_baseline": {"pbgui-node-...": 123},
    "checkpoint_epoch": 4
}
```

Sync decisions:

- Equal checkpoint IDs: exchange only operations above each state vector.
- Remote vector behind locally available history but above the baseline:
  exchange the missing retained tail.
- Remote vector below the committed baseline: return `checkpoint_required`.
- New or stale replica: install the verified checkpoint, reset its local
  baseline, then pull operations above the checkpoint vector.
- A replica must reject any operation at or below its committed baseline.

## Checkpoint Commit

1. The elected coordinator proposes a deterministic shadow checkpoint.
2. Every required active replica independently builds and compares the same ID.
3. Replicas return signed acknowledgements containing checkpoint ID and
   baseline vector.
4. The coordinator commits only matching acknowledgements.
5. Each replica atomically installs the committed checkpoint.
6. Retention remains dry-run until all required replicas report the committed
   checkpoint as active.

An offline replica behind the committed baseline cannot upload its old branch.
It installs the current checkpoint and required blobs first. A divergent local
tail is quarantined and blocks automatic merge; a non-divergent stale node can
rebootstrap automatically. New joins use a coordinator-signed join grant and
checkpoint instead of requiring genesis oplog files.

## Blob Garbage Collection

Reachability includes:

- Direct hashes in current and previous checkpoints.
- Config file hashes referenced by reachable config manifests.
- Hashes referenced by retained post-checkpoint operations.
- Hashes referenced by unexpired mailbox messages.
- Explicitly retained credential generations required by the current state.

GC first writes a deterministic dry-run report. The identical candidate set
must remain stable for 24 hours before an idempotent sweep can start. Blob GC is
independently unavailable unless mode `oplog_and_blobs` is explicitly signed.

## Implemented Phases

### Phase 1: Shadow Checkpoint

- Build deterministic checkpoints from the existing validated full replay.
- Persist owner-only local shadow files.
- Report seven-day retention candidates without deleting anything.
- Compare checkpoint materialized state with full replay in tests and on the
  current cluster.

### Phase 2: Checkpoint Plus Tail Reducer

- Seed a reducer from the checkpoint state and membership trust anchor.
- Apply only operations above the baseline vector.
- Run full replay and checkpoint-plus-tail in parallel in tests.
- Block rollout on any byte-for-byte materialized-state mismatch.

### Phase 3: Replication And Acknowledgements

- Extend handshake and restricted sync commands with checkpoint metadata.
- Add independent replica verification and signed acknowledgements.
- Add checkpoint-required responses and rebootstrap transfer.

### Phase 4: Configurable Operation Retention

- Enable daily dry-run reports.
- Verify active replica checkpoint convergence.
- Keep deletion disabled by default and require a signed explicit mode change.
- Enable deletion only below baseline and older than the configured window.
- Retain the current and previous checkpoints.

### Phase 5: Reachable-Blob GC

- Expand direct references through config manifests and retained operations.
- Add dry-run byte/file counts.
- Enable deletion only after operation retention and two GC reports are stable.

## Verification

- Deterministic checkpoint IDs across replicas.
- Checkpoint state equals full replay.
- Actor sequence gaps block checkpointing.
- Tampered, foreign, malformed, and symlink checkpoints are rejected.
- Operations exactly seven days old remain retained until they become older
  than the cutoff.
- Retention dry-run never changes files.
- Crash at every checkpoint/install/prune boundary remains recoverable.
- Stale nodes cannot resurrect tombstones or upload pre-baseline operations.
- Full offline test suite remains green.
