# CPU follow-up fixes — local implementation and validation

Date: 2026-10-16. This records the local implementation phase; the later explicitly authorized installation is documented in [the deployment report](2026-10-16-gap-cohort-deployment.md). PBGui base `605995b`; PB8 base `fb8bf8c644df4d4901ac41f736b1957491393817` (PR #1994 head).

PBGui changes were made directly in `/home/mani/software/pbgui`, as requested. PB8 changes are isolated in `/tmp/pb8-gap-cohort-fix`; the complete follow-up is preserved in `patches/pb8-gap-timestamp-cohort.patch`. No remote files, services, processes or bot configurations were changed. No commits, pushes or PR changes were made.

## Cluster Sync

An authenticated `handshake` now also returns the current mailbox index. The worker consumes it during idle sync instead of starting a second SSH forced-command Python process for `get-mailbox-index`. Older peers omit the field and retain the original request. Operation or checkpoint exchanges invalidate reuse and preserve the later index read. Message reads/deliveries/acks still use existing validation and expiry behavior; mailbox errors are propagated. `hello` capability-only probes do not incur this additional index work.

Tests exercise complete idle worker passes, old-peer fallback, changed operations/checkpoints, fresh index state, unknown-node rejection before mailbox access and expired/invalid mailbox handling. Existing security/cluster/PB8 compatibility tests also pass.

Paired local process CPU benchmark, five samples: a frozen base command performing handshake plus separate mailbox index took median **161.630 ms**; the current bundled command took **80.666 ms**. Complete hello and mailbox results were identical. Python processes fell from two to one for this pair. Commands ran against an isolated PBGui clone and synthetic temporary cluster, with keys prepared before timing. SSH transport/network overhead was not included.

This saves one process per eligible peer pass; it does not remove all Cluster RPC overhead or establish that the previously measured ~9% SSH child CPU will halve.

## Monitor software probes

The agent passes a bounded, at-most-32-entry RAM cache of successful non-secret Git/Python subprocess facts between host metadata cohorts. The script checks file identities, symbolic HEAD/ref targets, packed refs, common worktree refs, executable/symlink targets and venv configuration. A 60-second non-sliding TTL bounds unmodelled changes. Failed probes and signatures changing during execution are not cached. The agent removes the private cache from published host metadata.

Host metadata still runs every 10 seconds. Versions read from source, dynamic credentials, service state and PB8 executable/blocker/readiness checks remain live. The fallback remote host script does not publish/cache probe state unless explicitly invoked by the agent with its private cache environment.

Across six synthetic 10-second cohorts, two Git calls and one Python-version call per cohort fell from **18 child probes to three** with identical facts. Five-sample median CPU including reaped children was **104.430 → 38.541 ms**. This small isolated probe saving is not a measurement of the whole monitor agent or all previously observed 6.57% CPU.

Tests cover update invalidation, shared worktrees, virtual environments, TTL/clock rollback, failed and racing probes, malformed entries, entry bounds, private cache ownership and changing live fields.

## PB8 timestamp index

See `patches/pb8-gap-timestamp-cohort.md` for the exact frozen-reference coverage, native provenance and reproducible included benchmark. A large standardized gap check improved **1,165.668 → 103.701 ms CPU (11.24x)** with identical results. Raw HSL source reads bypass this path, so production benefit depends on actual standardized-read frequency and gap workload.

## Checks

- PBGui test gate: `focused`; no full suite run.
- PBGui: **278 passed**, covering Cluster command/worker, PB8 Cluster Sync, agent cache contract, software probes and credential capability. JUnit evidence: `/tmp/pbgui-cpu-followup-validation.xml`.
- PB8: **838 passed**, including **2,000 randomized exact state comparisons** and **16 byte-exact full candle/EMA combinations** through repeated reads. JUnit evidence: `/tmp/pb8-cohort-validation.xml`.
- PB8 docs: **5 passed**; checker **0 errors**, two documentation-size warnings.
- Both source diffs pass whitespace/error checks. PBGui API runtime serial incremented for startup-imported monitoring/Cluster modules.

These results support the local implementations and the tested result invariants. They do not prove an arbitrary workload cannot expose a difference or predict aggregate VPS CPU. A production before/after measurement would require separate deployment approval and must run without concurrent builds.
