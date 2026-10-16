# Remaining CPU work after the targeted PB8 fixes

16 October 2026. Read-only follow-up on manibot50 after PR #1994's source changes
were installed. No bot/VPS files, configuration, services or live processes were
changed by this investigation. Only separate diagnostic processes were run.
The published PR and original worker checkout remain unchanged.

## Measurement and attribution

A three-minute `/proc` accounting sample ran **07:45:56–07:48:56 UTC**. No
additional VPS probes or builds ran during that sample. Accounting includes
reaped-child CPU to capture collectors/RPC wrappers that exit between samples.
The diagnostic sampler itself consumed approximately **0.44% CPU**. These are
interval averages, not lifetime `ps` percentages; child accounting and host tick
sampling are different measurements and should not be forced to sum exactly.

| Work | Measured CPU |
| --- | ---: |
| Host executing work | 50.44% |
| Host I/O wait / steal | 0.45% / 0.00% |
| Bybit bot | 11.28% |
| Hyperliquid bot | 18.93% |
| PBRun | 3.24% |
| Monitor agent, own process | 3.48% |
| Monitor agent, reaped children | 3.09% |
| Monitor agent including those children | 6.57% |
| PBCluster daemon, own process | 0.14% |
| PBCoinData | 0.09% |

Four independent unprivileged SSH server processes accumulated approximately
**9.00% reaped-child CPU** combined (2.73%, 2.67%, 2.27%, 1.33%). Their distinct
parents were checked to avoid summing nested SSH processes. This is a child
accounting measurement, not an exact per-RPC profile or guaranteed removable load.

The approximately 30.2% combined bot CPU remains close to the earlier post-patch
trial's 30.08%, about an hour later. This is supporting evidence of persistence,
not a controlled isolation of patch effects from other changing workload.

## 1. Cluster Sync forced-command startup: current, measured overhead

A separate bounded process observation identified the short-lived Python workers
under those SSH servers as **cluster_sync_command.py**, launched with
`--cluster-root`, `--remote-node`, and `--allow-join`. Six such processes were
positively identified over twenty seconds. Observations emitted only known script
basenames, flags, hashes and accounting fields, never full arguments, environment,
SSH_ORIGINAL_COMMAND, credentials or private keys.

`cluster_sync_forced_command.sh` launches a new Python interpreter for each
restricted Cluster Sync command. `cluster_sync_command.py` eagerly imports the
credential, lease, checkpoint, state, API-key and logging helpers before command
dispatch. Even a small read-only command pays that import setup. Local isolated
imports of the same tracked code consumed **175.349 ms CPU median**, seven new
processes (range about 136–209 ms), after bytecode caches were warm. Import-time
wall figures were collected separately and are not substituted for CPU time.

Relevant deployed module hashes matched the inspected local files for
`cluster_sync_command.py`, `cluster_sync_forced_command.sh`, `monitor_agent.py`
and `master/cluster_state.py`. The cold-import probe used a clean temporary PBGui
clone without production INI, stores or runtime data. It did not run RPCs or use
credentials. The live INI was only statted: **593 bytes**. Its contents were never
read. Thus the evidence points to the whole command/bootstrap path, not a claim
that parsing a huge INI is the dominant cause.

Promising next steps: combine related restricted read-only RPCs per peer/pass and
avoid imports unrelated to the selected command. Any implementation must preserve
fresh peer validation, capability/credential checks, bounded payloads and failure
behavior. A persistent transport is a broader lifecycle/security change, not a
necessary first step. No handshake/readiness rule was weakened or changed.

## 2. Monitoring collectors: measurable overhead, narrower attribution pending

The monitor agent accounts for approximately **6.57% CPU** including children.
Source inspection shows a host-metadata shell/Python collector every ten seconds,
instance collection every thirty seconds and service collection every minute.
The host-metadata script repeats Git/ref/version checks and starts Python
interpreters to determine environment versions. The one-second live loop also
scans `/proc` and recomputes process identity/path checks.

Potential reductions: retain slow software/version facts behind file-identity
invalidation, keep current live metrics at their existing cadence, and reuse
validated process identities with correct exec/restart/config invalidation.
Action completion and actual source changes must still update displayed facts
automatically. The measured 3.09% child total includes all collectors: it has not
been attributed exclusively to host metadata, and 6.57% is not a savings forecast.

## 3. Strided NumPy timestamp searches: large reproducible operation-level defect

`CandlestickManager._unverified_uncovered_gap_ranges()` calls `_slice_ts_range()`
once per overlapping metadata gap. `_slice_ts_range()` obtains a timestamp view
from structured candle rows and calls NumPy `searchsorted` twice. That timestamp
view has a 48-byte row stride rather than a contiguous 8-byte stride. Repeated
scalar searches over this view are costly.

Fully synthetic data: 43,200 retained one-minute rows, 16,884 historical gap
records whose minutes are actually present, and a full-window query. The already
patched code measured:

| Operation | CPU median |
| --- | ---: |
| Uncovered-range check | 3,237 ms |
| Full standardized candle query | 3,625 ms |
| Same raw source query with standardize=False | 1.515 ms |

A profile attributed **2.859 seconds** to 33,768 ndarray searchsorted calls, from
one 16,884-gap check. These are profiler-instrumented times; the table uses
separate unprofiled process-time measurements. Repeated timestamp-sized
materialization represents approximately **11.67 GB** of potential copied bytes
for this workload; no hardware memory-bandwidth counter was collected.

An isolated proof reused **one contiguous timestamp vector for the synchronous
check**, leaving normalization, missing-span evaluation and all downstream policy
unchanged. In the paired probe, CPU fell from **2,411.075 ms to 186.715 ms**
(about 12.9×); the covered-gap result stayed empty. Another **500 randomized
comparisons** matched the unmodified implementation across unsorted/duplicate and
off-grid rows, synthetic timestamps, mixed gap reasons, missing intervals and
query bounds. This is a temporary benchmark prototype, not a committed fix.
Cross-call reuse would need additional mutation/ownership guarantees; the proof
only shares a vector within one synchronous ownership cohort.

Relevance limit: the HSL source reader explicitly requests `standardize=False`,
so it bypasses this standardization check. It must not be blamed for current HSL
CPU merely because the wide benchmark is slow. Standardized historical/EMA
reads can use the affected path; their live call frequency still needs evidence.
This is a strong general data-path fix candidate, not a quantified whole-bot gain.

## Lower-priority HSL source capture

A synthetic 43,200-row `capture_candles()` measured **415.847 ms CPU**; creating
a fresh native source from that tape measured **32.818 ms**. Source inspection
shows acquisition scheduled every **60 seconds**, not on every order check.
For one such one-minute-resolution tape, capture alone is roughly 0.69% of one
CPU averaged over a minute. Direct immutable native ingestion may reduce copying
and Python object creation, but this should not be presented as the principal
remaining load without actual source/symbol counts and acquisition-frequency
attribution. `_missing_spans()` also has a Python adjacency loop (13.164 ms for
43,200 rows); vectorization is possible but smaller than the proven wide-gap issue.

## Priority and validation boundary

Prioritize the current Cluster Sync/RPC startup and monitoring overhead for
additional steady VPS CPU reduction. Address the contiguous-timestamp defect
with focused parity tests for broad candle readers. Retain HSL source-capture
work as a smaller, frequency-dependent candidate.

No production code was edited, no functional fix was deployed or added to the PR,
and no full test suite was run. All benchmark assertions used isolated synthetic
data. GitNexus's PB8 index was refreshed to the published commit before final
caller analysis. `_slice_ts_range` resolves to the actual candle manager, and HSL
raw-source behavior was verified in `live/hsl_candles.py` and `hsl_live.py`.
Global indexed flow catalogs have known coverage limits; missing flows were not
interpreted as unused code.

Local evidence: `/tmp/manibot50-followup-process-accounting.jsonl`,
`/tmp/manibot50-followup-ssh-probes.json`,
`/tmp/manibot50-followup-ssh-identify.json`,
`/tmp/pbgui-followup-import-times.log`, `/tmp/pb8-followup-hotpaths.log`, and
`/tmp/pb8-followup-slice-proof.log`. No raw process arguments, environments,
configuration contents or credential files were collected into the report.
