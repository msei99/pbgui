# Candle-gap performance investigation

Follow-up to the first live-history performance patch, 16 October 2026.
The initial patch did **not** address the gap-processing bottleneck below; the
extended cumulative patch now includes the offline fix described at the end.
No live bot, configuration, candle index or service was modified. Existing local
worker checkout remains unchanged. No patch has been deployed.

## Main finding: historical gap metadata dominates unrelated recent reads

The deployed Hyperliquid one-minute candle index was approximately **3.2 MB** and
held **16,883–16,890 gap records** across successive read-only snapshots. The
corresponding Bybit indices had no gap records. An allowlisted snapshot used for
offline profiling contained 16,884 records: 16,882 `auto_detected` and two
`fetch_failed`, including 15,933 single-minute records. None overlapped the latest
day in that snapshot.

A separate read-only comparison with persisted minute timestamps found:

| Observation | Count |
| --- | ---: |
| Persisted distinct minute rows | 87,354 |
| Gap records in that later snapshot | 16,887 |
| Gap records fully covered by persisted timestamps | 16,884 |
| Partially covered records | 0 |
| Records with no persisted timestamps | 3 |

This checks timestamp coverage, not an independent audit of OHLC correctness.
Counts differ slightly because the live metadata continued changing between
read-only observations. No records were deleted or repaired.

## Expensive code paths

The pristine reference checkout is PB8
`82e0edf67a1f037b783d139039021ade8f449478`.

- `CandlestickManager._get_known_gaps_enhanced()` iterates and allocates a new
  normalized dictionary for **every stored gap** on each call.
- `_unverified_gap_ranges()` and `_unverified_uncovered_gap_ranges()` each obtain
  that full normalized collection before restricting it to the query window.
  A complete recent 500-minute window consequently pays for historical gaps
  which cannot affect its result.
- `_projection_shared_content_signature()` scans the complete gap collection
  **before checking the projected-EMA cache**. A warm cache hit still scales
  with historical metadata size.
- `_save_index()` serializes the whole gap collection when index metadata is
  written. The 3.2 MB index therefore adds recurring serialization and allocation
  cost independently of how few new candles arrived.

The disk loader merges persisted candles into memory without reconciling all
existing gap records against those loaded timestamps. Gap trimming exists on
`_persist_batch()` for its incoming rows. Thus existing historical rows can be
available while metadata for their formerly missing intervals remains retained.
The read path partly compensates by calculating uncovered intervals repeatedly.
This establishes a retention/read-cost problem; it does not establish which
historical writer originally produced every stale record.

These are general candle/EMA paths. The isolated reproduction performs **no HSL
evaluation**, so the problem does not require HSL to be active.

## Measurements

### Pure deployed methods measured on manibot50

A separate short-lived read-only diagnostic process extracted the two pure
reader/transform method bodies from the installed source AST. It used a stub
index reader over already-read candle metadata; it did not import/construct a
production manager, attach to a bot, write files or make exchange requests.
Median `time.process_time()` over three calls:

| Operation, 16,890 records | CPU time |
| --- | ---: |
| Normalize the complete gap collection | 26.217 ms |
| One `_unverified_gap_ranges()` check for the latest 500 minutes | 43.959 ms |
| Relevant gaps returned by that check | **0** |

These are actual VPS timings for the method bodies, excluding index I/O and
other work performed by a real bot. They are not full-process profiling or a
measurement of the number of calls in the running bot.

### Offline full-path isolation

The local harness uses synthetic dense prices and temporary manager caches,
retaining the shape of the allowlisted historical gap metadata with shifted
timestamps. Query rows and projected EMA results are asserted identical across
metadata sizes, and the projected cache is rebuilt separately for each case.

Representative final baseline run, unprofiled CPU medians:

| Operation | No historical gaps | 16,884 irrelevant historical gaps |
| --- | ---: | ---: |
| Read the latest 500 candles | 0.798 ms | 38.009 ms |
| Warm projected-EMA cache read | 0.065 ms | 15.143 ms |
| Serialize/write index | 0.508 ms | 33.523 ms |

Other repetitions measured roughly 36–84 ms for the candle request and 7–17 ms
for the warm EMA-cache read. The first performance patch leaves the gap methods
unchanged; its final control still measured 33.669 ms and 7.213 ms respectively.
Differences between repetitions are not evidence that the first patch fixes gap
processing. Synthetic managers without disk shards intentionally isolate this
metadata work from the earlier parsed-shard optimization.

An instrumented five-request profile made approximately **1.52 million
`dict.get` calls**. The two gap-check subtrees accounted for almost all measured
request time. Profiler instrumentation substantially increases absolute times;
use the separate unprofiled CPU medians for latency comparisons.

## Supporting live observations

- A 45-second `/proc/stat` window showed **50.49% actual CPU work**, 49.37% idle,
  0.14% I/O wait and zero steal. No continuous 100% saturation was present during
  that particular window.
- A later existing agent snapshot showed **100% for one second, 50.5% over
  61 seconds**; bot averages were 12.0% Bybit and 22.3% Hyperliquid. Peaks and
  sustained averages must be distinguished.
- Existing bounded EMA event samples showed Bybit bundle median approximately
  14.8 ms versus Hyperliquid 358 ms. Hyperliquid's measured candle-stage median
  was approximately 255 ms; EMA arithmetic itself was approximately 0.1 ms.
  These event measurements are wall time and include scheduling/I/O; they do not
  independently attribute CPU time to this gap bottleneck.
- A short process sample caught a Linux `check-new-release` CPU burst. Installed
  PAM/MOTD scripts can start this checker when its daily stamp is absent/stale.
  Its sustained contribution was not established. A roughly 5% SSH-child in the
  later process-sampling run was the diagnostic sampler itself; its shell was
  ambiguously labeled by a substring classifier and must not be blamed on a
  production collector.
- PBGui currently computes host CPU as `100 - idle`, including I/O wait and steal.
  That can overstate executing CPU work during swapping. In the measured
  45-second window this difference was only 0.14 percentage points, so it does
  not explain the measured work.

## Implication and next fix scope

The first patch alone is insufficient. This is an additional, demonstrated
bottleneck in common candle/EMA processing. Its exact share of total live bot CPU
still needs call-frequency/whole-process attribution; it is not evidence that
every historical 100% sample has one cause.

Prioritize a versioned normalized gap view and indexed range queries so recent
reads do not rebuild irrelevant historical records, including cache-key creation.
Preserve independent retry clocks, unresolved-gap policy, current-data validation
and complete reference results. Reconcile proven persisted coverage carefully;
do not blindly delete the gap index or weaken the three still-missing intervals.

Relevant Git history: `7a039fe6a` introduced the uncovered-range helper, and
`8295f07bf` introduced the shared projection content signature in July 2026.
The cost can grow as retained metadata accumulates; these findings are not proof
of a regression introduced by today's update.

Reference indexing included the large live-orchestration module. Direct caller
context resolves the gap helpers and EMA projection path; GitNexus reported
incomplete process catalogs, so absent flows were not treated as unused code.

Local diagnostic artifacts: `/tmp/pb8-gap-hotpath-profile.py`,
`/tmp/pb8-gap-hotpath-baseline.log`, and
`/tmp/pb8-gap-hotpath-with-first-fix.log`. The allowlisted runtime metadata remains
under `/tmp` and is not included in the repository or patch. The initial investigation made no code changes. The subsequent local fix and
focused validation are recorded below; no full test suite was run.

## Implemented offline fix

The cumulative artifact `patches/pb8-live-history-performance.patch` now adds:

- A lazily built interval view for owned JSON-loaded/newly persisted gap metadata.
  Prefix end maxima preserve nested/overlapping intervals, and selected records
  are restored to original input order. One view belongs to each metadata cohort;
  query windows do not create an accumulating cache.
- Mutation revisions for owned lists and records, plus existing external-file
  replacement detection. Injected unowned lists and newly inserted externally
  aliased containers use conservative scans. JSON serialization is unchanged.
- Window selection before normalization and projection-key construction. Legacy
  timestamp defaults and retry decisions are evaluated anew, not cached. Returned
  enhanced records remain detached.
- An early return before copying/sorting retained candles when no unknown gaps
  overlap the query. Full unresolved widths and real-versus-synthetic coverage
  retain their prior semantics. No gap records were deleted.

### Validation and measured effect

The test-suite gate required focused checks. The broad offline candle/EMA/HSL
selection passed **2,221 tests**, with **105 live-exchange tests intentionally
skipped**, in 259.47 seconds. After compacting the representation and hardening
inserted-alias handling, the final affected candle/cache selection passed
**1,793 tests**, with no skips or failures, in 13.57 seconds. These selections
overlap and their counts must not be added. Five documentation checks also pass.

The new regression module contains **43 tests**, including 480 randomized window
comparisons against frozen original full-scan algorithms; independent retry
clocks, duplicates/nested intervals, malformed/legacy metadata, full missing
widths, synthetic rows, mutation methods, external atomic replacement, mutable
return ownership and nonconstant-price full candle/projected EMA comparisons
are covered. A deterministic work-count check proves a warm recent window with
16,884 historical gaps neither rebuilds its interval view, normalizes historical
records nor copies retained candles. Tests use synthetic data only.

The unchanged Rust portion retains the previous independently built 3,072-output
parity result and 67 focused Rust HSL tests; the gap follow-up changes Python only.

The same offline workload, loading metadata through the real JSON ownership
boundary, measured these CPU-time medians across eleven warm calls:

| 16,884 historical gaps, zero current overlaps | Unpatched base | Final cumulative patch |
| --- | ---: | ---: |
| Recent 500-candle query | 44.177 ms | 0.942 ms |
| Warm projected EMA result | 14.631 ms | 0.182 ms |
| Shared projection key alone | 13.130 ms | 0.050 ms |

Candle outputs are exactly equal; repeated projected results are equal. The
metadata geometry comes from the allowlisted diagnostic snapshot, while every
price and manager is synthetic. Raw snapshot facts remain outside the repository.
Earlier patched runs measured 0.291 ms and 0.067 ms respectively; local CPU
conditions vary, so these are operation-level measurements, not predicted whole
VPS load reductions. The final representation adds approximately **1.99 MiB**
of retained Python allocations for a synthetic 16,884-record index relative to
plain JSON containers, measured with `tracemalloc`; this is not a process RSS
measurement or universal memory bound.

Full metadata reads and whole-index serialization still scale with history.
This fix removes the demonstrated repeated recent-window work; its effect on
production CPU remains unmeasured because no live code or runtime was changed.

Final scratch logs: `/tmp/pb8-gap-focused-final.log`,
`/tmp/pb8-gap-final-representation-tests.log`,
`/tmp/pb8-gap-hotpath-baseline-paired.log`, and
`/tmp/pb8-gap-hotpath-patched-final.log`.

A subsequent explicitly approved deployment and live measurement on manibot50
is recorded in [the targeted trial report](2026-10-16-manibot50-performance-trial.md).
The no-live-change statements above describe the original investigation and
offline implementation phases.
