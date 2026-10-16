# Live HSL and candle-cache performance investigation

Investigated 15–16 October 2026. No bot code, configurations, services or remote
files were changed. Remote work consisted of read-only source/configuration
inspection, bounded event-log aggregation and process/thread CPU sampling.
All workload experiments ran locally with synthetic data and temporary caches.

## Findings

### 1. Repeated disk reloads merge unchanged candles into the entire RAM history

The deployed PB8 `82e0edf67` `src/candlestick_manager.py` calls
`_load_from_disk()` in the one-minute `get_candles()` path before slicing memory.
The loader reads intersecting daily shards and calls `_merge_overwrite(existing,
merged_disk)` even when those shards have not changed. Historical overlaps enter
the concatenate/sort/deduplicate path rather than the append fast path. A small
query therefore repeatedly copies and merges the much larger retained history.

This was reproduced using the **exact deployed Python file**, obtained from its
Git commit and verified against manibot50 by SHA-256:
`03e2d8c890610a2b6aef82b8658b4d20526e295493b0cc4bacf0f31f1e584799`.

Every request below asks for the same 500 one-minute candles. The disk shard and
returned values remain unchanged. CPU time is the median of ten unprofiled
`time.process_time()` measurements on the local machine.

| Retained RAM candles | Existing path | Unchanged-shard control |
| --- | ---: | ---: |
| 1,440 | 0.453 ms | 0.032 ms |
| 43,200 | 3.561 ms | 0.184 ms |
| 200,000 | 17.032 ms | 0.700 ms |

The control temporarily replaces disk loading **only on the synthetic manager**;
all resulting 500-row arrays are identical. It is evidence of redundant work,
not a production fix: actual shard changes must still be detected and loaded.
Each ordinary request reread the daily shard. A separate profile at 200,000
candles attributed approximately 136 of 160 ms over ten requests to
`_merge_overwrite()` (about 85%).

Both running bots configure `max_memory_candles_per_symbol=200000` and 30-day HSL
lookbacks. Their actual in-process candle counts were not inspected. The result
establishes a real scaling defect, not that either bot currently holds 200,000
candles. Additional history retained by HSL can increase the cost of other
candle consumers as they share this manager.

The higher-timeframe branch has a related ordering problem: it calls
`_load_from_disk()` **before checking `_tf_range_cache`**. Ten valid warm RAM
cache hits caused ten disk-loader calls. With a synthetic 30-shard range, seven
RAM hits caused **210 shard reads**, approximately 3.4 ms CPU per request locally.
All requests still returned the original RAM array. This branch is also used by
HSL's four-resolution acquisition, scheduled every 60 seconds.

### 2. Ordinary live timestamps force an extra HSL reconstruction

`HslCandleSource.project()` produces minute-aligned rows. At a live timestamp
with seconds/milliseconds, it does not include the exact current endpoint.
However, `already_normalized()` requires that endpoint. Consequently,
`evaluate_inner<false>()` first runs `snapshot::prepare()`, then rejects the grid
as unnormalized, drops the prepared history, normalizes the whole grid and
reconstructs again through `compose_with_cashflow_peaks()`.

The condition and duplicate path exist in the exact deployed evaluator
(`passivbot-rust/src/hsl_evaluator.rs`, SHA-256
`d92f93fcb93619020ac50f64a62379625ef8e744eb750f65c52c1c98ff5345dc`).
Earlier minute-aligned microbenchmarks missed this usual live path.

A paired synthetic 30-day JSON-binding experiment held transport and account
facts constant, adding only the missing current endpoint for the control:

- First run: **19.001 versus 8.689 ms** CPU.
- Repeated run: **15.697 versus 10.073 ms** CPU.
- Decisions, lifecycle events, observation counts and episode counts matched.

These are local extension timings. Its build information reports an **unknown
source fingerprint**; they are not an attestation of the deployed native binary.
The local evaluator includes pre-existing worker simulator changes, whose relevant
diff is guarded by `SEED`; live takes `false`. The duplicate-work condition was
independently checked in deployed source. A production change needs source-matched
builds and parity checks for clock offsets, gaps, conflicting observations,
positions, fills, current marks and RED/cooldown behavior.

### 3. Full source reprojection and Python candle copying add measurable work

A synthetic one-pair, 30-day capture with 24 fills and all supported source
resolutions (1/5/15/60 minutes) contained 55,444 source rows. Local measurements:

- Combined capture and live-binding evaluation: approximately **20–25 ms CPU**.
- Projection alone: approximately **10–11 ms**, versus about 1–3 ms with a
  complete one-minute source alone. Coarser candles are expanded and sorted
  before finer-source arbitration, even across already covered timestamps.
- Copying the four structured NumPy arrays into Python Candle objects:
  approximately **280–286 ms CPU**, repeated at source acquisition rather than
  every decision. This creates new immutable Sources and native copies each time.

The native source object itself is cached; the projected grid is recomputed on
each capture. Protection, ordinary planning and actual order admission can each
invoke capture. Permission freshness cannot safely be replaced with cached GREEN
decisions. A safe optimization would reuse validated factual history with proper
window/source invalidation and still evaluate current account/market facts.

Real retained fill counts were only **16 Bybit / 24 Hyperliquid in 30 days**
(229/92 across stored history). Large-fill stress tests scale badly but do not
explain this deployment: at 24 fills the Python fill work was small in the profile.

## Production observations and limits

- A ten-second read-only thread sample measured about **10.2% CPU Bybit and
  19.9% Hyperliquid**. Almost all was in their main threads; their busiest
  background threads used approximately 0.6%/0.4%. This sample did not show
  continuously saturated CPU by the two bots alone.
- Bounded event tails showed Rust order-orchestrator medians of **2 ms** for both
  bots, with maxima 18/6 ms. These timings exclude HSL evaluation and do not
  establish that every Rust call is cheap.
- Completed-cycle wall-time medians were approximately 789/1755 ms and include
  network waits and scheduling. `ordinary_prepare_elapsed_ms` is observed when
  a later cycle collects a background task, so its 2–3 second values cannot be
  interpreted as pure preparation CPU time.
- Live event tails contained roughly 8/5 events per second. No debug setting was
  enabled for this investigation. The source/phase evidence does not support
  attributing all remaining host CPU to event logging.
- No installed py-spy was found; perf is present but
  `kernel.perf_event_paranoid=4`. No profiler was installed, kernel setting
  changed, or debugger attached to a live process.

The redundant cache merge and HSL reconstruction are demonstrated performance
problems. Their precise share of VPS CPU remains unmeasured; local microbenchmarks
cannot establish the cause of every 100% host sample. Prioritize unchanged-shard
invalidation/cache reuse and endpoint normalization before porting optimizer-only
rolling decision state into live execution.

## Reproduction artifacts

Local synthetic harnesses and captured outputs from this session:

- `/tmp/pbgui_hsl_perf_20261015.py` and `.log`: capture/evaluation/source-copy
  measurements and Python profile; synthetic fills, no manager or exchange I/O.
- `/tmp/pbgui_hsl_cache_probe_20261015.py` and `.log`: higher-timeframe warm-hit
  reads and paired current-endpoint evaluation.
- `/tmp/pbgui_candle_memory_probe_20261016.py` and `.log`: exact deployed Python,
  synthetic daily shards, retained-history scaling and identical-output control.
- `/tmp/pbgui-candlestick-manager-live.py` and
  `/tmp/pbgui-hsl-evaluator-live.rs`: source extracted from `82e0edf67`.

Harness interpreter: `/home/mani/software/venv_pb8/bin/python` with
`PYTHONDONTWRITEBYTECODE=1`. Manager construction and every cache write were
confined to fresh `/tmp` directories. No full suite was run for this documentation
and diagnostic-only work. No runtime/API source changed, so no API serial bump
was needed.
