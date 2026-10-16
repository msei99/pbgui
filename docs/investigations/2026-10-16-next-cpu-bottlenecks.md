# Next CPU bottlenecks after the three fixes

2026-10-16. Read-only investigation of manibot50 and isolated synthetic prototypes. No production source, configuration, service, bot process or kernel setting was changed. No profiler was installed or attached. Temporary probe scripts and files remained local; the VPS probes read existing public source and process metadata only.

## Current state and accounting

PBGui on the VPS is now `75278f11fcafe50d471d0941bf1d60e121717527` (v2.08.17), including the released Cluster/monitor fixes. PB8 remains on upstream `82e0edf67a1f037b783d139039021ade8f449478` with the approved overlays; candle source SHA256 is `8f7dc2162d433fe3328f76b714af8a2560d4932a4e2182b54df1f2b9f5ad57f1` (PR #1995 source).

A separate 60-second sample, 10:12:18–10:13:18 UTC, ran without concurrent VPS probes/builds:

| Measurement | Interval average |
|---|---:|
| Host CPU work | 45.51% |
| Host I/O wait / steal | 0.10% / 0.00% |
| HYPErQuantum | 10.63% |
| hl_HYPErQuantum5 | 17.90% |
| Combined bot CPU | 28.53% |
| PBRun own process | 3.13% |
| Monitor agent own process | 3.23% |

This sampler does not include reaped-child CPU in the per-daemon rows. They are not the complete daemon/SSH costs and must not be forced to sum to the host total. The sampler itself also consumes some CPU. These are current observations, not a controlled comparison against an earlier release.

## 1. PBRun resolves paths before rejecting unrelated processes

`RunV8._matches_process` constructs the complete expected command, resolves the instance config path and resolves the configured Python executable for every scanned process. Only afterward does it reject a nonmatching command line. `from_missing_config_process` likewise resolves Python before checking whether the command even has the PB8 live shape.

`watch_v8` runs the orphan scan and each runner's PID search during the five-second maintenance cycle. Two bots therefore repeat these invariants across much of the same process table. The live matches still require exact arguments and current cwd checks; those checks should remain.

A diagnostic extracted only these public read-only methods with AST, excluding constructors, configuration loading and all lifecycle methods. It compared them against a temporary early-negative wrapper on one in-memory snapshot of 126 real process argument lists. Arguments were never emitted or persisted. An argv without the literal `live` cannot match the original exact PB8 command; candidates containing it still used the complete original predicate.

Five-sample medians on the VPS, excluding initial snapshot acquisition:

| Predicate/pass | Current | Early-negative prototype |
|---|---:|---:|
| Exact managed-process matching | 23.836 ms CPU | 0.439 ms CPU |
| Missing-config/orphan matching | 11.734 ms CPU | 0.541 ms CPU |

Both variants returned exactly the same results for the observed cohort. The full-list probe is not an exact `pid()` timing: actual PID searches stop when the target is found, and command acquisition still has a cost. For a two-bot full-scan maintenance pass, the observed removable predicate work is on the order of 58 ms per five seconds (~1.2 CPU percentage points), conditional on comparable scans. It is not a measured whole-daemon saving.

A local 103-process synthetic pass independently reproduced 28.236 → 0.965 ms with identical results. Its profile attributed 37 of 52 instrumented milliseconds to 210 `Path.resolve` calls. Profiling times and uninstrumented medians are distinct measurements.

### Additional PBRun memory-query work

Each successful `RunV8.pid()` also calls `memory_full_info()`. Seven read-only samples per active bot measured medians:

| Bot | Full memory info | Basic RSS info |
|---|---:|---:|
| HYPErQuantum | 5.735 ms CPU | 0.023 ms CPU |
| hl_HYPErQuantum5 | 6.021 ms CPU | 0.018 ms CPU |

A blind replacement is **not equivalent**: `_memory_usage_bytes` uses both RSS and USS for the low-memory victim choice. A safe optimization would obtain full fresh information when the pressure decision needs it, while preserving that decision metric and any other consumers. The PID lookup should not perform this expensive measurement merely to answer process identity. At one query per bot every five seconds this component alone is roughly 0.24 CPU percentage points, not the entire 3.13% PBRun cost.

## 2. Monitor resolves both managed roots for unrelated processes every second

`_bot_identity_from_process` resolves the managed V7 root before checking for a V7 config argument, and the V8 root before checking whether there is a V8 config argument. The live loop does this for each process every second. Its `names` dictionary tracks start-time identity for CPU history; it does not bypass those path checks.

An AST-extracted copy of the exact current predicate was compared with a temporary wrapper that immediately returns `None` only when no argument has either suffix `/config_run.json` or `/config.json`. All candidates still pass through the complete original argument, root and cwd checks. No PID identity cache is needed for this prototype, so it does not introduce exec/restart/symlink invalidation state.

Seven-sample VPS medians on one 125-process cohort:

| Identity predicate over cohort | CPU |
|---|---:|
| Current | 12.717 ms |
| Early-negative wrapper | 0.600 ms |

Exact same identities, including both managed bots. With the current one-second cadence this is roughly 1.21 CPU percentage points of removable predicate work under the observed workload (~37% of the current agent's own-process CPU), conditional on steady comparable work. It excludes command acquisition and other live/collector work; it is not a deployed end-to-end result.

The local sandbox contained only a few readable processes and measured 0.469 ms for the full monitor scan. That small local result would have understated the actual VPS's 125-process workload.

## 3. Smaller or workload-dependent findings

### Rotation settings repeatedly parse the same INI

`get_rotate_settings` reads/locks INI for defaults and again for per-log settings. For a managed path it calls `get_managed_scope_settings`, which independently repeats the default and scope reads. `append_managed_transcript_line`, used for monitor samples, performs two reads through the managed-scope lookup even when settings are unchanged.

Tests used a temporary **630-byte, non-secret synthetic INI**, never the production INI. A temporary single-snapshot lookup preserved defaults, bounds, managed-scope precedence and filename-based overrides over 100 randomized configurations. The first managed-path prototype caught a filename-versus-service precedence mistake; that diagnostic prototype was corrected before collecting the successful result. No implementation change was made.

| Lookup | Reads currently | Current CPU | One-snapshot CPU |
|---|---:|---:|---:|
| PBRun daemon log | 2 | 0.4446 ms | 0.2494 ms |
| Synthetic managed `jobs` log | 4 | 0.9872 ms | 0.4177 ms |

The monitor transcript scope also performed two INI reads. One current snapshot per operation can avoid duplicate parsing/locking without a cross-call TTL or delayed settings updates. This is a small saving at one monitor line per second; high log volume or bulk settings lookup amplifies it. Existing issue #435 concerns related per-file settings work in the rotation API, not proof that this accounts for current VPS CPU.

### PB8 Fill transport still uses recursive dataclass conversion

`PairFills.payload()` in `live/hsl_inputs.py` uses `dataclasses.asdict` for every frozen fill. The canonical capture produces scalar-only `Fill` fields, so the recursive traversal/deepcopy is unnecessary for that contract. `CandleTape.payload()` already uses explicit scalar dictionaries for the analogous reason. `hsl_runtime.capture()` calls the fill payload while assembling fresh evaluation requests.

Local synthetic medians, seven samples, using the same exact scalar fields:

| Fill rows | Current asdict | Direct scalar dictionary |
|---|---:|---:|
| 100 | 0.183 ms CPU | 0.024 ms CPU |
| 1,000 | 1.747 ms CPU | 0.220 ms CPU |
| 10,000 | 20.971 ms CPU | 2.700 ms CPU |

500 randomized scalar tapes matched both dictionaries and serialized JSON exactly. This is an approximately eightfold transport-level opportunity with no retained trading decision. The actual current fill counts/evaluation frequency were not measured, so no live CPU share is claimed. A shipped fix would still need the complete factual-input and native-output parity tests, not only these payload comparisons.

## Checks and limits

- The renewed credential acknowledgement was inspected as a possible repeated-write problem, but `CredentialRuntimeJournal.record` already suppresses equal writes for 30 seconds. It is not reported as an every-five-second redundant-write defect.
- HSL history symbol capture belongs to source reads scheduled once per minute; it must not be misreported as an every-order scan.
- The one-second monitor/PBRun path work is the clearest newly demonstrated background opportunity. Its conditional estimates suggest a few CPU percentage points, not removal of the bots' remaining 28.53% CPU.
- Python sampling profiler is absent; the earlier inspected perf restriction is documented in the prior report. No tools were installed, debugger attached or kernel settings changed. The measured predicate probes are separate diagnostic processes, not live stack samples.
- No production functions were edited, fixed or deployed. No full test suite ran. Exact comparisons are offline/snapshot diagnostic assertions, not a replacement for regression tests before any future implementation.

Evidence: `/tmp/next-cpu-local-probes.log`, `/tmp/next-cpu-logging-probe.log`, `/tmp/next-cpu-logging-managed-probe.log`, `/tmp/next-cpu-fill-payload.log`, `/tmp/manibot50-next-perf-current.jsonl`. Remote predicate medians and full-memory query medians were returned as bounded JSON only.
