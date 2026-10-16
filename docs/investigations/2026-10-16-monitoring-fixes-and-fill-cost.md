# Monitoring fixes and sustained PB8 fill transport cost

2026-10-16. PBGui changes were made directly in `/home/mani/software/pbgui`. No bot/VPS source, configuration, services or running processes were modified. The PB8 experiment replaced a method only inside separate diagnostic Python processes; no PB8 source or PR was changed.

## Implemented PBGui changes

1. `RunV8._matches_process` and `from_missing_config_process` reject argument lists without the literal `live` before resolving configured Python/config paths. Every previously accepted command requires that argument. Candidates retain the original complete command, cwd, symlink and missing-config checks. No PID cache or altered memory-pressure metric was introduced.
2. `_bot_identity_from_process` skips root/path resolution only when no argument ends with `/config_run.json` or `/config.json`. All potential V7/PB8 processes retain exact identity validation on every sample, including changes after exec/restart. The one-second cadence and CPU-history bookkeeping remain unchanged.
3. Logging rotation getters acquire one fresh locked INI parser per public lookup. Defaults, managed-scope overrides and physical filename/service overrides are resolved from that parser. No cross-call cache/TTL is added. Invalid/default/interpolation fallback behavior, clamps, unknown-scope errors, transcript redaction and rotate/write locks are preserved. In particular a bad optional file override falls back to the configured global defaults, not hardcoded defaults.

API startup/runtime serial was incremented to 3266. Nothing was committed, released or deployed in this task.

## Regression validation

The test gate returned `focused`; no full suite ran.

The initial focused run in the shared workspace had **347 passes and two audit failures**. Both failures named unversioned files outside this change (`scripts/stress_inventory_cache_init.py` and `.local/sk_doge_research/...`); none of those paths are tracked. They were neither edited, hidden nor skipped.

A clean shared clone at the existing PBGui HEAD passed **all 13 unchanged logging policy audits**. Copying the exact three edited production modules, runtime serial, edited supervisor/monitor tests and new logging regression file into that clone then passed **all 349 focused tests**. The selection includes supervisor identity/signals/credentials, monitor contract, logging API/migrations/redaction/concurrent writers and policy audits. Fourteen warnings concern existing Pydantic/deprecation behavior.

New regressions require:

- no path resolution for unrelated process commands, while existing positive/decoy/cwd/PID-reuse tests still run;
- exactly one INI read per defaults/scope/service/file lookup and managed append/rotation operation;
- 1,000 comparisons against the frozen prior stable-file lookup semantics over invalid values, bounds, managed scope and filename precedence;
- coherent parser generations during lookup and immediate visibility of actual settings replacement;
- the original distinct fallback behavior for damaged ConfigParser interpolation in defaults, scope and filename settings.

JUnit: `/tmp/next-cpu-clean-validation.xml`. Baseline audits: `/tmp/next-cpu-clean-audit-baseline.log`. Initial workspace results: `/tmp/next-cpu-fixes-validation.log`.

## Concrete edited-code CPU check

A separate local probe compared the actual edited methods with the committed HEAD versions, using synthetic process facts and temporary non-secret INI/log paths. Five-sample process-time medians:

| Operation | Prior code | Edited code |
|---|---:|---:|
| PBRun managed-process predicate, 126-process cohort | 17.7015 ms | 0.2214 ms |
| PBRun orphan predicate, same cohort | 12.9798 ms | 0.3060 ms |
| Monitor identity predicate, same cohort | 8.7586 ms | 0.5272 ms |
| Managed rotation lookup | 2.9858 ms | 0.9973 ms |

All results matched exactly. These measure predicate/lookup operations, not the whole daemon or VPS. The work-count regressions avoid brittle timing thresholds. Earlier read-only VPS prototypes and their conditional whole-host estimates remain in `2026-10-16-next-cpu-bottlenecks.md`; this task did not install the new PBGui source remotely.

Evidence: `/tmp/pbgui-next-fixes-benchmark.log`.

## PB8: real workload size and frequency

Bounded startup logs report **229 cached fill events for HYPErQuantum** and **92 for hl_HYPErQuantum5**. These are startup cache counts, not an in-process measurement of the exact current HSL window. Retained HSL rows can be fewer; later fills can change them. No credentials, configuration contents or trade details were read/emitted for this check.

Existing monitor `current.ndjson` envelopes provided the following recent persisted cycle observations:

| Bot | Completed cycles | Observed event span | Cycles/second |
|---|---:|---:|---:|
| HYPErQuantum | 55 | 153.8 seconds | 0.3576 |
| hl_HYPErQuantum5 | 54 | 235.6 seconds | 0.2292 |

Only a bounded two-megabyte tail was examined. These are observed persisted cycle rates, not a count of every HSL capture call. Source tracing shows a protective capture each successful loop, a second when a ready ordinary plan is serviced, and additional fresh captures for order admission. The calculation below assumes one to two payload conversions per observed cycle; extra admission calls or dropped telemetry would change it.

## PB8: same-hardware conversion cost

A separate read-only VPS process extracted only the frozen `Fill` and `PairFills` dataclasses from public source and populated them with synthetic scalar data. It did not import lifecycle code or read financial caches. Five batches of 100 conversions per variant yielded median CPU per conversion:

| Synthetic rows | Existing asdict | Direct scalar dict | CPU saved |
|---|---:|---:|---:|
| 229 | 0.7770 ms | 0.0998 ms | 0.6772 ms |
| 92 | 0.3029 ms | 0.0396 ms | 0.2633 ms |

Payloads matched exactly. Combining these costs with the observed rates gives an **illustrative combined 0.030–0.061 CPU percentage points** at one to two conversions per cycle. That corresponds to approximately **26–52 CPU seconds per day** on one core. This is a conditional workload model, not a measured bot CPU reduction.

The cost occurs anew on each call. It does not grow simply because the bot has been running longer. It scales with the number of retained fills and capture/admission frequency. Therefore the eightfold conversion improvement is technically real but is unlikely to materially reduce current total VPS CPU for these small tapes.

## PB8: full sustained factual/native comparison

An isolated synthetic test exercised the real `hsl_runtime.capture` and verified native evaluator with 30-day/43,200-candle sources, current positions/quotes, observed fill provenance and 92/229/1,000 fill rows. Each fixture ran 100 alternating before/after pairs. All **600 repeated complete request-metadata/native-decision comparisons** matched, with explicit assertions that requests and native decisions were nonempty and no scopes were unavailable.

Only `PairFills.payload` was swapped within the diagnostic process, between its existing implementation and an explicit scalar mapping. The source tree was unchanged. The same native extension and Rust source fingerprint were verified before execution. This is repeated execution over three synthetic factual states, not 600 distinct randomized trading scenarios or a proof over all future states.

Full-pipeline timing varied substantially across local runs. At the current-sized tapes, small total differences were close to unrelated timing variation; it would be misleading to claim an eightfold HSL or bot speedup. At 1,000 fills the observed total CPU saving was around three milliseconds per evaluation, illustrating the larger-workload case. Earlier isolated scalar tests also matched 500 randomized payloads and serialized JSON strings.

Evidence: `/tmp/pb8-fill-sustained-probe.log`, `/tmp/pb8-fill-sustained-nonempty-validation.log`. These are diagnostics, not a shipped PB8 fix or replacement for production-path regression coverage.

## Decision

The three PBGui fixes are ready locally with focused validation. Keep PB8 scalar fill conversion as a lower-priority optimization for the current two bots. A future implementation should preserve detached scalar payloads and run complete factual-input/native parity tests; no prior trading permission or decision may be cached to save these conversion costs.
