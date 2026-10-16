# PB8 live history performance fix

Companion to `pb8-live-history-performance.patch`. This is a PB8 patch, not an
automatically activated PBGui runtime change.

## Base and implementation

Prepared in an isolated checkout of PB8 `82e0edf67`, the inspected deployment
source. The existing local PB8 checkout and its worker overlays were not changed.
The patch applies cleanly to that base; integration with other changes needs the
normal merge/review process and a rebuilt Rust extension. Nothing was committed
or pushed. Initial preparation made no live changes; the subsequent separately
approved manibot50 installation and before/after trial are documented in
`docs/investigations/2026-10-16-manibot50-performance-trial.md`. Other hosts were
not changed.

Changes:

- Cache immutable parsed disk rows for at most 16 exact ranges and eight MiB per
  candle manager. Check shard/index identity, size, nanosecond modification time
  and change time on each reuse. Atomic replacements, in-place corrections,
  inaccessible sources, strict audits and rewrites during parsing cannot inherit
  an unchanged-source certificate.
- Preserve detached array ownership and current memory merging. Identical sorted
  source overlaps avoid concatenating, sorting and deduplicating the whole RAM
  history. Changed, duplicate, unsorted and NaN-containing inputs retain the
  existing last-write-wins path.
- Reuse the history prepared within the current HSL evaluation when every selected
  price grid contains the exact dense minute prefix and omits only the non-minute
  current endpoint. The reconciler already appends that endpoint from the current
  observed mark. Preserve the reference normalizer's diagnostic reason. Sparse,
  empty, off-grid, invalid and differently observed grids keep full normalization.
- Index owned historical gap records by interval and mutation revision. Select
  overlapping records before normalization and shared projection-key creation;
  skip candle-history copies for windows without unknown gaps. Preserve source
  order, full missing widths, synthetic-versus-real coverage, legacy clock defaults,
  retry clocks, external writer reloads and detached enhanced results. Unowned or
  externally aliased inserted containers retain conservative scans. No gaps are
  deleted and the JSON schema is unchanged.
- Add regression tests and update PB8's changelog and subsystem contracts.

There is no cache of prior trading decisions, GREEN permissions, EMA decisions or
cooldown authority. Current balance, positions, fills, marks, policies and order
admission remain freshly evaluated.

## Validation

An independent native extension was first compiled from the unchanged base.
Its source fingerprint is
`1d6d2247fb05c77e84aece11d4176a5631f76cb13544b69e6bdef1d1fe7ddd56`.
The final patched extension was rebuilt and verified against source fingerprint
`77d78087d87c4dac55a5590ac9385594b47105d57bac6f35e41515f22e6280a5`.
Both builds were kept separate from the existing PB8 environment's extension.

- **3,072 complete canonical JSON outputs match the independently compiled base**:
  2,073 normal, 355 panic, 584 halted and 60 inactive. Corpus coverage includes
  coin/pside/unified modes, long/short, inverse contracts, fractional spans, window
  boundaries, non-minute clocks, missing/invalid/off-grid prices, fill revisions,
  flat histories, balance changes and restart/cooldown policies.
- **67 focused Rust HSL tests pass**, including 2,304 generated cases comparing
  complete live outputs and simulator outputs/cursors with forced full
  normalization/reconstruction. Aggregate cases include multiple contributing
  pairs and mixed current-endpoint availability.
- **1,073 focused Python tests pass** on the final source-verified build. Coverage
  includes native-grid versus JSON transport, current-fact
  changes, disk invalidation, same-size/same-mtime replacements, concurrent writes,
  strict-read failures, mutable consumer arrays, cache budgets, restart, independent
  merge-reference parity, ordinary candle/EMA behavior and offline fake-live HSL.
  The run took 276.64 seconds. Warnings were the existing portalocker timeout
  warning and deprecated UTC conversion; there were no test failures or skips.
- `cargo check --offline --tests`, targeted `rustfmt --check`, and five AI-doc tests
  pass. The test-suite gate returned **focused**; no full repository suite was run.
- Patch application and reverse application were checked without modifying the
  original checkout.

The 3,072-output corpus SHA-256, after canonical JSON serialization, is
`529b618b34351e96b4a87534052d27965b06bb320c4f815d2b41f8ad11089e5f`.

### Gap follow-up validation

The candle/EMA/HSL focused selection passed **2,221 tests**, with **105 live
exchange tests skipped**, in 259.47 seconds. After compacting the interval view
and preserving inserted aliases, the final affected selection passed **1,793
tests** with no skips/failures in 13.57 seconds; five documentation checks pass.
These selections overlap and are not additive. The Rust source and source-verified
extension were unchanged by the Python-only follow-up.

`tests/test_gap_metadata_performance_parity.py` adds 43 regression cases, including
480 randomized window comparisons with frozen full-scan reference algorithms,
local record/list mutations, inserted aliases, external atomic replacements,
legacy defaults, retry eligibility, detached return values, missing-width and
synthetic-coverage behavior. Full nonconstant-price candle and projected EMA
outputs are compared cold and warm. A deterministic work-count check confirms
that a recent warm window with 16,884 old gaps performs no historical
normalization, range-index rebuild or retained-candle copy.

## Measured performance

All measurements are local CPU time, not live VPS profiling. The initial probes
use fully synthetic inputs; gap probes use allowlisted historical gap geometry
with synthetic prices, clocks and isolated managers. Raw metadata is not included
in the patch or repository.

| Workload | Before | Patched |
| --- | ---: | ---: |
| 500-row read with 200,000 retained candles | 14.6 ms | 3.2–5.5 ms |
| Warm shard reads over ten requests | 10 | 0 |
| Rust HSL, 30 days, 24 fills, non-minute clock | 12.0 ms | 3.4–3.6 ms |
| Full HSL capture and evaluation with four source resolutions | 26.2 ms | 16.3–17.9 ms |
| Recent 500 candles, 16,884 unrelated historical gaps | 44.177 ms | 0.942 ms |
| Warm projected EMA result, same historical gaps | 14.631 ms | 0.182 ms |
| Shared projection key, same historical gaps | 13.130 ms | 0.050 ms |

The measurements preserve outputs. Source reprojection/copying still costs time;
full metadata reads and whole-index writes still scale with history. The compact
range representation adds approximately 1.99 MiB of retained Python allocations
for a synthetic 16,884-record index relative to plain JSON containers; this is
not a process RSS measurement or a universal memory bound. This patch does not
claim to remove all remaining VPS CPU load. A subsequent controlled live trial
measured 19.5% lower combined bot CPU and 11.1% lower host CPU work over matched
three-minute windows after warm-up. Peaks remain; the report linked above
documents restart/cache effects and other limitations.

## Reproduction

Review/apply the patch in a suitable isolated PB8 checkout, rebuild and verify the
extension using PB8's Rust-extension runbook, then run the focused tests. The new
files are `tests/test_live_performance_parity.py` and
`tests/test_gap_metadata_performance_parity.py`; Rust reference comparisons are in
`hsl_evaluator::tests::minute_prefix_reuse_preserves_complete_reference_outputs`.

The local completed work is in `/tmp/pb8-live-performance-fix`. Test and benchmark
logs use `/tmp/pb8-perf-*`. These temporary artifacts supplement the durable patch;
the patch itself contains the implementation, tests and generic PB8 documentation.

## Upstream pull request

The source patch, regression tests and generic PB8 documentation are published
as [PR #1994](https://github.com/enarjord/passivbot/pull/1994), commit
`fb8bf8c644df4d4901ac41f736b1957491393817`, against upstream `master`.
Private operator reports and VPS evidence are not part of that PR. The PR is
ready for review; publication does not imply upstream approval or merge.
