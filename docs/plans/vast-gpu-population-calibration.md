# Vast GPU population calibration

Status: implemented in PBGui; live provider validation remains to be performed.

## Goal

PBGui should be able to run a controlled calibration for a rented GPU model,
identify the useful population-size throughput plateau for representative PB8
workloads, and reuse the verified settings for later matching jobs. PB8 itself
must remain unchanged by this feature.

The hardware-based PBGui profile resolver remains the fallback until an exact
accepted local or bundled calibration profile matches the runtime GPU variant
and workload.

## Calibration run

Keep one rental and one immutable input snapshot while testing increasing
population sizes. A first RTX 3090 calibration starts with these mandatory
anchor sizes:

1. 1,024
2. 2,048
3. 4,096
4. 8,192

Further sizes are chosen adaptively from 12,288, 16,384, 24,576 and, when the
predicted VRAM margin permits it, 32,768. Do not assume that doubling from
8,192 to 16,384 is the next useful measurement.

Every case must use the same PB8 revision, seed, candidate inputs, strategy,
coins, scenarios, metrics and relevant GPU options. Exclude cold compilation
from the comparison and retain at least one warm measurement per size.

Record total proxy time, kernel time, candidate-bars per second, peak allocated
and reserved VRAM, GPU utilization, board power and time to the next Exact
validation. Stop before the next size when its predicted reservation would
exceed the configured VRAM safety margin. A failed or incomplete case must not
produce a recommendation.

### Bounded measurement window

Use PB8's periodic progress feedback to avoid waiting for a whole generation.
After cold compilation and other one-time startup work have been excluded, wait
for the first usable 300-second progress interval, persist its cumulative
counters and resource measurements, then stop PB8 cleanly before starting the
next population size. A generation may therefore be deliberately truncated;
that is a valid sampled calibration case, not an incomplete case, when PB8 has
reported a full warm interval and the counter deltas are internally consistent.

Do not treat a startup heartbeat, a partial interval, a zero-work interval or
an estimated percentage alone as a measurement. Throughput must be derived
from monotonic completed-work counter deltas and elapsed active time. Give each
case a bounded grace period beyond the expected feedback interval; timeout,
missing progress, cancellation failure, PB8 error or inconsistent counters
invalidate that case. Always wait for confirmed process termination before
changing the configuration or starting the next case.

The result records whether a case completed a generation or was intentionally
stopped after a valid warm interval. A failed or cancelled case stops the
adaptive sequence and cannot create an accepted recommendation.

### Adaptive population sequence

Protocol version 2 always runs the 1,024, 2,048, 4,096 and 8,192 anchors, then
continues in exact 4,096-candidate steps: 12,288, 16,384, 20,480 and upward.
There is no special 32k ceiling. A step gaining at least 10% over its immediate
predecessor continues probing. A smaller positive gain keeps that newly tested
population and stops; a regression returns to the preceding population. A
projected 15% VRAM reserve, rental deadline or 131,072 safety ceiling also
stops the sequence. The worker records the stop reason so a deadline-limited
result does not claim that its last population is the scaling plateau.

These thresholds live in the worker and local evidence validator rather than
as UI-only decisions. Protocol-v1 evidence retains its original 95%-of-best
validation and remains eligible after the v2 worker is published.

## Selection

Choose the last positively improving population. Continue beyond it only while
the latest measured step gains at least 10%; return to the preceding population
only after an actual regression or capacity failure.

After selecting the population, scale batch size and the existing Exact/drift
evidence settings together. The optimizer receives one fixed profile before it
starts; population size must not change during an NSGA-II run or after a
checkpoint has been created.

## Profile identity

A reusable profile must be keyed by more than the GPU name. At minimum include:

- provider GPU model and advertised VRAM
- runtime-reported GPU product name and exact total VRAM in MiB
- stable runtime device/board identifiers exposed by the NVIDIA tooling, when
  available, plus memory bandwidth and compute capability
- PB8 revision and worker-image identity
- strategy kind
- single-coin or multi-coin topology
- coin-count bucket
- active side count
- candidate-bar workload bucket
- kernel features that materially change resource use

Derive a versioned `gpu_variant_fingerprint` from the normalized hardware
fields that describe a reusable card variant. Never key a reusable profile by
GPU marketing name alone. In particular, a modified card with non-standard
VRAM must have a different fingerprint from the ordinary model even when Vast
reports the same `gpu_name`. Preserve the exact raw provider and runtime values
beside the fingerprint so the identity can be audited and re-derived after a
schema change.

The Vast offer ID and machine ID identify the offer and physical host, not the
GPU type. Retain both as provenance and use the machine ID to compare repeated
tests on the same host, but do not require the same host when matching a
verified reusable variant. Do not merge records when runtime identity is
missing or contradicts the offer. Such a run may remain visible as evidence but
must not create or update a reusable recommendation.

Controlled calibration records are distinct from ordinary Performance History
runs. A PB8 revision, progress-protocol revision or relevant workload-identity
change marks the record for revalidation. When no verified profile matches,
PBGui uses its existing conservative hardware profile.

## Shipped reference profiles

PBGui releases may include project-maintained reference profiles produced by
the same calibration protocol. Users therefore do not need to rent and retest
common GPU variants before benefiting from verified population sizing. Bundle
these profiles as versioned, read-only application data, separate from local
history and locally accepted profiles.

Every shipped profile must include its exact `gpu_variant_fingerprint`, raw
hardware identity, PB8 revision, worker image, calibration protocol version,
workload identity and bucket, tested population cases, measurement summaries,
selection thresholds, test date and PBGui release provenance. Do not ship only
the final population number. The workload identity proves which canonical
comparison produced the evidence; it is not compared with a later user's coin
selection. A profile is eligible only when its exact runtime GPU identity and
calibration/PB8/worker revisions match; a standard card profile must never
cover a same-name non-standard-VRAM mod card.

Resolve profiles in this order:

1. an accepted, current local profile with an exact variant and current calibration revisions
2. a current shipped reference profile with the same exact match
3. the existing conservative hardware resolver

A local test is always optional. The offer UI must identify the source as
**Local test**, **PBGui reference** or **Conservative default**, show whether it
is current, and keep **Run performance test** or **Repeat performance test**
available. Running a local test must not edit bundled data. Accepting its result
creates a local override; rejecting or deleting that override reveals the
matching shipped reference again.

Treat bundled profiles as release artifacts: validate their schema and internal
measurement evidence in offline tests, review additions like code changes and
never learn or upload them automatically from user history. PB8, image or
protocol incompatibility makes a bundled profile ineligible rather than
silently migrating its recommendation. Historical bundled and local results may
remain visible even when they are no longer eligible for automatic use.

## Product integration after validation

Place the calibration entry point in **GPU & Offers**, beside the existing
**Rent** action. Selecting an offer automatically shows one of these states for
its verified GPU-variant identity:

- no verified performance profile
- verified local or PBGui reference profile available, including its source,
  age and PB8/protocol revision
- profile needs revalidation
- offer identity is ambiguous until the rented GPU reports its runtime identity

The status must update automatically when the selected offer changes, when a
test finishes and when the user returns to the view. Do not add a manual refresh
control. Before rental, the status is a preliminary match based on provider
metadata; after startup, replace it with the authoritative runtime-verified
variant match. Make that distinction visible so a standard card is never shown
as covering a same-name mod card.

Add an explicit **Run performance test** action beside **Rent**. When a verified
profile exists, keep the action available as **Repeat performance test** rather
than hiding it. Starting either action is a paid rental operation and must show
the selected offer, maximum duration, budget estimate, cleanup behavior and
whether the new result will be a same-host repeat or a new-host validation in
the shared confirmation dialog. A test must never start from row selection
alone.

PBGui constructs the calibration input itself from a versioned canonical
workload. The canonical workload fixes BTC, ETH and SOL, Binance, the 2024 date
range, seed 7 and the `adg_strategy_eq` objective. User configs and arbitrary
queue jobs are never eligible inputs. On confirmation PBGui validates and
exports the corresponding local candles into an immutable snapshot containing
the config, PB8 revision, image and workload identity before any rental is
created. Missing canonical market data stops preparation without renting.

Once a calibration starts, all population cases use that same frozen snapshot;
only the population and explicitly defined coupled calibration settings may
vary between cases. Repeats rebuild the same canonical workload version and
preserve earlier evidence. A changed canonical workload requires a new explicit
protocol/workload version and does not silently replace older recommendations.

Before confirmation, show a conservative upper-bound duration and cost from
startup, the mandatory anchor cases, possible adaptive probes and transfer
allowances. The rental deadline and approved budget must cover that bound with
the defined grace period. If the adaptive sequence finishes early, stop and
clean up early rather than consuming the remaining allowance.

Run calibration as its own durable queue job type so browser navigation, API
restart and reconnect do not lose ownership or progress. It may use an already
retained compatible rental when explicitly selected; otherwise it rents the
exact selected offer and applies the normal deadline, budget, cancellation and
verified-cleanup guarantees. Normal optimization queue jobs never trigger
calibration automatically and calibration must not silently consume a rental
reserved by another job. One rental may run only one calibration case at a time,
and normal jobs must not start on it until calibration has ended or been
explicitly cancelled and process termination has been confirmed.

Show every population case, live measurements, rejection reason and the final
recommendation in the existing queue/job-detail workflow. Link completed tests
to Performance History, but store calibration cases and the active reusable
profile as distinct record types. Saving or replacing the active recommendation
must be a separate deliberate action. Repeating a test preserves prior records;
it must not overwrite the current verified profile until the new calibration
finishes validly and the user accepts it. Failed, cancelled or contradictory
repeats leave the previous profile unchanged.

The bounded remote protocol and result schema cover timeouts, cancellation,
selection, VRAM limits, standard-versus-mod-card identity, preliminary versus
runtime-verified matching, bundled-profile validation and precedence, repeat
preservation, profile replacement and invalidation.
