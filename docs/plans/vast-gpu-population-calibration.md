# Vast GPU population calibration

Status: deferred design; not implemented.

## Goal

PBGui should be able to run a controlled calibration for a rented GPU model,
identify the useful population-size throughput plateau for representative PB8
workloads, and reuse the verified settings for later matching jobs. PB8 itself
must remain unchanged by this feature.

The current hardware-based PBGui profile resolver remains authoritative until a
calibration feature is implemented and a matching profile has been verified.

## Calibration run

Keep one rental and one immutable input snapshot while testing increasing
population sizes. A first RTX 3090 calibration should measure:

1. 1,024
2. 2,048
3. 4,096
4. 8,192
5. 16,384
6. 24,576
7. 32,768 when the predicted VRAM margin permits it

Every case must use the same PB8 revision, seed, candidate inputs, strategy,
coins, scenarios, metrics and relevant GPU options. Exclude cold compilation
from the comparison and retain at least one warm measurement per size.

Record total proxy time, kernel time, candidate-bars per second, peak allocated
and reserved VRAM, GPU utilization, board power and time to the next Exact
validation. Stop before the next size when its predicted reservation would
exceed the configured VRAM safety margin. A failed or incomplete case must not
produce a recommendation.

## Selection

Choose the smallest population that reaches at least 95% of the best verified
candidate-bar throughput unless a later controlled study establishes a better
threshold. This favors bounded generation latency and memory use over a
negligible throughput gain.

After selecting the population, scale batch size and the existing Exact/drift
evidence settings together. The optimizer receives one fixed profile before it
starts; population size must not change during an NSGA-II run or after a
checkpoint has been created.

## Profile identity

A reusable profile must be keyed by more than the GPU name. At minimum include:

- GPU model and usable VRAM
- PB8 revision and worker-image identity
- strategy kind
- single-coin or multi-coin topology
- coin-count bucket
- active side count
- candidate-bar workload bucket
- kernel features that materially change resource use

Controlled calibration records are distinct from ordinary Performance History
runs. A PB8 revision or relevant workload-identity change marks the record for
revalidation. When no verified profile matches, PBGui uses its existing
conservative hardware profile.

## Product integration after validation

Expose calibration as an explicit PBGui test on a retained rental. Show every
case, live measurements, rejection reason and the final recommendation. Saving
the recommendation must be a separate deliberate action. Normal queue jobs do
not run calibration automatically.

Before implementation, define a bounded remote protocol, result schema,
timeouts, cancellation behavior, retained-rental behavior and offline tests for
selection, VRAM limits, profile matching and invalidation.
