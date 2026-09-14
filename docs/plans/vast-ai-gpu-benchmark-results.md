# Vast/PB8 GPU measurement, 13 September 2026

The RTX 3090 runs the pinned PB8 GPU optimizer successfully. For this ETH suite,
four exact CPU workers remove the large one-worker bottleneck; eight workers
do not improve throughput. A longer sixteen-worker GPU run is in progress at
the user's request. No CPU-only cloud benchmark was executed.

## Equal short measurement windows

Each row covers the first 25 committed GPU generations: 25,600 candidate
evaluations, each over five scenarios (128,000 scenario evaluations). All rows
have 184 exact evaluations recorded at the last generation-profile boundary.
The subsequent graceful interrupt can record a few additional completed CPU
results, explaining the different saved result counts.

| Exact CPU workers | GPU loop elapsed | Relative throughput | Mean sampled GPU utilization | Saved exact results |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 19 min 40 s | 1.00× | 24.8% | 188 |
| 4 | 7 min 00 s | 2.81× | 79.7% | 192 |
| 8 | 7 min 00 s | 2.81× | 76.0% | 187 |

Loop elapsed is reconstructed from native log timestamps (one-second
resolution) and the first generation's measured duration. Sub-second differences
between the four/eight-worker rows are not meaningful. This is the same GPU
optimizer with different exact-worker counts, not a GPU-versus-CPU-only speedup.
Full runner durations, including preparation and shutdown, were 20 min 35 s,
7 min 39 s and 7 min 41 s respectively.

Median generation computation was 17.03 / 16.89 / 16.89 seconds. Summed exact
worker time at the comparison boundary was approximately 1,149 / 1,151 / 1,174
seconds. These are sums across workers, not serial wall-time percentages.
The one-worker loop spent about 766 seconds outside generation computation;
the four/eight-worker loops spent about nine seconds there. Along with CPU
work and GPU utilization, this supports the observed CPU bottleneck at one
worker and its removal at four.

The container exposes 64 logical CPUs but `cpu.max` is `877714 100000`, or
8.77714 cores of aggregate execution time. The offer advertised 9.142857
effective vCPUs. Mean observed container CPU use was about 3.27 cores with
four workers and 3.31 with eight. The four-worker window had no throttled CPU
periods; the eight-worker window had 430 of 4,173 periods throttled. More visible
host CPUs do not bypass this quota. Peak sampled GPU memory was 754 MiB for
this one-coin workload; that is not a capacity estimate for multicoin jobs.

## Workload and limits of the conclusion

- PB8 `ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8`; RTX 3090 24 GB; CUDA 13;
  Torch 2.13.0+cu130 and CuPy 13.6.0.
- User's `gpu_test` ETH suite, five approximately 400-day scenarios,
  Binance/Bybit source data, native warmup, trailing martingale. The active
  GPU topology in these logs is `long_no_hsl`.
- Explicitly approved scoring change in benchmark copies only:
  `gain_strategy_eq` → `adg_strategy_eq`. The original optimizer config still
  matches its recorded SHA-256. Native preflight rejects the original gain
  objective as exact-only in this PB8 revision.
- Population 1,024; eight exact validations and four drift probes per
  generation; seed 7; automatic lean parallelism disabled. Native dispatch
  safety caps remain active. Configured `iters=512`, but these short runs were
  deliberately interrupted at a common measurement window to provide the
  requested quick conclusion. They are not completed 512-iteration runs.
- Asynchronous completion and the stop boundary can change which exact rows
  reach the saved front. Sixteen bot-parameter sets are shared between the
  saved one/four-worker Pareto fronts; their `suite_metrics` are identical.
  This is a limited consistency observation, not general GPU/CPU parity or
  evidence of a converged trading strategy.

## Reproducible packaging and data findings

The first host repeatedly retried an intact 3.43 GB compressed registry layer.
An independent complete download matched its SHA-256. The image was repackaged
with a largest compressed layer around 821 MB; another host pulled it
successfully. The original host's precise transport failure remains unknown.

Two further failures were diagnosed and corrected: PB8's source loader expects
`ohlcv/binance`, while PBGui stores originals under `binanceusdm`; and pip's
Rust build does not run the source-stamp step from PB8's `rustbuild.sh`.
The Dockerfile now uses PB8's own stamping/verification helpers. The worker's
binary and source hashes were verified against the local image before applying
that missing packaging step. No PB8 Python or Rust strategy source was changed.

Current private image:
`ghcr.io/msei99/pbgui-pb8-gpu:ee2b7d4-layered`

Current digest:
`sha256:4c280d1e027e037c7a17952b45c8c5f4af821ff3737bc3d469c34c35c3952a9d`

The initial running container was created from the preceding layered digest
`sha256:31ac85b55b08be8fbcdaf9df2411b8ae42df55462671c72562fc2e843f5e1227`
and received the verified source-stamp correction. The updated private Vast
template is ID 725870, hash `faa144fa9b630f32650512edbfbf32bc`.

The selected data export is 9,419 shards / 223,146,106 bytes; transfer took
108.56 seconds. Native preparation loaded the supplied candles successfully,
but still fetched public market/first-candle metadata. The package therefore
does not promise a completely offline optimizer startup.

## PBGui results and next architecture step

The short results are imported into the configured PB8 `optimize_results`
directory with suffixes `_vast_gpu1`, `_vast_gpu4`, `_vast_gpu8`. In PBGui,
open **Optimize V8 → Results**, refresh, filter `vast_gpu`, then select
**Pareto Explorer**. PBGui's native result reader decoded 188/192/187 entries
without errors or partial MessagePack entries. Its Pareto loader also loaded
the full four-worker result in an isolated copy, identifying PB8 and all five
scenarios.

Additional copies and machine-readable measurements are under
`data/vast_gpu_benchmark/2026-09-13/`. The private archive
`.local-work/vast-gpu-test/results-layered/complete-experiment.tar.gz` contains
the short-run native results, logs, checkpoints, config manifests, build
evidence and utilization series. Its verified SHA-256 is
`bf1b37b110afcb0994d5bb729ca2a8d69274c7de10e33421632a9f9bd12213ac`.

For the first productive integration, the tested PB8 image plus selective data
transfer is now the lower-effort path. Four bundled remote CPU workers suffice
for this workload at the current rental price. Local CPU plus remote GPU is
still possible through a versioned batch-RPC boundary; it is not implemented
or benchmarked here. Five TCP connection setups to this host had a median
of 0.221 seconds, versus roughly 17 seconds of generation computation. This
is only a network baseline, not a measurement of RPC serialization, transfer,
retry behavior, or complete hybrid execution. See the
[architecture comparison](vast-ai-gpu-integration.md) for the required changes.

## Candidate counts versus CPU iterations

The 25,600 count is 25 generations of 1,024 GPU proxy candidate evaluations,
not 25,600 exact CPU iterations or necessarily distinct configurations. Each
candidate covers five scenarios. At the same 25-generation boundary, 184
exact CPU evaluations had completed; graceful interruption persisted a few
more, giving 188/192/187 saved results. GPU screening therefore processed
about 61 candidates per second in the four/eight-worker windows, but those
approximate evaluations are not interchangeable with full CPU backtests.

The measured 2.81-fold improvement compares GPU plus four validation workers
against GPU plus one worker. No CPU-only speedup factor has been established.
A meaningful optimizer comparison needs the same data, objectives and time
budget, then compares exactly validated Pareto quality. A known local CPU
iteration rate can provide throughput context, but cannot establish equivalent
optimization quality. No CPU-only Vast benchmark was run.

## Completed 16-worker run

The GPU16 run completed normally at 2026-09-13 11:19:21 UTC with exit code 0
and no timeout: 512 exact evaluations, 64 GPU generations and 65,536 proxy
candidate evaluations. Total runner time was 1,136.20 seconds (18m56s),
including preparation and shutdown. The first 25 generations took about
421.74 seconds, versus 419.97/419.89 seconds with four/eight workers, with
the same 184 exact evaluations completed at that boundary. Sixteen workers
therefore provided no measurable throughput advantage on this workload and
this container's 8.78-core aggregate CPU quota.

All 512 native results and 53 Pareto files were imported into PBGui's configured
PB8 result root, with suffix `_vast_gpu16`. The native binary reader found no
parse error or partial trailing entry. Local copies are in
`data/vast_gpu_benchmark/2026-09-13/gpu16`; the complete result archive is
`.local-work/vast-gpu-test/results-layered/gpu16-long.tar.gz`, SHA-256
`a2963dc8af518710352c7a6a492184d68f02110b765699694eed0f8234fb34a0`.

## Sweep metadata recovery

The original `gpu_test` configuration already contained the `sweep_cycles`
template. Its SHA-256 still matches the benchmark input manifest. The standalone
export removed the `pbgui` section and did not preserve the accompanying
`.pbgui_sweep_cycles.json` result sidecar, so PBGui initially omitted sweep
columns such as `sweep_total_swept` from the imported results.

The missing sidecar was reconstructed using PBGui's existing `build_sweep_plan`
helper and restored to all four local and managed result directories. Existing
per-scenario gain metrics support sweep evaluation for every saved Pareto
candidate (17/18/17/53). Policy: starting balance 10,000, multiplier 2, refill
cost 0, cooldown 0. No GPU rerun was needed. This is post-processing of the five
training windows; the holdout was not evaluated by this benchmark. The frozen
archives remain unchanged and predate this sidecar restoration. Future exports
and imports must preserve the sweep sidecar separately from native PB8 config.

## Read-only comparison with manibot20

`manibot20` is the user's optimizer server name, not a duration of twenty days.
The previously stated approximately 1,500-fold time advantage was based on
that misunderstanding and is withdrawn.

Read-only SSH inspection found active CPU job
`6eada510-1a6d-4539-b6bd-0775436226a6` (`eth_new_sweep_no_hsl_15_95`),
using pymoo and 64 workers on an Intel Xeon E5-4620 at 2.20 GHz. Optimization
started at 2026-09-13 04:35:15 UTC; the log reached iteration 53,972 at
13:32:20 UTC, approximately 6,030 exact evaluations/hour over that window.
The configured budget is 200,000. These are logged progress measurements,
not a completed-run benchmark.

Both workloads use ETH, Binance/Bybit, five 400-day training windows and
starting balance 10,000, with HSL disabled. However, manibot20 optimizes
`ema_anchor`, whereas the GPU experiment optimized `trailing_martingale`.
CPU scoring maximizes `gain_strategy_eq`, GPU scoring `adg_strategy_eq`;
drawdown limits are 0.95 versus 0.80, training windows are shifted by two
days, and PB8 revisions differ (CPU `cbb23f9af07cc3639612485674a1d9e7703345fd`,
GPU `ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8`). Consequently, neither the
throughput counts nor result quality establish a controlled GPU speedup.
Differences in strategy/search space may contribute to the user's observed
quality improvement. No remote config, file, process or service was changed.

## Rental state

After the user authorized stopping the instance, the existing supervisor
destroyed instance 50869726 and verified absence twice, finishing successfully
at 2026-09-13 13:06:32 UTC. An independent API check at 13:06:42 UTC confirmed
zero instances in the account. The local supervisor is inactive with exit code
0. No persistent volumes were created. Evidence is in
`.local-work/vast-gpu-test/rental-3090-layered-12h.status.json` and
`rental-3090-layered-final-verification.json` in the same directory.

The rental rate was 0.1711111 USD/hour including 40 GB ephemeral storage,
plus transfer. Destruction ends ongoing instance/storage rental charges;
previously incurred usage and transfer remain billable. The first failed
instance, 50866485, was also destroyed and verified absent earlier.
