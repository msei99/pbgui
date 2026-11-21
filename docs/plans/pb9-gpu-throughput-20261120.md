# PB9 GPU throughput investigation — 20 November 2026

The original ~4 candidates/s measurement was dominated by Python preparation and
result handling. Supported Max mode and the normal PB9 CLI materially improve
throughput. A run-local immutable catalog cache provides another improvement,
but remains an experimental upstream idea, not a deployed PBGui workaround.

## Conditions and measurement boundaries

One disposable RTX 3090 (24 GB), 15.36 effective CPU cores, CUDA 13-compatible
host, USD 1 authorization and a fixed 60-minute destruction deadline. PB8 source
and Rust extension came from the existing `b9d6692-ema-span-alias` worker; PB9
used unchanged source `1b85ea6cc4bb7b917ada43725044b7559763c46e`. Both used the
PB9 worker's installed Python/CUDA libraries, with separate PB8 source/extension
paths and separate processes. This tests those pinned runtimes, not every PB8
installation or GPU.

Only generated BTC/ETH candles and public market metadata were uploaded. No
user candle files, accounts, secrets, live bots or VPS installations were used.
The native input requests 2025-01-01 through 2025-01-03. The recorded metric
sources confirm **48 hours**, 2,881 equity observations; it is not a 72-hour
simulation. The active paired dataset has generated history from 2024-10-01
for full native warmup. Long only, HSL disabled, two fixed position slots,
starting balance 100,000; seed 7, population 128, eight significant digits.
The PB9 pair was produced by the official best-effort PB8 converter, then both
test fixtures received matching data/position settings. Metric formulas and
inherited optimizer search spaces differ across generations; this is not an
assertion of identical PB8/PB9 trading behavior or search trajectories.

Each throughput row excludes its first complete generation and uses at least
60 seconds of complete generation evidence. Interrupted generations do not
count. Native rates count actual GPU replay rows, not requested batch width or
cached-only attempts. Startup/compilation and shutdown remain separate.
PB8 proxy-generation compute rates and observed CPU validation rates are listed
separately: they are different quantities. Single-host windows are not a claim
of universal performance or a converged optimizer result.

## Original diagnostic workload

The initial short fixture produced no trades. It is useful to isolate the host
pipeline, not to prove trading correctness or realistic simulation capacity.
Its sparse authored bound expands to 53 native dimensions during preparation;
it must not be described as a one-dimensional native optimizer.

| PB9 launch and mode | Completed GPU candidates/s |
| --- | ---: |
| Inherited script launch, Off | 3.85 |
| Script launch, Max, fixed batch 128 | 9.14 |
| Script launch, Max, automatic batch | 9.24 |
| Official CLI, Off | 5.01 |
| Official CLI, Max, automatic batch | 19.47 |
| CLI + Max + experimental side-catalog cache | 28.82 |

A representative Off generation spent 20.50 s preparing candidates and 12.68 s
consuming results, out of 33.27 s total. Main-thread CPU time was 31.55 s.
The owner-thread timing overlaps these spans and is not added to them.
Max enabled eight CPU preparation workers. `n_cpus` alone does not activate
that preparation pool in PB9's Off/conservative search path.

## Active trading workload

| Runtime / path | Measurement | Rate / second |
| --- | --- | ---: |
| PB8, four exact workers | GPU proxy candidates / generation compute time | 237.48 |
| PB8, four exact workers | CPU-validated results / observed steady wall time | 13.22 |
| PB8, eight exact workers | GPU proxy candidates / generation compute time | 223.09 |
| PB8, eight exact workers | CPU-validated results / observed steady wall time | 13.05 |
| PB9 CLI, Off | Fully evaluated GPU candidates | 3.29 |
| PB9 CLI, Max | Fully evaluated GPU candidates | 12.97 |
| PB9 CLI, Max, experimental catalog cache | Fully evaluated GPU candidates | 17.94 |

The PB8 window contains 268 post-cold generations, 34,304 proxy candidates and
144.45 s generation compute time. Its timestamped steady window is 162 s,
containing 2,142 additional completed exact CPU validations. PB8 validates eight
selected candidates per generation; its much larger proxy counter does not
represent that many fully validated output results.

Thus Max plus the correct native launch reaches a similar **completed-result
rate** in this workload. It does not match PB8's approximate GPU proxy rate.
The catalog prototype is about 38% faster than native CLI/Max here. The active
fixture has nonzero returns and actual fills, unlike the original fixture.

The eight-worker PB8 check contains 106 post-cold generations, 13,568 proxy
candidates and 60.82 s generation compute time; 848 additional exact validations
completed over its timestamped 65 s steady window. Doubling exact workers did
not improve this workload. The counters and denominators still differ across
the two engines: similar observed completed-result rates do not prove identical
work per result or equivalent financial accuracy.

## Concrete PBGui correction

The local optimizer runner already imports `optimize` and invokes its `main`.
The cloud worker inherited PB8's `python src/optimize.py` launch. In PB9 this
creates a second `optimize` module when `_Search` imports
`optimize.individual_to_config`. Its identity check consequently disables
`_reuse_record_configs`, rebuilding configurations during result consumption.

The prepared worker change uses the installed `passivbot optimize` entry point
under the same Python. The CLI imports `optimize.main`, preserving the intended
module identity. The shared validated options/packaged-path argv builder is
unchanged; the PB8 early return retains its previous command. Normal native
cloud optimization and calibration both use this path. No search policy is
silently changed to Max.

Focused tests include a negative script-launch countercheck, module import
identity, native option/manifest/seed path preservation and PB8 argv retention.
The candidate image also runs the real installed CLI offline, checks help,
constructs the real native `_Search` and asserts prepared-config reuse is on.
It is published as `1b85ea6-native-cli-v1` and pinned by verified public digest
`sha256:8093291ccefb61db45da7bdc7812186d2267ca91d2492f8d813fa4499b6d291f`
after explicit user approval. Previous rental ownership remains recognized.

## Experimental cache and correctness limits

The process-local experiment wraps only `get_side_field_catalog` in a bounded
one-entry LRU before normal imports, including spawned preparation processes.
No source file or validator is removed or modified. For this pinned source,
all 401 reachable nonprimitive catalog objects are frozen dataclasses,
immutable tuples/frozensets, enums or mapping proxies; no mutable nodes were
found. Thirty local config-cleaning results and twenty vector-config builds
were equal before/after experimentation. This does not authorize a global
persistent cache across source/schema changes.

The original workload supplied 512 identical script/CLI candidates and 1,496
identical CLI-Max/cache candidates with equal complete metric payloads. Those
zero-trade comparisons are deliberately insufficient on their own. The active
CLI-Max/cache comparison supplied 1,024 common candidates with nonzero CAGR,
with exactly equal complete metric payloads. A separate frozen-seed replay
produced 16 common, exactly equal bot configurations across native GPU Off,
GPU Max and CPU. All 16 GPU Off/Max complete metric payloads were exactly equal.
Max changes the search schedule/proposal budget; different resulting fronts are
not evidence of wrong metrics, and common candidates must be checked explicitly.

The separate native CPU comparison is **not numerically identical**: fill counts
match for all 16 seeds, but the largest relative CAGR difference is 2.822%.
Over this short 48-hour fixture, the corresponding maximum relative difference
in the ending-equity factor reconstructed from CAGR is 0.0156731%. Annualization
amplifies the discrepancy. Its native CPU/GPU numerical cause has not been
established; neither the launch fix nor cache is claimed to resolve it. This
limits claims about numerical equivalence, independently of throughput.

Initial paired attempts were rejected for incomplete synthetic warmup history
and inherited position bounds. They are retained as failed setup attempts,
excluded from performance results. The first frozen CPU attempt contained an
unsupported fixture field; its corrected rerun is recorded separately. Some
controlled native SIGINT shutdowns emit an upstream `HlcvsBundlePy` unsendable
finalizer warning after results are flushed. That warning is not hidden or
claimed fixed by the launch change.

## Validation, artifacts and rental closure

Fable 5.2 Medium reviewed the bounded analysis and proposed change twice. Its
requests for a real installed CLI identity check, active trading candidates and
catalog immutability inspection were followed. Focused PB8/PB9 option, native
calibration, cloud worker, image compatibility and logging checks: **136 passed**.
Publication checks including native profile/rental compatibility: **156 passed**.
The locally built candidate passed an offline, network-disabled image probe.

Candidate: `pbgui-pb9-worker:1b85ea6-native-cli-candidate`, local image ID
`sha256:9a1a63abab4a1b7187bddce35c246a8a633358169bf6b372285712572a7603c8`.
The registry manifest was verified anonymously and references this exact local
image ID. After a controlled API-only restart (serial 3770), an authenticated
browser read of `/api/vast-v9/profile` confirmed this published digest is active.
Physical GPU measurements exercised the
equivalent CLI command on the unchanged pinned base image, not a rental booted
from this new candidate. The catalog cache remains a temporary experiment;
no upstream Passivbot source file was edited.

Raw evidence is saved locally at
`/tmp/pb9-speed-study-20261120/evidence.tar.gz` (26,719,961 bytes, 109 files), SHA256
`ff0b3b71fe6d09c74022d352ff7c4cdc2037e592857f61fac9da6da5e1f4faa6`.
The temporary-directory archive should be retained elsewhere before cleanup if
long-term raw evidence is needed. The versioned candidate receipt summarizes
measurements and their limitations.

Vast instance **61703106** was destroyed, PBGui records `deletion_verified`, and
a fresh provider listing confirms its absence. Provider-reported charges at
closure: **USD 0.139** (GPU 0.127, disk 0.012, bandwidth 0.000), within the USD 1
authorization. This is a provider snapshot, not a final invoice.
