# RTX 3060 (170 W): EMA-anchor measurements, 25 September 2026

This is a measurement report, not an automatic PBGui/PB8 tuning rule. All tests used the same Vast RTX 3060 rental (instance 52481778, measured 170 W power limit), PB8 worker, EMA-anchor strategy, and seven scenario windows. The large prepared input was 1500mcap_ema_anchor_05_no_sort: 45 listed / 41 effective coins, Binance and Bybit. Each full-run figure is one complete PB8 GPU generation; jobs were deliberately stopped afterwards. Optimizer search quality and exact Rust throughput were not measured.

## Complete 41-coin comparison

Each scenario contains 442,657 candles and 41 effective coins. Both runs used max_dispatch_candidate_bars = 105,715,531,776, limiting one dispatch to 5,824 candidates.

| Population / batch | Actual dispatches per scenario | Generation wall time | Candidates/s | Candidate-bars/s | Median active power | Peak VRAM |
| ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| 12,288 / 12,288 | 5,824 + 5,824 + 640 | 4,465.7 s | 2.752 | 349.6 M | 107.6 W | 6,343 MiB |
| 11,648 / 11,648 | 5,824 + 5,824 | 3,434.1 s | 3.392 | 430.9 M | 108.0 W | 6,671 MiB |

Aligning population with dispatch increased measured candidates/s by **23.27%**. The full 5,824-candidate dispatches had virtually identical times between runs. In the baseline, the seven 640-candidate tails were only 5.2% of candidates but took 1,034 seconds / 23.4% of GPU proxy dispatch time. Both runs peaked around 109 W despite the 170 W limit. GPU activity read 100% during dispatch, but that metric alone does not establish compute saturation.

| Scenario | 12,288 wall seconds | 11,648 wall seconds |
| ---: | ---: | ---: |
| 1 | 448.9 | 348.1 |
| 2 | 566.0 | 435.2 |
| 3 | 604.8 | 465.6 |
| 4 | 623.0 | 480.7 |
| 5 | 676.2 | 518.7 |
| 6 | 729.8 | 555.3 |
| 7 | 773.2 | 588.7 |

All seven scenarios had identical coin, candle and batch counts, but the seventh took about 1.7 times as long as the first. Strategy and market behavior affect runtime beyond candidate-bar count.

## Work-limit sweep and middle workloads

The earlier 41-coin work-limit sweep at population 12,288 measured **only the first large dispatch in scenario 1**, not whole generations. Throughput rose from about 237 M candidate-bars/s at 2x to 379 M at 4x and 609 M at 8x. At 16x it was about 607 M: doubling beyond 8x gave no throughput gain for that dispatch. Maximum observed power was about 79, 94, 109 and 114 W respectively. Do not compare those partial-dispatch rates directly with the full-generation table.

| Effective coins | Population / batch | Full-generation candidate-bars/s | Median active power | Peak VRAM | Qualification |
| ---: | ---: | ---: | ---: | ---: | :--- |
| 11 | 8,192 / 8,192 | 634 M | 125.9 W | 2,449 MiB | Warm generations 2-3; complete unsplit batch. PB8 profiles valid, but result import failed due to invalid experimental result-root name. |
| 21 | 8,192 / 8,192 | 561 M | 121.2 W | 3,927 MiB | First complete generation; one full batch. |
| 21 | 16,384 / 16,384 | 558 M | 126.4 W | 4,609 MiB | First complete generation; doubling population gave no gain. |
| 41 | 11,648 / 11,648 | 431 M | 108.0 W | 6,671 MiB | First complete generation; two aligned batches. |

For this **tested** 41-coin EMA-anchor input, population 11,648 with a 105.7-billion candidate-bar work cap is better for throughput than 12,288 with a tiny tail. More work or more watts is not automatically faster: 8x to 16x reached a dispatch-throughput plateau, and the 21-coin population doubling also failed to help. The tiny canonical calibration dataset does not predict the large-run rate. A universal production formula for arbitrary strategies, coins, exchanges and scenarios is **not established** by these measurements. No production tuning or PB8 code was changed.

## One-batch 41-coin sweep (second 170 W RTX 3060)

Frozen EMA-anchor input: 41 effective coins, seven scenarios, 442,657 candles each. Vast machine 31594 supplied 24 CPU cores. Population equalled batch size, and each scenario used exactly one GPU dispatch. Jobs stopped after generation 1.

| Population / batch | Work cap (candidate-bars) | Generation wall | Generation candidates/s | Scenario 1 candidates/s | Median active W | Peak W | Peak VRAM |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5,632 | 102.3 B | 1,918.3 s | 2.936 | 28.16 | 100.2 | 121.1 | 6.50 GiB |
| 6,144 | 111.6 B | 1,937.0 s | 3.172 | 32.47 | 101.3 | 122.2 | 6.54 GiB |
| 6,656 | 120.9 B | 1,981.6 s | 3.359 | 34.06 | 101.3 | 126.7 | 6.60 GiB |
| 7,168 | 130.2 B | 2,010.9 s | 3.565 | 36.05 | 101.7 | 128.0 | 6.62 GiB |

Generation throughput improved by 8.0%, 5.9%, then 6.1%; 7,168 is the fastest tested on this host, not a bracketed sweet spot. The older ~33 candidates/s was only scenario 1's first 5,824-candidate dispatch (174.8 s), not the seven-scenario generation. On the same basis the new 7,168 test reached 36.05/s. Older complete-generation rates were 2.75–3.39/s, comparable despite a different host.

PB8's 300-second heartbeat covers asynchronous exact-result collection, not synchronous GPU dispatch. Its GPU progress tracker is disabled when the population fits in one batch; there are no intermediate candidate-count logs until generation completion. No PB8 code was changed. Evidence: data/vast/experiment_170w_one_batch_profiles.jsonl and data/vast/experiment_170w_one_batch_telemetry.jsonl; jobs ab5f5cbdf88e4a189241799b35ab884c, 1948600e55da402b92e85b38e22eeb68, 996698e82c2d4ee488a0dd12d294ac37, 93269a9a5c4b48f5a2a3662e8d3d0447.

## Evidence and cleanup

- 41 coins, 12,288: data/vast/jobs/527b5949fe384ffdb4444c4d3cc99e4e/final-results/optimizer.log (generation-1 gpu-profile).
- 41 coins, 11,648: data/vast/jobs/5c023185f402427e85545000c189dcd8/final-results/optimizer.log (generation-1 gpu-profile).
- 11 coins: data/vast/jobs/d3fc9fe2e9a24c269c65c5e978c8460d/final-results/optimizer.log.
- 21 coins: data/vast/jobs/a7ad83a2929c4435b20734d2a90a0df6/final-results/optimizer.log and data/vast/jobs/0c0f6353bcac498abbf61cca06088bb0/final-results/optimizer.log.
- Five-second GPU telemetry: data/vast/experiment_170w_12k_telemetry.jsonl. Power and VRAM figures are telemetry statistics, not PB8 profile fields.
- The sole Vast instance was confirmed deleted locally and by a fresh provider query. Provider-reported final cost was **$0.601** (approved ceiling $2 / six hours). The original automatic GPU queue settings were restored, with no experiment job ready and no active rental.

## Same-machine follow-up (instance 52578004)

The new rental is again Vast machine 31594, measured 170 W RTX 3060. The 7,680 and 8,192 runs use the same 120,676 market-data files (path and SHA-256), EMA-anchor workload, seed 42, 23 effective CPU workers, and one GPU dispatch per scenario. All figures below are full first generations; jobs were stopped immediately after the generation profile. Active power samples require at least 95% reported GPU utilization. Clock and temperature samples for 8,192 are in `data/vast/experiment_170w_followup_gpu_clocks.jsonl`.

| Population / batch | Work cap | Generation wall | Generation candidates/s | Scenario 1 candidates/s | Median active W | Peak W | Peak VRAM |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 6,656 (repeat) | 120.9 B | 1,827.2 s | 3.643 | 36.57 | 124.0 | 127.6 | 6.60 GiB |
| 7,680 | 139.5 B | 2,898.3 s | 2.650 | 25.42 | 118.4 | 125.0 | 6.67 GiB |
| 8,192 | 148.8 B | 3,023.4 s | 2.710 | 26.94 | 116.7 | 128.6 | 6.71 GiB |
| 8,704 | 158.1 B | 3,180.1 s | 2.737 | 27.17 | 122.7 | 131.3 | 6.77 GiB |

The 8,704 rate is only 1.0% above 8,192, but 24.9% below the 6,656 repeat; testing larger populations stops under the agreed 10% improvement rule. The 6,656 repeat on the same machine reached 3.643/s, 8.5% above its earlier 3.359/s, ruling out a simple all-runs temporal slowdown as the sole explanation for the 7,680–8,704 drop. The 8,704 run needed one GPU dispatch per scenario and sampled software thermal slowdown in 25 of 56 one-minute clock checks, peaking at 89 C; the 6,656 repeat also showed active thermal slowdown at 89 C, with no power-cap throttle. Changing population also changes random candidate/trading paths, so these runs do not isolate pure dispatch overhead or prove an exact universal optimum. The measured promising region is 6,656–7,168 on this workload/host, not 8,192 or larger. Do not treat 100% GPU utilization as full compute or power saturation.

Evidence: first-generation profiles in `data/logs/optimizes_v8/vast_31f8cb7fcac54bd99ff3d25ea55ed495.log`, `vast_6f1ff3689eff4c8cbf194d17fe26604e.log`, `vast_f34d48bdf6204e89a29fadcf79cdc008.log`, and `vast_3a941a926ac849e681b1d03b967dd53b.log`; power/VRAM in `data/vast/experiment_170w_followup_telemetry.jsonl`; clocks and thermal flags in `data/vast/experiment_170w_followup_gpu_clocks.jsonl`. All four jobs were stopped after generation 1. The sole rental (instance 52578004) reached `deletion_verified`, and a fresh Vast query returned zero instances. The original queue worker selection and automatic scheduling flags were restored.
