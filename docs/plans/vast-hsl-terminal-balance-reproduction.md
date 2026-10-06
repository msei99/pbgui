# Vast CUDA HSL terminal-balance reproduction

Publication follow-up: the newer official PB8 worker `00ce7d0-pr1871-v1` is now
published and pinned. See `vast-worker-upgrade-00ce7d0.md` for the release receipt
and validation limits. This report records the earlier isolated reproduction.

## Result (2026-10-06)

The reported `MPS proxy unavailable held-position valuation` error was reproduced
on a rented NVIDIA GeForce RTX 3060 (12 GB), using valid synthetic candles and
the currently pinned PBGui worker. Applying the targeted upstream HSL fix removed
the failure. This demonstrates the reported mechanism; it does not establish
which candidate caused the original user's three failures.

| Check | Original worker | Targeted fix |
|---|---|---|
| 12 terminal-closing-loss replays | Original valuation error | Completed with liquidation |
| 6 ordinary-fee control replays | Completed | Identical outputs |
| 48 boundary probes (4 kernels × 3 scopes × 4 balances) | Finite depleted balances invalidate HSL | Depleted balances preserved for liquidation; positive/NaN controls unchanged |
| Public GPU optimizer, identical config and seed | First proxy batch fails, rows 0–3 | 32 exact evaluations and 128 proxy candidates completed |

Three focused upstream CUDA tests also passed, covering genuinely unavailable
valuation candles in ordinary and temporally chunked replays. Non-finite HSL
budgets remain invalid. PBGui's 33 focused worker-image tests passed. The logging
audit's CLI allowlist was extended for the three new result-reporting commands;
all 13 audit checks passed across the initial run and the focused rerun of the
updated allowlist check.

## Fixture and limits

The public optimizer fixture uses canonical PB8 config v8.6.0, seed 42,
Trailing Martingale, long-only BTC, coin HSL, and an isolated synthetic offline
OHLCV cache. The optimizer budget is 32 exact evaluations. The replay matrix also
covers EMA Anchor and all three HSL scopes.

Synthetic taker fees of 2.0 and 5.0 intentionally exhaust cash on closing fills.
These are stress inputs, not realistic exchange fees or a reconstruction of the
reporter's strategy. Controls use 0.0005. Entries are limit orders; HSL closes
are market orders. Every replay candle has finite positive H/L/C and lies inside
its declared valid range. An initial exploratory market-entry fixture liquidated
at entry and did not reproduce this closing-boundary failure; it was excluded
from the paired result matrix.

The optimizer's default fee override must be changed as well as synthetic market
metadata. Otherwise the default override masks the intended stress fee. Both
paired optimizer configs are identical after excluding their output directory.
Short synthetic runs report insufficient rank-comparison evidence; this was a
functional regression check, not a performance or proxy-quality benchmark.

## Exact candidate provenance

- Base: `ghcr.io/msei99/pbgui-pb8-worker@sha256:792a11f40098a3fffe9365c46e38f60c7bddaa35179c9bc608d130b5e1449dbe`.
- Official PB8 base: `061e472e3d400cb3a740583d52f9d46e02eaf781`.
- Existing [PR #1871](https://github.com/enarjord/passivbot/pull/1871) overlay was retained, corresponding to head `83d18f0afbd0c927367aed522067558184a34933`.
- The new patch extracts only the terminal-balance guard and distinct fatal-error markers from [commit 1d142ec](https://github.com/enarjord/passivbot/commit/1d142ec9720f62cdce5c7b1c911efd7e45178501). Its unrelated tuning and throughput changes are excluded.
- Patch: `setup/vast_gpu_benchmark/patches/gpu-hsl-terminal-balance.patch`; SHA256 `e0911f278e653b8d34c5cb334da780a4b8e18c0f1b205354e1962e45a2628b5c`. `git apply --check` passed against the base.
- Native module rebuilt in an isolated checkout using the original worker's Cargo.lock; SHA256 of that lock: `13e753415d54ffcf2bd2febc7cc26e8614313f50fa0dab448fadd4da9b2555f0`. The final build used `maturin build --locked --release` with Rust 1.97.1.
- Final loaded native binary SHA256: `d60c816494e38da3789341d8512dc0a9848b1ed65b286a9e5ba66b2a179e09c7`.
- Its verified source stamp matches the isolated checkout and worker fingerprint: `fb7508072bce77286c8255971125dfaa2087aa37d5185bec3d5c2128c92f6cdb`.

The candidate is the official base plus two explicit overlays, not an unmodified
upstream revision. No registry image was published and no PBGui production pins
were changed. A production worker release still requires a reviewed image build,
public digest verification, and coordinated pin activation.

## Reproduction artifacts

The versioned scripts under `setup/vast_gpu_benchmark/` are:

- `reproduce_hsl_terminal_balance.py`: paired real-GPU replay matrix; reads `hsl_terminal_balance_stress.json`.
- `probe_hsl_terminal_balance.py`: uses the upstream episode-boundary probe through PB8's CUDA shader translator.
- `optimize_hsl_terminal_balance.py`: creates its own canonical config and offline cache in a new directory and runs the public optimizer.
- `verify_hsl_terminal_balance.py`: verifies paired outputs, unchanged controls, liquidation, and the 48 boundary probes.

GPU scripts require a disposable worker checkout at `/opt/passivbot`, its upstream
test helpers, pytest, and CUDA. They do not rent machines or change provider
state. The optimizer generator uses PB8's native configuration pipeline. Its fee
stress applies only to its newly created fixture directory.

Local evidence is retained under the ignored `.local-work/hsl-repro/` directory:
paired replay/probe JSON, canonical optimizer configs, optimizer logs, the isolated
candidate checkout, original lockfile, and rebuilt wheel. Verify the paired GPU
artifacts with:

```sh
/home/mani/software/venv_pbgui/bin/python setup/vast_gpu_benchmark/verify_hsl_terminal_balance.py .local-work/hsl-repro
```

The test used a separate uniquely labelled rental, a local systemd supervisor,
and a container deadline guard. Authorization was at most USD 5 and two hours;
the actual rental deadline was limited to 90 minutes. Early cleanup was requested
after collecting the results. Deletion was verified by the independent supervisor
and a separate fresh provider listing. The rental lasted approximately 19.3
minutes. Account credit decreased from USD 3.720666 to USD 3.700500 at the final
check (about USD 0.02 posted so far; provider billing can settle later).
