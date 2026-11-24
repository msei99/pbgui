# PB9 Optimize parity — current implementation

PB8 and PB9 use one Optimize engine and page. Generation adapters own native
config validation, metadata, effective-policy resume identity, process proofs and
Vast worker profiles. No PB7/PB8 bot source was modified.

| Area | Current state |
| --- | --- |
| Configs/editor | Shared layout, import/save/duplicate/archive, sparse native field bindings and Additional Parameters |
| Bounds/scoring/limits | Shared controls, native metrics and Global/Portfolio groups |
| Local queue | Separate native namespace, shared Autostart/capacity/owned shutdown and detached runner |
| OHLCV | PBGui data default, direct source references, shared native Readiness/1st/All/preload |
| Results/Paretos | Shared readers/selection/seeds/checkpoint/3D/Dash workflows |
| Vast | Shared account, marketplace, calibration, pool, transfer, requeue, cleanup and performance history; explicit runtime ownership |
| Worker candidate | Built locally: `pbgui-pb9-worker:1b85ea6-native-launch-candidate`; source/Rust/worker entry and native CPU CLI contract verified |
| Paid cloud execution | Blocked until a published immutable native image digest is configured and a separately authorized physical GPU smoke succeeds |
| AI Loops | Shared controller, routes, Template and UI; separate PB9 state and native policy/metric/backtest bindings. Offline and browser-fixture verification; no paid end-to-end AI/GPU run |
| Strategy Explorer | Deferred as requested |
| Pareto Explorer | PB9 result root, `v9` generation, native result reader for handoffs, global/portfolio parameters, native-metric presets |
| Sweep Cycles | Window gain derived from per-window `cagr_fraction_strategy_equity`; PB9 Sweep preset and Long/Global position bounds |
| PB8 migration | Official converter for saved PB8 Optimize and Pareto configs through the shared review dialog; unsaved PB9 draft |
| Services | PB9 Optimize Queue controller visible and controllable |

## Open items (2026-11-17 review)

These are intentionally separate and not part of the parity fixes:

1. **Optimizer CLI options completed (2026-11-20).** `--scenarios`,
   `--filter-starting-configs`, `--compress-starting-configs N` and
   `--preparation-manifest` have structured controls, snapshot validation,
   native argv, result-owned Resume metadata and shared cloud bundle transport.
   Imported reports use private content-addressed attachments so native config and
   browser draft limits stay unchanged. Queue options freeze their bytes and hashes; native PB9 verifies the
   actual prepared-input identity. Seed options require Pareto seed mode.
   Local checkpoints remain local; cloud Requeue starts a new job.
2. **AI integration implemented (2026-11-24).** Native PB9 optimizer proposals
   use the existing explicit review flow. AI Loops share PB8 supervision and UI,
   with native metadata, sparse policy inheritance, independent CPU validation,
   runtime-bound queues and isolated history. Paid AI/GPU execution has not been
   exercised by the offline integration checks.

## Verification evidence

The 2026-11-16 correction audit in [pb9-parity-audit.md](pb9-parity-audit.md) supersedes any claim that synthetic source fixtures establish real external-folder compatibility. Per-job native caches were removed; native Backtest and Optimize now retain the configured checkout as CWD. Installer/VPS parity and paid cloud validation are still outstanding.


- Fable 5.2 medium reviewed planning and a bounded static code snapshot. Review
  findings were checked against actual call paths and corrected with focused tests.
- Real native CPU Optimize ran against synthetic PBGui-format raw OHLCV files,
  public market/listing fixtures and a disposable source checkout under `/tmp`.
  Exit 0, four exact evaluations, progress file, checkpoint and Pareto output.
  A second native invocation resumed that checkpoint with exit 0 and restored
  compatible convergence history. Isolated browser-authored output also passed
  the installed native helper with no browser script errors.
  Production source/configs/candles, VPS hosts and cloud resources were untouched.
- A synthetic prepared-dataset replay exposed native upstream binding-authority
  behavior before evaluation; this does not establish a working prepared-dataset
  replay path. The default raw PBGui candle-source workflow above completed.
- The broad focused pass completed with 3,857 passing tests, two skips and
  13 failures (worker-candidate source contract and browser fixture/version
  assumptions). Corrected affected areas then had 182 passing tests with one
  remaining navigation-fixture failure; the final navigation/help/native-browser
  rerun passed all 27 tests, including that corrected case. All originally failed
  cases have now passed targeted reruns. No full suite was run.
- Full-suite gate returned `focused`; run the relevant Optimize/Vast/native-helper,
  guide/logging and offline browser regression files, not the complete suite.
- Real-browser parity checks cover the PBv9 menu entry, shared editor, native
  strategy/raw synchronization, draft restoration, Guide and Vast settings.

## Remaining external release steps

Publishing the candidate to `ghcr.io/msei99/pbgui-pb9-worker`, selecting its
registry digest and renting a GPU are distinct actions. None has been performed.
Obtain explicit publication and spending approval for a concrete GPU test before
those actions. An image ID from a local Docker build is not a registry digest.

## Native launch and worker completion (2026-11-20)

- Fable reviewed the bounded implementation and its corrections. Concrete
  findings covered actionable cloud validation errors, malformed result-owned
  metadata, UTF-8 and consistent artifact limits. Actual installed-source
  metadata advertises all four supported flags; PB8 argv retains its old flags
  and ordering.
- The old native Docker context lacked `/opt/pbgui/worker.py`, which the shared
  rental startup copies. The candidate now includes that shared worker plus the
  same portable argv/native-options modules used by the local runner.
- The worker pins public fork source `1b85ea6cc4bb7b917ada43725044b7559763c46e`.
  The configured PB9 checkout remains `93ff594`; no bot checkout or PR changed.
  The exact-source gate deliberately blocks native cloud preparation until the
  configured source matches the worker. `PB9.image` remains unset; no publication,
  GPU rental, credentials transfer or remote deployment occurred.
- Network-disabled container CPU smoke evaluated only the selected `train`
  scenario despite an initially disabled Suite. It then consumed saved Pareto
  results with both seed flags and a matching preparation report. A changed
  report was rejected before evaluation. This is not a physical GPU test.
- Real browser: actual PBv9 → Optimize menu, all new controls, separate Guide,
  browser reload preserving the open unsaved draft, current asset versions and
  no horizontal overflow. All test saves/queues use isolated fixtures.
- Current candidate evidence is in
  `setup/vast_gpu_benchmark/pb9_worker_candidate_native_launch.json`; the older
  ec20de4 receipt remains historical and does not prove this image.

- Attachment storage is persistent user data, bounded to 256 content-addressed
  files / 256 MiB. It uses a cross-process lock and private atomic writes.
  Configs/drafts contain only a validated hash reference; launches resolve and
  freeze bytes. Missing/corrupt attachments block new launches, while already
  frozen queued jobs remain independent. Native config limits are unchanged.
- Final cloud collection preserves session/search/native launch metadata through
  the existing checksum-verified archive and importer allowlist. Imported cloud
  checkpoints retain the existing local exclusion policy.

- Calibration cases now use the same portable native CLI options as the final
  optimizer. The last calibration/transfer check passed 57 tests; final
  worker/guide/logging audits passed 20. Serial 3753 was activated through the
  local API-only Services control; startup completed. No commit or push.
