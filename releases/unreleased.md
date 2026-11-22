# Unreleased

## Development

- Fix PB9 Suite queue/Vast data preparation by renewing native optimizer source
  bindings after scenario overrides, dates and exchanges on disposable configs.
  Preserve authored configs and bounds; keep non-Suite exports independent of
  the Suite rebinding API. Ignore private atomic-save stages/backups in shared
  PB8/PB9 config listings and use the owning generation in optimizer diagnostics.

- Fix reviewed PB9 launch/editor races: serialize native starts with bundle writers,
  recheck assignment and validated files before launch, merge pending Raw JSON/form
  edits in input order, and scope inactive-strategy caches to one Run document.
  Preserve same-document strategy switches. Reject blank filter tags, deduplicate
  padded tags, retain safe helper failure categories, and guide missing-runtime
  errors to Welcome Setup. No bot-source or remote-host changes.

- Extend PB9 Run/VPS through PB8's shared native runner, signed Cluster transport,
  manifest reconciliation, checkpoint replay, monitoring and remote log/stop paths.
  Keep PB9 desired state and account identities separate; require fresh runtime,
  schema and replica capability evidence before placement. Publish complete private
  bundles with rollback, explicit legacy adoption and protected backups. Preserve
  open local log inodes; refuse deletion without fresh stopped proof. Existing
  managed instances can still publish deactivation while a peer is unavailable.
  Show native PB9 rows in the shared VPS Monitor. Covered by isolated protocol,
  API, collector and browser tests; no real remote deployment or trading test.

- Complete PB9 support in Storage using the shared registry, Review selection and
  cleanup worker: list native optimizer results plus GUI/native backtests, preserve
  recent output and block deletion while PB9 simulations run (rechecked per item).
  Keep candle/config/environment protection and PB7/PB8 behavior unchanged. Show
  the configured PB9 Python version in System and update both Storage guides.
  Invalidate pre-PB9 category snapshots so the existing scheduler rescans them
  automatically instead of requiring a manual refresh.

- Complete Welcome's optional PB9 runtime display and setup panel using the existing
  authenticated PB9 inspection and atomic path writer. Show source/schema, Python,
  CLI/Rust, commit and incomplete-update state; update automatically without
  overwriting setup drafts, and retain the selected section across browser reloads.
  Read-only installation checks on magicnuc1 verified the development checkout,
  official PB9 source, installed wheel receipt, matching native Rust and CLI.

- Fix normal PB9 installation/update preflight on both masters and VPS hosts:
  pass the selected generation's official branch explicitly to the shared guard
  and validate its matching fetched ref (`v9` for PB9, `master` for PB8). New and
  existing PB9 checkouts no longer fetch PB8's master and fail the v9 guard.
  Regression tests execute the rendered playbook guard commands against isolated
  repositories with distinct master/v9 commits; no remote host is modified.

- Rebuild, verify and publish the native PB9 Vast worker for official v9 source
  `bf7329b03`, matching the updated local runtime. Pin its immutable public
  manifest, native config contract and calibration allowlist together; retain
  all earlier rental image identities. Offline image checks cover Rust/source,
  CLI/cohort protocol 5, numeric Hyperliquid asset ages and inconsistent-ID
  rejection. No GPU was rented and no new GPU throughput is claimed.

- Verify that Hyperliquid numeric market-ID fix PR #2305 was merged into the
  official v9 branch and validate source `bf7329b03` with a matching rebuilt
  Python/Rust wheel. Confirm offline asset-age recovery, PBGui native config
  compatibility and bounded Bybit/Hyperliquid replays using the existing PBGui
  candle source. The matching Vast worker is published and pinned separately to the same
  official source revision.

- Publish the current PB9 work on `dev-pb9-integration` as an explicitly
  unfinished, not fully tested development snapshot. Document completed focused
  checks and the still untested real remote install/update and latest-worker GPU
  paths in the README and PB9 development status. This is not a stable release
  and does not update any master or VPS.

- Add explicit PB9 Install/Update actions to the shared local installer and VPS
  Manager. Reuse the PB8 wheel, locking and validation flow with separate sibling
  source/environment paths, schema checks before checkout, slot-specific failed
  update guards, optional PB9 telemetry and default-path uninstall coverage.
  Update All includes PB9 only when configured; fresh installs retain PB7/PB8.
- Publish and pin the PB9 Vast worker to PR #2305 source `1b5805ed`, including
  Hyperliquid numeric market-ID age resolution. Verify native build/CLI/cohort
  probes, offline BTC/ETH/SOL/HYPE age resolution and anonymous image download.
  Retain previous rental identities and PB8 workers. No new GPU rental or VPS
  update is performed by this change.
  Activate locally with API serial 3772 and verify the local/VPS PB9 actions
  and native worker revision through the normal PBGui browser menus.

- Prioritize the shared PB8/PB9 Optimize config list before native metadata,
  queue/result inspection and WebSocket snapshots. Show loading and initial
  failure states instead of a misleading zero-config count; retain deferred
  backtest counts, navigation and selection.

- Activate Passivbot PR #2305 locally at `1b5805ed` for Hyperliquid numeric
  market-ID age resolution. Verify the installed package, native source pairing,
  and offline BTC/ETH/SOL/HYPE age recovery with the existing minimum-age policy.
  Cloud workers retain their separately pinned source revision.

- Publish and pin the PB9 Vast worker at official merged source `da9f90d6`,
  retaining existing CUDA layers, native CLI/calibration checks and previous
  rental identities. Local and cloud source selection agree again.

- Verify the merged Passivbot PR #2268 at `da9f90d6` and update the separate local
  PB9 runtime with its matching rebuilt Rust extension. Validate native config
  helpers and offline Bybit/Hyperliquid external-source reads without changing
  candle files. Cloud workers retain their separately pinned source revision.

- Correct the prepared PB9 cloud worker to use the installed native optimizer CLI,
  matching local module-import semantics and retaining prepared result configs.
  Keep PB8 launch behavior and validated native options unchanged. Measure PB8/PB9
  on one synthetic RTX 3090 workload; distinguish PB8 proxy throughput from CPU
  validation and native GPU results. Keep the metadata-cache experiment out of
  production. Publish the verified CLI worker and pin its immutable public digest;
  retain prior PB9 rental ownership and calibration protocol compatibility.

- Add a separate PB9 native-cohort calibration protocol using shared Vast test,
  scaling, supervision, acceptance and result controls. Exclude cold compilation
  and unproven GPU work, revalidate rates on import and isolate accepted profiles
  by runtime. Preserve explicit execution settings and never infer a PB8 dispatch
  work cap from native cohort throughput. Publish and pin the protocol-5 worker;
  use native 128/256 Small probes and bind profiles to allocated CPU workers.
  Fix helper discovery when the worker starts through SSH outside its image directory.
  Verify a real RTX 3090 native profile (128 candidates, 10 completed cohorts,
  316 seconds); discard the incomplete 256 probe at the rental deadline and
  verify all synthetic test rentals are deleted.
- Verify native PB9 GPU execution end to end on synthetic RTX 3090 data. Keep
  runtime-specific calibration evidence: the native GPU backend does not emit
  PB8 proxy-generation evidence. Reject incompatible images/protocols before
  credentials or payment.

- Accept and validate configurable protocol-4 calibration plans in the shared
  SSH startup path, alongside the existing fixed protocol-3 tests.

- Resolve referenced prepared snapshots when creating GPU calibrations, retaining
  native launch artifacts and candle hardlinks after Requeue without copying data.

- Filter Vast job lists by generation before estimates and row limits. Compute
  optional native workload estimates during input preparation and preserve them
  on Requeue; polling PB9 jobs never starts a native/PB8 helper.

- Activate the independently published PB9 Vast worker by its verified immutable
  digest, matching the local PB9 source. Keep PB8 on its existing worker image.

- Complete the PB9 Optimize launch controls on the shared PB8/PB9 page: select
  Suite scenarios, filter/compress managed seeds, and import a completed native
  preparation report. Keep large reports in bounded private attachments; configs/drafts hold references.
  Freeze report bytes and native selections in queue/result
  provenance and retain them for checkpoint resume. Preserve PB8 CLI arguments.
- Package native launch options and seeds through the shared Vast archive,
  verify their hashes, and use the same argv builder locally and in the worker.
  Fix PB9 admission incorrectly comparing against the PB8 source revision.
  Build a native PB9 worker with its runtime entry and public pinned source;
  verify all four CLI options in offline synthetic CPU runs. Publication and
  matching local runtime activation are complete; native GPU optimization passed
  a physical smoke test. Native performance calibration uses its own protocol.

- Document all 27 native PB9 metric selectors in the EN/DE Optimize guides,
  with definitions, units, objective/limit guidance and availability rules.
  Point Pareto Explorer users to the metric reference.

- Use native PB9 metrics in Pareto Explorer charts, Quick Views, activity
  details and risk panels. Limit Quick Views to available metrics, retain
  unavailable values, and omit single-scenario robustness from ranking. Read
  PB9 min/max reductions and reuse PB8 incremental replay for compressed results.
  Restore chart choices and the selected candidate on browser reload.
  Preserve the shared PB7/PB8 Explorer workflows.

- PB9 Sweep Cycles restore each window's gain from native CAGR and the actual
  evaluated period PB9 recorded for that window. Sweep is marked unavailable
  when that period is missing, a window averages several exchanges, CAGR is
  -100% (or too close to it), or values overflow. Suite scenario `coins` and
  `ignored_coins` now keep explicitly empty lists, text and sides; use
  **Inherit (base config)** to remove an override and **Not set** to omit a side.

- Fix PB9 Optimize review findings. Side JSON and native policy edits no longer
  revert each other, and changing the HSL scope keeps the policy fields. Suite
  scenario coins keep their list, text or Long/Short form. Editing a Suite
  scenario no longer hangs the PB8/PB9 Optimize editor. The GPU backend keeps
  `population_size` and Pymoo settings. Pareto Explorer, Results Strategy and
  Sweep Cycles now work for PB9, and saved PB9 results
  stay readable with local source changes while launches still require a clean
  checkout.

- Close PB9 Optimize editor gaps: show the value a fixed bound keeps, reset Long
  or Short overrides to inherited values, edit non-optimizable numeric policy
  values such as `forager.score_hysteresis_margin`, and select GPU screening
  scenarios from Suite labels.

- Add the official PB8 → PB9 Optimize migration (V9 on PB8 configs and Pareto
  configs) with the shared review dialog, the PB9 Optimize Queue controller on
  Services, PB9 Pareto Explorer navigation and corrected PB9 preload, Vast help
  and fine-tune selector guidance.

- Add dedicated PB9 Optimize configuration-prefilter controls for checks,
  directional selection, nullable thresholds, probe fractions, assumptions and
  penalties. Reuse shared native field controls, preserve sparse defaults and
  validate enabled checks before saving. Add editable GPU-screening scenario
  labels and verify the shared optimizer-override controls. Keep the fallback for
  future keys.

- Let PB9 Optimize add and remove native Global, Portfolio, Long, and Short
  bounds, including fields absent from the starter config. Reuse the PB8
  Additional Parameters controls for unmapped keys in the optimizer config.

- Make PB9 Optimize bounds easier to scan with collapsible policy and range
  sections, a global range filter and visible side override counts. Group side
  choices by inheritance and retain view preferences when the draft is reloaded.
  Keep the Optimize editor open when Escape closes its Guide.

- Stack portfolio policy and shared defaults above Long and Short in PB9
  Optimize. Keep both side-bound panels visible and let users copy or remove
  individual global ranges as explicit side overrides.

- Show only authored PB9 Optimize bounds. Keep shared starter and strategy
  ranges under Global and preserve intentional Long and Short overrides.

- Align native PB9 Optimize policy labels and checkboxes with PB8's shared
  runtime-override layout. Restore dotted, keyboard-accessible parameter help
  using the installed PB9 documentation and explicit inheritance context, sharing
  PB8's bounded document reader without borrowing unrelated field descriptions.

- Scope shared PB7/PB8/PB9 backtest capacity to the current PBGui installation.
  Another checkout's legacy backtest daemon no longer blocks local starts when
  both installations use the same native Passivbot checkout.

- Queue confirmed navigation restarts until protected work finishes instead of
  rejecting them with a Restart blocked dialog. Coalesce duplicate requests,
  defer new PB9 inspections, restore progress after browser reload and drain
  owned restart preparation safely on shutdown.

- Estimate local PB9 optimizer queue workloads through the PB9 config gateway
  and native training data plans, sharing PB8's candle-count calculation and
  bounded snapshot cache. Keep unresolved inputs unknown and stop routing PB9
  snapshots through the PB8 persistent helper.

- Extract authored PB9 Pareto configurations through the installed native result
  reader before handing them to Backtest. Remove native result-only fields such
  as `backtest.coins`, preserve sparse policy and GUI provenance, and retain
  strict validation for genuinely unknown configuration fields.

- Route PB9 optimizer candidates, validation jobs and Open Queue to PB9
  Backtests through the shared queue client. Preserve mounted application
  prefixes and keep queue status scoped to the selected generation.

- Follow up the separately maintained local PB9 external-source test build:
  reject corrupt/empty daily candle files, invalidate prepared caches on selected
  source-file changes, and warn about missing post-warmup coverage. The approved
  local native commit is `93ff594`; PBGui does not install or publish this
  bot-source patch automatically.
- Publish the reviewed native external-source fix against current Passivbot v9
  as [PR #2268](https://github.com/enarjord/passivbot/pull/2268), preserving the
  upstream cache publication lock. The active local bot checkout is unchanged.

- Correct PB9 source Readiness to preserve exact minute bounds instead of
  truncating the checked day to midnight. Read the actual native coverage catalog
  offline, restoring the temporary helper context and original manager range.

- Correct local PB9 Backtest and Optimize execution to use the configured bot
  checkout and its persistent native cache, as PB8 does. Reuse downloads,
  coverage proofs and prepared data across runs; do not create per-job raw stores.
- Share the PB8 persistent backtest launcher with PB9 while retaining isolated
  native environments, exact unit cleanup and legacy process recovery. Harden
  runner log creation and keep native Dash processes on the filtered environment.
- Readiness inspects the execution cache read-only, checks the same native
  offline metadata, and asks the native loader to admit an external source's
  first day. Preserve native acquisition warmup independently of trading-age
  gates; report native source rejection instead of treating daily files as proof.
- Enable PB9 Optimize from Result through PB8's shared handoff. Account for the
  PB9 installation in Storage and protect its configured environment, candle
  store and live configurations from unused-data cleanup.
- Restore unsaved Backtest names, values and sections on browser reload through
  the same credential-rejecting serialization as Optimize. Retain native source
  revisions, load unchanged documents afresh and reject stale editor requests.
  Allow native coin names such as TOKEN in inline override maps while continuing
  to reject credential fields inside those overrides.
- Show PBGui's selected candle source in new native Optimize drafts instead of
  presenting an empty path that differs from the queue's default execution source.

- Relocate the local PB9 installation and its environment outside PBGui, alongside
  PB8. Update the configured paths, environment launchers and result links while
  preserving the existing checkout, installed dependencies and job data.

- Fix the shared Run/Backtest/Optimize coin search for PB8 and PB9: match coin
  names and native contracts without treating the `hyperliquid` venue prefix
  as a HYPE match. Keep explicit qualified searches and stored IDs intact.

- Reuse PB8's native market-catalog algorithm and filter projection for PB9 Run,
  Backtest and Optimize. Show ordinary coins once by name, retain exact IDs for
  genuine collisions and imported selections, and read public metadata through
  an isolated cancellable PB9 helper without changing shared caches.

- Fix PB9 Optimize 1st/All lookups in unsaved drafts: stale effective side JSON
  no longer reintroduces cleared coin selections or causes false input-change
  errors. Synchronize side-JSON edits back to the pickers and retain rejection
  of results after actual edits during a running lookup.

- Fix PB9 Optimize saving without Suite mode: accept the native template's
  explicit null scoring/limit scenarios and reducers while still requiring
  Suite mode for named scenarios. Preserve PB8 validation and authored values.

- Fix native PB9 Optimize and Backtest exchange pickers: discover the installed
  connector/history capabilities instead of limiting choices to the two venues
  selected in the template. Use the same catalog in Suite Mode and coin sources.

- Build PB9 Optimize on an instance-owned engine shared with PB8: one editor,
  isolated native configs/queues, immutable provenance, cancellation and detached
  runner lifecycle. Reuse bounds, scoring, scenarios, results and Pareto controls;
  preserve sparse native inheritance and canonical PB9 names when editing.
- Add native PB9 cloud data planning and explicit Vast worker profiles. Reuse the
  existing account, marketplace and export pipeline; keep paid PB9 actions blocked
  until a verified native worker digest is published.
- Share Vast calibration, pool/requeue ownership and retained performance browsing
  with explicit PB9 runtime checks. Native performance comparisons survive job
  cleanup; history updates automatically without a manual refresh action.
- Preserve native strategy, GPU and inherited policy edits in the shared editor.
  Validate native checkpoint identities through effective policy, including coin
  overrides; retain the PB8 CLI contract while using PB9 polish option names.
- Keep the unpublished PB8 rental-protocol candidate source hash current after
  runtime dispatch changes; preserve its previous build receipt as historical
  evidence rather than claiming a new verified image.
- Build and verify a local PB9 worker candidate from the shared Docker recipe.
  No image publication, GPU rental or remote deployment is included. Keep PB8 AI
  Loops unavailable on PB9 until its separate execution protocol is adapted.

- Add native PB9 Retest & Replace and daily/weekly Retest Schedules through the
  existing shared archive dialog and scheduler. Native jobs keep their own queue,
  immutable snapshot and completion provenance; failed, altered or ambiguous
  results leave the archived original intact. Retain a recovery backup and
  resume interrupted enqueue/replacement stages after API restarts. Keep completed
  replacements recoverable when finalization fails, continue schedule batches
  after individual target errors, and retain terminal queue records until their
  companion cleanup finishes.
- Serialize shared retest/schedule state updates, join cancellable preparation
  on API shutdown, and push archive-state changes automatically. Native unattended
  execution uses the existing PB9 queue Autostart setting.

- Integrate native PB9 Results with the shared PB7/PB8 Archive: version-owned
  exports, native summaries/charts, Add to Run drafts and manual requeues through
  the cancellable PB9 request owner. Preserve native artifacts and provenance
  during export and group rename; keep unsupported optimizer archive controls blocked instead of using legacy conversion.
- Browse native archives without automatic migration, reuse archive change
  notifications and restore archive navigation, filters and row selection after reload. Share one bounded
  NPZ reader and summary projection between local and archived PB9 results.
  Validate NPY storage declarations before allocation and constrain imported
  candle/dataset paths to the existing approved source roots.
- Keep stored archive tokens out of settings responses and form HTML. Blank
  inputs retain credentials; explicit clearing removes them. Git authentication
  uses a transient, origin-scoped environment header with matching fetch/push
  origins and disabled redirects, never credential-bearing command arguments.

- Add PB9 queue Autostart and CPU slots through the existing Backtest Settings
  dialog, with separate native settings (default off / one slot), oldest-first
  dispatch, fresh pause checks and shared PB7/PB8/PB9 capacity. Push policy changes
  automatically while preserving in-progress dialog edits.
- Reject busy automatic job-lock admission without waiting, return cancelled
  unpublished starts to the queue after confirmed cleanup, stop dispatch at full
  shared capacity and count identified pending PB9 runners only once. Throttle
  repeated unchanged queue-deferral logs. Existing blocking transactions retain
  their locking behavior.
- Share the PB8/PB9 controller lifecycle and drain owned scans/launch preparation
  on API shutdown. Published native backtests remain independent and continue
  across API restarts; pausing autostart never stops running jobs.

- Add native PB9 OHLCV Readiness and 1st/All using the existing PB8 panel,
  planner reports and lookup controller, with PB9 universe/activation/age/warmup,
  approved source roots and direct PBGui candle access. Preserve empty native
  source selection and report prepared-dataset overrides separately.
- Use the PB9 selector transport for lookup progress, cancellation and unreaped
  process-group ownership; validate helper source identity before publishing
  results. Separate PB8/PB9 capacity and history, keep lookups login-scoped and
  join owned helpers at shutdown. Public metadata links are read-only; online
  fallback writes only in cleaned private workspaces and never copies candles.
- Automatically update OHLCV Readiness after edits, window return and background
  changes; ignore stale responses and restore the open panel on browser reload.
  Remove the shared manual Refresh Check control and genericize lookup labels.

- Address the bounded Fable review of PB9 Run/Backtest: preserve validated filter
  preferences and notes through migration, drafts, saves, queue snapshots and
  result reuse while keeping native inputs free of GUI metadata. Share the
  existing Run filter validator with Backtest.
- Correct PB9 stopped status, restart into a fresh result namespace, honour
  config deletion with results using recoverable managed trash, and remove owned
  queue workspaces without following public market-data links. Preserve prior
  result provenance and reject changed restart snapshots/runtimes.
- Reuse the shared Balance Calculator/Calc Balance in PB9 Backtest and the same
  native analysis projection in Run. Discover unregistered native Run fields
  under Additional Parameters, prevent duplicate parent/child field authors and
  unintended coin-list rewrites, and make Save & Queue single-flight/idempotent.

- Add PB9 Additional Parameters through the existing PB8 typed fallback, with
  managed result-path preview, template/registry/source discovery, sparse default
  preservation and invalid-JSON save protection. Discover unregistered bot and
  logging fields under Advanced Settings. Preserve current edits and GUI metadata
  in Add to Run drafts, prevent unconnected tools from routing PB9 configs to
  legacy handlers, and document remaining PB8 integration gaps. Replace the fixed
  zero in the PB9 config-list coin count with native side/global universe counting
  and return UTC modification dates instead of seconds misread as 1970 dates.

- Align the PB9 Backtest editor with PB8's compact identity, balance, fees,
  OHLCV, source, coin-filter and Long/Short layout. Reuse the existing source
  chips, market overrides, coin overrides, suite editor and numeric steppers;
  keep additional native fields under Advanced Settings. Show effective side
  JSON while saving only edited overrides, preserve native side-owned lists,
  reducer maps and sparse inheritance. Restore open sections/JSON views after
  reload and open the Guide topic for the selected bot version. Share compact
  metadata-field styles with Run, omit literal registry wildcard controls,
  escape numeric GUI metadata and retain coin edits during catalog loading.

- Extend the existing cross-version Backtest Compare view to PB9, with PB7/PB8/
  PB9 result selection, version-owned equity adapters and restored comparison
  state after reload. Retain an exact PB8 result reference for new native
  migrations through save, queue and result provenance, including after queue
  cleanup. Open the existing chart directly from that reference, reject changed
  source results, and leave older unlinked runs manually comparable. Keep GUI
  references out of native runtime configs and isolate malformed metadata from
  the result list.

- Restore PB9 result-chart close prices using the shared PB7/PB8 read-only
  PBGui MarketData price reader. Add the missing authenticated native endpoint
  and split combined exchange labels into individual venue choices, including
  already indexed results. Keep full-coverage auto-selection, venue switching
  and balance/equity traces on the existing chart path. Restore the selected
  venue on reload, avoid PB8-only HSL reconstruction for native results, and
  retain basic field edits when a value is changed back after autosync.

- Reuse the PB7/PB8 OHLCV source control in PB9 Backtest. Default new configs
  and Run handoffs to shared PBGui candles while preserving saved/imported
  source choices and explicit Passivbot defaults. Wait for the PBGui Data
  lookup before saving and ignore stale responses; initially open native
  Backtest Settings. Offline jobs reference existing public market/listing
  metadata without copying candles or metadata, keep online writers isolated,
  and report missing market metadata before consuming a process slot.

- Open saved PB8 backtest configs and local results as native PB9 drafts using
  the official installed Passivbot v8-to-v9 converter. Review the complete
  best-effort changes and warnings before adoption; retain the report in the
  native editor and reject source/converter changes during review. Snapshot
  local JSON overrides safely, isolate conversion, preserve PB8 originals and
  never automatically save or start a job. Align the parent PB9 backtest
  validation gate with its helper's inline coin-override support.

- Add manual local **PBv9 → Backtest** on the shared PB8 Backtest shell, with
  current Run-editor drafts, native typed fields, sparse config round trips,
  revision-protected saves and immutable queue snapshots. Isolate v9 configs,
  caches, logs and results; record schema/source provenance, reject changed
  snapshots or sources, and reuse the independent runner and shared slot/index
  helpers. Keep original source/provenance beside successful results after
  queue cleanup. Translate native NPZ equity timestamps for the existing charts
  and retain missing balance samples as gaps.
  Keep native jobs independent of API restarts and guard process control by
  exact PID/create-time/command ownership. Subsequent additions above provide
  autostart, archives and optimizer handoffs; paid cloud execution stays gated.

- Bring PB9 Add/Edit closer to the shared PB8 form: keep native basic live
  fields visible, group remaining settings under Advanced Settings, remove
  redundant side-activation checkboxes, and restore shared long/short JSON
  editors with inherited and sparse-override views. Only edited inherited
  values become overrides; invalid side JSON stays visible and blocks Save.
  Preserve the JSON view on reload and cover the native field mappings and
  inheritance round trips with isolated browser tests.

- Complete PB9 Run browser workflows against real native validation: retain the
  saved instance URL after applying a backup, count TWE only for explicitly
  enabled sides, and allow bounded parallel selector/status reads while keeping
  native helper concurrency capped at two. Cover these findings with focused
  regression tests; verify copy, inline overrides, strategy switching, rollback,
  account conflicts, WebSocket updates and sidebar bounds using temporary data.

- Add local **PBv9 → Run** using the shared PB7/PB8 instance list, structured
  editor, coin selectors, inline override editor, dialogs and log panel. Build
  typed PB9 controls from the native registry and preserve sparse inheritance.
  Keep native configs and private PBGui sidecars separate, require optimistic
  versions, retain filter preferences, and reuse the shared backup/publication
  service. Preserve newer typing during Save and restore editor navigation;
  update clean editors automatically without replacing unsaved changes.
- Extend the PB8 supervisor through runtime hooks for PB9 commands, process
  identity, crash handling, credentials and read-only Rust fingerprint checks.
  Serialize account starts across PB7/PB8/PB9 and reject conflicting assignments.
  PB9 currently runs locally; remote Run, Backtest and Strategy Explorer remain
  unavailable. Add bilingual guides and isolated API/browser/runtime coverage.

- Prepare PB9 Run to reuse the PB8 editor and live-bundle infrastructure:
  expose native field/strategy metadata through the isolated PB9 helper,
  validate inline live coin overrides without file references, and share
  backup/publication helpers through explicit generation-specific stores.
  Existing PB8 lifecycle and API contracts remain unchanged.

- Retain the advanced PB9 JSON config library at `/api/pb9/main_page` with
  separate runtime-path settings, native authored templates, inline validation,
  private revision-protected save/open and automatic file/status updates.
  Preserve newer edits, exact large integers and raw duplicate-key rejection;
  reconcile uncertain writes without automatic save retries. Unchanged files
  use a bounded revision-only read rather than native validation on each poll.
  Save reconciliation compares exact authored values on the server and performs
  no native validation or writes.
  Restore file/search/runtime-panel navigation, reuse the shared sidebar and
  Guide, and include matching English/German documentation. This preview needs
  a separately installed PB9 runtime for native operations and starts no bot jobs.

- Add an authenticated PB9 config library with bounded listing, validated loads
  and private atomic saves under `data/configs_v9`. Creation is exclusive;
  updates require the exact loaded revision. Keep native work and transactions
  owned through shutdown, allow bounded/cancellable lock waits, and check stop
  requests immediately before publication without rolling back committed files.
  Never follow symlink parents, repair existing permissions, delete or migrate
  configs. Missing reads do not create storage. File errors use fixed codes.

- Add authenticated PB9 inline-document validation with strict, bounded JSON
  requests, a 64-level nesting limit, server-selected runtime paths and separate
  authored GUI metadata. Reject lone Unicode surrogates and sanitize response
  encoding failures as well as native exceptions.
  Reject semantic input errors with fixed 422 responses; retain operational
  failure codes, shared origin protection and bounded worker ownership. Explicit
  shutdown also cancels native validation groups, including their descendants.
  This endpoint does not save files, migrate configs or start bot jobs.

- Expose authenticated, read-only PB9 runtime inspection and authored-template
  retrieval through the API. Use one allowlisted settings snapshot per request,
  retain at most two explicitly owned workers, and drain them on cancellation
  and shutdown. Active inspections participate in the coordinator restart gate;
  requests cannot supply runtime paths or mutate config files. Responses are
  uncached, and template POSTs reuse the shared browser-origin protection.
  Early SIGTERM/SIGINT handling cancels native helper groups before Uvicorn
  drains HTTP requests; shutdown then joins their owners. Coordinator restarts
  wait for active inspections, while an explicit stop aborts those read-only
  operations without extending existing service stop deadlines. Static Git
  preflight also gates each new command on the stop flag and uses a one-second
  per-command timeout for API-owned inspections. On slow disks, commit metadata
  may therefore be unavailable with a warning while static detection succeeds.

- Add opt-in SHA-256 revisions to PB9 config reads and guarded updates to the
  shared save entry point. Recheck exact source bytes under a reentrant,
  descriptor-bound directory lock before private atomic replacement, reject
  stale/deleted targets, and keep native validation outside the transaction.
  Default saves remain create-only; no revision is stored in config metadata.
  The lock coordinates PBGui writers; noncooperating same-UID file editors
  remain outside that guarantee. UI/API binding is still pending.

- Add validated, create-only PB9 config saving through the shared save entry
  point. Publish a complete private file atomically below an owned 0700 root,
  preserve authored sparse source and GUI metadata, bound formatted output,
  and reject existing names.
  Concurrent creators cannot overwrite one another. Default saves stay
  create-only; UI binding follows separately. Publication acknowledgement failures have a
  distinct error code so a committed file is never blindly removed or retried.

- Add an explicit, read-only PB9 file context to the shared config loader.
  Read only bounded regular JSON files beneath an approved directory using
  no-follow descriptors, reject duplicate keys/nonfinite numbers, and validate
  the authored source with its isolated PB9 runtime. Retain sparse source and
  GUI metadata without the PB7 pipeline. No saving or UI/API binding is added.

- Add an in-memory PB9 document boundary that validates an independent authored
  snapshot while keeping root GUI metadata outside the native helper. Return
  sparse config, unchanged GUI metadata and inspected source identity separately;
  reject non-JSON or oversized documents before runtime inspection. Existing
  PB7/PB8 file loaders and all persistence paths remain unchanged.
  The document cap counts ASCII-escaped JSON bytes; the helper's independent
  request cap also includes its runtime envelope and may reject near-limit
  native payloads after inspection, before launching a process.

- Add native in-memory PB9 validation through the isolated helper. Gate supported
  inline inputs before native imports, reject external coin/config references,
  GUI/internal metadata and symbolmap-dependent coin sources, and return only
  acceptance plus source identity. Keep the caller's sparse config untouched;
  no migration, file loading/saving, API binding or runtime compilation is added.

- Add an isolated PB9 authored-template adapter using a one-request helper in
  the selected environment. Bound execution time and protocol output, strip
  ambient Python/provider environment settings, verify source-module and schema
  identity, and retain sparse JSON-native template structure. The initial Linux
  transport cleans up its process group, including descendants within that group;
  other platforms fail explicitly before startup. No API/UI binding,
  config preparation, migration, saving or bot start is introduced.

- Protect existing PB9 path reservations when saving PB7/PB8 paths from Welcome.
  Validate only actual changes under the INI transaction lock, reject conflicts
  with HTTP 409 and stable error-code headers, and retain raw interpreter
  identity when legacy normalization follows a symlink. Unrelated settings and
  installations without PB9 keep their existing behavior.

- Add a validated PB9 path writer using the existing locked, atomic INI
  transaction. Save both canonical paths together while preserving interpreter
  symlinks, reject invalid layouts without publication, and support explicitly
  clearing both keys. Register next-operation apply metadata without exposing
  PB9 in Welcome setup or adding an API route.

- Add an unconnected, read-only PB9 layout detector with a lazy INI wrapper for
  pb9dir/pb9venv. Check source and environment separation, static Python version,
  CLI and Rust artifact presence without starting the runtime. Report detection
  separately from readiness; missing Git identity is a warning and Rust stamp
  presence is reported without requiring or verifying it.

- Reject v9 config schemas in PB8 guard checks for both local source trees and
  selected Git refs before process stops or checkout. Preserve PB7's package-only
  check, PB8 return values and CLI markers. Share static schema/literal helpers
  with UTF-8 BOM support and a 256 KiB parsing limit.

- Add an unconnected, read-only Passivbot source classifier that distinguishes
  PB8 and PB9 by their config schema rather than their shared package version.
  Inspect Python declarations with AST and report Git identity and diagnostics
  without importing bot code or changing existing runtime behavior. Reject unset
  or relative source paths, isolate Git reads from inherited repository/config
  overrides, and withhold the commit when inspected metadata differs from HEAD
  beyond CRLF/LF line-ending conversion.

## Documentation

- Add a staged Passivbot v9 integration plan with an initial compatibility
  inventory, independent Codex/Claude review workflow, and milestone acceptance
  criteria for development on main.
