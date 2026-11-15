# Unreleased

## Development

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
  exact PID/create-time/command ownership. Cloud, automatic startup, archives
  and optimizer handoffs remain unavailable for PB9.

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
