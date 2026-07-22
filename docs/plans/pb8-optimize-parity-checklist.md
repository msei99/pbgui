# PB8 Optimize Parity Checklist

Status: complete and verified with the full offline suite (`4484 passed, 39 skipped`). This checklist is the completion record for the PB8 Optimize parity audit.

## Security and lifecycle

- [x] Strip PBGui cookies, authorization credentials, proxy credentials, and unsafe response cookies from the Pareto Dash proxy.
- [x] Persist Pareto Dash ownership, recover orphans, enforce bounded sessions and idle TTL, stop process groups, and close admission during shutdown.
- [x] Prevent result deletion while an optimizer, queue continuation source, or Pareto Dash session owns the result.
- [x] Make Pareto Dash launch/shutdown races and partial cleanup deterministic.

## Queue and process recovery

- [x] Classify permanent pre-launch failures as queue errors so one invalid item cannot block later autostart jobs.
- [x] Keep transient PB8 update/runtime-lock failures queued for retry.
- [x] Recover or clear stale/reused PID ownership without signalling unrelated processes.
- [x] Make stopped jobs requeueable through the shared workflow.
- [x] Repair missing immutable snapshots from the selected managed config.
- [x] Reconcile orphan queue snapshots, launch directories, PID/state/ready files, and partial queue transactions at startup.
- [x] Make queue reorder selection consistent with the final persisted order.
- [x] Prevent the global autostart claim from expiring while a live owner is still preparing or handshaking a launch.
- [x] Keep the PB8 worker alive after unexpected loop errors and expose it in Services Monitor.
- [x] Make shared PB7/PB8 Optimize settings strict and request-atomically persisted from either endpoint.

## Runtime and OHLCV

- [x] Run PB8 Optimize OHLCV preflight/preload against the PB8 runtime and PB8 cache paths rather than PB7.
- [x] Coordinate PB8 config/metadata helper subprocesses with the master runtime update lock and invalidate caches after updates.

## Results, Pareto, and resume

- [x] Decode suite Pareto metrics by metric name and scenario instead of exposing engine-space `w_N` objectives.
- [x] Return the shared `suite`/`stats`/`legacy` mode contract, scenario count, and selectable `median` statistics.
- [x] Make 3D plotting understand nested PB8 suite/stat objective values.
- [x] Validate checkpoint readability, `all_results.bin`, `write_all_results`, and compatibility before advertising or starting exact resume.
- [x] Resume checkpoint-only result directories without requiring Pareto config artifacts.
- [x] Avoid orphan queue items when a continuation/resume action fails.
- [x] Gate every result action by the artifacts it actually requires.
- [x] Provide the shared live log-dashboard status contract for PB8.
- [x] Cache incremental `all_results.bin` progress instead of rescanning complete history on every refresh.
- [x] Report PB8 config seed mode and matching backtest counts accurately.
- [x] Validate Pareto seed files/extensions and reject empty seed directories.

## Shared editor parity

- [x] Route PB8 universe, suite, and `coin_sources` pickers through the authoritative cached `/api/v7/symbols` and `/tags` endpoints.
- [x] Preserve PB8 metadata when queue settings refresh.
- [x] Preserve all inactive custom strategy blocks while switching strategies.
- [x] Preserve unknown/future `fixed_runtime_overrides` during structured saves.
- [x] Normalize canonical and shorthand `fixed_params` selectors in the fixed-bound UI.
- [x] Keep visible seed controls and hidden `optimize_runtime.mode/source` synchronized.
- [x] Treat `polish_percentage` consistently with PB8's fractional `--polish-pct` contract.
- [x] Expose PB8 HSL signal modes.
- [x] Expose all installed PB8 optimizer override helpers in structured controls.
- [x] Preserve native pymoo NSGA-II automatic population sizing.
- [x] Use PB8's native NSGA-III automatic reference-direction budget without changing PB7 semantics.
- [x] Prevent stale HTTP refreshes from overwriting newer queue WebSocket state.

## Verification and documentation

- [x] Add focused backend, frontend, lifecycle, security, concurrency, and compatibility tests for every resolved item.
- [x] Update English and German guides, the integration plan, and `releases/unreleased.md`.
- [x] Bump `api/serial.txt` after the final API/runtime edit.
- [x] Pass the complete offline test suite and final GitNexus change review.
