# PBv8 Optimize

PBv8 Optimize manages Passivbot V8 optimizer configurations, queued jobs, results, and Pareto candidates independently from PBv7. The page uses the same template, panels, and visual editor as PBv7 Optimize. A version adapter translates only the PB8 API paths and nested configuration model; there is no separate PB8 optimizer UI.

## Configs

- **New Config** loads optimizer defaults, strategies, bounds, scoring metrics, limits, backend options, and Pymoo choices from the installed PB8 runtime.
- All installed PB8 strategies are supported: `trailing_martingale`, `ema_anchor`, and `trailing_grid_v7`.
- Changing `strategy_kind` activates that strategy's runtime-provided bot defaults and bound set without deleting any customized inactive strategy block. Unsaved bounds and bot values are cached per strategy while switching in the editor. The current runtime exposes 84 controls for `trailing_martingale`, 58 for `ema_anchor`, and 86 for `trailing_grid_v7`.
- The visual editor reads and writes nested PB8 bot and bound paths. Raw JSON remains synchronized and preserves future or expert fields, including unknown `fixed_runtime_overrides` and canonical or shorthand `fixed_params` selectors.
- Frequently used optimizer controls remain in their existing PBv7 editor sections. PB8-only RNG seed, fine-tune selectors, polish percentage, and polish bounds mode are included without creating a separate editor.
- Saved configurations are validated by PB8 and stored as recoverable bundles under `data/opt_v8`.
- Official **Convert to V8** migration is available for PBv7 Optimize configurations. Migration stops when PB8 reports unresolved or manual-review fields.
- PBv7 Pareto candidates expose the same official migration action and are accepted only from managed PB7 result directories.

The PB8 editor exposes all installed HSL modes and optimizer overrides in separate Long and Short cards. **HSL enabled** controls whether hard-stop behavior participates in optimizer evaluations. **Restart after RED** is an explicit `always`, `threshold`, or `never` selection; `always` is PB8's optimize default so evaluations resume after cooldown instead of terminating on persistent drawdown. `polish_percentage` is displayed as a normal percentage but converted to PB8's fractional `--polish-pct` value, so `20` means `0.20`. Pymoo keeps PB8's native automatic sizing: NSGA-II uses `250`, while NSGA-III derives its reference directions from a budget of `500`.

PB8's default optimize bounds are initial search ranges, not hard slider limits. The editor therefore uses parameter range metadata for the slider and allows values below PB8's defaults, such as `n_positions = 1`.

Forager volume and volatility EMA span sliders have a minimum of `1`. To exclude these parameters from optimization, keep a valid positive bot value and use the row's **Fixed** checkbox instead of setting the span to zero. Backend validation still accepts imported zero spans only when the corresponding Forager signals are guaranteed to remain disabled.

Selecting several exchanges keeps PB8's native combined-dataset behavior. Use explicit Suite scenarios when each exchange must be evaluated separately.

## Queue

Queue entries contain immutable PB8 configuration snapshots. Editing a saved configuration after queueing does not alter an existing queue item.

When the editor is opened explicitly from a queue row, **Save** is different: it saves the managed config and refreshes that same queue item's snapshot. Changes such as `optimize.n_cpus` are therefore present when the row is reopened or started.

The editor also keeps its navigation origin: **Home** or **Save** returns a queue-opened config to the Queue panel, while a config opened from Configs returns there.

- **Start** manually launches the selected item.
- **Stop** terminates only the verified PB8 optimizer process.
- **Requeue Fresh** starts a new optimizer run without reusing optimizer state.
- **Continue from Pareto** uses managed Pareto files as `--start` seeds.
- **Resume Checkpoint** resumes the exact managed optimizer state with `--resume`.

Checkpoint resume accepts only local PB8 results managed by PBGui. Arbitrary checkpoint files are rejected because Python pickle checkpoints must be treated as trusted executable data.

PBGui advertises exact resume only when the checkpoint and `all_results.bin` are readable, `write_all_results` was enabled, a config is recoverable, and PB8 confirms compatibility. Config and queue creation then happen as one transaction. Checkpoint-only result directories do not require a separate Pareto JSON config.

PB7 and PB8 share one automatic optimizer slot: autostart never launches both versions at the same time. Explicit manual starts may run in parallel. Each optimizer controls its own parallelism through `optimize.n_cpus`.

PB7 and PB8 use one shared Queue **Settings** configuration. Saving it on either Optimize page immediately controls both queues and both autostart workers. **Autostart CPU** may be edited and saved at any time; **Override config CPU** decides whether it replaces `optimize.n_cpus` for automatic starts, while manual starts keep the config value. **Use PBGui Market Data** applies the managed OHLCV source to a launch copy without changing the saved config or immutable queue snapshot.

Running PB8 optimizer jobs survive an API restart. On Linux, each optimizer runs in its own transient user-systemd unit outside the API service cgroup; PBGui records process ID, process creation time, PB8 version, and PB8 commit so stale or reused process IDs cannot be controlled accidentally.

Permanent preparation errors move only their queue row to an actionable error state, while update or runtime-lock contention stays queued for retry. Startup reconciles queue snapshots, launch directories, PID, ready, and state records without signalling unverified processes. The PB8 controller is shown in **Services Monitor** and survives unexpected worker-loop errors.

**OHLCV Readiness** and preload run through PB8's own virtualenv, planner, cache paths, and native `passivbot download` command. Explicit read-only sources outside the approved PB8 or PBGui market-data roots are rejected instead of falling back to PB7.

## Results And Paretos

Results are read only from `<pb8dir>/optimize_results`. The Results and Paretos panels provide the shared PB7 workflow for result inspection, deletion, 3D plots, Pareto Dash, candidate JSON, metric summaries, and seed bundles.

PB8 result actions distinguish three different workflows:

- Opening a Pareto candidate as a PB8 Backtest draft performs a standalone backtest.
- Starting a new PB8 Optimize draft uses one or more Pareto candidates as seeds.
- Resuming a checkpoint continues the existing backend state and result stream.

The shared Pareto Explorer uses version-specific roots and understands PB8 nested bounds, nested bot parameters, scoring goals, limits, suite metrics, and incremental `all_results.bin` records.

Suite summaries keep their configured objective and scenario names and support `mean`, `min`, `max`, `std`, and `median`. List rows also include canonical `gain`, using PB8's persisted `gain_usd`, `gain_strategy_eq`, or `gain` metric in that order. Statistics use the requested metric statistic when available and otherwise use the persisted scalar objective. Unrelated suite statistics are not sent to the browser. Compact file-signature caching keeps repeated large Pareto lists responsive while changed, deleted, malformed, or actively rewritten candidates are handled independently.

Result actions are enabled only when their required artifacts exist. A verified optimizer blocks deletion only for the exact immediate result directory that it or one of its recursive children has open. Unrelated older results remain deletable. Continuation queue sources and Pareto Dash sessions remain exact deletion blockers, and uncertain active-process ownership is handled conservatively. Batch deletion preserves these conflict details and stages selected directories atomically. Pareto Dash runs through a credential-isolated, bounded PBGui proxy with idle cleanup and verified orphan recovery. Its PBGui window can be moved by its header and resized from every edge or corner, while the dashboard retains PB8's original native presentation.

## Archives

PB8 Optimize configurations and PB8 Backtest results use the existing Archive workflow. Files are stored under their `config_version`, so PB7 and PB8 content cannot overwrite each other. Import, export, view, delete, restore, and handoff actions always use the parser belonging to the archived configuration version.
