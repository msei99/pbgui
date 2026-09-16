# PBv8 Optimize

**Settings → Performance History** retains Vast throughput and workload metadata after queue deletion. Select matching workloads for speed comparisons; see the [Vast GPU guide](48_vast_gpu.md#performance-history).

For cloud CPU capacity, set **Min CPU cores** in Vast Settings; Cloud Auto uses the rented allocation. Manual Rent takes its specifications from the selected offer. Later queued jobs may trigger a confirmed transfer-reserve adjustment that shortens rental time within the same budget. Optimizer log retrieval errors no longer prevent stop or final collection.

Vast input uploads recover from transient connection errors for up to 15 minutes without new transfer progress, with 15/30/60-second retry pauses and retained partial data. The log displays retry status. A healthy worker's upload is no longer cut short by its earlier setup timer; the rental deadline and collection reserve still apply.

Vast offer searches show **Previously used**, **Working** and **Preferred** host status. Use **Prefer host** in offer Details or the rental Host card to prioritize that physical machine within your existing limits. Manage preferences and manual Working marks in **Settings → Known & preferred hosts**. Blocked hosts remain excluded.

For Vast rentals, **Block host** in the log's Host card excludes the physical machine from future offers and rentals. Manage or undo exclusions under **Settings → Blocked hosts**. Blocking leaves the current rental running; **End rental** remains a separate action. See the Vast GPU guide for details.

PBv8 Optimize manages Passivbot V8 optimizer configurations, queued jobs, results, and Pareto candidates independently from PBv7. The page uses the same template, panels, and visual editor as PBv7 Optimize. A version adapter translates only the PB8 API paths and nested configuration model; there is no separate PB8 optimizer UI.

If PB8 is unavailable after an incomplete installation or update, a persistent **PB8 update required** warning appears above the workspace with the runtime error and a link to VPS Manager. The page remains usable for diagnosis instead of hiding the issue in transient notifications.

The Configs list starts loading in parallel with slower PB8 settings and metadata. Its table uses a lightweight summary request that skips optimize-result inspection, while the separate Results panel continues to load the complete result metadata.

## Parameter tooltips

Hover a parameter label to read the original explanation from the installed Passivbot documentation. The tooltip names its local source file; long descriptions can be scrolled by moving the pointer into the tooltip. The same parameter uses the same explanation in Run, Backtest, and Optimize, including nested bounds and optimizer overrides. Documentation is loaded locally without an internet request or a working Rust extension. PBGui-specific controls retain their own input hints; generic runtime placeholders are suppressed when upstream has no matching description.


## PB8 schema 8.4 settings

The installed PB8 loader migrates older configurations to its current schema. For schema 8.4, Trailing Martingale price EMA spans use `bot.<side>.strategy.trailing_martingale.entry.ema_span_0` and `ema_span_1`. Auto-unstuck has independent `bot.<side>.unstuck.ema_span_0` and `ema_span_1`. Their optimizer bounds appear under the corresponding paths; coin and scenario overrides follow PB8's native migration rules.

Enable `couple_unstuck_ema_spans` in the optimizer overrides to search strategy and unstuck EMA spans together. Independent search remains the default. Start a fresh optimization when changing this option or upgrading older GPU checkpoints with a different parameter layout.

`optimize.pymoo.shared.mutation_prob` controls mutation per individual; `mutation_prob_per_variable` controls mutation per variable. Both controls are available for CPU and GPU, support **auto** or an explicit probability from 0 to 1, and preserve zero. Auto uses `1 / n_params` for individuals and `min(0.5, 1 / n_params)` for variables. PB8 migrates the old `mutation_prob_var` value to `mutation_prob` without changing its meaning.

A Python/Rust schema mismatch can prevent metadata loading even when the PBGui editor supports these fields. Use **VPS Manager → Update PB8** on the affected host to rebuild and verify the matching PB8 runtime.


## Configs

- **New Config** loads optimizer defaults, strategies, bounds, scoring metrics, limits, backend options, and Pymoo choices from the installed PB8 runtime.
- All installed PB8 strategies are supported: `trailing_martingale`, `ema_anchor`, and `trailing_grid_v7`.
- Changing `strategy_kind` activates that strategy's runtime-provided bot defaults and bound set without deleting any customized inactive strategy block. Unsaved bounds and bot values are cached per strategy while switching in the editor. The current runtime exposes 84 controls for `trailing_martingale`, 58 for `ema_anchor`, and 86 for `trailing_grid_v7`.
- The visual editor reads and writes nested PB8 bot and bound paths. Raw JSON remains synchronized and preserves future or expert fields, including unknown `fixed_runtime_overrides` and canonical or shorthand `fixed_params` selectors.
- Frequently used optimizer controls remain in their existing PBv7 editor sections. PB8-only RNG seed, fine-tune selectors, polish percentage, and polish bounds mode are included without creating a separate editor.
- Saved configurations are validated by PB8 and stored as recoverable bundles under `data/opt_v8`.
- The Configs table shows the active PB8 strategy and supports sorting by Strategy.
- Official **Convert to V8** migration is available for PBv7 Optimize configurations. The complete config is passed to PB8 and opened as an unsaved editor preview; no config bundle is created or replaced until the user explicitly saves. The migration report travels with the preview and is persisted with that manual save. Review blocking is limited to `optimize`, `backtest`, and `bot` findings that can affect an Optimize evaluation; Run-only `live` findings do not block this context. PBGui metadata and the redundant legacy default `max_pending_starting_evals_per_cpu=1` are removed before migration. After PB8 migration, PBGui removes strategy-incompatible optimizer overrides, emits canonical fixed-runtime paths, freezes already disabled sides, and restores implicit positive-threshold V7 enforcers. These deterministic corrections are reported as `ok_with_adjustments`; conflicting or unresolved paths still block the preview. Weighted-only scoring, ADG/MDG floors, inserted V8 defaults, and fixed new cooldown bounds produce report warnings but are never rewritten as an optimizer recipe. Genuine failures show a bounded list of fields and behavior warnings instead of dumping the complete migration report.
- PBv7 Pareto candidates expose the same official migration action and are accepted only from managed PB7 result directories.

The PB8 editor exposes all installed HSL modes and optimizer overrides in separate Long and Short cards. **HSL enabled** controls whether hard-stop behavior participates in optimizer evaluations. **Restart after RED** is an explicit `always`, `threshold`, or `never` selection; `always` is PB8's optimize default so evaluations resume after cooldown instead of terminating on persistent drawdown. `polish_percentage` is displayed as a normal percentage but converted to PB8's fractional `--polish-pct` value, so `20` means `0.20`. Pymoo keeps PB8's native automatic sizing: NSGA-II uses `250`, while NSGA-III derives its reference directions from a budget of `500`.

PB8's experimental `gpu` backend supports **Apple MPS** and, in recent PB8 revisions, **NVIDIA CUDA on Linux/WSL2**. PBGui uses the installed PB8 device selector and shows the host runtime and NVIDIA device name. CUDA also requires working CuPy and NVRTC in the PB8 virtualenv. Native Windows Python is not supported; use WSL2 with the NVIDIA driver installed on Windows. A system-wide CUDA toolkit is not required. Older MPS-only PB8 revisions remain supported and report that a PB8 update is required for CUDA.

GPU remains selectable on unavailable hosts for editor preview and Save. Queue and Start fail before creating snapshots or processes when the GPU runtime is unavailable. Full installations and PB8 updates automatically select `gpu-mps` on Apple Silicon or `gpu-cuda` when a working NVIDIA driver is detected on Linux/WSL2 and the PB8 revision declares that extra. CPU hosts use `full` without optional GPU packages; VPS live-only installations stay live-only. Install the driver before running the PB8 update. Advanced Ansible runs can set `pb8_gpu_profile` to `auto` (default), `cpu`, `mps`, or `cuda`; explicit unsupported choices fail visibly.

PB8 selects the accelerator automatically; keep `optimize.backend: "gpu"`. `auto_lean_parallelism` tunes Apple M3 only. Start with modest batch sizes and exact worker counts, then measure throughput and VRAM usage. Exact Rust validation remains authoritative. Resume retains PB8's runtime/checkpoint compatibility checks; start a fresh run when changing incompatible runtime versions or accelerators. GPU metric choices include aliases accepted by the installed PB8 validator, including `long_short_profit_ratio` when it maps to `pnl_ratio_long_short`; exact-only metrics remain excluded.

When GPU is selected, the editor exposes PB8's runtime-provided nullable population, batch, and candidate-bar sizing, M3 lean auto-parallelism, exact-worker and drift controls, checkpoint interval, and Successive Halving policy. Controls are grouped as **Automatic sizing**, **Exact validation & checkpointing**, **Drift safety**, and **Successive halving**. They use the editor's standard responsive eight-column grid: 8×1 fields on wide screens, 4×2 on medium screens, and 2×4 on small screens. Blank sizing fields retain PB8's automatic defaults and display the effective runtime value as an `auto (…)` placeholder; typing a number intentionally disables automatic sizing for that field. **Reset GPU defaults** restores the installed runtime defaults without deleting unknown future GPU keys. New scoring and limit choices use PB8's GPU proxy allowlist; existing incompatible entries remain visible for repair and PB8's native preflight blocks them before queue or launch.

PB8's default optimize bounds are initial search ranges, not hard slider limits. The editor therefore uses parameter range metadata for the slider and allows values below PB8's defaults, such as `n_positions = 1`.

Forager volume and volatility EMA span sliders have a minimum of `1`. To exclude these parameters from optimization, keep a valid positive bot value and use the row's **Fixed** checkbox instead of setting the span to zero. Backend validation still accepts imported zero spans only when the corresponding Forager signals are guaranteed to remain disabled.

Selecting several exchanges keeps PB8's native combined-dataset behavior. Use explicit Suite scenarios when each exchange must be evaluated separately.

The two compact buttons beside PB8 Optimize's **start_date** resolve PB8's first available candles for the currently selected exchanges and explicit approved coins. **1st** uses the oldest known selected market history. **All** starts only after every selected coin has a known OHLCV timestamp on every selected exchange. While the lookup runs, a compact progress bar reports genuinely completed Exchange/Coin pairs and names the current PB8 operation. **Stop** cancels only this lookup. PBGui adds PB8's required strategy warmup and rounds up to the first fully usable UTC day before setting the date-only `backtest.start_date`. **All** fails with the first unresolved pair when a coin is missing from an exchange or its first timestamp is unknown. Dynamic `all` coin selection is not accepted, and one lookup is limited to 200 exchange/coin pairs. The explicit lookup may populate PB8's native first-timestamp cache but does not download the full OHLCV range. Closing or replacing the editor stops its active lookup automatically.

The **PB8 Scenario Generator** inside Suite Mode previews deterministic `rolling_windows`, `walk_forward`, and `sweep_cycles` plans from the editor's base date range. Window length, stride, training count, optional holdout count, and exchange expansion are validated server-side and capped at 64 generated scenarios. Preview does not modify the config. **Check & Apply windows** explicitly replaces the unsaved Suite scenarios and reducer and applies the same default scoring and limits recipe for all three templates; holdout windows remain outside `backtest.scenarios` and are stored as `pbgui.scenario_template` provenance. Legacy preset provenance is cleared by manual Suite edits. Explicit visual window plans retain their Holdouts when editing training scenarios or aggregation. Sweep Cycles additionally binds this immutable plan to the PB8 result and calculates sequential sweep/refill cash-flow metrics from each Pareto candidate's per-scenario gain. PBGui AI exposes the same generator as a read-only preview tool and must still use the existing proposal flow for Save or Queue operations.

### Scenario Generator

The Scenario Generator turns one PB8 Optimize config into a reproducible group of historical tests. PB8 still performs normal Suite optimization. PBGui is responsible for generating the date windows, preserving the experiment plan, evaluating Sweep cash flows after PB8 returns scenario metrics, and preparing the final Holdout backtests.

#### What Each Action Does

| Action | What changes | What does not change |
| --- | --- | --- |
| **1st / All** beside `start_date` | Resolves an OHLCV-based start date | Suite scenarios and generator settings |
| **Recalculate** | Re-reads current base settings and fits the maximum valid Training count for every template | Saved config and applied Suite |
| **Generate windows** | Shows exact Train/Holdout windows and warnings | Config, Suite, scoring, bounds, and queue |
| **Check & Apply windows** | Enables Suite Mode, installs Train scenarios/reducer, stores Holdout provenance, and applies the Sweep preset | No config is saved or queued yet |
| **Save / Save & Queue** | Persists or launches the applied experiment | Holdout remains excluded from optimization |
| **Paretos** | Shows PB8 metrics plus PBGui `sweep_*` cash-flow metrics | Original PB8 candidate metrics |
| **Holdout** in the Pareto sidebar | Builds standalone PB8 Backtest queue drafts from immutable Holdout dates | Candidate parameters, coins, exchange, balance, and overrides |

#### Settings At A Glance

| Setting | Meaning |
| --- | --- |
| **Template** | Rolling comparison, Walk-Forward validation, or sequential Sweep cash-flow evaluation |
| **Window days** | Trading days contained in each scenario |
| **Stride days** | Distance between consecutive window end dates; automatic for Sweep |
| **Training windows** | Scenarios PB8 evaluates; editable for Rolling/Walk-Forward, fitted by Recalculate, and automatic for Sweep |
| **Holdout windows** | Untouched periods reserved for final out-of-sample Backtests |
| **Exchange mode** | Inherit the combined base exchanges or expand separate exchange scenarios where supported |
| **Starting balance** | PB8 simulation capital and Sweep reset capital after Apply; defaults to the current base Starting balance |
| **Balance multiplier** | Sweep target: Starting balance multiplied by this value |
| **Refill cost** | Additional external cost booked when a loss window is refilled |
| **Cooldown days** | No-trading gap between Sweep windows; included automatically in Stride |

#### Recommended Sweep Workflow

1. Select explicit coins and exchanges.
2. Use **All** for a start date common to every selected Exchange/Coin pair, or **1st** when changing-universe history is intentional.
3. Select **Sweep Cycles**, set Window, Holdout, Starting balance, Multiplier, Refill cost, and Cooldown. PBGui calculates Stride and Training windows.
4. Click **Recalculate** after any OHLCV/date/exchange change, then **Generate windows**.
5. Click **Check & Apply windows**. PBGui synchronizes base balance, symmetric Suite coin lists, reducer, scoring, limits, and meaningful Long bounds.
6. Save and queue the Optimize run. `write_all_results=true` is mandatory so PBGui can bind the immutable Sweep plan to the correct result.
7. Rank completed candidates by `sweep_net_cashflow`, cycles completed, external capital/refills, Drawdown, and Sortino.
8. Select finalists and click **Holdout**. Queue the generated standalone Backtests without retuning them.

#### Important Boundaries

- PBGui does not modify Passivbot and does not move real funds.
- PB8 Gain is an end/start multiplier: `1.0` break-even, `2.0` doubles capital, `0.8` loses 20%.
- Sweep decisions happen at scenario-window boundaries, not at an unobserved intrawindow target crossing.
- Holdout data never influences optimization or Pareto generation.
- Manual Suite edits after Apply clear generator provenance because the saved Suite no longer matches the previewed experiment.

### Detailed Template Settings

1. Set the base **exchanges**, **start_date**, and **end_date** in Backtest Settings. The generator creates its windows backwards from the base end date and never creates a window before the base start date. An `end_date` of `now` is resolved to today's date for the preview.
2. Open **Suite Mode**. The generator is available in PB8 Optimize even while Suite Mode is disabled.
3. Choose a template:
   - **Rolling Windows** creates training windows only. Use it to compare performance across repeated historical periods.
   - **Walk-Forward** creates chronological training windows followed by separate holdout windows.
   - **Sweep Cycles** creates one sequential combined-exchange track and evaluates each candidate's window gains with carry, sweep-reset, and refill-reset rules. PBGui automatically calculates Stride and the maximum number of complete Training windows from the base date range after reserving Holdouts.
4. Set **Window days** to the length of each scenario. Rolling Windows and Walk-Forward accept a manual **Stride days** value. Sweep Cycles calculates Stride automatically as Window days plus Cooldown days.
5. Set **Training windows** manually for Rolling Windows or Walk-Forward, or use **Recalculate** to fit their maximum count from the current dates and configured Stride. Sweep Cycles always calculates the maximum complete Training count automatically after reserving **Holdout windows**. A range fitting no training window remains invalid instead of being forced to one. With **Exchange mode = Inherit base**, every window uses the combined base exchange selection.
6. Click **Generate windows**. Review and adjust the generated windows directly in the chart. Generating windows alone does not change the Suite or config.
7. Click **Check & Apply windows** when the plan is correct. This enables Suite Mode, replaces the current unsaved Suite scenarios, and preserves the configured reducer. Holdout rows are deliberately not copied into `backtest.scenarios`.
8. Review named Objective Scenario, scoring, and limit references after replacing an existing Suite. Their scenario labels must still exist in the newly generated training set.
9. Use the normal **Save** or Queue workflow only after reviewing the applied Suite. Saving persists the generator parameters and holdout rows under `pbgui.scenario_template` for traceability.

Run **Generate windows** again before Apply if the base dates or exchanges changed. PBGui blocks application of a stale preview. Editing, adding, removing, reordering, or replacing Suite scenarios after Apply clears the generator provenance because the saved Suite no longer exactly matches the generated plan.

After changing approved coins, base Starting balance, or `start_date` through **1st** or **All**, click **Recalculate** beside **Guide**. It reloads the current base settings, fits Rolling/Walk-Forward counts with their configured Stride, recalculates automatic Sweep Stride/counts, and discards stale Preview state. Preview still preserves a manually selected smaller Rolling/Walk-Forward count.

Example: for three non-overlapping quarterly training periods and one untouched quarter, choose **Walk-Forward**, `Window days = 90`, `Stride days = 90`, `Training windows = 3`, and `Holdout windows = 1`. For six overlapping three-month training periods sampled monthly, choose **Rolling Windows**, `Window days = 90`, `Stride days = 30`, and `Training windows = 6`.

**Sweep Cycles example:** evaluate repeated account-growth cycles from `1,000` to `2,000` USD. Select **Sweep Cycles**, set `Window days = 180`, `Cooldown days = 7`, and `Holdout windows = 1`. PBGui calculates `Stride days = 187` and the maximum complete Training count automatically from the base dates; incomplete leading days are reported instead of requiring manual arithmetic. Set **Starting balance** to `1000`, **Balance multiplier** to `2`, and **Refill cost** to `25`. Preview shows every complete 180-day training window separated by seven no-trading days plus the reserved untouched holdout window. For every Pareto candidate PBGui applies the windows chronologically. Positive gains below 2,000 USD carry into the next window. At or above 2,000 USD, everything above 1,000 USD becomes swept cash and working capital resets to 1,000 USD. Below 1,000 USD, PBGui books the missing amount plus 25 USD external refill cost and resets to 1,000 USD. Pareto columns then expose `sweep_net_cashflow`, `sweep_total_swept`, `sweep_external_capital`, `sweep_cycles_completed`, `sweep_refill_count`, `sweep_final_balance`, and `sweep_target_hit_rate`. The holdout remains pending until the selected candidate is run separately over that period. This is a deterministic window-boundary evaluation; it does not move real funds or claim target crossings inside a window.

PB8 Gain values are terminal multipliers, not additive returns: `1.0` is break-even, `2.0` doubles the opening balance, and `0.8` loses 20%. Sweep evaluation therefore calculates each window as `ending_balance = opening_balance × gain_strategy_eq`.

To run validation without manual editing, select one or more candidates in the Paretos table, choose **Holdout only**, **Full timerange only**, **Holdout + Full timerange**, or **Training + Holdout + Full timerange**, and click **Validate**. Full timerange is available for ordinary PB8 Pareto results without a Sweep plan. PBGui reads immutable holdout dates where available, creates one standalone PB8 Backtest item per candidate and holdout, and optionally adds one continuous Backtest over the candidate's original base `start_date` through `end_date`. The all-period mode additionally creates one standalone Backtest for every configured Suite training window, making Training, Holdout, and continuous Full results directly selectable in Backtest Compare. Combined mode without Holdout dates still queues the available Training and/or Full timerange jobs and reports the skipped Holdout. Every generated validation draft disables Suite Mode, preserves its own exact date range and configured exchange group, and carries a per-candidate validation group into Backtest Results. Completed members of that group stay together behind one expandable **Optimize validation** header. A multi-exchange optimizer scenario therefore remains one comparable Combined backtest per period instead of being split into artificial single-exchange jobs that may have no valid coin in an early window. The continuous run includes training data and is a path-dependence/compounding diagnostic, not a replacement for untouched out-of-sample Holdout validation.

Applying a Sweep Cycles preview also sets the main PB8 `backtest.starting_balance` to the generator's **Starting balance**. Save and Queue reject a later mismatch because PB8 must calculate gains at the same capital size used by the cash-flow model.

Apply also replaces the optimizer recipe with the Sweep preset: `gain_strategy_eq` max, `sortino_ratio_strategy_eq` max, and `drawdown_worst_strategy_eq` min, all inheriting Suite Aggregate. The Suite reducer uses `median` by default, `max` for worst Drawdown, and `min` for Backtest Completion Ratio so one incomplete scenario cannot be hidden by the others. Limits become Drawdown greater than `0.80` and Backtest Completion Ratio less than `0.99`. The 80% cap deliberately permits high-risk candidates for profit sweeping; Drawdown remains a minimizing Pareto objective so a lower-risk candidate is preferred when Gain is comparable.

For explicit Long coin selections, Apply also sets Long `n_positions` to `1..coin count`; one selected coin therefore becomes `1..1` and fixed. Long `total_wallet_exposure_limit` becomes the high-risk sweep range `6..10`, with the current Long bot value set to `6`. Remaining Long bounds are normalized by effect: real non-zero Trailing-Martingale, Filter, Risk, and Unstuck ranges stay active; zero-width and disabled-HSL ranges become fixed; one-coin Forager ranking weights become fixed because no ranking is possible. With several explicit Long coins those ranking weights remain active. Short bounds and their fixed state are unchanged.

PB8 Suite mode requires identical Long and Short approved-coin lists even when one side is disabled. Sweep Apply therefore mirrors the Long approved list to Short and removes those coins from Short ignored coins. This does not enable Short trading: Short remains disabled while its TWE is `0`. Fixed selectors written by this preset use the actual `long.*` optimize-bound keys, avoiding unmatched `bot.long.*` selectors.

PB8.1 scoring objectives can inherit the global **Objective Scenario**, explicitly use the suite aggregate, or select a named Suite scenario. Aggregate objectives support `mean`, `min`, `max`, `std`, and `median`. Limits can use the suite aggregate with an omitted Scenario, preserve an explicit `scenario: null`, or select a named Suite scenario; omitted and explicit null have the same runtime basis but remain structurally distinct. PBGui reads the canonical reduction field from the installed PB8 runtime: current PB8 uses `reducer`, while older compatible PB8 releases use `aggregate` for scoring and `stat` for limits. A named scenario cannot also use a reduction field. Scenario labels must exist in the active Suite. PBGui preserves these distinctions when synchronizing Visual Editor and Raw JSON.

PB8 market selection uses the official resolver across the complete exchange set. Unique markets remain short in the config; real multiplier or venue collisions use exact scoped identifiers while the editor keeps compact labels. Exact imported IDs remain unchanged in coin lists, Coin Sources, Suite scenarios, and Raw JSON.

Use **Apply Filters** after changing Market Cap, volume ratio, tags, CPT, or notice settings. The action filters every selected exchange, projects results through PB8's market resolver, and writes the combined result to both Long and Short approved/ignored lists. Saving without applying keeps the filter metadata but does not change explicit coin lists.

## Queue

Queue entries contain immutable PB8 configuration snapshots. Editing a saved configuration after queueing does not alter an existing queue item.

When the editor is opened explicitly from a queue row, **Save** is different: it saves the managed config and refreshes that same queue item's snapshot. Changes such as `optimize.n_cpus` are therefore present when the row is reopened or started.

The editor also keeps its navigation origin: **Home** or **Save** returns a queue-opened config to the Queue panel, while a config opened from Configs returns there.

- **Start** manually launches the selected item.
- **Stop** terminates only the verified PB8 optimizer process.
- **Requeue Fresh** starts a new optimizer run without reusing optimizer state.
- **Continue from Pareto** uses managed Pareto files as `--start` seeds.
- **Resume Checkpoint** resumes the exact managed optimizer state with `--resume`.

For an exact selected or running queue item, PBGui AI can invoke the page-advertised `show_log` action from any Optimize panel. Cross-page actions navigate to PB8 Optimize, wait for queue data, and then call the same existing log-panel function as the row action.

Checkpoint resume accepts only local PB8 results managed by PBGui. Arbitrary checkpoint files are rejected because Python pickle checkpoints must be treated as trusted executable data.

PBGui advertises exact resume only when the checkpoint and `all_results.bin` are readable, `write_all_results` was enabled, a config is recoverable, and PB8 confirms compatibility. Config and queue creation then happen as one transaction. Checkpoint-only result directories do not require a separate Pareto JSON config.

PB7 and PB8 share one automatic optimizer slot: autostart never launches both versions at the same time. Explicit manual starts may run in parallel. Each optimizer controls its own parallelism through `optimize.n_cpus`.

PB7 and PB8 use one shared Queue **Settings** configuration. Saving it on either Optimize page immediately controls both queues and both autostart workers. **Autostart CPU** may be edited and saved at any time; **Override config CPU** decides whether it replaces `optimize.n_cpus` for automatic starts, while manual starts keep the config value. **Use PBGui Market Data** applies the managed OHLCV source to a launch copy without changing the saved config or immutable queue snapshot.

Running PB8 optimizer jobs survive an API restart. On Linux, each optimizer runs in its own transient user-systemd unit outside the API service cgroup; PBGui records process ID, process creation time, PB8 version, and PB8 commit so stale or reused process IDs cannot be controlled accidentally.

Permanent preparation errors move only their queue row to an actionable error state, while update or runtime-lock contention stays queued for retry. Startup reconciles queue snapshots, launch directories, PID, ready, and state records without signalling unverified processes. The PB8 controller is shown in **Services Monitor** and survives unexpected worker-loop errors.

GPU log status reports the exact-validation budget separately from proxy work: the dashboard shows exact evaluations and percentage, generation, proxy evaluations, inflight exact jobs, dispatch chunks, and Successive Halving activity. Checkpoint resume compares GPU policy, Pymoo proposal settings, reducer and execution inputs, enabled sides, and approved/ignored coins before deferring final checkpoint-signature authority to PB8.

For a running CPU/Pymoo optimization, the dashboard reads its evaluation count from the durable `all_results.bin` file currently opened by the verified queue process. This remains current when PB8 rejects repeated candidates after evaluation and therefore emits no new Pareto-update counter. If all-results writing is disabled or the result file cannot be attributed safely, the dashboard falls back to the latest structured evaluation value in the optimizer log.

Strategy-specific optimizer overrides are removed when switching strategies and validated through the installed PB8 runtime before save, queue, and launch.

**OHLCV Readiness** and preload run through PB8's own virtualenv, planner, cache paths, and native `passivbot download` command. Explicit read-only sources outside the approved PB8 or PBGui market-data roots are rejected instead of falling back to PB7. GPU Suites require every scenario-specific exchange dataset instead of accepting the best exchange per coin; a scenario-only missing exchange disables the single-config preload action with an explanation.

## Results And Paretos

Results are read only from `<pb8dir>/optimize_results`. The Results table shows each run's configured PB8 strategy and can sort by that column. The Results and Paretos panels provide the shared PB7 workflow for result inspection, deletion, 3D plots, Pareto Dash, candidate JSON, metric summaries, and seed bundles.

Opening Results during a cold metadata scan shows an explicit loading state. A background refresh keeps the last confirmed rows visible, and changing panels while the request is running does not discard the completed response.

When Paretos is opened for a result, PBGui stores only that versioned result-directory ID in the tab's session state. Reloading on `#paretos` or returning through the sidebar first waits for the current Results list, revalidates the ID, and then reloads its candidates exactly once. Early navigation never deletes a still-valid selection. Missing or deleted results clear the saved selection; absolute result paths are not stored.

Switching Optimize result sets clears previous Pareto rows, metadata, and selections immediately before loading the new result. A late response from the earlier result cannot restore stale rows.

The Results list uses bounded cold-start metadata: it enumerates each Pareto directory once, uses directory timestamps instead of stat-ing every candidate, and decodes only the first MessagePack record when no Pareto config exists. Full `all_results.bin` validation remains mandatory for Resume/Continue actions but never blocks the visual Results list after an API restart.

PB8 result actions distinguish three different workflows:

- Opening a Pareto candidate as a PB8 Backtest draft performs a standalone backtest.
- Pareto candidates selected in different named Suite scenario views retain that scenario. The Backtest handoff queues each candidate only for its bound scenario exchanges instead of creating a candidate-by-exchange matrix.
- Starting a new PB8 Optimize draft uses one or more Pareto candidates as seeds.
- Resuming a checkpoint continues the existing backend state and result stream.

The shared Pareto Explorer uses version-specific roots and understands PB8 nested bounds, nested bot parameters, scoring goals, limits, suite metrics, and incremental `all_results.bin` records.

In PB8 Pareto Explorer, **Strategy Explorer** opens the selected candidate with its sparse overrides. To compare two candidates, pin the first with **Pin Explorer Baseline**, select a different candidate from the same result, and open Strategy Explorer. Missing referenced override files block pinning or opening instead of being silently ignored.

Suite summaries keep their configured objective and scenario names and support `mean`, `min`, `max`, `std`, and `median`. The **Columns** picker controls the sortable list metrics and remembers the PB8 selection. It advertises every numeric metric persisted in the Pareto JSON, but the list API transfers values only for defaults and currently selected columns. Newly selected metrics are fetched in one debounced batch and then retained in the bounded file-signature LRU cache, so statistics changes and repeated views do not reread unchanged candidates. The picker DOM is also reused while the metric catalog is unchanged. Defaults include canonical Gain, configured objectives, and canonical Drawdown; canonical values prefer the established PB8 aliases, for example `gain_usd` before `gain_strategy_eq` and `drawdown_worst_strategy_eq` before USD/fallback Drawdown. **All (slower)** explicitly opts into a very wide table and larger response; normal views remain compact. Changed, deleted, malformed, or actively rewritten candidates are handled independently.

Result actions are enabled only when their required artifacts exist. A verified optimizer blocks deletion only for the exact immediate result directory that it or one of its recursive children has open. Unrelated older results remain deletable. Continuation queue sources and Pareto Dash sessions remain exact deletion blockers, and uncertain active-process ownership is handled conservatively. Batch deletion preserves these conflict details and stages selected directories atomically. Pareto Dash runs through a credential-isolated, bounded PBGui proxy with idle cleanup and verified orphan recovery. Its PBGui window can be moved by its header and resized from every edge or corner, while the dashboard retains PB8's original native presentation.

Deletion accepts only complete, top-level run directories below the managed result root. Nested artifacts such as `run/pareto`, hidden staging directories, and parent/root selectors are rejected before any move or removal, including batch requests. Nested paths remain available for reading candidates and selecting seeds.

## Archives

PB8 Optimize configurations and PB8 Backtest results use the existing Archive workflow. Files are stored under their `config_version`, so PB7 and PB8 content cannot overwrite each other. Import, export, view, delete, restore, and handoff actions always use the parser belonging to the archived configuration version.

If an OHLCV start-date lookup does not confirm Stop within 10 seconds, Optimize releases the controls and reports a timeout. The backend may still be stopping; a late result is not applied.

## Vast.ai cloud execution

Choose **Execution → Vast.ai GPU** in the editor and use **Save & Queue**.
Save the GPU type and limits under **Queue → Settings → GPU requirements**. PBGui selects a current matching offer when starting the queue.
Configure your account under **Queue → Settings → Cloud setup** and confirm **Rent GPU &
start queue** there. Multiple cloud jobs share one rented worker and cached
market data. See [Vast.ai GPU queue](48_vast_gpu.md) for setup, limits and cleanup.

Pareto Explorer can be opened from imported Vast results as well as local results. Verified final Vast imports include all_results.bin; periodic snapshots contain the current Pareto files, so full evaluation history is available after final collection.

Queue Backtest and Queue Validation now add the selected candidates directly and keep the current chart, filters and selection. Open Queue is the only navigation action; its count includes pending and running jobs. The status shows whether existing autostart settings may launch queued jobs. Adding does not send a start command. Keep candidate dates, exchanges, balance and overrides as configured; change them in Backtests if needed. You can select and add further candidates while earlier batches are transferred in the background. Each click captures its configuration and validation mode; batches are submitted sequentially and the status shows waiting batches. Repeated submissions skip jobs already confirmed during this page session. Keep this page open until additions finish. While work is pending, Open Queue opens another tab so additions can continue. On a partial PB8 failure, retry continues with unconfirmed items using the same operation IDs.

Explicitly applying **Sweep Cycles** restores exactly three scoring objectives for both EMA and Trailing: gain (ADG for Vast GPU), Sortino, and worst drawdown. All supported metrics remain available for subsequent manual edits.

### Visual Scenario Editor

Open **Suite Mode → Visual windows**. The upper chart reads local OHLCV archives and shows daily candles or a price line; selecting its exchange/coin changes only the reference chart. Missing days are orange and partial days are counted. No exchange download is triggered. The chart automatically switches from a price line to candles when zoom provides enough space. Only configured optimizer coins are offered as reference sources. Empty Holdout lanes remain available as drop targets; lane positions stay stable during dragging. Optimization keeps its original data resolution.

- Drag a window body to move it or either edge to resize it. Dates snap to UTC days.
- Draw Training/Holdout windows on empty chart space, delete with the trash and undo/redo edits directly in the draft.
- Holdouts may lie between training periods. Training windows are automatically trimmed or split around Holdouts when the edit is completed. Training/Holdout overlap blocks Apply; overlapping training windows use separate lanes. Sweep also requires chronological, non-overlapping windows with the configured cooldown.
- **Check & Apply windows** validates dates and exports only Training to the Suite. It preserves scoring, limits and aggregation. Use **Save** or **Save & Queue** to persist the applied configuration. Up to 48 Training and 16 Holdout windows are supported.

Rolling Windows and Windows + Holdouts remain quick presets for creating equal windows. Preview regenerates their draft; the visual editor then allows individual changes. Existing presets remain compatible. A distributed Holdout is an excluded period, not necessarily a chronological forward test: true walk-forward would optimize separately using only data preceding each test period.

Local and Vast result imports retain the explicit Holdout dates. **Validate** in Results and Pareto Explorer uses those dates, including distributed Holdouts. Editing aggregation does not remove them. A stale plan that no longer matches the training config must be applied again before launching.

Visual window plans require `optimize.write_all_results=true` so local result metadata can be bound to its result stream.

New windows use the current **Window days** and **Stride days**. Training continues after the latest training start plus stride; the first Holdout follows the existing windows. If the full window does not fit, extend the range or draw it explicitly; PBGui does not shorten it automatically.

The reference defaults to a configured coin using local market mappings. If none matches, choose it explicitly. Manual reference choices survive editor refreshes. Missing history appears as a thin orange strip. **Full range** restores the entire configured period without changing window dates.

Daily chart summaries are cached in API memory for up to five minutes and reused across chart reloads. Changed source files invalidate the corresponding day immediately. The first load still reads the local minute archives; no exchange download is started.

Delete a selected window using the toolbar trash icon, or drag its bar onto the trash. Undo restores it. The markers inside the price chart show green joins, orange gaps and red overlaps across all windows; hover a marker for dates and day counts. Training overlaps can be intentional.

Thin green joins appear inside the price chart. A floating label follows a dragged window to the trash.

Use the mouse wheel over the chart to zoom around the pointer. Shift-drag the price area to pan; double-click empty chart space to restore the full period. Window bars still move and resize their dates.

Draw a new window directly by dragging empty chart space. The Holdout lane creates Holdouts; other empty areas create Training windows. A click alone creates nothing.

Browser refresh reopens the saved config currently open in Optimize, including configs opened from the queue. Unsaved changes to an existing saved config are not restored; new and copied drafts have separate temporary tab recovery. Home/closing the editor removes that editor address.

The four chart toolbar icons are Undo, Redo, Trash and Full range, with tooltips. Move a window onto the other Training/Holdout lane to change its role. New windows are drawn directly; there are no add-window or separate price-history buttons.

While dragging, only the window under the pointer is displayed; its original timeline copy is hidden until release. Refresh restores the editor without briefly displaying the Configs list.

The dragged window previews the destination role with a Training/Holdout label and matching color before dropping.

The magnet icon toggles snapping (initially on). Moving or resizing within eight screen pixels of another window boundary snaps to it; moving preserves duration. Bars show start/end dates and duration; hover for the complete text on narrow windows.

Check & Apply shows validation progress and any error beside the button. Successful application updates the scenario list below; it does not save the config to disk.

Holdouts automatically exclude their dates from Training when edited or dropped. Overlapping Training windows are trimmed, split or removed. Undo restores the complete previous edit, including affected Training. The backend still rejects any remaining Training/Holdout overlap.

Adjacent windows have a shared boundary grip when the pair is unambiguous. Drag it to change the left end and right start together, keeping outer dates fixed and both windows at least one day long. Undo restores both windows.

At a shared boundary, the left grip changes only the left window end, the middle grip resizes both, and the right grip changes only the right window start. Drag a side grip away to separate windows; the magnet still snaps within its normal distance and can be disabled.

Side grips use small marks along the bottom edge; their larger invisible mouse targets keep them easy to grab without covering dates.

Below the chart, only **Check & Apply windows** and validation feedback remain. Select and edit windows directly in the chart.

The chart is the scenario preview. **Generate windows** builds the graphical draft from template settings. **Check & Apply windows** validates it and directly replaces the Suite scenario list; no separate preview table or second Apply step is needed. Failed validation leaves existing scenarios unchanged. Save persists the configuration.

The compact reference line shows exchange, coin, days and Complete. Missing or incomplete days appear only when present. Hover for source resolution and coverage scope; this describes the reference chart only.

Vast uploads use resumable 2 MiB blocks and stable compressed archives. The progress bar counts checksum-verified blocks; in-flight bytes are separately acknowledged by the host. Reconnects retry only unverified blocks. Speed measures verified bytes during the current attempt. Uploads may exceed ten minutes while the receiver keeps progressing; 120 seconds without receiver progress triggers a retry, and the rental deadline still bounds the transfer.

Select a compatible offer in Optimizer Settings and click **Rent** to rent that exact GPU immediately. Billing and the rental deadline start immediately; queued jobs stay paused. An unavailable or more expensive offer requires a new selection, never an automatic substitute. The reservation stays available until the first job, explicit **End rental**, or the deadline. **Start queue** reuses the reserved GPU; after its first job, the normal idle cleanup setting applies. The Queue and Settings show the active rental and an **End rental** button. Ending a rental with an active job uses the existing stop-and-collect cleanup flow.

For Sweep Cycles, graphical **Check & Apply windows** restores the three scoring defaults for both EMA and Trailing: ADG (Vast) or Gain (local), Sortino, and worst Drawdown. Edited strategy settings and limits are retained. Apply also mirrors the Long approved coins to Short (without enabling Short trading) and copies the Sweep starting balance to the backtest.

Save and Save & Queue show pending status and notify validation or API failures without closing the editor. Fix the reported issue and retry; repeated clicks while saving do not create duplicate requests.

New and copied Optimize drafts are restored in the same browser tab after refresh. This is temporary browser recovery, not a saved config or queue entry. Closing the editor clears it. Configs containing credential fields are not stored for recovery.

Rent rechecks the selected offer directly by its contract ID; a general marketplace search may show a different representative offer. Rental failures appear beside Rent. No replacement GPU is rented automatically.

The GPU row and Details show Vast-reported TFLOPS. This compares compute capacity, not measured optimizer throughput; CPU validation and memory also affect run speed. Missing values show “TFLOPS unknown”.

On the legacy fallback for hosts without rsync, cache availability is checked in pages of 1,024 files, so large multi-coin datasets do not need a single oversized SSH response. Up to two 2 MiB blocks upload concurrently. Progress combines both connections; only checksum-verified blocks count as completed. A permanent transfer failure cancels and joins the other connection; verified blocks remain reusable. This works with the existing worker image.

Automatic CPU selection uses the smaller of measured container capacity and the rented offer allocation, rounded down to whole workers (minimum one). For example, a host reporting 256 CPUs with a 21.3-core rental uses 21 exact workers. Invalid allocation data blocks startup.

A manually reserved GPU has its provider startup log collected every 60 seconds, even before queue start. A preparing/queued cloud job can display this rental log until its own provider or optimizer log becomes available. The provider log describes container startup, not optimizer progress.

The queue rental bar also provides **Start queue**. After starting, a reserved GPU waits until an input bundle is ready; starting the queue does not bypass preparation.

If input preparation is interrupted by process shutdown, the job is marked failed when detected. Use **Requeue** to prepare it again; an existing rental can be reused.

The direct rsync upload distinguishes file preparation, synchronization and installation. Its progress counts logical file bytes processed by rsync, not measured network traffic. Legacy uploads without rsync retain cache checking, archive preparation and verified block transfer. Opening the log again shows the persisted phase and available startup/optimizer log; before a log exists, a phase-specific waiting message is shown.


Upload speed and host network bandwidth both use **Mbps**; payload sizes remain **MB** (1 byte = 8 bits). During transfer, PBGui shows measured speed, advertised host download speed, the percentage reached, and both remaining-time estimates simultaneously. The host-rate estimate is theoretical: local uplink, route, SSH overhead and retries also limit throughput, so a low reached percentage does not prove inaccurate host specifications. Packaging, reconnects and verification do not show transfer estimates.

The **− / +** buttons beside **Budget / deadline** request 30-minute changes for the active rental. They preserve the existing budget, allow at most 24 hours from rental acceptance, and require at least ten minutes remaining for collection and cleanup. The confirmed date stays visible while **Awaiting worker confirmation** is displayed; retries use the same request and cannot add another 30 minutes. Old worker images with immutable guards show disabled controls. New rentals created with PBGui v2.04.6 use the published queue-v2 worker with this protocol. Existing rentals retain their original worker and remain manageable, but require a new rental to gain deadline adjustment.

GPU logs may repeat `chunks=2/2 candidates=1024/1024`: in Suite mode a candidate batch is screened separately for each scenario. `eta=0` refers to that dispatch, not the whole optimization. Exact/Pareto results arrive after the selected candidates complete their CPU evaluations; the first suite pass can therefore show GPU activity before any exact results.

Multicoin GPU runtime need not scale linearly with coin count. Compare warm proxy profiles with the same coin set, scenarios and candidate count: `kernel_execution` isolates GPU computation from compilation and data transfer. A busy GPU alone does not establish normal host performance.

Only the legacy fallback performs cache checking. It shows acknowledged files out of the total and its own percentage before byte transfer starts. This counter is retained when reopening the log; it is separate from upload progress.

For queued bundles without public market snapshots, PBGui fetches fresh Binance/Bybit market metadata locally just before starting the remote optimizer. These describe instruments, not additional OHLCV candles. A failure to fetch or install this metadata prevents startup; PBGui does not substitute stale snapshots.

Vast input uploads synchronize individual files directly with rsync when it is installed on PBGui and the worker. There is no preceding cache query, dataset rehash or large upload archive. Immutable data files have content-hash names; rsync uses name and size to skip identical data across jobs even if local timestamps differ. Changed transfers use rsync’s built-in integrity checks and atomic publication; interrupted files remain separate and are reused on retry. Job configuration and manifest are sent separately. Completed data is linked into the job input without another full read or copy. The initial total includes reusable files, so remaining-time estimates can overstate the transfer. After synchronization the transfer-cost estimate uses rsync-reported sent bytes, not the size of reused data; provider billing remains authoritative. This works with the published queue-v3 image through a PBGui-supplied SSH helper; no worker image replacement is needed. Older workers without rsync retain verified chunk uploads. Existing PBGui hosts can install rsync through their package manager; new installers include it. This does not guarantee the advertised host rate, which can still exceed your local uplink or route capacity.

During image preparation, the animated bar indicates an unknown amount of work remaining, not measured byte progress. The elapsed time and completed layer counts are shown with the host log source (Vast Extra Debug Logs). Fetched records when PBGui retrieved that log snapshot, not when its last line was produced. Vast Instance Logs may report No such container until the image has been downloaded and the container created.

Requeue immediately shows Preparing and disables repeated submission while the local input bundle is rebuilt. It queues the replacement without renting a GPU.

Before optimizer launch, PBGui also fetches the authoritative first daily candle for every exported coin (including BTC) on each selected exchange. It sends the complete PB8 inception cache, including exchange-specific timestamps, resolved symbols and the resolver version. This prevents a region-blocked worker from trying to discover coin inception remotely. Minimum coin age is preserved; missing or incompatible metadata stops startup with an error. This also supports older queued bundles and needs no replacement worker image.
