# PBv8 Optimize

Queue state updates automatically when it changes. Background tabs defer queue rendering and results requests; returning to the tab updates the current view while preserving selections and unfinished edits. Active Results and Pareto views continue updating while a run is in progress, even if its queue status stays unchanged.

PB8 runtimes with offline simulation support expose **offline** under Market & Universe. It uses cached candles and metadata only; missing inputs fail instead of downloading. OHLCV Readiness respects this mode and disables remote preload. This setting does not put other PBGui services offline. Prepared datasets also require PB8's verified offline provenance.

Newer PB8 GPU runtimes expose **drift_rank_halt** (blank inherits `drift_halt`) and **drift_objective_tolerance** (default `0.000001`, zero is valid). The tolerance is an absolute allowance in fixed initial objective-scale units, not a profit percentage. Constraint agreement still uses `drift_halt`. A local PB8 update does not update the pinned Vast image: that image retains scalar drift checks, accepts the new default values for compatibility, and rejects custom values or offline mode before queueing.

Local PB8 config validation reuses the managed helper process after its initial startup. Draft recovery permits TOKEN coin override objects while still rejecting nested credential fields. After changing base dates or exchanges, Check & Apply validates the retained visual windows against the current form; it does not require reopening the editor.

The visual Scenario Editor offers supported reference exchanges independently of Optimize exchanges. Local Hyperliquid USDC and Bybit USDT perpetual candles can both supply the price chart for a matching base coin; choosing one does not change the optimizer markets or fetch missing candles.

**Vast.ai → Performance History** retains Vast throughput and workload metadata after queue deletion. Select matching workloads for speed comparisons; see the [Vast GPU guide](48_vast_gpu.md#performance-history).

For cloud CPU capacity, set **Min CPU cores** in Vast Settings; Cloud Auto uses the rented allocation. Manual Rent takes its specifications from the selected offer. Later queued jobs may trigger a confirmed transfer-reserve adjustment that shortens rental time within the same budget. Optimizer log retrieval errors no longer prevent stop or final collection.

When **Run on** is set to **Vast.ai**, the optimizer's **n_cpus** and **exact_workers** fields are greyed out. Their dotted help labels explain that **Min CPU cores** in Vast GPU settings controls the rental minimum; the actual run uses the effective container CPU allocation, capped by the rented CPU quota. Switching back to **Local** restores editing and the previous values.

**Save & Queue** reports cloud compatibility checking, config saving and job creation in the editor. The save buttons stay disabled throughout this operation, with a steady **Saving & queueing…** label until completion or an error. Failures open a centered **Cannot queue configuration** dialog with **OK**; cloud compatibility failures include the validation details. The dialog stays open until explicitly dismissed and preserves the editor draft. Once the server confirms the new job, PBGui opens Queue with **Preparing** while the full queue refresh continues in the background.

Vast input uploads recover from transient connection errors for up to 15 minutes without new transfer progress, with 15/30/60-second retry pauses and retained partial data. The log displays retry status. A healthy worker's upload is no longer cut short by its earlier setup timer; the rental deadline and collection reserve still apply.

Vast offer searches show **Previously used**, **Working** and **Preferred** host status. Use **Prefer host** in offer Details or the rental Host card to prioritize that physical machine within your existing limits. Manage preferences and manual Working marks in **Hosts**. Blocked hosts remain excluded.

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

PB8 selects the accelerator automatically; keep `optimize.backend: "gpu"`. For Vast.ai GPU jobs, leave **Population size**, **Batch size**, and **Max candidate bars** blank with `auto_lean_parallelism` enabled; PBGui selects values after renting and measuring the GPU. PBGui automatically checks for a saved measurement of the same workload and GPU variant, including advertised power limit, and shows a dispatch preview; **Apply measured suggestion** fills all three fields only when you choose it. A preview is an estimate, not a pre-run benchmark or a guarantee of one batch. Without a matching measurement, PBGui uses its conservative hardware profile; review throughput, power and VRAM after the run. Other Vast GPU controls, including Exact validations and drift safety, keep their existing behavior. Eligible Apple M3 workloads retain their separate tuning. Exact Rust validation remains authoritative. Resume retains PB8's runtime/checkpoint compatibility checks; start a fresh run when changing incompatible runtime versions or accelerators. GPU metric choices include aliases accepted by the installed PB8 validator, including `long_short_profit_ratio` when it maps to `pnl_ratio_long_short`; exact-only metrics remain excluded.

When GPU is selected, the editor exposes PB8's runtime-provided nullable population, batch, and candidate-bar sizing, M3 lean auto-parallelism, exact-worker and drift controls, checkpoint interval. **Staged history** is an optional search method, not GPU sizing: `0.25, 0.5, 1` evaluates candidates on 25%, then 50%, then 100% of history. Its three settings appear only when enabled for a local GPU job. Vast.ai does not support it, so the option is hidden there unless an older config needs it switched off. They use the editor's standard responsive eight-column grid: 8×1 fields on wide screens, 4×2 on medium screens, and 2×4 on small screens. For local GPU jobs, blank sizing fields retain PB8's automatic defaults and display the effective runtime value as an `auto (…)` placeholder; Vast GPU jobs show `auto after rent` for blank fields. On the three optional sizing fields, **−** from 1 returns to Auto and **+** from Auto starts at 1. **Reset GPU defaults** restores the installed runtime defaults without deleting unknown future GPU keys. New scoring and limit choices use PB8's GPU proxy allowlist; existing incompatible entries remain visible for repair and PB8's native preflight blocks them before queue or launch.

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
| **Training windows** | Editable for Rolling/Walk-Forward; automatically fitted for Sweep when generating windows |
| **Holdout windows** | Untouched periods reserved for final out-of-sample Backtests |
| **Exchange mode** | Inherit the combined base exchanges or expand separate exchange scenarios where supported |
| **Starting balance** | PB8 simulation capital and Sweep reset capital after Apply; always uses the current general Starting balance; no separate generator field |
| **Balance multiplier** | Sweep target: Starting balance multiplied by this value |
| **Refill cost** | Additional external cost booked when a loss window is refilled |
| **Cooldown days** | No-trading gap between Sweep windows; included automatically in Stride |

#### Recommended Sweep Workflow

1. Select explicit coins and exchanges.
2. Use **All** for a start date common to every selected Exchange/Coin pair, or **1st** when changing-universe history is intentional.
3. Select **Sweep Cycles**, set Window, Holdout, Multiplier, Refill cost, and Cooldown. PBGui calculates Stride and Training windows.
4. After changing dates or exchanges, click **Generate windows** to calculate and display the new windows.
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
5. Set the Training count for Rolling/Walk-Forward. Sweep automatically fits complete windows after reserving Holdouts.
6. Click **Generate windows**. Review and adjust the generated windows directly in the chart. Generating windows alone does not change the Suite or config.
7. Click **Check & Apply windows** when the plan is correct. This enables Suite Mode, replaces the current unsaved Suite scenarios, and preserves the configured reducer. Holdout rows are deliberately not copied into `backtest.scenarios`.
8. Review named Objective Scenario, scoring, and limit references after replacing an existing Suite. Their scenario labels must still exist in the newly generated training set.
9. Use the normal **Save** or Queue workflow only after reviewing the applied Suite. Saving persists the generator parameters and holdout rows under `pbgui.scenario_template` for traceability.

Run **Generate windows** again before Apply if the base dates or exchanges changed. PBGui blocks application of a stale preview. Editing, adding, removing, reordering, or replacing Suite scenarios after Apply clears the generator provenance because the saved Suite no longer exactly matches the generated plan.

**Generate windows** uses the current base dates and generator inputs and displays the result directly. **Check & Apply windows** validates and adopts this preview. There is no separate recalculation step.

Example: for three non-overlapping quarterly training periods and one untouched quarter, choose **Walk-Forward**, `Window days = 90`, `Stride days = 90`, `Training windows = 3`, and `Holdout windows = 1`. For six overlapping three-month training periods sampled monthly, choose **Rolling Windows**, `Window days = 90`, `Stride days = 30`, and `Training windows = 6`.

**Sweep Cycles example:** evaluate repeated account-growth cycles from `1,000` to `2,000` USD. Select **Sweep Cycles**, set `Window days = 180`, `Cooldown days = 7`, and `Holdout windows = 1`. PBGui calculates `Stride days = 187` and the maximum complete Training count automatically from the base dates; incomplete leading days are reported instead of requiring manual arithmetic. Set the general **Starting balance** to `1000`, **Balance multiplier** to `2`, and **Refill cost** to `25`. Preview shows every complete 180-day training window separated by seven no-trading days plus the reserved untouched holdout window. For every Pareto candidate PBGui applies the windows chronologically. Positive gains below 2,000 USD carry into the next window. At or above 2,000 USD, everything above 1,000 USD becomes swept cash and working capital resets to 1,000 USD. Below 1,000 USD, PBGui books the missing amount plus 25 USD external refill cost and resets to 1,000 USD. Pareto columns then expose `sweep_net_cashflow`, `sweep_total_swept`, `sweep_external_capital`, `sweep_cycles_completed`, `sweep_refill_count`, `sweep_final_balance`, and `sweep_target_hit_rate`. The holdout remains pending until the selected candidate is run separately over that period. This is a deterministic window-boundary evaluation; it does not move real funds or claim target crossings inside a window.

PB8 Gain values are terminal multipliers, not additive returns: `1.0` is break-even, `2.0` doubles the opening balance, and `0.8` loses 20%. Sweep evaluation therefore calculates each window as `ending_balance = opening_balance × gain_strategy_eq`.

To run validation without manual editing, select one or more candidates in the Paretos table, choose **Holdout only**, **Full timerange only**, **Holdout + Full timerange**, or **Training + Holdout + Full timerange**, and click **Validate**. Full timerange is available for ordinary PB8 Pareto results without a Sweep plan. PBGui reads immutable holdout dates where available, creates one standalone PB8 Backtest item per candidate and holdout, and optionally adds one continuous Backtest over the candidate's original base `start_date` through `end_date`. The all-period mode additionally creates one standalone Backtest for every configured Suite training window, making Training, Holdout, and continuous Full results directly selectable in Backtest Compare. Combined mode without Holdout dates still queues the available Training and/or Full timerange jobs and reports the skipped Holdout. Every generated validation draft disables Suite Mode, preserves its own exact date range and configured exchange group, and carries a per-candidate validation group into Backtest Results. Completed members of that group stay together behind one expandable **Optimize validation** header. A multi-exchange optimizer scenario therefore remains one comparable Combined backtest per period instead of being split into artificial single-exchange jobs that may have no valid coin in an early window. The continuous run includes training data and is a path-dependence/compounding diagnostic, not a replacement for untouched out-of-sample Holdout validation.

Generating or applying Sweep windows reads the current general `backtest.starting_balance`. After changing that value, apply the windows again before saving or queueing so PB8 and the cash-flow model use the same capital.

Apply also replaces the optimizer recipe with the Sweep preset: `gain_strategy_eq` max, `sortino_ratio_strategy_eq` max, and `drawdown_worst_strategy_eq` min, all inheriting Suite Aggregate. The Suite reducer uses `median` by default, `max` for worst Drawdown, and `min` for Backtest Completion Ratio so one incomplete scenario cannot be hidden by the others. Limits become Drawdown greater than `0.80` and Backtest Completion Ratio less than `0.99`. The 80% cap deliberately permits high-risk candidates for profit sweeping; Drawdown remains a minimizing Pareto objective so a lower-risk candidate is preferred when Gain is comparable.

For explicit Long coin selections, Apply also sets Long `n_positions` to `1..coin count`; one selected coin therefore becomes `1..1` and fixed. Long `total_wallet_exposure_limit` becomes the high-risk sweep range `6..10`, with the current Long bot value set to `6`. Remaining Long bounds are normalized by effect: real non-zero Trailing-Martingale, Filter, Risk, and Unstuck ranges stay active; zero-width and disabled-HSL ranges become fixed; one-coin Forager ranking weights become fixed because no ranking is possible. With several explicit Long coins those ranking weights remain active. Short bounds and their fixed state are unchanged.

PB8 Suite mode requires identical Long and Short approved-coin lists even when one side is disabled. Sweep Apply therefore mirrors the Long approved list to Short and removes those coins from Short ignored coins. This does not enable Short trading: Short remains disabled while its TWE is `0`. Fixed selectors written by this preset use the actual `long.*` optimize-bound keys, avoiding unmatched `bot.long.*` selectors.

PB8.1 scoring objectives can inherit the global **Objective Scenario**, explicitly use the suite aggregate, or select a named Suite scenario. Aggregate objectives support `mean`, `min`, `max`, `std`, and `median`. Limits can use the suite aggregate with an omitted Scenario, preserve an explicit `scenario: null`, or select a named Suite scenario; omitted and explicit null have the same runtime basis but remain structurally distinct. PBGui reads the canonical reduction field from the installed PB8 runtime: current PB8 uses `reducer`, while older compatible PB8 releases use `aggregate` for scoring and `stat` for limits. A named scenario cannot also use a reduction field. Scenario labels must exist in the active Suite. PBGui preserves these distinctions when synchronizing Visual Editor and Raw JSON. The Limits table's **Stat** column shows the saved reduction choice (for example, `max`) after leaving inline editing.

PB8 market selection uses the official resolver across the complete exchange set. Unique markets remain short in the config; real multiplier or venue collisions use exact scoped identifiers while the editor keeps compact labels. Exact imported IDs remain unchanged in coin lists, Coin Sources, Suite scenarios, and Raw JSON.

Use **Apply Filters** after changing Market Cap, volume ratio, tags, CPT, or notice settings. The action filters every selected exchange, projects results through PB8's market resolver, and writes the combined result to both Long and Short approved/ignored lists. Saving without applying keeps the filter metadata but does not change explicit coin lists.

## Queue

The log window is nonmodal: page controls and other queue logs remain accessible. Results and Pareto Explorer navigate without blocking the destination. Returning through the browser history revalidates the selected queue log before resuming updates; a newer selection takes precedence.

**Open log** opens a movable, resizable window with run/queue actions above the status cards. Four initially collapsed groups show **Rental & Hardware**, **Run Details & Objectives**, **Utilization & Throughput**, and **Convergence & Stagnation** in a 2×2 grid when the window is at least 640 px wide; narrower windows use one column. Unavailable groups are hidden. Opening groups automatically grows the detail area and moves the log down; collapsing them gives the space back to the log. There is no draggable separator. Groups show their full content; when space is limited, the detail area scrolls together while keeping the log usable. Rental inputs retain unsaved edits during updates. Errors remain visible outside the groups; throughput summaries distinguish sample age and historical intervals. This tab restores the available queue log, window geometry, group states and automatic layout after browser reload. Logs update automatically. **Reset to default** restores the centered default window size and collapses all detail groups while preserving the selected log and unsaved rental inputs.

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

Vast.ai direction checks run automatically in the editor, before export, and on frozen queue/calibration inputs before rental or a current-worker assignment. Blocking errors name the scenario/field; checks awaiting the prepared coin count show no provisional warning and do not block queueing. The early check keeps the base approved coins; scenario coin selection belongs to the later dataset context. Supported older leases retain their previous behavior. See [Vast.ai GPU queue](48_vast_gpu.md#direction-checks-before-renting) for the scope and correction options.

Under **Execution & optimizer backend**, choose **Run on → Vast.ai GPU**, leave the three GPU sizing fields blank for Auto or set manual overrides directly below, then use **Save & Queue**. Cloud validation appears beside those fields.
Save the GPU type and limits under **Queue → Settings → GPU requirements**. PBGui selects a current matching offer when starting the queue.
Configure your account under **Queue → Settings → Cloud setup** and confirm **Rent GPU &
start queue** there. Multiple cloud jobs share one rented worker and cached
market data. See [Vast.ai GPU queue](48_vast_gpu.md) for setup, limits and cleanup.

Pareto Explorer can be opened from imported Vast results as well as local results. Verified final Vast imports include all_results.bin; periodic snapshots contain the current Pareto files, so full evaluation history is available after final collection.

Queue Backtest and Queue Validation send the selected PB8 jobs in one durable backend request and leave the current chart, filters, and selection in place. The server adds jobs after page navigation or browser refresh; returning to Optimize restores the batch progress automatically. Open Queue shows the jobs already added. Autostart may launch those jobs while the batch is still being added. Each request captures the selected configs, exact validation periods, and overrides. Repeating the same batch resumes unconfirmed jobs through stable operation IDs; a failed batch reports the stopped job. Adding jobs does not itself send a start command.

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

Select a compatible offer in Optimizer Settings and click **Rent** to rent that exact GPU immediately. Billing and the rental deadline start immediately. The selected GPU stays reserved until **Start queue** is clicked on its rental card; jobs already running on other GPUs continue. An unavailable or more expensive offer requires a new selection, never an automatic substitute. The reservation stays available until the first job, explicit **End rental**, or the deadline. **Start queue** reuses the reserved GPU; after its first job, the normal idle cleanup setting applies. Queue cards show each active rental and its own **Start queue** or **End rental** button. Ending a rental with an active job uses the existing stop-and-collect cleanup flow.

For Sweep Cycles, graphical **Check & Apply windows** restores the three scoring defaults for both EMA and Trailing: ADG (Vast) or Gain (local), Sortino, and worst Drawdown. Edited strategy settings and limits are retained. Apply also mirrors the Long approved coins to Short (without enabling Short trading) and copies the Sweep starting balance to the backtest.

Save and Save & Queue show pending status and notify validation or API failures without closing the editor. Fix the reported issue and retry; repeated clicks while saving do not create duplicate requests.

New and copied Optimize drafts are restored in the same browser tab after refresh. This is temporary browser recovery, not a saved config or queue entry. Closing the editor clears it. Configs containing credential fields are not stored for recovery.

Rent rechecks the selected offer directly by its contract ID; a general marketplace search may show a different representative offer. Rental failures appear beside Rent. No replacement GPU is rented automatically.

The GPU row and Details show Vast-reported TFLOPS. This compares compute capacity, not measured optimizer throughput; CPU validation and memory also affect run speed. Missing values show “TFLOPS unknown”.

On the legacy fallback for hosts without rsync, cache availability is checked in pages of 1,024 files, so large multi-coin datasets do not need a single oversized SSH response. Up to two 2 MiB blocks upload concurrently. Progress combines both connections; only checksum-verified blocks count as completed. A permanent transfer failure cancels and joins the other connection; verified blocks remain reusable. This works with the existing worker image.

Automatic CPU selection uses the smaller of measured container capacity and the rented offer allocation, rounded down to whole workers (minimum one). For example, a host reporting 256 CPUs with a 21.3-core rental uses 21 exact workers. Invalid allocation data blocks startup.

A manually reserved GPU has its provider startup log collected every 60 seconds, even before queue start. A preparing/queued cloud job can display this rental log until its own provider or optimizer log becomes available. The provider log describes container startup, not optimizer progress.

The queue rental bar also provides **Start queue**. After starting, a reserved GPU waits until an input bundle is ready; starting the queue does not bypass preparation.

A prepared, inactive PB8 GPU Queue item offers **Tune GPU with this frozen input**. This is an optional, separately confirmed paid test of the exact prepared data, not part of a normal optimizer start. It creates an isolated copy; the original queue item is unchanged. Choose a GPU offer first. The action requires the separately published protocol-4 worker. See the [GPU performance-test guide](48_vast_gpu.md#optimizer-settings).

If input preparation is interrupted by process shutdown, the job is marked failed when detected. Use **Requeue** to prepare it again; an existing rental can be reused.

Direct rsync distinguishes file preparation, cache comparison and installation. While it runs, progress shows logical bytes compared and does not label them as network traffic. Its final status separates data reused from the GPU cache from bytes actually sent. Legacy uploads without rsync retain cache checking, archive preparation and verified block transfer. Opening the log again shows the persisted phase and available startup/optimizer log; before a log exists, a phase-specific waiting message is shown.


Upload speed and host network bandwidth both use **Mbps**; payload sizes remain **MB** (1 byte = 8 bits). During transfer, PBGui shows measured speed, advertised host download speed, the percentage reached, and both remaining-time estimates simultaneously. The host-rate estimate is theoretical: local uplink, route, SSH overhead and retries also limit throughput, so a low reached percentage does not prove inaccurate host specifications. Packaging, reconnects and verification do not show transfer estimates.

The **− / +** buttons beside **Budget / deadline** request 30-minute changes for the active rental. They preserve the existing budget, allow at most 24 hours from rental acceptance, and require at least ten minutes remaining for collection and cleanup. The confirmed date stays visible while **Awaiting worker confirmation** is displayed; retries use the same request and cannot add another 30 minutes. Old worker images with immutable guards show disabled controls. New rentals created with PBGui v2.04.6 use the published queue-v2 worker with this protocol. Existing rentals retain their original worker and remain manageable, but require a new rental to gain deadline adjustment.

GPU logs may repeat `chunks=2/2 candidates=1024/1024`: in Suite mode a candidate batch is screened separately for each scenario. `eta=0` refers to that dispatch, not the whole optimization. Exact/Pareto results arrive after the selected candidates complete their CPU evaluations; the first suite pass can therefore show GPU activity before any exact results.

Long CPU exact populations and starting-config evaluations emit a progress heartbeat after every five-minute quiet interval. It reports completed, pending and submitted work, elapsed time, throughput and an ETA when at least one evaluation has completed; before that, ETA remains unknown instead of inventing a duration.

Multicoin GPU runtime need not scale linearly with coin count. Compare warm proxy profiles with the same coin set, scenarios and candidate count: `kernel_execution` isolates GPU computation from compilation and data transfer. A busy GPU alone does not establish normal host performance.

Only the legacy fallback performs cache checking. It shows acknowledged files out of the total and its own percentage before byte transfer starts. This counter is retained when reopening the log; it is separate from upload progress.

For queued bundles without public market snapshots, PBGui fetches fresh Binance/Bybit market metadata locally just before starting the remote optimizer. These describe instruments, not additional OHLCV candles. A failure to fetch or install this metadata prevents startup; PBGui does not substitute stale snapshots.

Vast input uploads synchronize individual files directly with rsync when it is installed on PBGui and the worker. There is no preceding cache query, dataset rehash or large upload archive. Immutable data files have content-hash names; rsync uses name and size to skip identical data across jobs even if local timestamps differ. Changed transfers use rsync’s built-in integrity checks and atomic publication; interrupted files remain separate and are reused on retry. Job configuration and manifest are sent separately. Completed data is linked into the job input without another full read or copy. The initial total includes reusable files, so remaining-time estimates can overstate the transfer. After synchronization the transfer-cost estimate uses rsync-reported sent bytes, not the size of reused data; provider billing remains authoritative. This works with the published queue-v3 image through a PBGui-supplied SSH helper; no worker image replacement is needed. Older workers without rsync retain verified chunk uploads. Existing PBGui hosts can install rsync through their package manager; new installers include it. This does not guarantee the advertised host rate, which can still exceed your local uplink or route capacity.

During image preparation, the animated bar indicates an unknown amount of work remaining, not measured byte progress. The elapsed time and completed layer counts are shown with the host log source (Vast Extra Debug Logs). Fetched records when PBGui retrieved that log snapshot, not when its last line was produced. Vast Instance Logs may report No such container until the image has been downloaded and the container created.

Requeue immediately shows Preparing and disables repeated submission while the local input bundle is rebuilt. It queues the replacement without renting a GPU.

Before optimizer launch, PBGui also fetches the authoritative first daily candle for every exported coin (including BTC) on each selected exchange. It sends the complete PB8 inception cache, including exchange-specific timestamps, resolved symbols and the resolver version. This prevents a region-blocked worker from trying to discover coin inception remotely. Minimum coin age is preserved; missing or incompatible metadata stops startup with an error. This also supports older queued bundles and needs no replacement worker image.

## Optimizer Settings

**Queue Settings** controls local autostart, CPU overrides and PBGui market data. Vast.ai has five independent sidebar areas: GPU & Offers, Rental & Automation, Hosts, Performance History and Account. See the [Vast.ai guide](48_vast_gpu.md#optimizer-settings).

The queue column **Est. coin candles / candidate** estimates the full-candidate data volume from the frozen config before a run starts. It counts each selected coin on each selected exchange within every active training scenario, and excludes warm-up and data availability. See [Vast.ai workload comparison](48_vast_gpu.md#config-workload-comparison) for the calculation and Performance History comparisons.

Date fields in the optimizer overwrite existing digits when typing within a complete YYYY-MM-DD value; selected text and pasted dates can still be replaced normally. After applying graphical windows, automatically generated scenario names reflect the current training/holdout role and dates. Only training windows appear in optimizer scenarios; both distributed holdouts remain in the validation plan.

Parameter help appears when hovering over the dotted field title. Clicking, focusing, or editing a field does not open parameter help.

After changing visual windows, use **Check & Apply windows** before Save or Save & Queue. PBGui rejects unapplied window dates/roles and applied windows outside the current base dates. Queue workload estimates describe the applied training scenarios, not the generator draft; holdouts are excluded.

Green lines also mark the start of the first window and the end of the last window.

For Sweep Cycles, **Generate windows** also copies the current general **Starting balance** into the generator and generated preview.

The pinned Vast GPU worker supports HSL for ema_anchor and trailing_martingale. HSL settings are preserved during cloud export; native GPU configuration checks still apply.

Set **Max concurrent GPUs** and enable **Auto rent & start** under **Rental & Automation** to run prepared queued jobs concurrently on separate Vast GPUs (default 1). After the explicit save confirmation, **Save & Queue** needs no per-job Start action. Budgets and deadlines apply per rental. See the [GPU pool guide](48_vast_gpu.md#gpu-pool-and-shared-rentals) for pause, cleanup and replacement behavior.

Optimize bounds now keep position counts positive and avoid the invalid long/short auto-unstuck endpoints. Forager EMA spans remain at least 1, and an enabled HSL red threshold stays strictly below 1, including when PB8 supplies broader slider metadata. The Results list reuses unchanged summaries and updates them when result artifacts change. In the Scenario Visual Editor, Check & Apply keeps the current window editor and its Undo history open after a successful apply.

In the visual scenario editor, zoom stays within the configured dates. A successful retry clears earlier errors, and cancelling scenario replacement leaves the open editor unchanged.

## AI Loop Optimizer (PB8 only)

Setup uses the full-width optimizer editor grid: Starting configuration, Coins and positions, Result goals, then Run limits. Each goal is paired with its optional target.

Open **PBv8 → Optimize → AI Loops Config**. Use **New Config** for a named loop configuration. Select a starting PB8 config and Local CPU, compatible Local GPU, or Vast.ai. **Save** stores the definition; **Save & Queue** stores it and creates a waiting entry in **AI Loops Queue**. Use **Queue Selected** for saved configurations, then **Start Selected** to start waiting entries. The loop takes provider, ChatGPT profile, model, reasoning and speed from the existing PBGui AI assistant when you start it. Change these in the AI assistant; the loop has no separate model settings. The selection stays fixed for that loop.

Choose coins, Direction and total position capacity. Inherited fields show the selected config values: approved/ignored coins by side, active direction, effective position ranges (including fixed runtime overrides), and the native iteration budget. Blank inputs continue to inherit instead of pinning the displayed preview; switching configs updates previews and preserves typed values. Direction defaults to **Use config**, which preserves the active sides of the starting configuration; Long/Short/both are explicit alternatives; for both directions you can also assign separate Long/Short capacities. Capacity is a configured upper bound, not evidence of positions actually opened. Combine Gain, Low drawdown, Stable uptrend, Consistent time windows, Enough trades and Robust across coins with your own goals and priorities. Optional numeric targets use the metric units shown in the pinned goal interpretation. Unknown or qualitative goals cannot produce confirmed goal achievement.

**Allow AI to change Scenario Editor** controls the entire effective scenario setup, including inherited dates, exchanges, balance and editor metadata. Disabled means it remains fixed. Enabled permits training changes while the original reference comparison and reserved final holdout remain fixed.

**Limit per optimizer run** controls each individual optimizer job: **Use config** keeps `optimize.iters` editable by the AI; **Iterations (iters)** pins that native budget for every variant; **GPU proxy evaluations** stops a Local GPU/Vast job at its observed proxy counter; **Hours** limits runtime from optimizer launch, excluding waiting/setup. Native `iters` and GPU proxy counts are different units. Proxy/time modes use the maximum supported cloud iteration budget (10 million) as an additional native ceiling. Earlier natural completion is allowed. Proxy counters use each native generation profile, including screening evaluations and reused seeds, rather than waiting for the summary every ten generations. PB8 has no native hard proxy budget: counters are sampled and in-flight GPU work can overshoot; time stops and result collection take a short additional interval. Saved partial Pareto results are evaluated through the same exact backtests; missing usable results are recorded instead of claimed as progress.

**Total loop hours** limits the whole workflow, including execution waits after Start, setup, AI evaluation and validation. Time spent waiting before **Start Selected** does not consume that allowance. Set optimizer attempt, validation and parallelism limits. There are no separate AI call, token or USD budget settings. Model context/output limits still apply; run/time limits, repeated-error termination and interrupted-request reconciliation prevent endless retries. Paid API usage is recorded when pricing and usage are available. Optional JEV uses the existing OpenRouter credential and AI JEV cost setting per consultation; its cumulative allowance is derived from the optimizer attempt count. Missing JEV does not prevent the primary AI from continuing.

**Save & Queue / Queue Selected** authorizes this loop to change its complete optimizer configuration, run associated optimizer/backtest jobs and call the selected model automatically. Subsequent cycles never ask for approval. The AI can change the search space, algorithm, scoring and run scope; no optimizer parameter allowlist is imposed. User goals, inherited AI selection, execution target, Vast limits and disabled Scenario Editor settings remain fixed.

Total loop hours defaults to the saved **Rental & Automation** duration. For Vast.ai, the loop duration cannot exceed that rental duration; shorter loops are allowed. The API checks the current saved limit again at start. Existing loop workers are reused across cycles.

While a loop is running, its GPU remains reserved during local comparison backtests and AI evaluation, so the normal **When idle** timer does not delete it between rounds. The next optimizer job reuses that worker. Pause or completion restores the saved idle policy; Stop requests cleanup. Rental deadlines, budget limits and explicit rental cleanup still apply. Local backtest scheduling uses native backtest slots, independently of the CPU-worker count recorded by a cloud optimizer.

Vast.ai uses the existing saved GPU preferences, per-rental budget/deadline and maximum concurrent rentals. For example, RTX 3060, $5 per rental and three slots allow up to three separate one-GPU experiments, with $15 reserved across those rentals. The budget is per rental, not a lifetime cap across later rentals. Starting rentals, other workloads and leases awaiting confirmed deletion consume shared slots. A loop worker claims only its own loop jobs. Local execution waits for free resources; local GPU scheduling is conservative and uses one compatible job at a time. Exact comparison/holdout validations use the existing local PB8 backtest queue.

Progress updates automatically without changing your selected loop or unsaved setup fields. **Pause** prevents new jobs/rentals; running jobs can finish. **Resume** continues the durable state. **Stop** stops owned work and requests collection/cleanup of owned cloud rentals. Detached PB8 jobs survive API restarts; the controller reconciles them without duplicate launches. Managed API restarts are temporarily blocked while an AI Loop request is in flight, with a visible reason. Once its response is recorded, restarting is allowed; no new AI request starts during the reserved handoff. Optimizer/backtest phases and retry cooldowns do not block restarts. An external forced shutdown can still interrupt a request; its reservation is retained and the run ends with an explicit reconciliation error. A changed PB8 checkout or dataset ends the comparison before another launch.

GPT Loop requests use an inactivity timeout, not a fixed 180-second total response limit: meaningful reasoning/answer progress from the selected turn resets it. Normal reasoning allows 180 seconds without progress, and high/xhigh/ultra allows 300 seconds. Heartbeats, empty chunks and unrelated turns do not reset it. The original total Loop deadline remains fixed. Request diagnostics record runtime/thread/turn setup, last activity, progress counts and outcome without saving streamed reasoning content. Setup, idle and total-Loop deadline timeouts are reported separately.

Before allocating a GPU optimizer job, Loop validation checks the coordinated drift-evidence settings. For example, increasing `validate_per_generation` to 32 requires `drift_window` of at least 256; fewer broad probes can require a larger window. An invalid AI proposal returns its specific constraint to the model for bounded correction before launch. These static checks also work on a master without local CUDA.

Proxy limits read completed evolution counters from current PB8 progress logs as well as older logs; JSON profiling is optional. Seeds and pending work do not consume that allowance. Dataset protection compares file contents, so rewriting identical candle files with new timestamps does not stop a run. Changed contents or selected files still stop comparison before another launch. Older metadata fingerprints upgrade only after their original signature still matches.

Temporary AI connection failures, request timeouts and provider overload/rate limits retry automatically, with at most three attempts per decision and pauses of 30 then 60 seconds. A longer provider **Retry-After** is honored. Completed optimizer/backtest results remain in the same cycle; only the AI request repeats, using the same selected model and the original loop deadline. Each sent attempt retains its usage reservation because a timeout can still consume provider tokens. Cooldowns survive API restarts, and Pause/Stop prevents another attempt. Authentication, exhausted account quotas, billing limits and invalid model selections require correction and are not retried. Exhausted retries show a clear failure reason in the run log/results.

For a failed run, **Open → Overview / Analysis → Termination details** shows the actual failure reason, failure timestamp, step/round, error type, selected provider/model/reasoning and request attempt count. The affected round also shows these details. **Failed request attempts** lists each transient failure with its timestamp, timeout/error and following retry pause. This evidence is saved with the run and included in the PBGui error log; later cleanup does not change the recorded failure time. An earlier optimizer stopping limit cannot replace the actual fatal reason. Older runs retain their existing messages and show **Not recorded** for missing evidence rather than inferred times or attempts.

Received but unusable AI replies (including truncated output or tool requests) record their usage and enter automatic correction; they are not treated as unanswered requests. No returned tools are executed. Three consecutive errors end the loop. A valid goal interpretation clears its previous error, and a failed initial evaluation shows **failed**. Decision output uses the selected model's advertised output allowance, limited by available context, instead of a separate fixed 16,384-token cap. Output-limit and tool-request errors are reported separately.

Loop JEV comparisons send the fixed goals, interpreted rubric and candidate metrics referenced by that rubric. Unrelated optimizer metrics and duplicate metric descriptions are omitted to keep the request within JEV's context allowance. Candidate identities and goal values are retained; original result metrics remain stored unchanged. The existing JEV price/cost check still runs before reservation and dispatch. A failed consultation retains the specific safe validation, cost or provider error in its analysis instead of only reporting that JEV is unavailable.

The AI selects bounded Pareto candidates for exact PB8 backtests on the original comparison conditions. Proxy metrics alone cannot confirm an improvement. The best exact result is retained when later experiments perform worse. With user evaluations enabled, completion checks the isolated Holdout of the unchanged AI-selected exact candidate, after selection is frozen. It does not pick a replacement from Holdout scores or make another AI decision. Without user evaluations, an unused Scenario Editor holdout is reserved across your loops and tested once at regular completion. Missing, failed, incomplete or qualitative evidence remains **unconfirmed**. No adaptive retry follows a final holdout result.

The detail view shows goals, cycle decisions, JEV reasoning, job status, estimated budgets and evidence. Download the best validated config/override bundle when available. Evidence and corrected findings are stored privately per account in `data/loop_optimizer/<owner>/knowledge.json`, with full archived evidence alongside it. Future compatible loops receive bounded relevant findings and provenance; this is persistent learning from recorded experiments, not model retraining. PB7 has no Loop tab.

Open **AI Loops Knowledge** in the left sidebar for central findings across runs. Up to 200 recent indexed entries appear newest first, with finding text, time and evidence status. Search by finding, hypothesis, coin, run or status. Expand **Evidence and source** for the source run, PB8 revision, comparison period, uncertainty and exact-backtest scores; **Open run** navigates to its details when the run still exists. **Raw evidence** retains the stored record. Hypotheses, historical guidance, unconfirmed and superseded findings stay labeled. The view updates automatically, retains its search and expanded evidence on reload, and keeps existing findings visible if an update fails. Round-specific findings remain under **AI analysis → Findings**. Future decisions still receive at most 12 entries matching the PB8 revision and configured coin list; this view does not alter selection or learning.

Queue and Results identify each main run by name, short ID, creation time and status. Expand it for **Initial evaluation** and sequential **Loop 1, Loop 2, …** rounds. Multiple optimizer variants appear only for several experiments within one round, subject to parallel/resource limits. Comparison backtests appear separately. JEV reserve records consultations reserved so far, not the available allowance.

**Starting bot for each round:** PBGui carries forward the best bot parameters from the fixed exact comparison backtests, including the matching coin override bundle. The original bot remains the start until a candidate strictly beats its measured baseline; ties or worse rounds retain the incumbent. This survives controller/API restarts. The AI still proposes optimizer settings, bounds, scoring and limits independently: winning bot parameters do not establish that their optimizer settings are best. Bounds are not tightened automatically and the selected per-run budget remains fixed. Native bounds/fixed overrides still constrain the seed. A permitted strategy switch uses its own compatible template. Holdout and Full Time Range results never select this starting bot.

When the starting config has usable saved optimizer results, the loop first evaluates candidates from up to three recent matching result runs and makes targeted config changes before its first new optimizer run. This initial evaluation is shown in **Initial evaluation** and can consult JEV when useful. Historical metrics guide the first experiment; they do not confirm improvement or consume optimizer attempts. Current goals, Scenario Editor protection and exact validation still apply. With no usable saved results, the loop runs the starting config normally.

**AI Loops Config** lists saved definitions with the usual **New Config**, **Edit Selected**, **Duplicate**, **Delete Selected** and **Queue Selected** actions. Saving the same name updates that definition; changing the name creates another configuration. **AI Loops Queue** contains waiting/active main loops and their grouped optimizer/backtest jobs; **AI Loops Results** contains finished runs and recorded evidence. **Edit Selected** in either run view opens that run’s actual settings and source snapshot. Saving a waiting entry with its existing name updates that entry; editing a running/finished run prepares a config for a future run without changing its execution history. Queue provides **Start**, **Pause**, **Resume** and **Stop Selected**. Loop-owned optimizer/backtest jobs use the existing controllers but are excluded from the ordinary Optimize/Backtest queue views.

**Delete Selected** in Queue removes waiting runs; in Results it removes selected finished runs and their round history after confirmation. Active/paused runs must first be stopped and their jobs collected. Saved configurations, reusable learned findings and native optimizer/backtest result files are retained. Multiple main runs can be selected; child rows belong to their main run and are not deleted individually.

AI Loops lists use the same compact dark **Search loop name...** control as ordinary Optimize lists. The config editor starts directly with **Starting configuration**; its list search is hidden while editing.

**Save** automatically migrates the captured starting PB8 config with the installed native migration tool, including its captured coin overrides. **Save & Queue** uses the migrated snapshot; queuing an older saved definition also migrates its new run input. Existing run history and original source configs remain unchanged. Explicitly unresolved HSL choices must be selected in the starting PB8 config first; no restart policy is chosen silently. A native migration failure preserves the previous saved definition. New Vast.ai rentals use a worker supporting schema v8.6.0 and the migrated HSL config. Existing rentals retain their recorded image version; after a worker-pin update, restart the PBGui API using its restart-required control before queuing with the new worker.

**Initial evaluation** opens a window with saved-result sources, the initial AI assessment and the first applied changes. Historical optimizer metrics are starting evidence, not independent comparison backtests. Vast.ai results are linked through their original job identity. Older runs without a recorded initial evaluation say so explicitly.

### Round overview and details

Use **Log** beside an optimizer job to open its native Queue log window, including its live status dashboard, local CPU/GPU output or Vast.ai worker/optimizer logs. Main-run and round rows provide the same shortcut when one native optimizer is available; expand rounds with multiple variants to choose a job. Waiting main runs have no native job until started. The window opens over the current AI Loops view, and the chosen log is restored on browser reload.

Queue and Results group each row's controls below its name in a single aligned action strip. Expand/collapse, Open, Log and Results retain their order; an available Continue action appears at the end.

Drag a report window by its title bar; resize it at any edge or corner. Its position and size survive tab changes, updates and browser reload, and remain bounded by the viewport.

AI Loop detail windows follow the standard PBGui overlay design: centered above the current page, with a persistent title/close control and regular-size tabs. Long report content scrolls inside the window. The main view uses the Optimize editor grid, section headings, toolbars and table styles.

Long AI Loops Config, Queue and Results views scroll vertically inside the main area; wide tables also scroll horizontally within their table container.

The expandable Queue/Results table shows status, duration, completed comparison backtests, exact goal score, score change and changed-field count against the start config per round. Click a row or press Enter to open its detail window:

The **Comparison metrics · Δ prior round** column shows exact goal metric values on round and individual comparison-backtest rows without opening a window. Each candidate is compared against the best complete comparison of the previous measured round, or the unchanged start when no earlier measured round exists. Arrows and colors mark **↑ improved** (green), **↓ worse** (red), and **= unchanged** (neutral), respecting each goal’s minimizing/maximizing direction. Gain uses terminal multipliers; Drawdown/ADG changes use percentage points, time metrics use hours. Hover reveals the metric/reducer and exact reference round. Missing references and incomplete simulations have no improvement badge. Goal-score changes and Holdout/Full-Range Gain/DD changes are also colored directly in the overview. Older runs use their already recorded comparison evidence; no additional backtests are launched.

- **Overview**: round timing and associated jobs/errors. Duration includes provisioning, waiting, optimization and validation; older incomplete timestamps are marked as estimates.
- **Goals**: actual exact-backtest metric values, targets and changes against the previous validated round. The goal progress matrix also shows these values across rounds. Missing values stay empty; proxy values and cumulative-best scores are never presented as measured round improvement.
- **Backtests**: real comparison/holdout jobs, statuses, fixed comparison dates/exchanges and recorded metric evidence. These jobs appear below their round in AI Loops Queue/Results, using the native PB8 Backtest worker.
- **Changes**: applied field-by-field before/after diff. Select the optimizer variant and compare it with the previous snapshot, another snapshot or the starting config.
- **AI analysis**: the recorded evaluation, reasons for each adjustment and saved findings. Applied values are authoritative when controller limits differ from an AI suggestion.

**Open** on a main run opens the run summary, errors and recorded decision. Automatic updates and browser reload preserve the selected run, open window, tab and diff selection. Close the window explicitly with **Close** or Escape.

A new adaptive optimizer round is blocked unless the current round has a successful, comparable exact validation backtest. Missing candidates, failed backtests or proxy-only evidence stop continuation with a clear reason before another AI decision or optimizer job. Older quota-stopped results skipped before validation are recovered for candidate selection after the current optimizer round finishes. GPU native iters count exact validations and differ from proxy evaluations.

### Automatic user evaluation per round

Each successful, complete comparison candidate receives its own **Holdout** backtest, including the unchanged starting config. After all Holdouts in a round have finished or failed, PBGui selects the best complete comparable Holdout: candidates meeting the frozen numeric targets come first, then the highest Holdout score computed with the existing goal rubric. Only this candidate receives **Full Time Range** on the original dates without the training suite. For three comparison candidates, this means three Holdouts and one Full Time Range. These jobs run on local CPU workers within available resources and the original deadline while optimization continues. Missing Holdout windows show **skipped**; without a valid Holdout there is no Full Time Range selection.

The Queue/Results round summaries show the selected best Holdout and its Full Time Range. Expanded jobs identify the corresponding Comparison backtest and mark **Best Holdout**; each job displays its own metrics. While selection is pending, the summary shows how many Holdouts have finished. **User evaluation** also shows the separate **Holdout score**. Expand a round to access these user-evaluation jobs with **Open**, **Results** (native Compare), and **Log** (only that job’s native file). **User evaluation** in the main detail window compares all rounds; the same tab in a round shows that round’s metrics, periods, durations and changes versus the unchanged start and previous round. Gain is a terminal multiplier (`1 ×` = break-even); Drawdown and ADG are percentages, recovery time is hours. Across Holdout windows the display uses mean Gain/ADG/Sharpe/Sortino, maximum Drawdown/recovery, and total fills. Missing or incomplete results never receive comparison deltas.

These results are saved separately for your evaluation and are excluded from optimizer AI prompts, JEV candidate decisions, optimizer candidate ranking/goal scores, search-stopping decisions, continuation seeds and learned knowledge. Legacy final-Holdout findings are also excluded from future model context. The Holdout score selects only the user-facing Full Time Range job. Their failures cannot trigger optimizer repairs or change the best comparison candidate. Comparison validation remains required before the next optimizer round. After search has stopped and AI selection is frozen, its own complete native Holdout can serve as the final check, including a previously evaluated window that remained isolated from the AI. PBGui verifies the executed snapshot and numeric targets; another candidate with a better Holdout cannot replace it. Incomplete, failed, mismatched or target-violating evidence remains unconfirmed. Without numeric targets, completion is labeled **Holdout checked**, rather than claiming numeric goals were achieved. Full Time Range remains supplemental user evidence because it also includes training dates. User-evaluation jobs do not consume the comparison-validation quota; their status/counts remain separate. Pause prevents new starts; Stop cancels owned user-evaluation jobs. Normally finished loops keep collecting already started jobs and may launch pending evaluations until the deadline. Deletion waits for job collection. Runs without enabled user evaluations remain unchanged. Enabled runs recover missing candidate Holdouts within their original deadline. Completed historical Full Time Range jobs are retained; a job from an earlier candidate is labeled **Previous selection** when the new Holdout winner differs. Older completed Holdouts can reload their native assessment to obtain a missing selection score.

Select one run under **AI Loops Queue** or **AI Loops Results** and use **Compare all Holdouts** or **Compare all Full Time Ranges** in the sidebar. The same buttons are available under **Open → User evaluation**. They open native Backtest Compare with every completed evaluation of that type from the selected run, including the unchanged start and all candidates/rounds; incomplete or failed jobs are excluded. Curves identify the round, comparison candidate and exchange. Results are loaded only for these jobs, including every result page. New completed evaluations appear during automatic updates; a narrowed selection is preserved. Browser reload restores the comparison, and **Back to AI Loop** restores the originating run/report tab. These buttons only display recorded evidence and do not send it to the AI.

### Create a loop from Optimize

Select one entry in the ordinary PB8 **Configs**, **Queue** or **Results** view and click **Create AI Loop** in the sidebar. It opens an unsaved draft in **AI Loops Config** with an editable name. Configs use the selected config; Queue uses the actual local/Vast job snapshot; Results uses the recoverable executed result config. Sparse coin overrides and source provenance are retained. No config is saved or job started until you use Save or Save & Queue. Saved definitions are private per account under `data/loop_optimizer/<owner>/configs/`; deleting a definition preserves its runs and history. Selections, filters, expanded groups and report navigation survive automatic updates and browser reload; an open unsaved editor is restored as a draft.

### Compare the start, best round and final result

New runs execute an **unchanged starting baseline** on the fixed comparison windows before any optimizer starts. The baseline consumes one validation job and appears under **Initial evaluation / Start comparison**. Older runs explicitly show that a baseline was not recorded.

**Open** on the main result shows the search stop reason, best retained round, numeric-target status and final holdout confirmation. The start-versus-best table uses exact comparison values. Goals show percentages or days/hours with the reduction used: worst window for minimize rules and mean for maximize rules; numeric targets must pass in every report. Targetless presets rank preferences rather than guaranteeing success. Qualitative free-text requirements remain unconfirmed. Partial/liquidated or missing-completion simulations are retained as negative evidence and cannot qualify a winner or authorize another round. When numeric targets are violated, the UI says **target violated**, not goal achievement.

Each Backtest has **Log** and **Results** actions. Log opens only that job’s file in the existing movable, resizable log window over AI Loops. Results opens the native PB8 Backtest Compare directly with this job’s scenario results selected; the normal chart, Analysis JSON and Config JSON controls remain available. **Back to AI Loop** returns to the selected run/round and report tab; this return context survives browser reload. Parent counters include baseline, comparison and final holdout jobs. An intentional optimizer budget stop is shown as **Proxy limit reached**; native work already dispatched can exceed the sampled proxy threshold.

Final holdout scenarios inherit balance at the backtest root. Reservation is distinct from evaluation. Only a proven schema failure before any data/result evaluation can release the reservation and expose **Retry unchanged final holdout**; it keeps the same candidate, allows one technical retry and respects the original deadline and validation budget. Other failures do not authorize adaptive reuse of holdout evidence. Exported best comparison configs include provisional status and final-validation metadata.

### Allow strategy changes

**Allow AI to change strategy (ema_anchor / trailing_martingale)** is off by default. Off protects `strategy_kind` in the config, optimizer runtime overrides and sparse coin overrides. On permits a compatible complete strategy/config change while coins, direction, capacities, per-run limits and disabled Scenario Editor settings remain protected. The Changes tab records the strategy change with other applied fields.

Opening a Loop comparison loads the result list without requesting comparison curves. Curves load only after **Apply comparison**; reloading an explicitly started comparison restores it. In the native Loop comparison, choose **All** or **Top X**, enter the candidate count and click **Apply comparison**. Top X ranks complete scored evaluations by target compliance first, then evaluation score (highest first). Each selected candidate includes all its exchange results, so there may be more than X curves. Holdout and Full Time Range use their own evaluation scores. Evaluations with missing scores or missing native results are excluded from Top X. Selection persists on reload and updates automatically as results arrive.

### AI instructions

Open **AI Instructions** in the Optimize sidebar to read the complete Loop analyst instructions. Select **PBGui default** or a saved personal version. Saved instructions are read-only. Enter a unique **New version name** first to unlock an editable copy and enable **Save & use new version**; the name cannot match PBGui default or an existing personal version. Saving creates a separate immutable version and selects it for your new runs; the built-in default and earlier versions remain available. **Use selected version** switches back to an existing version without creating another copy. **Delete selected version** removes a personal version after confirmation. The built-in default cannot be deleted. Deleting the active version selects PBGui default for new runs; existing run snapshots remain intact. Versions and the active selection belong to your PBGui account.

New runs, including **Continue**, capture the selected instructions when they are created. Already queued or running runs retain their snapshot through API restarts. Open a run's **AI analysis → View instructions** to inspect its actual snapshot and optionally save an edited copy as a new version. Older runs without a recorded snapshot are explicitly labelled as using the current built-in fallback; this does not reconstruct their historical prompt.

The editor retains unfinished edits during automatic updates and panel switches. Browser reload restores the selected version/run and loads its saved text; save unfinished text before reloading. Only navigation identifiers enter browser storage/URLs. Instructions control the analyst's proposals; controller-enforced goals, budgets, owner boundaries and protected config settings still apply. Keep the required JSON response formats when editing. Provider credentials are configured separately in the AI drawer.

### Continue an AI Loop result

If AI evaluation fails after exact comparison backtests have completed, that round remains a usable checkpoint even before its evaluation is committed to history. Its missing user-only Holdout and Full Time Range backtests are queued automatically, including after API restart, within the original time allowance. They run locally and do not restart the failed optimizer or affect AI decisions; Pause/explicit Stop and the original deadline still prevent launches. Use that round's **Continue**, for example **Loop 2 → Continue → 20 → Continue**, to start from its own best verified candidate rather than the main run's overall best.

Use **Continue** directly on the finished main-run row for **Continue from best exact comparison**, or on a round with comparable complete evidence for **Continue from this round’s best exact comparison**. The action opens the existing detail window to choose the additional count; it does not launch immediately. Choose **5 more**, **10 more**, or enter 1–200 additional optimizer runs, then **Continue**. The input keeps its focus and unfinished value during automatic updates. After clicking Continue, **Starting…** and a status message immediately confirm the request; the count and buttons stay disabled until it finishes. Creating the linked run may take time while PBGui loads the current AI selection and verifies the saved checkpoint. Errors appear in the detail window and allow another attempt. A round must have a complete independent comparison checkpoint. After preparation, this starts a new linked run; the existing history is retained. It inherits the pinned goals/rubric, chosen candidate parameters and that round’s optimizer settings/overrides, then establishes a new unchanged baseline before further adjustments. The existing shared AI selection and current Rental & Automation limits apply. The number authorizes additional optimizer work (parallel variants each consume one attempt). Continue also records the explicitly requested completed loops: an optional AI finish, targetless presets or an already achieved financial target do not end that requested continuation early. Native optimizer-attempt, validation, resource, time and cost limits still take precedence, and errors can stop the run. A previously evaluated Holdout never enters training or becomes fresh unseen data. Its isolated result may check the unchanged AI-selected candidate once the new search has ended.

## PB8 8.6: HSL and adaptive entry cooldown

Opening a saved pre-8.6 HSL config automatically creates an unsaved draft using the installed PB8 migration tool. The existing Long/Short JSON editors mark changed HSL lines. Raw JSON marks all compatibility changes by their exact parameter paths, including cooldown/EMA moves, added defaults and optimizer changes; removed parameters mark their surviving parent line. Corresponding structured GUI fields have an amber outline. These marks remain while editing and clear after a successful save. Removed HSL fields appear with their exact old values directly below the corresponding JSON editor; they are read-only and are not saved back into the config. Unchanged fields remain unmarked. Existing valid restart choices are preserved. An active `threshold` policy needs `always` or `never`, selected directly in the editor. Missing unified portfolio values remain blank for explicit entry. Only **Save** validates and writes the draft. HSL behavior changed: re-backtest before live use. Invalid optimizer or override references still require correction.

In `unified` mode, author and review the portfolio policy in `bot.hsl` in **Raw JSON**. PBGui does not automatically promote a side policy. Its required fields are `enabled`, `red_threshold`, `ema_span_minutes`, `cooldown_minutes_after_red`, `restart_after_red_policy` (`always` or `never`) and `panic_close_order_type` (`limit` or `market`). Coin and pside modes use `bot.long.hsl` and `bot.short.hsl`. Retired engine/recovery controls are hidden when absent from the installed runtime.

HSL, entry cooldown and Forager parameters are edited in the existing Long/Short JSON editors. Adaptive cooldown uses `entry_cooldown.base_duration_minutes`, `min_duration_minutes`, optional `max_duration_minutes`, and `weights_minutes.exposure_ratio` / `adverse_directionality`. Use JSON `null` for an unlimited maximum; enabling either weight requires a finite maximum. Forager adds `score_weights.unilateralness` and `unilateralness_ema_span_1m`; a zero weight disables its score contribution. Supported cooldown leaves are also available as typed sparse Coin Overrides. Optimizer bounds come from the installed runtime; PBGui no longer adds retired no-restart threshold fields to current metadata.

AI Loops keep Suite Long/Short coin lists identical for a disabled direction while preserving its zero exposure and position bounds. Different coin lists with both directions active are rejected before launch. Cloud execution failures report the PB8 exit code; a Suite coin-list error is distinguished from result-import failures.

GPU/Vast AI Loop optimizer copies use the previously used completion floor: an enabled `backtest_completion_ratio` limit with `penalize_if: less_than` and value `1.0` becomes `0.99` in the job copy. GPU timestamp precision can otherwise report `0.999994662` for a complete run while Rust reports `1.0`, triggering the constraint drift safety check. Saved configs, CPU optimizer limits and independent exact comparison completeness checks are unchanged. Drawdown limits and Passivbot drift safeguards stay active. If a failed optimizer still produced imported exact candidates, those candidates undergo selection and fresh fixed-window comparison backtests; the optimizer's failure remains visible.

AI-generated date templates (`contract_version: 1`) and graphical explicit windows (`contract_version: 2`) both retain their Holdout periods when queued as AI Loops and when optimizer results are collected. PBGui regenerates the plan from its parameters and verifies that the actual Suite scenarios match training; Holdout scenarios remain separate. This also applies when queueing an existing saved AI-generated definition. Historical failed runs keep their original recorded state.

The current Vast.ai worker includes a temporary GPU suite fix: invalid non-finite candidate metrics receive the CPU optimizer invalid-candidate penalty instead of aborting the whole batch. New rentals use the patched image; existing rentals keep their original image.
