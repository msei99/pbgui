# PBv8 Optimize

PBv8 Optimize manages Passivbot V8 optimizer configurations, queued jobs, results, and Pareto candidates independently from PBv7. The page uses the same template, panels, and visual editor as PBv7 Optimize. A version adapter translates only the PB8 API paths and nested configuration model; there is no separate PB8 optimizer UI.

If PB8 is unavailable after an incomplete installation or update, a persistent **PB8 update required** warning appears above the workspace with the runtime error and a link to VPS Manager. The page remains usable for diagnosis instead of hiding the issue in transient notifications.

The Configs list starts loading in parallel with slower PB8 settings and metadata. Its table uses a lightweight summary request that skips optimize-result inspection, while the separate Results panel continues to load the complete result metadata.

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

PB8's `gpu` backend means experimental **Apple MPS**, not CUDA. PBGui distinguishes a backend registered by PB8 from one available on the current host. GPU remains selectable on unsupported hosts as an explicit editor preview, so all fields can be tested and the portable config can be saved without silently replacing its backend. Queue and Start still fail before creating snapshots or processes with PB8's exact runtime reason. PBGui installation and PB8 update workflows request the optional `gpu-mps` profile; its platform marker installs PyTorch only on Apple Silicon.

When GPU is selected, the editor exposes PB8's runtime-provided nullable population, batch, and candidate-bar sizing, M3 lean auto-parallelism, exact-worker and drift controls, checkpoint interval, and Successive Halving policy. Controls are grouped as **Automatic sizing**, **Exact validation & checkpointing**, **Drift safety**, and **Successive halving**. They use the editor's standard responsive eight-column grid: 8×1 fields on wide screens, 4×2 on medium screens, and 2×4 on small screens. Blank sizing fields retain PB8's automatic defaults and display the effective runtime value as an `auto (…)` placeholder; typing a number intentionally disables automatic sizing for that field. **Reset GPU defaults** restores the installed runtime defaults without deleting unknown future GPU keys. New scoring and limit choices use PB8's GPU proxy allowlist; existing incompatible entries remain visible for repair and PB8's native preflight blocks them before queue or launch.

PB8's default optimize bounds are initial search ranges, not hard slider limits. The editor therefore uses parameter range metadata for the slider and allows values below PB8's defaults, such as `n_positions = 1`.

Forager volume and volatility EMA span sliders have a minimum of `1`. To exclude these parameters from optimization, keep a valid positive bot value and use the row's **Fixed** checkbox instead of setting the span to zero. Backend validation still accepts imported zero spans only when the corresponding Forager signals are guaranteed to remain disabled.

Selecting several exchanges keeps PB8's native combined-dataset behavior. Use explicit Suite scenarios when each exchange must be evaluated separately.

The two compact buttons beside PB8 Optimize's **start_date** resolve PB8's first available candles for the currently selected exchanges and explicit approved coins. **1st** uses the oldest known selected market history. **All** starts only after every selected coin has a known OHLCV timestamp on every selected exchange. While the lookup runs, a compact progress bar reports genuinely completed Exchange/Coin pairs and names the current PB8 operation. **Stop** cancels only this lookup. PBGui adds PB8's required strategy warmup and rounds up to the first fully usable UTC day before setting the date-only `backtest.start_date`. **All** fails with the first unresolved pair when a coin is missing from an exchange or its first timestamp is unknown. Dynamic `all` coin selection is not accepted, and one lookup is limited to 200 exchange/coin pairs. The explicit lookup may populate PB8's native first-timestamp cache but does not download the full OHLCV range. Closing or replacing the editor stops its active lookup automatically.

The **PB8 Scenario Generator** inside Suite Mode previews deterministic `rolling_windows`, `walk_forward`, and `sweep_cycles` plans from the editor's base date range. Window length, stride, training count, optional holdout count, and exchange expansion are validated server-side and capped at 64 generated scenarios. Preview does not modify the config. **Apply Training Scenarios** explicitly replaces the unsaved Suite scenarios and reducer; holdout windows remain outside `backtest.scenarios` and are stored as `pbgui.scenario_template` provenance. Any later manual Suite edit clears that provenance. Sweep Cycles additionally binds this immutable plan to the PB8 result and calculates sequential sweep/refill cash-flow metrics from each Pareto candidate's per-scenario gain. PBGui AI exposes the same generator as a read-only preview tool and must still use the existing proposal flow for Save or Queue operations.

### Scenario Generator

The Scenario Generator turns one PB8 Optimize config into a reproducible group of historical tests. PB8 still performs normal Suite optimization. PBGui is responsible for generating the date windows, preserving the experiment plan, evaluating Sweep cash flows after PB8 returns scenario metrics, and preparing the final Holdout backtests.

#### What Each Action Does

| Action | What changes | What does not change |
| --- | --- | --- |
| **1st / All** beside `start_date` | Resolves an OHLCV-based start date | Suite scenarios and generator settings |
| **Recalculate** | Re-reads current dates/exchanges and recalculates automatic Sweep counts | Saved config and applied Suite |
| **Preview** | Shows exact Train/Holdout windows and warnings | Config, Suite, scoring, bounds, and queue |
| **Apply Training Scenarios** | Enables Suite Mode, installs Train scenarios/reducer, stores Holdout provenance, and applies the Sweep preset | No config is saved or queued yet |
| **Save / Save & Queue** | Persists or launches the applied experiment | Holdout remains excluded from optimization |
| **Paretos** | Shows PB8 metrics plus PBGui `sweep_*` cash-flow metrics | Original PB8 candidate metrics |
| **Holdout** in the Pareto sidebar | Builds standalone PB8 Backtest queue drafts from immutable Holdout dates | Candidate parameters, coins, exchange, balance, and overrides |

#### Settings At A Glance

| Setting | Meaning |
| --- | --- |
| **Template** | Rolling comparison, Walk-Forward validation, or sequential Sweep cash-flow evaluation |
| **Window days** | Trading days contained in each scenario |
| **Stride days** | Distance between consecutive window end dates; automatic for Sweep |
| **Training windows** | Scenarios PB8 evaluates during optimization; automatic for Sweep |
| **Holdout windows** | Untouched periods reserved for final out-of-sample Backtests |
| **Exchange mode** | Inherit the combined base exchanges or expand separate exchange scenarios where supported |
| **Starting balance** | PB8 simulation capital and Sweep reset capital after Apply |
| **Balance multiplier** | Sweep target: Starting balance multiplied by this value |
| **Refill cost** | Additional external cost booked when a loss window is refilled |
| **Cooldown days** | No-trading gap between Sweep windows; included automatically in Stride |

#### Recommended Sweep Workflow

1. Select explicit coins and exchanges.
2. Use **All** for a start date common to every selected Exchange/Coin pair, or **1st** when changing-universe history is intentional.
3. Select **Sweep Cycles**, set Window, Holdout, Starting balance, Multiplier, Refill cost, and Cooldown. PBGui calculates Stride and Training windows.
4. Click **Recalculate** after any OHLCV/date/exchange change, then **Preview**.
5. Click **Apply Training Scenarios**. PBGui synchronizes base balance, symmetric Suite coin lists, reducer, scoring, limits, and meaningful Long bounds.
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
5. Set **Training windows** manually for Rolling Windows or Walk-Forward. Sweep Cycles calculates the maximum complete Training count automatically from `start_date` through `end_date` after reserving the selected **Holdout windows**. With **Exchange mode = Inherit base**, every window uses the combined base exchange selection.
6. Click **Preview**. Review the generated labels, exact date ranges, Train/Holdout classification, scenario count, and warnings. Preview alone does not change the Suite or config.
7. Click **Apply Training Scenarios** when the plan is correct. This enables Suite Mode, replaces the current unsaved Suite scenarios, and applies the suggested reducer. Holdout rows are deliberately not copied into `backtest.scenarios`.
8. Review named Objective Scenario, scoring, and limit references after replacing an existing Suite. Their scenario labels must still exist in the newly generated training set.
9. Use the normal **Save** or Queue workflow only after reviewing the applied Suite. Saving persists the generator parameters and holdout rows under `pbgui.scenario_template` for traceability.

Run **Preview** again before Apply if the base dates or exchanges changed. PBGui blocks application of a stale preview. Editing, adding, removing, reordering, or replacing Suite scenarios after Apply clears the generator provenance because the saved Suite no longer exactly matches the generated plan.

After changing approved coins and using **1st** or **All** to update `start_date`, click **Recalculate** beside **Guide**. It reloads the current base dates and exchanges, recalculates automatic Sweep Stride and Training windows, and discards any stale Preview before a new one can be applied.

Example: for three non-overlapping quarterly training periods and one untouched quarter, choose **Walk-Forward**, `Window days = 90`, `Stride days = 90`, `Training windows = 3`, and `Holdout windows = 1`. For six overlapping three-month training periods sampled monthly, choose **Rolling Windows**, `Window days = 90`, `Stride days = 30`, and `Training windows = 6`.

**Sweep Cycles example:** evaluate repeated account-growth cycles from `1,000` to `2,000` USD. Select **Sweep Cycles**, set `Window days = 180`, `Cooldown days = 7`, and `Holdout windows = 1`. PBGui calculates `Stride days = 187` and the maximum complete Training count automatically from the base dates; incomplete leading days are reported instead of requiring manual arithmetic. Set **Starting balance** to `1000`, **Balance multiplier** to `2`, and **Refill cost** to `25`. Preview shows every complete 180-day training window separated by seven no-trading days plus the reserved untouched holdout window. For every Pareto candidate PBGui applies the windows chronologically. Positive gains below 2,000 USD carry into the next window. At or above 2,000 USD, everything above 1,000 USD becomes swept cash and working capital resets to 1,000 USD. Below 1,000 USD, PBGui books the missing amount plus 25 USD external refill cost and resets to 1,000 USD. Pareto columns then expose `sweep_net_cashflow`, `sweep_total_swept`, `sweep_external_capital`, `sweep_cycles_completed`, `sweep_refill_count`, `sweep_final_balance`, and `sweep_target_hit_rate`. The holdout remains pending until the selected candidate is run separately over that period. This is a deterministic window-boundary evaluation; it does not move real funds or claim target crossings inside a window.

PB8 Gain values are terminal multipliers, not additive returns: `1.0` is break-even, `2.0` doubles the opening balance, and `0.8` loses 20%. Sweep evaluation therefore calculates each window as `ending_balance = opening_balance × gain_strategy_eq`.

To run the validation without manual editing, select one or more candidates in the Paretos table and click **Holdout** in the sidebar. PBGui reads immutable holdout dates from the result sidecar, creates one standalone PB8 Backtest item per candidate and holdout, disables Suite Mode in those drafts, preserves candidate settings, coins, exchanges, balance, and overrides, and opens the Backtest queue draft ready for submission.

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

## Archives

PB8 Optimize configurations and PB8 Backtest results use the existing Archive workflow. Files are stored under their `config_version`, so PB7 and PB8 content cannot overwrite each other. Import, export, view, delete, restore, and handoff actions always use the parser belonging to the archived configuration version.
