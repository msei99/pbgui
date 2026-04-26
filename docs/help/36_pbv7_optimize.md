# PBv7 Optimize

The **PBv7 Optimize** page now opens as a standalone **FastAPI + Vanilla JS** page.
It lets you create, queue and inspect Passivbot v7 optimisations without relying on the old Streamlit worker loop.
The top **PBv7** navigation now also switches directly between the FastAPI **Run**, **Backtest**, and **Optimize** pages instead of routing Optimize back through Streamlit.
The page is organised into four sidebar panels:

| Panel | Purpose |
|-------|---------|
| **Configs** | Search, multi-select, edit, duplicate, delete and queue saved optimize configs |
| **Queue** | Monitor queued and running jobs, open queue settings, and inspect logs |
| **Results** | Browse completed optimize result sets |
| **Paretos** | Inspect the Pareto files belonging to one result set |

---

## Configs panel

Shows all saved optimisation configurations from `data/opt_v7/`.

Sidebar actions:

| Button | Action |
|--------|--------|
| **New Config** | Open a new config in the structured editor |
| **Edit Selected** | Open exactly one selected config in the editor |
| **Duplicate** | Copy the selected config under a new name |
| **Queue Selected** | Add all selected configs to the queue |
| **Delete Selected** | Delete all selected configs |

Use the search field to filter by config name. Config rows support the same click-and-drag multi-selection pattern as Backtest, and the actions on the right are compact icon buttons.

### Structured editor

Creating or editing a config replaces the list with a dedicated editor view and shows an editor sidebar, similar to Backtest.
The editor uses the same responsive 8-column field grid, popup date selector and mouse-driven multiselect widgets as the Backtest and Run editors.
Hover dotted field labels to see the inline help tooltip for that setting.

| Button | Action |
|--------|--------|
| **Home** | Return to the config list |
| **Save** | Save the config to `data/opt_v7/{name}.json` |
| **Save and Queue** | Save the config and enqueue it immediately |
| **🧭 OHLCV Readiness** | Open a draggable, resizable floating window and run a PB7-backed read-only preflight for the current optimize config, showing whether the approved coin set is locally ready, can import from legacy OHLCV data, would fetch on start, or is blocked by persistent gaps; the list evaluates the union of `approved_coins_long` and `approved_coins_short`, and each entry now shows whether it comes from `long`, `short`, or both. If PB7 would fetch missing ranges, the window also offers **Preload OHLCV Data** to warm the cache in the background before queueing, automatically jumps to the preload job log section when that preload starts, shows real log-derived progress rows from the active archive/ccxt download lines plus duration, PID, log counters, and last-update details, follows CCXT progress via the moving request cursor instead of bouncing to 100% when an exchange returns newer candles than requested, uses the same warmup-adjusted effective start as the readiness check so the post-preload refresh no longer leaves the warmup days behind, classifies markets that only launched after the requested window as too young instead of pretending those older candles can be fetched, prunes such coins from preload jobs, includes a **Stop Preload** action while the downloader is active, provides a top-right fit-to-browser-window control for the floating panel, keeps that log tail running without jumping back to the top, and keeps the finished preload result visible until a fresh readiness check replaces it |

The **Raw Config JSON** section now matches Backtest and Run: typing in raw JSON syncs automatically back into the structured fields, structured edits rebuild raw JSON automatically, and invalid JSON is highlighted live with line-based error reveal.

Older optimize files that only store minimal `backtest` + `optimize` blocks can now also be reopened in the editor again. PBGui fills the missing base sections from the current optimize template before handing the config to the PB7 preparation pipeline, so legacy queue candidates no longer fail just because they were saved as a stripped-down stub.

`btc_collateral_ltv_cap` now matches Backtest as well: the form shows `0` when PB7 stores `null`, and saving `0` maps back to the underlying unlimited-debt `null` value.

Main editor areas:

| Area | Description |
|------|-------------|
| **Top row** | Name, exchanges, and date range for the optimize config |
| **Market & Universe** | Starting balance, candle interval, OHLCV source with the `PBGui Data` quick-fill button, BTC collateral caps, `hsl_signal_mode`, market filters, approved/ignored coins, and `coin_sources` |
| **Optimization** | The shared Suite editor plus `Scoring`, `Limits`, `Bounds & Overrides`, and the backend-specific optimizer controls |
| **Run Settings** | Starting seeds, iterations, CPUs, pareto retention, logging, memory snapshots, output throttles, significant-digit rounding, and result persistence toggles |
| **Additional Parameters** | Always-visible expander for unknown `optimize.*` settings; when none exist it shows an empty-state hint, while canonical backend fields such as `optimize.backend`, `optimize.pymoo.*`, `compress_results_file`, and `write_all_results` stay in their dedicated sections |
| **Raw Config JSON** | Full config base object used during save so untouched sections stay preserved, with automatic two-way sync and live validation |

The editor now keeps only three visible main section titles: **Market & Universe**, **Optimization**, and **Run Settings**. The goal is to reduce label noise while still keeping the flow obvious: first define the data scope and coin universe, then the optimizer search space, then the run-time settings. This also matches the technical dependencies in the page: Pymoo `auto` resolves the effective algorithm from the current scoring objective count, and Pymoo mutation `auto` derives from the active bound count.

The `n_cpus` input in **Run Settings** is capped at the CPU count of the host running PBGui/Optimize, so the structured editor cannot request more worker processes than that machine has.

The first header row now starts with `config_name` before `exchanges`, because naming the optimize config is usually the first meaningful input before choosing markets and the test window.

The `end_date` field now preserves the literal `now` token when a config uses rolling-today semantics instead of materializing it into today's fixed date just by opening and saving the editor.

When **Starting Seeds** is set to `self`, the section shows the seed config directly with quick controls for `total_wallet_exposure_limit` and `n_positions` plus the full `bot.long` and `bot.short` JSON editors. For `path`, the `seed_path` field stays next to `seed_mode` without an extra helper line underneath, so the controls stay aligned. The config block stays hidden for `none` and `path`, because those modes do not seed from the current config.

The `bot.long` and `bot.short` JSON editors now also mirror Backtest/Run neutralization feedback: fields normalized or injected by the Passivbot preparation pipeline are marked inline via the same amber/red line highlighting and legend badges, so long/short diffs are easier to review before saving.

The **Scoring** expander now mirrors PB7's canonical objective format directly: existing objectives are shown as explicit Metric / Goal rows which can be edited inline by clicking the row, and new objectives stay hidden until you click **Add**. Creation then opens in the same inline table layout, but with the Type / Metric / Currency selectors arranged side by side in a single row instead of stacked vertically, because the scoring row has enough horizontal space for all three controls. Known Passivbot defaults are preselected where PB7 defines them, while metrics without a PB7 default still keep the goal explicit in the saved config.

The **Limits** expander now mirrors the old Streamlit workflow more closely while also matching the current PB7 schema: existing limits are shown in a compact table with Metric / Penalize If / Stat / Value / Enabled columns and can be edited inline by clicking the row. New limits stay hidden until you click **Add**; only then does the same inline table row appear for creation, without a separate “Add New Limit” block or helper text below it. The stacked Metric selector stays grouped on the left as Type / Metric / Currency, and `Enabled` sits at the end next to the row actions instead of in the middle. The editor now exposes the full canonical operator set (`>`, `>=`, `<`, `<=`, `==`, `!=`, `outside_range`, `inside_range`, `auto`), supports `median` as an aggregate stat, and offers the newer PB7 metric families from `docs`, `schema`, and `src` such as strategy-PnL-rebased objectives, HSL/hard-stop metrics, trade loss metrics, win rate, paper-loss / exposure ratios, and `backtest_completion_ratio`.

The **Bounds & Overrides** expander now replaces the old raw `optimize.bounds` textarea with a Streamlit-like layout: **Bounds long** and **Bounds short** sit side by side, and each bound row uses a range slider with the current Min / Max values shown above it, a compact `step` input on the right, and a per-row **Fixed** checkbox that stores that bound key in `optimize.fixed_params`. The bound name, `step`, and `fixed` labels carry their hover help directly through the dotted inline label text, without extra question-mark buttons in that row. When both slider handles are pushed very close together, the Min / Max labels are automatically separated so the numbers remain readable. If both thumbs truly sit on exactly the same value, the next drag direction decides which handle PBGui moves: dragging left picks the lower bound, dragging right picks the upper bound. If the thumbs are only very close, such as `0 | 1`, PBGui now prefers the clicked side first instead of forcing direction-based selection, so the left handle can still be moved to the right and the right handle can still be moved to the left naturally. Bounds precision now follows the per-row `step` first; if no bound-specific step is set, PBGui uses the built-in per-parameter slider defaults again, and only unknown bounds fall back to `round_to_n_significant_digits` with `5` as the final fallback. Once a bound step is present, the live range inputs immediately adopt that step too, so dragging the slider follows the entered increment right away instead of waiting for a full editor rebuild. Direct Min / Max chip edits now follow that same explicit step as well: extra decimal places are trimmed while typing, and the committed value snaps onto the same step grid the slider itself uses. The visible Min / Max chips use that precision only as an upper limit and trim trailing zero padding, so values like `0`, `10`, and a step of `0.1` stay displayed as `0`, `10`, and `0.1` instead of padded forms such as `0.00000`, `10.0`, or `0.10000`. The same section also keeps the TP-grid direction and `lossless_close_trailing` search constraints next to the runtime-override controls, so all search-space restrictions stay together before the backend-specific settings. It also exposes `fixed_runtime_overrides` as two dedicated optimize-only inputs for `bot.long.hsl_no_restart_drawdown_threshold` and `bot.short.hsl_no_restart_drawdown_threshold`, matching the PB7 prepare pipeline that currently preserves only these two runtime override keys. The GUI keeps that subsection compact and does not repeat the raw dotted keys under the inputs; the details live here in the guide: the fields always display dot decimals such as `0.1`, accept values in the documented `0.0` to `1.0` range, and PB7 clamps values below the matching `hsl_red_threshold` up at runtime.

The optimizer part of the editor is now backend-aware instead of treating all optimizer keys as one flat block. The general header row only keeps config identity fields, while `optimize.backend` now sits only after **Scoring**, **Limits**, and **Bounds & Overrides**, because those sections directly influence the backend-specific auto displays. Selecting that backend switch toggles between the canonical nested Pymoo controls and the DEAP-only legacy controls. When an older optimize config has no explicit `optimize.backend` yet but still carries the legacy DEAP fields, the editor now opens it as `deap` instead of silently falling back to `pymoo`. Switching between the backends now also performs an explicit migration step in the editor: shared fields such as population size and the eta values are copied across, DEAP-only fields that cannot be derived from pymoo are reset to clear PB7 defaults, and the inactive backend's stale fields are removed again on save so the config does not keep drifting into a mixed DEAP/Pymoo state. For Pymoo, the dedicated section now edits `optimize.pymoo.algorithm`, `optimize.pymoo.shared.*`, and the NSGA-III `ref_dirs` keys directly, while the editor shows the effective algorithm PB7 will actually run from the current objective count and keeps mutation auto mode aligned with the active bound count. That keeps canonical PB7 fields out of **Additional Parameters** and avoids maintaining a second static optimizer option list in PBGui.

Raw JSON sync now also recalculates that legacy backend inference from the parsed config itself, so stale `deap` hints do not stick around after you remove the old DEAP-only keys or add an explicit `optimize.backend` in the raw editor.

The **coin_sources** expander now uses the same chip-based interaction pattern as Backtest instead of the old PBGui JSON block: choose an exchange, pick a coin from the loaded symbol list, and the editor stores the override under `backtest.coin_sources`. Legacy `pbgui.coin_sources` values are folded into the structured editor when loading, and the obsolete PBGui `market_settings_sources` field is no longer shown there.

The **Suite Mode** section now reuses the same shared component as Backtest instead of leaving suite-only configs stranded in the legacy Streamlit page. That means `backtest.suite_enabled`, `backtest.scenarios`, and `backtest.aggregate` can now be edited and saved directly in FastAPI Optimize, including the built-in templates, scenario override rows, per-scenario `coin_sources`, and aggregate metric rules.

The **Additional Parameters** expander is reserved for optimizer settings the GUI does not handle yet and now always stays visible at the bottom of the editor. If `optimize.*` contains unknown keys, they appear there as typed fields or JSON editors instead of disappearing behind the raw editor; if none are present, the expander shows a small empty-state hint so it is still clear where extra optimize keys would appear. Known fields that already have dedicated controls, such as `round_to_n_significant_digits`, `compress_results_file`, and `write_all_results`, stay in their normal section instead of being duplicated there. Changes made there flow into the saved config like the rest of the structured editor.

Important config points:

| Section | Description |
|---------|-------------|
| **Exchange / Symbols** | Exchange and coins to optimise for |
| **Date range** | Start and end date of the optimisation simulation |
| **Iterations** | Number of optimizer generations |
| **CPU cores** | Parallel workers inside one optimisation run |
| **Market & Universe** | Groups starting balance, candle interval, OHLCV source, BTC collateral caps, and the full coin-universe controls before optimizer-specific settings |
| **Run Settings** | Holds starting seeds, iterations, CPUs, pareto retention, logging, throttles, rounding and result persistence after the search setup is defined |
| **Logging level** | Uses the same selector as Run with `warning`, `info`, `debug`, and `trace` labels while still saving PB7's numeric `0`-`3` log levels |
| **hsl_signal_mode** | PB7-derived selector for account-level HSL behaviour during optimize/backtest evaluation: `pside` keeps long/short signals separate, `unified` shares one combined signal |
| **Backend** | Selects `optimize.backend` from PB7's supported optimizer backends only after objectives, limits and bounds are already defined, so the backend-specific auto behaviour reflects the current search space |
| **Pymoo algorithm** | Uses the canonical `optimize.pymoo.algorithm` setting with PB7's `auto`, `nsga2`, and `nsga3` values, and shows the effective algorithm resolved from the current objective count |
| **Population size** | For pymoo/NSGA-III, `auto` is now a read-only display that shows the effective numeric population size derived from the active reference directions. When NSGA-III uses an explicit population that is smaller than the current reference-direction minimum, the editor raises it to the actual PB7 runtime-effective size instead of leaving a misleading smaller value in the field. For pymoo/NSGA-II the editor forces an explicit population size instead of leaving that contradiction editable. For legacy DEAP configs with `population_size = null`, the editor reopens the field as `500`, because that is the PB7 runtime fallback actually used by DEAP |
| **Pymoo shared params** | `optimize.pymoo.shared.crossover_eta`, `crossover_prob_var`, `mutation_eta`, `mutation_prob_var`, and `eliminate_duplicates` now have dedicated controls instead of living under Additional Parameters; when `mutation_prob_var_mode` is `auto`, the editor shows the derived PB7 value `1 / n_params` as a read-only display instead of leaving the field blank and disabled |
| **NSGA-III ref_dirs** | `optimize.pymoo.algorithms.nsga3.ref_dirs.method` and `n_partitions` are now exposed structurally when NSGA-III is active; when PB7 exposes only one supported `ref_dirs_method`, the editor shows it as a fixed read-only value instead of a meaningless dropdown, and when `ref_dirs_n_partitions_mode` is `auto`, the editor shows the PB7-derived partition count as a read-only value instead of a blank disabled field |
| **Backend switching** | Switching between `pymoo` and `deap` now copies the overlapping fields, including the DEAP `crossover_probability` → pymoo `crossover_prob_var` mapping, applies PB7 DEAP defaults where no direct pymoo mapping exists, and removes stale fields from the inactive backend on save |
| **crossover_probability + mutation_probability** | The DEAP editor keeps the combined probability at or below `1.0`, matching the legacy Streamlit optimizer behavior |
| **Bounds & Overrides** | Edit optimizer Min / Max / Step bounds structurally, mark individual bounds as `fixed_params`, keep TP-grid / lossless-close search constraints with the bounds, and manage the two PB7-preserved HSL `fixed_runtime_overrides` fields |
| **round_to_n_significant_digits** | Rounds optimized parameter values to the configured number of significant digits before they are written back into saved configs and artifacts |
| **Pareto max size** | Maximum configs kept on the Pareto front |
| **Suite Mode** | Enables PB7 multi-scenario optimization from the FastAPI editor and stores the suite config back under `backtest.suite_enabled`, `backtest.scenarios`, and `backtest.aggregate` |
| **Scoring** | Objective functions. PB7 stores them as explicit metric/goal pairs, and the FastAPI editor now exposes those pairs directly with dedicated Metric/Goal controls instead of requiring JSON edits |
| **Starting Seeds** | `none` disables seeding, `self` seeds from the config being queued and reveals the seed config directly, and `path` passes an explicit file or directory into Passivbot `--start` |

---

## Queue panel

Shows all pending, running and completed optimisation jobs from `data/opt_v7_queue/`.

Table columns:

| Column | Description |
|--------|-------------|
| **Name** | Source config name |
| **Exchange** | Exchanges configured for this job |
| **Status** | Current state: `queued`, `running`, `optimizing`, `complete`, `error` |
| **Created** | Queue entry creation time |
| **Actions** | Compact icon actions for start, stop, restart, log, reopen config, delete |

Sidebar actions:

| Button | Action |
|--------|--------|
| **Delete Selected** | Remove the selected queue items, including completed entries, matching the Config list pattern |
| **Settings** | Open the queue settings dialog for `Autostart` and the CPU value that autostart should enforce before launching queued configs |

Use the thin grab strip at the far left edge of each queue row to reorder the queue with drag and drop. That same start strip shows the blue marker for selected rows, but stays hidden on unselected rows until you hover that left edge, so the queue does not look permanently preselected. PBGui persists that row order on disk, and autostart now respects the same top-to-bottom order when it picks the next queued optimize job.
If you have multiple queue rows selected and start dragging one of those selected rows, PBGui now moves that whole selected block together. The grabbed item(s) stay visible through a drag preview built from clones of the real queue rows, so the cursor carries the same row layout you see in the queue instead of a generic browser drag ghost.
Dragging across rows to add or remove a selection now tracks the rows actually under the cursor. If the pointer briefly slips between rows or outside that small block while you are deselecting a middle subset, PBGui keeps the last valid row anchor instead of suddenly expanding the deselection to a much larger range.
PBGui also recomputes the live queue selection from the row state captured at mouse-down on every drag update. If you briefly drag one row too far and then shrink the selection or deselection range again, rows outside the final range are restored instead of staying accidentally lost.
Live queue websocket updates no longer rerender the table in the middle of that drag selection. PBGui now waits until the mouse interaction finishes before applying the latest queue refresh, so selecting rows feels stable again like the Results table.

### Log viewer

Each queue row has a **Log** action.
It opens the shared floating log viewer and streams the local file from `data/logs/optimizes/`.
The **Edit** action opens the actual config file referenced by that queue row, even if the row label differs from the stored config filename.
If an older queue row still points to a deleted config path but PBGui finds matching configs, the matching-config modal can now repair that existing queue entry in place. Choose the correct candidate and PBGui updates the queue row to the selected config path plus a fresh embedded snapshot, so you do not have to delete the row and queue it again manually.
Newer queue items also keep an embedded config snapshot. If the original config file is later renamed or deleted, PBGui updates queued references on save/rename and can still reopen or start that queued job from its stored snapshot instead of failing on the stale path.
If an older queue row predates snapshots and its original config path is gone while multiple matching configs still exist, PBGui now opens a selection modal with direct **Open** buttons for those candidates instead of only flashing a short error toast.
PBGui now also rejects **Requeue** for queue rows whose config is still unlaunchable. Those rows keep their current `error` state and existing optimize log until the config has actually been fixed, instead of being reset to a misleading `queued` state with no runnable job behind it.

The queue itself no longer needs a manual refresh button in the sidebar. It keeps updating from the live websocket feed, and the **Settings** dialog now owns the autostart controls instead of a permanent sidebar checkbox. When autostart is enabled, PBGui rewrites each queued config's `optimize.n_cpus` to the configured queue CPU value immediately before launching that item.
The log dashboard summary now uses the **CPU** field for the configured optimizer cores. Hovering that CPU value opens an htop-like per-core view with memory, swap, and load-average details, and that hover keeps updating live while it stays open.
If an optimize launcher PID goes stale while the actual `optimize.py` job is still alive, PBGui now re-attaches the queue row to the live process so the item stays visible as running and **Stop** still terminates the real job.
If multiple queue rows point at the same config, PBGui now binds the live process only to the row whose own optimize log is actually attached to that process. Other rows no longer inherit that same `running` state just because they share the config file.
Optimize page toasts are now also appended to the global PBGui notification log, so messages shown briefly in the page can still be reopened later from the top-right notification bell.

---

## Results panel

Browse completed optimisation result sets in `pb7/optimize_results/`.

Toolbar and sidebar actions:

| Button | Action |
|--------|--------|
| **Delete Selected** | Delete all selected result directories from the sidebar, matching the Backtest result workflow |
| **Search** | Filter by optimize name or result folder |

While any optimize job is running, this panel refreshes automatically every few seconds so newly written Pareto files show up in the **Paretos** count without manual reload.

Click the sortable table headers to reorder by **Name**, **Result Directory**, **Paretos**, **Mode**, or **Modified**.

Table columns:

| Column | Description |
|--------|-------------|
| **Name** | Config name detected from the result |
| **Result Directory** | Result folder name |
| **Paretos** | Number of Pareto JSON files found |
| **Mode** | Shows whether the result is a regular single-result optimize set or a suite result, including the number of suite scenarios |
| **Modified** | Timestamp of the result set |
| **Actions** | Compact icon actions for the pareto file list, the full **Pareto Explorer**, the original PB7 **Pareto Dash**, the legacy PB7 **3D plot**, continue optimize from the result's `pareto/` directory, and open the first config draft |

The compact **pareto list** action now uses its own folder-style icon, while **🎯 Pareto Explorer** is restored as a separate dedicated action again, matching the old Streamlit results table instead of overloading the list button with the explorer icon. When you click **🎯 Pareto Explorer** from Results, PBGui now stays in the current tab and forwards that selected result path through the Streamlit relay automatically. When you return to FastAPI Optimize afterwards, the page refreshes its configs, queue, and results automatically so the Results view does not stay on a cached empty snapshot.

When Pareto Explorer switches from the fast `pareto/*.json` view to **Load all_results.bin**, it now keeps the persisted PB7 Pareto set in that larger sample as well. PBGui hashes the full PB7 result entries the same way PB7 names the `pareto/<hash>.json` files, injects those known Pareto members into the sampled config window, and preserves those official Pareto flags instead of recomputing a smaller subset-only front.

The **PB7 3D plot** action now renders the legacy PB7-style interactive 3D Plotly view directly inside a large modal in the current FastAPI tab when that result exposes exactly 3 objectives. This keeps the original PB7 3D perspective distinct from the richer PBGui Pareto Explorer page, but avoids opening a separate browser tab. If a result does not provide valid 3D Pareto points, the page falls back to a details modal with the PB7 reason instead of only showing a generic launch toast.

The **PD** action opens Passivbot's original `tools/pareto_dash.py` dashboard in the same large modal window style. PBGui launches the Dash app in the PB7 environment, stages only the selected result so the original run selector lands on the correct run immediately, and serves it back through the FastAPI origin so you can use the native PB7 dashboard without leaving the current tab.

Use **Continue Optimize** when you want to start a fresh optimize run seeded from an earlier result set. It pre-fills the editor with the result config and points `seed_path` at that result's `pareto/` directory.

Use **Open Config** when you want to load the first Pareto config from that result into the editor as a draft without adding any seed metadata.

---

## Paretos panel

Open this panel from **Results → Paretos**.
It shows the Pareto JSON files for one selected result set.

Sidebar actions:

| Button | Action |
|--------|--------|
| **Seed Selected** | Open a new optimize draft using the selected pareto file or a bundle made from the selected pareto rows as `seed_path` |
| **Seed Whole Result** | Open a new optimize draft using the current result's full `pareto/` directory as `seed_path` |

While an optimize job is still writing paretos, the open list refreshes automatically every few seconds.

Toolbar controls:

| Control | Description |
|--------|-------------|
| **Mode chip** | Shows whether the selected result is a regular result, a suite result, or a legacy pareto format |
| **Scenario** | For suite results, switches the summary between **Aggregated** and one concrete scenario label |
| **Statistic** | For aggregated suite view and regular single-result paretos, switches the summary stat between `mean`, `min`, `max`, and `std` |

Table columns:

| Column | Description |
|--------|-------------|
| **Name** | Pareto filename |
| **Summary** | Quick metric badges extracted from the file when available |
| **Modified** | File modification time |
| **Actions** | Compact icon actions for raw JSON view or using one pareto file directly as the starting seed for a new optimize draft |

For suite results, the **Summary** badges now follow the toolbar selection instead of always showing one fixed aggregate view. That means you can inspect either the aggregated suite statistic or one specific scenario directly in the FastAPI page without switching back to the legacy Streamlit Optimize page.

**Use as Seed** opens a new optimize draft from the current result config and points `seed_path` at that pareto file.

When you multi-select several pareto rows, **Seed Selected** creates a small seed directory from exactly those files and uses that directory as `seed_path`.

---

## Typical workflows

### Run a new optimisation
1. Open **Configs** and click **New Config**.
2. Fill the structured fields, adjust advanced JSON sections if needed, then **Save and Queue**.
3. Open **Queue**, use **Settings** if you want to enable **Autostart** or set the queue CPU value, and use **Log** to watch progress.
4. When the job finishes, go to **Results**.

### Explore results
1. Open **Results** and filter to the run you want.
2. Click **Paretos** to inspect the generated Pareto files.
3. Use **View JSON** for raw inspection, click the seed icon to continue from one pareto, or multi-select rows and use **Seed Selected**.

### Tune an existing config
1. In **Configs**, search for the config and select it.
2. Click **Edit Selected**.
3. Adjust the structured form or advanced JSON sections, then **Save** or **Save and Queue**.

### Continue from an earlier optimize run
1. Open **Results** and find the result set you want to reuse.
2. Click **Continue Optimize** to seed from the full `pareto/` directory, or open **Paretos** and use **Seed Whole Result** / **Seed Selected** for more control.
3. Adjust the draft if needed, then **Save** or **Save and Queue**.
4. The new run starts from the saved pareto seeds, but it is still a fresh optimize run, not an exact checkpoint resume.
