# PBv9 Optimize

The exchange picker lists all historical-data venues supported by the installed PB9 connectors and CCXT. Template exchanges are the initial selection, not the available choices; Suite Mode and coin sources use the same catalog.

Coin selection and Apply Filters share PB8's market-catalog and filter logic,
using the native PB9 resolver. Normal coins appear once by name. Exact market
IDs remain available for ambiguous markets and existing exact selections.

The shared coin search matches names and native contracts, not exchange prefixes
or display qualifiers: `hype` finds HYPE/HYPER rather than STX on Hyperliquid.
Full identifiers such as `hyperliquid::19` remain searchable and are preserved
when selected.

Open **PBv9 → Optimize**. Configs, Queue, Results and Paretos use the same page,
controls and engine as PBv8, with a separate native PB9 runtime and data namespace.

Configs load before native metadata, queue and result inspection. While the first
list request is pending, the page shows a loading state instead of zero configs.
Backtest counts update separately without delaying the config rows.

## Editor and native policy

Policy labels and checkboxes follow the PB8 layout. Dotted-underlined labels open
field help on hover or keyboard focus, including the installed PB9 documentation
source and the field's canonical path. Directional overrides identify their shared
`bot.global` default; side activation remains explicit. If the installed PB9
documentation has no description, the tooltip says so rather than borrowing an
unrelated setting's explanation.

Use New Config or import a native PB9 optimizer config. The shared editor contains
Market & Universe, Scoring, Limits, Bounds & Overrides, backend settings and Raw
JSON. PB9 additionally has shared **Global** and **Portfolio** bounds. Policy
controls use native field names. Inherited side values remain inherited until
edited; viewing a config does not pin every displayed default.

Global bounds apply to sides that inherit the corresponding setting. The Long
and Short lists show explicit ranges for each side, so the lists can differ.
Open **Policy settings** to edit policy or open **Global bounds** and **Portfolio
bounds** to edit the corresponding ranges. Use **Search global bounds** to find
a slider by its parameter name. Filtering only changes which sliders are shown.
**Portfolio policy (account-wide)** and **Shared defaults (Long and Short)**
appear above the two directional policy cards. Use **Add bound** under Global or
Portfolio or **Add override** under Long or Short to choose eligible native
parameters, including parameters absent from the starter config. A directional
override starts with the global range when one is configured. **Remove**
deletes the selected range. In the native HSL scope selector, **portfolio**
makes the four portfolio HSL bounds available. Remove any configured
directional or global HSL ranges when switching to that scope.
An explicitly pinned side value with no side range stays fixed. Changing
strategy can populate shared ranges when a shared strategy search is already
configured. Existing side ranges remain scoped to their own side and strategy.

**Long overrides** and **Short overrides** at the top of the directional policy
cards list the values a side sets itself. **Reset** removes that side value, so
the side inherits the shared default again (or the native default for side-only
settings such as `enabled`). Side JSON and policy fields stay synchronized; the
most recent edit of a value wins.

A **fixed** bound keeps its current config value, not a value from the slider
range. The value used appears below the checkbox as `uses: …`. To freeze a
different value, change the config value or set the bound's minimum and maximum
to that value. Numeric policy values that cannot be optimized, for example
`forager.score_hysteresis_margin` and the portfolio warmup and market-order
settings, are regular fields in the policy cards.

PB9 metrics have their native names and units. CAGR is not PB8 ADG. Review goals
and thresholds before comparing generations. Scenario presets select native PB9
objectives and limits. Like PB8, **Additional Parameters** lists entries from
the config's `optimize` section that have no dedicated control, including new
fields from the installed Passivbot template. Edit values directly in the list
and use the normal **Save** action.

The native template's `scenario: null` scoring and limit entries are valid with
Suite Mode off and remain unchanged when saved. Named scenarios require Suite
Mode and a matching scenario label.

In Suite scenarios, `coins` and `ignored_coins` can be a coin list, a text value
(for example a coin-list file) or separate Long/Short selections. The editor
keeps the saved form. Use the selector next to each field to change the form.
**Inherit (base config)** removes the override. An empty list, empty text or
empty side is saved as an explicit empty override, so it differs from Inherit.
With Long/Short, set a side to **Not set** to omit it.

## Metric reference

This reference covers the current **27 native selectors**: 26 quantities, with
separate Long/Short selectors for position utilization. The picker dynamically
reads the installed PB9 catalog. These explanations follow that installation's
`docs/metrics.md` and Rust core metric catalog; older results can use a different
definition version.

### Units, objectives and limits

- **Strategy equity** includes initial trading capital, realized trading PnL,
  fees/rebates and unrealized trading PnL. Direct collateral appreciation is
  excluded. With USD-only collateral it equals the reported USD equity.
- **Fraction** uses scale one: `0.05` means 5%, and `1` means 100%.
  **Ratio** is dimensionless. Durations use the hours/days named by the selector.
  Annualization uses 365.25 calendar days.
- **Maximize/minimize** below recommends an objective direction; **upper/lower
  bound** recommends a limit. PB9 permits explicit objective directions and both
  limit directions for every numeric metric. Adding objectives changes the
  optimization and Pareto front. Risk-only goals can favor inactivity: combine
  them with suitable growth or activity requirements.
- **Goal** and **Reducer** differ. For Ulcer, `goal: min` with `reducer: max`
  minimizes the worst source/scenario value. Mean, min, max, std and median are
  aggregation choices, not additional metric names. A named-scenario objective
  selects that scenario instead of a suite aggregate.

### Growth and risk-adjusted performance

| Metric | Meaning | Unit | Typical use |
| --- | --- | --- | --- |
| `cagr_fraction_strategy_equity` | Compounded annual growth over the actual evaluated period: `(end / start)^(365.25 / days) - 1`. | Fraction (annualized) | Maximize growth. |
| `cagr_fraction_strategy_equity_ew` | Annualized log growth with exponential recency weighting; recent intervals count more. | Fraction (annualized) | Maximize recent-weighted growth. |
| `drawdown_max_fraction_strategy_equity` | Largest decline from the running equity high. | Fraction | Upper bound on maximum drawdown. |
| `ulcer_index_fraction_strategy_equity` | Root mean square (RMS) of drawdowns; captures depth and time underwater. | Fraction | Minimize persistent/deep drawdowns. |
| `pain_index_fraction_strategy_equity` | Mean drawdown over all observations, including flat periods and zero drawdowns. | Fraction | Minimize average drawdown burden. |
| `volatility_annual_fraction_strategy_equity` | Sample standard deviation of daily equity returns, annualized. | Fraction (annualized) | Upper bound on return variability. |
| `sharpe_ratio_strategy_equity` | Mean daily return divided by sample standard deviation, scaled by `sqrt(365.25)`. | Ratio | Maximize return relative to total variability. |
| `sortino_ratio_strategy_equity` | Mean daily return relative to downside deviation, scaled by `sqrt(365.25)`. | Ratio | Maximize return relative to negative-return variability. |
| `omega_ratio_strategy_equity` | Sum of positive daily returns divided by the magnitude of negative daily returns; not annualized. | Ratio | Maximize positive versus negative daily returns. |

Daily-return metrics use complete UTC daily-close returns. PB9 subtracts no
risk-free rate; Sortino and Omega use a zero-return cutoff. Sortino includes all
daily returns in its downside mean, with nonnegative returns contributing zero.
CAGR is not PB8 ADG. For CAGR EW, omitted `backtest.ew_half_life_days` defaults
to 360 days; explicit `null` uses the native 180-day fallback. There are no EW
Sharpe/Sortino/Omega or automatic USD/BTC copies of these metrics.

### Coverage, activity and holding time

| Metric | Meaning | Unit | Typical use |
| --- | --- | --- | --- |
| `backtest_completion_fraction` | Covered evaluation duration / intended duration, clipped to `[0, 1]`. | Fraction | Lower bound, commonly `1`, for full coverage. This does not exclude liquidation at the end. |
| `fills_gap_max_days` | Longest portfolio-wide no-fill gap, including evaluation boundaries. | Days | Upper bound on long inactive intervals. |
| `fills_gap_time_weighted_mean_hours` | Duration-weighted no-fill gap: `sum(gap_hours²) / sum(gap_hours)`. Long gaps weigh more. | Hours | Minimize prolonged inactivity. |
| `peak_recovery_max_days_strategy_equity` | Longest underwater episode until regaining the previous high; an unfinished episode ends at evaluation end. | Days | Upper bound on recovery time, or minimize it as an objective. |
| `position_held_max_days` | Longest coin/position-side open-to-flat episode, including still-open positions at evaluation end. | Days | Upper bound on maximum holding time. |
| `position_held_time_weighted_mean_hours` | Duration-weighted holding time: `sum(holding_hours²) / sum(holding_hours)`. Additions and partial closes do not restart the clock. | Hours | Minimize long holding episodes. |
| `gross_turnover_ratio_per_day` | Entry and exit notionals divided by account balance immediately before each fill, then divided by elapsed days. | Per day | Maximize only when turnover is desired, or set an activity bound. |

Without fills, both gap metrics equal the actual evaluated duration in their
respective units. Without positive-duration holding episodes, both holding
metrics are zero. An unfinished recovery is not proof of eventual recovery.
Turnover counts both entries and exits and includes inactive days: `0.5` means
half an account balance of gross traded notional per day at unchanged balance.
More turnover is not inherently better.

### Exposure and profit composition

| Metric | Meaning | Unit | Typical use |
| --- | --- | --- | --- |
| `gross_exposure_rms_ratio_strategy_equity` | RMS of total absolute marked position notional / strategy equity. Long and Short add. | Ratio | Minimize sustained gross exposure. |
| `gross_exposure_max_ratio_strategy_equity` | Maximum of that gross-exposure-to-equity ratio. | Ratio | Upper bound on peak exposure. |
| `position_exposure_utilization_rms_ratio_long` | RMS Long utilization relative to nominal position/side WEL/TWEL budgets, excluding excess allowance. | Ratio | Minimize or bound Long budget utilization. |
| `position_exposure_utilization_rms_ratio_short` | The same budget-relative utilization for Short positions. | Ratio | Minimize or bound Short budget utilization. |
| `loss_profit_ratio` | Negative net realized fill cashflow magnitude / positive net realized fill cashflows, including fees/rebates; excludes unrealized PnL. | Ratio | Minimize realized loss relative to profit. Above `1` means net realized loss. |
| `short_net_profit_contribution_fraction` | Net Short strategy profit / positive combined Long+Short strategy profit, including unrealized PnL changes and trading costs. | Fraction | Lower/upper bounds for the desired directional profit mix. |

Position utilization measures budget use, not absolute gross exposure. Short
contribution requires positive combined net profit. It can be negative or above
`1` if one side loses; it is not clipped to 0–100%. Bounds `0.1`–`0.9` require
each side to contribute at least 10% of positive combined net profit.

### HSL behavior and losses

| Metric | Meaning | Unit | Typical use |
| --- | --- | --- | --- |
| `hsl_panic_episode_loss_max_fraction` | Largest executed HSL episode loss / balance before its first HSL fill. | Fraction | Upper bound on a single HSL episode loss. |
| `hsl_panic_episode_loss_mean_fraction` | Mean of those episode loss fractions, including zero-loss executed episodes. | Fraction | Upper bound on average HSL episode loss. |
| `hsl_execution_episode_count` | HSL episodes with at least one fill from an order generated by that episode; multiple fills in one episode count once. | Count | Lower bound such as `1` when a window must exercise HSL execution. |
| `hsl_triggers_per_year` | Observed HSL trigger count annualized to 365.25 days. | Per year | Bound the frequency of HSL triggers. |
| `hsl_red_time_fraction` | Union of observed RED time across enabled controllers / observed HSL time; includes enforced restart cooldown, excludes warmup. | Fraction | Upper bound on time under HSL RED conditions. |

A trigger without an executed HSL fill is not an execution episode. Episode
losses sum negative net fill cashflows; profitable fills do not offset them.
Unfinished executed episodes are included through evaluation end. Coin/position-
side RED time does not imply that the whole bot was inactive. Core definition
version 2 also counts fills attributable to an HSL order after the controller
recovers to GREEN; older results can use different semantics. More executions
or triggers are not inherently better.

### Availability and saved optimizer results

The optimizer computes selected objectives, enabled limits and necessary
validity facts. Its default profile therefore saves far fewer than 27 metrics.
A standalone backtest produces the core report. Adding objectives or active
limits affects optimization; it is not a display-only option.

Missing support, invalid inputs and undefined ratios are not zero. PB9 records
availability/status metadata. A selected objective or enabled limit requiring
an unavailable value can make the candidate infeasible. Some metrics need
one-minute observations or enough complete daily returns; check those
requirements when changing candle interval. Historical results retain their
original definition and evaluated period.

## Configuration prefilter

**Configuration prefilter** edits the native `optimize.prefilter` settings.
Use **Add check** to choose a metric, enable the rule, select Long and/or Short
and set optional minimum and maximum thresholds. An enabled rule requires at
least one threshold and the trailing_martingale strategy. **Remove check**
deletes a rule. With no enabled rules the prefilter is inactive.

Set probe assumptions using the numeric fields. Exposure fractions support
**Add exposure fraction** and **Remove fraction** and must contain 1–64 values
from zero up to 0.999, exclusive. The assumptions describe hypothetical
configuration probes. Save uses the same action as the rest of the editor.
Unedited inherited defaults remain inherited. Percentage thresholds use
fractions, so `0.10` means 10%.

**Run Settings** exposes the installed optimizer override switches,
including `mirror_short_from_long` and `couple_unstuck_ema_spans`.
`fine_tune_params` uses PB9 selectors such as `global.risk`, `long.forager`,
`*.strategy.close` or a leaf name such as `we_excess_allowance_fraction`.

The GPU backend uses the same `population_size` and Pymoo/NSGA settings as the
pymoo backend, so both sections stay visible. **Screening scenarios** lists the
Suite scenario labels. Check the scenarios for the partial GPU screening stage;
select none to disable partial screening. A saved label that no longer exists in
the Suite is marked `(not in Suite)` and must be unchecked before saving.

## Candles and execution

PBGui Market Data is the default candle source and its path is shown in new drafts. Changing that queue setting
allows the config's own source. Candle files are referenced directly. Backtest and Optimize use the same persistent native cache under the configured
PB9 installation (`caches/`), like PB8. Downloads, coverage facts and prepared
data remain reusable across runs. PBGui candles stay in place.
OHLCV Readiness and 1st/All date lookup use the native PB9 tools. They do not
copy or preload candles; use **Market Data** to acquire missing history.
Offline mode requires complete local candles and public market metadata.

1st/All works directly on unsaved drafts and uses the latest valid editor inputs.
Coin pickers and side JSON stay synchronized; if you change the configuration
while a lookup is running, its older result is not applied.

Save, Save & Queue, Autostart, CPU overrides, Pareto seeds and checkpoint resume
use the shared workflow. PB9 verifies source commit, schema and immutable inputs
before starting. Queueing, starting and resuming require a PB9 checkout without
local changes; saved results stay readable when the checkout has local changes.
**Services** shows the **PB9 Optimize Queue** controller, which you can stop and
start there like the PB8 queue. Restarting the API reconstructs its controllers while detached
optimizer jobs continue. Stop acts only on a verified owned process.

The queue's estimated coin candles per candidate uses PB9's resolved training
data plan and the shared candle-count calculation. It counts training windows
once, excluding Holdouts, warmup and iteration/population multipliers. Dynamic
coin selections or unresolved dates show **—** instead of a guessed count.

## Native launch options

These controls use the shared execution settings, without editing Raw JSON:

- **Run scenarios** selects a subset of Suite labels (`--scenarios`). No selection
  runs all configured scenarios. Labels update with Suite edits; removed labels
  must be corrected before saving or queueing.
- **Filter starting configs** applies the final limits to the saved metrics of
  starting artifacts (`--filter-starting-configs`); it does not recalculate them.
- **Maximum starting configs** limits the seed population
  (`--compress-starting-configs N`). Leave it empty for no compression.
  Filtering and compression require **seed_mode self or path**, not a fresh run
  or checkpoint resume. Metric filtering is useful for saved Pareto artifacts.
- **Preparation manifest** imports the completed JSON report produced by
  `passivbot download --for-optimize`, up to 4 MiB. A preparation plan is not a
  completed report. PBGui stores the report as a private, hash-addressed attachment under
  `data/opt_v9_manifests`; configs and browser drafts keep only its reference.
  Each queued job freezes the imported bytes independently and passes
  `--preparation-manifest` to PB9. PB9 verifies that its actual prepared inputs
  match the report before evaluation. This report is optional and does not
  replace the candle source or copy candle data. Use **Remove manifest** to
  stop requiring that report.

Only flags advertised by the installed PB9 CLI are accepted. Queue snapshots
retain the chosen options. Checkpoint Resume restores the scenario subset and
manifest from result-owned metadata; seed filtering/compression do not apply to
checkpoint continuation. Old jobs without these options retain their defaults.

## Results and archives

Use the shared Results/Paretos views for metrics, candidate JSON, seed selection,
3D plots, Pareto Dash, Pareto Explorer and native PB9 backtest drafts. The Results
**Strategy** column shows the native `bot.portfolio.strategy_kind`. Pareto Explorer
is also available as **PBv9 → Pareto Explorer**; its optimize presets use native
PB9 metrics and keep the run's limits. Archive export/import uses
the existing archive transaction and collision dialog, selecting the PB9 slot.
Native source policies and unknown authored fields remain intact.

**Queue Backtest** and **Queue Validation** add PB9 candidates to the PB9
Backtest queue. **Open Queue** opens that same PB9 queue; its status and
Autostart information belong to PB9. Submitting candidates keeps the current
Optimize view and selection in place.

Saved Pareto files contain evaluation metadata as well as configuration. PBGui
uses the installed PB9 result reader to extract a valid authored config before
opening or queueing a candidate. Runtime fields such as `backtest.coins` are not
passed as authored settings. The saved result, selected policy and dates remain
unchanged.

### Sweep Cycles

PB9 has no gain metric. Sweep Cycles derives each window's growth from the
per-window `cagr_fraction_strategy_equity` and the actual evaluated period that
PB9 records for that window (`suite_metrics.sources`), not the planned scenario
dates. A run that ended early therefore uses its real duration. The Sweep
preset uses this metric as its first objective. If you change the objectives,
keep `cagr_fraction_strategy_equity` as an objective or limit so PB9 records it
for every window.

Sweep is shown as unavailable when a window's gain cannot be restored exactly:
the evaluated period is missing, the window averages several exchanges, CAGR is
-100% or so close to it that the result would be inaccurate, or the values
exceed the number range.

### PB8 migration

On the **PBv8 → Optimize** page, **V9** next to a saved config or Pareto config
opens the official PB8 → PB9 converter. Review the proposed changes and
warnings, then open the result as an unsaved PB9 Optimize draft. The PB8 source
stays unchanged. PBGui run settings (starting configs, fine-tune, polish and
execution) are not migrated. The converter supports PB8 schemas v8.0.0, v8.1.0
and v8.6.0.

## Optimizer Settings

Queue Settings controls Autostart, CPU allocation and the default PBGui candle source. Run Settings controls optimizer iterations, backend, seed, fine-tuning, polishing and optional stagnation detection. Native inherited values stay unpinned until edited.

## Vast.ai

The account, marketplace, calibration, rental automation and controls are shared with PB8. Native Performance History updates automatically and remains readable after job cleanup. PB9 requires its own
pinned worker image and native GPU contract. The published PB9 image is selected
by its immutable digest, independently of PB8. Before renting, PBGui verifies
that the exact digest is publicly downloadable without GitHub credentials.
Missing or unverified images block rental; PB9 never falls back to a PB8 image.
Building an image locally does not publish it, rent a GPU or deploy to a host.
A physical GPU test is required before claiming a verified cloud execution path.

GPU optimization has passed a synthetic RTX 3090 smoke test, including upload
and result import. PB9 performance tuning uses a separate native-cohort
measurement (protocol 5) through the shared test controls. Each population/batch
probe runs in a fresh process, excludes its first compilation cohort and requires
at least five minutes of fully completed GPU cohort measurements. Failed,
prefiltered and cached-only cohorts cannot establish a recommendation. The rate
includes host preparation and result handling; it is not a CUDA-kernel-only rate.
The result lists GPU rows, requested batch width and completed cohorts. It does
not claim a measured dispatch work cap. PB8 retains its own measurement protocol.

The published native-calibration worker supports protocol 5 and is pinned by
its verified public digest. Small tests compare populations 128 and 256. Native
reference workloads use 2 coins / 3 days (Small), 3 coins / 7 days (Medium), or
3 coins / 14 days (Large); PB8 reference workloads remain unchanged. A matching
job snapshot measures its own workload instead. Physical GPU verification must
complete before a measured profile can be accepted.
A synthetic RTX 3090 calibration verified population 128 with ten completed
cohorts over 316 seconds. The 256 probe reached the rental collection reserve
before its full window and was excluded; this does not establish an optimum
for other workloads. Accepted PB9 profiles are separate from PB8. Automatic application requires the
same workload, allocated CPU worker count, source/image and measured GPU identity and both population and
batch set to Auto with tuning enabled. Explicit values remain authoritative;
the profile does not replace the dispatch work cap.

Native cloud bundles include the same launch options and managed seed artifacts.
Host paths are replaced by verified bundle paths. Only the selected scenarios'
training inputs are exported. Local checkpoints use local Resume; cloud Requeue
starts a new job. The native worker is pinned to public source `bf7329b03`; the local
PB9 source must match that worker before preparation. The existing local
installation is not updated implicitly. An imported preparation report may reject
cloud inputs when their dataset identity differs; the worker never bypasses
PB9's report verification.

The separate AI Loops subsystem is not enabled for PB9 yet. Strategy Explorer
remains a separate integration step.

The PB9 cloud worker starts through the official native CLI, preserving prepared
configuration reuse. Search mode is not changed automatically: Max can improve
CPU preparation throughput but changes the search schedule. PB8 GPU proxy rates
are not directly comparable to complete PB9 GPU evaluations.

The current worker pins official v9 source `bf7329b03` (including merged PR #2305) and includes the Hyperliquid
numeric market-ID age fix. The published image passed offline age and native
CLI/cohort probes; this revision has no new physical GPU performance measurement.
