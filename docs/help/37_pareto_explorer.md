# Pareto Explorer (PBv7)

Pareto Explorer helps you analyze PBv7 optimize results, compare tradeoffs, shortlist configs, and create follow-up Optimize presets. It is designed for multi-objective results where no single metric is the whole answer.

## Where to Open It

- PBGui: **PBv7 -> Optimize -> Results**.
- Open a result with **Pareto Explorer** from the Optimize results table or result sidebar.
- The page can start in fast pareto-only mode and later load the full `all_results.bin` dataset from the sidebar.

## Core Idea

Every score, chart, and Pareto star is relative to the currently loaded and visible config set.

- Fast mode compares mainly the Passivbot pareto JSON candidates.
- Full mode compares against the wider `all_results.bin` candidate set.
- Display Range changes the visible slice, so rankings and visible Pareto stars can change.
- Treat rank and score as shortlisting signals, not as final live-trading decisions.

## Overview

Overview is the decision dashboard. Use it first after loading a result.

- **Top Champions** shows the strongest candidates in the current visible slice.
- **Insights** highlights obvious signals such as parameter-bound pressure or style diversity.
- **Pareto Front Preview** shows the shape of the current tradeoff frontier.
- **Robustness vs Performance** shows whether return is supported by consistency.
- The selected config details appear below the charts when a config is selected.

Recommended flow:

1. Scan Top Champions.
2. Click a champion or chart point.
3. Review Metrics, Trading Style, Robustness, Scenario Metrics, and Full Configuration.
4. Use **Run Backtest** before trusting a candidate.
5. Use **Create PBv7 Optimize Preset from this Config** only after the config looks worth refining.

## Explorer

Explorer is for interactive tradeoff analysis.

- **Visualization** changes between 2D scatter, 3D scatter, 3D projections, and radar charts.
- **Quick Views** pick useful metric combinations for common decisions.
- **Custom** lets you choose X, Y, and optionally Z metrics manually.
- **Color by** adds one more metric dimension through point color.
- **Show all configs** compares the selected candidate against the full visible cloud instead of pareto points only.
- **Performance Priority**, **Risk Aversion**, and **Robustness Importance** drive the Best Match helper.

Use Explorer when you need to answer questions like:

- Is this config really on a good frontier, or just good by one metric?
- Which nearby config gives up little profit but reduces risk a lot?
- Does a radar candidate have a balanced profile or one extreme strength hiding weaknesses?

## Deep Intelligence Parameters

Parameters Intelligence explains how the optimize search behaved around parameter values.

- **Parameter Influence Heatmap** shows correlations between variable parameters and performance metrics.
- **Parameters Near Bounds** shows parameters close to their optimize bounds.
- **Top N Parameters** controls how many parameters are shown.

Use this tab before creating a follow-up preset. Parameters near bounds are good candidates for refinement because the optimizer may have wanted to search farther in that direction.

## Deep Intelligence Scenarios

Scenario Analysis compares the visible config set across loaded backtest scenarios.

- The metric selector chooses the value used for the scenario boxplots and statistics.
- The chart and statistic cards are aggregate views over the visible config set.
- This tab does not represent a single selected config; it shows how the visible population behaves under different scenarios.

Use it to avoid selecting a config that only looks good in one narrow scenario.

## Deep Intelligence Evolution

Optimization Evolution shows whether the optimize run was still finding meaningfully better configs over time. It needs Full Mode because the fast pareto JSON files do not preserve the original `all_results.bin` config index.

- **Metric** chooses the timeline value.
- **Show all configs** switches between pareto-only points and all visible configs.
- **Hide liquidation outliers** keeps extreme values from crushing the chart scale.
- **Meaningful Improvement (%)** ignores tiny best-so-far changes so noise does not look like progress.
- The blue **Best So Far** line shows the best value found up to each point.
- Clicking a point in Full Mode selects that config for inspection below the chart.

In Fast Mode this tab shows a hint instead of a chart. Use the sidebar **Load all_results** button when you need the real timeline.

Use the summary to decide whether another run is likely to help:

- **Last meaningful improvement** near the end suggests the search may still be productive.
- **Final 20% improvement** near zero suggests the run was already flattening out.
- **Suggested minimum iterations** gives a practical next-run target based on where the last meaningful improvement occurred.

## Deep Intelligence Correlations

Multi-Metric Correlation compares several configs across normalized risk/profile dimensions.

- **Selection Strategy** chooses how configs are picked: Top Performers, Diverse Styles, or Risk Spectrum.
- **Configs** controls how many traces appear in the radar.
- Weighted and BTC toggles choose the preferred metric variants when available.

Use it to compare candidate shapes quickly. A balanced radar is usually easier to validate than a config that wins one axis and loses several others.

## Settings and Loading

Settings controls what data is loaded.

- **Result Path** is the optimize result directory or pareto directory.
- **Max Configs** limits the fast loaded subset.
- **Load Strategy** controls how candidates are selected when loading a subset.
- **Persist defaults** saves the current loading preferences.
- Use the sidebar **Load all_results** button for full mode.
- Use **Show Passivbot Paretos** to switch back to fast pareto-only mode.

If the UI feels slow, reduce Max Configs or work in pareto-only mode until you know which part of the result is worth deeper inspection.

## Optimize Preset Refinement

The preset panel creates a PBv7 Optimize config from the selected Pareto config.

- Choose **Optimization goal** first. The default Balanced option keeps the run scoring.
- Keep the generated **Preset name** unless you need a custom name.
- Keep **Only adjust parameters near optimize bounds** enabled for normal refinement runs.
- Use **Bounds window (%)** to tighten search bounds around selected values.
- Use **Risk adjustment** to tighten or relax risk-related bounds and limits.
- **Create Optimize Preset** saves the config and opens Optimize.
- **Create & Queue** saves and queues it without opening Optimize.

Use a small bounds window first. A tight window is useful for refinement, but too much tightening can hide better nearby regions.

## Best Practices

1. Start in Overview, not Deep Intelligence. First identify candidates worth studying.
2. Load `all_results.bin` before making final decisions if it is available.
3. Use Display Range intentionally. A config that is strong in the top 500 may look ordinary in the top 5000.
4. Prefer balanced candidates with acceptable risk over the absolute highest profit point.
5. Always validate selected configs in Backtest before using them as a live candidate.
6. Use Deep Intelligence Parameters before creating a follow-up Optimize preset.
7. For refinement presets, adjust near-bound parameters first and keep bounds changes modest.
8. Compare at least two nearby alternatives before deciding. The best live candidate is often not the highest ranked point.

## Related

- PBv7 Optimize: create and queue follow-up optimize runs.
- PBv7 Backtest: validate a selected config before trusting it.
- Strategy Explorer: inspect one config visually after narrowing the shortlist.
