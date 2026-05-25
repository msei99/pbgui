# Pareto Explorer (PBv7)

Pareto Explorer is an interactive UI for analyzing PB7 optimization results (multi-objective search). It helps you find robust configs, compare tradeoffs, and export candidates.

## Where to open it

- PBGui: **PBv7 → Optimize → Results**
- Click **🎯 Pareto Explorer** on an optimization result.

## What it needs

A PB7 optimization results folder, typically containing:

- `pareto_front.json` (or similar Pareto JSONs)
- `all_results.bin` (optional but recommended for full exploration)

If `all_results.bin` is missing, Pareto Explorer will run in a **fast mode** with limited views.

## How to use it

### 1) Start in fast mode
Fast mode loads only Pareto JSONs first, so the UI opens quickly.

- Use it to spot promising configs early.
- If you need the full dataset (all candidates), enable full load.

### 2) Load all results (full mode)
Full mode loads `all_results.bin`.

- More configs available
- More reliable filtering and selection
- Can be slower depending on file size and disk speed

### 3) Explore tradeoffs
Common workflows:

- Find configs with best **profit vs drawdown** tradeoff
- Filter by **stuck time**, **exposure**, or other safety metrics
- Compare a handful of top candidates

## Overview page

The first page in Pareto Explorer is **Overview**. Treat it as a quick decision page:

- **Top Champions** gives you the current top 5 candidates for the loaded slice.
- **Insights** highlights obvious signals such as parameter-bound pressure or style diversity.
- **Pareto Front Preview** helps you visually understand the shape of the current candidate set before drilling deeper.

For most runs, the normal flow is:

1. Scan the Top Champions list.
2. Click a champion or a point in a chart.
3. Read the selected config details below.
4. Open the selected config in Backtest from the sidebar when it looks promising.

## How to read Top Champions and scores

Top Champions is a **ranking helper**, not a guarantee that rank 1 is always the best config for your live goals.

- **Score** is the explorer's composite ranking score for the currently visible slice.
- Higher **Score** is better within the same loaded view.
- The score is best used to shortlist candidates, not as a single final truth.

The supporting chips help you understand *why* a config ranks where it does:

- **Perf**: return/performance strength. Higher is generally better.
- **Rob**: robustness/consistency quality. Higher is generally better.
- **Risk**: risk pressure from drawdown/choppiness/tail-risk style metrics. Lower is generally better.

Practical rule:

- Start with the highest **Score** configs.
- Prefer configs where **Perf** and **Rob** are both strong.
- Be careful when a config wins mainly on performance but still shows clearly worse **Risk** than nearby alternatives.

## How to read the preview charts

The two charts on Overview are quick visual summaries of the currently visible config set.

### Pareto Front Preview (left)

This chart plots two key metrics against each other for the currently visible configs.

- Each point is one config.
- Star-marked points are Pareto members in the current visible slice.
- The highlighted star marks the currently selected config.
- The color scale is an extra metric, used to add another dimension at a glance.

How to interpret it:

- Look for the outer edge/frontier of points rather than dense middle clusters.
- A config on the frontier is interesting because improving one axis would usually worsen another.
- Nearby frontier points are often your real decision candidates.
- If one candidate is only slightly better on one axis but much worse on the other, it is usually not the better trade.

### Robustness vs Performance (right)

This chart answers a simpler decision question: how much performance are you getting for the amount of robustness?

- **X axis**: performance metric. Further right is better.
- **Y axis**: robustness score. Higher is better.
- The dashed lines mark the current average split.

Interpret the quadrants like this:

- **Top right**: strong performance and strong robustness. Usually the best hunting ground.
- **Top left**: stable but slower. Good when safety matters more than absolute returns.
- **Bottom right**: fast but fragile. These need extra caution.
- **Bottom left**: usually weak candidates unless they serve a very specific purpose.

## Important scoring caveat

Scores and chart positions are always relative to the **currently loaded and currently visible** set.

- In **fast mode**, you are mainly comparing Pareto JSON candidates.
- In **full mode**, you are comparing against the larger `all_results.bin` sample.
- When you change **Display Range**, rankings and visible Pareto stars can change because the comparison slice changed.

So the right question is not only "Which config has the best score?" but also:

- "Does this config still look strong when I compare it against the wider loaded set?"
- "Is it strong because it is balanced, or only because one metric dominates?"

### 4) Export candidates
Once you have a shortlist:

- Export config JSONs to test them in backtests.
- Keep notes on which filters/metrics produced the best candidates.
- Use **Run Backtest** on a selected config to open that exact config directly in the FastAPI Backtest editor.

## Tips

- If the UI feels slow, reduce the number of displayed configs or filters.
- Use consistent time ranges and exchanges when comparing multiple optimize runs.

## Related

- Strategy Explorer: great for visual debugging of a single config.
