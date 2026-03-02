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

### 4) Export candidates
Once you have a shortlist:

- Export config JSONs to test them in backtests.
- Keep notes on which filters/metrics produced the best candidates.

## Tips

- If the UI feels slow, reduce the number of displayed configs or filters.
- Use consistent time ranges and exchanges when comparing multiple optimize runs.

## Related

- Strategy Explorer: great for visual debugging of a single config.
