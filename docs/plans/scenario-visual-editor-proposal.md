# Visual Scenario Editor — proposal

Status: approved and implemented in the working tree. The reference chart reads daily local OHLCV; full configured-data readiness uses the existing OHLCV Readiness panel after applying windows.

## Current implementation

`scenario_templates.py::generate_scenario_template` constructs equal-length windows with a shared stride. It places holdouts after all training windows and rejects holdouts for rolling_windows. The walk_forward template does not orchestrate repeated optimizer runs; its default reducer is median rather than rolling_windows mean. `sweep_cycles.py::sweep_holdout_scenarios` already accepts explicit holdout dates. Market Data already exposes OHLCV chart/data coverage components.

## Proposed interaction

Use a wide expandable Scenario Editor with a shared time axis: local OHLCV chart above draggable Training and Holdout lanes, with overlapping training windows arranged on separate sublanes. Drag window bodies to move, edges to resize, or draw a new interval. Offer exact date inputs, role selection, duplicate/delete, undo/redo and day snapping. A reference coin/exchange selector changes the chart only, not optimizer scope. Display actual source and missing-data coverage; aggregate chart data to the visible scale without changing evaluation resolution.

One explicit window list is authoritative, with stable IDs, labels, roles and dates. Existing rolling and walk_forward templates become quick generation presets; generated windows remain individually editable. Applying only exports Training to PB8 scenarios and saves Holdout metadata for validation. Preserve reducers and scoring independently of the preset. Keep Sweep accounting as a separate evaluation policy with its chronology/non-overlap constraints.

Holdouts may appear anywhere. Block Training/Holdout overlaps for the same effective dataset; do not silently trim or split a training window. Offer an explicit split/exclusion action. Training/Training overlap is allowed with a visible reuse notice. Check data availability across all configured coins/exchanges, distinguishing a reference chart from full coverage validation.

Distributed holdouts measure excluded-period robustness, not necessarily chronological forward performance. True walk-forward requires separate optimizer runs whose training precedes each associated test. Windows chosen using historical prices also constitute experiment design; reserve a final untouched test when making out-of-sample claims.

## Delivery

1. Versioned explicit window plan and compatibility loading of existing templates, including custom holdout validation/grouping.
2. Shared timeline editor and local OHLCV integration, responsive rendering and server-side date/coverage validation.
3. Regression checks for drag/resize, boundaries, overlap, multiple holdouts, round-trip save/load, training-only export, grouping and stale chart requests.

No PB8 bot changes are expected for explicit date windows using the existing scenario format. A true repeated walk-forward optimizer would be a separate feature.
