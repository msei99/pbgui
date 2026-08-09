# Strategy Explorer - Help

Strategy Explorer is the shared visual debugging and analysis GUI for PB7 and PB8 strategies. Both versions use the same page shell and workflow; version-specific engines, fields, labels, and unavailable controls adapt automatically.

- A **Snapshot** at a chosen **Analysis Time**.
- A bounded **Simulation** or native replay with fills.
- A **Compare** workflow for stored results and fresh calculations.
- A **Movie Builder** for time-stepped replay.

PB7 keeps its existing local/PB7-engine behavior. PB8 uses native PB8 calculations and candle preparation.

---

## Core concepts

### Exchange / Coin
Strategy Explorer always works on one market:

- **Exchange**: for example `bybit`
- **Coin**: a market available to the selected config and local engine data

PB7 uses its configured local OHLCV sources. PB8 uses **PB8 native candles** and rejects an explicit OHLCV directory outside the approved PB8/PBGui roots. Without suitable candles, Strategy Explorer renders an explicit unavailable snapshot shell with the handed-off config and tuning fields, but it cannot calculate native ideal orders or replay the selected window.

Exchange and Coin are analysis selectors. Changing them does not rewrite the config's exchanges or its potentially different Long and Short approved-coin lists.

### Analysis Time (the most important control)
**Start Date** and **Start Time** select the first candle of the snapshot context window. **Analysis Time** is the right-edge candle where the snapshot is calculated.

- **Chart Context** extends forward from the selected start.
- The candle at the right edge of that bounded window supplies the snapshot price, indicators, and order state.
- PB7 retains its existing local/PB7-engine state behavior.
- PB8 calculates native ideal entry orders from a supplied **flat position**.
- PB8 calculates close orders separately from a **representative hypothetical position** at the selected price.

The PB8 snapshot explains strategy behavior for those supplied states. It is not a forecast of a live account, its positions, or its future orders.

### Context window
The chart shows the bounded window starting at Start Date/Start Time:

- **Chart Context** controls how much forward candle history is displayed and where the snapshot state is calculated.

---

## Variants / modes

### 1) Snapshot (single view)
The shared snapshot renders the entry and close orders, reference lines, and strategy parameters returned for the selected version.

PB8 derives tuning groups, field types, choices, and ranges from the installed PB8 runtime. Edits are written back to canonical nested PB8 paths while preserving the complete handed-off config.

For PB7, this is the existing PB7/Rust calculation view. For PB8, entry and close output comes from the two supplied-state calculations described above. PB8 close orders therefore illustrate one representative position, not the historical or live account state.

This mode helps answer questions such as:

- "How do these parameters shape entries and closes?"
- "Why is an order level tight or wide?"
- "How does changing one strategy parameter affect the snapshot?"

### 2) Simulation / native replay
The **Simulation** stage walks a selected candle window and records fills.

- **PBGui Simulation** is the existing PB7 local candle-walk.
- **PB7 Backtest Engine** uses the existing PB7 engine path.
- **PB8 Native Replay** runs a native PB8 backtest in memory without writing a result folder.

PB8 replay is deliberately bounded by the selected window and server limits: at most 20,000 replay candles and 2,000 displayed fills. It starts from PB8's native flat state; manual starting positions are unavailable because the native replay API does not accept them. It is historical replay, not live-account forecasting.

### 3) Compare
PB7 retains both existing compare choices:

- **PB7 Backtest Result vs PBGui Simulation vs PB7 Backtest Engine**
- **PBGui Simulation vs PB7 Backtest Engine**

PB8 provides **Stored PB8 Result vs Fresh PB8 Replay**. A PB8 result handoff keeps its validated result location server-side behind an owner-bound opaque draft ID; the browser does not receive or edit that path. Compare reads the stored fills and runs a fresh bounded native replay for the handed-off config and window. Compare refuses to report success unless a validated stored result or a runtime-distinct pinned config is present, and warns when the candle limit covers only part of a stored fill range.

### 4) Movie Builder
PB7 retains its three existing engines:

- **PBGui Simulation**: local replay with evolving grids and fills.
- **PB7 Backtest Engine**: PB7-engine fills/upcoming-fill view.
- **PB7 fills.csv (from backtest)**: visualization of recorded result fills without recomputation.

PB8 offers **PB8 Native Replay**. Its movie uses real step-aggregated candles and fills returned by the native PB8 replay. PB8 upstream does not expose a historical ideal-order trace for every frame, so PBGui cannot show exact historical resting entry/close ladders per candle. Empty per-frame order ladders are intentional. Fill-derived position annotations are available only through the displayed fill range and stop once the fill-display limit is reached.

---

## Direct PB8 handoffs
You can open the shared PB8 Strategy Explorer directly from:

- **PB8 Run**
- **PB8 Backtest**
- PB8 **Backtest Results**
- PB8 **Pareto Explorer** results

These handoffs pass the canonical config and applicable overrides through an authenticated opaque draft. If referenced sparse overrides cannot be loaded, PBGui blocks the handoff instead of opening an incomplete config. The PB8 Backtest Results handoff also retains validated stored-result provenance for **Compare** without exposing a filesystem path in the page or URL. It initially selects the result dataset's validated source exchange, the first approved coin with stored fills, and that fill's UTC time; validated dataset metadata is the fallback.

PB8 parameter fields within 5% of their active Optimize lower or upper bound are marked directly in the tuning panels. If the config has no Optimize bound for a field, the installed runtime's parameter range is used. To compare two Pareto candidates, select the first candidate, click **Pin Explorer Baseline**, select a different candidate from the same result, and open **Strategy Explorer**. The page-local baseline is cleared when the result changes or the page reloads. The owner-bound draft carries both configs and **Compare** runs both through the same native PB8 replay contract.

PB8 Strategy Explorer drafts belong to the current authenticated session and expire after 10 minutes without use; an API restart clears them immediately. Browser requests use the same-origin HttpOnly session cookie, never a rendered or stored bearer token. At most two native PB8 helper operations run concurrently. Runtime update contention or occupied helper slots return a retryable busy response. Changing config or operation controls supersedes older browser requests so late results cannot overwrite the current selection.

---

## Long/Short grid display (how to read it)
Strategy Explorer can show Long and/or Short depending on the config.

### Long
- **Long entry grid**: buy orders that open or increase a Long position.
- **Long close grid**: sell orders that reduce or close a Long position.

### Short
- **Short entry grid**: sell orders that open or increase a Short position.
- **Short close grid**: buy orders that reduce or close a Short position.

### Both sides active
When both Long and Short are enabled:

- Snapshot can show output for both sides.
- Movie Builder offers **Side** values `Auto`, `Long`, and `Short`.

Remember that PB8 snapshot closes use a separate representative hypothetical position for each side.

---

## Common gotchas

### "I don't see any orders/markers"
Check the selected market, side, and time window first:

- Analysis Time or the movie window may not overlap any fills.
- A PB8 side may be disabled by its risk/position settings.
- PB8 Movie Builder does not contain historical per-frame ideal-order ladders; use Snapshot to inspect native ideal orders for one supplied state.
- A PB8 Backtest Results handoff supplies stored-result provenance for Compare; verify that the selected fresh replay window covers the stored fills.

---

## Where to go next
- Read the tutorials in the Strategy Explorer docs selector inside the Strategy Explorer page.
