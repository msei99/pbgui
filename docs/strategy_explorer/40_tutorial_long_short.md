# Tutorial: Understanding Long vs Short grids

This tutorial explains how the shared PB7/PB8 Strategy Explorer GUI displays Long and Short output and how to avoid common misinterpretations.

---

## 1) Long grids
### Long entry grid
- Represents buy orders that open or increase a Long position.
- Usually appears below the current price, depending on strategy and state.

### Long close grid
- Represents sell orders that reduce or close a Long position.
- Usually appears above the position price.

For a PB8 snapshot, Long entries come from the supplied flat state. Long closes come from a separate representative hypothetical Long position at the selected price.

---

## 2) Short grids
### Short entry grid
- Represents sell orders that open or increase a Short position.
- Usually appears above the current price, depending on strategy and state.

### Short close grid
- Represents buy orders that reduce or close a Short position.
- Usually appears below the position price.

For a PB8 snapshot, Short entries come from the supplied flat state. Short closes come from a separate representative hypothetical Short position at the selected price.

---

## 3) Both sides active
If both Long and Short are enabled in the config:

- Snapshot can show output for both sides.
- Movie Builder lets you select `Auto`, `Long`, or `Short`.

If only one side has fills, the native or local replay may simply have traded one direction. PB8's representative snapshot positions do not mean those positions existed historically or in a live account.

---

## 4) Trailing lines
Trailing is path-dependent.

- Snapshot reference lines explain one supplied state at Analysis Time.
- PB7 local/PB7-engine tools retain their existing behavior.
- PB8 Native Replay supplies real candles and fills, but PB8 upstream does not expose per-frame historical ideal-order ladders or a complete resting-order trace.

Do not infer an exact historical order sequence from a PB8 snapshot or movie.

---

## 5) Debugging checklist
If grids look inverted or wrong:

- Confirm **Side** (`Long` or `Short`).
- Confirm **Exchange** and **Coin**.
- Confirm Analysis Time is inside the period you care about.
- For PB8 closes, remember the representative hypothetical position assumption.
- For result comparison, use the direct Backtest Results handoff so stored PB8 provenance remains behind the opaque draft.
