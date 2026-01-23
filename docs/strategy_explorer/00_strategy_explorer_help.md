# Strategy Explorer – Help

Strategy Explorer is a visual debugging and analysis tool for PB7 grid strategies. It combines:

- A **snapshot view** of what the bot would place (entry/close grids, trailing thresholds) at a chosen **Analysis Time**.
- Optional **historical simulation** (local candle-walk) to show fills.
- A **Compare** workflow to reconcile Strategy Explorer vs PB7 backtest output.
- A **Movie Builder** to create a time-stepped animation of bot behavior.

This document explains the available variants and how the Long/Short grid display works.

---

## Core concepts

### Exchange / Coin
Strategy Explorer always works on a specific market:

- **Exchange**: e.g. `bybit`
- **Coin**: the market symbol/coin code available in your local PB7 OHLCV cache

If no candles are found, Strategy Explorer cannot render anything.

### Analysis Time (the most important control)
Strategy Explorer computes grids and trailing state at a single point in time: **Analysis Time**.

- Think of it as the candle at the **right edge** of the chart.
- All grids/levels shown are “what the bot would do next” *given the state injected at that time*.

### Context window
The chart shows a context window around Analysis Time:

- **Context days** controls how much history to display for context.

---

## Variants / modes

### 1) Snapshot (single view)
This is the default behavior of Strategy Explorer.

You choose Analysis Time and Strategy Explorer renders:

- **Entry grid** levels (potential buys for Long / sells for Short)
- **Close grid** levels (take-profit / close orders)
- Trailing thresholds/triggers and related reference lines

This mode answers questions like:

- “What grid would PB7 place right now?”
- “Why does trailing trigger here?”
- “Why are my close orders so tight/wide?”

### 2) Historical Simulation (local candle-walk)
If you enable historical simulation, Strategy Explorer walks candles forward and records fills.

- This is a *local simulation* for intuition and debugging.
- It may not match PB7 backtest 1:1 (exchange specifics, rounding, engine differences), but it is useful to understand behavior.

The fills appear as markers and in a fills table.

### 3) Compare (PB7 vs B vs C)
Compare is used to check / debug discrepancies between an actual PB7 backtest and the local PBGui calculations.

Typical meaning of series:

- **PB7**: fills read from the PB7 backtest result (usually `fills.csv`).
- **B**: Strategy Explorer local simulation variant (Mode B).
- **C**: PB7 engine-based path used by PBGui (Mode C, "upcoming fills" style).

Use Compare when:

- You want to ensure your time window matches the backtest time window.
- You want to see if discrepancies arise between an actual backtest and the PBGui calculation.

### 4) Movie Builder
Movie Builder generates an animation over time.

It has three engines:

- **Local (B) – full grids**
  - Shows evolving grids + trailing lines.
  - Computes fills via local simulation.

- **PB7 backtest engine (C) – upcoming fills**
  - Uses the PB7 engine path used by the tool.
  - Focuses on fills and upcoming fill previews; it does not provide full “open grid ladders” per candle.

- **PB7 fills.csv (from backtest)**
  - Uses an existing PB7 backtest folder (`fills.csv`) as ground truth.
  - No recomputation of the engine; it simply visualizes recorded fills.

---

## Long/Short grid display (how to read it)
Strategy Explorer can show Long and/or Short depending on your config.

### Long
- **Long entry grid**: typically plotted as buy levels below price.
- **Long close grid**: typically plotted as sell/close levels above entry/price (take-profit ladder).

### Short
- **Short entry grid**: typically plotted as sell levels above price.
- **Short close grid**: typically plotted as buy/close levels below entry/price.

### Both sides active
When both Long and Short are enabled:

- Snapshot view may show both sets of grids.
- Movie Builder also offers a **Side** selector:
  - `Auto` (prefers Long)
  - `Long`
  - `Short`

Tip: If your backtest only traded one side, pick that side (or use Auto).

---

## Common gotchas

### “I don’t see any orders/markers”
Almost always this is a time-window issue:

- Your selected Analysis Time / Movie window does not overlap the period where fills happened.
- If you launch Strategy Explorer from Backtest Results, the tool should auto-jump to the fills range.


---

## Where to go next
- Read the tutorials in the Strategy Explorer docs selector inside the Strategy Explorer page.
