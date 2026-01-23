# Tutorial: Strategy Explorer Quickstart

This tutorial gets you from “open Strategy Explorer” to “I can interpret what I see”.

---

## 1) Choose a market
1. Open Strategy Explorer.
2. In **Data + Time & View**:
   - Select **Exchange**
   - Select **Coin**
3. Confirm you see a message like “Loaded N candles…”.

If you see “No candles found”, you need to download/prepare OHLCV data in PB7 first.

---

## 2) Set Analysis Time
1. Use the day slider/date/time controls to pick a moment.
2. Keep **Context days** reasonably small at first (e.g. 3–10) to make the chart readable.

Rule of thumb:
- Analysis Time = the “right edge” candle used to compute grids.

---

## 3) Read the snapshot
Look for:

- Entry grid levels (where new positions would be opened)
- Close grid levels (where positions would be reduced/closed)
- Trailing thresholds/triggers

Ask yourself:

- Are entry levels where I expect?
- Is the close ladder too aggressive / too conservative?
- Does trailing activate when price reaches the threshold and then retraces?

---

## 4) (Optional) Enable historical simulation
If you want to see fills (markers) based on a local candle-walk:

1. Enable **Historical Simulation**.
2. Move Analysis Time forward/backward to understand how fills accumulate.

This is primarily a debugging/intuition tool; exact matching with PB7 backtests is not guaranteed.

---

## 5) Next steps
- If you want to reconcile with a PB7 backtest: continue with “Compare (PB7 vs B vs C)”.
- If you want an animation: continue with “Movie Builder”.
