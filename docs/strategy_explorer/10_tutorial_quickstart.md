# Tutorial: Strategy Explorer Quickstart

This tutorial gets you from "open Strategy Explorer" to "I can interpret what I see" in the shared PB7/PB8 page shell and workflow.

---

## 1) Choose a market
1. Open Strategy Explorer directly or use **Strategy Explorer** from PB8 Run, PB8 Backtest, Backtest Results, or Pareto Explorer.
2. In the shared controls:
   - Select **Exchange**
   - Select **Coin**
3. Confirm that candles load for the selected window.

PB7 uses its existing local OHLCV choices. PB8 labels its source **PB8 native candles** and uses native PB8 candle preparation. A handoff preloads its config and applicable overrides.

---

## 2) Set Analysis Time
1. Use **Start Date** and **Start Time** to select a moment.
2. Keep **Chart Context** reasonably small at first, for example 3-10 days.

Rule of thumb:
- Start Date/Start Time select the first displayed candle. Chart Context extends forward; its right-edge candle is Analysis Time and supplies the snapshot state.
- PB8 entry orders use a supplied flat position; PB8 close orders use a representative hypothetical position at that price.

The PB8 snapshot is not a live-account state or forecast.

---

## 3) Read the snapshot
Look for:

- Entry order levels
- Close order levels
- Available strategy reference or trailing lines
- Long/Short parameter and summary values

PB7 keeps its existing local/PB7-engine snapshot behavior. PB8 shows native ideal orders for the supplied states, not exact orders that historically rested on an exchange.

Ask yourself:

- Are entry levels where I expect?
- Is the representative close output too aggressive or conservative?
- Does changing a parameter have the expected effect?

---

## 4) (Optional) Run Simulation
If you want historical fills:

1. Open **Simulation**.
2. For PB7, choose **PBGui Simulation** or **PB7 Backtest Engine**.
3. For PB8, run **PB8 Native Replay**.

PB8 replay is a bounded native backtest over the selected candles, with server limits of 20,000 candles and 2,000 displayed fills. It does not forecast a live account.

---

## 5) Next steps
- To reconcile a stored result with calculations, continue with "Compare".
- To animate a candle window, continue with "Movie Builder".
