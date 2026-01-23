# Tutorial: Compare (PB7 vs B vs C)

Compare is used to debug differences between:

- PB7 backtest fills (`fills.csv`)
- Strategy Explorer local simulation (Mode B)
- Strategy Explorer PB7-engine-based visualization (Mode C)

---

## 1) Start from a backtest (recommended)
1. Run a PB7 backtest from PBGui.
2. In Backtest Results, click **Strategy Explorer**.

When launched this way:
- Compare is automatically set to the correct backtest folder.
- Strategy Explorer automatically jumps to the fills time range.

---

## 2) Start Compare
1. Choose the Compare mode (e.g. PB7 vs B vs C).
2. Click **Start Compare**.

Interpretation tips:
- If PB7 has fills but B/C do not, your local simulation window/time selection likely doesn’t overlap.
- If B differs from PB7 but C matches, it’s probably a simulation/engine mismatch.
- If C differs from PB7, check start-time/state injection assumptions and config alignment.

---

## 3) Typical debug workflow
1. Verify you are looking at the same market (exchange/coin).
2. Verify time overlap (fills timestamps vs selected window).
3. Use Strategy Explorer Movie Builder with **PB7 fills.csv** to confirm the backtest fills are being read correctly.
4. Only then compare against Mode B/C.
