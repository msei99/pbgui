# Tutorial: Movie Builder

Movie Builder creates a time-stepped animation of bot behavior in Strategy Explorer.

---

## 1) Choose a window that covers what you want to see
Movie Builder is driven by:

- **Step Size** (e.g. 1m, 5m, 1h, 4h)
- **Duration** (preset) or **Frames** (custom)

Total covered time:

- If Duration preset: Frames are calculated from Duration / Step Size
- If Custom (Frames): covered time ≈ Frames × Step Size

Tip: When launching Strategy Explorer from a backtest result, Step Size and Duration are auto-picked to cover the full backtest fills window.

---

## 2) Select the Movie engine
### Local (B) – full grids
Use when:
- You want to see evolving grid ladders and trailing lines.

Pros:
- Most visual detail.

Cons:
- It is a local simulation; it may diverge from PB7 backtest semantics.

### PB7 backtest engine (C) – upcoming fills
Use when:
- You want a PB7-engine-based fill preview.

Pros:
- Closer to PB7 backtest logic.

Cons:
- Does not render full “open grid ladders” per candle.

### PB7 fills.csv (from backtest)
Use when:
- You want to visualize the exact recorded fills from a finished PB7 backtest.

Pros:
- Ground truth for fills.

Cons:
- Only as good as the backtest result; it does not generate new fills.

---

## 3) Pick Long/Short
If both Long and Short are active, choose a side:

- Auto (prefers Long)
- Long
- Short

Tip: If the selected side has no fills but the other side does, Strategy Explorer may fall back so you still see markers.

---

## 4) Generate and inspect
1. Click **Generate Movie**.
2. Use the fills table under the movie to verify:
   - timestamps
   - order_type
   - price/qty
   - wallet balance evolution

---

## 5) Export (optional)
Use **Export video (mp4)** to render a standalone mp4.

If export is slow:
- Increase Step Size (fewer frames)
- Reduce Frames / Duration
- Use a faster export preset
