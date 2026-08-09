# Tutorial: Movie Builder

Movie Builder creates a time-stepped replay in the shared PB7/PB8 Strategy Explorer page shell.

---

## 1) Choose a window that covers what you want to see
Movie Builder is driven by:

- **Step Size** (for example 1m, 5m, 1h, 4h)
- **Duration** (preset) or **Frames** (custom)

Total covered time:

- With a Duration preset, Frames are calculated from Duration / Step Size.
- With Custom, covered time is approximately Frames x Step Size.

A PB7 result handoff can align the recorded fill window. PB8 movie generation uses the selected start and duration for a fresh bounded native replay, not playback of the stored result or a live account.

---

## 2) Select the Movie engine
### PBGui Simulation (PB7)
Use when:
- You want PB7 local replay with evolving grid ladders, trailing lines, and fills.

Tradeoff:
- It is a local simulation and may diverge from PB7 backtest semantics.

### PB7 Backtest Engine
Use when:
- You want PB7-engine fills and the existing upcoming-fill view.

Tradeoff:
- It does not provide full open grid ladders for every candle.

### PB7 fills.csv (from backtest)
Use when:
- You want to visualize recorded fills from a completed PB7 backtest without recomputing them.

Tradeoff:
- It cannot generate new fills or an historical ideal-order trace.

### PB8 Native Replay
Use when:
- You want movie frames from real candles and fills produced by a fresh bounded native PB8 replay.

Important limitation:
- PB8 upstream does not expose historical ideal orders for every replay frame. PBGui therefore leaves per-frame entry/close order ladders empty instead of claiming exact historical resting orders. The candle path and fills are real replay output; fill-derived position annotations stop if the displayed-fill limit is reached. Inspect **Snapshot** separately for native ideal orders at one supplied state.
- Dashed **Upcoming Entries** and **Upcoming Closes** are previews derived from displayed replay fills, not native historical resting-order ladders. EMA High/Low traces appear only when the selected engine supplies real EMA-band values; PB8 Native Replay normally omits them.

PB8 generation is capped at 2,000 frames, 20,000 replay candles, and 2,000 displayed fills.

---

## 3) Pick Long/Short
If both Long and Short are active, choose **Side**:

- `Auto` (prefers Long)
- `Long`
- `Short`

If one side has no fills, select the other side. In PB8, an empty order ladder is expected even when fill markers exist.

---

## 4) Generate and inspect
1. Click **Generate Movie**.
2. Use Plotly's **Play**, **Slow**, or **Very Slow** controls below the chart to start playback. **Pause** stops at the current frame.
3. Drag Plotly's frame slider below the chart to inspect a specific candle, order state, or fill marker.
4. Hover a dashed red or green entry/close line near one of its endpoints to see its exact price.
5. While playback is paused, use the left and right arrow keys to move exactly one frame backward or forward. Arrow keys are ignored while a form control has focus.
6. Use the fills table to verify:
   - timestamps
   - order type
   - price and quantity
   - wallet balance and position evolution

For PB8, the frame table's Entry/Close Order counts remain zero because historical native order state is unavailable. Green `B` and red `S` chart markers are actual replay fills at their UTC timestamps. Multi-minute candles are complete OHLCV aggregations of their one-minute replay interval, not sampled single minutes.
The Movie X-axis is locked to each frame's visible candle window so resize or refresh operations cannot move the dashed line start away from the left chart boundary.

For PB8, refreshing the browser tab keeps the active section, approved non-sensitive config sections, Movie controls, and up to about 3 MiB of generated Movie data in tab-scoped `sessionStorage` for 24 hours. A sensitive config key prevents that config from being cached. The cached section is selected before the restored Plotly chart is created, and the chart is resized after it becomes visible. Authentication data and the owner-bound source-result provenance path are never stored. If the server draft is gone, PBGui rebuilds the snapshot from this cache. An oversized Movie is regenerated automatically only when Movie Builder was the restored active section and the cache records that a Movie existed. Simulation and Compare results are not restored.

**Stop Movie Builder** cancels the active helper operation or MP4 export owned by the current session. Changing Movie controls supersedes an older result.

Do not interpret a PB8 movie as live-account forecasting or as an exact record of historically resting orders.

---

## 5) Export (optional)
Use **Export MP4** to render a standalone video.

PB7 and PB8 share one encoder slot. Retry if another export is active. Export is limited to 2,500 frames and 512 MiB output; PB8 additionally rejects export payloads above 16 MiB.

If export is slow:
- Increase Step Size.
- Reduce Frames or Duration.
- Use the **Fast** export preset.
