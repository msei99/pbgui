# Tutorial: Live vs Backtest (PBv7)

This page helps you compare **Live performance** (from exchange income history stored in PBGui’s DB) against a **PB7 backtest result** (from `fills.csv`).

The goal is to quickly answer:
- “Does the backtest roughly reproduce the live equity curve?”
- “Which coins/symbols contribute most to the difference?”

---

## What is compared?

### Live
Live is computed from **income events** fetched from the exchange and stored in the PBGui database (`history` table). Depending on exchange and account type this typically includes:
- realized PnL
- commissions/fees
- funding fees

### Backtest
Backtest is computed from the selected PB7 result folder using `fills.csv`.

PBGui uses:
- `net = pnl + fee_paid` (from each fill)

---

## Prerequisites
- You have API-Keys configured in **API-Keys**.
- The user has live income data in the DB (otherwise Live will be empty).
- A PB7 backtest exists in PB7’s results folder (`backtests/pbgui/...`).

---

## Step-by-step workflow

### 1) Select User
- By default, the user selector shows only users which already have live income rows.
- Enable **All users** to show all API-key users.

### 2) Select the Exchange for the compare backtest
- This exchange determines which PB7 market universe is used for the compare run.
- If you are running live on Hyperliquid, you will often want to compare to a Binance backtest (because Hyperliquid 1m OHLCV snapshots may not match historic reality reliably).

### 3) Select the time range
- Set **Start** and **End**.
- You can also enable **Select range** and drag a box in the chart to sync Start/End to a window.

### 4) Choose a Backtest Result (optional)
- If you pick an existing result, PBGui overlays the backtest curve on top of the live curve.
- Use **Sync Start/End to backtest** to align your time window to the backtest.

### 5) (Optional) Select Coins/Symbols
- If empty: compare totals.
- If selected: compare only the chosen subset.

### 6) Starting Balance
- PBGui shows a suggested starting balance based on DB data.
- You can override the starting balance used for the compare calculation.

### 7) Run Compare Backtest
- Click **Run Compare Backtest** to enqueue a PB7 run.
- PBGui forces `combine_ohlcvs = false` for new compare runs.

---

## Interpreting results
- If the curves differ mainly by a constant offset, it’s often just a **starting balance** mismatch.
- If the difference grows over time and is concentrated in a few coins, it usually indicates **path differences** (different fills/positions), not just fees.

---

## Known limitations (current state)
- The page compares **income events** (Live) vs **fills-based net** (Backtest). These are not identical sources.
- Trade-level matching (detecting missed orders + slippage by matching live trades against backtest fills) is planned but not yet integrated into this page.
- “Combined” results can be selected for comparison, but you cannot run a new compare backtest in “combined” mode from this page.

---

## Troubleshooting
- **No Live curve:** make sure income history exists (run PBGui services / ensure exchange history fetch works).
- **No Backtest results found:** run a PB7 backtest from PBGui, then return and click Refresh.
- **Empty coin list:** try comparing total first; coin parsing depends on the available data.
