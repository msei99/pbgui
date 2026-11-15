# PB9 Backtest

Open **PBv9 → Backtest**, or choose **Backtest** in the PB9 Run editor to carry the current, including unsaved, native config into a private ten-minute draft. It uses the shared PB8 Backtest page with separate PB9 configuration, queue and result directories.

## Configure and queue

Set the name, dates, exchanges and starting balance. Exchange choices come from the installed native template and the current document. Native Backtest Settings, Portfolio, Global Defaults, Long/Short Overrides and Logging use the installed PB9 field registry. Inherited side fields remain absent until you edit them. Long/short JSON contains authored overrides; Raw JSON contains the complete authored source. The Run instance is never changed by this handoff.

**Save** stores the native source. **Save & Queue** creates an immutable snapshot of the current form and records the schema, package version, source commit and snapshot hash. Existing saves require the revision you opened; a conflict requires reopening or saving with a different name. Refresh preserves the current draft or saved config destination.

## Run and inspect

Open **Queue** and use its row Start action. PB9 needs a separate installed runtime, a clean source tree and a Rust extension matching that source. A source update or changed queue snapshot is rejected; enqueue a new job deliberately. Native caches and incidental files stay in the job's own working directory. The backtest is an independent local process and survives API restarts; API shutdown stops its request controllers, not the native job.

Use Stop/Restart for the selected managed job and the shared log viewer for output. On completion, open **Results** to inspect native metrics, fills and balance/equity charts. The chart translates PB9's numeric NPZ timestamps to UTC without treating them as PB8 minute counters. **Backtest** from a result opens its original config for a deliberate new run; it does not silently use the latest saved config. Each successful result retains a private copy of its original snapshot and provenance even after queue cleanup. Missing native balance samples remain gaps in the chart. Result filters, selections and opened chart views are retained on reload.

This integration currently offers manual local PB9 jobs. Automatic queue startup, cloud/VPS execution, archives and optimizer handoffs are unavailable here. The existing PB7/PB8 workflows remain separate. Running a backtest never starts a live trading bot.

## Start from a PB8 backtest

In **PBv8 → Backtest**, use **V9 — Open as PB9 Backtest** on a saved config or local result. PBGui uses the official converter of your installed PB9 runtime and displays the full best-effort changes and warnings before adoption. Conversion can infer different risk-memory policies and metric formulas; freshly evaluate the result. No PB8 file is rewritten and no job starts automatically.

After **Open PB9 draft**, the native editor retains the migration report, dates, balance and migrated settings. The draft is private to your login and expires after ten minutes; save it before expiry. Reload restores the same draft while available. Supported PB8 schemas are `v8.0.0`, `v8.1.0` and `v8.6.0`. Local JSON overrides are materialized; external references and legacy coin flags must be resolved before conversion.

## OHLCV data source

The editor uses the same **ohlcv_source_dir** field and **PBGui Data** action as PB7/PB8. New PB9 backtests and Run-to-Backtest drafts default to the shared PBGui OHLCV directory. Saved/imported configs retain their chosen source. Clear the field to use Passivbot's own candle management, or enter a custom source directory. The selection is saved in the config and immutable queue snapshot; starting a job never silently replaces it. Backtest Settings opens initially so the separate **offline** setting and other native controls are visible.

PB9 reads existing PBGui candles directly. No candle directory or metadata file is copied. Offline jobs reference existing public CCXT market and versioned listing metadata through file links while keeping their own derived caches and results. PBGui Coin Data and PB9 caches are used first; compatible metadata in other configured bot caches may also be read, without importing their code or requiring those installations. Native PB9 still validates listing-cache versions and market identities. Online jobs receive no shared metadata links, so native cache updates cannot write through them.

If offline market metadata is missing, the job stays queued with an actionable message. Update the exchange in Coin Data, provide the native offline listing metadata where required, or deliberately disable offline. Offline never silently downloads missing inputs. Existing source files remain unchanged.

## Market price overlay

Open the balance/equity chart with its eye action. **Price (PBGui MarketData)** reads existing PBGui candles using the same reader as PB7/PB8 and plots the close price on the right axis. Combined results list each configured exchange separately; change the selector to compare venues. The initial selection prefers a venue covering the full chart period. Missing or partial data is shown explicitly. Result refresh/reload restores the open chart and your selected venue. Native HSL plots remain available through the plot-images action; PB8-specific HSL reconstruction is not applied to native PB9 results. No candle data is downloaded or copied by this view.
