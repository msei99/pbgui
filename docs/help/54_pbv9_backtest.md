# PB9 Backtest

Open **PBv9 → Backtest**, or choose **Backtest** in the PB9 Run editor to carry the current, including unsaved, native config into a private ten-minute draft. It uses the shared PB8 Backtest page with separate PB9 configuration, queue and result directories.

## Configure and queue

Set the name, dates, exchanges and starting balance. Exchange choices come from the installed native template and the current document. Native Backtest Settings, Portfolio, Global Defaults, Long/Short Overrides and Logging use the installed PB9 field registry. Inherited side fields remain absent until you edit them. Long/short JSON contains authored overrides; Raw JSON contains the complete authored source. The Run instance is never changed by this handoff.

**Save** stores the native source. **Save & Queue** creates an immutable snapshot of the current form and records the schema, package version, source commit and snapshot hash. Existing saves require the revision you opened; a conflict requires reopening or saving with a different name. Refresh preserves the current draft or saved config destination.

## Run and inspect

Open **Queue** and use its row Start action. PB9 needs a separate installed runtime, a clean source tree and a Rust extension matching that source. A source update or changed queue snapshot is rejected; enqueue a new job deliberately. Native caches and incidental files stay in the job's own working directory. The backtest is an independent local process and survives API restarts; API shutdown stops its request controllers, not the native job.

Use Stop/Restart for the selected managed job and the shared log viewer for output. On completion, open **Results** to inspect native metrics, fills and balance/equity charts. The chart translates PB9's numeric NPZ timestamps to UTC without treating them as PB8 minute counters. **Backtest** from a result opens its original config for a deliberate new run; it does not silently use the latest saved config. Each successful result retains a private copy of its original snapshot and provenance even after queue cleanup. Missing native balance samples remain gaps in the chart. Result filters, selections and opened chart views are retained on reload.

This integration currently offers manual local PB9 jobs. Automatic queue startup, cloud/VPS execution, archives and optimizer handoffs are unavailable here. The existing PB7/PB8 workflows remain separate. Running a backtest never starts a live trading bot.
