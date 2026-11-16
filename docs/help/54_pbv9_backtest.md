# PB9 Backtest

Open **PBv9 → Backtest**, or choose **Backtest** in the PB9 Run editor to carry the current, including unsaved, native config into a private ten-minute draft. It uses the shared PB8 Backtest page with separate PB9 configuration, queue and result directories.

## Configure and queue

The editor follows the PB8 layout: exchanges/name/dates, balance and fee rows, OHLCV controls, source maps, **Coins & Filters**, **Coin Overrides**, **Suite Mode**, then side-by-side **Long** and **Short** configuration. The usual numeric controls have the shared plus/minus buttons. Fields retain their native PB9 names. Additional native settings are grouped under **Advanced Settings**.

Coin selectors, filters, source chips, market-setting overrides and the suite editor reuse the existing Backtest components. Apply Filters writes PB9's side-owned approved/ignored lists. Suite Mode uses native PB9 parameter paths and saves its aggregation as `backtest.reducer`. Exchange choices come from the installed native template and current document.

Each side initially shows **Full Config JSON**, including inherited defaults. Editing it creates only the changed side overrides. Choose **Overrides only** to inspect authored values or delete a pin to inherit again. Small **Explicit value** checkboxes in Advanced Settings also restore inheritance when cleared. Raw JSON contains the authored source. Open sections and JSON views are restored after reload; unsaved configurations are never stored in browser navigation state. The Run instance is never changed by this handoff.

**Additional Parameters**, immediately before Raw JSON, reuses the PB8 fallback for every backtest key without a dedicated control. It includes installed template/registry defaults and extra keys in the loaded config, with typed number, checkbox, text, null and JSON inputs. Defaults remain inherited until edited; source values and their types are preserved. New bot/logging parameters appear under Advanced Settings even if the field registry has not caught up. Invalid JSON blocks Save and Queue. The read-only `base_dir` follows the config name; PB9 jobs receive separate managed result directories. Native PB9 validation still decides which parameters the installed version accepts.

**Add to Run** passes the current form, including unsaved changes, filters and notes, to a disabled PB9 Run draft. It does not save or start an instance.

**Save** stores the native source. **Save & Queue** creates an immutable snapshot of the current form and records the schema, package version, source commit and snapshot hash. Existing saves require the revision you opened; a conflict requires reopening or saving with a different name. Refresh preserves the current draft or saved config destination.

## Run and inspect

PB9 does not yet provide **Strategy Explorer** or native optimization. Unconnected tools are hidden rather than forwarding native configs to PB7/PB8 handlers. The implemented editor, manual queue, result charts and cross-version Compare remain available.

Open **Queue** and use its row Start action, or open **Settings** to enable **Autostart** and choose **Backtest CPU Slots**. PB9 keeps its own queue settings; autostart defaults to off and the slot limit to one. Automatic jobs start oldest first. Slots are shared across running PB7/PB8/PB9 backtests. Disabling autostart or reducing the limit prevents further dispatches without stopping jobs already running. Queue settings update automatically in open tabs; unsaved dialog edits are preserved. PB9 needs a separate installed runtime, a clean source tree and a Rust extension matching that source. A source update or changed queue snapshot is rejected; enqueue a new job deliberately. Native caches and incidental files stay in the job's own working directory. The backtest is an independent local process and survives API restarts; API shutdown stops its request controllers, not the native job.

Filter preferences and notes survive Save, drafts, migration and result reuse. Native execution inputs contain no GUI metadata. Save & Queue ignores repeated clicks and safely reuses the enqueue request ID after an uncertain response.

**Balance Calculator** and **Calc Balance** reuse the shared calculator with the enabled native sides, inherited risk and initial quantity values. These analysis inputs are never saved as a bot config.

Use Stop/Restart for the selected managed job and the shared log viewer for output. Stop is displayed as **stopped**. Restart creates a new job and output directory from the frozen snapshot, preserving the previous run. Deleting a config honours **Also delete results**, moving the selected data to managed trash. Queue cleanup removes its private workspace without following market-data links or removing results. On completion, open **Results** to inspect native metrics, fills and balance/equity charts. The chart translates PB9's numeric NPZ timestamps to UTC without treating them as PB8 minute counters. **Backtest** from a result opens its original config for a deliberate new run; it does not silently use the latest saved config. Each successful result retains a private copy of its original snapshot and provenance even after queue cleanup. Missing native balance samples remain gaps in the chart. Result filters, selections and opened chart views are retained on reload.

This integration runs local native PB9 jobs, with optional queue autostart. Cloud/VPS execution and optimizer handoffs are not yet available. Running a backtest never starts a live trading bot.

## Start from a PB8 backtest

In **PBv8 → Backtest**, use **V9 — Open as PB9 Backtest** on a saved config or local result. PBGui uses the official converter of your installed PB9 runtime and displays the full best-effort changes and warnings before adoption. Conversion can infer different risk-memory policies and metric formulas; freshly evaluate the result. No PB8 file is rewritten and no job starts automatically.

After **Open PB9 draft**, the native editor retains the migration report, dates, balance and migrated settings. The draft is private to your login and expires after ten minutes; save it before expiry. Reload restores the same draft while available. Supported PB8 schemas are `v8.0.0`, `v8.1.0` and `v8.6.0`. Local JSON overrides are materialized; external references and legacy coin flags must be resolved before conversion.

## OHLCV data source

The editor uses the same **ohlcv_source_dir** field and **PBGui Data** action as PB7/PB8. New PB9 backtests and Run-to-Backtest drafts default to the shared PBGui OHLCV directory. Saved/imported configs retain their chosen source. Clear the field to use Passivbot's own candle management, or enter a custom source directory. The selection is saved in the config and immutable queue snapshot; starting a job never silently replaces it. The separate **offline** setting is under **Additional Parameters**. Changing the source never toggles offline automatically.

PB9 reads existing PBGui candles directly. No candle directory or metadata file is copied. Offline jobs reference existing public CCXT market and versioned listing metadata through file links while keeping their own derived caches and results. PBGui Coin Data and PB9 caches are used first; compatible metadata in other configured bot caches may also be read, without importing their code or requiring those installations. Native PB9 still validates listing-cache versions and market identities. Online jobs receive no shared metadata links, so native cache updates cannot write through them.

If offline market metadata is missing, the job stays queued with an actionable message. Update the exchange in Coin Data, provide the native offline listing metadata where required, or deliberately disable offline. Offline never silently downloads missing inputs. Existing source files remain unchanged.

## Market price overlay

Open the balance/equity chart with its eye action. **Price (PBGui MarketData)** reads existing PBGui candles using the same reader as PB7/PB8 and plots the close price on the right axis. Combined results list each configured exchange separately; change the selector to compare venues. The initial selection prefers a venue covering the full chart period. Missing or partial data is shown explicitly. Result refresh/reload restores the open chart and your selected venue. Native HSL plots remain available through the plot-images action; PB8-specific HSL reconstruction is not applied to native PB9 results. No candle data is downloaded or copied by this view.

## Compare PB7, PB8 and PB9

PB9 extends the existing **Compare** view. Under **Results**, choose PB7, PB8, PB9 or **All**, select at least two rows and press **Compare**. Each result loads its own version's equity CSV; the existing chart overlays equity and balance on a shared UTC axis. Different metric formulas are not converted into matching scores. The version filter, selected rows and open comparison are restored on reload.

New conversions from a local PB8 **result** retain a PBGui comparison reference through the draft, saved config, queue and final result. Use the result-row **Compare with PB8 source** action to select that exact pair and open the same chart directly. The reference remains available after queue cleanup. It identifies a comparison target; it does not assert that edited PB9 parameters are equivalent to PB8. If the original is removed or its config/analysis changes, the direct comparison reports that instead of guessing another result. Native bot parameters remain separate from this GUI metadata.

Older PB9 runs and conversions from a saved config without a specific result can use the normal manual Compare selection. No new backtest is required and comparison does not modify result or candle data.

## OHLCV Readiness and 1st/All

Open **OHLCV Readiness** in the Backtest sidebar to check the current native PB9 configuration. It reuses the shared PB8 panel and reporting logic, with PB9's own universe, activation, minimum-age and warmup functions. The panel updates automatically after edits, when returning to the window and periodically while open. Browser reload restores an open panel for the current config/draft. No manual Refresh control is needed.

The default **ohlcv_source_dir** remains PBGui's shared candle directory. No candles are copied, imported or preloaded by these tools. Readiness accepts absolute custom sources below the PBGui market-data root or configured PB9 checkout. An empty source checks a fresh native PB9 job cache; missing inputs can be acquired by the native backtest when offline is explicitly disabled. Use **Market Data** to acquire missing history for the PBGui source.

**1st** uses the earliest known first OHLCV date among the selected markets plus native PB9 warmup. **All** uses the common date for every selected exchange/coin pair plus warmup. Select explicit approved coins on an enabled side with positive exposure and positions; `all` and more than 200 exchange/coin pairs require a narrower selection. Progress and Stop use the same controller as PB8. A late result never changes the date after the editor inputs changed.

Offline mode uses cached public exchange/inception metadata. With offline disabled, missing metadata can be fetched into a private temporary cache. Public metadata links are read-only; an online fallback removes them before writing private cache files. No shared metadata or candles are overwritten. Readiness checks coverage; the native backtest still verifies runtime, dataset and provenance at start. When **hlcvs_data_dir** selects a prepared dataset, raw-candle readiness does not apply and 1st/All is unavailable; use that dataset's dates.


## Shared Archive

**Add to Archive** under Results exports the selected PB9 result to the existing own archive configured in **Archive → Setup**. PB7, PB8 and PB9 share the same archive list, transactions, versioned layout and manifests. Native config/source/provenance, NPZ equity, fills and report files are retained as result artifacts; candle data is not copied. Export does not push Git changes or start a job.

Open **Archive** in the sidebar to browse results. Native PB9 rows use the same summary formulas as local PB9 Results and load their charts, images, files, price overlay and source through the PB9 reader. Browsing from PB9 does not automatically migrate an archive. Selection, search/config/coin filters and the open archive survive browser reload, and the existing archive watcher updates the view automatically.

**Backtest** on selected archive rows opens the existing date/balance/exchange dialog and queues a fresh native PB9 snapshot. Imported candle/dataset paths must remain below PBGui Market Data or the configured PB9 source; use the dialog’s PBGui data option or edit an unavailable foreign path. Shared mixed-generation selections route each result to its own queue. **Add to Run** opens a disabled PB9 Run draft. Renaming a native archive config group preserves its artifact and provenance bytes. Automated **Retest & Replace**, retest schedules and native optimizer archive settings are not yet connected for PB9 and cannot fall back to PB7.

Archive Setup never returns a stored access token to the browser. An empty token input keeps the stored token; **Clear stored token** removes it explicitly. Git authentication stays server-side in the transient subprocess environment, over HTTPS, with matching push/fetch origins and no token in command arguments or repository URLs. Git push, pull and history compaction remain explicit user actions.
