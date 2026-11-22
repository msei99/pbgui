# PB9 Backtest

Raw JSON and form edits are merged in input order, including when saving immediately. An incomplete JSON document blocks Save without discarding pending form edits.

Coin selection and Apply Filters reuse PB8's catalog and filter logic with the
native PB9 resolver. Normal coins appear once by name; exact IDs remain for
ambiguous markets and existing exact selections. Only existing local public
market metadata is read for this selection.

The exchange picker lists all historical-data venues supported by the installed PB9 connectors and CCXT. Template exchanges are the initial selection, not the available choices; Suite Mode and coin sources use the same catalog.

Open **PBv9 → Backtest**, or choose **Backtest** in the PB9 Run editor to carry the current, including unsaved, native config into a private ten-minute draft. It uses the shared PB8 Backtest page with separate PB9 configuration, queue and result directories.

## Configure and queue

The editor follows the PB8 layout: exchanges/name/dates, balance and fee rows, OHLCV controls, source maps, **Coins & Filters**, **Coin Overrides**, **Suite Mode**, then side-by-side **Long** and **Short** configuration. The usual numeric controls have the shared plus/minus buttons. Fields retain their native PB9 names. Additional native settings are grouped under **Advanced Settings**.

Coin selectors, filters, source chips, market-setting overrides and the suite editor reuse the existing Backtest components. Apply Filters writes PB9's side-owned approved/ignored lists. Suite Mode uses native PB9 parameter paths and saves its aggregation as `backtest.reducer`. Exchange choices come from the installed native template and current document.

Each side initially shows **Full Config JSON**, including inherited defaults. Editing it creates only the changed side overrides. Choose **Overrides only** to inspect authored values or delete a pin to inherit again. Small **Explicit value** checkboxes in Advanced Settings also restore inheritance when cleared. Raw JSON contains the authored source. Open sections and JSON views are restored after reload. Unsaved draft recovery uses guarded tab-local storage and rejects credential fields. The Run instance is never changed by this handoff.

**Additional Parameters**, immediately before Raw JSON, reuses the PB8 fallback for every backtest key without a dedicated control. It includes installed template/registry defaults and extra keys in the loaded config, with typed number, checkbox, text, null and JSON inputs. Defaults remain inherited until edited; source values and their types are preserved. New bot/logging parameters appear under Advanced Settings even if the field registry has not caught up. Invalid JSON blocks Save and Queue. The read-only `base_dir` follows the config name; PB9 jobs receive separate managed result directories. Native PB9 validation still decides which parameters the installed version accepts.

**Add to Run** passes the current form, including unsaved changes, filters and notes, to a disabled PB9 Run draft. It does not save or start an instance.

**Save** stores the native source. **Save & Queue** creates an immutable snapshot of the current form and records the schema, package version, source commit and snapshot hash. Existing saves require the revision you opened; a conflict requires reopening or saving with a different name. Refresh preserves the current draft or saved config destination.

## Run and inspect

Shared backtest slots belong to the current PBGui installation. Backtests from a separate PBGui checkout, such as `pbgui-share`, do not occupy its slots even when both installations use the same Passivbot checkout.

PB9 does not yet provide **Strategy Explorer** or native AI Loops. **Optimize from Result** uses the same handoff as PB8 and opens a native draft under **PBv9 → Optimize**. Unconnected tools are hidden rather than forwarding native configs to PB7/PB8 handlers. The implemented editor, manual queue, result charts and cross-version Compare remain available.

Open **Queue** and use its row Start action, or open **Settings** to enable **Autostart** and choose **Backtest CPU Slots**. PB9 keeps its own queue settings; autostart defaults to off and the slot limit to one. Automatic jobs start oldest first. Slots are shared across running PB7/PB8/PB9 backtests. Disabling autostart or reducing the limit prevents further dispatches without stopping jobs already running. Queue settings update automatically in open tabs; unsaved dialog edits are preserved. PB9 needs a separate installed runtime, a clean source tree and a Rust extension matching that source. A source update or changed queue snapshot is rejected; enqueue a new job deliberately. Backtest and Optimize run in the configured PB9 checkout and share its persistent native `caches/` directory, like PB8. Queue cleanup does not delete this cache. The backtest is an independent local process and survives API restarts; API shutdown stops its request controllers, not the native job.

Filter preferences and notes survive Save, drafts, migration and result reuse. Native execution inputs contain no GUI metadata. Save & Queue ignores repeated clicks and safely reuses the enqueue request ID after an uncertain response.

**Balance Calculator** and **Calc Balance** reuse the shared calculator with the enabled native sides, inherited risk and initial quantity values. These analysis inputs are never saved as a bot config.

Use Stop/Restart for the selected managed job and the shared log viewer for output. Stop is displayed as **stopped**. Restart creates a new job and output directory from the frozen snapshot, preserving the previous run. Deleting a config honours **Also delete results**, moving the selected data to managed trash. Queue cleanup removes its private workspace without following market-data links or removing results. On completion, open **Results** to inspect native metrics, fills and balance/equity charts. The chart translates PB9's numeric NPZ timestamps to UTC without treating them as PB8 minute counters. **Backtest** from a result opens its original config for a deliberate new run; it does not silently use the latest saved config. Each successful result retains a private copy of its original snapshot and provenance even after queue cleanup. Missing native balance samples remain gaps in the chart. Result filters, selections and opened chart views are retained on reload.

This integration runs local native PB9 jobs, with optional queue autostart. VPS execution remains unavailable. Native Optimize has its own shared page and publication requirements for cloud execution. Running a backtest never starts a live trading bot.

## Start from a PB8 backtest

In **PBv8 → Backtest**, use **V9 — Open as PB9 Backtest** on a saved config or local result. PBGui uses the official converter of your installed PB9 runtime and displays the full best-effort changes and warnings before adoption. Conversion can infer different risk-memory policies and metric formulas; freshly evaluate the result. No PB8 file is rewritten and no job starts automatically.

After **Open PB9 draft**, the native editor retains the migration report, dates, balance and migrated settings. The draft is private to your login and expires after ten minutes; save it before expiry. Reload restores the same draft while available. Supported PB8 schemas are `v8.0.0`, `v8.1.0` and `v8.6.0`. Local JSON overrides are materialized; external references and legacy coin flags must be resolved before conversion.

## OHLCV data source

The editor uses the same **ohlcv_source_dir** field and **PBGui Data** action as PB7/PB8. New PB9 backtests and Run-to-Backtest drafts default to the shared PBGui OHLCV directory. Saved/imported configs retain their chosen source. Clear the field to use Passivbot's own candle management, or enter a custom source directory. The selection is saved in the config and immutable queue snapshot; starting a job never silently replaces it. The separate **offline** setting is under **Additional Parameters**. Changing the source never toggles offline automatically.

PB9 reads existing PBGui candles directly. No candle directory or metadata file is copied. Offline execution reads the PB9 installation's own cached market/listing metadata and coverage catalog. Prepare that native cache online before offline use. Execution creates no links to another runtime's metadata. Native PB9 still validates listing-cache versions and market identities.

If offline market metadata is missing, the job stays queued with an actionable message. Update the exchange in Coin Data, provide the native offline listing metadata where required, or deliberately disable offline. Offline never silently downloads missing inputs. Existing source files remain unchanged.

## Market price overlay

Open the balance/equity chart with its eye action. **Price (PBGui MarketData)** reads existing PBGui candles using the same reader as PB7/PB8 and plots the close price on the right axis. Combined results list each configured exchange separately; change the selector to compare venues. The initial selection prefers a venue covering the full chart period. Missing or partial data is shown explicitly. Result refresh/reload restores the open chart and your selected venue. Native HSL plots remain available through the plot-images action; PB8-specific HSL reconstruction is not applied to native PB9 results. No candle data is downloaded or copied by this view.

## Compare PB7, PB8 and PB9

PB9 extends the existing **Compare** view. Under **Results**, choose PB7, PB8, PB9 or **All**, select at least two rows and press **Compare**. Each result loads its own version's equity CSV; the existing chart overlays equity and balance on a shared UTC axis. Different metric formulas are not converted into matching scores. The version filter, selected rows and open comparison are restored on reload.

New conversions from a local PB8 **result** retain a PBGui comparison reference through the draft, saved config, queue and final result. Use the result-row **Compare with PB8 source** action to select that exact pair and open the same chart directly. The reference remains available after queue cleanup. It identifies a comparison target; it does not assert that edited PB9 parameters are equivalent to PB8. If the original is removed or its config/analysis changes, the direct comparison reports that instead of guessing another result. Native bot parameters remain separate from this GUI metadata.

Older PB9 runs and conversions from a saved config without a specific result can use the normal manual Compare selection. No new backtest is required and comparison does not modify result or candle data.

## OHLCV Readiness and 1st/All

Open **OHLCV Readiness** in the Backtest sidebar to check the current native PB9 configuration. It reuses the shared PB8 panel and reporting logic, with PB9's own universe, activation, minimum-age and warmup functions. The panel updates automatically after edits, when returning to the window and periodically while open. Browser reload restores an open panel for the current config/draft. No manual Refresh control is needed.

The default **ohlcv_source_dir** remains PBGui's shared candle directory. No candles are copied, imported or preloaded by these tools. Readiness accepts absolute custom sources below the PBGui market-data root or configured PB9 checkout. An empty source checks the persistent native PB9 cache also used by execution; missing inputs can be acquired by the native backtest when offline is explicitly disabled. Use **Market Data** to acquire missing history for the PBGui source.

**1st** uses the earliest known first OHLCV date among the selected markets plus native PB9 warmup. **All** uses the common date for every selected exchange/coin pair plus warmup. Select explicit approved coins on an enabled side with positive exposure and positions; `all` and more than 200 exchange/coin pairs require a narrower selection. Progress and Stop use the same controller as PB8. A late result never changes the date after the editor inputs changed.

Offline mode uses cached public exchange/inception metadata. With offline disabled, missing metadata can be fetched into a private temporary cache. Public metadata links are read-only; an online fallback removes them before writing private cache files. No shared metadata or candles are overwritten. Readiness inspects the same persistent native cache as execution and asks the native loader to check the first day of an external source; the native backtest still verifies runtime, dataset and provenance at start. When **hlcvs_data_dir** selects a prepared dataset, raw-candle readiness does not apply and 1st/All is unavailable; use that dataset's dates.


## Shared Archive

**Add to Archive** under Results exports the selected PB9 result to the existing own archive configured in **Archive → Setup**. PB7, PB8 and PB9 share the same archive list, transactions, versioned layout and manifests. Native config/source/provenance, NPZ equity, fills and report files are retained as result artifacts; candle data is not copied. Export does not push Git changes or start a job.

Open **Archive** in the sidebar to browse results. Native PB9 rows use the same summary formulas as local PB9 Results and load their charts, images, files, price overlay and source through the PB9 reader. Browsing from PB9 does not automatically migrate an archive. Selection, search/config/coin filters and the open archive survive browser reload, and the existing archive watcher updates the view automatically.

**Backtest** on selected archive rows opens the existing date/balance/exchange dialog and queues a fresh native PB9 snapshot. Imported candle/dataset paths must remain below PBGui Market Data or the configured PB9 source; use the dialog’s PBGui data option or edit an unavailable foreign path. Shared mixed-generation selections route each result to its own queue. **Add to Run** opens a disabled PB9 Run draft. Renaming a native archive config group preserves its artifact and provenance bytes. Native optimizer archive settings remain unavailable and cannot fall back to PB7.

Archive Setup never returns a stored access token to the browser. An empty token input keeps the stored token; **Clear stored token** removes it explicitly. Git authentication stays server-side in the transient subprocess environment, over HTTPS, with matching push/fetch origins and no token in command arguments or repository URLs. Git push, pull and history compaction remain explicit user actions.

### Retest & Replace and schedules

In **PBv9 → Backtest → Archive**, open **My Archive**, select results and click **Retest & Replace**. The shared PB8 dialog offers **Queue Now** or **Create Schedule** with daily/weekly cadence, time and weekday. Choose the original duration or last X days; the new range ends yesterday. Balance, exchanges and PBGui Market Data use the same controls as PB8.

Open **Retest Schedules** to run, pause/resume or delete a schedule. Navigation and the schedules tab survive browser reload. Queueing does not itself start a backtest: enable native PB9 **Autostart** in **Settings** for unattended execution. API restarts leave running native jobs intact and resume the replacement controller.

Only a successful, uniquely identified native result with matching snapshot/schema/commit can replace its original. Liquidated results are skipped by default; changed originals, missing provenance and ambiguous multi-result suites leave the original intact. The original is retained as a private recovery backup, and only this retest’s local job is retired. Schedule targets follow the replacement automatically; Git Push remains manual.

## Draft recovery and native source boundaries

Browser reload restores unsaved names, values and open sections through the same guarded draft storage used by Optimize. Native revision checks remain tied to the original source. Credential-bearing custom fields are never kept in browser storage. Unchanged configurations are loaded from their current source again.

External-source start handling follows the installed PB9 build. The local source-window test fix uses each coin/source's first available real candle when an external directory is explicitly selected, online and offline. This defines the selected dataset window, not market inception. Requested dates stay recorded, no earlier prices are invented and raw candles are not copied. Internal-gap and complete offline-tail checks remain active. Unpatched PB9 builds may still reject a late external-source beginning. Enqueue a fresh job after changing the native source commit; old queue snapshots retain their original identity.
