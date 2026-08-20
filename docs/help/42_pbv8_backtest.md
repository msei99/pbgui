# PBv8 Backtest

PBv8 Backtest manages Passivbot V8 configurations and jobs independently from PBv7. PBGui validates every configuration through the currently installed PB8 loader before saving or starting it.

The page renders the exact same page template and visual configuration editor as PBv7 Backtest. There is no separate PB8 editor implementation. PB8-specific behavior is limited to a path/API adapter, config validation, the process runner, and result data.

PBGui keeps a short, bounded cache of PB8 templates and already validated config files. The first PB8 operation after an API restart may still initialize the isolated PB8 Python runtime; subsequent editor, queue, and start steps reuse canonical results while file-signature checks invalidate changed configs.

## Configs

- **New Config** loads the defaults from the installed PB8 version.
- The Configs table shows the active PB8 strategy and supports sorting by Strategy.
- Double-click a row or use **Edit** to open the full visual editor used by PBv7, including dates, exchanges, fees, market data, coin filters, approved/ignored coins, suites, coin overrides, PB8 result metrics and market-setting overrides, Long/Short JSON, and Raw JSON.
- **Queue** or **Save & Queue** captures an immutable snapshot of the saved config.
- Coin-override JSON files and `backtest.json` are validated and published as one config bundle. A failed save leaves the previous config and override files unchanged; removing an override reference removes its obsolete file from the bundle.
- PBGui controls `backtest.base_dir` and writes results below `<pb8dir>/backtests/pbgui/<config>`.
- Saved PBv7 backtest configs, individual PBv7 results, and PBv7 Run rows offer **V8** conversion. Conversion uses PB8's official `migrate-config-v7` implementation, leaves the V7 source unchanged, and stores `migration_report.json` beside the new V8 config. For result conversions, PBGui first restores effective linear-market maker and taker rates evidenced by `fills.csv`, and records every correction in `pbgui_result_fee_adjustments`. PBGui-only metadata and stale temporary `live.base_config_path` values are removed before invoking PB8.

Migration stops if unsupported or manual-review fields remain. Resolve the reported fields rather than assuming V7 and V8 behavior is identical.

## Editor

The visual controls and JSON synchronization are shared directly with PBv7. For canonical V8 configs the adapter reads and writes exposure and position controls below `bot.<side>.risk`; the Long/Short and Raw JSON editors retain all nested V8 sections such as `risk`, `strategy`, `forager`, `hsl`, and `unstuck`. Imports and saves are prepared by the installed PB8 loader, and invalid JSON blocks saving. PBGui-owned metadata below the top-level `pbgui` object remains unchanged even when the installed PB8 version does not know those fields.

V8 uses the existing PBGui coin metadata, filter, suite, override, date picker, validation, OHLCV-readiness, and selection components. Market identity itself comes from PB8's official resolver: short IDs remain the default, while exact scoped IDs are stored only for real venue or multiplier collisions. Imported exact IDs remain lossless in approved/ignored lists, Suite scenarios, Coin Sources, Market Settings, and Coin Overrides. **Balance Calculator** opens the shared calculator under Information with the current PB8 config, while **Calc Balance** runs the same calculation inline. Saved configs, local results, and PB8 archive results can open a short-lived PB8 Run draft. **Optimize from Result** opens the selected result as a PB8 Optimize draft. **Refine Optimize Preset** builds nested PB8 bounds from the selected result and routes Save, Queue, and Open Optimize actions exclusively through the PB8 APIs. **Strategy Explorer** opens the shared PB7/PB8 visual shell with the PB8 config and sparse overrides; a selected local PB8 result also carries validated stored-fill provenance through an opaque draft for native Compare. That result handoff initially selects the validated source exchange, the first approved coin with stored fills, and its first-fill UTC time.

**Apply Filters** only selects PB8 catalog entries that resolve to at least one eligible market on the chosen exchange. CoinData entries that PB8 cannot resolve losslessly are reported as unavailable and skipped instead of being queued under a heuristic coin name.

Frequently used PB8-specific backtest fields have structured controls:

- **Market Settings Overrides** appears below `market_settings_sources`. Add all-exchange or exchange-specific coin rows and override quantity/price steps, minimum quantity/cost, and contract multiplier. Blank values inherit from the selected source; exchange-specific rows take precedence. Fields unknown to the current PBGui version are preserved without modification. Backtest fees remain controlled by the dedicated maker/taker fee overrides above because PB8 resolves them before applying market-setting overrides.
- **Result Metrics** is kept inside **Additional Parameters** because it only controls terminal and queue-log output. **Default** uses metrics implied by optimize scoring and limits, **All** shows every metric, and **Custom additions** uses a searchable categorized list loaded from the installed PB8 runtime. Complete metrics remain computed and saved in every mode.
- **minimum_coin_age_days** preserves an explicit `0`, including Pareto-to-Backtest drafts. Use `0` to disable the age gate; blank or invalid input falls back to `30` days.

**Additional Parameters** contains Result Metrics, the PBGui-managed `base_dir` as read-only, and the expert-only `hlcvs_data_dir` and `hlcvs_data_override_mode` fields. Prepared-dataset replay requires a server-side PB8 dataset path with a valid manifest and is normally left at `null`; PB8 then resolves datasets automatically. Future unknown top-level backtest fields also appear in this fallback section.

## Queue

The V8 queue is stored separately under `data/bt_v8_queue`. **Start** launches `<venv_pb8>/bin/passivbot backtest <snapshot>`. **Stop**, **Restart**, **Delete**, and **Clear Finished** affect only V8 queue items.

Running backtests are independent jobs. Restarting PBGui or updating PB8 does not stop them, and they do not block an update. New starts remain queued while PB8 is being installed or updated and continue afterward. A new start always uses the PB8 installation available at that time; the version and Git commit are recorded on the queue item.

The queue row's log action opens `data/logs/backtests_v8/<queue-id>.log`. The top-right notification bell opens `PBGui.log`, which persists the short GUI notifications and errors that otherwise disappear after a few seconds. Technical PB8 backend diagnostics remain available separately in `BacktestV8.log`.

Open **Settings** from the Queue sidebar actions to enable **Start queued jobs automatically**, choose the number of parallel jobs, and select **Use PBGui Market Data**. PB7 and PB8 read and write this one shared configuration. The CPU value limits automatic jobs across both versions together. The market-data option is applied to a fresh copy of the immutable queue snapshot immediately before every start or restart, so changing it never mutates the saved config.

The Settings dialog opens immediately from the current state. It refreshes the host's authoritative values in the background and updates the visible controls only while they remain untouched.

PB8 stores reusable datasets under `pb8/caches/hlcvs_data` and temporary materialized runs under `pb8/caches/ohlcvs/materialized`. Cleanup coordinates with PB8's root operation lock and preserves active local runs, foreign-host locks, and malformed locks whose safety cannot be established. Only old unlocked runs or runs with a confirmed dead local owner are removed. The **Clean Now** result reports how many locked directories were preserved.

## Results

The editor sidebar's **Results** button is always available and opens the complete unfiltered result list, including while editing a new config with no own results. Global Results load newest-first in small pages and render after every page, with a live loaded/total count while older rows continue loading. The queue row's green result action instead requests only its config and applies that filter before opening **Results**. The **Version** filter defaults to PBv8 and can switch to PBv7 or **Both**. The list shows each result's version, config, active PB8 strategy, exchange, run directory, and compact scalar metrics. The sortable Strategy column appears whenever V8 rows are visible and remains absent from a pure V7 view. Final balance and equity use the terminal values recorded in PB8's `balance_and_equity.csv` or compressed `.csv.gz` artifact when analysis metadata does not provide explicit totals. Per-symbol PnL uses the authoritative fill timestamps and includes recorded trading fees, so its timing and net total remain comparable to the balance chart. Opening a result also provides a **Price (PBGui MarketData)** selector that overlays one configured exchange/coin close-price series as a muted dotted trace on a contrasting second Y-axis and reports coverage against the visible equity-chart range without hiding that chart. For combined results, PBGui initially selects a configured exchange with full visible price coverage when available; suite results use their real configured exchange as well. Select PBv7 and PBv8 rows together and click **Compare** to overlay their equity and balance series; each file is read from its matching result root. **Delete Selected** also supports a mixed selection and sends every result to its owning PBv7 or PBv8 backend.

PB8 may download historical data when a backtest starts. Review the config, exchanges, coin selection, dates, and PB8 migration report before running a large backtest.

The Results toolbar includes a persistent **Columns** picker. **Defaults** restores the established result table, while **All** also exposes available Final Equity and Equity/Balance Difference values. This browser-local selection is independent from Archive.

Panel navigation synchronously closes the Config editor sidebar before showing Results, Queue, Archive, or Refine actions, so delayed editor state cannot leave the wrong sidebar attached to the active panel.

## Archive

Archive Backtests have their own persistent **Columns** selection, so an archive-specific layout does not alter local Results.

PBv8 uses the same Archive panel and configured Git archives as PBv7. Results are stored below `pbgui/configs/<config_version>/backtests`, so V7 and V8 files cannot overwrite each other. Mixed archive lists show the owning version and the active PB8 strategy, with a sortable Strategy column whenever V8 rows are visible, and route charts, files, comparisons, deletion, rebacktest, and Retest & Replace through the matching backend. **Add to Run** on one PB8 archive result creates an unsaved PB8 Run draft for explicit review and saving. V8 retests use immutable `data/bt_v8_queue` snapshots.
