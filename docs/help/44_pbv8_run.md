# PBv8 Run

PBv8 Run manages Passivbot V8 live instances. PB7 and PB8 Run use the same editor template; a version adapter maps the visible controls to the correct config paths and API contracts.

## Run List

Open **PBv8 -> Run** to view configs stored below `data/run_v8`. PB7 and PB8 use the same responsive Run-list layout with sidebar search and status filters. The table shows the active PB8 strategy, exchange user, target host, config and running versions, exposure summary, confirmed running hosts, note, and PBCluster desired state. Strategy is sortable and included in list search.

The main **Status** column combines published desired state with exact PB8 process observations reported by the local runner and remote monitors:

- **synced** means the exact assigned PB8 process is running with the current config version.
- **outdated**, **sync needed**, and **stop needed** identify a confirmed runtime/config mismatch requiring reconciliation.
- **blocked** reports an actionable Cluster gate or PB8 runtime failure. Only when PBRun reports that the validated PB8 runtime is not ready, the Run list shows an **Open VPS Manager -> Update PB8** warning for the affected host. Cluster gates and ordinary process-exit failures do not show this update prompt. PBRun retries after the validated update completes.
- **PB8 update required** means the local master cannot load any PB8 config because its own PB8 runtime failed readiness validation. The persistent banner and Status cell show the exact safe runtime reason and link directly to **VPS Manager -> Update PB8**. A failure in only one config remains **config error** and shows that config's own loader reason instead of incorrectly requesting a runtime update.
- **collecting** means no exact process observation is available yet; PBGui does not guess that the bot is stopped.
- **disabled** means the desired target is disabled and no running process was reported.
- **conflicted** means concurrent cluster operations require resolution.

The separate **Desired** column remains the published Cluster request. The authenticated WebSocket refreshes both views and stale REST responses cannot overwrite newer socket state.

The row actions **P**, **G**, and **T** set global `panic`, `graceful_stop`, or `tp_only` for both long and short PB8 positions after explicit confirmation. Status shows the configured global mode separately from process synchronization. When either side is forced, **N** clears both global modes through a version-bound confirmation; new entries may resume only after the target applies the published config. Disabled sides stay disabled, per-coin and HSL overrides remain unchanged, and completed Panic closes cannot be undone. Each action uses the normal PB8 bundle pipeline: it creates a complete backup, increments the config version, validates the config and sparse overrides through PB8, publishes the Cluster operation, and attempts immediate target activation. The editor losslessly maps PB8's canonical forced-mode values to its visible selectors and back.

## Create Or Edit

The editor provides the same workflow as PBv7 Run:

- **User**, **Enabled on**, **Config version**, and **Note** manage deployment identity and PBGui metadata. As in PB7, the selected User is also the instance name; PBGui rejects a second live instance or a custom name for the same exchange user.
- **strategy_kind** is populated from metadata reported by the installed PB8 runtime and appears at the beginning of **Bot Configuration**. Changing it immediately replaces the active `bot.long.strategy` and `bot.short.strategy` keys, restores previously edited values when switching back, or loads runtime defaults for a strategy not yet configured. The synchronization is bidirectional: entering one supported strategy key in either Long or Short JSON also updates `strategy_kind` and switches the other side. Runtime-default strategy blocks are highlighted in red as **review** until their values are edited.
- The normal controls retain the familiar PB7 order from User and Enabled on through execution flags.
- Approved and ignored lists use PB8's official market resolver. Normal markets stay short, while real collisions such as `CAT` and `1000CAT` are shown with short labels but stored with PB8's exact exchange-qualified identifier. Imported exact native IDs, CCXT symbols, and namespaced identifiers remain unchanged; invalid or ambiguous values stay visible for correction.
- **Apply Filters** is an explicit sidebar action rather than a one-shot checkbox. Coin filters still use PBGui CoinData policy, but project every resolved result onto PB8's collision-safe market catalog instead of replacing exact identifiers; unavailable entries are reported and skipped while valid lists are retained.
- `dynamic_ignore` is shown disabled as a PB7-only runtime feature. PB8's supervisor does not watch PB7 dynamic list files, so PB8 Run uses the explicit lists written by Apply Filters instead of persisting a non-functional flag.
- **Coin Overrides** supports inline and sparse-file overrides. Exact PB8 market keys remain distinct, and referenced override files are saved together with the config as one exact bundle.
- Coin Override choices come from PB8's official policy for the active `strategy_kind` and `hsl_signal_mode`. HSL fields are offered only in `coin` mode. Explicit `false`, zero, and default values remain sparse overrides; `null` is invalid and omission means inheritance. Save validates inline and referenced files through PB8's runtime override parser.
- Long and short exposure and position controls map to `bot.<side>.risk`; the complete nested side configs remain editable as JSON.
- The normal and **Advanced Settings** sections expose every live, logging, and monitoring parameter reported by the installed PB8 runtime. PB8-only fee, order-churn, WebSocket-forager, startup, logging, and monitor controls are hidden automatically when an older runtime does not provide them.
- **Advanced Settings** includes PB8's `coin`, `pside`, and `unified` `hsl_signal_mode` choices; the installed template's default is retained when the editor is opened and saved.
- Structured controls, Long/Short JSON, and Raw JSON synchronize in both directions. Numeric zeroes, nullable auto values, unknown runtime fields, and unknown nested or top-level JSON are preserved rather than replaced by editor defaults.
- **Additional Parameters** is reserved for newly introduced runtime live fields that do not yet have dedicated controls. They remain editable and are preserved on save.
- PB8.1 has dedicated controls for WebSocket forager candles, `exchange_symbol_unavailable_cooldown_hours`, the four order-replacement churn-gate values, and Expert/Diagnostic `startup_phase_budgets`. Startup budgets affect reporting only and do not gate trading. Opening a v8.0 config normalizes these fields in memory through PB8; the source changes only after an explicit save.
- **Raw JSON** remains synchronized with the structured controls and preserves unknown top-level and nested fields.

Import, Copy, Backtest handoff, **Strategy Explorer**, live logs, and Raw JSON editing are available from the same sidebar workflow. PBGui AI may invoke the page-advertised `show_log` action for an exact active bot from the Run list or another page; PBGui keeps the action pending while it navigates through the bot editor and then reuses the sidebar's existing live-log function. Strategy Explorer receives the current unsaved PB8 config and every referenced sparse override through an authenticated opaque draft. The Import dialog provides searchable User suggestions and rejects names outside the configured exchange-user catalog. **Balance Calculator** opens the shared calculator with the current unsaved config, while **Calc Balance** calculates and can apply the recommended `balance_override` inline. Browser requests use the HttpOnly PBGui session cookie; no session token is rendered into the editor.

When a new Backtest/Archive handoff draft selects a user that already has a PB8 Run config, Save loads the authoritative current version and asks before replacing it. Confirmation backs up the existing bundle, increments from that current version, and syncs the selected `enabled_on` target; cancellation leaves the existing instance unchanged.

Every save runs through the installed PB8 prepare/save pipeline. PBGui validates the editor's expected version, atomically replaces the complete config-and-override directory under a cross-process lock, publishes an immutable manifest, and appends an explicit `UPSERT_PB8_CONFIG` operation. A running remote assignment is sent directly to its target in one Cluster bundle with a three-second transport limit; PBCluster remains the durable retry path if that fast activation cannot complete. PBRun polls PB8 desired state and config signatures every second so successful materialization is reconciled immediately. If operation publication or local placement fails, the previous local bundle is retained or restored.

## Backups

PBv8 Run uses the same **Backups** workflow as PBv7. Before an existing instance is overwritten or deleted, PBGui stores the complete previous bundle under `data/backup/v8`: `config.json` plus every referenced sparse override file. The retention setting controls how many versions are kept per instance.

Opening a backup creates a short-lived editor draft. Review it and use the normal Save action to restore it through PB8 validation, optimistic version handling, atomic bundle persistence, and Cluster publication. Deleting a backup affects only that immutable backup bundle.

PBRun supervises PB7 and PB8 through the same controller service. Restarting that controller does not stop already running bots; after startup it adopts matching processes again. Explicit disable, move, delete, runtime-profile changes, and Cluster tombstones still stop the affected bot.

## Eligible Hosts

The target list is fail-closed. A host appears only when one of these sources confirms PB8 capability and its reported `pb8_config_schema` is at least as new as the current config's `config_version`:

- The local `pb8_runtime_status` is ready.
- VPS Manager records runtime profile `pb8` or `pb7_pb8` and a successful setup.
- An unmanaged remote host reports a fresh `pb8ready` value through host metadata.

PB7-only, not-ready, stale, schema-incompatible, and unknown new targets are rejected with HTTP 409. For example, a `v8.1.0` config cannot target a host that reports only schema `v8.0.0`; update PB8 on that host first. An unchanged unknown target from an older saved config may remain selectable so the config can be edited without forcing an unsafe move; it cannot be selected for a new deployment.

## Cluster Rollout

PB8 live operations use a separate Cluster protocol namespace so older nodes can never interpret them as PB7 configs. Before the first PB8 save or delete, update every active Cluster state replica to a PBGui version that advertises `pb8_instances_v1` and wait for a fresh successful Cluster Sync pass. Until then, the API rejects PB8 publication with HTTP 409.

## Delete

Stop the instance before deleting. PBGui checks its exact local process even when it is disabled, reassigned, or cannot currently start. Relevant remote hosts must provide an explicit stopped observation from one snapshot generation no older than 90 seconds. Missing, stale, unstamped or inconsistent observations return HTTP 409 before backup, tombstone or directory removal. Fresh monitor diagnostics cannot make an older instance list current; wait for the next complete instance snapshot. After upgrading, the monitor service must load the new collector code before remote deletion can use these observations.

Delete publishes `DELETE_PB8_INSTANCE` before removing the local bundle. PB8 tombstones are separate from PB7 tombstones, so equal PB7 and PB8 instance names do not affect each other. Cluster Sync and PBRun consume the tombstone to stop and remove the PB8 deployment.
