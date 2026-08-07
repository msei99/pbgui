# PBv8 Run

PBv8 Run manages Passivbot V8 live instances. PB7 and PB8 Run use the same editor template; a version adapter maps the visible controls to the correct config paths and API contracts.

## Run List

Open **PBv8 -> Run** to view configs stored below `data/run_v8`. PB7 and PB8 use the same responsive Run-list layout with sidebar search and status filters. The table shows the active PB8 strategy, exchange user, target host, config and running versions, exposure summary, confirmed running hosts, note, and PBCluster desired state. Strategy is sortable and included in list search.

The main **Status** column combines published desired state with exact PB8 process observations reported by the local runner and remote monitors:

- **synced** means the exact assigned PB8 process is running with the current config version.
- **outdated**, **sync needed**, and **stop needed** identify a confirmed runtime/config mismatch requiring reconciliation.
- **blocked** reports an actionable Cluster gate or PB8 runtime failure. Only when PBRun reports that the validated PB8 runtime is not ready, the Run list shows an **Open VPS Manager -> Update PB8** warning for the affected host. Cluster gates and ordinary process-exit failures do not show this update prompt. PBRun retries after the validated update completes.
- **collecting** means no exact process observation is available yet; PBGui does not guess that the bot is stopped.
- **disabled** means the desired target is disabled and no running process was reported.
- **conflicted** means concurrent cluster operations require resolution.

The separate **Desired** column remains the published Cluster request. The authenticated WebSocket refreshes both views and stale REST responses cannot overwrite newer socket state.

## Create Or Edit

The editor provides the same workflow as PBv7 Run:

- **User**, **Enabled on**, **Config version**, and **Note** manage deployment identity and PBGui metadata. As in PB7, the selected User is also the instance name; PBGui rejects a second live instance or a custom name for the same exchange user.
- **strategy_kind** is populated from metadata reported by the installed PB8 runtime and appears at the beginning of **Bot Configuration**. Changing it immediately replaces the active `bot.long.strategy` and `bot.short.strategy` keys, restores previously edited values when switching back, or loads runtime defaults for a strategy not yet configured. The synchronization is bidirectional: entering one supported strategy key in either Long or Short JSON also updates `strategy_kind` and switches the other side. Runtime-default strategy blocks are highlighted in red as **review** until their values are edited.
- The normal controls retain the familiar PB7 order from User and Enabled on through execution flags.
- Coin filters, approved and ignored lists, and coin status validation use the same current CoinData mappings as PBv7.
- **Coin Overrides** supports inline and sparse-file overrides. Referenced override files are saved together with the config as one exact bundle.
- Long and short exposure and position controls map to `bot.<side>.risk`; the complete nested side configs remain editable as JSON.
- The normal and **Advanced Settings** sections expose every live, logging, and monitoring parameter reported by the installed PB8 runtime. PB8-only fee, order-churn, WebSocket-forager, startup, logging, and monitor controls are hidden automatically when an older runtime does not provide them.
- **Advanced Settings** includes PB8's `coin`, `pside`, and `unified` `hsl_signal_mode` choices; the installed template's default is retained when the editor is opened and saved.
- Structured controls, Long/Short JSON, and Raw JSON synchronize in both directions. Numeric zeroes, nullable auto values, unknown runtime fields, and unknown nested or top-level JSON are preserved rather than replaced by editor defaults.
- **Additional Parameters** is reserved for newly introduced runtime live fields that do not yet have dedicated controls. They remain editable and are preserved on save.
- **Raw JSON** remains synchronized with the structured controls and preserves unknown top-level and nested fields.

Import, Copy, Backtest handoff, live logs, and Raw JSON editing are available from the same sidebar workflow. The Import dialog provides searchable User suggestions and rejects names outside the configured exchange-user catalog. **Balance Calculator** opens the shared calculator with the current unsaved config, while **Calc Balance** calculates and can apply the recommended `balance_override` inline. Browser requests use the HttpOnly PBGui session cookie; no session token is rendered into the editor.

Every save runs through the installed PB8 prepare/save pipeline. PBGui validates the editor's expected version, atomically replaces the complete config-and-override directory under a cross-process lock, publishes an immutable manifest, and appends an explicit `UPSERT_PB8_CONFIG` operation. If operation publication or local placement fails, the previous local bundle is retained or restored.

## Backups

PBv8 Run uses the same **Backups** workflow as PBv7. Before an existing instance is overwritten or deleted, PBGui stores the complete previous bundle under `data/backup/v8`: `config.json` plus every referenced sparse override file. The retention setting controls how many versions are kept per instance.

Opening a backup creates a short-lived editor draft. Review it and use the normal Save action to restore it through PB8 validation, optimistic version handling, atomic bundle persistence, and Cluster publication. Deleting a backup affects only that immutable backup bundle.

PBRun supervises PB7 and PB8 through the same controller service. Restarting that controller does not stop already running bots; after startup it adopts matching processes again. Explicit disable, move, delete, runtime-profile changes, and Cluster tombstones still stop the affected bot.

## Eligible Hosts

The target list is fail-closed. A host appears only when one of these sources confirms PB8 capability:

- The local `pb8_runtime_status` is ready.
- VPS Manager records runtime profile `pb8` or `pb7_pb8` and a successful setup.
- An unmanaged remote host reports a fresh `pb8ready` value through host metadata.

PB7-only, not-ready, stale, and unknown new targets are rejected with HTTP 409. An unchanged unknown target from an older saved config may remain selectable so the config can be edited without forcing an unsafe move; it cannot be selected for a new deployment.

## Cluster Rollout

PB8 live operations use a separate Cluster protocol namespace so older nodes can never interpret them as PB7 configs. Before the first PB8 save or delete, update every active Cluster state replica to a PBGui version that advertises `pb8_instances_v1` and wait for a fresh successful Cluster Sync pass. Until then, the API rejects PB8 publication with HTTP 409.

## Delete

Delete publishes `DELETE_PB8_INSTANCE` before removing the local bundle. PB8 tombstones are separate from PB7 tombstones, so equal PB7 and PB8 instance names do not affect each other. Cluster Sync and PBRun consume the tombstone to stop and remove the PB8 deployment.
