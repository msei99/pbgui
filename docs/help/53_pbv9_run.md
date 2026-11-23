# PB9 Run

PB9 is optional and experimental. Updating PBGui does not install or start PB9.
Before saving an enabled PB9 instance in an existing Cluster, update PBGui and
its running Cluster processes on every active state replica and wait for their
PB9 capability confirmation. This applies to instances assigned to the local
master too. Only PB9 execution hosts need a Passivbot 9 installation. The shared
operation stream cannot be read by PB9-unaware older PBGui versions afterwards.
Remote/live-trading acceptance is still outstanding; see the PB9 development
status linked from the repository README.

Importing a config discards inactive strategy values from the previous document. Raw changes to the account or strategy tree also clear that cache; unrelated Raw edits keep it. Switching strategies within the same document keeps your edits. Deactivation and destructive changes are synchronized with local process starts.

Open **PBv9 → Run** to manage PB9 live instances. The list and editor share the PB7/PB8 Run interface. PB9 uses its own source tree, Python environment, instance directory and backups.

## Configure an instance

The basic live controls stay visible, as in PB8; Advanced Settings groups the remaining live, logging and monitoring fields. PB9 shows its native field names and units. In particular, risk_bootstrap_max_lookback_minutes is a separate startup setting, not the removed PB8 PnL lookback.

Long and short JSON reuse the shared editor. **With global defaults** displays inherited side settings; only changed values become explicit overrides. **Overrides only** shows the sparse authored side. Delete a property there to inherit it again. Unchanged defaults stay absent from the saved side, and existing explicit values remain pinned. Side activation is always explicit. The view selector is preserved on browser reload.

Choose **Add PB9 Instance**, select an exchange account and edit the strategy, portfolio, global defaults and long/short overrides. The form reads the installed PB9 field registry and strategy metadata. An inherited side value remains inherited until you explicitly override it. Enable each intended side explicitly.

Coin selections use PB8's shared catalog algorithm with the native PB9 resolver and existing local public market metadata. Normal coins appear once by name; exact identifiers distinguish genuinely ambiguous markets. Existing exact selections retain their identity. Apply Filters shares PB8's filter projection. Coin overrides are inline PB9 patches. File-backed overrides and external config references are rejected.

The catalog uses the native venue's default settlement quote: USDT for standard venues and USDC for Hyperliquid/Lighter. Hyperliquid HIP-3 namespaces and numeric native market IDs remain distinct.

Save creates a new config version. Existing instances require the version you opened; conflicting saves are rejected. PBGui stores its runtime, version, note and host assignment in a private sidecar; the native config contains only Passivbot fields. Raw JSON is available for advanced editing.

Unregistered fields from the installed prototype/template or loaded config appear under **Advanced Settings → Additional Parameters** until a dedicated control is available. Unedited defaults remain inherited; editing and removing an explicit value preserve native types and inheritance.

## Local and Cluster operation

**Enabled on → disabled** saves without scheduling trading. Selecting this PBGui host schedules the instance for the PBRun daemon. A working separately installed PB9 runtime, current Rust extension, available account credentials and running PBRun service are required. A conflicting PB7/PB8/PB9 assignment for the same account blocks activation. Saving a config never bypasses these start checks.

After updating PBGui, restart the local API and PBRun services to load the new code. Use **System → PBGUI Services** for the individual services. The restart indicator shows services still running an older code serial.

**Enabled on** also offers joined, reachable Cluster hosts with fresh PB9 runtime/schema metadata. All active state replicas must confirm the PB9 operation protocol before enabling or moving a managed instance. Install/update PBGui and the separate PB9 runtime on the intended target, and complete Cluster Join first. Missing, old or incompatible host metadata keeps the target unavailable; the existing automatic monitoring updates the selector.

Saving a managed instance publishes a signed PB9 operation and an immutable native-config/sidecar manifest through the same Cluster transport as PB8. The response may remain pending until automatic reconciliation. PBRun checks the assigned node, complete manifest and version before starting; PB8 and PB9 states remain separate even for identical names. Existing local PB9 bundles remain local until explicitly saved into Cluster. A replicated name cannot overwrite an unrelated local bundle without matching its exact predecessor manifest. Deactivation of an already managed instance remains possible when a peer is unavailable; pending sync does not confirm a remote stop.

Use **Backtest** to pass the current editor config to the shared PB9 Backtest page. Strategy Explorer remains pending. Changing a host assignment can start or stop trading; use only accounts and settings you intend to run.

To move an existing instance, select its new host and Save. PBGui first publishes a disabled source version through the existing Cluster transport, waits for a confirmed stop, then saves and activates the requested target. The target is checked again before publication. If the stop cannot be confirmed within 30 seconds, the instance stays disabled; the editor preserves your draft and acknowledges the disabled version so Save can be retried. A remote stop must match that disabled config generation. Concurrent saves cancel the move rather than overwriting newer state.

## Backups and logs

Previous saved versions are backed up with the PBGui sidecar. Open a backup as an editor draft or restore it to a new version. Restoring initially disables the instance. Running instances must be stopped before deletion. The shared log panel follows the saved execution host. While an enabled remote host has no log available, it keeps that host selected instead of displaying old local output. Disabled instances retain local and backup log access. Previously detected targets remain visible but disabled when Cluster readiness fails; eligibility updates automatically without changing your selection. Restart stops the exact managed process so PBRun can relaunch it through its regular checks.

The list updates automatically. Browser refresh preserves its search, sorting and navigation context. The Guide opens over the current page; Information → Help opens the complete documentation.

## Current PB9 risk layout

PBGui requires the post-PR #2318 PB9 risk layout. Older PB9 development configs
are shown as incompatible and cannot be edited, saved, queued, started or resumed.
Delete them explicitly and create new configs from the current native template.
PBGui does not migrate, rewrite or automatically delete them. Existing stop and
delete safeguards for enabled or running instances remain in place. Old result
charts and metrics remain readable; launching their old configuration is blocked.

Directional HSL and Unstuck settings are under `bot.<scope>.risk`. Portfolio HSL
is under `bot.portfolio.risk.hsl`. The shared realized-loss memory is configured
under `bot.portfolio.risk.loss_realization.memory_ema_span_minutes`. Its loss gate
has an explicit `enabled` switch and a separate `max_realized_drawdown_fraction`
in `[0, 1)`, required only when enabled. The old `unstuck.close_fraction` is removed.
These fields use the normal GUI controls and native parameter help.

GPU execution requires a policy supported by the installed PB9 native preflight.
In this revision, active Unstuck or the realized-loss gate requires CPU, including
scenario and coin overrides. PBGui rejects unsupported GPU requests before launch
or rental and never silently disables risk settings. A worker built for a different
PB9 source revision cannot run the job.

The existing reviewed PB8-to-PB9 converter uses the installed official converter
and must produce this current layout. It creates an unsaved draft; it does not
convert existing PB9 development configs. Update the separate PB9 runtime if the
editor reports that its native template is too old.

If a new editor cannot initialize because the installed PB9 runtime predates the required risk layout, its metadata error now shows the update requirement. Update PB9 separately when you choose to; opening the editor does not install, migrate or delete anything.
