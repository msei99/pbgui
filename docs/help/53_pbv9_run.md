# PB9 Run

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

Before moving an existing instance to another host, disable it and wait for a fresh confirmed stop. The disabled config remains assigned to its previous host until that proof arrives.

## Backups and logs

Previous saved versions are backed up with the PBGui sidecar. Open a backup as an editor draft or restore it to a new version. Restoring initially disables the instance. Running instances must be stopped before deletion. Use the shared log panel to inspect local runtime output. Restart stops the exact managed process so PBRun can relaunch it through its regular checks.

The list updates automatically. Browser refresh preserves its search, sorting and navigation context. The Guide opens over the current page; Information → Help opens the complete documentation.
