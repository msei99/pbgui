# PB9 Run

Open **PBv9 → Run** to manage PB9 live instances. The list and editor share the PB7/PB8 Run interface. PB9 uses its own source tree, Python environment, instance directory and backups.

## Configure an instance

Choose **Add PB9 Instance**, select an exchange account and edit the strategy, portfolio, global defaults and long/short overrides. The form reads the installed PB9 field registry and strategy metadata. An inherited side value remains inherited until you explicitly override it. Enable each intended side explicitly.

Coin selections use published local Coin Data mappings. Exact venue-qualified market identifiers preserve distinctions between independently traded markets with the same coin name. Coin overrides are inline PB9 patches. File-backed overrides and external config references are rejected.

The catalog uses the native venue's default settlement quote: USDT for standard venues and USDC for Hyperliquid/Lighter. Hyperliquid HIP-3 namespaces and numeric native market IDs remain distinct.

Save creates a new config version. Existing instances require the version you opened; conflicting saves are rejected. PBGui stores its runtime, version, note and host assignment in a private sidecar; the native config contains only Passivbot fields. Raw JSON is available for advanced editing.

## Local operation

**Enabled on → disabled** saves without scheduling trading. Selecting this PBGui host schedules the instance for the PBRun daemon. A working separately installed PB9 runtime, current Rust extension, available account credentials and running PBRun service are required. A conflicting PB7/PB8/PB9 assignment for the same account blocks activation. Saving a config never bypasses these start checks.

After updating PBGui, restart the local API and PBRun services to load the new code. Use **System → PBGUI Services** for the individual services. The restart indicator shows services still running an older code serial.

PB9 currently supports local operation. Remote hosts, backtests and Strategy Explorer require their own PB9 integration and are not offered as PB8 fallbacks. Changing a host assignment can start or stop trading; use only accounts and settings you intend to run.

## Backups and logs

Previous saved versions are backed up with the PBGui sidecar. Open a backup as an editor draft or restore it to a new version. Restoring initially disables the instance. Running instances must be stopped before deletion. Use the shared log panel to inspect local runtime output. Restart stops the exact managed process so PBRun can relaunch it through its regular checks.

The list updates automatically. Browser refresh preserves its search, sorting and navigation context. The Guide opens over the current page; Information → Help opens the complete documentation.
