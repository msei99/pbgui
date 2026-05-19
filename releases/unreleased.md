# Unreleased

- Fixed the local V7 bot log viewer to read native PB7 logs from the configured `pb7dir/logs`, offer each bot's `passivbot_err.log` as a selectable local log entry, group local PB7 archive logs under the bot with readable timestamps like the VPS view, keep the bot `error` entry last like VPS, keep `PBRun.log` selectable for local startup issues, and hide unrelated master logs/tasks when the Run Editor opens a local bot log.
- Replaced the removed PB7 `live.price_distance_threshold` setting with `live.initial_entry_exec_max_market_dist_pct` across the v7 editors and Live vs Backtest diagnostics, and stopped restoring the deprecated raw key in PBGui's PB7 config wrapper.
- Normalized v7 coin override config files through PB7's config pipeline when loading, saving, and importing them into run instances so old override files no longer keep stale live keys and deprecated params.
- Made a normal V7 instance save also re-normalize all referenced per-coin override files in the instance folder so simply opening and saving an instance upgrades stale override configs to the current PB7 schema.
- Added V7 instance rollback from backups for stopped existing instances, automatically creating a pre-restore safety backup before replacing the current config and override files, and showed backup creation timestamps in the Run backup dialog.
- Added a live filter field to the V7 instance backup dialog so large backup lists can be narrowed by instance name, backup ID, or displayed timestamp.
- Raised the shared dialog overlay above the V7 backup modal so rollback confirmation dialogs stay usable instead of rendering behind the backup window.
- Bumped the V7 Run dialog asset version so browsers reload the shared dialog script and pick up the rollback confirmation z-index fix immediately.
- Replaced the V7 rollback confirmation on the Run page with a local backup-dialog confirmation layer so the prompt always opens above the backup window instead of competing with the shared global dialog stack.
- Changed V7 rollback restore versioning so a restored stopped instance keeps the backup content but is re-saved as the previous current version plus one, ensuring PBRun recognizes it as a new activatable version.
- Extended V7 restore versioning for deleted instances too: when no current instance exists, the restored config now uses the highest numeric backup version plus one so restoring an older backup still produces a new activatable version.
