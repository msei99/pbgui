# DB Tools

DB Tools provides controlled maintenance and one-way migration workflows for PBGui databases, dashboard definitions, and templates across the local master and known remote masters. Open it from **System -> DB Tools**.

Every manual database maintenance write creates a backup first. Destructive actions require an explicit preview or confirmation, and remote operations use the hosts already configured in VPS Manager.

## Targets and safety

- **local** is the PBGui installation where the page is open.
- Remote targets are known master nodes from VPS Manager.
- Source and target must be different for copy and sync operations.
- PBData write activity can block an operation when changing the affected target safely is not possible.
- Manual cleanup, user copy, complete DB copy and restore reserve the target exclusively before confirming PBData shutdown and creating backups. A failed stop or unknown service state blocks the operation rather than being treated as an already stopped writer.
- Restore changes existing SQLite contents in place, preserving open connections, file identity and WAL handling. PBData restarts only after a consistent result or successful rollback, and only if it was previously running.
- Remote maintenance uses one target-owned worker/lease for the complete operation. Older remote checkouts or API processes without the writer guards are rejected; update and restart the remote PBGui installation explicitly before retrying. DB Tools does not deploy updates automatically.
- Long-running operations appear in the progress panel and can also be inspected through the DB Tools log viewer.

Do not close or restart the API server while an attached DB Tools operation is running. The restart control reports a blocker until the operation finishes.

## Cleanup User Data

Cleanup removes rows belonging to selected users from `pbgui.db` and `pbgui_trades.db` on one target.

1. Select the target master.
2. Choose **Remove all data for users** or **Remove data older than date**.
3. Select one or more users.
4. Click **Preview** and verify the row counts by database and table.
5. Click **Run Cleanup** only after the preview matches your intent.

The date mode removes rows older than the UTC cutoff from tables with timestamp columns. A fresh preview is required after changing the target, mode, date, or selection.

The target reservation covers backup, every table commit and connection cleanup. If a cleanup or copy fails, the operation attempts to restore the complete pre-operation target bundle before allowing writers to continue.

## Copy User Data

Copy User Data transfers selected users between two masters.

| Mode | Behavior |
|---|---|
| **Add only missing** | Inserts rows that are absent on the target and keeps existing target rows. |
| **Replace user data** | Deletes the selected users' target rows first, then imports their source rows. |

Always preview before copying. The preview checks source users, target safety, and the expected operation. Replace mode is intentionally destructive on the target for the selected users.

## Copy Complete Database

This operation replaces both `pbgui.db` and `pbgui_trades.db` on the target with the source master's files.

Use it only when the target should become a complete database copy of the source. The operation:

1. validates source and target,
2. stages consistent source snapshots,
3. reserves the target and confirms PBData shutdown,
4. prepares all source and target recovery snapshots before changing any target DB,
5. restores both databases through SQLite without replacing connected files,
6. restarts PBData if it was previously running.

Prefer Copy User Data or Sync Jobs when only specific users need to be transferred.

## Sync Jobs

Sync Jobs periodically copy selected users from one source master to one or more targets.

- Sync is one-way from the configured source to every selected target.
- Jobs add only missing rows; they never delete or overwrite existing target rows.
- Scheduled sync retains its row-level behavior and does not create manual maintenance snapshots. Ordinary scan/sync leases remain shared; an exclusive maintenance reservation blocks overlapping writes without killing persistent jobs.
- The minimum interval is 30 seconds.
- A target is skipped or blocked when PBData is actively writing data for a selected user.

Recommended setup:

1. Create a job and give it a clear source-to-target name.
2. Select one source, at least one target, and the users to replicate.
3. Run **Check Safety**.
4. Use **Run Now** for an initial controlled run.
5. Review progress and logs.
6. Enable the job only after the manual run succeeds.

Saved enabled jobs are reconstructed after an API restart. Detached sync runs are independent jobs and remain visible through their persisted job state and logs.

## Backup Manager

The Backup Manager lists backups created by DB Tools for one master. Sort or select rows to restore or delete files.

Restore reserves the target, confirms PBData shutdown and creates another safety backup before changing any database. All replacements are prepared first. A failure or cancellation before the consistent completion point rolls back the touched databases, including removal of an optional DB created by the failed operation. Existing databases are restored in place; their live WAL/SHM files are not deleted.

Local backup creation has a 30-second per-file deadline that includes SQLite busy retries. Backups and validation run outside the API event loop; cancellation waits for their cleanup before releasing maintenance leases. Snapshot names are unique and publication does not overwrite existing backups. Staged schemas are allowlisted before integrity checking: executable schema objects, unsupported expressions and oversized SQLite values are rejected. A failed validation never reaches installation.

Remote database backups also use unique names and a 30-second SQLite deadline. A complete WAL-aware snapshot is built privately before atomic publication. A filename collision fails without replacing an existing file or symlink; a failed backup removes only its own temporary files. This protects earlier recovery snapshots even when two requests target the same backup name.

Delete permanently removes the selected backup files. Keep at least one verified recent backup before deleting older copies.

## Interrupted maintenance recovery

The private journal `data/locks/db-tools-recovery.json` records the original existence of each DB and durable undo snapshots before publication. After an interrupted publication or failed rollback, cooperative writes and PBData/Database startup remain blocked. The API restart control also reports the pending recovery. Never delete the journal to bypass this protection.

Use the authenticated `POST /api/db-tools/maintenance/recover` endpoint with `{"target":"local"}` or the selected remote master name. It returns the usual operation progress handle. Recovery rolls back an incomplete publication; if consistency was already recorded, it only reconciles writer restart and cleanup. It does not execute a new cleanup/copy/restore request. If API startup is blocked, an operator can run `python -m db_maintenance recover` from the affected PBGui checkout using that installation's PBGui Python environment. Recovery failure leaves the journal and snapshots intact for repair; inspect the DB Tools log.

After a lost remote response, the initiating master retains a `db-tools-remote-*.json` receipt and blocks restart until explicit target recovery establishes the outcome. Do not assume a disconnected request stopped its remote worker. Cancellation waits for the owned worker and rollback; a cancellation received after consistency was published does not undo a completed operation.

The SSH transport never automatically resends maintenance or recovery commands, even after a timeout or lost reply; read-only preflight requests retain their normal retry behavior. Recovery also removes recognized private `.sqlite-backup-*` and `.sqlite-restore-*` staging remnants left by a crash inside a SQLite helper. Symlinks, unexpected files and staging belonging to another operation are not followed or recursively deleted.

Safety backups remain available in Backup Manager. Private recovery staging is removed only after consistency and writer restart have been reconciled. This is recoverable multi-file maintenance, not a single crash-atomic SQLite transaction across both files. All PBGui writers must run the updated guards; arbitrary external SQLite tools or old peers bypassing the protocol are outside this coordination.

## Dashboards

Dashboards copies dashboard JSON files and dashboard template JSON files between masters.

| Mode | Behavior |
|---|---|
| **Add only missing** | Creates items that do not exist on the target and skips existing names. |
| **Replace all selected** | Replaces existing selected items and creates missing ones. |

Existing files are backed up before replacement. Preview the selected dashboards, templates, source, target, and mode before starting the copy.

## Troubleshooting

- **Target unavailable**: verify the host and SSH status in VPS Manager.
- **Safety check blocked**: stop or wait for the reported PBData writer, then run the check again.
- **Operation already running**: wait for the active DB Tools task to finish and inspect its progress or log.
- **Sync job skipped a target**: open the job log and check PBData activity, user availability, and target connectivity.
- **Restore or copy failed**: do not retry blindly. Inspect the log and verify that the automatically created target backup exists first.

## Best practices

1. Preview every manual write and confirm source and target names.
2. Start with **Add only missing** when existing target data must be preserved.
3. Test a sync job with **Run Now** before enabling its schedule.
4. Keep recent backups until the changed target has been verified.
5. Avoid simultaneous maintenance operations against the same target.
