# DB Tools

DB Tools provides controlled maintenance and one-way migration workflows for PBGui databases, dashboard definitions, and templates across the local master and known remote masters. Open it from **System -> DB Tools**.

Every database write creates a backup first. Destructive actions require an explicit preview or confirmation, and remote operations use the hosts already configured in VPS Manager.

## Targets and safety

- **local** is the PBGui installation where the page is open.
- Remote targets are known master nodes from VPS Manager.
- Source and target must be different for copy and sync operations.
- PBData write activity can block an operation when changing the affected target safely is not possible.
- Database replacement and restore stop PBData on the target, remove stale SQLite WAL/SHM sidecars, install the files, and restart PBData if it was running before the operation.
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
3. creates target backups,
4. stops target PBData when required,
5. installs both databases and removes stale sidecars,
6. restarts PBData if it was previously running.

Prefer Copy User Data or Sync Jobs when only specific users need to be transferred.

## Sync Jobs

Sync Jobs periodically copy selected users from one source master to one or more targets.

- Sync is one-way from the configured source to every selected target.
- Jobs add only missing rows; they never delete or overwrite existing target rows.
- Each run creates target backups before writing.
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

Restore performs another safety backup before replacing any database. It stops PBData, installs the selected backup, removes stale SQLite sidecars, and restarts PBData when appropriate.

Delete permanently removes the selected backup files. Keep at least one verified recent backup before deleting older copies.

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
