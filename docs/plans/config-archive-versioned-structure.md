# Config Archive Versioned Structure Plan

## Goals

- Stop asking users to type an archive sub-path manually.
- Store archived content under a deterministic path derived from the Passivbot/PB7 config version.
- Keep existing backtest-result archive behavior working.
- Add support for sharing PBv7 Optimize settings through the same archive system.
- Migrate old archive layouts automatically where it is safe to do so.
- Keep remote archive changes explicit: local migration happens automatically, but pushing still uses the existing explicit Git Push action.

## Current State

- Backtest archive settings are stored in the `config_archive` INI section.
- The UI asks for `my_archive_path` in the Archive setup modal.
- `POST /archives/{name}/add-config` receives `source_path` and `dest_name` from the frontend.
- The backend copies a backtest result directory to `archive_dir / my_archive_path / dest_name`.
- Archive listing scans for `**/analysis.json` and treats every matching directory as a backtest result.
- Optimize settings are separate PB7 config JSON files under `data/opt_v7/*.json` and are managed by `api/optimize_v7.py`.
- Backtest and Optimize configs include a Passivbot/PB7 `config_version`; this is the version that should drive the archive path.
- Run/instance configs may additionally include `pbgui.version`; keep that as metadata only, not as the archive path version.

## Target Archive Layout

New writes should use this structure:

```text
archive-root/
  pbgui/
    archive_manifest.json
    configs/
      v7.4.2/
        backtests/
          my_config/
            bybit/
              2026-06-21T120000Z/
                config.json
                analysis.json
                ...
        optimize/
          my_optimizer_settings.json
          my_optimizer_settings.meta.json
      unknown/
        optimize/
          my_optimizer_settings__a1b2c3d4.json
          my_optimizer_settings__a1b2c3d4.meta.json
```

Rules:

- Use `config.config_version` when present and valid. This is the Passivbot/PB7 config version and applies to both Backtest and Optimize configs.
- Normalize the version into a safe path segment, e.g. `v7.4.2` stays `v7.4.2`; invalid characters are replaced.
- Store `pbgui.version` separately as metadata if it exists, but do not use it for the archive path.
- If no PB7 `config_version` exists, use `unknown` and add a short content fingerprint to avoid collisions.
- Sanitize all generated path parts server-side.
- Keep old paths readable.

## Archive Item Types

### Backtest Result

Source:

- Existing PB7/PBGui backtest result directories containing `config.json` and `analysis.json`.

Destination:

```text
pbgui/configs/v{version}/backtests/{config_name}/{exchange_or_combined}/{result_name}/
```

Derivation:

- `version`: from `config.json -> config_version`, fallback `unknown` plus fingerprint.
- `pbgui_version`: optional metadata from `config.json -> pbgui.version`.
- `config_name`: from `config.backtest.base_dir` last path segment, fallback existing archive result heuristic.
- `exchange_or_combined`: from `config.backtest.exchanges`; use `combined` for multiple or absent values.
- `result_name`: original result directory name or timestamp-like fallback.

### Optimize Settings

Source:

- Existing Optimize settings in `data/opt_v7/{name}.json`.
- Optional direct JSON payload from future UI flows.

Destination:

```text
pbgui/configs/{pb7_config_version}/optimize/{name}.json
pbgui/configs/{pb7_config_version}/optimize/{name}.meta.json
```

If the Optimize config has no PB7 `config_version`:

```text
pbgui/configs/unknown/optimize/{name}__{sha8}.json
```

Save behavior:

- Optimize configs should keep their Passivbot/PB7 `config_version` when saved.
- If an Optimize save flow prepares/normalizes config data, it must preserve or inject the current PB7 template `config_version`.
- Archive versioning for Optimize settings should not fall back to `v0` when `config_version` exists.

Metadata file should include:

```json
{
  "schema_version": 1,
  "type": "optimize_config",
  "name": "my_optimizer_settings",
  "pb7_config_version": "v7.4.2",
  "pbgui_version": "0",
  "fingerprint": "a1b2c3d4",
  "created_at": "2026-06-21T12:00:00Z",
  "source": "pbgui"
}
```

## Manifest

Add `pbgui/archive_manifest.json` for fast listing and future compatibility.

Decision: manifest hardening is enabled. Mutating archive operations rebuild `pbgui/archive_manifest.json` atomically; read-only paths keep scan fallback and do not mutate non-own archives.

Example:

```json
{
  "schema_version": 1,
  "items": [
    {
      "type": "backtest_result",
      "name": "my_config",
      "pb7_config_version": "v7.4.2",
      "pbgui_version": "12",
      "path": "pbgui/configs/v7.4.2/backtests/my_config/bybit/2026-06-21T120000Z",
      "fingerprint": "...",
      "created_at": "2026-06-21T12:00:00Z"
    },
    {
      "type": "optimize_config",
      "name": "my_optimizer_settings",
      "pb7_config_version": "v7.4.2",
      "pbgui_version": "0",
      "path": "pbgui/configs/v7.4.2/optimize/my_optimizer_settings.json",
      "fingerprint": "a1b2c3d4",
      "created_at": "2026-06-21T12:00:00Z"
    }
  ]
}
```

Implementation notes:

- Update manifest atomically with temp file plus `os.replace()`.
- If manifest is missing or invalid, use scan fallback; rebuild it on the next intentional archive mutation.
- Listing must not rely only on the manifest; it should remain able to scan legacy paths.

## Legacy Setting Handling

`my_archive_path` should not be used for new writes.

Backend behavior:

- `GET /archives/settings` may still return the old key for compatibility during transition.
- `POST /archives/settings` may tolerate the key but should ignore it for new behavior.
- Existing old settings must not break old browser tabs immediately.

Frontend behavior:

- Remove the Archive Path field from the setup modal.
- Show a short note: archive paths are generated from config version automatically.

## Automatic Migration Of Old Archives

Automatic migration should be as complete as possible but safe by default.

Decision: automatic migration only runs for the configured own archive (`my_archive`). Non-own archives stay read-compatible but are not mutated automatically after pull.

### What Counts As Legacy

Legacy backtest-result entries are directories containing `analysis.json` outside the new `pbgui/configs/` tree.

Examples:

```text
archive-root/my/manual/path/config_a/bybit/result_001/analysis.json
archive-root/pbgui/configs/pb7/config_a/bybit/result_001/analysis.json
archive-root/config_a/bybit/result_001/analysis.json
```

### Migration Trigger

Run automatic local migration for the configured `my_archive` only in these situations:

- before adding a new item to the archive;
- before Git Push;
- when opening the archive panel for `my_archive`, if the archive is clean and migration is quick.

For non-own archives:

- keep read compatibility;
- show a migration-available status;
- do not mutate after pull;
- do not expose automatic migration actions unless the archive is first configured as `my_archive`.

Reason: pulled public archives may not be intended to receive local structural changes.

### Git Safety

Before automatic mutation:

- check that the archive clone exists;
- check that it is a git repo;
- check `git status --porcelain`;
- if dirty, skip automatic migration and report `migration_skipped: dirty_worktree`.

Migration writes are local only. Remote state changes only after the existing explicit Push action.

### Migration Algorithm

1. Scan for legacy `analysis.json` files outside `pbgui/configs/`.
2. For each result directory, load `config.json` if present.
3. Derive target path with the new backtest-result path rules.
4. If target does not exist, move the full result directory to the new path.
5. If target exists:
   - compare content fingerprint;
   - if identical, remove the legacy duplicate directory;
   - if different, add suffix `__{sha8}` to the result directory name and migrate there.
6. Clean empty legacy parent directories up to the archive root.
7. Rebuild archive listing metadata and `pbgui/archive_manifest.json`.
8. Write a local migration report, e.g. `pbgui/archive_migration_report.json`.

### Migration Constraints

- Never follow symlinks outside the archive root.
- Never delete non-empty directories that contain files unrelated to the migrated result.
- Skip unreadable or malformed entries and keep them visible via legacy scanning.
- Preserve file contents exactly; migration is a move/re-layout, not a format rewrite.
- Do not rewrite `config.json` during migration.

### UI For Migration Status

Archive panel should show one compact status line for `my_archive`:

- `Archive layout: current`
- `Archive layout: migrated locally, push pending`
- `Archive layout: legacy entries detected`
- `Archive layout: migration skipped, dirty worktree`

No dedicated `Migrate Now` UI is planned because clean own archives migrate automatically during normal archive operations.

## Backend API Changes

Preferred helper module:

```text
api/archive_helpers.py
```

Responsibilities:

- version/fingerprint derivation;
- path derivation;
- path/name validation;
- manifest load/save/rebuild;
- legacy scan and migration;
- item listing.

Route changes:

- `POST /archives/{name}/add-config`
  - body should require only `source_path`;
  - `dest_name` becomes optional/deprecated;
  - server derives destination path.
- `POST /archives/{name}/add-optimize-config`
  - body: `{ "config_name": "..." }` initially;
  - copies from `data/opt_v7/{config_name}.json` using `load_pb7_config`/`save_pb7_config` semantics where appropriate.
- `GET /archives/{name}/optimize-configs`
  - lists archived Optimize settings.
- `POST /archives/{name}/optimize-configs/import`
  - imports an archived Optimize config into `data/opt_v7/{name}.json`.
- `POST /archives/{name}/results/rebacktest`
  - queues new local backtests from selected archived results without mutating the archive.
- `POST /archives/{name}/results/remove-liquidated`
  - removes liquidated archived result directories from `my_archive` only, with dry-run support.
- Optional later: `GET /archives/{name}/items` for a combined typed listing.
- Optional later: `POST /archives/{name}/migrate` for explicit migration of `my_archive` when automatic migration was skipped.

Keep existing routes working:

- `GET /archives/{name}/results` must still return backtest results from both new and old layouts.
- archive push/pull endpoints keep their existing explicit behavior.

## Frontend Changes

### Backtest Archive UI

- Remove Archive Path from setup modal.
- `addResultToArchive()` should stop calculating `destName`.
- Send only `{ source_path: p }`.
- Show migration status when viewing `my_archive`.
- Add `Rebacktest` action for archive result selection.
- Add `Remove Liquidated` action only when viewing `my_archive`.

### Optimize UI

- Add an action in `frontend/v7_optimize.html`: `Add Optimize Settings to Archive`.
- It should use the currently selected/saved Optimize config name.
- If unsaved changes exist, require save first or create a draft export flow in a later phase.
- Add archive import action for archived Optimize configs.

### Archive Panel

- Add tabs or filters:
  - `Backtest Results`
  - `Optimize Settings`
- Backtest table continues to use existing result renderer.
- Optimize Settings table needs columns:
  - name;
  - PB7 config version;
  - PBGui version, when available;
  - modified/created;
  - source path;
  - actions: import, view JSON.

## Archived Backtest Maintenance Flows

### Rebacktest Archived Results

It should be easy to run a new backtest from archived backtest results.

Behavior:

- Rebacktest can be used on any readable archive item because it does not mutate that archive.
- The action loads the archived `config.json`, opens a small parameter modal, and queues a new local PB7 backtest.
- Default modal values come from the archived config:
  - start date;
  - end date;
  - starting balance;
  - exchanges;
  - PBGui Market Data toggle.
- The new backtest result is written to the normal local backtest results area first.
- The user can archive the new result afterwards, or a later option can offer `Rebacktest and add to my archive`.

Recommended backend route:

```text
POST /archives/{name}/results/rebacktest
```

Request body:

```json
{
  "paths": ["/absolute/archive/result/path"],
  "overrides": {
    "start_date": "2022-01-01",
    "end_date": "2026-06-21",
    "starting_balance": 1000,
    "exchanges": ["bybit"],
    "use_pbgui_market_data": true
  }
}
```

Response:

```json
{
  "ok": true,
  "queued": 3,
  "queue_items": []
}
```

Implementation notes:

- Reuse existing queue creation logic rather than duplicating config launch behavior in the frontend.
- Validate every selected path is inside the selected archive root.
- Do not mutate the source archive item.
- For multiple selected results, keep one modal and apply the same override values to all.

### Remove Liquidated Archive Results

It should be possible to remove archived configs/results that liquidated.

Behavior:

- This mutates the archive, so it should only be available for `my_archive`.
- UI should offer:
  - `Remove Liquidated` for the current archive result view;
  - optional `Dry Run` preview listing affected items;
  - confirmation modal before deletion.
- Detection should use existing archive result liquidation logic:
  - `analysis.liquidated` if present;
  - fallback to drawdown / equity-balance diff / final-balance threshold logic already used in archive listing.

Cleanup modes:

- `selected_results`: remove only selected archived result directories that are liquidated.
- `visible_results`: remove all visible/filter-matched archived result directories that are liquidated.
- `config_if_all_results_liquidated`: backend-only safety scope; not exposed in the UI because selected/visible result cleanup already removes empty parent config groups when all matching liquidated results are deleted.

Safety rule: never delete a config group that still contains at least one non-liquidated backtest result unless the user explicitly selects those result directories for deletion through the existing manual delete flow.

Recommended backend route:

```text
POST /archives/{name}/results/remove-liquidated
```

Request body:

```json
{
  "paths": [],
  "scope": "visible_results",
  "dry_run": true
}
```

Rules:

- `paths` limits cleanup to selected result paths.
- Empty `paths` with `scope: visible_results` allows the UI to send the currently filtered visible paths.
- `scope: config_if_all_results_liquidated` works on config groups derived from the selected/visible paths.
- Backend should not infer frontend filters by itself; the frontend should pass selected/visible paths explicitly.
- Deletion removes the archived result directory, then removes empty parent directories up to the archive root.
- Config-group cleanup removes the config group directory only after verifying all contained result directories are liquidated.
- After deletion, rebuild the archive listing and migration status.
- Remote deletion happens only when the user explicitly runs Git Push later.

Response:

```json
{
  "ok": true,
  "dry_run": true,
  "matched": 4,
  "removed": 0,
  "items": [
    {"path": "...", "config_name": "...", "reason": "analysis.liquidated"}
  ]
}
```

## Collision Handling

Backtest results:

- identical target: skip duplicate or remove migrated legacy duplicate;
- different target content: suffix result directory with `__{sha8}`.

Optimize settings:

- identical `{name}.json`: skip duplicate;
- different content with same name/version: write `{name}__{sha8}.json`;
- import conflict with local `data/opt_v7/{name}.json`: return 409 and let UI choose overwrite/import-as-copy/cancel.

## Testing Plan

Backend tests:

- add-config derives `pbgui/configs/v7.4.2/...` from `config.json -> config_version`.
- add-config works without `dest_name`.
- add-config falls back to `unknown` with fingerprint when no PB7 `config_version` exists.
- old archive paths remain listed by `/archives/{name}/results`.
- migration moves legacy backtest results into new layout.
- migration skips dirty git worktree.
- migration only runs automatically for `my_archive`.
- non-own archives are never mutated after pull.
- migration handles target collision with identical and different content.
- optimize config export writes under `pbgui/configs/vX/optimize/`.
- optimize config export uses the PB7 `config_version`, not `pbgui.version`.
- optimize config import detects name collision.
- archived-result rebacktest queues local backtests without changing archive contents.
- remove-liquidated dry-run reports liquidated archived results without deleting.
- remove-liquidated refuses non-own archives.
- remove-liquidated deletes matching result directories in `my_archive` and leaves remote changes pending until explicit Git Push.
- manifest rebuild writes `pbgui/archive_manifest.json` after archive mutations.

Frontend checks:

- setup modal no longer shows Archive Path.
- adding a backtest result succeeds without `dest_name`.
- optimize config can be exported and listed.
- import conflict modal is shown.
- migration status is visible.
- rebacktest action is available for selected archived results.
- remove-liquidated action is visible only for `my_archive`.
- remove-liquidated preview uses a confirmation modal before deletion.

## Rollout Phases

### Phase 1: Backend Path Derivation For Backtest Results

- Add helper module.
- Keep current routes.
- Make `dest_name` optional.
- Derive new versioned path server-side.
- Keep legacy listing.

### Phase 2: UI Simplification

- Remove Archive Path from setup modal.
- Stop sending `dest_name` from frontend.
- Display generated-path behavior in help text.

### Phase 3: Automatic Local Migration

- Add safe migration helper.
- Auto-run for `my_archive` before add/push when clean.
- Add migration status to archive panel.
- Do not mutate non-own archives after pull.

### Phase 4: Optimize Settings Sharing

- Add Optimize export/import API routes.
- Ensure Optimize saves preserve or inject PB7 `config_version`.
- Add Optimize archive actions in the frontend.
- Extend archive panel with Optimize Settings tab.

### Phase 5: Archived Backtest Maintenance

- Add backend route and frontend action for rebacktesting archived results.
- Add backend route and frontend action for liquidated-result cleanup in `my_archive`.
- Require dry-run preview and confirmation before deleting liquidated archived results.

### Phase 6: Manifest Hardening

- Implemented after explicit user approval.
- Write and rebuild manifest after archive mutations.
- Use manifest for fast archive counts with scan fallback.
- Keep scan fallback for compatibility.

## Finalized Decisions

- Automatic migration runs only for the configured own archive (`my_archive`).
- Non-own archives are read-compatible only and are not mutated automatically after pull.
- Backtest and Optimize archive paths use the PB7/Passivbot `config_version`.
- Optimize configs should preserve or receive the PB7 `config_version` on save.
- `pbgui.version` is metadata only for this archive layout.
- Manifest hardening is enabled; `pbgui/archive_manifest.json` is rebuilt after intentional archive mutations.
