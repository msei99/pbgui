# Architecture: SSH API Sync

> Replaces the rclone-based API key distribution with a direct SSH/SFTP approach.
> First use-case: `api-keys.json` sync to all VPS.
> Designed to be generic enough for future config sync (PB7 configs, etc.).

---

## 1. Goals

| # | Goal |
|---|------|
| 1 | Push `api-keys.json` from Master → VPS(es) via SSH/SFTP (no rclone dependency) |
| 2 | Instant notification from VPS → other Masters when file changes (multi-master) |
| 3 | Support PB6 **and** PB7 target directories on the VPS |
| 4 | Selective VPS targeting (individual or "All") |
| 5 | Backup retention on VPS (configurable via VPS `pbgui.ini` `[filesync]` section) |
| 6 | Bot restart after push (always — no toggle) |
| 7 | Dry-run mode (preview what would happen without writing) |
| 8 | MD5 verify after every push |
| 9 | Push-history log (via `[FileSync]` tag in shared `PBGui.log`) |
| 10 | Generic `FileSyncWorker` reusable for future file types |
| 11 | Generic remote `pbgui.ini` read/write over existing SSH sessions |
| 12 | Backups follow existing PBRemote convention (`data/backup/api-keys_v7/` + `data/backup/api-keys/`) |

---

## 2. Metadata in `api-keys.json`

Every push writes these underscore-prefixed metadata fields at the top level of the JSON:

```json
{
    "_sync_serial": 42,
    "_sync_by": "manibot01",
    "_sync_ts": "2026-03-26T14:30:00Z",
    "_sync_lock": "manibot51",
    "user1": { "exchange": "binance", ... }
}
```

| Field | Always set? | Purpose |
|-------|-------------|---------|
| `_sync_serial` | **Yes** | Monotonically incrementing integer. Bumped on every push. |
| `_sync_by` | **Yes** | Hostname of the Master that pushed this version. |
| `_sync_ts` | **Yes** | ISO 8601 UTC timestamp of the push. |
| `_sync_lock` | **No** (optional) | If set: the file is locked to **this one VPS** for testing. Other Masters seeing this field must **not** pull. Only used for targeted single-VPS pushes. |

### `_sync_lock` semantics

- When a Master pushes to a **single** VPS for testing, it sets `_sync_lock = "<vps_hostname>"`.
- On the VPS itself, bots read the file normally (lock is transparent to PB6/PB7).
- Other Masters running an inotifywait watcher see the change → read metadata → see `_sync_lock` is set → **skip** the pull.
- When the user later pushes to "All" (production push), `_sync_lock` is removed.

---

## 3. End-to-End Push Flow

```
User clicks "SSH API Sync"
         │
         ▼
┌──────────────────────┐
│  1. Pre-checks       │  Read local api-keys.json
│     - VPS reachable? │  Validate JSON, build diff preview (dry-run)
│     - Paths known?   │  pb6dir/pb7dir from cached VPS ini (read on connect)
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│  2. Metadata update  │  _sync_serial++
│                      │  _sync_by = local hostname
│                      │  _sync_ts = now (UTC)
│                      │  _sync_lock = vps_hostname (only if single-VPS push)
│                      │  Write updated JSON to local file
└─────────┬────────────┘
          │
          ▼  (parallel per VPS)
┌──────────────────────────────────────────────────┐
│  3. Per-VPS push                                 │
│                                                  │
│  a) Backup remote api-keys.json                  │
│     → data/backup/api-keys_v7/{timestamp}/        │
│       api-keys.json (matches PBRemote pattern)    │
│     → data/backup/api-keys/{timestamp}/           │
│       api-keys.json (if pb6dir set)               │
│                                                  │
│  b) SFTP push api-keys.json to:                  │
│     → {pb7dir}/api-keys.json (if pb7dir set)     │
│     → {pb6dir}/api-keys.json (if pb6dir set)     │
│                                                  │
│  c) MD5 verify (read back + compare hash)        │
│                                                  │
│  d) Retention cleanup                            │
│     → Read [filesync] from VPS pbgui.ini         │
│     → Delete backups older than configured days  │
│     → Keep at least configured min versions      │
│                                                  │
│  e) Kill affected bots (SIGTERM)                 │
│     → Only AFTER verify succeeds                 │
│     → PBRun auto-restarts them with new keys     │
└──────────────────────────────────────────────────┘
          │
          ▼
┌──────────────────────┐
│  4. Result & logging │  Per-VPS success/failure
│                      │  Written to data/logs/PBGui.log ([FileSync] tag)
│                      │  Returned via REST response
└──────────────────────┘
```

### Bot kill/restart detail

- `kill_instance()` in `master/async_monitor.py` sends SIGTERM via SSH.
- PBRun (the daemon on each VPS) detects the bot died and auto-restarts it.
- Bots pick up the new `api-keys.json` on restart.
- Bots are killed only **after** the SFTP push + MD5 verify both succeed.

---

## 4. Instant Notification (VPS → Master)

Multi-master setups need instant propagation: when Master A pushes to a VPS, Master B should learn about the new version without polling.

### Mechanism: `inotifywait` over existing SSH sessions

```
Master B                        VPS (manibot51)
   │                                 │
   │  SSH (persistent, via pool)     │
   │─────────────────────────────────│
   │                                 │
   │  start_process():               │
   │  inotifywait -m -e close_write  │
   │    {pb7dir}/api-keys.json       │
   │─────────────────────────────────│
   │                                 │
   │          (file event)      ◄────│  Master A pushes new file
   │                                 │
   │  Callback on Master B:          │
   │  1. SFTP read _sync_serial,     │
   │     _sync_by, _sync_lock        │
   │  2. If _sync_lock set → SKIP    │
   │  3. If _sync_serial > local     │
   │     → SFTP pull file            │
   │     → Install locally           │
   │     → Backup local version      │
   │     → Restart affected bots     │
   └─────────────────────────────────┘
```

### Key behaviors

- **Started on SSH connect**: When `AsyncSSHPool` connects to a VPS, `FileSyncWorker` starts an `inotifywait` process via `start_process()`.
- **Auto-restart on reconnect**: If SSH connection drops and reconnects, watcher is restarted automatically.
- **`_sync_lock` respected**: Watcher callback checks `_sync_lock` — if set, the file is for testing on that VPS only. Master B does **not** pull.
- **Serial comparison**: Watcher callback only pulls if remote `_sync_serial` > local `_sync_serial`. Prevents pulling your own push back.
- **Latency**: < 1 second from file write on VPS to notification on Master B.

---

## 5. Existing Infrastructure (what we build on)

### `master/async_pool.py` — AsyncSSHPool

Pure asyncssh connection pool. Persistent connections with keepalive, auto-reconnect with exponential backoff.

| Method | Purpose |
|--------|---------|
| `run(hostname, command)` | Execute command, wait for result |
| `start_process(hostname, command)` | Start long-running process (for inotifywait) |
| `connect(hostname)` / `disconnect(hostname)` | Connection lifecycle |
| `health_check()` | Check all connections, detect dead ones |
| `reconnect_lost(enabled_hosts)` | Reconnect with backoff |

**Constants**: `REMOTE_PBGUI_DIR = "software/pbgui"`, connect timeout 10s, keepalive 10s.

**`VPSConnection` dataclass** has a `data` field — natural place to cache per-host info (remote paths, ini config) after connect. Populated on SSH connect, available for the connection lifetime.

**Missing** (to be added): SFTP methods, remote ini read/write (see Section 6.1 + 6.2).

### `master/async_monitor.py` — VPSMonitor

Orchestrates monitoring: system metrics, instances, services, alerts.

| Method | Purpose |
|--------|---------|
| `kill_instance(hostname, name, pb_version)` | Kill bot via SIGTERM over SSH |
| `_metrics_stream(hostname)` | Ongoing system metrics via SSH process |
| `_check_and_heal_services(hostnames)` | Service health checks + auto-restart |

### `master/async_store.py` — VPSStore

In-memory data store. Writers (monitor tasks) update data, readers (WebSocket) consume.

| Field | Type | Purpose |
|-------|------|---------|
| `instances` | `dict[str, list[dict]]` | Per-host bot instance data |
| `changed` | `asyncio.Event` | Signals data change for WebSocket push |

### `VPSManager.py` — VPS class (legacy, NOT extended)

VPS configuration persisted at `data/vpsmanager/hosts/{hostname}/{hostname}.json`.

| Field | Notes |
|-------|-------|
| `ip` | VPS IP address |
| `user` | SSH user |
| `firewall_ssh_port` | SSH port (default 22) |
| `bucket` | rclone remote (legacy) |

**`fetch_vps_info()`**: Reads remote `software/pbgui/pbgui.ini` via Paramiko SFTP. Currently only extracts `pbdir` (PB6). **We do NOT extend VPSManager** — it is scheduled for deprecation. Instead, remote path discovery and ini access are handled by the new `read_remote_ini()` / `write_remote_ini()` methods on `AsyncSSHPool` (see Section 6.2).

### `PBRemote.py` — Legacy rclone sync

`sync_api_up()` copies `api-keys.json` to `data/cmd/` for rclone distribution. Called from 4 explicit UI sites only (never in daemon loop):

1. `pbgui_func.py:385` — Legacy Streamlit sync button
2. `navi/system_services.py:260` — Service monitoring UI red sync button
3. `api/api_keys.py:1754` — `POST /sync/push` REST endpoint
4. `navi/system_vps_manager.py:181` — VPS Manager UI red sync button

**No conflict with SSH sync** — old buttons trigger rclone, new "SSH API Sync" button triggers the new FileSyncWorker. Both can coexist during migration.

---

## 6. New / Modified Components

### 6.1 `master/async_pool.py` — SFTP extensions + remote ini access

New methods on `AsyncSSHPool`:

```python
# --- SFTP file operations ---
async push_file(hostname: str, local_path: Path, remote_path: str) → bool
async pull_file(hostname: str, remote_path: str, local_path: Path) → bool
async read_remote_file(hostname: str, remote_path: str) → Optional[bytes]
async list_remote_dir(hostname: str, remote_path: str) → list[str]
async remove_remote_file(hostname: str, remote_path: str) → bool
async start_file_watcher(hostname: str, remote_path: str) → Optional[asyncssh.SSHClientProcess]

# --- Remote pbgui.ini access ---
async read_remote_ini(hostname: str) → configparser.ConfigParser
async write_remote_ini(hostname: str, config: configparser.ConfigParser) → bool
async get_remote_ini_value(hostname: str, section: str, key: str, fallback=None) → Optional[str]
async set_remote_ini_value(hostname: str, section: str, key: str, value: str) → bool
```

SFTP operations use the existing `asyncssh` connection — no Paramiko, no separate SFTP sessions.

#### Remote `pbgui.ini` access pattern

The `read_remote_ini()` / `write_remote_ini()` methods provide generic access to each VPS's `pbgui.ini` via the existing SSH connection pool. This replaces VPSManager's Paramiko-based `fetch_vps_info()`.

- **Read path**: `{REMOTE_PBGUI_DIR}/pbgui.ini` (= `software/pbgui/pbgui.ini`)
- **Parsed with**: `configparser.ConfigParser`
- **Cached in**: `VPSConnection.data['ini']` after first read (per-connection lifetime)
- **Invalidated on**: reconnect, or explicit re-read (e.g. after `write_remote_ini`)
- **Write**: Read-modify-write pattern — read current ini, apply changes, write back via SFTP

#### On-connect auto-read

When `connect()` successfully establishes an SSH session, the pool automatically reads the remote `pbgui.ini` and caches it in `VPSConnection.data`. This makes remote paths (pb6dir, pb7dir) and other config immediately available without extra round-trips.

```python
# After successful asyncssh.connect():
entry.conn = conn
entry.status = ConnectionStatus.CONNECTED
# Auto-read remote ini and cache
ini = await self._read_ini_internal(hostname, conn)
entry.data['ini'] = ini
entry.data['pb6dir'] = ini.get('main', 'pbdir', fallback=None)
entry.data['pb7dir'] = ini.get('main', 'pb7dir', fallback=None)
```

### 6.2 Remote `pbgui.ini` — new `[filesync]` section

Retention settings and other sync config are stored in the VPS's own `pbgui.ini`, so every Master that connects automatically learns the configuration.

```ini
[filesync]
backup_retention_days = 180
backup_min_versions = 10
```

| Key | Default | Purpose |
|-----|---------|---------|
| `backup_retention_days` | `180` | Delete backups older than this many days |
| `backup_min_versions` | `10` | Always keep at least this many backup versions regardless of age |

- **Shared across Masters**: Since config lives on VPS, every connecting Master reads the same values. No config drift between Masters.
- **Per-VPS customizable**: Each VPS can have different retention settings (e.g. VPS with limited disk → shorter retention).
- **Defaults used if section missing**: If a VPS's `pbgui.ini` has no `[filesync]` section, defaults (180 / 10) apply.
- **Editable via UI**: The SSH API Sync panel shows the current retention settings per VPS (read from cached ini). Admin can adjust `backup_retention_days` and `backup_min_versions` directly — changes are written to the VPS's `pbgui.ini` via `set_remote_ini_value()` and the cache is refreshed.

### 6.3 `master/file_sync.py` — FileSyncWorker (NEW)

Central sync orchestrator. Generic by design — parameterized by file type.

```python
class FileSyncWorker:
    def __init__(self, pool: AsyncSSHPool, store: VPSStore, monitor: VPSMonitor):
        ...

    # --- Push (Master → VPS) ---
    async push_api_keys(
        hostnames: list[str] | None,  # None = all connected
        dry_run: bool = False,
        lock_hostname: str | None = None,  # sets _sync_lock
    ) → dict[str, dict]:  # per-VPS result

    # --- Single VPS push steps (called by push_api_keys) ---
    async _backup_remote(hostname: str) → bool
    async _push_and_verify(hostname: str, local_path: Path, remote_path: str) → bool
    async _retention_cleanup(hostname: str)
    async _kill_affected_bots(hostname: str)

    # --- Pull (VPS → Master) ---
    async _pull_from_vps(hostname: str) → bool

    # --- Watchers (inotifywait) ---
    async start_watchers(hostnames: list[str])
    async stop_watchers()
    async _watcher_callback(hostname: str, data: str)

    # --- Metadata ---
    def _bump_serial(self, lock_hostname: str | None = None) → dict
    def _read_local_serial(self) → int
```

**Key changes from earlier design:**

- `_backup_remote()` writes to `~/software/pbgui/data/backup/api-keys_v7/{timestamp}/api-keys.json` (PB7) and `~/software/pbgui/data/backup/api-keys/{timestamp}/api-keys.json` (PB6). Follows the existing PBRemote backup convention exactly — same directory structure on VPS as today.
- `_retention_cleanup()` reads `backup_retention_days` and `backup_min_versions` from the VPS's cached `pbgui.ini` `[filesync]` section (via `pool.get_remote_ini_value()`). Falls back to defaults (180 / 10) if section missing.
- Remote paths (pb6dir, pb7dir) read from `pool.connections[hostname].data['pb6dir']` / `data['pb7dir']` — cached on SSH connect, no VPSManager dependency.

### 6.4 `api/api_keys.py` — New REST endpoints

```
POST /sync/push-ssh
  Body: { "hostnames": ["manibot51"] | null, "dry_run": false, "lock_hostname": null }
  Returns: { "results": { "manibot51": { "success": true, "backup": true, "verified": true, "bots_killed": 3 } } }

GET /sync/ssh-status
  Returns: { "connected_hosts": [...], "watcher_active": {...}, "last_push": {...} }
```

### 6.5 `frontend/api_keys_editor.html` — UI additions

- **"SSH API Sync" button** in the header area
- **VPS selectbox** with "All" as default + individual VPS hostnames
- **Dry-run toggle**
- **Status display**: per-VPS results (success/failure, backup status, MD5 match, bots killed)
- **Visual feedback**: spinner during push, success/error icons per VPS

---

## 7. File & Directory Layout

```
master/
├── async_pool.py          # + SFTP methods, remote ini read/write, start_file_watcher()
├── async_monitor.py       # kill_instance() — unchanged
├── async_store.py         # unchanged
├── file_sync.py           # NEW — FileSyncWorker
└── ws_server.py           # unchanged

api/
├── api_keys.py            # + POST /sync/push-ssh, GET /sync/ssh-status
└── serial.txt             # bumped on API changes

frontend/
└── api_keys_editor.html   # + SSH API Sync UI section
```

### VPS-side file layout (after push)

```
~/software/pbgui/
├── pbgui.ini              # [filesync] section with retention config
└── data/
    └── backup/
        ├── api-keys_v7/   # PB7 api-keys backups (same pattern as PBRemote)
        │   ├── 2026-03-26_14-30-00/
        │   │   └── api-keys.json
        │   ├── 2026-03-25_10-00-00/
        │   │   └── api-keys.json
        │   └── ...
        └── api-keys/      # PB6 api-keys backups (if pb6dir set)
            ├── 2026-03-26_14-30-00/
            │   └── api-keys.json
            └── ...

~/software/pb7/
└── api-keys.json          # pushed by FileSyncWorker (if pb7dir set)

~/software/pb6/            # (if pb6dir set)
└── api-keys.json          # pushed by FileSyncWorker
```

**Note**: Backups follow the existing PBRemote convention: `data/backup/api-keys_v7/{timestamp}/api-keys.json` for PB7 and `data/backup/api-keys/{timestamp}/api-keys.json` for PB6. Separate backup per passivbot version, consistent with how backups are already created today. The passivbot directories only contain the active `api-keys.json`.

---

## 8. Logging

All logging via `from logging_helpers import human_log as _log`.

| Component | Service tag | Log file | Routing |
|-----------|------------|----------|---------|
| `FileSyncWorker` | `[FileSync]` | `data/logs/PBGui.log` | Via `LOG_GROUPS` (Tier 3) |
| `AsyncSSHPool` SFTP | `[SSHPool]` | (existing pool log) | (unchanged) |

No separate `api_sync.log` — `[FileSync]` tag makes grep easy in the shared `PBGui.log`:
```bash
grep '\[FileSync\]' data/logs/PBGui.log
```

Log entries include: hostname, operation, serial, success/failure, MD5 hashes, timing.

---

## 9. Security Considerations

- **No credentials in logs**: API keys, passwords, private keys never logged.
- **SFTP over existing SSH**: No new ports opened. Uses the same SSH connection already in the pool.
- **`_sync_lock` is advisory**: It prevents automated pulls but does not enforce access control. It's a coordination mechanism, not a security boundary.
- **MD5 verify**: Catches truncated/corrupt transfers. Not for tamper detection (SSH already provides that).
- **Backup before overwrite**: Always create backup before writing. If push fails mid-transfer, the backup remains intact.

---

## 10. Migration Path

1. **Phase 1** (this implementation): SSH sync coexists with rclone. Both buttons work. No rclone removal.
2. **Phase 2** (future): Once SSH sync proven stable, remove rclone-based `sync_api_up()` calls and `POST /sync/push` endpoint.
3. **Phase 3** (future): Extend `FileSyncWorker` to sync PB7 configs (same pattern, different file paths).

---

## 11. Implementation Order

| Step | File | What |
|------|------|------|
| 1 | `master/async_pool.py` | Add SFTP methods + remote ini read/write + `start_file_watcher()` |
| 2 | `master/async_pool.py` | On-connect auto-read of remote `pbgui.ini` → cache in `VPSConnection.data` |
| 3 | `master/file_sync.py` | Create `FileSyncWorker` (push, pull, backup, retention, watcher) |
| 4 | `api/api_keys.py` | New endpoints: `POST /sync/push-ssh`, `GET /sync/ssh-status` |
| 5 | `frontend/api_keys_editor.html` | SSH API Sync UI (button, selectbox, dry-run, status) |
| 6 | `api/serial.txt` | Increment (API change) |
| 7 | `README.md` | Changelog entry |

---

## 12. Design Decisions (confirmed)

| Decision | Rationale |
|----------|-----------|
| Always restart bots (no toggle) | Bots must pick up new keys. No reason to skip restart. |
| Kill only after verify | Prevents downtime from failed transfers. |
| inotifywait over SSH (not polling) | < 1s latency, no loop overhead, reuses existing SSH. |
| `_sync_lock` advisory only | Sufficient for test-push scenarios. No security claim. |
| Retention configurable in VPS ini | Each VPS controls its own limits. Masters learn config on connect. No config drift. |
| Backups in `pbgui/data/backup/api-keys[_v7]/` | Follows existing PBRemote convention. Same structure locally and on VPS. |
| No VPSManager extension | VPSManager is legacy/scheduled for deprecation. Remote config via SSH pool instead. |
| Remote paths cached on connect | Read `pbgui.ini` on SSH connect, cache in `VPSConnection.data`. No extra round-trips. |
| Metadata fields in JSON | Avoids sidecar files. Fields prefixed with `_` to avoid conflicts. |
| SFTP via asyncssh (not Paramiko) | Consistent with existing pool. No second SSH library. |
| Generic FileSyncWorker | Future reuse for config sync without duplication. |
| Logging to PBGui.log (Tier 3) | No separate log file — `[FileSync]` tag is sufficient for filtering. |
| Dry-run mode | Safety net for reviewing changes before pushing. |
| MD5 verify after push | Confirms file integrity after SFTP transfer. |
| Generic remote ini read/write | Enables VPS config access for any feature, not just file sync. |
