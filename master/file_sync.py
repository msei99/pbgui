"""
FileSyncWorker — SSH/SFTP-based file sync for api-keys.json.

Pushes api-keys.json from Master → VPS(es) via the AsyncSSHPool,
with backup, MD5 verify, retention cleanup, bot restart, and
inotifywait-based multi-master notification.

See docs/architecture_ssh_api_sync.md for the full design.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import shutil
import socket
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, pb7dir as _pb7dir, pbdir as _pb6dir

SERVICE = "FileSync"

# Remote pbgui data dir (relative to home)
REMOTE_PBGUI_DIR = "software/pbgui"

# Minimal Python inotify watcher — uses ctypes to call Linux inotify syscalls
# directly, no packages needed beyond stdlib.
# ONE-SHOT variant: waits for the first IN_CLOSE_WRITE event, prints the
# affected filepath to stdout, then exits with code 0.
# _watcher_loop restarts it in a loop via pool.run().
# Watches DIRECTORIES (not files) to avoid inode-change issues:
# SFTP writes can replace a file's inode, causing a file-level watch
# to go stale (IN_IGNORED).  Directory inodes never change.
_INOTIFY_WATCHER_SCRIPT = """
import ctypes, struct, os, sys
libc = ctypes.CDLL('libc.so.6', use_errno=True)
IN_CLOSE_WRITE = 0x8
fd = libc.inotify_init()
if fd < 0:
    sys.exit(1)
watches = {}
for p in sys.argv[1:]:
    d = os.path.dirname(p)
    f = os.path.basename(p)
    wd = libc.inotify_add_watch(fd, d.encode(), IN_CLOSE_WRITE)
    if wd >= 0:
        if wd in watches:
            watches[wd][1].add(f)
        else:
            watches[wd] = (d, {f})
if not watches:
    sys.exit(2)
while True:
    buf = os.read(fd, 4096)
    o = 0
    while o + 16 <= len(buf):
        wid, mask, cookie, nlen = struct.unpack_from('iIII', buf, o)
        name = buf[o+16:o+16+nlen].rstrip(b'\\x00').decode(errors='replace')
        o += 16 + nlen
        if mask & IN_CLOSE_WRITE and wid in watches:
            d, targets = watches[wid]
            if name in targets:
                print(os.path.join(d, name), flush=True)
                sys.exit(0)
""".strip()

# Persist last-push state across API restarts
_LAST_PUSH_FILE = Path(PBGDIR) / "data" / "ssh_sync_status.json"

# Defaults when [filesync] section missing from VPS pbgui.ini
DEFAULT_RETENTION_DAYS = 180
DEFAULT_MIN_VERSIONS = 10

# Local api-keys.json path (pb7dir preferred, pb6dir fallback)
def _local_api_keys() -> Path:
    """Return the local api-keys.json path based on pbgui.ini."""
    try:
        d = _pb7dir()
        if d:
            return Path(d) / "api-keys.json"
    except Exception:
        pass
    try:
        d = _pb6dir()
        if d:
            return Path(d) / "api-keys.json"
    except Exception:
        pass
    # Last-resort fallback: PBGDIR (original behaviour)
    return Path(f"{PBGDIR}/api-keys.json")

LOCAL_API_KEYS = _local_api_keys()


class FileSyncWorker:
    """Central sync orchestrator for api-keys.json distribution."""

    def __init__(self, pool, store, monitor):
        """
        Args:
            pool: AsyncSSHPool instance
            store: VPSStore instance
            monitor: VPSMonitor instance (for kill_instance)
        """
        self.pool = pool
        self.store = store
        self.monitor = monitor
        self._watchers: dict[str, asyncio.Task] = {}
        self._local_hostname = socket.gethostname()
        _state = self._load_state()
        self._last_push: dict[str, dict] = _state.get("last_push", {})
        # Cache: hostname → last-seen _api_serial from VPS (updated by watcher + push)
        self._remote_serials: dict[str, int] = _state.get("remote_serials", {})
        # Cache: hostname → {"pb7": md5|None, "pb6": md5|None}
        self._remote_md5s: dict[str, dict] = _state.get("remote_md5s", {})
        # Cache: hostname → raw bytes of pb7 api-keys.json as last seen
        # (populated by _fetch_remote_state / watcher, consumed by _push_single_vps
        # to diff credentials without an extra SFTP read during push)
        self._remote_content: dict[str, bytes] = {}
        # SSE subscribers: each is an asyncio.Queue that receives serial-update dicts
        self._sse_queues: list[asyncio.Queue] = []
        # Watchdog task handle (started via start_watchdog)
        self._watchdog: Optional[asyncio.Task] = None

    # ── Persistence ──────────────────────────────────────────
    @staticmethod
    def _load_state() -> dict:
        """Load persisted state (last_push + remote_serials) from disk."""
        try:
            if _LAST_PUSH_FILE.exists():
                data = json.loads(_LAST_PUSH_FILE.read_text(encoding="utf-8"))
                # Migrate old flat format ({hostname: {...}}) to new format
                if data and "last_push" not in data:
                    return {"last_push": data, "remote_serials": {}}
                return data
        except Exception:
            pass
        return {"last_push": {}, "remote_serials": {}, "remote_md5s": {}}

    def _save_last_push(self) -> None:
        """Persist last-push + remote_serials to disk (atomic write)."""
        try:
            _LAST_PUSH_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = _LAST_PUSH_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(
                {
                    "last_push": self._last_push,
                    "remote_serials": self._remote_serials,
                    "remote_md5s": self._remote_md5s,
                },
                indent=2,
            ), encoding="utf-8")
            tmp.replace(_LAST_PUSH_FILE)
        except Exception as e:
            _log(SERVICE, f"[persist] Failed to save state: {e}", level="WARNING")

    # ── Push (Master → VPS) ──────────────────────────────────

    async def push_api_keys(
        self,
        hostnames: list[str] | None = None,
        dry_run: bool = False,
        no_propagate: bool = False,
    ) -> dict[str, dict]:
        """Push api-keys.json to one or more VPS.

        Pure distribution — does NOT bump _api_serial (that is the
        editor's responsibility).  Pushes the file as-is, only
        manipulating _sync_lock when *no_propagate* is requested.

        Args:
            hostnames: Target VPS list, or None for all connected.
            dry_run: If True, only preview — no writes.
            no_propagate: If True, set _sync_lock so other masters
                          won't pull this version.

        Returns:
            Dict of {hostname: result_dict} per VPS.
        """
        if not LOCAL_API_KEYS.exists():
            _log(SERVICE, "Local api-keys.json not found", level="ERROR")
            return {"error": "Local api-keys.json not found"}

        targets = hostnames or self.pool.connected_hosts()
        if not targets:
            _log(SERVICE, "No connected VPS to push to", level="WARNING")
            return {"error": "No connected VPS"}

        # Prepare content: read current file, update only push metadata
        api_data = self._prepare_push_content(no_propagate)
        local_content = json.dumps(api_data, indent=4).encode("utf-8")
        local_md5 = hashlib.md5(local_content).hexdigest()

        if dry_run:
            results = {}
            for h in targets:
                entry = self.pool.get_connection(h)
                pb7dir = entry.data.get("pb7dir") if entry else None
                pb6dir = entry.data.get("pb6dir") if entry else None
                instances = self.store.instances.get(h, [])
                results[h] = {
                    "dry_run": True,
                    "pb7dir": pb7dir,
                    "pb6dir": pb6dir,
                    "bots_affected": len(instances),
                    "serial": api_data.get("_api_serial"),
                    "md5": local_md5,
                }
            _log(SERVICE, f"Dry-run push to {len(targets)} VPS(es)")
            return results

        # Write updated local file (with metadata)
        tmp = LOCAL_API_KEYS.with_suffix(".tmp")
        tmp.write_bytes(local_content)
        tmp.replace(LOCAL_API_KEYS)
        _log(SERVICE, f"Updated local api-keys.json (serial="
             f"{api_data.get('_api_serial')}, md5={local_md5})")

        # Push to each VPS in parallel
        tasks = {h: self._push_single_vps(h, local_content, local_md5)
                 for h in targets}
        raw = await asyncio.gather(*tasks.values(), return_exceptions=True)
        results = {}
        for h, r in zip(tasks.keys(), raw):
            if isinstance(r, Exception):
                _log(SERVICE, f"Push to {h} failed: {r}", level="ERROR",
                     meta={"traceback": traceback.format_exc()})
                results[h] = {"success": False, "error": str(r)}
            else:
                results[h] = r

        ok = sum(1 for r in results.values() if r.get("success"))
        pushed_serial = api_data.get("_api_serial", 0)
        _log(SERVICE, f"Push complete: {ok}/{len(targets)} VPS(es) OK "
             f"(serial={pushed_serial})")
        # Record last push info for status reporting
        ts = datetime.now(timezone.utc).isoformat()
        local_serial = self._read_local_serial()
        for h, r in results.items():
            if not r.get("dry_run"):  # don't persist dry-run as real sync
                self._last_push[h] = {
                    "ts": ts,
                    "serial": pushed_serial,
                    "success": r.get("success", False),
                }
                if r.get("success"):
                    # After a successful push the VPS has the pushed content
                    self._remote_serials[h] = pushed_serial
                    entry = self.pool.get_connection(h)
                    pb7dir = entry.data.get("pb7dir") if entry else None
                    pb6dir = entry.data.get("pb6dir") if entry else None
                    self._remote_md5s[h] = {
                        "pb7": local_md5 if pb7dir else None,
                        "pb6": local_md5 if pb6dir else None,
                    }
                    self._broadcast_serial_full(
                        h, pushed_serial, local_serial, True,
                        self._last_push[h])
                else:
                    # Partial push: update only dirs that succeeded
                    md5s = self._remote_md5s.get(h, {"pb7": None, "pb6": None})
                    if r.get("pushed_v7"):
                        md5s["pb7"] = local_md5
                    if r.get("pushed_v6"):
                        md5s["pb6"] = local_md5
                    self._remote_md5s[h] = md5s
                    in_sync = self._check_in_sync(h, local_md5)
                    self._broadcast_serial_full(
                        h, pushed_serial, local_serial, in_sync,
                        self._last_push[h])
        self._save_last_push()
        return results

    async def _push_single_vps(self, hostname: str, content: bytes,
                                expected_md5: str) -> dict:
        """Push to a single VPS: backup → push → verify → cleanup → kill."""
        entry = self.pool.get_connection(hostname)
        if not entry:
            return {"success": False, "error": "Not connected"}

        pb7dir = entry.data.get("pb7dir")
        pb6dir = entry.data.get("pb6dir")
        if not pb7dir and not pb6dir:
            return {"success": False, "error": "No pb6dir/pb7dir configured"}

        result = {
            "success": False,
            "backup_v7": False,
            "backup_v6": False,
            "pushed_v7": False,
            "pushed_v6": False,
            "verified": False,
            "bots_killed": 0,
            "retention_cleaned": False,
        }

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # a) Backup remote files
        if pb7dir:
            result["backup_v7"] = await self._backup_remote(
                hostname, f"{pb7dir}/api-keys.json",
                f"{REMOTE_PBGUI_DIR}/data/backup/api-keys_v7/{ts}")
        if pb6dir:
            result["backup_v6"] = await self._backup_remote(
                hostname, f"{pb6dir}/api-keys.json",
                f"{REMOTE_PBGUI_DIR}/data/backup/api-keys/{ts}")

        # b) Push file to target dirs — single SFTP session for both
        sftp = await self.pool._open_sftp(hostname)
        if not sftp:
            _log(SERVICE, f"Push to {hostname}: cannot open SFTP",
                 level="ERROR")
            return result
        # Use pre-cached content (populated by _fetch_remote_state / watcher)
        # to determine which users changed — zero extra SFTP round-trip.
        old_remote_raw: bytes | None = self._remote_content.get(hostname)
        try:
            if pb7dir:
                result["pushed_v7"] = await self._push_and_verify(
                    hostname, content, f"{pb7dir}/api-keys.json",
                    expected_md5, sftp)
            if pb6dir:
                result["pushed_v6"] = await self._push_and_verify(
                    hostname, content, f"{pb6dir}/api-keys.json",
                    expected_md5, sftp)
        finally:
            sftp.exit()

        # c) Verify all configured dirs were pushed successfully
        v7_ok = result["pushed_v7"] if pb7dir else True
        v6_ok = result["pushed_v6"] if pb6dir else True
        result["verified"] = v7_ok and v6_ok

        if not (result["pushed_v7"] or result["pushed_v6"]):
            _log(SERVICE, f"Push to {hostname}: no target dir was written "
                 "successfully — skipping kill", level="ERROR")
            return result

        # d) Retention cleanup
        result["retention_cleaned"] = await self._retention_cleanup(hostname)

        # e) Kill affected bots (only bots whose credentials actually changed)
        # Update cache to reflect the freshly pushed content so the next push
        # (or the watchdog's state refresh) sees correct pre-push data.
        if pb7dir and result["pushed_v7"]:
            self._remote_content[hostname] = content
        try:
            new_data = json.loads(content.decode("utf-8"))
        except Exception:
            new_data = {}
        changed_users = self._find_changed_users(old_remote_raw, new_data)
        if changed_users:
            _log(SERVICE, f"[kill] {hostname}: credential change detected for "
                 f"{len(changed_users)} user(s): {sorted(changed_users)}")
        else:
            _log(SERVICE, f"[kill] {hostname}: no credential changes — "
                 "skipping bot restart", level="DEBUG")
        result["bots_killed"] = await self._kill_affected_bots(
            hostname, changed_users)

        # success = ALL configured dirs pushed OK
        result["success"] = v7_ok and v6_ok

        if result["success"]:
            _log(SERVICE, f"Push to {hostname} OK "
                 f"(v7={result['pushed_v7']}, v6={result['pushed_v6']}, "
                 f"killed={result['bots_killed']})")
        else:
            _log(SERVICE, f"Push to {hostname} PARTIAL "
                 f"(v7={result['pushed_v7']}, v6={result['pushed_v6']}, "
                 f"killed={result['bots_killed']})", level="WARNING")
        return result

    async def _backup_remote(self, hostname: str, source_path: str,
                              backup_dir: str) -> bool:
        """Copy a remote file to a backup directory on the VPS."""
        # Check if source exists
        attrs = await self.pool.stat_remote(hostname, source_path)
        if not attrs:
            _log(SERVICE, f"[backup] {hostname}:{source_path} does not exist "
                 "— skipping backup", level="DEBUG")
            return True  # No file to back up is OK

        ok = await self.pool.makedirs_remote(hostname, backup_dir)
        if not ok:
            return False
        dest = f"{backup_dir}/api-keys.json"
        cp_result = await self.pool.run(hostname, f"cp '{source_path}' '{dest}'")
        if cp_result and cp_result.exit_status == 0:
            _log(SERVICE, f"[backup] {hostname}: {source_path} → {dest}",
                 level="DEBUG")
            return True
        _log(SERVICE, f"[backup] {hostname}: backup copy failed", level="ERROR")
        return False

    async def _push_and_verify(self, hostname: str, content: bytes,
                                remote_path: str,
                                expected_md5: str,
                                sftp) -> bool:
        """Write content to remote path and MD5-verify via given SFTP session."""
        # Ensure parent directory exists
        parent = remote_path.rsplit("/", 1)[0]
        await self.pool.makedirs_remote(hostname, parent)

        try:
            async with sftp.open(remote_path, "wb") as f:
                await f.write(content)
            # Read back in the same session
            async with sftp.open(remote_path, "rb") as f:
                read_back = await f.read()
        except Exception as e:
            _log(SERVICE, f"[push] Write/verify on {hostname}:{remote_path} "
                 f"failed: {type(e).__name__}: {e}", level="ERROR")
            return False

        actual_md5 = hashlib.md5(read_back).hexdigest()
        if actual_md5 != expected_md5:
            _log(SERVICE, f"[push] MD5 mismatch on {hostname}:{remote_path}: "
                 f"expected={expected_md5}, actual={actual_md5}", level="ERROR")
            return False
        _log(SERVICE, f"[push] Verified {hostname}:{remote_path} "
             f"(md5={actual_md5})", level="DEBUG")
        return True

    async def _retention_cleanup(self, hostname: str) -> bool:
        """Remove old backups beyond configured retention on a VPS."""
        days = int(await self.pool.get_remote_ini_value(
            hostname, "filesync", "backup_retention_days",
            fallback=str(DEFAULT_RETENTION_DAYS)) or DEFAULT_RETENTION_DAYS)
        min_versions = int(await self.pool.get_remote_ini_value(
            hostname, "filesync", "backup_min_versions",
            fallback=str(DEFAULT_MIN_VERSIONS)) or DEFAULT_MIN_VERSIONS)

        cleaned = False
        for backup_subdir in ("data/backup/api-keys_v7", "data/backup/api-keys"):
            remote_dir = f"{REMOTE_PBGUI_DIR}/{backup_subdir}"
            ok = await self._cleanup_dir(
                hostname, remote_dir, days, min_versions)
            cleaned = cleaned or ok
        return cleaned

    async def _cleanup_dir(self, hostname: str, remote_dir: str,
                            max_age_days: int, min_versions: int) -> bool:
        """Clean up timestamped backup dirs in a single directory."""
        entries = await self.pool.list_remote_dir(hostname, remote_dir)
        if not entries:
            return True

        # Sort entries (they are YYYY-MM-DD_HH-MM-SS format)
        sorted_entries = sorted(entries, reverse=True)

        # Always keep at least min_versions
        if len(sorted_entries) <= min_versions:
            return True

        now = datetime.now()
        removed = 0
        for entry_name in sorted_entries[min_versions:]:
            try:
                entry_dt = datetime.strptime(entry_name, "%Y-%m-%d_%H-%M-%S")
                age_days = (now - entry_dt).days
                if age_days > max_age_days:
                    path = f"{remote_dir}/{entry_name}"
                    result = await self.pool.run(
                        hostname, f"rm -rf '{path}'", timeout=10)
                    if result and result.exit_status == 0:
                        removed += 1
            except ValueError:
                continue  # Skip non-timestamp entries

        if removed:
            _log(SERVICE, f"[retention] {hostname}: removed {removed} old "
                 f"backups from {remote_dir}", level="DEBUG")
        return True

    @staticmethod
    def _find_changed_users(old_raw: bytes | None, new_data: dict) -> set[str]:
        """Return usernames whose credentials differ between old and new content.

        Compares the credential fields (key, secret, passphrase, private_key,
        wallet_address) for every user entry in *new_data* against *old_raw*.
        If *old_raw* is ``None`` or unparseable, all users are returned
        (conservative fallback — restart everything).
        """
        _CRED_FIELDS = frozenset({
            "key", "apiKey", "api_key",
            "secret",
            "passphrase", "password",
            "wallet_address", "walletAddress", "wallet",
            "private_key", "privateKey",
        })
        user_entries = {
            k: v for k, v in new_data.items()
            if not k.startswith("_")
            and isinstance(v, dict)
            and "exchange" in v
        }
        if old_raw is None:
            return set(user_entries.keys())
        try:
            old_data = json.loads(old_raw)
        except (json.JSONDecodeError, ValueError):
            return set(user_entries.keys())
        changed: set[str] = set()
        for name, new_entry in user_entries.items():
            old_entry = old_data.get(name)
            if not isinstance(old_entry, dict):
                changed.add(name)  # new user
                continue
            for field in _CRED_FIELDS:
                if new_entry.get(field) != old_entry.get(field):
                    changed.add(name)
                    break
        return changed

    async def _kill_affected_bots(self, hostname: str,
                                   changed_users: set[str] | None = None
                                   ) -> int:
        """Kill running bot instances for users whose credentials changed.

        If *changed_users* is provided, only instances whose ``u`` field
        (the API-keys username) is in that set are killed.  Passing ``None``
        kills all running instances (conservative fall-back).
        """
        instances = self.store.instances.get(hostname, [])
        if not instances:
            return 0

        killed = 0
        for inst in instances:
            name = inst.get("u", "")
            pb_version = inst.get("p", "")
            if not name:
                continue
            if changed_users is not None and name not in changed_users:
                _log(SERVICE,
                     f"[kill] {hostname}/{name}: skipping — credentials unchanged",
                     level="DEBUG")
                continue
            try:
                result = await self.monitor.kill_instance(
                    hostname, name, pb_version)
                if result.get("success"):
                    killed += 1
            except Exception as e:
                _log(SERVICE, f"[kill] {hostname}/{name}: {e}", level="WARNING")
        if killed:
            _log(SERVICE, f"[kill] {hostname}: killed {killed} bot(s) — "
                 "PBRun will auto-restart them")
        return killed

    # ── Pull (VPS → Master) ──────────────────────────────────

    async def _pull_from_vps(self, hostname: str) -> bool:
        """Pull api-keys.json from a VPS to local (multi-master sync)."""
        entry = self.pool.get_connection(hostname)
        if not entry:
            return False
        pb7dir = entry.data.get("pb7dir")
        if not pb7dir:
            return False

        remote_path = f"{pb7dir}/api-keys.json"
        raw = await self.pool.read_remote_file(hostname, remote_path)
        if raw is None:
            return False

        try:
            remote_data = json.loads(raw)
        except json.JSONDecodeError as e:
            _log(SERVICE, f"[pull] Invalid JSON from {hostname}: {e}",
                 level="ERROR")
            return False

        remote_serial = remote_data.get("_api_serial", 0)
        local_serial = self._read_local_serial()

        if remote_serial <= local_serial:
            _log(SERVICE, f"[pull] {hostname}: remote serial "
                 f"{remote_serial} <= local {local_serial} — skip",
                 level="DEBUG")
            return False

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Backup + write pb7 (LOCAL_API_KEYS)
        if LOCAL_API_KEYS.exists():
            backup_dir = Path(f"{PBGDIR}/data/backup/api-keys_v7/{ts}")
            backup_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(LOCAL_API_KEYS, backup_dir / "api-keys.json")
        tmp = LOCAL_API_KEYS.with_suffix(".tmp")
        tmp.write_bytes(raw)
        tmp.replace(LOCAL_API_KEYS)

        # Also write pb6 if configured
        pb6dir_local = _pb6dir()
        if pb6dir_local:
            local_pb6 = Path(pb6dir_local) / "api-keys.json"
            if local_pb6.exists():
                backup_dir6 = Path(f"{PBGDIR}/data/backup/api-keys/{ts}")
                backup_dir6.mkdir(parents=True, exist_ok=True)
                shutil.copy2(local_pb6, backup_dir6 / "api-keys.json")
            tmp6 = local_pb6.with_suffix(".tmp")
            tmp6.write_bytes(raw)
            tmp6.replace(local_pb6)

        _log(SERVICE, f"[pull] Updated local api-keys.json from {hostname} "
             f"(serial {local_serial} → {remote_serial})")
        return True

    # ── Watchers (inotifywait) ───────────────────────────────

    async def start_watchers(self, hostnames: list[str] | None = None):
        """Start inotifywait watchers on connected VPS(es)."""
        targets = hostnames or self.pool.connected_hosts()
        for h in targets:
            if h in self._watchers and not self._watchers[h].done():
                continue  # Already watching
            entry = self.pool.get_connection(h)
            if not entry:
                continue
            pb7dir = entry.data.get("pb7dir")
            pb6dir = entry.data.get("pb6dir")
            paths = []
            if pb7dir:
                paths.append(f"{pb7dir}/api-keys.json")
            if pb6dir:
                paths.append(f"{pb6dir}/api-keys.json")
            if not paths:
                continue
            task = asyncio.create_task(
                self._watcher_loop(h, paths),
                name=f"watcher-{h}",
            )
            self._watchers[h] = task
            _log(SERVICE, f"[watcher] Started for {h} "
                 f"({len(paths)} path(s))", level="DEBUG")
            # Proactively seed remote state cache (don't await — fire and forget)
            asyncio.create_task(
                self._fetch_remote_state(h), name=f"state-init-{h}"
            )

    async def start_watchers_single(self, hostname: str):
        """Start inotifywait watcher for a single host.

        Used as the AsyncSSHPool on-connect callback — called automatically
        on every successful connect or reconnect.
        """
        await self.start_watchers([hostname])

    async def stop_watchers(self):
        """Cancel all watcher tasks."""
        for h, task in self._watchers.items():
            task.cancel()
            _log(SERVICE, f"[watcher] Stopped for {h}", level="DEBUG")
        self._watchers.clear()

    # ── Watchdog ─────────────────────────────────────────────

    WATCHDOG_INTERVAL = 60  # seconds

    def start_watchdog(self) -> None:
        """Start the background watchdog (call once at startup)."""
        if self._watchdog and not self._watchdog.done():
            return
        self._watchdog = asyncio.create_task(
            self._watchdog_loop(), name="filesync-watchdog")
        _log(SERVICE, "[watchdog] Started", level="DEBUG")

    def stop_watchdog(self) -> None:
        """Cancel the watchdog task."""
        if self._watchdog:
            self._watchdog.cancel()
            self._watchdog = None

    async def _watchdog_loop(self) -> None:
        """Periodic check: restart dead watchers + refresh MD5 state.

        Runs every WATCHDOG_INTERVAL seconds.  Catches all exceptions
        so a single bad cycle never kills the loop.
        """
        while True:
            try:
                await asyncio.sleep(self.WATCHDOG_INTERVAL)
                connected = self.pool.connected_hosts()
                if not connected:
                    continue

                # 1) Restart dead watcher tasks
                restarted = 0
                for h in connected:
                    task = self._watchers.get(h)
                    if task is None or task.done():
                        await self.start_watchers([h])
                        restarted += 1
                if restarted:
                    _log(SERVICE, f"[watchdog] Restarted {restarted} "
                         "dead watcher(s)", level="WARNING")

                # 2) Refresh MD5 state — only for hosts where the watcher is
                # dead or where we have no cached MD5 yet.  If the watcher is
                # alive it keeps the cache current; polling every 60 s for all
                # VPS would create unnecessary SFTP traffic.
                for h in connected:
                    task = self._watchers.get(h)
                    watcher_alive = task is not None and not task.done()
                    has_cache = h in self._remote_md5s
                    if not watcher_alive or not has_cache:
                        try:
                            await self._fetch_remote_state(h)
                        except Exception:
                            pass  # _fetch_remote_state already handles errors
            except asyncio.CancelledError:
                return
            except Exception as e:
                _log(SERVICE, f"[watchdog] Error: {e}", level="ERROR",
                     meta={"traceback": traceback.format_exc()})

    async def _fetch_remote_state(self, hostname: str) -> None:
        """Read remote api-keys.json files to seed MD5 and serial caches.

        Called as a fire-and-forget task when a watcher starts so the sync
        status is populated immediately without waiting for a file change.
        Reads both pb7 and pb6 copies, computes MD5 for each, and compares
        with the local file to determine in_sync status.
        """
        try:
            entry = self.pool.get_connection(hostname)
            if not entry:
                return
            pb7dir = entry.data.get("pb7dir")
            pb6dir = entry.data.get("pb6dir")
            if not pb7dir and not pb6dir:
                return

            local_md5 = self._compute_local_md5()
            md5s = {"pb7": None, "pb6": None}
            remote_serial = None
            pb7_data = None

            if pb7dir:
                raw = await self.pool.read_remote_file(
                    hostname, f"{pb7dir}/api-keys.json")
                if raw:
                    md5s["pb7"] = hashlib.md5(raw).hexdigest()
                    # Cache raw bytes so _push_single_vps can diff without
                    # an extra SFTP round-trip during the push.
                    self._remote_content[hostname] = raw
                    try:
                        pb7_data = json.loads(raw)
                        remote_serial = pb7_data.get("_api_serial", 0)
                        # Seed last_push from remote metadata
                        sync_ts = pb7_data.get("_api_ts")
                        if sync_ts and hostname not in self._last_push:
                            self._last_push[hostname] = {
                                "ts": sync_ts,
                                "serial": remote_serial,
                                "success": True,
                            }
                    except json.JSONDecodeError:
                        pass

            if pb6dir:
                raw = await self.pool.read_remote_file(
                    hostname, f"{pb6dir}/api-keys.json")
                if raw:
                    md5s["pb6"] = hashlib.md5(raw).hexdigest()

            self._remote_md5s[hostname] = md5s
            if remote_serial is not None:
                self._remote_serials[hostname] = remote_serial

            in_sync = self._check_in_sync(hostname, local_md5, entry)
            local_serial = self._read_local_serial()
            lp = self._last_push.get(hostname)
            self._broadcast_serial_full(
                hostname,
                remote_serial if remote_serial is not None
                else self._remote_serials.get(hostname),
                local_serial, in_sync, lp)
            self._save_last_push()
            _log(SERVICE, f"[state-init] {hostname}: serial={remote_serial}, "
                 f"pb7_md5={md5s['pb7']}, pb6_md5={md5s['pb6']}, "
                 f"in_sync={in_sync}", level="DEBUG")

            # If remote has a higher serial → pull (same logic as _watcher_callback)
            if remote_serial is not None and remote_serial > local_serial:
                if pb7_data and pb7_data.get("_sync_lock"):
                    _log(SERVICE, f"[state-init] {hostname}: _sync_lock — skip pull",
                         level="DEBUG")
                else:
                    _log(SERVICE, f"[state-init] {hostname}: new serial "
                         f"{remote_serial} (local={local_serial}) — pulling")
                    await self._pull_from_vps(hostname)
        except Exception:
            pass  # Non-critical — watcher will update on next file change

    async def _watcher_loop(self, hostname: str, remote_paths: list[str]):
        """Watch api-keys.json on a VPS for changes via Python ctypes inotify.

        Runs a one-shot Python script via pool.run() in a continuous loop.
        Each script invocation blocks until one IN_CLOSE_WRITE event fires,
        prints the affected path to stdout, then exits.  pool.run() returns
        the completed result, we call _watcher_callback, then restart.

        Using pool.run() (instead of streaming stdout) avoids asyncssh
        channel-buffer issues that can stall long-running process readers.
        Uses plain python3 — script only needs stdlib, no venv required.
        """
        try:
            # Filter to paths whose parent directory exists (dir-level
            # watches work even when the file itself doesn't exist yet).
            valid_paths = []
            seen_dirs = set()
            for p in remote_paths:
                d = p.rsplit("/", 1)[0] if "/" in p else "."
                if d not in seen_dirs:
                    seen_dirs.add(d)
                    if await self.pool.stat_remote(hostname, d):
                        valid_paths.append(p)
                    else:
                        _log(SERVICE, f"[watcher] {hostname}: dir {d} "
                             "does not exist — skipping", level="DEBUG")
                else:
                    valid_paths.append(p)  # dir already verified
            if not valid_paths:
                _log(SERVICE, f"[watcher] {hostname}: no api-keys.json "
                     "files found to watch", level="WARNING")
                return

            script_b64 = base64.b64encode(
                _INOTIFY_WATCHER_SCRIPT.encode()
            ).decode()
            paths_str = " ".join(f"'{p}'" for p in valid_paths)
            cmd = (
                f"python3 -c "
                f"\"import base64,sys;exec(base64.b64decode('{script_b64}').decode())\" "
                f"{paths_str}"
            )

            _log(SERVICE, f"[watcher] {hostname}: inotify watcher active "
                 f"({len(valid_paths)} path(s))", level="DEBUG")

            while True:
                # Blocks on the remote until one close_write event fires.
                # timeout=None = wait indefinitely (no timeout).
                # Returns None if connection is lost.
                result = await self.pool.run(hostname, cmd, timeout=None)
                if result is None:
                    _log(SERVICE, f"[watcher] {hostname}: connection lost — "
                         "watcher stopped", level="WARNING")
                    return
                # Guard against tight loop if script crashes immediately
                if result.exit_status not in (0, None):
                    _log(SERVICE, f"[watcher] {hostname}: script exited "
                         f"with code {result.exit_status} — backing off",
                         level="WARNING")
                    await asyncio.sleep(30)
                    continue
                changed = result.stdout.strip() if result.stdout else ""
                if changed:
                    await self._watcher_callback(hostname, changed)
        except asyncio.CancelledError:
            return
        except Exception as e:
            _log(SERVICE, f"[watcher] {hostname} error: {e}", level="ERROR",
                 meta={"traceback": traceback.format_exc()})

    async def _watcher_callback(self, hostname: str, data: str):
        """Handle a file-change event from inotifywait.

        Reads both pb7 and pb6 remote files, computes MD5 for each,
        compares with the local file, and broadcasts in_sync status.
        If the remote serial is higher, triggers a pull.
        """
        _log(SERVICE, f"[watcher] {hostname}: file changed ({data})",
             level="DEBUG")

        entry = self.pool.get_connection(hostname)
        if not entry:
            return
        pb7dir = entry.data.get("pb7dir")
        pb6dir = entry.data.get("pb6dir")
        local_md5 = self._compute_local_md5()
        md5s = {"pb7": None, "pb6": None}
        remote_serial = None
        pb7_data = None

        if pb7dir:
            raw = await self.pool.read_remote_file(
                hostname, f"{pb7dir}/api-keys.json")
            if raw:
                md5s["pb7"] = hashlib.md5(raw).hexdigest()
                try:
                    pb7_data = json.loads(raw)
                    remote_serial = pb7_data.get("_api_serial", 0)
                except json.JSONDecodeError:
                    pass

        if pb6dir:
            raw = await self.pool.read_remote_file(
                hostname, f"{pb6dir}/api-keys.json")
            if raw:
                md5s["pb6"] = hashlib.md5(raw).hexdigest()

        self._remote_md5s[hostname] = md5s
        if remote_serial is not None:
            self._remote_serials[hostname] = remote_serial

        in_sync = self._check_in_sync(hostname, local_md5, entry)
        local_serial = self._read_local_serial()
        self._broadcast_serial_full(
            hostname,
            remote_serial if remote_serial is not None
            else self._remote_serials.get(hostname),
            local_serial, in_sync, self._last_push.get(hostname))

        # Check if remote has higher serial → pull
        if remote_serial is not None and remote_serial > local_serial:
            if pb7_data and pb7_data.get("_sync_lock"):
                _log(SERVICE, f"[watcher] {hostname}: _sync_lock — skip",
                     level="DEBUG")
                return
            _log(SERVICE, f"[watcher] {hostname}: new serial {remote_serial} "
                 f"(local={local_serial}) — pulling")
            await self._pull_from_vps(hostname)

    # ── Metadata ─────────────────────────────────────────────

    def _prepare_push_content(self, no_propagate: bool = False) -> dict:
        """Read api-keys.json as-is and optionally set/clear _sync_lock.

        Does NOT touch _api_serial / _api_ts / _api_by — those are
        owned by the editor (Users.save).
        """
        if LOCAL_API_KEYS.exists():
            data = json.loads(LOCAL_API_KEYS.read_text(encoding="utf-8"))
        else:
            data = {}

        if no_propagate:
            data["_sync_lock"] = self._local_hostname
        else:
            data.pop("_sync_lock", None)

        return data

    def _bump_serial(self, lock_hostname: str | None = None) -> dict:
        """(Legacy) Read api-keys.json, increment _api_serial, update metadata.

        Kept for potential future internal use. Not called by push.
        Returns the updated dict (not yet written to disk — caller writes).
        """
        if LOCAL_API_KEYS.exists():
            data = json.loads(LOCAL_API_KEYS.read_text(encoding="utf-8"))
        else:
            data = {}

        serial = data.get("_api_serial", 0) + 1
        data["_api_serial"] = serial
        data["_api_by"] = self._local_hostname
        data["_api_ts"] = datetime.now(timezone.utc).isoformat()

        if lock_hostname:
            data["_sync_lock"] = lock_hostname
        else:
            data.pop("_sync_lock", None)

        return data

    def _read_local_serial(self) -> int:
        """Read current _api_serial from local api-keys.json."""
        if not LOCAL_API_KEYS.exists():
            return 0
        try:
            data = json.loads(LOCAL_API_KEYS.read_text(encoding="utf-8"))
            return data.get("_api_serial", 0)
        except (json.JSONDecodeError, OSError):
            return 0

    def _compute_local_md5(self) -> str:
        """Compute MD5 of local api-keys.json content."""
        if not LOCAL_API_KEYS.exists():
            return ""
        return hashlib.md5(LOCAL_API_KEYS.read_bytes()).hexdigest()

    def _check_in_sync(self, hostname: str, local_md5: str,
                        entry=None) -> bool | None:
        """Check if a host's remote api-keys.json files match local MD5.

        Returns True (all match), False (mismatch), or None (unknown).
        """
        md5s = self._remote_md5s.get(hostname)
        if not md5s:
            return None
        if not entry:
            entry = self.pool.get_connection(hostname)
        if not entry:
            return None
        pb7dir = entry.data.get("pb7dir")
        pb6dir = entry.data.get("pb6dir")
        pb7_ok = (md5s.get("pb7") == local_md5) if pb7dir else True
        pb6_ok = (md5s.get("pb6") == local_md5) if pb6dir else True
        return pb7_ok and pb6_ok

    # ── Status ───────────────────────────────────────────────

    def get_status(self) -> dict:
        """Build status dict for the REST API."""
        watchers = {
            h: not t.done()
            for h, t in self._watchers.items()
        }
        connected = self.pool.connected_hosts()
        local_serial = self._read_local_serial()
        local_md5 = self._compute_local_md5()

        # Gather per-host info
        hosts = {}
        for h in connected:
            entry = self.pool.get_connection(h)
            if entry:
                remote_serial = self._remote_serials.get(h)
                watcher_ok = watchers.get(h, False)
                in_sync = self._check_in_sync(h, local_md5, entry)
                # Per-path detail for UI tooltip
                md5s = self._remote_md5s.get(h, {})
                pb7dir = entry.data.get("pb7dir")
                pb6dir = entry.data.get("pb6dir")
                md5_detail = {}
                if pb7dir:
                    md5_detail["pb7"] = (md5s.get("pb7") == local_md5) if md5s.get("pb7") else None
                if pb6dir:
                    md5_detail["pb6"] = (md5s.get("pb6") == local_md5) if md5s.get("pb6") else None
                hosts[h] = {
                    "pb7dir": pb7dir,
                    "pb6dir": pb6dir,
                    "pbname": entry.data.get("pbname"),
                    "watcher_active": watcher_ok,
                    "remote_serial": remote_serial,
                    "in_sync": in_sync,
                    "md5_detail": md5_detail,
                }

        return {
            "connected_hosts": connected,
            "hosts": hosts,
            "local_serial": local_serial,
            "local_hostname": self._local_hostname,
            "last_push": self._last_push,
        }

    # ── SSE helpers ──────────────────────────────────────────

    def subscribe_sse(self) -> asyncio.Queue:
        """Register a new SSE subscriber; returns its queue."""
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._sse_queues.append(q)
        return q

    def unsubscribe_sse(self, q: asyncio.Queue) -> None:
        """Remove an SSE subscriber queue."""
        try:
            self._sse_queues.remove(q)
        except ValueError:
            pass

    def _broadcast_serial(self, hostname: str, remote_serial: int,
                           local_serial: int,
                           in_sync: bool | None = None) -> None:
        """Push a serial-update event (without last_push) to all SSE subscribers."""
        self._broadcast_serial_full(hostname, remote_serial, local_serial,
                                     in_sync, None)

    def _broadcast_serial_full(self, hostname: str, remote_serial: int,
                                local_serial: int,
                                in_sync: bool | None,
                                last_push: dict | None) -> None:
        """Push a serial-update event (with MD5-based in_sync) to all SSE subscribers."""
        # Build per-path detail
        md5_detail = {}
        local_md5 = self._compute_local_md5()
        md5s = self._remote_md5s.get(hostname, {})
        entry = self.pool.get_connection(hostname)
        if entry:
            if entry.data.get("pb7dir"):
                md5_detail["pb7"] = (md5s.get("pb7") == local_md5) if md5s.get("pb7") else None
            if entry.data.get("pb6dir"):
                md5_detail["pb6"] = (md5s.get("pb6") == local_md5) if md5s.get("pb6") else None

        msg: dict = {
            "hostname": hostname,
            "remote_serial": remote_serial,
            "local_serial": local_serial,
            "in_sync": in_sync,
            "md5_detail": md5_detail,
        }
        if last_push:
            msg["last_push"] = last_push
        dead = []
        for q in self._sse_queues:
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self.unsubscribe_sse(q)

    async def get_retention_settings(self, hostname: str) -> dict:
        """Read retention settings from a VPS's pbgui.ini."""
        days = await self.pool.get_remote_ini_value(
            hostname, "filesync", "backup_retention_days",
            fallback=str(DEFAULT_RETENTION_DAYS))
        min_ver = await self.pool.get_remote_ini_value(
            hostname, "filesync", "backup_min_versions",
            fallback=str(DEFAULT_MIN_VERSIONS))
        return {
            "backup_retention_days": int(days or DEFAULT_RETENTION_DAYS),
            "backup_min_versions": int(min_ver or DEFAULT_MIN_VERSIONS),
        }

    async def set_retention_settings(self, hostname: str,
                                      days: int, min_versions: int) -> bool:
        """Write retention settings to a VPS's pbgui.ini."""
        ok1 = await self.pool.set_remote_ini_value(
            hostname, "filesync", "backup_retention_days", str(days))
        ok2 = await self.pool.set_remote_ini_value(
            hostname, "filesync", "backup_min_versions", str(min_versions))
        if ok1 and ok2:
            _log(SERVICE, f"[retention] {hostname}: set days={days}, "
                 f"min_versions={min_versions}")
        return ok1 and ok2
