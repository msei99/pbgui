"""
V7ConfigSyncWorker — inotify-based multi-master sync for v7 instance configs.

When Master1 pushes a config change to VPS via SSH Activate, an inotify
watcher on the VPS detects the write.  The callback reads the config,
compares the pbgui.version, and pulls higher versions to the local
data/run_v7/ directory — making the change available on Master2.

Follows the same pattern as FileSyncWorker (api-keys.json sync).
See docs/architecture_ssh_api_sync.md for the general design.
"""

from __future__ import annotations

import asyncio
import base64
import json
import traceback
from pathlib import Path
from typing import Optional

from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR

SERVICE = "V7ConfigSync"

# Remote pbgui data dir (relative to home)
REMOTE_PBGUI_DIR = "software/pbgui"
REMOTE_RUN_V7 = f"{REMOTE_PBGUI_DIR}/data/run_v7"

# Local v7 config directory
LOCAL_RUN_V7 = Path(PBGDIR) / "data" / "run_v7"

# Persistent inotify script — runs continuously on VPS, prints every
# matched event as a line to stdout (not one-shot).
# Uses select() to also monitor stdin — when the SSH session drops, stdin
# returns EOF and the script exits cleanly (prevents orphan processes that
# leak inotify instances).
_INOTIFY_WATCHER_SCRIPT = """
import ctypes, struct, os, sys, select
libc = ctypes.CDLL('libc.so.6', use_errno=True)
IN_CLOSE_WRITE = 0x8
fd = libc.inotify_init()
if fd < 0:
    errno = ctypes.get_errno()
    print(f"inotify_init failed: errno={errno} ({os.strerror(errno)})", file=sys.stderr, flush=True)
    sys.exit(1)
watches = {}
failed = 0
for p in sys.argv[1:]:
    d = os.path.dirname(p)
    f = os.path.basename(p)
    wd = libc.inotify_add_watch(fd, d.encode(), IN_CLOSE_WRITE)
    if wd >= 0:
        if wd in watches:
            watches[wd][1].add(f)
        else:
            watches[wd] = (d, {f})
    else:
        failed += 1
if not watches:
    print(f"no watches added (tried {len(sys.argv)-1} paths, {failed} failed)", file=sys.stderr, flush=True)
    sys.exit(2)
if failed:
    print(f"partial: {len(watches)} watches OK, {failed} failed", file=sys.stderr, flush=True)
stdin_fd = sys.stdin.fileno()
while True:
    ready, _, _ = select.select([fd, stdin_fd], [], [])
    if stdin_fd in ready:
        if not os.read(stdin_fd, 1):
            sys.exit(0)
    if fd in ready:
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
""".strip()

# Max consecutive failures before exponential backoff caps
WATCHER_BASE_BACKOFF = 5       # seconds
WATCHER_MAX_BACKOFF = 300      # 5 minutes

# Watchdog interval in seconds
WATCHDOG_INTERVAL = 120


class V7ConfigSyncWorker:
    """Watches v7 config.json files on VPS for changes (multi-master sync)."""

    def __init__(self, pool, store, monitor):
        self.pool = pool
        self.store = store
        self.monitor = monitor
        self._watchers: dict[str, asyncio.Task] = {}
        self._watchdog: Optional[asyncio.Task] = None

    # ── Public API ───────────────────────────────────────────

    async def start_watchers(self, hostnames: list[str] | None = None):
        """Start inotify watchers on connected VPS(es)."""
        targets = hostnames or self.pool.connected_hosts()
        for h in targets:
            if h in self._watchers and not self._watchers[h].done():
                continue  # Already watching
            entry = self.pool.get_connection(h)
            if not entry:
                continue

            # Discover v7 instance dirs on the VPS
            paths = await self._discover_remote_configs(h)
            if not paths:
                _log(SERVICE, f"[watcher] {h}: no v7 configs found to watch",
                     level="DEBUG")
                continue

            task = asyncio.create_task(
                self._watcher_loop(h, paths),
                name=f"v7cfg-watcher-{h}",
            )
            self._watchers[h] = task
            _log(SERVICE, f"[watcher] Started for {h} "
                 f"({len(paths)} path(s))", level="DEBUG")

    async def start_watchers_single(self, hostname: str):
        """Start watcher for a single host (on-connect callback)."""
        await self.start_watchers([hostname])

    async def restart_watchers(self, hostname: str):
        """Restart watcher for a host (e.g. after SSH Activate adds new instances)."""
        # Cancel existing watcher so it picks up new instance dirs
        task = self._watchers.pop(hostname, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self.start_watchers([hostname])

    async def stop_watchers(self):
        """Cancel all watcher tasks."""
        for h, task in self._watchers.items():
            task.cancel()
            _log(SERVICE, f"[watcher] Stopped for {h}", level="DEBUG")
        self._watchers.clear()

    def start_watchdog(self) -> None:
        """Start the background watchdog (call once at startup)."""
        if self._watchdog and not self._watchdog.done():
            return
        self._watchdog = asyncio.create_task(
            self._watchdog_loop(), name="v7cfg-watchdog")
        _log(SERVICE, "[watchdog] Started", level="DEBUG")

    def stop_watchdog(self) -> None:
        """Cancel the watchdog task."""
        if self._watchdog:
            self._watchdog.cancel()
            self._watchdog = None

    # ── Discovery ────────────────────────────────────────────

    async def _discover_remote_configs(self, hostname: str) -> list[str]:
        """List config.json + running_version.txt paths in run_v7/*/.

        Both are watched via inotify:
        - config.json changes  → pull newer config (multi-master sync)
        - running_version.txt  → trigger immediate instance collection
                                 (fast activation feedback)
        """
        entries = await self.pool.list_remote_dir(hostname, REMOTE_RUN_V7)
        if not entries:
            return []
        paths = []
        for entry in entries:
            # Skip dotfiles and non-directory entries
            if entry.startswith("."):
                continue
            dir_path = f"{REMOTE_RUN_V7}/{entry}"
            if await self.pool.stat_remote(hostname, dir_path):
                paths.append(f"{dir_path}/config.json")
                paths.append(f"{dir_path}/running_version.txt")
        return paths

    # ── Watcher loop ─────────────────────────────────────────

    async def _watcher_loop(self, hostname: str, remote_paths: list[str]):
        """Watch config.json + running_version.txt on VPS via persistent
        inotify stream.

        Uses pool.start_process() to launch a long-running inotify script
        that prints every matched event to stdout.  Events are processed
        as they arrive — no restart needed between events.
        """
        try:
            # Verify which directories actually exist
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
                _log(SERVICE, f"[watcher] {hostname}: no v7 config dirs "
                     "found to watch", level="WARNING")
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

            consecutive_failures = 0
            while True:
                proc = await self.pool.start_process(hostname, cmd)
                if proc is None:
                    _log(SERVICE, f"[watcher] {hostname}: connection lost",
                         level="WARNING")
                    return

                _log(SERVICE, f"[watcher] {hostname}: inotify streaming "
                     f"({len(valid_paths)} path(s))", level="DEBUG")

                try:
                    async for line in proc.stdout:
                        consecutive_failures = 0
                        changed = line.strip()
                        if changed:
                            await self._watcher_callback(hostname, changed)
                except asyncio.CancelledError:
                    proc.close()
                    raise

                # Process ended — check exit status
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.close()
                exit_status = proc.exit_status

                if exit_status in (0, None):
                    # Clean exit (e.g. stdin EOF) — restart immediately
                    _log(SERVICE, f"[watcher] {hostname}: stream ended, "
                         "restarting", level="DEBUG")
                    continue

                # Error exit — backoff
                consecutive_failures += 1
                stderr_msg = ""
                try:
                    stderr_msg = ((await proc.stderr.read()) or "").strip()
                except Exception:
                    pass
                backoff = min(
                    WATCHER_BASE_BACKOFF * (2 ** (consecutive_failures - 1)),
                    WATCHER_MAX_BACKOFF,
                )
                _log(SERVICE, f"[watcher] {hostname}: script exited "
                     f"with code {exit_status} "
                     f"(attempt {consecutive_failures}, "
                     f"backoff {backoff}s)"
                     + (f" — {stderr_msg}" if stderr_msg else ""),
                     level="WARNING")
                await asyncio.sleep(backoff)

        except asyncio.CancelledError:
            return
        except Exception as e:
            _log(SERVICE, f"[watcher] {hostname} error: {e}", level="ERROR",
                 meta={"traceback": traceback.format_exc()})

    async def _watcher_callback(self, hostname: str, changed_path: str):
        """Handle an inotify event for config.json or running_version.txt."""
        # Extract instance name from path:
        # .../data/run_v7/{instance_name}/{filename}
        parts = changed_path.replace("\\", "/").split("/")
        try:
            idx = parts.index("run_v7")
            instance_name = parts[idx + 1]
            filename = parts[idx + 2] if len(parts) > idx + 2 else ""
        except (ValueError, IndexError):
            _log(SERVICE, f"[watcher] {hostname}: cannot parse instance "
                 f"from path: {changed_path}", level="WARNING")
            return

        # ── running_version.txt → fast activation feedback ───
        if filename == "running_version.txt":
            _log(SERVICE, f"[watcher] {hostname}/{instance_name}: "
                 "running_version.txt changed — triggering collect")
            try:
                await self.monitor.collect_instances_now(hostname)
            except Exception as e:
                _log(SERVICE, f"[watcher] {hostname}: collect failed: {e}",
                     level="WARNING")
            return

        # ── config.json → multi-master config sync ───────────
        _log(SERVICE, f"[watcher] {hostname}: config changed for "
             f"'{instance_name}'", level="DEBUG")

        # Read remote config
        raw = await self.pool.read_remote_file(hostname, changed_path)
        if raw is None:
            _log(SERVICE, f"[watcher] {hostname}/{instance_name}: "
                 "could not read remote config", level="WARNING")
            return

        try:
            remote_cfg = json.loads(raw)
        except json.JSONDecodeError as e:
            _log(SERVICE, f"[watcher] {hostname}/{instance_name}: "
                 f"invalid JSON: {e}", level="ERROR")
            return

        remote_version = remote_cfg.get("pbgui", {}).get("version", 0)

        # Read local config
        local_path = LOCAL_RUN_V7 / instance_name / "config.json"
        local_version = 0
        if local_path.is_file():
            try:
                local_cfg = json.loads(local_path.read_text(encoding="utf-8"))
                local_version = local_cfg.get("pbgui", {}).get("version", 0)
            except (json.JSONDecodeError, OSError):
                pass

        if remote_version <= local_version:
            _log(SERVICE, f"[watcher] {hostname}/{instance_name}: remote "
                 f"version {remote_version} <= local {local_version} — skip",
                 level="DEBUG")
            return

        # Pull: write remote config to local
        _log(SERVICE, f"[pull] {hostname}/{instance_name}: version "
             f"{local_version} → {remote_version}")
        await self._pull_config(instance_name, raw)

    async def _pull_config(self, instance_name: str, content: bytes):
        """Write a config pulled from VPS to local data/run_v7/."""
        dest_dir = LOCAL_RUN_V7 / instance_name
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / "config.json"

        # Atomic write
        tmp = dest.with_suffix(".tmp")
        try:
            tmp.write_bytes(content)
            tmp.replace(dest)
            _log(SERVICE, f"[pull] Updated local {instance_name}/config.json")
        except OSError as e:
            _log(SERVICE, f"[pull] Write failed for {instance_name}: {e}",
                 level="ERROR")
            # Clean up tmp if it exists
            tmp.unlink(missing_ok=True)

    # ── Watchdog ─────────────────────────────────────────────

    async def _watchdog_loop(self) -> None:
        """Periodic check: restart dead watchers + discover new instances."""
        while True:
            try:
                await asyncio.sleep(WATCHDOG_INTERVAL)
                connected = self.pool.connected_hosts()
                if not connected:
                    continue

                restarted = 0
                for h in connected:
                    task = self._watchers.get(h)
                    if task is None or task.done():
                        await self.start_watchers([h])
                        restarted += 1
                if restarted:
                    _log(SERVICE, f"[watchdog] Restarted {restarted} "
                         "dead watcher(s)", level="WARNING")

            except asyncio.CancelledError:
                return
            except Exception as e:
                _log(SERVICE, f"[watchdog] Error: {e}", level="ERROR",
                     meta={"traceback": traceback.format_exc()})
