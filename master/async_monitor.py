"""
Async VPS Monitor — system metrics, instances, services, alerts.

All coroutines run on the FastAPI event loop.  No threads, no paramiko.
Uses asyncssh via ``AsyncSSHPool`` for all SSH operations.
"""

from __future__ import annotations

import asyncio
import json
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import asyncssh

from pbgui_purefunc import PBGDIR, load_ini, save_ini
from logging_helpers import human_log as _log
from ini_watcher import IniWatcher
from master.async_pool import (
    AsyncSSHPool, ConnectionStatus, REMOTE_PBGUI_DIR,
)
from master.async_store import VPSStore, SystemMetrics

SERVICE = "VPSMonitor"

# ── Constants ───────────────────────────────────────────────

LOOP_INTERVAL = 15          # seconds between main loop iterations
SERVICE_CHECK_EVERY = 4     # every N iterations (= 60s at 15s)
INSTANCE_COLLECT_INTERVAL = 30  # seconds

# ── Remote scripts (same as old realtime_collector) ─────────

MONITOR_AGENT_SCRIPT = r'''python3 -u -c "
import json, os, time
def rcpu():
    with open('/proc/stat') as f:
        p = f.readline().split()
    idle = int(p[4])
    return idle, sum(int(x) for x in p[1:])
def rmem():
    d = {}
    with open('/proc/meminfo') as f:
        for ln in f:
            k, v = ln.split(':')
            if k in ('MemTotal','MemAvailable','SwapTotal','SwapFree'):
                d[k] = int(v.split()[0]) * 1024
    mt = d.get('MemTotal', 0)
    ma = d.get('MemAvailable', 0)
    mu = mt - ma
    mp = round(mu / mt * 100, 1) if mt else 0
    st = d.get('SwapTotal', 0)
    sf = d.get('SwapFree', 0)
    su = st - sf
    sp = round(su / st * 100, 1) if st else 0
    return [mt, ma, mp, mu], [st, su, sf, sp]
pi, pt = rcpu()
time.sleep(1)
while True:
    try:
        ci, ct = rcpu()
        di, dt = ci - pi, ct - pt
        cpu = round((1 - di / dt) * 100, 1) if dt else 0
        pi, pt = ci, ct
        mem, swap = rmem()
        s = os.statvfs('/')
        dtot = s.f_frsize * s.f_blocks
        dused = s.f_frsize * (s.f_blocks - s.f_bfree)
        dfree = s.f_frsize * s.f_bavail
        dpct = round(dused / dtot * 100, 1) if dtot else 0
        print(json.dumps({'ts': time.time(), 'cpu': cpu, 'mem': mem, 'disk': [dtot, dused, dfree, dpct], 'swap': swap}), flush=True)
    except Exception:
        pass
    time.sleep(1)
"'''

INSTANCE_COLLECT_SCRIPT = r'''python3 -u -c "
import json, glob, os, subprocess
HOME = os.path.expanduser('~')
PBGDIR = os.path.join(HOME, 'software/pbgui')
running_dirs = set()
try:
    out = subprocess.check_output(['ps', 'aux'], text=True)
    for line in out.splitlines():
        if 'main.py' in line and 'config_run.json' in line:
            for part in line.split():
                if part.endswith('/config_run.json'):
                    running_dirs.add(os.path.dirname(part))
        elif 'passivbot.py' in line and 'config.json' in line:
            for part in line.split():
                if part.endswith('/config.json'):
                    running_dirs.add(os.path.dirname(part))
        elif 'passivbot_multi.py' in line and 'multi_run.hjson' in line:
            for part in line.split():
                if part.endswith('/multi_run.hjson'):
                    running_dirs.add(os.path.dirname(part))
except Exception:
    pass
result = []
patterns = [
    os.path.join(PBGDIR, 'data/run_v7/*/monitor.json'),
    os.path.join(PBGDIR, 'data/multi/*/monitor.json'),
    os.path.join(PBGDIR, 'data/instances/*/monitor.json'),
]
for pat in patterns:
    for mf in glob.glob(pat):
        inst_dir = os.path.dirname(mf)
        if inst_dir not in running_dirs:
            continue
        try:
            with open(mf) as f:
                result.append(json.load(f))
        except Exception:
            pass
print(json.dumps(result))
"'''


# ── Service definitions ─────────────────────────────────────

class ServiceStatus(Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    UNKNOWN = "unknown"
    RESTARTING = "restarting"


@dataclass
class ServiceInfo:
    name: str
    pid_file: str       # relative to PBGUI dir
    script_file: str    # Python script to run
    process_match: str  # grep string in cmdline


MONITORED_SERVICES = {
    "PBRun": ServiceInfo("PBRun", "data/pid/pbrun.pid",
                         "PBRun.py", "pbrun.py"),
    "PBRemote": ServiceInfo("PBRemote", "data/pid/pbremote.pid",
                            "PBRemote.py", "pbremote.py"),
    "PBCoinData": ServiceInfo("PBCoinData", "data/pid/pbcoindata.pid",
                              "PBCoinData.py", "pbcoindata.py"),
}


# ── Main orchestrator ───────────────────────────────────────

class VPSMonitor:
    """
    Async VPS monitoring orchestrator.

    Lifecycle:
        monitor = VPSMonitor()
        await monitor.start()   # launches all background tasks
        ...
        await monitor.stop()    # cancels everything, disconnects
    """

    def __init__(self):
        self.pool = AsyncSSHPool()
        self.store = VPSStore()

        # Config
        self._auto_restart: Optional[bool] = None
        self._enabled_hosts: Optional[set[str]] = None

        # Telegram
        self._telegram_token = ""
        self._telegram_chat_id = ""

        # Alert dedup
        self._connection_alerts: set[str] = set()
        self._service_alerts: set[str] = set()

        # Restart rate limiting
        self._restart_history: dict[str, dict[str, list[datetime]]] = {}
        self.max_restarts_per_hour = 3

        # Instance collection timing
        self._last_instance_collect: float = 0.0

        # ini watcher (thread-based, fine alongside asyncio)
        self._ini_watcher = IniWatcher()

        # Background tasks
        self._tasks: list[asyncio.Task] = []
        self._stream_tasks: dict[str, asyncio.Task] = {}
        self._running = False

    # ── Config ──────────────────────────────────────────────

    @property
    def auto_restart(self) -> bool:
        if self._auto_restart is None:
            val = load_ini("vps_monitor", "auto_restart")
            self._auto_restart = val.lower() == "true" if val else True
        return self._auto_restart

    @property
    def enabled_hosts(self) -> set[str]:
        if self._enabled_hosts is None:
            val = load_ini("vps_monitor", "enabled_hosts")
            if val and val.strip():
                self._enabled_hosts = {
                    h.strip() for h in val.split(",") if h.strip()
                }
            else:
                self._enabled_hosts = set()
        return self._enabled_hosts

    @property
    def telegram_token(self):
        if not self._telegram_token:
            self._telegram_token = load_ini("main", "telegram_token") or ""
        return self._telegram_token

    @property
    def telegram_chat_id(self):
        if not self._telegram_chat_id:
            self._telegram_chat_id = load_ini("main", "telegram_chat_id") or ""
        return self._telegram_chat_id

    # ── Lifecycle ───────────────────────────────────────────

    async def start(self):
        """Initialize and start all monitoring tasks."""
        if self._running:
            return
        self._running = True
        _log(SERVICE, "Starting VPS monitor...")

        self.pool.load_vps_configs()
        self.store.load_ui_settings()
        self._ini_watcher.start()

        enabled = self.enabled_hosts
        if not enabled:
            _log(SERVICE, "No VPS hosts enabled for monitoring. "
                 "Enable hosts in Services → API Server → Settings.")
        else:
            # Remove non-enabled hosts from pool
            for h in list(self.pool.hostnames()):
                if h not in enabled:
                    self.pool.remove_host(h)

            results = await self.pool.connect_enabled(enabled)
            connected = sum(1 for v in results.values() if v)
            _log(SERVICE, f"Connected to {connected}/{len(results)} VPS servers")

            # Start metric streams for connected hosts
            for hostname, success in results.items():
                if success:
                    self._start_metrics_stream(hostname)

        # Launch main loop as background task
        self._tasks.append(asyncio.create_task(
            self._main_loop(), name="vps-main-loop"
        ))

        _log(SERVICE, "VPS monitor started")

    async def stop(self):
        """Cancel all tasks and disconnect."""
        if not self._running:
            return
        self._running = False
        _log(SERVICE, "Stopping VPS monitor...")

        # Cancel stream tasks
        for task in self._stream_tasks.values():
            task.cancel()
        # Cancel main tasks
        for task in self._tasks:
            task.cancel()

        # Wait for cancellation
        all_tasks = list(self._tasks) + list(self._stream_tasks.values())
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        self._tasks.clear()
        self._stream_tasks.clear()
        self._ini_watcher.stop()
        await self.pool.disconnect_all()
        _log(SERVICE, "VPS monitor stopped")

    # ── Main loop ───────────────────────────────────────────

    async def _main_loop(self):
        """Main monitoring loop — health checks, reconnects, services."""
        loop_count = 0
        while self._running:
            try:
                await self._loop_iteration(loop_count)
                loop_count += 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                _log(SERVICE, f"Error in main loop: {e}", level="WARNING",
                     meta={'traceback': traceback.format_exc()})

            # Sleep but wake on ini change
            try:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, self._ini_watcher.changed.wait, LOOP_INTERVAL
                    ),
                    timeout=LOOP_INTERVAL + 1,
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

    async def _loop_iteration(self, loop_count: int):
        """Single iteration of the main loop."""
        # Config changes
        if self._ini_watcher.changed.is_set():
            self._ini_watcher.changed.clear()
            await self._apply_config_changes()

        enabled = self.enabled_hosts
        if not enabled:
            return

        # 1. Health check
        status = self.pool.health_check()
        enabled_status = {h: s for h, s in status.items() if h in enabled}
        self._handle_connection_changes(enabled_status)

        # 2. Reconnect lost
        reconnected = await self.pool.reconnect_lost(enabled)
        newly_reconnected: list[str] = []
        for hostname, success in reconnected.items():
            if success:
                _log(SERVICE, f"Reconnected to {hostname}")
                self._start_metrics_stream(hostname)
                alert_key = f"conn:{hostname}"
                if alert_key in self._connection_alerts:
                    self._connection_alerts.discard(alert_key)
                    newly_reconnected.append(hostname)

        # Send reconnect alerts (batched if mass reconnect)
        if newly_reconnected:
            if len(newly_reconnected) >= max(2, len(enabled) * 0.5):
                hosts_str = ", ".join(sorted(newly_reconnected))
                await self._send_alert(
                    f"✅ *VPSMonitor*: Network recovered — "
                    f"SSH reconnected to *{len(newly_reconnected)}* "
                    f"hosts ({hosts_str})"
                )
            else:
                for hostname in newly_reconnected:
                    await self._send_alert(
                        f"✅ *VPSMonitor*: SSH reconnected to "
                        f"*{hostname}*"
                    )

        # 3. Restart dead metric streams
        self._restart_dead_streams()

        # 4. Collect instances (every ~30s)
        await self._collect_instances_all()

        # 5. Service monitoring (every N iterations)
        if loop_count % SERVICE_CHECK_EVERY == 0:
            connected = [
                h for h, s in enabled_status.items()
                if s == ConnectionStatus.CONNECTED
            ]
            if connected:
                results = await self._check_and_heal_services(connected)
                self.store.update_services(results)

    # ── Config reload ───────────────────────────────────────

    async def _apply_config_changes(self):
        """Re-read config and apply host enable/disable changes."""
        prev_enabled = self._enabled_hosts or set()
        self._enabled_hosts = None
        self._auto_restart = None
        enabled = self.enabled_hosts

        newly_disabled = prev_enabled - enabled
        newly_enabled = enabled - prev_enabled

        if newly_disabled:
            _log(SERVICE, f"Hosts disabled: {', '.join(sorted(newly_disabled))}")
            for h in newly_disabled:
                self._stop_metrics_stream(h)
                await self.pool.disconnect(h)
                self.pool.remove_host(h)
                self.store.remove_host(h)

        if newly_enabled:
            _log(SERVICE, f"Hosts newly enabled: "
                 f"{', '.join(sorted(newly_enabled))}")
            self.pool.load_vps_configs()
            for h in list(self.pool.hostnames()):
                if h not in enabled:
                    self.pool.remove_host(h)
            for h in newly_enabled:
                if h in self.pool.hostnames():
                    if await self.pool.connect(h):
                        self._start_metrics_stream(h)

    # ── Metric streams ──────────────────────────────────────

    def _start_metrics_stream(self, hostname: str):
        """Launch an async task that reads system metrics from SSH."""
        self._stop_metrics_stream(hostname)
        task = asyncio.create_task(
            self._metrics_stream(hostname),
            name=f"metrics-{hostname}",
        )
        self._stream_tasks[hostname] = task

    def _stop_metrics_stream(self, hostname: str):
        """Cancel the metrics stream task for a host."""
        task = self._stream_tasks.pop(hostname, None)
        if task and not task.done():
            task.cancel()

    def _restart_dead_streams(self):
        """Restart metric streams that have ended."""
        for hostname in list(self._stream_tasks):
            task = self._stream_tasks[hostname]
            if task.done():
                if hostname in self.pool.connected_hosts():
                    _log(SERVICE, f"Restarting dead metrics stream for "
                         f"{hostname}")
                    self._start_metrics_stream(hostname)
                else:
                    self._stream_tasks.pop(hostname, None)

    async def _metrics_stream(self, hostname: str):
        """Read system metrics from SSH stdout (JSON per line, 1/s)."""
        try:
            proc = await self.pool.start_process(hostname, MONITOR_AGENT_SCRIPT)
            if not proc:
                _log(SERVICE, f"[metrics] Cannot start stream for {hostname}",
                     level="WARNING")
                return

            self.store.update_stream_info(hostname, {
                "alive": True, "active": True, "error": None,
            })

            async for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    metrics = SystemMetrics.from_json(data)
                    self.store.update_system(hostname, metrics)
                except json.JSONDecodeError:
                    continue

        except asyncio.CancelledError:
            pass
        except Exception as e:
            _log(SERVICE, f"[metrics] Stream error for {hostname}: {e}",
                 level="WARNING")
            self.store.update_stream_info(hostname, {
                "alive": False, "active": False, "error": str(e),
            })
        finally:
            self.store.update_stream_info(hostname, {
                "alive": False, "active": False, "error": None,
            })
            _log(SERVICE, f"[metrics] Stream ended for {hostname}")

    # ── Instance collection ─────────────────────────────────

    async def _collect_instances_all(self):
        """Collect bot instance data from all connected VPS."""
        now = time.time()
        if now - self._last_instance_collect < INSTANCE_COLLECT_INTERVAL:
            return
        self._last_instance_collect = now

        connected = self.pool.connected_hosts()
        targets = [
            h for h in connected
            if h in self._stream_tasks and not self._stream_tasks[h].done()
        ]
        if not targets:
            return

        results = await asyncio.gather(
            *(self._collect_instances(h) for h in targets),
            return_exceptions=True,
        )
        for hostname, result in zip(targets, results):
            if isinstance(result, Exception):
                _log(SERVICE, f"[instances] Error on {hostname}: {result}",
                     level="WARNING")

    async def _collect_instances(self, hostname: str):
        """Collect bot instances from a single VPS."""
        result = await self.pool.run(hostname, INSTANCE_COLLECT_SCRIPT,
                                     timeout=15)
        if result and result.exit_status == 0 and result.stdout:
            try:
                instances = json.loads(result.stdout.strip())
                self.store.update_instances(hostname, instances)
                _log(SERVICE, f"[instances] Collected {len(instances)} from "
                     f"{hostname}", level="DEBUG")
            except json.JSONDecodeError:
                pass

    # ── Service monitoring ──────────────────────────────────

    async def _check_service(self, hostname: str, svc: ServiceInfo
                             ) -> dict:
        """Check if a service is running on a VPS."""
        pid_path = f"{REMOTE_PBGUI_DIR}/{svc.pid_file}"

        # Step 1: Read PID file
        result = await self.pool.run(hostname, f'cat {pid_path}', timeout=10)
        if result is None:
            return {
                "status": ServiceStatus.UNKNOWN.value,
                "pid": None,
                "error": "SSH connection error",
                "was_restarted": False,
            }
        pid_str = (result.stdout or "").strip()
        if not pid_str.isdigit():
            return {
                "status": ServiceStatus.STOPPED.value,
                "pid": None,
                "error": "No PID file or invalid PID",
                "was_restarted": False,
            }
        pid = int(pid_str)

        # Step 2: Check if process is running
        check = await self.pool.run(
            hostname,
            f'ps -p {pid} -o cmd= 2>/dev/null | grep -qi '
            f'"{svc.process_match}" && echo "yes" || echo "no"',
            timeout=10,
        )
        if check is None:
            return {
                "status": ServiceStatus.UNKNOWN.value,
                "pid": pid,
                "error": "SSH error during process check",
                "was_restarted": False,
            }
        running = (check.stdout or "").strip() == "yes"
        return {
            "status": (ServiceStatus.RUNNING.value if running
                       else ServiceStatus.STOPPED.value),
            "pid": pid if running else None,
            "error": (None if running
                      else f"PID {pid} not running"),
            "was_restarted": False,
        }

    async def _restart_service(self, hostname: str,
                               service_name: str) -> bool:
        """Restart a service on a VPS (same logic as old ServiceMonitor)."""
        svc = MONITORED_SERVICES.get(service_name)
        if not svc:
            return False

        if not self._can_restart(hostname, service_name):
            _log(SERVICE, f"[service] Restart limit reached for "
                 f"{service_name} on {hostname}", level="WARNING")
            return False

        _log(SERVICE, f"[service] Restarting {service_name} on {hostname}")

        # Detect venv
        venv_check = await self.pool.run(
            hostname,
            f'test -f ~/software/venv_pbgui/bin/activate && echo "venv_pbgui" '
            f'|| (test -f ~/{REMOTE_PBGUI_DIR}/.venv/bin/activate '
            f'&& echo "dotvenv" || echo "system")',
            timeout=5,
        )
        venv_type = (venv_check.stdout or "").strip() if venv_check else "system"

        if venv_type == "venv_pbgui":
            start_cmd = (
                f"cd ~/{REMOTE_PBGUI_DIR} && "
                f"source ~/software/venv_pbgui/bin/activate && "
                f"nohup python -u starter.py -r {service_name} "
                f"> /dev/null 2>&1 &"
            )
        elif venv_type == "dotvenv":
            start_cmd = (
                f"cd ~/{REMOTE_PBGUI_DIR} && "
                f"source ~/{REMOTE_PBGUI_DIR}/.venv/bin/activate && "
                f"nohup python -u starter.py -r {service_name} "
                f"> /dev/null 2>&1 &"
            )
        else:
            start_cmd = (
                f"cd ~/{REMOTE_PBGUI_DIR} && "
                f"nohup python3 -u starter.py -r {service_name} "
                f"> /dev/null 2>&1 &"
            )

        result = await self.pool.run(hostname, start_cmd, timeout=15)
        if result and result.exit_status == 0:
            self._record_restart(hostname, service_name)
            _log(SERVICE, f"[service] {service_name} restart sent to "
                 f"{hostname}")
            return True
        _log(SERVICE, f"[service] Failed to restart {service_name} on "
             f"{hostname}", level="ERROR")
        return False

    async def _check_and_heal_services(self, hostnames: list[str]) -> dict:
        """Check + auto-heal all services on given hosts."""
        all_results: dict[str, dict] = {}
        for hostname in hostnames:
            host_svc: dict[str, dict] = {}
            for svc_name, svc_info in MONITORED_SERVICES.items():
                check = await self._check_service(hostname, svc_info)

                status_val = check["status"]
                alert_key = f"svc:{hostname}:{svc_name}"

                if status_val == ServiceStatus.STOPPED.value and self.auto_restart:
                    _log(SERVICE, f"[service] {svc_name} down on {hostname}, "
                         "attempting restart")
                    restarted = await self._restart_service(hostname, svc_name)
                    check["was_restarted"] = restarted
                    if restarted:
                        check["status"] = ServiceStatus.RESTARTING.value

                # Alerts
                if status_val == ServiceStatus.STOPPED.value:
                    if alert_key not in self._service_alerts:
                        self._service_alerts.add(alert_key)
                        if check.get("was_restarted"):
                            await self._send_alert(
                                f"🔄 *VPSMonitor*: {svc_name} was down on "
                                f"*{hostname}*, restart initiated"
                            )
                        else:
                            await self._send_alert(
                                f"❌ *VPSMonitor*: {svc_name} is down on "
                                f"*{hostname}*"
                            )
                elif status_val == ServiceStatus.RUNNING.value:
                    if alert_key in self._service_alerts:
                        self._service_alerts.discard(alert_key)
                        await self._send_alert(
                            f"✅ *VPSMonitor*: {svc_name} is running on "
                            f"*{hostname}*"
                        )

                host_svc[svc_name] = check
            all_results[hostname] = host_svc
        return all_results

    # ── Restart rate limiting ───────────────────────────────

    def _can_restart(self, hostname: str, service_name: str) -> bool:
        history = self._restart_history.get(hostname, {}).get(
            service_name, []
        )
        now = datetime.now()
        history = [ts for ts in history if (now - ts).total_seconds() < 3600]
        self._restart_history.setdefault(hostname, {})[service_name] = history
        return len(history) < self.max_restarts_per_hour

    def _record_restart(self, hostname: str, service_name: str):
        self._restart_history.setdefault(hostname, {}).setdefault(
            service_name, []
        ).append(datetime.now())

    # ── Connection alerts ───────────────────────────────────

    def _handle_connection_changes(self, status: dict[str, ConnectionStatus]):
        newly_disconnected: list[str] = []
        for hostname, conn_status in status.items():
            alert_key = f"conn:{hostname}"
            if conn_status == ConnectionStatus.DISCONNECTED:
                if alert_key not in self._connection_alerts:
                    self._connection_alerts.add(alert_key)
                    newly_disconnected.append(hostname)
            elif conn_status == ConnectionStatus.CONNECTED:
                self._connection_alerts.discard(alert_key)

        if not newly_disconnected:
            return

        total_hosts = len(status)
        # Mass disconnect: ≥50% of monitored hosts lost simultaneously
        if len(newly_disconnected) >= max(2, total_hosts * 0.5):
            hosts_str = ", ".join(sorted(newly_disconnected))
            asyncio.create_task(self._send_alert(
                f"⚠️ *VPSMonitor*: Network blip — SSH lost to "
                f"*{len(newly_disconnected)}* hosts ({hosts_str})"
            ))
        else:
            for hostname in newly_disconnected:
                asyncio.create_task(self._send_alert(
                    f"⚠️ *VPSMonitor*: SSH connection lost to "
                    f"*{hostname}*"
                ))

    async def _send_alert(self, message: str):
        """Send Telegram alert."""
        if not self.telegram_token or not self.telegram_chat_id:
            _log(SERVICE, f"[alert] No Telegram config: {message}",
                 level="WARNING")
            return
        try:
            from telegram import Bot
            bot = Bot(token=self.telegram_token)
            async with bot:
                await bot.send_message(
                    chat_id=self.telegram_chat_id,
                    text=message,
                    parse_mode='Markdown',
                )
            _log(SERVICE, f"[alert] Sent: {message}")
        except Exception as e:
            _log(SERVICE, f"[alert] Failed: {e}", level="ERROR")

    # ── Kill instance (called by WebSocket command) ─────────

    async def kill_instance(self, hostname: str, name: str,
                            pb_version: str = "") -> dict:
        """Kill a bot instance on a VPS."""
        if pb_version == "7":
            grep_pattern = f"main.py.*{name}"
        elif pb_version == "6":
            grep_pattern = f"passivbot_multi.py.*{name}"
        elif pb_version == "s":
            grep_pattern = f"passivbot.py.*{name}"
        else:
            grep_pattern = (
                f"(main.py|passivbot_multi.py|passivbot.py).*{name}"
            )

        kill_cmd = (
            f"pid=$(ps aux | grep -E '{grep_pattern}' | grep -v grep "
            f"| awk '{{print $2}}' | head -1) && "
            f'[ -n "$pid" ] && kill $pid && echo "killed:$pid" '
            f'|| echo "not_found"'
        )

        result = await self.pool.run(hostname, kill_cmd, timeout=15)
        success = (result and result.exit_status == 0
                   and "killed:" in (result.stdout or ""))
        killed_pid = ""
        if success:
            killed_pid = result.stdout.split("killed:")[1].strip()

        _log(SERVICE,
             f"[cmd] Kill instance {name} on {hostname}: "
             f"{'OK pid=' + killed_pid if success else 'not found'}",
             level="INFO" if success else "WARNING")

        return {
            "success": success,
            "pid": killed_pid,
        }
