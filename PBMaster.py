"""
PBMaster — SSH-based real-time VPS management service for PBGui.

Maintains persistent SSH connections to all registered VPS servers.
Monitors PBRun, PBRemote, PBCoinData services and restarts them if needed.
Sends alerts via PBMon (Telegram) on connection loss or service failures.
Provides live log streaming from remote VPS servers.

Runs as a background daemon, following the same pattern as PBMon/PBRun/PBRemote.
"""

import asyncio
import os
import platform
import subprocess
import sys
import traceback
from datetime import datetime
from io import TextIOWrapper
from pathlib import Path, PurePath
from time import sleep

import psutil

from pbgui_purefunc import PBGDIR, load_ini, save_ini
from logging_helpers import human_log as _log
from ini_watcher import IniWatcher
from master.connection_pool import SSHConnectionPool, ConnectionStatus
from master.command_executor import CommandExecutor
from master.service_monitor import ServiceMonitor, ServiceStatus
from master.log_streamer import LogStreamer
from master.realtime_collector import RealtimeCollector
from master.ws_server import WSServer


SERVICE = "PBMaster"

# Main loop interval (seconds)
LOOP_INTERVAL = 15

# How often to do a full service check (every N loops)
SERVICE_CHECK_EVERY = 4  # = 60 seconds at 15s interval


class PBMaster:
    """
    SSH-based real-time VPS management daemon.

    Components:
        - ConnectionPool: Manages persistent SSH connections
        - CommandExecutor: Runs commands on remote VPS
        - ServiceMonitor: Monitors and restarts services
        - LogStreamer: Streams logs from remote VPS

    Lifecycle (same pattern as PBMon):
        master = PBMaster()
        master.run()       # Start daemon in background
        master.stop()      # Stop daemon
        master.is_running() # Check if running
    """

    def __init__(self):
        self.piddir = Path(f'{PBGDIR}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbmaster.pid')
        self.my_pid = None

        # Configuration (persisted in pbgui.ini)
        self._auto_restart = None
        self._monitor_interval = None
        self._ws_port = None
        self._enabled_hosts = None  # set of hostnames or None (not loaded yet)

        # Components (initialized in _setup())
        self.pool: SSHConnectionPool = None
        self.executor: CommandExecutor = None
        self.monitor: ServiceMonitor = None
        self.streamer: LogStreamer = None
        self.realtime: RealtimeCollector = None
        self.ws_server: WSServer = None

        # Alert state tracking (to avoid duplicate alerts)
        self._connection_alerts: set[str] = set()
        self._service_alerts: set[str] = set()

        # Telegram config (shares with PBMon)
        self._telegram_token = ""
        self._telegram_chat_id = ""

        # ini file watcher for instant config change detection
        self._ini_watcher = IniWatcher()

    # ── Configuration properties (persisted in pbgui.ini) ──

    @property
    def auto_restart(self) -> bool:
        if self._auto_restart is None:
            val = load_ini("pbmaster", "auto_restart")
            self._auto_restart = val.lower() == "true" if val else True
        return self._auto_restart

    @auto_restart.setter
    def auto_restart(self, value: bool):
        if self._auto_restart != value:
            self._auto_restart = value
            save_ini("pbmaster", "auto_restart", str(value))

    @property
    def monitor_interval(self) -> int:
        if self._monitor_interval is None:
            val = load_ini("pbmaster", "monitor_interval")
            self._monitor_interval = int(val) if val and val.isdigit() else LOOP_INTERVAL
        return self._monitor_interval

    @monitor_interval.setter
    def monitor_interval(self, value: int):
        if self._monitor_interval != value:
            self._monitor_interval = max(5, value)
            save_ini("pbmaster", "monitor_interval", str(self._monitor_interval))

    @property
    def ws_port(self) -> int:
        if self._ws_port is None:
            val = load_ini("pbmaster", "ws_port")
            self._ws_port = int(val) if val and val.isdigit() else 8765
        return self._ws_port

    @ws_port.setter
    def ws_port(self, value: int):
        if self._ws_port != value:
            self._ws_port = max(1024, min(65535, value))
            save_ini("pbmaster", "ws_port", str(self._ws_port))

    @property
    def enabled_hosts(self) -> set[str]:
        """Set of VPS hostnames that PBMaster should monitor. Empty = none."""
        if self._enabled_hosts is None:
            val = load_ini("pbmaster", "enabled_hosts")
            if val:
                self._enabled_hosts = {h.strip() for h in val.split(",") if h.strip()}
            else:
                self._enabled_hosts = set()
        return self._enabled_hosts

    @enabled_hosts.setter
    def enabled_hosts(self, value: set[str]):
        self._enabled_hosts = set(value)
        save_ini("pbmaster", "enabled_hosts", ",".join(sorted(self._enabled_hosts)))

    def is_host_enabled(self, hostname: str) -> bool:
        """Check if a specific VPS is enabled for monitoring."""
        return hostname in self.enabled_hosts

    def available_hosts(self) -> list[str]:
        """Return sorted list of all known VPS hostnames from config files."""
        import json
        import glob
        from pathlib import Path
        vps_dir = Path(f'{PBGDIR}/data/vpsmanager/hosts')
        hostnames = []
        pattern = str(vps_dir / '*' / '*.json')
        for filepath in sorted(glob.glob(pattern)):
            try:
                with open(filepath, 'r') as f:
                    config = json.load(f)
                hostname = config.get('_hostname')
                if hostname:
                    hostnames.append(hostname)
            except Exception:
                pass
        return sorted(set(hostnames))

    @property
    def telegram_token(self):
        if not self._telegram_token:
            self._telegram_token = load_ini("main", "telegram_token")
        return self._telegram_token

    @property
    def telegram_chat_id(self):
        if not self._telegram_chat_id:
            self._telegram_chat_id = load_ini("main", "telegram_chat_id")
        return self._telegram_chat_id

    # ── Process lifecycle (same pattern as PBMon) ──

    def run(self):
        """Start PBMaster as a background daemon."""
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/PBMaster.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR,
                                 text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR,
                                 text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    _log(SERVICE, "Cannot start PBMaster", level="ERROR")
                    break
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        """Stop the PBMaster daemon."""
        if self.is_running():
            _log(SERVICE, "Stop: PBMaster")
            try:
                psutil.Process(self.my_pid).kill()
            except psutil.NoSuchProcess:
                pass

    def restart(self):
        """Restart the PBMaster daemon."""
        if self.is_running():
            self.stop()
            sleep(2)
        self.run()

    def is_running(self) -> bool:
        """Check if PBMaster daemon is running."""
        self.load_pid()
        try:
            if (self.my_pid and psutil.pid_exists(self.my_pid) and
                    any(sub.lower().endswith("pbmaster.py")
                        for sub in psutil.Process(self.my_pid).cmdline())):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

    def load_pid(self):
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read()
                self.my_pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        self.my_pid = os.getpid()
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))

    # ── Alert system ──

    async def _send_alert(self, message: str):
        """Send a Telegram alert (reuses PBMon's Telegram config)."""
        if not self.telegram_token or not self.telegram_chat_id:
            _log(SERVICE, f"[alert] No Telegram config, skipping alert: {message}",
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
            _log(SERVICE, f"[alert] Failed to send Telegram message: {e}",
                 level="ERROR")

    def _send_alert_sync(self, message: str):
        """Synchronous wrapper for sending alerts."""
        try:
            asyncio.run(self._send_alert(message))
        except Exception as e:
            _log(SERVICE, f"[alert] Alert send error: {e}", level="ERROR")

    # ── Core setup and loop ──

    def _setup(self):
        """Initialize all components. Called once at daemon start."""
        _log(SERVICE, "Initializing components...")

        self.pool = SSHConnectionPool()
        self.executor = CommandExecutor(self.pool)
        self.monitor = ServiceMonitor(self.executor, auto_restart=self.auto_restart)
        self.streamer = LogStreamer(self.pool)
        self.realtime = RealtimeCollector(self.pool)
        self.ws_server = WSServer(self)

        # Load VPS configs — only load enabled hosts into pool
        all_hostnames = self.pool.load_vps_configs()
        _log(SERVICE, f"Found {len(all_hostnames)} VPS configurations")

        enabled = self.enabled_hosts
        if not enabled:
            _log(SERVICE, "No VPS hosts enabled for monitoring. "
                 "Enable hosts in Services \u2192 PBMaster \u2192 Settings.")
            # Remove all hosts from pool so reconnect_lost won't touch them
            self.pool.disconnect_all()
            with self.pool._lock:
                self.pool._connections.clear()
            # Still start WS server so UI can connect
            self.ws_server.start()
            # Start ini watcher so we detect when user enables hosts
            self._ini_watcher.start()
            return

        # Remove non-enabled hosts from pool
        for hostname in list(self.pool._connections.keys()):
            if hostname not in enabled:
                self.pool.disconnect(hostname)
                with self.pool._lock:
                    self.pool._connections.pop(hostname, None)

        targets = [h for h in self.pool.hostnames() if h in enabled]
        _log(SERVICE, f"Enabled hosts: {', '.join(sorted(enabled))} "
             f"({len(targets)} found in configs)")

        if targets:
            results = {}
            for hostname in targets:
                results[hostname] = self.pool.connect(hostname)
            connected = sum(1 for v in results.values() if v)
            _log(SERVICE, f"Connected to {connected}/{len(results)} VPS servers")

            # Start realtime monitoring streams for connected hosts
            for hostname, success in results.items():
                if success:
                    self.realtime.start_stream(hostname)

        # Start WebSocket server for browser push
        self.ws_server.start()

        # Start ini file watcher for instant config change detection
        self._ini_watcher.start()
        _log(SERVICE, "ini file watcher started")

    def _apply_config_changes(self):
        """Re-read config from ini and apply host enable/disable changes."""
        prev_enabled = self._enabled_hosts or set()
        # Invalidate all cached config values
        self._enabled_hosts = None
        self._auto_restart = None
        self._monitor_interval = None
        enabled = self.enabled_hosts

        # Update auto_restart on service monitor
        if self.monitor:
            self.monitor.auto_restart = self.auto_restart

        # Detect config changes: newly enabled / disabled hosts
        newly_enabled = enabled - prev_enabled
        newly_disabled = prev_enabled - enabled

        if newly_disabled:
            _log(SERVICE, f"Hosts disabled: {', '.join(sorted(newly_disabled))}")
            for h in newly_disabled:
                if self.realtime:
                    self.realtime.stop_stream(h)
                if self.pool:
                    self.pool.disconnect(h)
                    with self.pool._lock:
                        self.pool._connections.pop(h, None)

        if newly_enabled:
            _log(SERVICE, f"Hosts newly enabled: {', '.join(sorted(newly_enabled))}")
            self.pool.load_vps_configs()
            for h in list(self.pool._connections.keys()):
                if h not in enabled:
                    self.pool.disconnect(h)
                    with self.pool._lock:
                        self.pool._connections.pop(h, None)
            for h in newly_enabled:
                if h in self.pool.hostnames():
                    if self.pool.connect(h):
                        if self.realtime:
                            self.realtime.start_stream(h)

    def _loop_iteration(self, loop_count: int):
        """Single iteration of the main loop."""
        # Apply any pending config changes (triggered by ini watcher)
        if self._ini_watcher.changed.is_set():
            self._ini_watcher.changed.clear()
            self._apply_config_changes()

        enabled = self.enabled_hosts

        if not enabled:
            return

        # 1. Connection health check (only enabled hosts)
        full_status = self.pool.health_check()
        status = {h: s for h, s in full_status.items() if h in enabled}
        self._handle_connection_changes(status)

        # 2. Reconnect lost connections (only enabled)
        all_reconnected = self.pool.reconnect_lost()
        reconnected = {h: s for h, s in all_reconnected.items() if h in enabled}
        # Disconnect any non-enabled hosts that reconnect_lost may have touched
        for h in all_reconnected:
            if h not in enabled and all_reconnected[h]:
                self.pool.disconnect(h)
        for hostname, success in reconnected.items():
            if success:
                _log(SERVICE, f"Reconnected to {hostname}")
                # Restart realtime stream for reconnected host
                self.realtime.start_stream(hostname)
                alert_key = f"conn:{hostname}"
                if alert_key in self._connection_alerts:
                    self._connection_alerts.discard(alert_key)
                    self._send_alert_sync(
                        f"✅ *PBMaster*: SSH reconnected to *{hostname}*"
                    )

        # 3. Restart dead realtime streams
        self.realtime.restart_dead_streams()

        # 4. Collect instance data (every ~30s, managed internally)
        self.realtime.collect_instances_all()

        # 5. Service monitoring (every N iterations)
        if loop_count % SERVICE_CHECK_EVERY == 0:
            connected_hosts = [
                h for h, s in status.items()
                if s == ConnectionStatus.CONNECTED
            ]
            if connected_hosts:
                service_results = self._check_and_heal_services(connected_hosts)
                # Feed results to WS server for push to clients
                if self.ws_server:
                    self.ws_server.update_services(service_results)

    def _handle_connection_changes(self, status: dict[str, ConnectionStatus]):
        """Detect new disconnections and send alerts."""
        for hostname, conn_status in status.items():
            alert_key = f"conn:{hostname}"

            if conn_status == ConnectionStatus.DISCONNECTED:
                if alert_key not in self._connection_alerts:
                    self._connection_alerts.add(alert_key)
                    self._send_alert_sync(
                        f"⚠️ *PBMaster*: SSH connection lost to *{hostname}*"
                    )
            elif conn_status == ConnectionStatus.CONNECTED:
                # Connection restored
                self._connection_alerts.discard(alert_key)

    def _check_and_heal_services(self, hostnames: list[str]) -> dict:
        """Check services on all connected VPS and auto-heal if enabled.
        Returns dict of {hostname: {service: status}} for status file."""
        all_results = {}
        for hostname in hostnames:
            results = self.monitor.auto_heal(hostname)
            host_svc = {}

            for check in results:
                host_svc[check.service] = {
                    "status": check.status.value,
                    "pid": check.pid,
                    "error": check.error,
                    "was_restarted": check.was_restarted,
                }
                alert_key = f"svc:{hostname}:{check.service}"

                if check.status == ServiceStatus.STOPPED:
                    if alert_key not in self._service_alerts:
                        self._service_alerts.add(alert_key)
                        if check.was_restarted:
                            self._send_alert_sync(
                                f"🔄 *PBMaster*: {check.service} was down on "
                                f"*{hostname}*, restart initiated"
                            )
                        else:
                            self._send_alert_sync(
                                f"❌ *PBMaster*: {check.service} is down on "
                                f"*{hostname}* (auto-restart disabled or limit reached)"
                            )

                elif check.status == ServiceStatus.RUNNING:
                    if alert_key in self._service_alerts:
                        self._service_alerts.discard(alert_key)
                        self._send_alert_sync(
                            f"✅ *PBMaster*: {check.service} is running again on "
                            f"*{hostname}*"
                        )

                elif check.status == ServiceStatus.RESTARTING:
                    pass  # Will be checked on next iteration

            all_results[hostname] = host_svc
        return all_results

    def _shutdown(self):
        """Clean shutdown of all components."""
        _log(SERVICE, "Shutting down...")
        self._ini_watcher.stop()
        if self.ws_server:
            self.ws_server.stop()
        if self.realtime:
            self.realtime.stop_all_streams()
        if self.streamer:
            self.streamer.stop_all_streams()
        if self.pool:
            self.pool.disconnect_all()
        _log(SERVICE, "Shutdown complete")


def main():
    """Daemon entry point (same pattern as PBMon)."""
    dest = Path(f'{PBGDIR}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBMaster.log')
    sys.stdout = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBMaster')

    pbmaster = PBMaster()
    if pbmaster.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBMaster already started')
        exit(1)
    pbmaster.save_pid()

    try:
        pbmaster._setup()
    except Exception as e:
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error during setup: {e}')
        traceback.print_exc()
        exit(1)

    loop_count = 0
    while True:
        try:
            if logfile.exists():
                if logfile.stat().st_size >= 10485760:
                    logfile.replace(f'{str(logfile)}.old')
                    sys.stdout = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile, "ab", 0), write_through=True)

            pbmaster._loop_iteration(loop_count)
            loop_count += 1
            # Sleep but wake instantly when pbgui.ini changes
            pbmaster._ini_watcher.changed.wait(timeout=pbmaster.monitor_interval)

        except KeyboardInterrupt:
            pbmaster._shutdown()
            break
        except Exception as e:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} '
                  f'Error in main loop, but continue: {e}')
            traceback.print_exc()
            pbmaster._ini_watcher.changed.wait(timeout=pbmaster.monitor_interval)


if __name__ == '__main__':
    main()
