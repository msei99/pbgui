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
from master.connection_pool import SSHConnectionPool, ConnectionStatus
from master.command_executor import CommandExecutor
from master.service_monitor import ServiceMonitor, ServiceStatus
from master.log_streamer import LogStreamer


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

        # Components (initialized in _setup())
        self.pool: SSHConnectionPool = None
        self.executor: CommandExecutor = None
        self.monitor: ServiceMonitor = None
        self.streamer: LogStreamer = None

        # Alert state tracking (to avoid duplicate alerts)
        self._connection_alerts: set[str] = set()
        self._service_alerts: set[str] = set()

        # Telegram config (shares with PBMon)
        self._telegram_token = ""
        self._telegram_chat_id = ""

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

        # Load VPS configs and connect
        hostnames = self.pool.load_vps_configs()
        _log(SERVICE, f"Found {len(hostnames)} VPS configurations")

        if hostnames:
            results = self.pool.connect_all()
            connected = sum(1 for v in results.values() if v)
            _log(SERVICE, f"Connected to {connected}/{len(results)} VPS servers")

    def _loop_iteration(self, loop_count: int):
        """Single iteration of the main loop."""
        # 1. Connection health check
        status = self.pool.health_check()
        self._handle_connection_changes(status)

        # 2. Reconnect lost connections
        reconnected = self.pool.reconnect_lost()
        for hostname, success in reconnected.items():
            if success:
                _log(SERVICE, f"Reconnected to {hostname}")
                alert_key = f"conn:{hostname}"
                if alert_key in self._connection_alerts:
                    self._connection_alerts.discard(alert_key)
                    self._send_alert_sync(
                        f"✅ *PBMaster*: SSH reconnected to *{hostname}*"
                    )

        # 3. Service monitoring (every N iterations)
        if loop_count % SERVICE_CHECK_EVERY == 0:
            connected_hosts = [
                h for h, s in status.items()
                if s == ConnectionStatus.CONNECTED
            ]
            if connected_hosts:
                self._check_and_heal_services(connected_hosts)

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

    def _check_and_heal_services(self, hostnames: list[str]):
        """Check services on all connected VPS and auto-heal if enabled."""
        for hostname in hostnames:
            results = self.monitor.auto_heal(hostname)

            for check in results:
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

    def _shutdown(self):
        """Clean shutdown of all components."""
        _log(SERVICE, "Shutting down...")
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
            sleep(pbmaster.monitor_interval)

        except KeyboardInterrupt:
            pbmaster._shutdown()
            break
        except Exception as e:
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} '
                  f'Error in main loop, but continue: {e}')
            traceback.print_exc()
            sleep(pbmaster.monitor_interval)


if __name__ == '__main__':
    main()
