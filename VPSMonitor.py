"""Systemd-owned entry point for persistent VPS monitoring."""

from __future__ import annotations

import asyncio
import signal
from pathlib import Path

from credential_process_registry import ProcessCapabilityHeartbeat
from logging_helpers import human_log as _log
from master.vps_monitor_daemon import VPSMonitorRPCDaemon
from pbgui_purefunc import PBGDIR, load_ini
from secure_files import harden_sensitive_paths


SERVICE = "VPSMonitor"


async def main() -> None:
    """Run the signal-aware monitor daemon until systemd requests shutdown."""
    root = Path(PBGDIR)
    heartbeat: ProcessCapabilityHeartbeat | None = None
    daemon: VPSMonitorRPCDaemon | None = None
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass
    try:
        configured_pb7 = str(load_ini("main", "pb7dir") or "").strip()
        try:
            harden_sensitive_paths(
                root,
                Path(configured_pb7) if configured_pb7 else None,
                Path.home() / ".aws",
            )
        except Exception as exc:
            _log(SERVICE, f"Sensitive path hardening failed: {type(exc).__name__}", level="CRITICAL")
            raise
        heartbeat = ProcessCapabilityHeartbeat(root, SERVICE)
        heartbeat.__enter__()
        daemon = VPSMonitorRPCDaemon()
        await daemon.start()
        await stop_event.wait()
    except Exception as exc:
        _log(SERVICE, f"Daemon failed: {type(exc).__name__}", level="ERROR")
        raise
    finally:
        if daemon is not None:
            await daemon.stop()
        if heartbeat is not None:
            heartbeat.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _log(SERVICE, "Shutdown interrupted", level="INFO")
