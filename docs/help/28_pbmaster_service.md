# VPS Settings (formerly PBMaster)

> **PBMaster has been replaced.** Its functionality is now part of **PBAPIServer**.

- For the **services overview** and startup sequence, see **Guide 23 — Services Overview**.
- For the **PBAPIServer** settings (host, port, VPS hosts), see **Guide 26 — PBAPIServer**.
- For the **VPS Monitor** dashboard and live logs, see **Guide 29 — VPS Monitor**.

## What changed

- PBMaster ran as a separate daemon on port 8765. It has been removed.
- The **PBAPIServer** (FastAPI, port 8000) now handles all SSH connections, VPS monitoring, and WebSocket streaming.
- VPS host settings moved from `System → Services → PBMaster` to `System → Services → PBAPIServer → Settings`.
- INI sections were renamed automatically: `[pbmaster]` → `[vps_monitor]`, `[pbmaster_ui]` → `[vps_monitor_ui]`.
- Log output moved from `PBMaster.log` to `VPSMonitor.log` (SSH connections, metrics) and `PBApiServer.log` (API server startup/requests).
