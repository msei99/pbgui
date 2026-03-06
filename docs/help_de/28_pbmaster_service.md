# VPS-Einstellungen (ehemals PBMaster)

> **PBMaster wurde ersetzt.** Die Funktionalität ist jetzt Teil des **PBAPIServer**.

- Für die **Dienste-Übersicht** und Startreihenfolge siehe **Guide 23 — Services Overview**.
- Für die **PBAPIServer**-Einstellungen (Host, Port, VPS-Hosts) siehe **Guide 26 — PBAPIServer**.
- Für das **VPS Monitor**-Dashboard und Live-Logs siehe **Guide 29 — VPS Monitor**.

## Was sich geändert hat

- PBMaster lief als separater Daemon auf Port 8765. Er wurde entfernt.
- Der **PBAPIServer** (FastAPI, Port 8000) übernimmt jetzt alle SSH-Verbindungen, VPS-Monitoring und WebSocket-Streaming.
- VPS-Host-Einstellungen wurden von `System → Services → PBMaster` nach `System → Services → PBAPIServer → Settings` verschoben.
- INI-Sektionen wurden automatisch umbenannt: `[pbmaster]` → `[vps_monitor]`, `[pbmaster_ui]` → `[vps_monitor_ui]`.
- Log-Ausgabe verschoben von `PBMaster.log` nach `VPSMonitor.log` (SSH-Verbindungen, Metriken) und `PBApiServer.log` (API-Server Start/Requests).
