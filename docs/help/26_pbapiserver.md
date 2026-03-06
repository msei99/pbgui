# PBAPIServer

The **PBAPIServer** runs a FastAPI backend (REST + WebSocket) that powers real-time features in PBGui — including the VPS Monitor, Market Data status, and the Gap/Coverage Heatmap.

## Status button

The **🟢 PBAPIServer ●** / **🔴 PBAPIServer ○** button in the sidebar shows the current state. Click it to restart the server. A toast notification confirms the result.

## Settings

Open **Settings** from the sidebar to configure:

- **Endpoints** — current API, Docs, WebSocket and Frontend URLs (read-only, updates after save + restart)
- **Bind address / Port** — network interface and port (default: `0.0.0.0:8000`). Requires restart to take effect.
  - `0.0.0.0` — accessible from any network interface (remote access)
  - `127.0.0.1` — localhost only
- **VPS Monitoring — Auto-restart** — automatically restart monitored services if they go down
- **VPS Monitoring — Monitored VPS Hosts** — select which VPS servers to monitor

Save all settings with the **💾** button in the sidebar.

## Log

All PBAPIServer activity is logged to `PBApiServer.log`, visible in the log viewer on the main tab.

## Troubleshooting

- **🔴 status**: click the button to restart; check `PBApiServer.log` for errors
- **VPS Monitor shows no data**: ensure PBAPIServer is running and the WebSocket endpoint is reachable
- **Port conflict**: change the port in Settings, save, then restart
