# PBMaster Service Details

PBMaster is the SSH-based VPS management service for PBGui.

It keeps persistent SSH connections to enabled VPS hosts, monitors key services, and provides the real-time data backend for the VPS Monitor page.

## What PBMaster does

- Maintains persistent SSH sessions to enabled VPS hosts
- Collects host status and service state continuously
- Supports remote service restart actions from the monitor UI
- Streams live logs through a local WebSocket server
- Writes logs to `data/logs/PBMaster.log`

## Requirements

- This service is intended for the **Master** node
- VPS hosts must be configured in VPS Manager
- SSH key setup must be completed for each VPS

## PBMaster details page

On `System → Services → PBMaster → Show Details` you can:

- Start/stop PBMaster
- Configure `Auto-restart services`
- Set `Monitor interval (seconds)`
- Set `WebSocket port` for the VPS Monitor frontend
- Enable/disable monitored VPS hosts
- Open the integrated filtered PBMaster log viewer

## Host enablement behavior

- Hosts are disabled by default for PBMaster monitoring
- Enable hosts explicitly in PBMaster settings
- Changes take effect after saving and restarting PBMaster

## Troubleshooting

- **PBMaster not running**: the VPS Monitor page will show a warning and no live data
- **Host stays disconnected**: verify SSH keys and host reachability from the master
- **No live updates in VPS Monitor**: verify PBMaster WebSocket port and restart PBMaster after port changes
