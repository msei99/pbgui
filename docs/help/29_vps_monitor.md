# VPS Monitor

VPS Monitor is a real-time operations dashboard for all configured VPS hosts.

It uses PBMaster as backend and receives updates over WebSocket, so UI updates are live without Streamlit reruns.

## What you can monitor

- Host connection state (connected/disconnected)
- System metrics per host (CPU, RAM, disk, swap)
- Service state per host (PBRun, PBRemote, PBCoinData)
- Bot instances across hosts
- Live logs for services and bots

## Tabs and workflow

- **Dashboard**: quick health overview for all hosts
- **Instances**: running bot instances and status details
- **Services**: restart services on selected host
- **Live Logs**: stream logs in real time

## Live log features

- Real file line numbers
- Group collapse/expand for log blocks
- Full-text search with highlighting
- Auto-scroll and compact mode
- Host/service selectors and stream toggle

## Requirements

- PBMaster must be running
- Target VPS hosts must be enabled in PBMaster settings
- PBMaster WebSocket port must be reachable from the UI process

## Troubleshooting

- **No data shown**: start PBMaster from `System → Services`
- **One host missing**: confirm host is enabled in PBMaster settings
- **Logs do not stream**: check WebSocket port and PBMaster log for connection errors
