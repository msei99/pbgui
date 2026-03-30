# PBMon Service

PBMon is a lightweight background daemon that watches over your PBRemote servers and
running Passivbot instances. When it detects a problem it sends you a Telegram message
so you can react immediately — without having to check the UI manually.

## What PBMon monitors

PBMon calls PBRemote's error-detection logic every 60 seconds and checks for:

| Alert type | Triggered when |
|------------|----------------|
| **Server offline** | A remote VPS is unreachable |
| **System resource** | A server exceeds the configured memory, CPU, swap or disk thresholds |
| **Instance error** | A running Passivbot instance reports an error or unexpected stop |
| **HL API key expiry** | A Hyperliquid API key is about to expire (configurable warning days in `pbgui.ini`) |

Each alert is sent **once** per error occurrence. PBMon tracks which errors have already
been reported and only sends a new message when a previously-cleared error reappears.
HL key expiry warnings are deduplicated to once per user per day.

## Setup: create a Telegram bot

1. Open Telegram and chat with **[@BotFather](https://t.me/botfather)**
2. Send `/newbot` and follow the prompts — copy the **Bot Token**
3. Start a conversation with your new bot (send `/start`)
4. Find your **Chat ID** — easiest via **[@userinfobot](https://t.me/userinfobot)**
5. Enter both values in the PBMon Settings (see below) and save

> **Important:** your bot cannot send you a message until you have sent it at least
> one message first. The `/start` command is enough.

## PBMon detail panel — Log tab (default)

Click the PBMon card on the Services overview (or use the sidebar) to open the detail panel.
The **Log** tab loads by default, showing a live filtered stream of `PBMon.log`.

- Use the **level filter** buttons (DBG / INF / WRN / ERR / CRT) to focus on what matters
- Use **Search** to find a specific server name or error text
- Open the **Files** sidebar to switch to rotated log archives (`.1`, `.old`) if available

## PBMon detail panel — Settings tab

Switch to the **Settings** tab to configure Telegram notifications.

| Field | Description |
|-------|-------------|
| **Telegram Bot Token** | Token from @BotFather (stored in `pbgui.ini`) |
| **Telegram Chat ID** | Your personal or group chat ID |

Click **Save** to store the settings.

> Changes take effect on the **next monitoring cycle** (within 60 seconds) — no restart needed.

## Starting and stopping PBMon

Use the **Start** / **Stop** buttons in the control strip at the top of the PBMon detail panel, or use the buttons on the PBMon overview card.

- **Start** → spawns PBMon as a detached background process
- **Stop** → gracefully stops the process

## Log format

PBMon uses the central PBGui logger. Each line follows the format:

```
2026-03-01T12:55:50.123 [PBMon] [INFO] Start: PBMon
2026-03-01T12:55:52.456 [PBMon] [INFO] Send Message: Server: *myVPS* is offline
2026-03-01T12:58:00.789 [PBMon] [ERROR] Something went wrong, but continue: ...
```

## Troubleshooting

| Symptom | Check |
|---------|-------|
| No Telegram alerts | Verify Token and Chat ID in Settings · Check `PBMon.log` for `[ERROR]` lines |
| "Chat not found" error | Make sure you sent `/start` to the bot before PBMon first ran |
| PBMon won't start | Another instance may be running — check `data/pid/pbmon.pid` |
| Alerts stop arriving | PBMon may have crashed — recheck status in the Overview tab |
