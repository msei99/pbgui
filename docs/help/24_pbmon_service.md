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

Each alert is sent **once** per error occurrence. PBMon tracks which errors have already
been reported and only sends a new message when a previously-cleared error reappears.

## Setup: create a Telegram bot

1. Open Telegram and chat with **[@BotFather](https://t.me/botfather)**
2. Send `/newbot` and follow the prompts — copy the **Bot Token**
3. Start a conversation with your new bot (send `/start`)
4. Find your **Chat ID** — easiest via **[@userinfobot](https://t.me/userinfobot)**
5. Enter both values in the PBMon Settings (see below) and save

> **Important:** your bot cannot send you a message until you have sent it at least
> one message first. The `/start` command is enough.

## PBMon tab — viewer mode (default)

When you open **System → Services → PBMon**, the full-height log viewer loads immediately,
showing the live `PBMon.log` stream.

- Use the **level filter** buttons (DBG / INF / WRN / ERR) to focus on what matters
- Use **Search** to find a specific server name or error text
- The **Version** dropdown lets you look at rotated archives (`.1`, `.old`)

## PBMon tab — settings mode

Click **⚙ Settings** in the sidebar to open the settings view.

| Field | Description |
|-------|-------------|
| **Telegram Bot Token** | Token from @BotFather (stored in `pbgui.ini`) |
| **Telegram Chat ID** | Your personal or group chat ID |

Press **💾** to save and return to the viewer. The save button turns **primary (red)**
whenever there are unsaved changes.

> Changes take effect on the **next monitoring cycle** (within 60 seconds) — no restart needed.

## Starting and stopping PBMon

Use the **PBMon toggle** in the left sidebar on the PBMon tab:

- Toggle **on** → spawns PBMon as a detached background process
- Toggle **off** → gracefully stops the process

The **Overview** tab shows a ✅ / ❌ status indicator for all services at a glance.

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
