# VPS Monitoring Alerts

VPS monitoring alerts now run inside **PBAPIServer** together with the live
VPS monitor. There is no separate `PBMon` daemon anymore.

## What is monitored

The API server keeps the VPS monitor connected and evaluates active alert
conditions directly from the live in-memory state.

| Alert type | Triggered when |
|------------|----------------|
| **Offline host** | SSH connection to a monitored VPS is lost |
| **Service problem** | A monitored VPS service is down or a restart was initiated |
| **System threshold** | A host exceeds configured memory, CPU, swap, or disk thresholds |
| **Instance threshold** | A monitored Passivbot instance exceeds configured limits |
| **HL API key expiry** | A Hyperliquid API key is about to expire |

Active alerts are shown in the navigation bar as a dedicated alert indicator.
The badge displays `new/ack` counts.

## Where to configure it

Open:

1. **System -> Services**
2. Select **PBAPIServer**
3. Open the **Settings** tab
4. Go to the **VPS Monitoring** section

The `Alerts / Telegram` block lets you configure:

- **Telegram Bot Token**
- **Telegram Chat ID**
- Which active alert groups are visible in the GUI
- Which problem and recovery events are sent to Telegram

Settings are grouped into:

- **Offline Hosts**
- **Services**
- **System Thresholds**
- **Instance Thresholds**

Each group allows fine-grained Telegram routing while still keeping the UI compact.

## GUI behavior

- The GUI shows **only currently active problems**
- Cleared alerts disappear automatically
- If a problem returns later, it becomes **new/unacknowledged** again
- You can acknowledge single alerts or all visible alerts from the nav overlay

## Telegram setup

1. Open Telegram and chat with **[@BotFather](https://t.me/botfather)**
2. Send `/newbot` and copy the **Bot Token**
3. Start a conversation with the bot using `/start`
4. Find your **Chat ID**, for example via **[@userinfobot](https://t.me/userinfobot)**
5. Save both values in **PBAPIServer -> Settings -> VPS Monitoring**

> The bot cannot message you until you have sent it at least one message first.

## Logs

Alert routing and VPS monitoring activity are logged through the API server and
VPS monitor logs, mainly:

- `PBApiServer.log`
- `VPSMonitor.log`

## Troubleshooting

| Symptom | Check |
|---------|-------|
| No Telegram alerts | Verify Bot Token and Chat ID in PBAPIServer settings · check `PBApiServer.log` and `VPSMonitor.log` |
| Alert badge stays empty | Confirm the alert group is enabled for GUI visibility and that the VPS host is included in monitored hosts |
| "Chat not found" | Send `/start` to the bot before testing alerts |
| Alerts disappear after recovery | Expected: the GUI lists only active problems |
