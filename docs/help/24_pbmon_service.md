# PBMon Service Details

PBMon is a background service that monitors running Passivbot instances and sends alerts via Telegram if it detects unusual behavior or errors.

## What PBMon does

- Continuously checks the health and status of active bot processes
- Monitors for stuck positions, excessive errors, or unexpected stops
- Sends real-time alert messages to a configured Telegram chat
- Writes service logs to `data/logs/PBMon.log`

## Configuration

To use PBMon, you need to configure a Telegram Bot.

1. Create a bot via [@BotFather](https://t.me/botfather) on Telegram and get the **Bot Token**
2. Start a chat with your new bot and send a message
3. Get your **Chat ID** (you can use bots like `@userinfobot` to find your ID)
4. Enter both values in the PBMon Details page:
   - `Telegram Bot Token`
   - `Telegram Chat ID`

Changes are saved automatically. Restart the PBMon service for the new token to take effect.

## PBMon Details page

On the `System → Services → PBMon → Show Details` page you can:

- Check current PBMon service status (running/stopped)
- Toggle the service on/off
- Configure Telegram credentials
- Use the integrated filtered PBMon log viewer

## Troubleshooting

- **No alerts received**: Check `data/logs/PBMon.log` for Telegram API errors. Verify your Token and Chat ID are correct.
- **Bot must be started**: You must send at least one message (e.g., `/start`) to your Telegram bot before it can send messages to you.
