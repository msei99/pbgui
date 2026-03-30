# PBRun Service Details

PBRun is the local service orchestrator for PBGui. It keeps bot processes in sync with configured instances and updates runtime files used by Passivbot.

## What PBRun does

PBRun runs a 5-second daemon loop that:

- Starts/stops local Passivbot processes for configured instances (PB7, PB6 Multi, PB6 Single)
- Monitors each running bot's resource usage (CPU, memory) and collects PnL/error/traceback counts from log files
- Watches dynamic coin filters (via PBCoinData mappings) and writes `ignored_coins.json` / `approved_coins.json`
- Reacts to activation and status command files from PBRemote (filesystem-based message queue in `data/cmd/`)
- Runs a memory watchdog: if free system memory drops below 250 MB, it restarts the bot with the highest memory usage
- Writes service logs to `data/logs/PBRun.log`

## PBRun detail panel

Click the PBRun card on the Services overview (or use the sidebar) to open the detail panel:

- The control strip at the top shows the current status (running/stopped) and Start/Stop/Restart buttons
- The Log tab shows a live filtered PBRun log viewer

## Typical startup behavior

After a restart or first run, PBRun may log large `Change ignored_coins` / `Change approved_coins` updates. This is normal while dynamic coin lists are initialized from current mapping data.

## Troubleshooting quick checks

- Confirm PBRun is running in Services
- Check `data/logs/PBRun.log` for recent `ERROR` lines
- Verify `data/run_v7/<instance>/ignored_coins.json` and `approved_coins.json` exist and are valid JSON lists
- If dynamic lists look stale, restart PBRun once after PBCoinData mapping updates complete
