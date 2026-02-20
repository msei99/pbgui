# PBRun Service Details

PBRun is the local service orchestrator for PBGui. It keeps bot processes in sync with configured instances and updates runtime files used by Passivbot.

## What PBRun does

- Starts/stops local Passivbot processes for configured instances
- Watches dynamic coin filters and writes `ignored_coins.json` / `approved_coins.json`
- Reacts to activation/status command files from PBRemote
- Writes service logs to `data/logs/PBRun.log`

## PBRun Details page

On the `System → Services → PBRun → Show Details` page you can:

- Check current PBRun service status (running/stopped)
- Toggle the service on/off
- Use the integrated filtered PBRun log viewer in the details section (no separate `Show logfile` toggle)

## Typical startup behavior

After a restart or first run, PBRun may log large `Change ignored_coins` / `Change approved_coins` updates. This is normal while dynamic coin lists are initialized from current mapping data.

## Troubleshooting quick checks

- Confirm PBRun is running in Services
- Check `data/logs/PBRun.log` for recent `ERROR` lines
- Verify `data/run_v7/<instance>/ignored_coins.json` and `approved_coins.json` exist and are valid JSON lists
- If dynamic lists look stale, restart PBRun once after PBCoinData mapping updates complete
