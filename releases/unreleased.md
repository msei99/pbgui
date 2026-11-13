# Unreleased

- VPS updates now accept PBCoinData's clean, monitor-agent-managed idle stop during systemd verification, so an otherwise healthy PBGui update can continue to the bot update.
- PB7/PB8 VPS wheel updates can retire obsolete Rust build tools after verifying the transferred bundle and before the installation capacity check. Cleanup still waits when another installed bot lacks a verified wheel runtime.
