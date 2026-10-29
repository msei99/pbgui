# Unreleased

## Fixes

- Normalize PB8 base and scenario datetime strings to calendar dates at the shared server preparation/load boundary, so Backtest and Optimize saves keep `YYYY-MM-DD` dates. Cloud history export and integrity checks now also accept legacy ISO datetime bounds without changing warmup or suite envelopes.
