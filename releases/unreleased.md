# Unreleased

## Fixes

- Vast jobs now check OHLCV integrity for every exported exchange/coin and the complete suite/warmup window before packaging or GPU rental. Failures list the exact incomplete days and missing minutes and remain visible in background preparation errors. Unchanged days reuse the Market Data integrity catalog; only missing or stale entries require a bounded validation pass. Frozen bundles carry an integrity proof, so unchecked legacy snapshots must be prepared again rather than rented or silently reused.
