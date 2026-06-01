# Unreleased

- Performance: Speed up Optimize config listing and delete refreshes by using cached lightweight config summaries and cached backtest counts instead of re-normalizing every config and re-scanning all backtests on every load.
- Performance: Speed up Optimize queue deletes by skipping full process scans for non-running items, adding a batch delete API, and updating the queue table locally after delete.
- Performance: Speed up Optimize result overview and delete refreshes with cached result summaries, a batch delete API, and local result table updates after deletion.
- Fix: Prevent Optimize HSL red-threshold bounds from including zero while HSL is enabled, avoiding PB7 candidate crashes during HSL optimization.
- Fix: Sort Optimize bounds in a stable long/short strategy lifecycle order instead of using historical JSON key order.
