# Unreleased

- Performance: Speed up Optimize config listing and delete refreshes by using cached lightweight config summaries and cached backtest counts instead of re-normalizing every config and re-scanning all backtests on every load.
- Fix: Prevent Optimize HSL red-threshold bounds from including zero while HSL is enabled, avoiding PB7 candidate crashes during HSL optimization.
