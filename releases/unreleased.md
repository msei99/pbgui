# Unreleased

- Performance: Speed up Optimize config listing and delete refreshes by using cached lightweight config summaries and cached backtest counts instead of re-normalizing every config and re-scanning all backtests on every load.
