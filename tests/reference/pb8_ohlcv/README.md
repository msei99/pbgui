# Pinned PB8 OHLCV decisions

Unmodified function/method excerpts from
`enarjord/passivbot@b9d6692ea41266ecb4ab81d40bb5e5baef29b50a`
(the Vast worker pin). Upstream: https://github.com/enarjord/passivbot/tree/b9d6692ea41266ecb4ab81d40bb5e5baef29b50a/src

- `source_loader.py.txt`: `hlcv_preparation.py`, `_try_load_ohlcvs_from_source_dir` and `get_ohlcvs` (from line 819).
- `preparation.py.txt`: `hlcv_preparation.py`, `prepare_hlcvs` (from line 1223).
- `ohlcv_utils.py.txt`: `ohlcv_utils.py`, original reader, normalization and deduplication (lines 1–180).
- `days.py.txt`: `ohlcv_utils.py`, `get_days_in_between` (from line 183).
- `dates.py.txt`: `utils.py`, date conversion and normalization (from `ts_to_date` through `utc_ms`).
- `dataset_windows.py.txt`: `suite_runner.py`, `_scenario_date_window` through `_config_for_dataset_coins` (from line 574).
- `coin_window.py.txt`: `hlcv_preparation.py`, `_plan_combined_coin` (including minimum age and stock-perp handling).
- `positive_timestamp.py.txt`: `hlcv_preparation.py`, `_coerce_positive_ts` (from line 2656).
- `direct_coin_window.py.txt`: `hlcv_preparation.py`, `prepare_hlcvs_internal` (from line 4271, including minimum-age skips and empty-payload rejection).

The source bytes are checked with SHA256 in `tests/pb8_ohlcv_reference.py`.
That adapter compiles original function bodies, replacing imports, metadata I/O,
tracing and post-admission gap filling only. It does not replace gap, format,
listing, date, or range decisions. Original NPY reads run on disposable copies
because upstream can rewrite duplicate source rows. No installed PB8 checkout
or network access is used by these tests. See `LICENSE` for the upstream
public-domain dedication (Unlicense).
