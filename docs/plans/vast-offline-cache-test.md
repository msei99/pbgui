# PB8 offline cache experiment — 2026-09-13

## Result

The existing, unmodified Vast worker image can prepare the affected ETH dataset
and execute a native Rust backtest without exchange requests when supplied with
the necessary public caches. A PB8 patch or exchange proxy was not required for
these tested paths. This is a preparation/backtest experiment, not a completed
GPU optimizer run or a deployed integration change.

## Isolation and version

- Image: `ghcr.io/msei99/pbgui-pb8-worker@sha256:0d827eb097a9d26c9088421097b4a9f0eacf660f08613871c48946ddf71a0a88`.
- Pinned PB8 revision: `ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8`.
- Docker used `--network none --read-only --tmpfs /tmp --user 1026:100`.
- Only an isolated `/tmp/pbgui-offline-cache-test` copy was mounted writable.
  No production directories, credentials or Docker socket were mounted.
- Input was copied from job `9149286d8519407bbc30bc3b5bf65726`:
  ETH, Binance and Bybit, requested dates 2019-12-23 through 2026-09-11.
- Public caches were copied with their original modification times. Only the
  stale-cache negative control changed timestamps, inside the temporary copy.
- A test-only replacement of CCXT's HTTP fetch boundary recorded requests and
  raised immediately. It did not return fake market/candle responses. Docker
  also disabled networking independently of this hook.
- The installed Rust extension was imported before PB8 modules to avoid the
  development rebuild check from the temporary working directory.

## Exercised path

The probe loaded the job config through PB8 `config_utils.load_config`, then ran
the optimizer's `format_approved_ignored_coins`, `build_optimizer_data_config`,
and `prepare_hlcvs_mss(..., 'combined', allow_internal_nan_gaps=True)` sequence.
It passed the prepared arrays and market settings to `backtest.run_backtest`.
It did not replace preparation, market resolution, array validation or Rust
evaluation with mocks.

| Input variant | Result |
| --- | --- |
| Original OHLCV input, no metadata caches | Blocked immediately on Binance/Bybit market HTTP requests. |
| OHLCV plus public market, symbol and first-timestamp caches | Full preparation and backtest succeeded; zero CCXT HTTP attempts. |
| Same bundle, `markets.json` timestamps aged 48 hours | Market HTTP requests attempted again. |
| OHLCV plus only the two fresh `markets.json` files | Initial market normalization succeeded; first-timestamp discovery attempted HTTP during preparation. |
| Prepared HLCV override plus full public cache bundle | Preparation and backtest succeeded; zero CCXT HTTP attempts. |
| Prepared HLCV override plus only the two fresh `markets.json` files | Preparation and backtest succeeded; zero CCXT HTTP attempts. Raw OHLCV source deliberately pointed at a nonexistent directory. |

All three successful variants produced `['ETH']`, HLCV shape
`(3484801, 1, 4)`, 1,225 fills and 55,717 equity rows. The effective dataset
started 2020-01-26 due to PB8's coin-age handling. Equal counts are not a claim
of a separate bitwise numerical equivalence test.

The full public bundle contained only these allowlisted files, where present:

- `caches/{binance,bybit}/{markets,coin_to_symbol_map,first_timestamps}.json`
- `caches/symbol_to_coin_map.json` and `symbol_to_coin_ambiguities.json`
- `caches/first_ohlcv_timestamps_unified.json`
- `caches/first_ohlcv_timestamps_unified_exchange_specific.json`
- `caches/first_ohlcv_timestamps_unified_exchange_specific_symbols.json`
- `caches/first_ohlcv_timestamps_unified.version`

The prepared-data variant used the complete PB8-generated dataset directory
with its manifest, coins, market-specific settings, HLCV array, BTC/USD prices
and timestamps via `backtest.hlcvs_data_dir`. Its manifest and coverage checks
were executed by PB8. The minimal successful variant added only
`caches/binance/markets.json` and `caches/bybit/markets.json`; PB8 regenerated
symbol mappings itself.

## Integration implications

The smallest tested route is to prepare and validate the dataset locally, ship
the complete manifest-backed dataset and fresh public market snapshots, and
set `backtest.hlcvs_data_dir` in the worker's execution config. Alternatively,
the existing raw OHLCV bundle works with the broader public cache bundle.

PB8 `utils.load_markets` has a 24-hour freshness window based on file mtime.
Expired market snapshots are not an offline guarantee. Refresh snapshots on
the local machine before dispatch and verify the complete bundle without
network access; do not merely retimestamp stale data as fresh. Missing candle
coverage, first-timestamp entries, different coins, or different PB8 revisions
require their own validation. This experiment does not modify the worker's
current packaging or certify every optimizer backend/strategy.

The temporary probe and six logs are retained under
`/tmp/pbgui-offline-cache-test/` (`probe.py`, `empty.log`, `fresh.log`,
`stale.log`, `markets_only.log`, `override.log`, `override_minimal.log`).
