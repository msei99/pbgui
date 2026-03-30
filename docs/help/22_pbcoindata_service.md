# PBCoinData Service

PBCoinData is a background service that fetches CoinMarketCap (CMC) listings and metadata and builds exchange symbol mappings. These mappings power the dynamic coin filter logic used by PBRun (`ignored_coins.json` / `approved_coins.json`).

## What PBCoinData does

PBCoinData runs a daemon loop (60-second cycle) that:

- Fetches CMC listings (rank, market cap, volume, tags) on a configurable schedule
- Fetches CMC metadata — primarily the `notice` field (delisting/migration warnings used by dynamic filters)
- Builds per-exchange symbol mappings (`data/coindata/{exchange}/mapping.json`) for all V7-supported exchanges (binance, bybit, bitget, gateio, hyperliquid, okx)
- Detects copy-trading symbols per exchange (bybit: from CCXT market data; binance/bitget: via authenticated API with automatic user discovery)
- Resolves duplicate CMC symbols (e.g. HOT, ACT) via price-based disambiguation using exchange ticker prices
- Runs TradFi sync for Hyperliquid HIP-3 stock-perp symbols (master-only for web scraping, all nodes for spec sync)
- Runs a self-heal cycle that automatically retries exchanges with failed mappings (exponential backoff)
- Writes service logs to `data/logs/PBCoinData.log`

## Configuration

All PBCoinData settings are configured in the **Settings** tab of the PBCoinData detail panel.
Click the PBCoinData card on the Services overview, then switch to the **Settings** tab.

| Setting | Default | Description |
|---|---|---|
| `CoinMarketCap API_Key` | *(empty)* | CoinMarketCap API key (required for CMC fetches) |
| `Fetch Interval` | `24` | How often CMC listings are re-fetched (hours) |
| `Fetch Limit` | `5000` | Max symbols fetched per CMC call |
| `Metadata Interval` | `1` | CMC metadata refresh (days) |
| `Mapping Interval` | `24` | Exchange mapping rebuild interval (hours) |

A CMC API key is required. Free Basic plans are sufficient for most setups.
After entering a valid API key, the status bar above the tabs shows your API credit status (monthly limit, usage, remaining credits).

## PBCoinData detail panel

Click the PBCoinData card on the Services overview (or use the sidebar) to open the detail panel:

- The control strip shows the current status (running/stopped) and Start/Stop/Restart buttons
- The **Log** tab shows a live filtered PBCoinData log viewer
- The **Settings** tab provides the configuration form described above

## Self-heal cycle

If a mapping build fails for an exchange (e.g. due to a temporary network error), PBCoinData automatically retries that exchange in the next cycle with exponential backoff. The log shows `[self-heal]` entries for these retries.

## Data files

| Path | Description |
|---|---|
| `data/coindata/coindata.json` | CMC listings snapshot |
| `data/coindata/metadata.json` | CMC metadata snapshot |
| `data/coindata/{exchange}/mapping.json` | Exchange symbol → CMC coin mapping |
| `data/coindata/{exchange}/ccxt_markets.json` | Raw CCXT market snapshot |
| `data/logs/PBCoinData.log` | Service log |

## Troubleshooting

- **No mapping built yet**: Confirm PBCoinData is running and a valid CMC API key is set in the PBCoinData **Settings** tab
- **Mapping stale**: Check `data/logs/PBCoinData.log` for repeated `ERROR` or `self-heal` entries
- **CMC rate-limit errors (429)**: PBCoinData retries automatically; increase `fetch_interval` if persistent
- **Ignored/approved lists not updating in PBRun**: Verify mapping files exist under `data/coindata/{exchange}/` and restart PBCoinData once
