# PBCoinData Service

PBCoinData is a background service that fetches CoinMarketCap (CMC) listings and metadata and builds exchange symbol mappings. These mappings power the dynamic coin filter logic used by PBRun (`ignored_coins.json` / `approved_coins.json`).

## What PBCoinData does

- Fetches CMC listings (rank, market cap, tags) on a configurable schedule
- Fetches CMC metadata (descriptions, categories)
- Builds per-exchange symbol mappings (`data/coindata/{exchange}/mapping.json`)
- Runs a self-heal cycle that automatically retries exchanges with failed mappings
- Writes service logs to `data/logs/PBCoinData.log`

## Configuration (`pbgui.ini`)

PBCoinData reads from the `[coinmarketcap]` section in `pbgui.ini`:

| Setting | Default | Description |
|---|---|---|
| `api_key` | *(empty)* | CoinMarketCap API key (required for CMC fetches) |
| `fetch_interval` | `24` | How often CMC listings are re-fetched (hours) |
| `fetch_limit` | `5000` | Max symbols fetched per CMC call |
| `metadata_interval` | `1` | CMC metadata refresh (days) |
| `mapping_interval` | `24` | Exchange mapping rebuild interval (hours) |

A CMC API key is required. Free Basic plans are sufficient for most setups.

## PBCoinData Details page

On `System → Services → PBCoinData → Show Details` you can:

- Check current PBCoinData service status (running/stopped)
- Toggle the service on/off
- Use the integrated filtered PBCoinData log viewer in the details section

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

- **No mapping built yet**: Confirm PBCoinData is running and a valid CMC API key is set in `pbgui.ini`
- **Mapping stale**: Check `data/logs/PBCoinData.log` for repeated `ERROR` or `self-heal` entries
- **CMC rate-limit errors (429)**: PBCoinData retries automatically; increase `fetch_interval` if persistent
- **Ignored/approved lists not updating in PBRun**: Verify mapping files exist under `data/coindata/{exchange}/` and restart PBCoinData once
