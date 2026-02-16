# Coin Data

The Coin Data page manages the mapping between exchange symbols and CoinMarketCap metadata, and shows filterable symbol quality and trading constraints.

## What this page does

- Builds and refreshes exchange-specific symbol mappings
- Merges CoinMarketCap data (rank, market cap, tags, metadata)
- Updates live prices and derived `vol/mcap`
- Shows copy-trading availability and exchange limits (min amount/cost, leverage)

## Refresh controls (sidebar)

- `:material/sync:` Refresh selected exchange
  - Fetches markets, updates copy-trading cache, rebuilds mapping, refreshes prices
- `:material/sync_alt:` Refresh all exchanges
  - Runs the same workflow for all V7 exchanges with progress status
- `:material/cloud_sync:` Refresh CoinMarketCap data
  - Reloads listings and metadata, then refreshes the selected exchange

## Last refreshed status

At the top, Coin Data shows timestamps for:

- CMC listings and metadata files
- Selected exchange market snapshot (`ccxt_markets.json`)
- Mapping file (`mapping.json`)
- Latest observed price timestamp from mapping rows
- Copy-trading cache file

Use these timestamps to verify data freshness before applying filters.

## Filters and tables

Top-row filters:

- Exchange
- Minimum `market_cap`
- Maximum `vol/mcap`
- Tags

The page contains:

- **CMC unmatched** expander: symbols not currently matched to CMC
- **Main table**: matched non-HIP-3 symbols after filters
- **HIP-3 symbols** expander (Hyperliquid only)

Click a table row to display full notice text below the table (if available).

## Hyperliquid notes

- Quote preference defaults to `USDC`, then `USDT0`
- If no HIP-3 symbols are found, Coin Data can auto-rebuild Hyperliquid mapping once
- If HIP-3 exists but is hidden, reduce filters (e.g. set `market_cap` to `0` and clear tags)

## Data files

Coin Data reads/writes under:

- `data/coindata/coindata.json`
- `data/coindata/metadata.json`
- `data/coindata/<exchange>/ccxt_markets.json`
- `data/coindata/<exchange>/mapping.json`
- `data/coindata/<exchange>/copy_trading.json`

## Troubleshooting

### No rows shown

- Refresh selected exchange
- Temporarily relax filters (`market_cap=0`, high `vol/mcap`, no tags)
- Check CMC timestamps and mapping timestamp

### Price is missing for some symbols

- Refresh selected exchange again
- Verify exchange markets file timestamp is recent
- For Hyperliquid, keep in mind some symbols may rely on market-info fallback pricing

### CMC unmatched count is high

- Refresh CMC data first, then refresh selected exchange
- Check if symbols are newly listed or use exchange-specific naming variants
