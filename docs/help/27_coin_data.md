# Coin Data

The Coin Data page is now available in the FastAPI UI shell and focuses on symbol mapping quality, CoinMarketCap coverage, and exchange trading constraints without changing the underlying CoinData service logic.

## What this page does

- Builds and refreshes exchange-specific symbol mappings
- Merges CoinMarketCap data such as rank, market cap, tags, and metadata
- Updates live prices and derived `vol/mcap`
- Shows copy-trading availability together with exchange limits such as min amount, min cost, precision, and leverage

## FastAPI layout

The page uses the normal FastAPI shell:

- shared top navigation and About dialog
- left sidebar for actions and view toggles, with adjustable width on desktop
- main content area with the filter row at the top and exactly one active table view below it
- table section headers styled like the existing FastAPI Backtest and Run pages

The Guide button in the FastAPI header opens the shared Help system.

## Sidebar actions

- `Refresh Selected Exchange`
  - Fetches markets, updates the copy-trading cache, rebuilds mapping, and refreshes prices for the current exchange
- `Refresh All Exchanges`
  - Runs the same workflow for all V7 exchanges
- `Refresh CMC + Selected Exchange`
  - Reloads CMC listings and metadata, then refreshes the selected exchange so the visible table immediately uses the new CMC data
  - Shows a centered busy overlay with real percentage progress based on the completed refresh steps while the existing workflow is running
- `Refresh CMC + All Exchanges`
  - Reloads CMC listings and metadata, then rebuilds all exchanges so every exchange mapping is aligned with the new CMC data in one run
  - Uses the same real-percentage busy overlay across the longer full rebuild workflow
- `Matched Symbols`
  - Shows the matched main result table
- `CMC Unmatched`
  - Shows only the unmatched CMC symbols table
- `HIP-3 Symbols`
  - Shows only the Hyperliquid HIP-3 table and is only shown for the `hyperliquid` exchange
- `Only Copy Trading`
  - Limits the main table to copy-trading symbols and is only shown for exchanges with a supported copy-trading filter (`bybit`, `binance`, `bitget`)

## Freshness info

Coin Data shows freshness information as one inline status next to `Filtered symbols`.

- The visible text is a compact summary for both the selected exchange refresh and the latest CMC refresh.
- Hovering the inline status shows the detailed markets, mapping, prices, copy-trading cache, listings, and metadata timestamps.

## Filters and table behavior

Main filters:

- Exchange
- Minimum `market_cap` updates while typing, keeps decimal input stable while editing, and uses `250` as the editor-style `+/-` step
- Maximum `vol/mcap` updates while typing, preserves direct decimal input such as `0.` and `0,`, and makes `+/-` jump across readable rounded thresholds derived from the current exchange data instead of tiny raw-value steps
- Tags via the same searchable chip-based multiselect used in PBv7 Run/Backtest, without checkboxes inside the dropdown
- `Reset` button at the right side of the filter row to restore the default filter state

FastAPI UI improvements:

- sticky table headers in scrollable tables
- the HIP-3 table keeps its own scroll container on desktop so long symbol lists remain usable, with the dedicated `DEX` selector placed in the HIP-3 section header instead of the global filter row
- denser table rows and compact tag chips to reduce wasted vertical space
- full-width table layout with balanced column distribution, so the page uses the available width without oversized gaps between values
- active desktop table view expands to use the remaining window height instead of leaving empty space below the table
- sortable table headers for matched, unmatched, and HIP-3 views
- hover tooltips for tags, notices, and long values
- row selection with a centered floating detail panel that auto-fits its content on open when the browser window allows it, can be dragged and resized from every side and corner, shows all tags without truncation, offers a direct `Open CMC` link when a mapping exists, and uses an `X` close button instead of only showing the notice below the table
- a single active main table view, switched from the sidebar, instead of showing matched and auxiliary tables at the same time
- a single-line desktop filter bar without a separate `Filters` title block

The page contains:

- **Matched symbols** table: matched non-HIP-3 rows after filters
- **CMC unmatched** table: symbols not currently matched to CMC
- **HIP-3 symbols** table (Hyperliquid only)
- The `HIP-3 Symbols` sidebar button is hidden for all non-Hyperliquid exchanges.
- The `Only Copy Trading` sidebar button is hidden on exchanges without supported copy-trading detection.

## Hyperliquid notes

- Quote preference defaults to `USDC`, then `USDT0`
- If no HIP-3 symbols are found, Coin Data can auto-rebuild Hyperliquid mapping once
- HIP-3 rows are shown separately and use the dedicated `DEX` selector; CMC-based filters such as `market_cap`, `vol/mcap`, and tags apply to matched non-HIP-3 rows

## Data files

Coin Data reads and writes under:

- `data/coindata/coindata.json`
- `data/coindata/metadata.json`
- `data/coindata/<exchange>/ccxt_markets.json`
- `data/coindata/<exchange>/mapping.json`
- `data/coindata/<exchange>/copy_trading.json`

## Troubleshooting

### No rows shown

- Refresh the selected exchange
- Relax filters temporarily (`market_cap=0`, higher `vol/mcap`, no tags)
- Check the CMC and mapping timestamps
- Disable `Only Copy Trading` if it is active

### Price is missing for some symbols

- Refresh the selected exchange again
- Verify that the exchange markets timestamp is current
- For Hyperliquid, keep in mind that some symbols rely on market-info fallback pricing

### CMC unmatched count is high

- Refresh CMC data first, then refresh the selected exchange
- Check whether symbols are newly listed or use exchange-specific naming variants
