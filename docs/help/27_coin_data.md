# Coin Data

Coin Data updates automatically when opened, when changing exchanges, on returning to the page, and while it remains open. There are no manual refresh controls. Cached snapshots never start exchange or CMC requests. Server-side checks start only missing or expired work using the configured mapping, CMC listings and metadata intervals, even if PBCoinData is stopped or idle.

Progress checks run sequentially. **Dismiss to background** or Escape hides only the busy overlay; the sidebar status and job monitoring remain active. Failed network requests reconnect with bounded backoff without replacing the job. Reload and returning to the page rediscover running jobs. Session expiry stops requests and displays an authentication message.

Current filters, focused drafts, selected symbols and sorting survive automatic updates; view, selection and sorting are restored on browser reload. Job results cannot restore obsolete filters. If a selected symbol disappears, the status explains that it is no longer available.

A missing mapping or rejected view does not stop background job monitoring. Saved quotes are retained while mapping data is unavailable. If a quote disappears, the page keeps any remaining valid quotes or explains the fallback to the exchange default. HIP-3 DEX options also update automatically: a valid selection stays selected; a removed DEX falls back to **All DEXes** with a status message.

Failed Coin Data loads display an error while cached data remains available; filters can be retried without reloading the page.

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

Browser requests use the HttpOnly PBGui session cookie; the session token is never rendered into the page or sent in a browser Bearer header. API and local asset paths retain the configured ASGI mount prefix. Help Markdown is sanitized, and links or images are limited to HTTP(S) URLs before content is displayed.

## Sidebar actions

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

## CMC readiness

Automatic CMC updates require at least one active credential generation in the local materialized CMC pool. Without a key, the sidebar reports the limitation; exchange updates and cached Coin Data remain available. Listings are published before metadata and exchange enrichment. Failed producers retry automatically with backoff; fresh data is reused under the same resource locks as daemon and API work. Add or inspect keys under **System -> Services -> PBCoinData -> Pool**. Do not add a key to `pbgui.ini`.

On VPS targets, readiness is secret-free capability metadata: protocol version, active-key count, and catalog/materialized generations. A node is ready only after the required sealed generation is materialized locally.

## Filters and table behavior

Main filters:

- Exchange
- Quotes via a compact multi-select. Coin Data defaults to `USDT` when that quote exists; Hyperliquid defaults to both `USDC` and `USDT0`. Any quote family present in the selected exchange mapping can be selected explicitly.
- Minimum `market_cap` updates while typing, keeps decimal input stable while editing, and uses `250` as the editor-style `+/-` step
- Maximum `vol/mcap` updates while typing, preserves direct decimal input such as `0.` and `0,`, and makes `+/-` jump across readable rounded thresholds derived from the current exchange data instead of tiny raw-value steps
- Tags via the same searchable chip-based multiselect used in PBv7 Run/Backtest, without checkboxes inside the dropdown
- `Reset` button at the right side of the filter row to restore the default filter state immediately

FastAPI UI improvements:

- sticky table headers in scrollable tables
- the HIP-3 table keeps its own scroll container on desktop so long symbol lists remain usable, with the dedicated `DEX` selector placed in the HIP-3 section header instead of the global filter row
- denser table rows and compact tag chips to reduce wasted vertical space
- full-width table layout with balanced column distribution, so the page uses the available width without oversized gaps between values
- active desktop table view expands to use the remaining window height instead of leaving empty space below the table
- sortable table headers for matched, unmatched, and HIP-3 views
- sorting updates only table content; selecting a row changes its highlight and detail card without rebuilding table rows, so open tag filters keep their focus and draft
- repeated clicks keep one row selected; closing the details with `X` or Escape clears all row highlights without rebuilding the tables or filter controls
- unavailable CMC market cap, 24-hour volume, and `vol/mcap` values are shown as `N/A` and sort after known values
- hover tooltips for tags, notices, and long values
- row selection with a centered floating detail panel that auto-fits its content on open when the browser window allows it, can be dragged and resized from every side and corner, shows all tags without truncation, offers a direct `Open CMC` link when a mapping exists, and uses an `X` close button instead of only showing the notice below the table
- a single active main table view, switched from the sidebar, instead of showing matched and auxiliary tables at the same time
- a single-line desktop filter bar without a separate `Filters` title block
- long-running exchange and CMC refreshes can be dismissed to the background with the button or Escape; polling continues and applies the completed state

The page contains:

- **Matched symbols** table: matched non-HIP-3 rows after filters
- **CMC unmatched** table: symbols not currently matched to CMC
- **HIP-3 symbols** table (Hyperliquid only)
- The `HIP-3 Symbols` sidebar button is hidden for all non-Hyperliquid exchanges.
- The `Only Copy Trading` sidebar button is hidden on exchanges without supported copy-trading detection.

## Hyperliquid notes

- Quote preference defaults to `USDC`, then `USDT0`
- The default includes both quote families when both are available; selecting either quote explicitly filters matched, unmatched, and HIP-3 rows. This Coin Data view selection does not change the USDT-linear rules used by PB7/PB8 configurations.
- Missing or expired Hyperliquid mappings are updated automatically; loading a cached snapshot never forces a HIP-3 rebuild
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

- Wait for the automatic update and inspect the sidebar status
- Relax filters temporarily (`market_cap=0`, higher `vol/mcap`, no tags)
- Check the CMC and mapping timestamps
- Disable `Only Copy Trading` if it is active

### Price is missing for some symbols

- Read the automatic update status; incomplete price coverage is retried with backoff
- Verify that the exchange markets timestamp is current
- For Hyperliquid, keep in mind that some symbols rely on market-info fallback pricing

### CMC unmatched count is high

- Inspect the automatic CMC update status and the data timestamps
- Check whether symbols are newly listed or use exchange-specific naming variants
- If CMC updates are unavailable, confirm the PBCoinData Pool has an active local key; updates resume automatically

The selected coin's detail card opens at its fitted size during the same selection, including in long lists.

Reset Filters also clears the HIP-3 DEX selector. Escape closes the selected coin details card. The status line remains visible during background work and reports partial results or errors.
