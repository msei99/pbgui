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

Click the PBCoinData card on the Services overview. Credentials are managed in **Pool**; non-secret schedules remain in **Settings**.

### CMC pool

Use **Pool -> Add Key** to add one or more CMC keys. Secrets are stored in the owner-only credential vault and are never returned by status APIs or reveal controls.

- **Imported / externally used** keys are allowed and participate in local fair selection.
- Mark **Shared quota** when the same provider quota is shared outside this PBGui entry.
- Cluster Sync distributes sealed generations; do not put a CMC key in `pbgui.ini` or configure one per VPS.
- Leases coordinate usage when available, but are best effort. If leasing is unavailable, each node falls back to its local soft budget.
- A provider `429` cools down that key and the request can fail over to another eligible key. Invalid, disabled, exhausted, cooling, or conflicted keys are skipped.
- **Rotate** is optional and replaces the selected key with a new immutable generation. **Disable** keeps its history; **Delete** publishes a tombstone.

PBCoinData can run without a ready pool to refresh exchange-side mapping inputs, but CMC listings and metadata fetches are skipped until at least one active key is materialized.

### Scheduling

| Setting | Default | Description |
|---|---|---|
| `Fetch Interval` | `24` | How often CMC listings are re-fetched (hours) |
| `Fetch Limit` | `5000` | Max symbols fetched per CMC call |
| `Metadata Interval` | `1` | CMC metadata refresh (days) |
| `Mapping Interval` | `24` | Exchange mapping rebuild interval (hours) |

Free Basic plans are sufficient for most setups. The status bar and Pool tab show readiness, active-key count, health, generations, local usage, provider remaining credits when reported, cooldowns, failures, and secret-free lease statistics.

## PBCoinData detail panel

Click the PBCoinData card on the Services overview (or use the sidebar) to open the detail panel:

- The control strip shows the current status (running/stopped) and Start/Stop/Restart buttons
- The **Log** tab shows a live filtered PBCoinData log viewer
- The **Pool** tab manages CMC credentials and secret-free pool/lease status
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

- **No CMC data yet**: Confirm PBCoinData is running and **Services -> PBCoinData -> Pool** reports at least one active materialized key
- **Mapping stale**: Check `data/logs/PBCoinData.log` for repeated `ERROR` or `self-heal` entries
- **CMC rate-limit errors (429)**: The affected key enters cooldown and the pool tries another eligible key; increase `fetch_interval` if every key remains limited
- **Ignored/approved lists not updating in PBRun**: Verify mapping files exist under `data/coindata/{exchange}/` and restart PBCoinData once
