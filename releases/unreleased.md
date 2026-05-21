# Unreleased

- Fix dashboard position `Next DCA` and `Next TP` values for short positions by classifying open buy/sell orders with the correct side-aware logic and nearest-price selection in both snapshot and live API paths.
- Fix the dashboard orders chart entry-line profit/loss color for short positions so it no longer uses long-only price-vs-entry comparisons.
- Add regression tests covering long/short dashboard order classification so nearest DCA/TP price selection stays correct for both snapshot and live API helpers.
- Make the dashboard Orders widget hedge-aware without a DB migration by sending the selected position side through `/ws/candles` and `/dashboard/orders_data`, showing `Orders: unknown` for hedged DB snapshots, and replacing that placeholder once live exchange order metadata can identify the correct leg.
