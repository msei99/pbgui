# Transfers

## Purpose

**System > Transfers** provides explicit manual transfers between fixed internal accounts. It never accepts an external address and does not change Profit Sweep due amounts, baselines, high-water marks, or confirmed sweep totals.

Supported route pairs are derived from each fresh account snapshot:

- Hyperliquid Standard: Perps to Spot and Spot to Perps.
- Hyperliquid Vault: Vault to Leader Main Perps, Leader Main Perps to Vault, and, in Standard/Manual leader mode, Leader Main Perps to Main Spot plus Main Spot to Main Perps.
- Bybit: Unified to Funding and Funding to Unified.
- Binance: USD-M Futures to Funding and Funding to USD-M Futures.
- Bitget Classic: USDT Futures to Spot and Spot to USDT Futures.
- Bitget UTA: UTA to Spot and Spot to UTA.

## Internal Transfer

1. Select a supported exchange account in the sidebar.
2. Select one of the server-advertised fixed routes.
3. Review the fresh source balance, actual transferable amount, destination balance, asset, and route minimum.
4. Enter an amount without exceeding **Available to transfer**.
5. Click **Review transfer**.
6. Confirm the exact account, route, amount, source, and destination in the PBGui dialog.

PBGui resolves every source and destination from the selected configured account. The route selector contains only server-derived allowlisted routes; addresses and assets are not freely editable. A Profit Sweep policy is not required.

The **Direction** selector sits directly beside **Amount** and **Review transfer**. If a Hyperliquid Vault leader uses Unified or Portfolio Margin, Hyperliquid exposes Main Perps and Main Spot as one shared **Main Unified** balance. PBGui then correctly omits Main-to-Spot directions and shows that explanation next to the transfer controls; no internal transfer is needed between those merged accounts.

For Vault accounts, the preview distinguishes **Your Vault Equity** from the complete **Vault Account Value** and Hyperliquid's user-specific **Your Max Withdrawable** value. It also lists every sanitized open Vault position with coin, side, size, position value, entry price, unrealized PnL, liquidation price, and leverage type. A transfer does not modify or close positions directly, but moving collateral can affect Passivbot wallet-exposure-based sizing, available margin, and later order sizes.

## Operation Safety

Each transfer receives a browser-generated idempotency UUID and is persisted before exchange I/O. The account operation lock prevents it from racing another Profit Sweep or manual transfer operation. An unresolved Profit Sweep intent or manual transfer blocks a new transfer.

PBGui submits each operation at most once and performs bounded ledger reconciliation. A lost browser response may be retried only with the same retained operation ID, route, and amount. **Unknown** operations expose **Reconcile**, which only queries exchange history and never resubmits the transfer. Submitting operations block API restart; startup reconciles submitted operations and marks an interrupted pre-submission transfer as failed without sending it.

Some exchange-history rows do not include PBGui's operation UUID. PBGui therefore prevents its own equal route-and-amount operations inside the same ten-minute matching window. Do not initiate an equal manual transfer through another client during a PBGui operation; an externally created identical row may not be distinguishable reliably.

Transfer history is separate from Profit Sweep Live intents and Test Transfers. It shows route, requested and received amounts, timestamps, status, and bounded error or reconciliation reasons without exposing addresses, descriptors, signatures, credentials, or raw provider responses.

## Profit Sweep Handoff

For a Hyperliquid Vault, **Profit Sweep > Exchange / Vault > Fund account** opens Transfers with that exact account and **Main Perps to Vault** selected. Reconcile an existing `PAUSED_UNKNOWN` Profit Sweep intent before moving funds.

## Troubleshooting

- **Transfer blocked:** reconcile the unresolved Profit Sweep or manual transfer first.
- **Insufficient balance:** reduce the amount below the selected route's fresh **Available to transfer** value.
- **Minimum rejected:** follow the displayed route minimum; Hyperliquid Vault deposits require at least `5 USDC`.
- **Unknown:** do not create another identical transfer. Use **Reconcile** for the existing operation.
