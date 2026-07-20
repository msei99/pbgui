# Balance Calculator

The shared Balance Calculator estimates the wallet balance required to run a PBv7 or PBv8 configuration from its approved coins, position count, wallet exposure limit, initial entry size, and exchange minimum order sizes.

## How to open it

Open the standalone page from:

- **Information → Balance Calculator**: load a PBv7 instance or paste a PBv7/PBv8 config
- **PBv7 → Run**: click the **$** action on an instance
- **PBv7 → Backtest**: open a config or select a result and click **Balance Calculator**
- **PBv8 → Backtest**: open a config or select a PBv8 result and click **Balance Calculator**

Both Backtest pages also offer **Calc Balance** for a quick inline calculation without leaving the page.

For PBv8, an exact `approved_coins` value of `all` is expanded from the selected exchange's local mapping. Only active linear swap markets with PB8's default quote are considered, and side-specific ignored coins are removed before calculation.

## Layout

| Area | Content |
|------|---------|
| Left column | Editable config JSON |
| Toolbar | Optional PBv7 instance, exchange selector, and Calculate button |
| Right column | Recommendation, per-side balances, and coin minimum-order information |

## Workflow

1. Open the calculator from Information, Run, or Backtest.
2. Load a PBv7 instance, follow a Backtest handoff, or paste a PBv7/PBv8 config.
3. Select the **Exchange** if multiple exchanges are configured.
4. Optionally edit the config JSON directly in the left text area.
5. Click **Calculate** to compute the balance requirements.

## Exchange selection

- Backtest and Run handoffs preselect their detected exchange.
- Direct navigation defaults to the current dropdown selection.
- You can change the exchange at any time using the **Exchange** dropdown.

## Editing the config

- The left text area shows the full config as JSON.
- Changes are applied when you click **Calculate**.
- Invalid JSON shows an error without submitting the calculation.

## Results

After clicking **Calculate**, the right column shows:

- Recommended wallet balance with a 10% buffer, rounded up to the next 10 USDT
- Required balance per long and short coin
- Coin price and minimum-order information used by the calculation

For PBv7, bot parameters are read from `bot.<side>`. For PBv8, position count and exposure are read from `bot.<side>.risk`, while initial entry size is read from `bot.<side>.strategy.<live.strategy_kind>.entry.initial_qty_pct`. PBv7 Dynamic Ignore remains supported. Both versions resolve market minimums through the local CoinData mapping.

## Troubleshooting

- **No result for one side**: verify that the side has approved coins, a positive position count and exposure limit, and a positive initial entry size.
- **CoinData not configured**: add or activate a CMC pool key under **System -> Services -> PBCoinData -> Pool** and wait for local materialization.
- **Unexpected PBv7 coin list**: if Dynamic Ignore is enabled, CoinData settings may filter the approved coins.
