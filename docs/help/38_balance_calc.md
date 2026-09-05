# Balance Calculator

The shared Balance Calculator estimates the wallet balance required to run a PBv7 or PBv8 configuration from its approved coins, position count, wallet exposure limit, initial entry size, and exchange minimum order sizes.

## How to open it

Open the standalone page from:

- **Information → Balance Calculator**: load a PBv7 or PBv8 instance, or paste a PBv7/PBv8 config
- **PBv7 → Run**: click the **$** action on an instance
- **PBv7 → Backtest**: open a config or select a result and click **Balance Calculator**
- **PBv8 → Backtest**: open a config or select a PBv8 result and click **Balance Calculator**

Both Backtest pages also offer **Calc Balance** for a quick inline calculation without leaving the page.

Config handoff links contain a temporary draft ID. Drafts expire after 10 minutes and are lost when the API restarts; reopen the calculator from the editor if a draft has expired.

For PBv8, an exact `approved_coins` value of `all` is expanded from the selected exchange's local mapping. Only active linear swap markets with PB8's default quote are considered, and side-specific ignored coins are removed before calculation.

PB8 initial sizing follows the active strategy schema: trailing strategies use `strategy.entry.initial_qty_pct`, while EMA Anchor uses its root `strategy.base_qty_pct`. Inline Calc Balance actions and the standalone page therefore produce the same recommendation for PB8 Backtest results.

## Layout

| Area | Content |
|------|---------|
| Left column | Editable config JSON |
| Toolbar | Optional version-labelled PBv7/PBv8 instance, exchange selector, and Calculate button |
| Right column | Recommendation, per-side balances, and coin minimum-order information |

## Workflow

1. Open the calculator from Information, Run, or Backtest.
2. Load a version-labelled PBv7/PBv8 instance, follow a Backtest handoff, or paste a PBv7/PBv8 config.
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

Coin names and exchange symbols are resolved through the local mapping, including multiplier contracts. Distinct mapped markets such as `CAT` and `1000CAT` remain separate. Dominant coins and sides are selected using unrounded requirements; rounding the displayed values does not affect the recommendation.

The API also accepts `config_file` below `data/run_v7/` or `data/run_v8/` and uses the matching config loader. Paths outside these roots, traversal, and symlinks are rejected. The browser picker uses versioned instance names instead of filesystem paths. Reverse-proxy deployments must configure the correct ASGI `root_path`; page API and asset URLs retain that prefix.

- **No result for one side**: verify that the side has approved coins, a positive position count and exposure limit, and a positive initial entry size.
- **Invalid calculation parameters**: position count, exposure limit, and initial entry size must be finite non-negative numbers. Zero disables a side; NaN, Infinity, negative values, and numeric overflow/underflow are rejected instead of producing a misleading recommendation.
- **CoinData not configured**: add or activate a CMC pool key under **System -> Services -> PBCoinData -> Pool** and wait for local materialization.
- **Unexpected PBv7 coin list**: if Dynamic Ignore is enabled, CoinData settings may filter the approved coins.
