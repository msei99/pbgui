# PBv7 Balance Calculator

The Balance Calculator estimates the wallet balance required to run a PBv7 configuration based on its approved coins, position sizes, and leverage settings.

## How to open it

The Balance Calculator requires exchange context — open it from:

- **PBv7 → Run**: click the **Balance Calculator** button on an instance
- **PBv7 → Backtest**: click **Balance Calculator** after selecting a config

Opening it directly from the navigation without context will show an error.

## Layout

| Area | Content |
|------|---------|
| Left column | Editable config JSON |
| Right column | Exchange selector, Calculate button, results |

## Workflow

1. Open Balance Calculator from Run or Backtest.
2. The config is loaded automatically from the selected instance or backtest config.
3. Select the **Exchange** if multiple exchanges are configured.
4. Optionally edit the config JSON directly in the left text area.
5. Click **Calculate** to compute the balance requirements.

## Exchange selection

- If the config targets a single exchange, it is set automatically.
- If multiple exchanges are present, a dialog asks you to choose one.
- You can change the exchange at any time using the **Exchange** dropdown.

## Editing the config

- The left text area shows the full config as JSON.
- Changes are applied when you click **Calculate**.
- Invalid JSON shows an error popup — the previous valid config is restored.

## Results

After clicking **Calculate**, the right column shows:

- Required balance for long positions
- Required balance for short positions
- Total estimated balance requirement

The calculation uses the coin list from `approved_coins` in the config, filtered through CoinData (market cap, volume, etc.) when dynamic ignore is enabled.

## Troubleshooting

- **"Missing exchange context"**: do not open Balance Calculator directly from the navigation — use the button in RunV7 or BacktestV7.
- **CoinData not configured**: configure your CoinMarketCap API key in **System → API Keys**.
- **Unexpected coin list**: if dynamic ignore is enabled, the coin list is filtered by your CoinData settings (market cap, volume, tags).
