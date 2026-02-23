# API-Keys (PBGui / PB7)

PBGui supports both exchange API credentials and TradFi provider credentials used for stock-perp backtesting.

## Where credentials are used

- **Setup → API-Keys** edits exchange users in `api-keys.json`.
- PB7 live trading reads these exchange users from `api-keys.json`.
- TradFi provider config is saved in `pbgui.ini` (`[tradfi_profiles]`) and in PBGui users config (`users.tradfi`).

## Exchange users (`api-keys.json`)

Each user is a JSON object keyed by username:

```json
{
  "myuser": {
    "exchange": "bybit",
    "key": "...",
    "secret": "...",
    "passphrase": "..."
  }
}
```

### Recognized fields

- Required
  - `exchange`

- Credentials
  - `key` (aliases accepted on load: `apiKey`, `api_key`)
  - `secret`
  - `passphrase` (alias accepted on load: `password`)

- Hyperliquid-specific
  - `wallet_address` (aliases accepted on load: `walletAddress`, `wallet`)
  - `private_key` (alias accepted on load: `privateKey`)
  - `is_vault` (boolean)

- Optional PB7/CCXT passthrough
  - `quote` (string)
  - `options` (JSON object)
  - `extra` (JSON object passthrough for exchange-specific values)

Additional unknown keys are preserved so existing configs stay compatible.

## TradFi Data Provider (Stock Perps Backtesting)

The API-Keys page also has **TradFi Data Provider** settings:

- **yfinance**
  - Default source for the last 7 days.
  - No API key required.
  - Install/uninstall and test buttons are available.

- **Extended provider** (optional, for older history)
  - Providers: `alpaca`, `polygon`, `finnhub`, `alphavantage`
  - API key is required.
  - Includes **Test Connection**, **Save TradFi Config**, and **Clear TradFi Config**.

### TradFi runtime behavior (single source of truth)

- This page owns **credentials and provider setup**.
- Runtime market-data behavior (HIP-3 flow, source priority, and loop scope) is documented in the Market Data guide:
  - `docs/help/26_market_data.md` (EN)
  - `docs/help_de/26_market_data.md` (DE)

### Free provider coverage (quick reference)

Notes below are practical PBGui/PB7 guidance and can change with provider plans.

- `yfinance`
  - Free in PBGui, no key.
  - In PBGui workflow used as default for the most recent ~7 days.
- `alpaca`
  - Free API key available.
  - Recommended provider in the API-Keys UI.
  - Free tier can provide multi-year 1m history for this workflow.
- `polygon`
  - Coverage is plan-dependent.
  - Free plans can return `0 candles` for 1m intraday requests.
- `finnhub`
  - Free tier does not provide practical 1m intraday history for this workflow.
  - Not recommended for PBGui stock-perp backtesting.
- `alphavantage`
  - Free tier is heavily rate-limited (e.g. daily call caps).
  - Usually too limited for larger historical backfills.

### Provider matrix (free-tier oriented)

| Provider | API key needed | Practical 1m depth for PBGui/PB7 | Free-tier limits (practical) | Recommendation |
|---|---:|---|---|---|
| `yfinance` | No | Recent window (PBGui default workflow: last ~7 days) | No dedicated key, external source behavior may vary | Keep enabled for recent candles |
| `alpaca` | Yes (`key` + `secret`) | Multi-year 1m history (good practical coverage) | Requires market-data enabled account credentials | **Recommended extended provider** |
| `polygon` | Yes (`key`) | Plan-dependent for 1m intraday history | Free plan can be insufficient for backfills | Optional, verify your plan |
| `finnhub` | Yes (`key`) | Not practical for 1m backtest history | Free tier generally unsuitable for this workflow | Not recommended |
| `alphavantage` | Yes (`key`) | Often too shallow/slow for larger 1m ranges | Strong daily rate limits on free usage | Only for small ad-hoc checks |

### Recommendation

- Keep `yfinance` for the recent window (automatic in PBGui).
- Configure **`alpaca`** as your extended provider for stock-perp (HIP-3) 1m backtests.

### TradFi test behavior

- Test fetches `AAPL` 1m candles for the last 7 completed days.
- Success means candles were returned.
- `0 candles` can indicate plan/tier limits, not necessarily a technical error.

## Example (PB7/CCXT-style)

```json
{
  "myuser": {
    "exchange": "bybit",
    "apiKey": "...",
    "secret": "...",
    "password": "...",
    "quote": "USDT",
    "options": {"defaultType": "swap"},
    "uid": "123456"
  }
}
```

## Upstream reference

- https://github.com/enarjord/passivbot
