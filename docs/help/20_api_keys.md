# API-Keys (PBGui / PB7)

PBGui supports both its own `api-keys.json` format and PB7/CCXT-style credential field names.

## Where it is used

- PBGui reads and writes `api-keys.json` via the **Setup → API-Keys** editor.
- PB7 reads `api-keys.json` for live trading.

## User entry format

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

- Hyperliquid
  - `wallet_address` (aliases accepted on load: `walletAddress`, `wallet`)
  - `private_key` (alias accepted on load: `privateKey`)
  - `is_vault` (boolean)

- Optional PB7/CCXT passthrough
  - `quote` (string)
  - `options` (JSON object)

- Any additional exchange/bot specific keys are preserved by PBGui (so configs remain compatible).

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
