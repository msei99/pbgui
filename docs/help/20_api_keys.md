# API Keys

Manage exchange API credentials and TradFi provider profiles. Exchange users remain in `api-keys.json`; TradFi secrets are stored separately in PBGui's owner-only credential vault.

---

## Page layout

The page runs as a standalone FastAPI page with a full topnav for navigating to all PBGui sections. It consists of a **sidebar** (left) and a **main panel** (right).

### Sidebar buttons

| Button | Action |
|---|---|
| **+ Add User** | Opens the create form for a new exchange user |
| **HL Expiry Check** | Bulk-checks Hyperliquid key expiry for all HL users |
| **Bybit Expiry Check** | Bulk-checks Bybit API key expiry + IP whitelist for all Bybit users |
| **Comments** | Opens the comment management panel |
| **HL Warning Config** | Configures the Hyperliquid expiry Telegram warning threshold |
| **TradFi** | Opens the TradFi Data Provider panel |
| **🗄 Backups** | Opens the backup browser and diff viewer |
| **📋 Logs** | Opens the live log viewer (streams `ApiKeys.log` and other logs) |
| **Refresh** | Reloads the user list from disk |
| **🟠 Restart** | Visible when the API server has pending code changes; click to restart |

---

## User list

Displays all entries from `api-keys.json`.

- **Filter box** — type to search by name or exchange; state is preserved in the URL (`?filter=`)
- **Column headers** — click to sort; sort direction persists in the URL (`?sort=`, `?dir=`)
- **Keyboard navigation** — ArrowDown from the filter box selects the first row; ArrowUp/ArrowDown move between rows; Enter opens the selected user
- **In Use badge** — shown when the user is referenced by a live bot

Usernames are rendered strictly as text and row actions use delegated browser events. Names imported from backups or Cluster Sync therefore cannot be interpreted as page markup or JavaScript; row clicks, keyboard navigation, Edit, and Delete behave as before.

### Expiry columns

- **HL Expiry** — shows days remaining / expiry date for Hyperliquid users (read from local cache, no API call); sortable ascending (soonest expiry first)
- **Bybit Expiry** — shows days remaining for Bybit users (read from local cache)

---

## Create / Edit a user

Click a user row to open, or use **+ Add User**. The URL hash updates to `#edit/username` so a browser refresh reopens the same user.

Press **Escape** to close without saving (confirms if there are unsaved changes).

### Edit form fields

| Field | Description |
|---|---|
| **Username** | Key in `api-keys.json`; can be renamed — type a new name and save |
| **Exchange** | Exchange name (e.g. `bybit`, `binanceusdm`, `hyperliquid`) |
| **API Key** | Exchange API key |
| **Secret** | API secret |
| **Passphrase** | Required by some exchanges (e.g. OKX) |
| **Wallet Address** | Hyperliquid only |
| **Private Key** | Hyperliquid only |
| **Is Vault** | Hyperliquid vault mode |
| **Quote** | Optional CCXT passthrough (e.g. `USDT`) |
| **Options** | Optional JSON object (e.g. `{"defaultType": "swap"}`) |
| **Extra** | Optional JSON passthrough for exchange-specific fields |

### Eye-toggle (exchange credentials)

Stored exchange fields (Secret, Passphrase, Private Key) have an 👁 button:

- **Click** — fetches the real stored value from the server and shows it in plain text
- **Click again** — hides and clears the field (saving with an empty field keeps the stored value unchanged)
- To replace a credential, reveal it, clear it, type the new value, and save

TradFi vault secrets are different: stored values are never returned to the browser. Their eye buttons can only show text entered during the current edit. Leave a field empty to keep the stored value, or enter a replacement and save.

### Validation

- Standard exchanges require **API Key + Secret**
- Passphrase exchanges additionally require **Passphrase**
- Hyperliquid requires **Wallet Address**; Private Key required only on creation (leave blank on edit to keep existing)
- Username must be unique; rename is rejected if the new name is already in use or the user is in use by a bot

### Check Expiry / Test Connection

Both buttons use the **currently typed credentials** from the form — not just the saved ones. This lets you verify a new key before committing to Save.

- **Check Expiry** (HL / Bybit) — result is preview-only; not persisted until you click Save
- **Test Connection** — tests the connection live; also uses unsaved credentials

Unsaved Hyperliquid private keys used by **Check Expiry** are sent only in an authenticated POST request body. They are never added to the request URL; checks without an unsaved override continue to use the stored key.

---

## Backups

A backup is created automatically before every save. Backups are stored in `data/api-keys/` as timestamped JSON files.

Open via **🗄 Backups** in the sidebar (URL hash: `#backups`).

| Entry | Description |
|---|---|
| **Current (live)** | The active `api-keys.json` for each PB version (pb7/pb6); selectable for diff comparison |
| Timestamped entries | Previous saves; **Restore** overwrites the current file (pre-restore snapshot created first) |

### Diff viewer

Compare any two entries side-by-side or unified:
- Green = added, red = removed, grey = unchanged context
- "✓ Files are identical" shown when both versions match

---

## Cluster Sync

Exchange users are projected into the local `api-keys.json`. Remote exchange API-key writes are owned by **Cluster Sync**.

When you save exchange credentials, PBGui records the updated API-key metadata and restricted secret blob in cluster state. Use **System -> Cluster Sync** to preview and explicitly materialize `api-keys.json` on a reachable node.

Cluster materialization creates replacement backups only on master nodes when the target file differs. These backups are stored with the normal API-key backups in `data/api-keys/`. VPS runner nodes skip local backups, write the verified secret blob atomically, and do not restart bots or deploy any other files.

TradFi profiles use credential protocol v2 sealed envelopes instead. They are addressed only to active masters; VPS nodes may relay the ciphertext but cannot decrypt or project TradFi credentials.

---

## HL Warning Config

Open via **HL Warning Config** in the sidebar.

- If `hl_expiry.telegram_warning_days` is already present in `pbgui.ini`, the panel shows it as **configured**.
- If the INI entry is still missing, the panel now shows **Not configured** and makes it explicit that PBAPIServer currently falls back to the default **7-day** warning window.
- Clicking **Save** writes the chosen threshold to `pbgui.ini` and switches the panel state to configured.

---

## Live Log Viewer

Open via **📋 Logs** in the sidebar.

Streams log files in real time via WebSocket.

### Controls

| Control | Description |
|---|---|
| **Files** button / sidebar | Toggle the collapsible left sidebar listing all available log files; click a file to switch |
| **DBG / INF / WRN / ERR / CRT** | Toggle visibility by log level |
| **Lines** | Number of initial lines to load (200 – 5000) |
| **⏸ Pause / ▶ Stream** | Pause or resume live streaming |
| **🗑 Clear** | Clears the terminal view |
| **↓ Download** | Downloads the currently loaded lines as a text file |
| **# Lines** | Toggles line-number display |
| **— Preset —** | Preset search patterns (Errors, Warnings, Connection, Traceback, …) |
| **Search box** | Live search / filter; **Filter** checkbox hides non-matching lines; ▲▼ navigate matches |

Key log files:
- `ApiKeys.log` — API-key editor activity
- `VPSMonitor.log` — VPS monitoring
- `PBGui.log` — general UI activity

---

## Comments

Open via **Comments** in the sidebar (URL hash: `#comments`).

Manages `_comment_*` top-level entries in `api-keys.json` — free-text notes not associated with any exchange user.

---

## TradFi Data Provider (Stock Perps Backtesting)

Open via **TradFi** in the sidebar (URL hash: `#tradfi`).

Stock-perp backtests for Hyperliquid XYZ symbols require 1-minute OHLCV data for traditional assets (stocks, FX).

> 💡 **Recommended for full stock-perp history:** Add a **Tiingo** profile here, then use PBGui's **Market Data** module to build a comprehensive local 1-minute OHLCV archive with **Build best 1m OHLCV**.

### yfinance (automatic default)

- No configuration needed; automatic fallback for the most recent ~7 days
- Free, no API key required
- **Install** / **Uninstall** buttons manage the Python package

### Extended provider (optional, for older history)

| Provider | Key needed | Free-tier 1m depth | Notes |
|---|---|---|---|
| **alpaca** | key + secret | 5+ years | Free (IEX feed, 15-min delay — fine for backtesting). **Recommended.** |
| **polygon** | key only | 2 years | Paid plans offer longer history |
| **finnhub** | key only | Not usable | Free tier has no 1-minute intraday |
| **alphavantage** | key only | Very limited | 25 API calls/day on free tier |

A registration link is shown when a provider is selected.

Saved profiles show only metadata such as provider, active state, and generation. **Test Connection** uses the saved profile server-side when the fields are empty, or one-time credentials from the authenticated request body before saving. Stored TradFi values cannot be revealed.

PBGui automatically projects the active master-side TradFi profiles into its reserved PB7 `api-keys.json` subtree with atomic merge and retry handling. Do not edit PB7 TradFi entries manually. Replacing a provider key creates a new vault generation; provider rotation is optional and is not required for credential migration.

---

## `api-keys.json` field reference

```json
{
  "myuser": {
    "exchange": "bybit",
    "key": "...",
    "secret": "...",
    "passphrase": "...",
    "quote": "USDT",
    "options": {"defaultType": "swap"},
    "extra": {}
  },
  "myhl": {
    "exchange": "hyperliquid",
    "wallet_address": "0x...",
    "private_key": "0x...",
    "is_vault": false
  }
}
```

---

## Upstream reference

- https://github.com/enarjord/passivbot
