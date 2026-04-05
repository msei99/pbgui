# PBv7 Run

The **PBv7 Run** page manages your live Passivbot v7 trading instances.
Each instance ties together an API-key user, a bot configuration, and a target VPS.

---

## Instance list

Displays all configured V7 instances in a table.

Sidebar actions:

| Button | Action |
|--------|--------|
| `:recycle:` | Reload all instances and remote status |
| **Add** | Create a new blank instance |
| **Activate ALL** | Push activation for every instance at once |

Table columns:

| Column | Description |
|--------|-------------|
| **Edit** | Open the instance in the edit form |
| **User** | API-key user assigned to this instance |
| **Enabled On** | VPS where the bot is deployed (`disabled` = not deployed) |
| **TWE** | Total Wallet Exposure — `L=` long / `S=` short |
| **Version** | Config version stored locally |
| **Remote** | Live running state pulled from the VPS (see status icons below) |
| **Remote Version** | Config version currently running on the VPS |
| **Note** | Free-text note for your own reference |
| **Delete** | Remove the instance (not allowed while running) |

**Remote status icons:**

| Icon | Meaning |
|------|---------|
| ✅ Running … | Bot is running on the expected VPS with the current config version |
| 🔄 Running … | Bot is running but config version differs (activation required) |
| 🔄 Activation required | Instance is assigned to a VPS but not yet activated |
| ❌ | Instance is disabled |

---

## Edit form

Opens when you click **Edit** on a row or after clicking **Add**.

Sidebar actions:

| Button | Action |
|--------|--------|
| 🏠 Home | Return to the instance list |
| 💾 Save | Save changes and sync config to VPS |
| 📥 Import | Import an existing Passivbot config file |
| 📊 Backtest | Open this instance's config in the Backtest page |
| 🔍 Strategy Explorer | Open the Strategy Explorer pre-loaded with this config |
| 💰 Balance Calculator | Open the standalone Balance Calculator for this instance |
| ⚡ Calc Balance | Calculate the recommended balance inline (shown as a popup) |
| 📖 Guide | Open this guide |

Key settings in the edit form:

| Section | Description |
|---------|-------------|
| **User** | Select the API-key user (exchange account) |
| **Enabled On** | Target VPS for deployment |
| **Note** | Optional label shown in the list |
| **Long / Short** | Bot parameters — positions, TWE, entry/close ranges |
| **Filters** | CoinMarketCap-based symbol filter for this instance |
| **Coin Overrides** | Per-coin parameter overrides (bot params, live mode, separate config files) |
| **Dynamic Ignore** | Preview of symbols automatically ignored based on filter settings |

---

## Typical workflows

### Start a new live instance
1. **Add** → select **User** and **Enabled On** (target VPS)
2. Configure **Long / Short** parameters and coin filters → **💾 Save**
3. Status column will show 🔄 until the VPS confirms activation

### Update a running bot
1. Open the instance with **Edit** → adjust parameters → **💾 Save**
2. The config is automatically pushed to the VPS; status shows 🔄 until confirmed

### Validate parameters before going live
1. Open the instance with **Edit**
2. Click **📊 Backtest** → run a backtest with the same config
3. Click **🔍 Strategy Explorer** → explore Pareto metrics interactively

### Check if you have enough balance
1. Open the instance with **Edit**
2. Click **⚡ Calc Balance** to see the recommended balance needed for your current config
3. Or click **💰 Balance Calculator** to open the full standalone calculator

### Disable a bot
1. Open the instance with **Edit** → set **Enabled On** to `disabled` → **💾 Save**
2. The bot is stopped on the VPS automatically
