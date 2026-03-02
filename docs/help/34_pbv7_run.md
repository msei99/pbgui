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
| `:material/home:` | Return to the instance list |
| `:material/save:` | Save changes locally |
| **Import** | Import an existing Passivbot config file |
| **Activate** | Push the saved config to the VPS and restart the bot |
| **Backtest** | Open this instance's config in the Backtest page |
| **Calculate Balance** | Open the Balance Calculator for this instance |
| **Strategy Explorer** | Open the Strategy Explorer pre-loaded with this config |

Key settings in the edit form:

| Section | Description |
|---------|-------------|
| **User** | Select the API-key user (exchange account) |
| **Enabled On** | Target VPS for deployment |
| **Note** | Optional label shown in the list |
| **Long / Short** | Bot parameters — positions, TWE, entry/close ranges |
| **coin filter** | CoinMarketCap-based symbol filter for this instance |

---

## Typical workflows

### Start a new live instance
1. **Add** → select **User** and **Enabled On** (target VPS)
2. Configure **Long / Short** parameters and coin filter → `:material/save:`
3. Click **Activate** → the config is pushed to the VPS and the bot starts

### Update a running bot
1. Open the instance with **Edit** → adjust parameters → `:material/save:`
2. Click **Activate** to push the new config and restart the bot
3. Status column will show 🔄 until the VPS confirms the new version

### Validate parameters before going live
1. Open the instance with **Edit**
2. Click **Backtest** → run a backtest with the same config
3. Click **Strategy Explorer** → explore Pareto metrics interactively

### Disable a bot
1. Open the instance with **Edit** → set **Enabled On** to `disabled` → `:material/save:`
2. Click **Activate** to stop the bot on the VPS
