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
| **P** | Set global Panic forced mode for long and short positions, save the config, and sync it after a safety confirmation |
| **G** | Set global Graceful Stop forced mode for long and short positions, save the config, and sync it after a safety confirmation |
| **T** | Set global Take Profit Only forced mode for long and short positions, save the config, and sync it after a safety confirmation |
| **Edit** | Open the instance in the edit form |
| **V8** | Convert this exact V7 run config with PB8's official migrator and open the new config in PBv8 Backtest |
| **User** | API-key user assigned to this instance |
| **Enabled On** | VPS where the bot is deployed (`disabled` = not deployed) |
| **TWE** | Total Wallet Exposure — `L=` long / `S=` short |
| **Version** | Config version stored locally |
| **Remote** | Live running state pulled from the VPS (see status icons below) |
| **Remote Version** | Config version currently running on the VPS |
| **Note** | Free-text note for your own reference |
| **Delete** | Remove the instance (not allowed while running) |

The `P`, `G`, and `T` row buttons write PB7 `live.forced_mode_long` and `live.forced_mode_short` in `config.json`, bump the instance config version, create a backup of the previous config, and sync the changed config to the target host. They are Passivbot forced-mode actions, not direct exchange orders.

**V8** leaves the V7 run config unchanged. PBGui removes only its own metadata and stale temporary loader path before invoking PB8. If PB8 reports unsupported or manual-review strategy fields, conversion stops and shows those fields instead of publishing a runnable V8 config.

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
| 📊 Backtest | Open this instance's config directly in the FastAPI Backtest page as a draft |
| 🔍 Strategy Explorer | Open the Strategy Explorer pre-loaded with this config |
| 💰 Balance Calculator | Open the standalone Balance Calculator for this instance |
| ⚡ Calc Balance | Calculate the recommended balance inline (shown as a popup) |
| 📖 Guide | Open this guide |

Key settings in the edit form:

| Section | Description |
|---------|-------------|
| **User** | Select the API-key user (exchange account) |
| **Enabled On** | Target VPS for deployment. The selector shows host names only; technical credential diagnostics are reported only when an affected action is validated |
| **Note** | Optional label shown in the list |
| **Logging level** | Passivbot logging verbosity selector with `warning`, `info`, `debug`, and `trace` |
| **Long / Short** | Bot parameters — positions, TWE, entry/close ranges |
| **JSON editors** | Raw JSON, Long JSON, Short JSON, Import JSON, and JSON-based Additional Parameters are validated while typing; invalid JSON shows the exact line/column and blocks Save until fixed. Older configs loaded into Run, including pasted imports and Backtest→Run drafts, also keep the `neutralized` / `review` markers in Long/Short JSON |
| **Filters** | CoinMarketCap-based symbol filter for this instance |
| **Approved / Ignored coins** | The approved coin pickers now use Passivbot's canonical `all` handling directly. The old `empty_means_all_approved` toggle is no longer shown or written back on save |
| **Coin Overrides** | Per-coin parameter overrides (bot params, live mode, separate config files) |
| **Dynamic Ignore** | Preview of symbols automatically ignored based on filter settings |

### Dynamic Ignore and the CMC pool

Dynamic Ignore is a target-host capability, not a per-instance or per-VPS key setting. Before save, sync, or start, PBGui checks secret-free host metadata for credential protocol v2, an active local CMC pool, and matching catalog/materialized generations. If the target reports no active pool or its status is still unknown, the action is blocked with the reported reason. Materialize the Cluster CMC pool on that host first. Disabled instances do not require pool readiness.

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
