# PBv7 Run

The **PBv7 Run** page manages your live Passivbot v7 trading instances.
Each instance ties together an API-key user, a bot configuration, and a target VPS.

---

## Instance list

Displays all configured V7 instances in a table.

Sidebar actions:

| Button | Action |
|--------|--------|
| **Search / Status** | Filter the shared Run table without changing instance state |
| **Refresh** | Reload all instances and remote status |
| **Add Instance** | Create a new blank instance |
| **Backups** | Browse, filter, load, or delete V7 config backups |

Table columns:

| Column | Description |
|--------|-------------|
| **Name** | Stable instance name |
| **User** | API-key user assigned to this instance |
| **Enabled On** | VPS where the bot is deployed (`disabled` = not deployed) |
| **Status** | Confirmed synchronization/runtime state; `collecting` means no exact observation is available yet |
| **Cfg Ver / Run Ver** | Config version stored locally and the version confirmed by the running process |
| **TWE** | Total Wallet Exposure — `L=` long / `S=` short |
| **Running On** | Hosts reporting the exact managed process identity |
| **Desired** | Cluster desired state when the runtime publishes one; otherwise `-` for V7 |
| **Note** | Free-text note for your own reference |
| **Actions** | P/G/T forced modes, conditional N/Normal, Edit, Balance Calculator, V8 migration, and Delete |

PBGui AI exposes active bots as exact action targets. A `show_log` request from the Run list or another page navigates through the existing instance editor, keeps the action pending, and then calls the same live-log function as the editor's sidebar **Log** button.

The `P`, `G`, and `T` row buttons write PB7 `live.forced_mode_long` and `live.forced_mode_short` in `config.json`, bump the instance config version, create a backup of the previous config, and sync the changed config to the target host. Status shows the configured global mode separately from process synchronization. When either side is forced, `N` returns both global modes to PB7 normal (`n`) through a confirmation bound to the displayed config version; new entries may resume after synchronization, while disabled sides and per-coin modes remain unchanged. These are Passivbot forced-mode actions, not direct exchange orders. The editor displays canonical values such as `graceful_stop` through their matching PB7 dropdown option even when the saved config uses the long form instead of the short alias.

**V8** leaves the V7 run config unchanged and passes the complete strategy, Backtest, and Optimize structure through PB8's official migrator. PBGui removes its own metadata and stale temporary loader path, extracts retired price-distance names, and drops disabled retired volatility filters before that call. After V7 shape conversion, the extracted value is handed to PB8's canonical config preparation: a positive value becomes `live.order_replacement_churn_gate_market_dist_pct`, while a disabled value becomes `live.order_replacement_churn_gate_activation_count = 0`; explicit conflicting old and new settings are rejected by PB8. Neither retired distance name is written to the V8 draft or shown for manual review. Successful and review-required Run migrations stay in the Run workflow and open as short-lived unsaved PB8 Run editor drafts; they are never written into Backtest config storage. Successful adjusted drafts retain their migration report and show a compact informational notice stating that no manual field review is required. The Run review shows only remaining Run-relevant findings, not `backtest.*` or `optimize.*` findings. A persistent notice shows unresolved fields and original V7 values without inserting retired V7 paths into the V8 config. Existing V7 review styling marks affected canonical bot sections red. Non-reviewable or invalid output still stops with a compact error list.

**Status values:**

| Icon | Meaning |
|------|---------|
| **synced** | Bot is running on the expected VPS with the current config version |
| **outdated** | Bot is running but the config version differs |
| **sync needed** | Instance is assigned but the current version is not confirmed running |
| **stop needed** | A process is still reported although the instance is disabled |
| **collecting** | No exact process observation is available yet |
| **disabled** | Instance is disabled and no process is reported |

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
| **Enabled On** | Target VPS for deployment. The selector shows host names only; an already configured target remains visible when its current capability cannot be confirmed, while validation still blocks unsafe target changes |
| **Note** | Optional label shown in the list |
| **Logging level** | Passivbot logging verbosity selector with `warning`, `info`, `debug`, and `trace` |
| **Long / Short** | Bot parameters — positions, TWE, entry/close ranges |
| **JSON editors** | Raw JSON, Long JSON, Short JSON, Import JSON, and JSON-based Additional Parameters are validated while typing; invalid JSON shows the exact line/column and blocks Save until fixed. Older configs loaded into Run, including pasted imports and Backtest→Run drafts, also keep the `neutralized` / `review` markers in Long/Short JSON |

The Import dialog's **User** field is searchable. Type part of a configured exchange-user name and select the matching suggestion; arbitrary unknown names are rejected.
| **Filters** | CoinMarketCap-based symbol filter for this instance |
| **Apply Filters** | Rebuild both Long/Short approved and ignored lists immediately from Market Cap, volume ratio, tags, CPT, and notice settings |
| **Approved / Ignored coins** | The approved coin pickers now use Passivbot's canonical `all` handling directly. The old `empty_means_all_approved` toggle is no longer shown or written back on save |
| **Coin Overrides** | Per-coin parameter overrides (bot params, live mode, separate config files). Allowed inline parameters load from the installed PB7 runtime; an already open editor refreshes when metadata arrives and shows an explicit error instead of empty sections if loading fails |
| **Dynamic Ignore** | PB7-only runtime watcher that continuously rebuilds coin lists. PB8 shows this disabled because its supervisor currently uses explicit lists produced by Apply Filters. |

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
3. Click **🔍 Strategy Explorer** → inspect entry/close orders, test parameter changes, run bounded simulations, compare fills, and build a replay movie

### Check if you have enough balance
1. Open the instance with **Edit**
2. Click **⚡ Calc Balance** to see the recommended balance needed for your current config
3. Or click **💰 Balance Calculator** to open the full standalone calculator

### Disable a bot
1. Open the instance with **Edit** → set **Enabled On** to `disabled` → **💾 Save**
2. The bot is stopped on the VPS automatically
