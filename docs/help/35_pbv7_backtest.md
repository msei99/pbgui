# PBv7 Backtest

The **PBv7 Backtest** page lets you create, run and evaluate Passivbot v7 backtests.
It is organised into five tabs at the top of the page:

| Tab | Purpose |
|-----|---------|
| **Configs** | Create and edit backtest configurations |
| **Queue** | Monitor and control the backtest runner |
| **Log** | Stream the live log output of a running backtest |
| **Results** | Browse and analyse completed backtest results |
| **Archive** | Access shared community config archives |

---

## Configs tab

### List view
The sidebar provides the following actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload the config list |
| `:material/add:` | Create a new backtest config |
| `:material/chart_data:` | Open the results of the selected config |
| `:material/edit:` | Edit the selected config |
| `:material/delete:` | Delete the selected config (tick *Results* to also remove results) |

### Edit view
When you create or open a config the edit form opens in the Configs tab.
Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/home:` | Return to the config list |
| `:material/save:` | Save the current config |
| **Import** | Import a config from clipboard / file |
| **Results** | Jump straight to this config's results (Results tab) |
| **Calculate Balance** | Open the Balance Calculator for this config |
| **Add to Backtest Queue** | Save and enqueue the config → switches to Queue tab |

---

## Queue tab

Shows all pending, running and finished backtest jobs.

The table has the following columns:

| Column | Description |
|--------|-------------|
| **Start/Stop** | Toggle to start or stop a job |
| **View Results** | Open the results for that job (switches to Results tab) |
| **View Logfile** | Check the box to stream the log on the **Log** tab |
| **Finished** | Indicates a completed job |

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload the queue |
| **Max CPU** | Maximum parallel backtest processes |
| **Autostart** | Automatically start queued jobs |
| `:material/delete: selected` | Remove selected jobs |
| `:material/delete: finished` | Remove all finished jobs |
| `:material/delete: all` | Remove all jobs |

---

## Log tab

Streams the live log output of a running (or recently finished) backtest.

**How to open the log for a specific job:**
1. Go to the **Queue** tab.
2. Tick the **View Logfile** checkbox in the row of the job you want to watch.
3. PBGui switches automatically to the **Log** tab and begins streaming.

If the backtest has not started yet, the log viewer waits and begins streaming as soon as the log file appears on disk.

Use the **Lines** selector to control how many historical lines are loaded on connect.

---

## Results tab

Browse all completed backtest results.

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload results |
| **All Results** | Reset to the global results view |
| **BT selected** | Re-run the selected result as a new backtest |
| **Strategy Explorer** | Open the Strategy Explorer for the selected result |
| **Calculate Balance** | Open the Balance Calculator |
| **Add to Compare** | Add result to the Live-vs-Backtest comparison |
| **Add to Run** | Create a live run from the selected config |
| **Optimize from Result** | Start an optimisation based on the selected result |
| **Add to Config Archive** | Save the config to your personal archive |
| **Go to Config Archives** | Jump to the Archive tab |
| `:material/delete: selected` | Delete selected results |
| `:material/delete: all` | Delete all results |

Sort the table by **Result Time** or other columns. Use the **Filter by Backtest Name** field to narrow down large result sets.

---

## Archive tab

Community and personal config archives.

### Archive list view
Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload archives |
| `:material/settings:` | Configure archive settings |
| **Sync Github** | Pull the latest community archives from GitHub |
| **Push own Archive** | Push your personal archive changes to GitHub |

### Config archive detail view
Clicking into an archive shows its results. Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload results |
| `:material/arrow_upward_alt:` | Back to the archive list |
| **BT selected** | Enqueue the selected config as a new backtest → switches to Queue tab |
| **Calculate Balance** | Open the Balance Calculator → returns to Archive tab |
| **Add to Compare** | Add to Live-vs-Backtest comparison |
| `:material/delete: selected` | Delete selected results |


## Typical workflows

### Run a new backtest
1. **Configs** → `:material/add:` → configure the backtest → **Add to Backtest Queue**
2. **Queue** → set *Max CPU*, enable *Autostart*
3. Tick *View Logfile* → watch progress on **Log** tab
4. When finished → tick *View Results* or go to **Results**

### Re-run / tune a result
1. **Results** → select a result → **BT selected** → adjust dates/balance → **OK**
2. **Queue** → monitor

### Use a community config
1. **Archive** → **Sync Github** → open an archive → select configs → **BT selected**
2. Returns to **Queue** automatically
3. After completion → go back to **Archive** to compare results

### Compare live vs backtest
1. **Results** → select a result → **Add to Compare**
2. Navigate to *Information → Live vs Backtest*
