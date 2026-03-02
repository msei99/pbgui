# PBv7 Optimize

The **PBv7 Optimize** page lets you create, run and analyse Passivbot v7 optimisations (genetic-algorithm based parameter search).
It is organised into four tabs at the top of the page:

| Tab | Purpose |
|-----|---------|
| **Config** | Create and edit optimisation configurations |
| **Queue** | Monitor and control the optimisation runner |
| **Log** | Stream the live log output of a running optimisation |
| **Results** | Browse completed results and explore Pareto fronts |

---

## Config tab

Shows all saved optimisation configurations.

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload the config list |
| `:material/add:` | Create a new optimisation config |
| `:material/edit:` | Edit the selected config |
| `:material/delete:` | Delete the selected config(s) |

When you create or open a config the edit form opens in the Config tab.

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/home:` | Return to the config list |
| `:material/save:` | Save the current config |
| **Add to Queue** | Save and enqueue the config → switches to Queue tab |
| **Preset… / Load / Save / Del** | Manage named parameter presets |

Key settings in the edit form:

| Section | Description |
|---------|-------------|
| **Exchange / Symbols** | Exchange and coins to optimise for |
| **Date range** | Start and end date of the optimisation simulation |
| **Starting balance** | Initial capital |
| **Iterations** | Number of genetic-algorithm generations |
| **CPU cores** | Parallel workers per optimisation run |
| **Population size** | Size of the genetic-algorithm population per generation |
| **Pareto max size** | Maximum configs kept on the Pareto front |
| **Scoring** | Objective functions (e.g. Sharpe, drawdown, profit) |
| **Filter coins** | Apply CoinMarketCap-based coin filters |
| **Starting config** | Seed the optimiser with an existing config |

---

## Queue tab

Shows all pending, running and finished optimisation jobs.

Table columns:

| Column | Description |
|--------|-------------|
| **Start/Stop** | Toggle to start or stop a job |
| **Status** | Current state: *not started / running / optimizing… / complete / error* |
| **Edit** | Opens the job's source config in the edit form (Config tab) |
| **View Logfile** | Tick to stream the job's log on the **Log** tab |
| **Delete** | Removes the job (and its log) from the queue |
| **starting_config** | Whether a seed config was used |
| **exchange** | Exchanges configured for this job |
| **finish** | Indicates a completed job |

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload the queue from disk |
| **Autostart** | Automatically start queued jobs one after another |
| `:material/delete: selected` | Remove selected jobs |
| `:material/delete: finished` | Remove all completed jobs |
| `:material/delete: all` | Remove all jobs (running ones are stopped first) |

---

## Log tab

Streams the live log output of a running (or recently finished) optimisation.

**How to open the log for a specific job:**
1. Go to the **Queue** tab.
2. Tick the **View Logfile** checkbox in the row of the job you want to watch.
3. PBGui switches automatically to the **Log** tab and begins streaming.

If the optimisation has not started yet, the log viewer waits and begins streaming as soon as the log file appears on disk.

Log files are stored in `data/logs/optimizes/`.
Use the **Lines** selector to control how many historical lines are loaded on connect.

---

## Results tab

Browse all completed optimisation results.

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload results |
| `:material/delete: selected` | Delete selected results |
| `:material/delete: all` | Delete all results |

Table columns:

| Column | Description |
|--------|-------------|
| **View Paretos** | Open the Pareto front viewer for this result |
| **🎯 explorer** | Launch the full **Pareto Explorer** page for deep analysis |
| **3d plot** | Generate a 3-D scatter plot of the result (opens external viewer) |
| **Result Time** | When the optimisation completed |
| **Name** | Config name used for this run |
| **Result** | Path to the result directory |

Filter by name using **Filter by Optimize Name**.

### Pareto viewer (inline)
After clicking **View Paretos** for a result you can browse the Pareto-optimal configs:

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload Pareto data |
| `:material/arrow_upward_alt:` | Back to the results list |
| **BT selected** | Enqueue the selected Pareto config as a backtest → switches to Backtest page |
| **BT all** | Enqueue all Pareto configs as backtests |

Use the **Scenario**, **Statistic** and **analyses** selectors to change which Pareto slice is displayed.

### 🎯 Pareto Explorer (full page)
Launches the dedicated **Pareto Explorer** page with interactive scatter plots, correlation analysis, config inspection and one-click backtest queuing.

Click **← Back to Optimize Results** in the sidebar to return to the Results tab.

---

## Typical workflows

### Run a new optimisation
1. **Config** → `:material/add:` → configure exchange, coins, date range, scoring → **Add to Queue**
2. **Queue** → enable *Autostart* (and set CPU cores in the config)
3. Tick **View Logfile** → watch the genetic-algorithm progress on the **Log** tab
4. When complete → go to **Results**

### Explore results
1. **Results** → tick **View Paretos** to browse the Pareto front inline
2. Or tick **🎯 explorer** for the full interactive Pareto Explorer
3. Select promising configs → **BT selected** → backtests are queued on the Backtest page

### Tune an existing config
1. **Config** → select → `:material/edit:` → adjust bounds, scoring or coin list
2. Enable **Starting config** to seed the new run with the best result from a previous run
3. **Add to Queue** → monitor progress on the **Log** tab

### Use presets
Save frequently used parameter sets (bounds, population, scoring) as named presets via the **Preset… / Save** controls in the edit sidebar.
Load them instantly on future configs with **Load**.
