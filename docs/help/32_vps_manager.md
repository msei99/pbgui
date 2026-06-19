# VPS Manager

The **VPS Manager** page lets you add, configure, and maintain remote VPS servers that run Passivbot instances.
Each VPS is managed via Ansible playbooks executed from the Master (local) server.

The default menu entry **System -> VPS Manager** opens the standalone **FastAPI** page.

---

## Overview table

The main view shows a table with all servers (Master + VPS) and their current status.

| Column | Description |
|--------|-------------|
| **Name** | Server hostname (Master shown as local) |
| **Role** | 🧠 Master / 💻 VPS |
| **Online** | ✅ reachable / ❌ offline |
| **Bots** | Count of unique running bots currently reported for that VPS |
| **Started** | Last boot time |
| **Updates** | Pending Linux package updates |
| **PBGui / PBGui Branch / PBGui GitHub** | Installed version, branch, and whether it matches GitHub origin |
| **PB7 / PB7 Branch / PB7 GitHub** | PB7 version, branch, and whether it matches GitHub origin |

Overview interactions:

- Click a column header to sort by that column; click the same header again to reverse the sort order.
- Each visible column header includes a small hide icon so you can remove that column directly from the table.
- A single small reset icon at the far right of the header row restores the default Overview columns and default sorting.
- Column visibility and sorting are saved locally in the browser.
- Click and drag across VPS rows to select multiple deploy targets from the Overview table.

Left sidebar:

| Button | Action |
|--------|--------|
| **Add VPS** | Open the add / initialize form |
| **Refresh** | Reload all VPS status and version data via the refresh icon |
| **Overview / Settings / History** | Switch between the live Overview table, shared deploy settings, and recent deploy history |
| **Import by Hostname** | Open the manual hostname import dialog from the **Import Host** sidebar section; the hostname must already resolve via local `/etc/hosts` |
| **Import Cluster Nodes** | Preview and import safe SSH metadata from Cluster Sync nodes into local VPS Manager host entries; secrets are not imported |

The overview uses the normal shared PBGui FastAPI shell. When you switch to **Master** or a specific **VPS**, the left sidebar changes into the view-specific action list. The main overview area stays focused on the table, while host import stays available from the sidebar as a manual hostname-based action or as an **Import Cluster Nodes** action after joining an existing Cluster Sync state.

**Import Cluster Nodes** reads the local materialized `cluster_nodes` state and imports non-local nodes that have SSH metadata, regardless of their Cluster Sync mode. Disabled Cluster Sync nodes can still be imported into VPS Manager; disabled only means PBCluster should not replicate through that node. The import writes only safe local VPS Manager metadata such as hostname, SSH host, SSH user, SSH port and Remote PBGui Dir; VPS passwords, sudo passwords, CoinMarketCap keys and private keys stay local and are not copied from Cluster Sync. If local `/etc/hosts` is missing or points the hostname at a different IP, the import preview shows the required host entry changes and the apply step asks for the local sudo password before writing them. The modal asks for each imported host's VPS user password; rows left without a password are skipped, while entered passwords are used once to refresh remote settings, install the monitoring SSH key and keep the password only in the current browser/API session for later SSH-backed actions.

The page keeps a live WebSocket connection for overview rows, progress logs, and branch state.

Live updates do not close the **VPS** selector anymore while you are choosing another host from the sidebar.

Live refreshes now update only the changed status regions, so typing in Add/Edit forms keeps the cursor in place and opened password reveal fields stay open while new monitor or progress data arrives.

---

## Master management

Open **Master** in the left control rail to manage the local server.

Sidebar actions:

| Button | Action |
|--------|--------|
| **Overview** | Return to the main VPS Manager overview |
| **Back to Master Overview** | Return from branch/log subviews to the normal Master detail view |
| **Task Logs** | Open the dedicated shared log-viewer screen for stored Master playbook logs |
| **Host Logs** | Open the dedicated shared log-viewer screen for local service logs and file targets |
| **PBGui Branch** | Open the PBGui branch management view |
| **PB7 Branch** | Open the PB7 branch management view |
| **Update PBGui and PB7** | Update all components |
| **Update PBGui** | Update only PBGui |
| **Update PB7** | Update only PB7 |
| **Update Linux** | Run Linux package updates (optional reboot checkbox) |
| **Reboot Master** | Restart the local server |
| **Install or Update rustup** | Install or refresh the Rust toolchain |

The **Master** content area also contains:
- a live status grid for CoinData / last command state
- **PBGui Branch Management** for branch or commit switches
- **PB7 Branch Management** with optional custom remote / fork URL support
- a **Monitor** section with server metrics plus PB7 activity data from live processes, PB7 logs, and Cluster Sync desired state
- a **Progress** section with separate status buckets; when a sidebar action starts a master ansible task, the main pane switches to the shared **Command Log Viewer** for the full output, and **Home** returns to the normal master overview

In cluster mode, **Update PBGui** and PBGui branch switches sync the local PBCluster systemd user unit and restart PBCluster. PBCluster is also visible in local service monitoring and service-control views. A manual `git pull` does not restart PBCluster; use `systemctl --user restart pbgui-pbcluster.service` afterward.

---

## VPS management

Click a VPS card in the left rail to open its detail view.

Sidebar actions:

| Button | Action |
|--------|--------|
| **Overview** | Return to the main VPS Manager overview |
| **Hostname selector** | Switch directly between saved VPS hosts without leaving the VPS context |
| **Back** | Return from branch/log/setup subviews to the normal VPS detail view |
| **Task Logs** | Open the dedicated shared log-viewer screen for all stored VPS playbook logs and their history |
| **Host Logs** | Open the dedicated shared log-viewer screen for VPS service logs and file targets |
| **Change VPS** | Open the VPS configuration view for saved host settings |
| **PBGui Branch** | Open the PBGui branch management view |
| **PB7 Branch** | Open the PB7 branch management view |
| **Initialize** | Run initial VPS setup wizard |
| **Delete VPS** | Remove this VPS from PBGui |
| **Update PBGui** | Update PBGui on this VPS |
| **Update PBGui and PB7** | Update all components |
| **Update Linux** | Run `apt upgrade` (optional reboot checkbox) |
| **Reboot VPS** | Restart the VPS |
| **Cleanup VPS** | Remove old packages and logs |
| **Update CoinData API** | Push updated CoinMarketCap API key |

The **VPS** content area also contains:
- a setup/config grid for password, swap, CoinMarketCap key and firewall fields; **Apply VPS Changes** saves changes locally and applies changed swap, firewall, and CoinMarketCap settings on the VPS
- **PBGui Branch Management** and **PB7 Branch Management** with the same switch / update workflow as the Master page
- a **Remote Monitor** section with server metrics plus PB7 activity data from live processes, PB7 logs, and Cluster Sync desired state
- a **Progress** section with separate status buckets for init, setup and update runs; use the sidebar action buttons to open the shared **Command Log Viewer** whenever you need the full ansible output

In cluster mode, **Update PBGui** and PBGui branch switches on a VPS sync PBCluster service files and restart PBCluster, PBRun and PBCoinData where those services are configured. VPS systemd migration checks include PBCluster, and the remote service/host log views expose `PBCluster.log`. Pure VPS runners still do not need `pbgui-api.service` or `PBApiServer.py`.

The sidebar keeps the detailed log workflows separate from the normal host overview:
- utility actions such as **Task Logs**, **Host Logs**, **Change VPS**, **Initialize**, or **Delete VPS** stay above a divider, while the executable ansible playbook buttons are grouped below it
- **Task Logs** opens a dedicated filtered viewer for all stored playbook logs of the selected VPS, including rotated history files
- actions such as **Initialize**, **Setup VPS**, **Update PBGui**, **Update PBGui and PB7**, **Update Linux**, **Cleanup VPS**, or **Update CoinData API** switch the main pane to the shared **Command Log Viewer** automatically
- **Host Logs** opens a dedicated **Host Log Viewer** screen for service logs, running bot logs, and file-style targets such as `PBCluster.log`
- **Back** returns from branch, setup, or log screens to the normal VPS detail view without losing the selected host context
- every callable VPS Manager task now keeps its own current log plus rotated history entries in the shared viewer; the retention defaults to 10 history files and can be changed via `[vps_manager] task_log_history` in `pbgui.ini`
- when ansible output already contains terminal ANSI colors, the shared viewer now preserves those colors in the browser instead of relying only on text-pattern guesses
- ansible task logs with glued result markers or escaped payload control sequences like `\n` / `\r` are now expanded into readable separate display lines inside the shared viewer
- structured ansible result payloads with JSON bodies are now pretty-printed into multiline blocks, which makes nested metadata like `stat` results readable directly in the shared viewer

The status cards above the setup grid are live operator hints:
- **Update Ready** turns green as soon as a VPS user password is entered locally and shows how many Linux updates are pending.
- **CoinData Ready** shows the remaining CoinMarketCap credits when the monitor reports them.
- Pending Linux updates and reboot-needed hints are refreshed from a live SSH package-status probe.
- The detail page also includes a one-row summary table plus a remote server resource snapshot similar to the previous server view.

`Cleanup VPS` also installs or refreshes two small daily cleanup cron jobs on the VPS: one user-level job for pip and rustup caches, plus one root-level job for `journalctl --vacuum-time=1d`. The periodic jobs run quietly and do not keep their own log history.

Sensitive fields such as **VPS User Password** and **CoinMarketCap API Key** include an eye button so you can temporarily reveal the stored value while editing.

The reveal state is preserved during live updates, so opening an eye button does not immediately flip back to hidden when fresh WebSocket data arrives.

---

## Adding a new VPS

1. Click **Add VPS** in the left sidebar, or use **Import by Hostname** from the **Import Host** section to prefill the Add form from a hostname already mapped in local `/etc/hosts`.
2. Follow the step cards at the top of the page:
   - prepare an Ubuntu VPS
   - add the hostname to your local `/etc/hosts`
   - save the VPS record first
   - run **Initialize & Setup VPS** from the Add view, or open the host later and finish the initial setup from the **Change VPS** page
3. Fill the **Step 4: Initialize & Setup your VPS** form and the **Save VPS Entry** defaults.
4. Click **Save VPS** to create or update the stored record.
5. Click **Initialize & Setup VPS** to start the bootstrap run directly from the Add view.
6. After initialization succeeds, use **Change VPS** and **Apply VPS Changes** for normal saved setting changes.

---

## Typical workflows

### Update all servers
1. Click **Master (local)** → **Update PBGui and PB7** → wait for the log to show *successful*
2. For each VPS: click the hostname → **Update PBGui and PB7**

The PBGui update workflow restarts PBCluster for cluster-mode hosts. If you update any host manually with `git pull`, restart PBCluster on that host afterward with `systemctl --user restart pbgui-pbcluster.service`.

### Switch to a feature branch
1. Open Master or VPS detail
2. Expand **Branch Management** → select the target branch → click **Switch Branch**

PBGui branch switches use the same PBCluster service sync/restart handling as PBGui updates.

### Materialize API keys
- Use **System -> Cluster Sync** to preview and materialize `api-keys.json` on reachable nodes.
- Per-VPS: open the VPS detail → **Update CoinData API** only updates the CoinMarketCap key used by market-data filters.
