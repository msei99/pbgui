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
| **Updates** | Pending Linux package updates; healthy rows show only the count, while Stale/Missing/Error states remain visible |
| **PBGui / PBGui Branch / PBGui GitHub** | Installed version, branch, and whether it matches GitHub origin |
| **PB7 / PB7 Branch / PB7 GitHub** | PB7 version, branch, and whether it matches GitHub origin |
| **PB8 / PB8 Branch / PB8 GitHub** | PB8 version, branch, and whether it matches the current upstream PB8 revision |

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

The Master and VPS detail headers repeat PBGui, PB7, and PB8 version cards with their branch/commit and update status. These values come from the same monitor-agent snapshot as the Overview row.

PB8 updates intentionally use a detached checkout at the verified upstream `master` commit. When that detached commit exactly matches the verified upstream revision, VPS Manager labels it `master` instead of showing the low-level Git state as `unknown`.

**Import Cluster Nodes** reads the local materialized `cluster_nodes` state and imports non-local nodes that have SSH metadata, regardless of their Cluster Sync mode. Disabled Cluster Sync nodes can still be imported into VPS Manager; disabled only means PBCluster should not replicate through that node. The import writes only safe local VPS Manager metadata such as hostname, SSH host, SSH user, SSH port and Remote PBGui Dir; passwords and private keys are not imported. CMC secrets are never VPS Manager fields: Cluster Sync materializes sealed pool generations separately. If local `/etc/hosts` is missing or points the hostname at a different IP, the import preview shows the required host entry changes and the apply step asks for the local sudo password before writing them. The modal asks for each imported host's VPS user password; rows left without a password are skipped, while entered passwords are used once to refresh remote settings, install the monitoring SSH key and keep the password only in the current browser/API session for later SSH-backed actions.

The page keeps a live WebSocket connection for overview rows, progress logs, and branch state. Browser authentication is cookie-only; PBGui never renders the session token into this page or sends a browser Bearer header.

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
| **Install PB8 / Update PB8** | Install PB8 from upstream `master`, or update the existing separate PB8 checkout and virtualenv |
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

PB8 installation is master-only. It uses `<install_dir>/pb8` and `<install_dir>/venv_pb8`, validates the PB8 CLI, Rust extension, and V8 config schema, and then saves `pb8dir` and `pb8venv` in `pbgui.ini`. It does not stop PBRun, PB7, or running PB8 jobs. A selected managed remote master exposes the same action; normal VPS/slave hosts and bulk deployments do not.

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
| **Install PB8 / Update PB8** | Available only when the selected host reports a fresh `master` role; installs or updates the separate PB8 runtime |
| **Update Linux** | Run `apt upgrade` (optional reboot checkbox) |
| **Reboot VPS** | Restart the VPS |
| **Cleanup VPS** | Remove old packages and logs |

The **VPS** content area also contains:
- a setup/config grid for password, swap, and firewall fields; **Apply VPS Changes** saves changes locally and applies changed swap and firewall settings on the VPS
- **PBGui Branch Management** and **PB7 Branch Management** with the same switch / update workflow as the Master page
- a **Remote Monitor** section with server metrics plus PB7 activity data from live processes, PB7 logs, and Cluster Sync desired state
- a **Progress** section with separate status buckets for init, setup and update runs; use the sidebar action buttons to open the shared **Command Log Viewer** whenever you need the full ansible output

In cluster mode, **Update PBGui** and PBGui branch switches on a VPS sync PBCluster service files and restart PBCluster, PBRun and PBCoinData where those services are configured. VPS systemd migration checks include PBCluster, and the remote service/host log views expose `PBCluster.log`. Pure VPS runners still do not need `pbgui-api.service` or `PBApiServer.py`.

The sidebar keeps the detailed log workflows separate from the normal host overview:
- utility actions such as **Task Logs**, **Host Logs**, **Change VPS**, **Initialize**, or **Delete VPS** stay above a divider, while the executable ansible playbook buttons are grouped below it
- **Task Logs** opens a dedicated filtered viewer for all stored playbook logs of the selected VPS, including rotated history files
- actions such as **Initialize**, **Setup VPS**, **Update PBGui**, **Update PBGui and PB7**, **Update Linux**, or **Cleanup VPS** switch the main pane to the shared **Command Log Viewer** automatically
- **Host Logs** opens a dedicated **Host Log Viewer** screen for service logs, running bot logs, and file-style targets such as `PBCluster.log`
- **Back** returns from branch, setup, or log screens to the normal VPS detail view without losing the selected host context
- every callable VPS Manager task now keeps its own current log plus rotated history entries in the shared viewer; the retention defaults to 10 history files and can be changed via `[vps_manager] task_log_history` in `pbgui.ini`
- when ansible output already contains terminal ANSI colors, the shared viewer now preserves those colors in the browser instead of relying only on text-pattern guesses
- ansible task logs with glued result markers or escaped payload control sequences like `\n` / `\r` are now expanded into readable separate display lines inside the shared viewer
- structured ansible result payloads with JSON bodies are now pretty-printed into multiline blocks, which makes nested metadata like `stat` results readable directly in the shared viewer

The status cards above the setup grid are live operator hints:
- Linux package status is independent of the VPS session password. It is read only from the monitor-agent cache and never triggers a direct local or remote package probe.
- **Credential Capability** and **Credential Protocol** report secret-free CMC pool readiness, active-key count, and catalog/materialized generations when available.
- **Monitor Agent Cache** always shows **Source: agent cache** and an explicit **OK**, **Stale**, **Missing**, or **Error** state. A non-OK cache does not mean SSH is offline; SSH connection and telemetry/cache health are displayed separately.
- The panel lists `live_metrics.ndjson`, `instance_snapshot.json`, `host_meta.json`, `service_status.json`, `package_status.json`, and `collector_status.json` with each file's effective age. Live data becomes stale after 15 seconds and collector status after 30 seconds. Collector loops and their last errors are listed separately.
- Pending Linux updates and reboot-needed hints come only from the validated `package_status.json` agent payload. Stale payloads retain and clearly label their last-known update/reboot values. Missing, invalid, or error payloads remain **N/A** and are never shown as zero updates or as a current system.
- The detail page also includes a one-row summary table plus a remote server resource snapshot similar to the previous server view.

For a non-OK agent, use **Update PBGui** in the inline remediation. That action installs or refreshes the agent service, restarts it, and the UI then allows the next 30-second collector cycle to repopulate status. To inspect or recover it manually on the affected host, run exactly:

```bash
systemctl --user status pbgui-monitor-agent.service
systemctl --user restart pbgui-monitor-agent.service
journalctl --user -u pbgui-monitor-agent.service
```

`Cleanup VPS` also installs or refreshes two small daily cleanup cron jobs on the VPS: one user-level job for pip and rustup caches, plus one root-level job for `journalctl --vacuum-time=1d`. The periodic jobs run quietly and do not keep their own log history.

Sensitive login fields such as **VPS User Password** include an eye button so you can temporarily reveal the value entered for the current session. VPS Manager has no raw CoinMarketCap key field or reveal action.

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
6. After setup succeeds, PBGui registers the host locally as a Cluster node candidate. If you are adding this VPS to an existing Cluster, open **System -> Cluster Sync -> Nodes**, set the new node to **Reachable via SSH**, probe it, run **Join**, then add the VPS to the local master's sync peers and run **Install Key** or **Repair All SSH**.
7. If the VPS was already set up before automatic Cluster registration existed, open the VPS detail page and click **Add to Cluster** first. That action writes only local Cluster metadata; it does not SSH to the VPS or join it.
8. After initialization succeeds, use **Change VPS** and **Apply VPS Changes** for normal saved setting changes.

---

## Typical workflows

### Update all servers
1. Click **Master (local)** → **Update PBGui and PB7** → wait for the log to show *successful*
2. For each VPS: click the hostname → **Update PBGui and PB7**

The PBGui update workflow restarts PBCluster for cluster-mode hosts and installs/restarts `pbgui-monitor-agent.service` on VPS hosts. Agent-backed package and collector status may remain stale for up to the next 30-second collector cycle. If you update any host manually with `git pull`, restart PBCluster and the monitor agent on that host afterward with `systemctl --user restart pbgui-pbcluster.service pbgui-monitor-agent.service`.

### Switch to a feature branch
1. Open Master or VPS detail
2. Expand **Branch Management** → select the target branch → click **Switch Branch**

PBGui branch switches use the same PBCluster service sync/restart handling as PBGui updates.

### Materialize API keys
- Use **System -> Cluster Sync** to preview and materialize `api-keys.json` on reachable nodes.
- CMC pool credentials are separate sealed generations. Manage them under **Services -> PBCoinData -> Pool** and let Cluster Sync materialize them; there are no per-VPS CMC keys.
