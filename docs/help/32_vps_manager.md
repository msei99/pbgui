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
| **Updates** | Pending Linux package updates; click a non-zero count to review package names, installed/candidate versions, source, and Security/Kernel/Routine classification |
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
| **Refresh** | Optionally force an immediate reload of all VPS status and version data via the refresh icon |
| **Overview / Settings / History** | Switch between the live Overview table, shared deploy settings, and recent deploy history |
| **Import by Hostname** | Open the manual hostname import dialog from the **Import Host** sidebar section; the hostname must already resolve via local `/etc/hosts` |
| **Import Cluster Nodes** | Preview and import safe SSH metadata from Cluster Sync nodes into local VPS Manager host entries; secrets are not imported |

The overview uses the normal shared PBGui FastAPI shell. When you switch to **Master** or a specific **VPS**, the left sidebar changes into the view-specific action list. The main overview area stays focused on the table, while host import stays available from the sidebar as a manual hostname-based action or as an **Import Cluster Nodes** action after joining an existing Cluster Sync state.

The Master and VPS detail headers repeat PBGui, PB7, and PB8 version cards with their branch/commit and update status. These values come from the same monitor-agent snapshot as the Overview row.

The normal **Update PB8** action intentionally returns PB8 to a detached checkout at the verified upstream `master` commit. When that detached commit exactly matches the verified upstream revision, VPS Manager labels it `master` instead of showing the low-level Git state as `unknown`. The explicit **PB8 Branch** view can instead track a validated v8 branch or commit from the configured upstream or a custom remote/fork.

**Import Cluster Nodes** reads the local materialized `cluster_nodes` state and imports non-local nodes that have SSH metadata, regardless of their Cluster Sync mode. Disabled Cluster Sync nodes can still be imported into VPS Manager; disabled only means PBCluster should not replicate through that node. The import writes only safe local VPS Manager metadata such as hostname, SSH host, SSH user, SSH port and Remote PBGui Dir; passwords and private keys are not imported. CMC secrets are never VPS Manager fields: Cluster Sync materializes sealed pool generations separately. If local `/etc/hosts` is missing or points the hostname at a different IP, the import preview shows the required host entry changes and the apply step asks for the local sudo password before writing them. The modal asks for each imported host's VPS user password; rows left without a password are skipped, while entered passwords are used once to refresh remote settings, install the monitoring SSH key and keep the password only in the current browser/API session for later SSH-backed actions.

The page keeps a live WebSocket connection for overview rows, progress logs, and branch state. PBGui checks the tracked PBGui branch head automatically every 60 seconds and refreshes the full release metadata when it changes, so update colors do not require the manual Refresh action. Browser authentication is cookie-only; PBGui never renders the session token into this page or sends a browser Bearer header.

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
| **PB8 Branch** | Open PB8 branch management when PB8 is installed |
| **Update PBGui and PB7 / Update PBGui and PB8** | Update PBGui together with the selected runtime; combined PB7+PB8 hosts offer both actions |
| **Update PBGui** | Update only PBGui |
| **Install PB7 / Update PB7** | Install a missing PB7 runtime or update the existing PB7 checkout and virtualenv |
| **Install PB8 / Update PB8** | Install PB8 from upstream `master`, or update the existing separate PB8 checkout and virtualenv |
| **Update Linux** | Run Linux package updates (optional reboot checkbox) |
| **Reboot Master** | Restart the local server |
| **Install or Update rustup** | Install or refresh the Rust toolchain |

The **Master** content area also contains:
- a live status grid for CoinData / last command state
- **PBGui Branch Management** for branch or commit switches
- **PB7 Branch Management** with optional custom remote / fork URL support
- **PB8 Branch Management** for installed PB8 checkouts, with independent branch/commit selection and optional custom remote / fork URL support
- a **Monitor** section with server metrics plus runtime-labelled PB7 and PB8 activity from live processes and Cluster Sync desired state
- a **Progress** section with separate status buckets; when a sidebar action starts a master ansible task, the main pane switches to the shared **Command Log Viewer** for the full output, and **Home** returns to the normal master overview

In cluster mode, **Update PBGui** and PBGui branch switches sync the local PBCluster systemd user unit and restart PBCluster. PBCluster is also visible in local service monitoring and service-control views. A manual `git pull` does not restart PBCluster; use `systemctl --user restart pbgui-pbcluster.service` afterward.

PB8 uses `<install_dir>/pb8` and `<install_dir>/venv_pb8`, validates the PB8 CLI, Rust extension, and V8 config schema, and then saves `pb8dir` and `pb8venv` in `pbgui.ini`. Local and remote masters receive the full PB8 profile for backtest and optimize. Managed VPS runners receive only the minimal live profile, use pip without a download cache, remove the temporary Rust build directory after validation, and enable the shared PBRun controller for PB8 live supervision. A first remote PB8 installation requires at least 3 GiB free on the installation filesystem. A validated existing PB8 runtime remains updateable below that first-install reserve. The playbook reports free space plus PB8 checkout/venv sizes before and after either operation. RAM size is not an installation gate.

A first remote PB7 installation also requires at least 3 GiB free. The VPS Manager disables **Install PB7** when fresh telemetry reports less space, and the playbook repeats the check before rustup or Git downloads. Existing validated PB7 installations remain updateable below the first-install reserve.

Overview bulk updates are runtime-profile aware. **Update Runtime by Profile** and **Update PBGui and Runtime by Profile** dispatch PB7-only hosts to PB7 playbooks, PB8-only hosts to PB8 playbooks, and combined hosts to ordered PB7+PB8 playbooks. Mixed selections therefore retain one parallel or sequential rollout without applying PB7 tasks to PB8-only hosts.
Combined local-master PBGui/runtime updates defer the API restart until every imported runtime update and validation step has finished. The command log remains open during this work and receives its terminal status before the API restarts.
When PB8 is not installed, **Install PB8** is shown as a filled blue action so it remains distinct from routine update buttons.

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
| **PB8 Branch** | Open PB8 branch management when PB8 is installed |
| **Initialize** | Run initial VPS setup wizard |
| **Delete VPS** | Remove this VPS from PBGui |
| **Update PBGui** | Update PBGui on this VPS |
| **Install PB7 / Update PB7** | Install PB7 on a PB8-only host or update an existing PB7 runtime |
| **Update PBGui and PB7 / Update PBGui and PB8** | Update PBGui together with the selected runtime; combined PB7+PB8 hosts offer both actions |
| **Install PB8 / Update PB8** | Installation is available when fresh telemetry reports a supported role and at least 3 GiB free disk; an already validated PB8 runtime can be updated without the first-install reserve |
| **Update Linux** | Run `apt upgrade` (optional reboot checkbox) |
| **Reboot VPS** | Restart the VPS |
| **Cleanup VPS** | Remove old packages and logs |

The **VPS** content area also contains:
- a setup/config grid for password, swap, and firewall fields; **Apply VPS Changes** saves changes locally and applies changed swap and firewall settings on the VPS
- **PBGui Branch Management**, **PB7 Branch Management**, and installed-runtime **PB8 Branch Management** with the same switch / update workflow as the Master page
- a **Remote Monitor** section with server metrics plus runtime-labelled PB7 and PB8 activity from detailed process metrics and Cluster Sync fallbacks
- a **Progress** section with separate status buckets for init, setup and update runs; use the sidebar action buttons to open the shared **Command Log Viewer** whenever you need the full ansible output

In cluster mode, **Update PBGui** and PBGui branch switches on a VPS sync PBCluster service files and restart PBCluster, PBRun and PBCoinData where those services are configured. VPS systemd migration checks include PBCluster, and the remote service/host log views expose `PBCluster.log`. Pure VPS runners still do not need `pbgui-api.service` or `PBApiServer.py`.

PB7 and PB8 branch management keep separate browser state and remote caches. In either runtime view, choose a known remote or enter a fork URL, load its branches and commits, select the local target branch, and run the labelled switch/update action. The action remains disabled when the selected source branch is missing from a loaded remote. Live status updates do not rebuild the branch panel while one of its selectors is open. Running normal **Update PB8** without branch-management selections still restores the verified upstream `master`; use **PB8 Branch** only when intentionally tracking another validated v8 branch or commit.

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
- local **Update PB8** retries automatically recover an orphaned writer lock left by a crashed update when the lock is empty, at least five minutes old, and no Ansible/PB8 build process is still running; the recovery is recorded in the task log, while recent, populated, or actively owned locks remain blocked

The status cards above the setup grid are live operator hints:
- Linux package status is independent of the VPS session password. Normal display refreshes read only from the monitor-agent cache. A successful **Update Linux** performs one final package probe after any requested reboot, atomically updates that cache, and makes the master consume it immediately.
- Click a non-zero **Updates** value in Overview or the host header to inspect the cached apt package list, including newly installed dependencies and planned removals. Security updates are marked for prompt installation, removals require a dependency/service review, kernel updates recommend a maintenance window and possible reboot, and routine updates can be scheduled with normal maintenance. Incomplete lists remain unclassified instead of understating urgency. Older agent caches remain readable but can show only the count until PBGui refreshes the agent payload.
- **Credential Capability** and **Credential Protocol** report secret-free CMC pool readiness, active-key count, and catalog/materialized generations when available.
- **Monitor Agent Cache** always shows **Source: agent cache** and an explicit **OK**, **Stale**, **Missing**, or **Error** state. A non-OK cache does not mean SSH is offline; SSH connection and telemetry/cache health are displayed separately.
- The panel lists `live_metrics.ndjson`, `instance_snapshot.json`, `host_meta.json`, `service_status.json`, `package_status.json`, and `collector_status.json` with each file's effective age. Live data becomes stale after 15 seconds and collector status after 30 seconds. Collector loops and their last errors are listed separately.
- Pending Linux updates and reboot-needed hints come only from the validated `package_status.json` agent payload. A positive reboot hint collected before the host's current boot is discarded because that reboot has already happened. Other stale payloads retain and clearly label their last-known values. Missing, invalid, or error payloads remain **N/A** and are never shown as zero updates or as a current system.
- PB8 **PNL Tdy** uses the exchange fill timestamp embedded in each individual PB8 fill log entry. Undated startup batch summaries represent historical synchronization and are never assigned to the current day.
- Click a bot's CPU, memory, or swap value to open its runtime-specific 24-hour history. PB7 and PB8 instances with the same name remain separate, and a valid zero-swap sample is retained.
- PB8 **ERR 4W** and **TB 4W** come from the local monitor agent's bounded UTC-hour scan of native and stderr logs. **PNL Hist** uses the latest authoritative PB8 fill-batch total and stays separate from legacy PB7 history with the same bot name. PB8 does not expose reliable per-day net PNL, so PBGui shows the authoritative total without fabricating a daily curve.
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
4. Select **PB7** for the current runner stack, **PB8 Live only (no PB7)** for a clean minimal PB8 runner, or **PB7 + PB8** to install both runtimes. The combined profile installs PB7 with PBRun first and then the PB8 live profile. PB8-only setup skips the PB7 checkout and virtualenv but enables the shared PBRun controller, and every profile containing PB8 requires at least 3 GiB free for the PB8 build.
5. Click **Save VPS** to create or update the stored record.
6. Click **Initialize & Setup VPS** to start the bootstrap run directly from the Add view. The UI remains on the current host's task log while initialization and setup run. For PB8-only and combined hosts, PB8 clone, virtualenv installation, and validation run as a second play in the same setup job and appear in the same setup log.
7. After setup succeeds, PBGui registers the host locally as a Cluster node candidate. Click **Add to Cluster** on the VPS detail page to complete onboarding. PBGui opens a progress window for registration, SSH repair, identity checks, pull/push synchronization, materialization and PBRun start. The window cannot be closed while onboarding is active; the API-owned job continues across a page reload or navigation and is restored when the VPS detail page is reopened.
8. The same **Add to Cluster** action can be used for a VPS that was set up before automatic candidate registration existed. Manual **Edit**, **Repair SSH**, **Probe Active Nodes** and **Join & Sync** steps are not required for normal VPS onboarding.
9. After initialization succeeds, use **Change VPS** and **Apply VPS Changes** for normal saved setting changes.

If a VPS was reinstalled, its SSH host key changes. The Add-page preflight blocks initialization and shows **Review changed key**. Verify the displayed SHA-256 fingerprint through a trusted channel, then explicitly confirm replacement. PBGui atomically replaces only the conflicting user `known_hosts` entries for that hostname and IP and repeats the connectivity check.

**Import Existing VPS** applies the same protection. A changed key is shown with **Replace stored key and probe again** instead of leaving the import permanently blocked. After you verify and accept the exact displayed fingerprint, PBGui fetches it again, atomically replaces only that hostname and IP, and repeats the import probe. A fingerprint that changed while the dialog was open is rejected. Saving the import reconnects VPS Monitor and requests an immediate PBCluster retry on the same master, so the confirmed key does not require a second review in VPS Manager or Cluster Sync.

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
